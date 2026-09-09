const redis = require("../config/redis");
const { getSourcePool } = require("../config/sourceDb");

const key = "Pending-Regular-Results";
const DAY_MS = 86400000;
const reviewKey = `${key}:review`;
// Older scans interpreted MySQL BIT buffers with Number(), skipping inactive rows.
// Invalidate those checkpoints once so a corrected full pass starts immediately.
const RECOVERY_VERSION = 2;
let recoveryCursor = 0;
let nextRecoveryAt = 0;
let lastRecoveryAt = 0;
let recoveryLoaded = false;
let scannedRows = 0;
let completedPasses = 0;

function recoveryStatus() {
  return { cursor: recoveryCursor, scannedRows, completedPasses,
    lastScanAt: lastRecoveryAt ? new Date(lastRecoveryAt).toISOString() : null,
    nextScanAt: nextRecoveryAt ? new Date(nextRecoveryAt).toISOString() : null };
}

async function client() {
  const value = await redis.getRedisClient();
  if (!value?.isOpen) throw new Error("Pending results queue requires Redis");
  return value;
}

async function enqueue(ids) {
  const members = [...new Set(ids.map(String))];
  if (!members.length) return;
  const c = await client();
  const now = Date.now();
  await c.eval(`
    for i=2,#ARGV do
      if redis.call('HEXISTS',KEYS[3],ARGV[i]) == 0 then
        redis.call('HSETNX',KEYS[2],ARGV[i],ARGV[1])
        redis.call('ZADD',KEYS[1],'NX',ARGV[1],ARGV[i])
      end
    end
    return 1`, { keys: [key, `${key}:firstQueuedAt`, reviewKey], arguments: [String(now), ...members] });
}

async function moveExpired(ids) {
  if (!ids.length) return [];
  const c = await client();
  return c.eval(`
    local moved = {}
    for i=3,#ARGV do
      local id = ARGV[i]
      local first = tonumber(redis.call('HGET',KEYS[2],id))
      if first and tonumber(ARGV[1])-first >= tonumber(ARGV[2]) then
        redis.call('HSET',KEYS[4],id,cjson.encode({marketId=id,firstQueuedAt=first,
          reviewAt=tonumber(ARGV[1]),attempts=tonumber(redis.call('HGET',KEYS[3],id) or '0'),
          reason='unresolved-after-24-hours'}))
        redis.call('ZREM',KEYS[1],id)
        redis.call('HDEL',KEYS[2],id)
        redis.call('HDEL',KEYS[3],id)
        table.insert(moved,id)
      end
    end
    return moved`, { keys: [key, `${key}:firstQueuedAt`, `${key}:attempts`, reviewKey],
    arguments: [String(Date.now()), String(DAY_MS), ...ids] });
}

async function listReview(cursor = "0") {
  const c = await client();
  const page = await c.hScan(reviewKey, cursor, { COUNT: 100 });
  return { cursor: page.cursor, total: await c.hLen(reviewKey),
    entries: page.entries.map((entry) => JSON.parse(entry.value)) };
}

async function excludeReviewed(rows) {
  if (!rows.length) return rows;
  const c = await client();
  const flags = await c.hmGet(reviewKey, rows.map((row) => String(row.marketid)));
  return rows.filter((_row, index) => flags[index] == null);
}

async function remove(ids) {
  if (!ids.length) return;
  const c = await client();
  await c.multi().zRem(key, ids).hDel(`${key}:attempts`, ids).hDel(`${key}:firstQueuedAt`, ids).exec();
}

// Bounded keyset recovery also seeds historical inactive markets after deployment
// or Redis loss. Advance only after enqueue succeeds.
async function recover(accelerate = false) {
  const c = await client();
  if (!recoveryLoaded) {
    const saved = await c.get(`${key}:recovery`);
    if (saved) {
      const value = JSON.parse(saved);
      if (value.version === RECOVERY_VERSION) {
        recoveryCursor = Number(value.cursor) || 0;
        nextRecoveryAt = Number(value.nextRecoveryAt) || 0;
        lastRecoveryAt = Number(value.lastRecoveryAt) || 0;
        scannedRows = Number(value.scannedRows) || 0;
        completedPasses = Number(value.completedPasses) || 0;
      }
    }
    recoveryLoaded = true;
  }
  if (Date.now() < nextRecoveryAt && !(accelerate && recoveryCursor && Date.now() - lastRecoveryAt >= 1000)) return 0;
  const batchSize = accelerate ? 2000 : 500;
  const pool = getSourcePool();
  const [tables] = await pool.query("SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_matchabondendtie'");
  const [rows] = await getSourcePool().query(
    "SELECT id,marketid,isactive+0 AS isactive FROM t_market WHERE id>? ORDER BY id LIMIT ?",
    [recoveryCursor, batchSize],
  );
  const inactive = rows.filter((row) => Number(row.isactive) === 0).map((row) => row.marketid);
  if (inactive.length) {
    const [unsettled] = await pool.query(
      `SELECT m.marketid FROM t_market m WHERE m.marketid IN (${inactive.map(() => "?").join(",")})
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
       ${tables.length ? "AND NOT EXISTS (SELECT 1 FROM t_matchabondendtie r WHERE r.marketid=m.marketid)" : ""}`, inactive,
    );
    await enqueue(unsettled.map((row) => row.marketid));
  }
  const cursor = rows.length === batchSize ? rows[rows.length - 1].id : 0;
  const now = Date.now();
  const checkpoint = { version: RECOVERY_VERSION, cursor, lastRecoveryAt: now,
    nextRecoveryAt: now + (cursor ? (accelerate ? 1000 : 60000) : 3600000),
    scannedRows: scannedRows + rows.length, completedPasses: completedPasses + (cursor ? 0 : 1) };
  // Commit after enqueue; replaying a page after a crash is safe (NX enqueue).
  await c.set(`${key}:recovery`, JSON.stringify(checkpoint));
  recoveryCursor = cursor;
  lastRecoveryAt = now;
  nextRecoveryAt = checkpoint.nextRecoveryAt;
  scannedRows = checkpoint.scannedRows;
  completedPasses = checkpoint.completedPasses;
  return rows.length;
}

async function load({ adaptive = false } = {}) {
  const connection = await client();
  const due = adaptive ? await connection.zCount(key, 0, Date.now()) : 0;
  const recovered = await recover(adaptive && due < 100);
  const c = await client();
  let ids = await c.zRangeByScore(key, 0, Date.now(), { LIMIT: { offset: 0, count: 250 } });
  const moved = new Set(await moveExpired(ids));
  ids = ids.filter((id) => !moved.has(id));
  const reviewCount = await c.hLen(reviewKey);
  if (!ids.length) return { rows: [], recovered, depth: await c.zCard(key), reviewCount };
  const [tables] = await getSourcePool().query(
    "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_matchabondendtie'",
  );
  const [rows] = await getSourcePool().query(
    `SELECT m.id AS candidateid,m.marketid,m.marketname,m.eventid,m.matchname,m.sportid
     FROM t_market m WHERE m.marketid IN (${ids.map(() => "?").join(",")})
     AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ${tables.length ? "AND NOT EXISTS (SELECT 1 FROM t_matchabondendtie r WHERE r.marketid=m.marketid)" : ""}`,
    ids,
  );
  const found = new Set(rows.map((row) => String(row.marketid)));
  await remove(ids.filter((id) => !found.has(id)));
  const sports = new Set(String(process.env.SPORT_IDS || "1,2,4").split(",").map(Number));
  const excluded = rows.filter((row) => !sports.has(Number(row.sportid)));
  await defer(excluded.map((row) => String(row.marketid)));
  return { rows: rows.filter((row) => sports.has(Number(row.sportid))), recovered, depth: await c.zCard(key), reviewCount };
}

function retryDelay(attempt) {
  return attempt <= 1 ? 60000 : attempt === 2 ? 300000 : 1800000;
}

async function defer(ids) {
  if (!ids.length) return;
  const moved = new Set(await moveExpired(ids));
  ids = ids.filter((id) => !moved.has(id));
  if (!ids.length) return;
  const c = await client();
  const now = Date.now();
  const tx = c.multi();
  for (const id of ids) tx.hIncrBy(`${key}:attempts`, id, 1);
  // Existing queue entries acquire an age on their first retry after upgrade.
  for (const id of ids) tx.hSetNX(`${key}:firstQueuedAt`, id, String(now));
  const attempts = await tx.exec();
  const firstQueued = await c.hmGet(`${key}:firstQueuedAt`, ids);
  await c.zAdd(key, ids.map((value, index) => ({
    value,
    score: Math.min(now + retryDelay(Number(attempts[index])), Number(firstQueued[index] || now) + DAY_MS),
  })), { XX: true });
}

module.exports = { enqueue, load, remove, defer, retryDelay, moveExpired, listReview, excludeReviewed, recoveryStatus, __testing__: { recover } };

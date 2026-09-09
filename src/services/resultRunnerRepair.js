const redis = require("../config/redis");
const provider = require("./providerApi");
const sourceDb = require("../config/sourceDb");
const key = "Result-Runner-Repair";

async function enqueue(marketId, selectionId) {
  const c = await redis.getRedisClient();
  await c.zAdd(key, [{ value: JSON.stringify([String(marketId), Number(selectionId)]), score: Date.now() }], { NX: true });
}

async function repairOne() {
  const c = await redis.getRedisClient();
  const [member] = await c.zRangeByScore(key, 0, Date.now(), { LIMIT: { offset: 0, count: 1 } });
  if (!member) return { checked: 0 };
  const [marketId, selectionId] = JSON.parse(member);
  // Reserve a cooldown before I/O, including timeouts and persistence failures.
  await c.zAdd(key, [{ value: member, score: Date.now() + 300000 }], { XX: true });
  if (await c.hExists("Pending-Regular-Results:review", marketId)) {
    await c.zRem(key, member);
    return { checked: 1, marketId, skipped: "manual-review" };
  }
  const pool = sourceDb.getSourcePool();
  const [existing] = await pool.query(
    "SELECT 1 FROM t_matchresult WHERE marketid=? UNION ALL SELECT 1 FROM t_selectionid WHERE marketid=? AND selectionid=? LIMIT 1",
    [marketId, marketId, selectionId],
  );
  if (existing.length) { await c.zRem(key, member); return { checked: 1, marketId, skipped: "already-present" }; }
  const source = require("../cron/marketDiscoverySync").runnerSourceMarketId(marketId);
  const response = await provider.runners(source, { priority: 9, source: "result-runner-repair", retries: 0 });
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  const winner = rows.find((row) => Number(row.runnerId ?? row.selectionId) === selectionId);
  const name = String(winner?.name ?? winner?.nation ?? "").trim();
  if (!name) return { checked: 1, marketId, repaired: false };
  await pool.query(
    `INSERT INTO t_selectionid (createdon,marketid,runner_name,selectionid,is_redis_updated)
     VALUES (NOW(),?,?,?,false) ON DUPLICATE KEY UPDATE runner_name=VALUES(runner_name)`,
    [marketId, name, selectionId],
  );
  await c.zRem(key, member);
  return { checked: 1, marketId, repaired: true };
}

module.exports = { enqueue, repairOne };

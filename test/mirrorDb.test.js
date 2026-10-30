const test = require("node:test");
const assert = require("node:assert/strict");
const mysql = require("mysql2/promise");
const redis = require("../src/config/redis");
const mirror = require("../src/config/mirrorDb");

process.env.MIRROR_DB_ENABLED = "true";
process.env.MIRROR_DB_HOST = "mirror.test";
process.env.MIRROR_DB_DATABASE = "mirror_test";

function fakeRedis() {
  const lists = new Map();
  const strings = new Map();
  const list = (key) => {
    if (!lists.has(key)) lists.set(key, []);
    return lists.get(key);
  };
  const client = {
    isOpen: true,
    lists,
    async rPush(key, value) {
      return list(key).push(value);
    },
    async lTrim(key, start) {
      lists.set(key, list(key).slice(start));
    },
    async lIndex(key, index) {
      return list(key)[index] ?? null;
    },
    async lRange(key, start, stop) {
      return list(key).slice(start, stop + 1);
    },
    async lPop(key) {
      return list(key).shift() ?? null;
    },
    async lLen(key) {
      return list(key).length;
    },
    async memoryUsage(key) {
      return lists.has(key) ? list(key).reduce((bytes, raw) => bytes + raw.length, 0) : null;
    },
    async get(key) {
      return strings.get(key) ?? null;
    },
    async set(key, value, { NX } = {}) {
      if (NX && strings.has(key)) return null;
      strings.set(key, value);
      return "OK";
    },
    async pExpire() {
      return 1;
    },
    async del(key) {
      strings.delete(key);
    },
    multi() {
      const steps = [];
      const chain = {
        rPush: (...args) => (steps.push(() => client.rPush(...args)), chain),
        lTrim: (...args) => (steps.push(() => client.lTrim(...args)), chain),
        exec: async () => {
          for (const step of steps) await step();
        },
      };
      return chain;
    },
  };
  return client;
}

function fakeSource(log) {
  const connection = {
    query: async (sql) => {
      log.push(sql);
      return [[]];
    },
    execute: async (sql) => {
      log.push(sql);
      return [[]];
    },
    beginTransaction: async () => {},
    commit: async () => {},
    rollback: async () => {},
    release: () => {},
  };
  return { query: connection.query, execute: connection.execute, getConnection: async () => connection };
}

async function outbox(client) {
  await mirror.flushRecording();
  return (client.lists.get(mirror.outboxKey()) || []).map((raw) => JSON.parse(raw).sql);
}

test("committed writes reach the outbox as formatted SQL; reads and rollbacks do not", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  const pool = mirror.wrapPool(fakeSource([]));
  const connection = await pool.getConnection();

  await connection.beginTransaction();
  await connection.query("SELECT marketid FROM t_market WHERE marketid IN (?)", [["1.1"]]);
  await connection.query("INSERT INTO t_market (marketid,opendate) VALUES ?", [
    [["1.1", new Date(2026, 9, 13, 10, 0, 0)]],
  ]);
  assert.deepEqual(await outbox(client), [], "nothing is queued before commit");
  await connection.commit();

  await connection.beginTransaction();
  await connection.query("UPDATE t_market SET isactive=? WHERE marketid=?", [false, "1.2"]);
  await connection.rollback();

  await pool.query("UPDATE t_matchfancy SET name=? WHERE fancyid=?", ["Over 6", "9.1"]);
  await connection.execute("INSERT INTO t_fancyresult (fancyid) SELECT ? WHERE NOT EXISTS (SELECT 1)", [
    "9.1",
  ]);

  assert.deepEqual(await outbox(client), [
    ["INSERT INTO t_market (marketid,opendate) VALUES ('1.1', '2026-10-13 10:00:00.000')"],
    ["UPDATE t_matchfancy SET name='Over 6' WHERE fancyid='9.1'"],
    ["INSERT INTO t_fancyresult (fancyid) SELECT '9.1' WHERE NOT EXISTS (SELECT 1)"],
  ]);
});

test("source-id statements are replaced by their natural-key form, or refused", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  const sourceLog = [];
  const connection = await mirror.wrapPool(fakeSource(sourceLog)).getConnection();

  await connection.beginTransaction();
  await mirror.queryWithMirror(
    connection,
    "UPDATE t_market SET isactive=? WHERE id IN (?) ORDER BY id",
    [false, 41],
    "UPDATE t_market SET isactive=? WHERE marketid IN (?)",
    [false, "1.41"],
  );
  await connection.commit();
  await connection.query("UPDATE t_event SET isactive=? WHERE id=?", [false, 7]);

  assert.deepEqual(sourceLog, [
    "UPDATE t_market SET isactive=? WHERE id IN (?) ORDER BY id",
    "UPDATE t_event SET isactive=? WHERE id=?",
  ]);
  assert.deepEqual(await outbox(client), [["UPDATE t_market SET isactive=false WHERE marketid IN ('1.41')"]]);
});

test("queryWithMirror on an unwrapped connection only runs the source statement", async () => {
  const log = [];
  await mirror.queryWithMirror(
    { query: async (sql) => log.push(sql) },
    "UPDATE t SET a=1 WHERE id=?",
    [1],
    "x",
    [],
  );
  assert.deepEqual(log, ["UPDATE t SET a=1 WHERE id=?"]);
});

test("the worker applies entries in order, dead-letters data errors and keeps entries on outages", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  const applied = [];
  let staged = [];
  let failure = null;
  const connection = {
    beginTransaction: async () => {
      staged = [];
    },
    query: async (sql) => {
      for (const statement of sql.split(";\n")) {
        if (failure?.(statement)) {
          const error = new Error(`failed: ${statement}`);
          error.code = failure.code;
          throw error;
        }
        staged.push(statement);
      }
    },
    commit: async () => {
      applied.push(...staged);
      staged = [];
    },
    rollback: async () => {
      staged = [];
    },
    release: () => {},
  };
  t.mock.method(mysql, "createPool", () => ({ getConnection: async () => connection, end: async () => {} }));
  const key = mirror.outboxKey();
  for (const sql of [["A1", "A2"], ["B"], ["C"]]) await client.rPush(key, JSON.stringify({ at: "now", sql }));

  failure = Object.assign((sql) => sql === "B", { code: "ER_DUP_ENTRY" });
  mirror.startMirrorWorker();
  t.after(() => mirror.closeMirror());
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.deepEqual(applied, ["A1", "A2", "C"]);
  assert.equal(client.lists.get(key).length, 0);
  assert.equal(JSON.parse(client.lists.get(`${key}:dead`)[0]).code, "ER_DUP_ENTRY");

  await mirror.closeMirror();
  failure = Object.assign(() => true, { code: "ECONNREFUSED" });
  await client.rPush(key, JSON.stringify({ at: "now", sql: ["D"] }));
  mirror.startMirrorWorker();
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(client.lists.get(key).length, 1, "an unreachable mirror keeps the entry queued");
  assert.match((await mirror.getMirrorStatus()).lastError, /failed: D/);

  failure = null;
  await new Promise((resolve) => setTimeout(resolve, 1200));
  assert.deepEqual(applied, ["A1", "A2", "C", "D"]);
  assert.equal(client.lists.get(key).length, 0);
});

test("bootstrap DDL drops the source auto-increment counter and can switch MyISAM to InnoDB", () => {
  const { mirrorDdl } = require("../scripts/bootstrap-mirror-db");
  const ddl =
    "CREATE TABLE `t_matchresult` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=MyISAM AUTO_INCREMENT=10547 DEFAULT CHARSET=utf8mb4";
  assert.equal(
    mirrorDdl(ddl),
    "CREATE TABLE `t_matchresult` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4",
  );
  assert.match(mirrorDdl(ddl, { innodb: true }), /\) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4$/);
});

test("source-only writes reach the source but never the outbox", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  const sourceLog = [];
  const connection = await mirror.wrapPool(fakeSource(sourceLog)).getConnection();

  await connection.beginTransaction();
  await mirror.querySourceOnly(connection, "INSERT INTO t_marketid_for_rollback (marketid) VALUES (?)", ["1.1"]);
  await connection.query("UPDATE t_market SET isactive=? WHERE marketid=?", [false, "1.1"]);
  await connection.commit();
  await mirror.querySourceOnly(connection, "INSERT INTO t_marketid_for_rollback (marketid) VALUES (?)", ["1.2"]);

  assert.equal(sourceLog.filter((sql) => String(sql).includes("t_marketid_for_rollback")).length, 2);
  assert.deepEqual(await outbox(client), [["UPDATE t_market SET isactive=false WHERE marketid='1.1'"]]);
});

test("the worker applies a backlog in one transaction with few round trips", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  const queries = [];
  let transactions = 0;
  const connection = {
    beginTransaction: async () => {
      transactions += 1;
    },
    query: async (sql) => {
      queries.push(sql);
    },
    commit: async () => {},
    rollback: async () => {},
    release: () => {},
  };
  t.mock.method(mysql, "createPool", () => ({ getConnection: async () => connection, end: async () => {} }));
  const key = mirror.outboxKey();
  for (let index = 0; index < 50; index += 1) {
    await client.rPush(key, JSON.stringify({ at: "now", sql: [`S${index}a`, `S${index}b`] }));
  }

  mirror.startMirrorWorker();
  t.after(() => mirror.closeMirror());
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(client.lists.get(key).length, 0);
  assert.equal(transactions, 1);
  assert.equal(queries.length, 1);
  assert.deepEqual(
    queries[0].split(";\n"),
    Array.from({ length: 50 }, (_, index) => [`S${index}a`, `S${index}b`]).flat(),
  );
});

test("the outbox drops its oldest entries once it grows past its byte budget", async (t) => {
  const client = fakeRedis();
  t.mock.method(redis, "getRedisClient", async () => client);
  process.env.MIRROR_OUTBOX_MAX_BYTES = "1000";
  t.after(() => delete process.env.MIRROR_OUTBOX_MAX_BYTES);
  const pool = mirror.wrapPool(fakeSource([]));
  const write = (index) => pool.query("UPDATE t_market SET marketname=? WHERE marketid=?", ["x".repeat(150), `1.${index}`]);
  for (let index = 1; index <= 10; index += 1) await write(index);
  await mirror.flushRecording();

  const raws = [...client.lists.get(mirror.outboxKey())];
  assert.ok(raws.reduce((bytes, raw) => bytes + raw.length, 0) <= 1000);
  // The newest writes are the ones kept.
  assert.match(JSON.parse(raws.at(-1)).sql[0], /marketid='1\.10'/);
  assert.ok(raws.length < 10 && raws.length > 0);

  process.env.MIRROR_OUTBOX_MAX_BYTES = "0";
  await write(11);
  await write(12);
  assert.equal((await outbox(client)).length, raws.length + 2, "0 disables the byte cap");
});

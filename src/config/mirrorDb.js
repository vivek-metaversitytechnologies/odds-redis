// Mirror database: every write committed to the source database is replayed on a second
// database of the same schema. The source database stays authoritative for every read and
// decision; the mirror only follows. Committed statements are formatted to plain SQL and
// appended to a Redis outbox, and a single worker applies them to the mirror in order, so a
// mirror outage never blocks or fails a source write — its statements wait in the outbox.
const crypto = require("node:crypto");
const mysql = require("mysql2/promise");
const { format } = require("mysql2");
const mysqlTypeCast = require("./mysqlTypeCast");
const { integer, boolean } = require("./env");
const logger = require("../utils/logger");

const STATE = Symbol("mirrorState");
const WRITE_STATEMENT = /^(INSERT|UPDATE|DELETE|REPLACE)\b/i;
// Auto-increment ids differ between the two databases; such statements need a natural-key
// counterpart supplied through queryWithMirror.
const PRIMARY_KEY_PREDICATE = /\b(WHERE|AND|OR)\s+(`?\w+`?\.)?`?id`?\s*(=|IN\s*\()/i;
// Errors caused by the statement's data. Anything else (connection, lock, missing table or
// column) is retried in place so the outbox order is kept and the backlog stays visible.
const DATA_ERRORS = new Set([
  "ER_DUP_ENTRY",
  "ER_DATA_TOO_LONG",
  "ER_TRUNCATED_WRONG_VALUE",
  "ER_TRUNCATED_WRONG_VALUE_FOR_FIELD",
  "ER_WARN_DATA_OUT_OF_RANGE",
  "ER_DATA_OUT_OF_RANGE",
  "ER_BAD_NULL_ERROR",
  "ER_NO_DEFAULT_FOR_FIELD",
  "ER_PARSE_ERROR",
  "ER_WRONG_VALUE_COUNT_ON_ROW",
  "ER_NO_REFERENCED_ROW_2",
  "ER_ROW_IS_REFERENCED_2",
  "ER_CHECK_CONSTRAINT_VIOLATED",
]);
const DEAD_LETTER_LIMIT = 1000;
const APPLY_QUERY_BYTES = 1024 * 1024;

let pool;
let recording = Promise.resolve();
let timer;
let workerRunning = false;
let lockToken;
const stats = {
  recorded: 0,
  applied: 0,
  deadLettered: 0,
  dropped: 0,
  lost: 0,
  refused: 0,
  lastAppliedAt: null,
  lastError: null,
  lastErrorAt: null,
};

function mirrorEnabled() {
  return boolean("MIRROR_DB_ENABLED", false) && Boolean(process.env.MIRROR_DB_HOST);
}

function outboxKey() {
  return process.env.MIRROR_OUTBOX_KEY || `Mirror-Outbox-Rs:${process.env.MIRROR_DB_DATABASE || "mirror"}`;
}

function getMirrorPool() {
  if (pool) return pool;
  pool = mysql.createPool({
    host: process.env.MIRROR_DB_HOST,
    port: Number(process.env.MIRROR_DB_PORT || 3306),
    user: process.env.MIRROR_DB_USERNAME,
    password: process.env.MIRROR_DB_PASSWORD,
    database: process.env.MIRROR_DB_DATABASE,
    waitForConnections: true,
    connectionLimit: Number(process.env.MIRROR_DB_CONNECTION_LIMIT || 4),
    queueLimit: 0,
    // Outbox statements are already formatted and escaped; batching them into one query saves
    // a network round trip per statement to the remote mirror.
    multipleStatements: true,
    dateStrings: true,
    typeCast: mysqlTypeCast,
  });
  return pool;
}

function sqlText(sql) {
  return typeof sql === "string" ? sql : sql?.sql;
}

function isWrite(sql) {
  const text = sqlText(sql);
  return typeof text === "string" && WRITE_STATEMENT.test(text.replace(/^(\s|\/\*[\s\S]*?\*\/)+/, ""));
}

// Formatted with the same escaping and local timezone mysql2 uses for the source pool, so
// Date parameters reach both databases as the same DATETIME text.
function formatStatement(sql, values) {
  const text = sqlText(sql);
  if (PRIMARY_KEY_PREDICATE.test(text)) {
    stats.refused += 1;
    logger.error("[Mirror] statement filters on the source auto-increment id and was not mirrored", {
      sql: text.slice(0, 300),
    });
    return null;
  }
  return format(text, values ?? (typeof sql === "object" ? sql.values : undefined));
}

function record(statements) {
  if (!statements.length) return;
  stats.recorded += 1;
  const entry = JSON.stringify({ at: new Date().toISOString(), sql: statements });
  recording = recording
    .then(() => pushEntry(entry))
    .catch((error) => {
      stats.lost += 1;
      stats.lastError = `outbox write failed: ${error.message}`;
      stats.lastErrorAt = new Date().toISOString();
      logger.error("[Mirror] outbox write failed; the mirror misses this write", {
        error: error.message,
        statements: statements.length,
      });
    });
}

async function pushEntry(entry) {
  const redis = await require("./redis").getRedisClient();
  if (!redis?.isOpen) throw new Error("Redis unavailable");
  const key = outboxKey();
  const max = integer("MIRROR_OUTBOX_MAX_ENTRIES", 200000, { min: 1000 });
  const length = await redis.rPush(key, entry);
  if (length > max) {
    await redis.lTrim(key, -max, -1);
    stats.dropped += length - max;
    logger.error("[Mirror] outbox full; oldest entries dropped", { dropped: length - max, max });
    return;
  }
  await capOutboxBytes(redis, key, length);
}

// The outbox shares Redis with the live odds data. Unbounded during a mirror outage, it filled
// the instance, Redis evicted the odds keys and refused every write. Kept under a byte budget,
// an outage costs the mirror its oldest writes instead.
async function capOutboxBytes(redis, key, length) {
  const maxBytes = integer("MIRROR_OUTBOX_MAX_BYTES", 5 * 1024 * 1024, { min: 0 });
  if (!maxBytes) return;
  let bytes;
  try {
    // Estimated from a sample of elements, so this stays cheap on a long list.
    bytes = await redis.memoryUsage(key);
  } catch {
    return;
  }
  if (!bytes || bytes <= maxBytes) return;
  const dropped = length - Math.floor((length * maxBytes) / bytes);
  if (dropped <= 0) return;
  await redis.lTrim(key, dropped, -1);
  stats.dropped += dropped;
  logger.error("[Mirror] outbox over its byte budget; oldest entries dropped", { dropped, bytes, maxBytes });
}

function capture(state, sql, values) {
  if (state.skipNext) {
    state.skipNext = false;
    return;
  }
  if (!isWrite(sql)) return;
  const statement = formatStatement(sql, values);
  if (!statement) return;
  if (state.inTransaction) state.buffer.push(statement);
  else record([statement]);
}

function wrapConnection(connection) {
  const state = { inTransaction: false, buffer: [], skipNext: false };
  const reset = () => {
    state.inTransaction = false;
    state.buffer = [];
    state.skipNext = false;
  };
  const methods = {
    async query(sql, values) {
      const result = await connection.query(sql, values);
      capture(state, sql, values);
      return result;
    },
    async execute(sql, values) {
      const result = await connection.execute(sql, values);
      capture(state, sql, values);
      return result;
    },
    async beginTransaction() {
      reset();
      const result = await connection.beginTransaction();
      state.inTransaction = true;
      return result;
    },
    async commit() {
      const result = await connection.commit();
      const statements = state.buffer;
      reset();
      record(statements);
      return result;
    },
    async rollback() {
      reset();
      return connection.rollback();
    },
    release() {
      reset();
      return connection.release();
    },
  };
  return new Proxy(connection, {
    get(target, property) {
      if (property === STATE) return state;
      if (Object.hasOwn(methods, property)) return methods[property];
      const value = Reflect.get(target, property);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function wrapPool(source) {
  const state = { inTransaction: false, buffer: [], skipNext: false };
  const methods = {
    async query(sql, values) {
      const result = await source.query(sql, values);
      capture(state, sql, values);
      return result;
    },
    async execute(sql, values) {
      const result = await source.execute(sql, values);
      capture(state, sql, values);
      return result;
    },
    async getConnection() {
      return wrapConnection(await source.getConnection());
    },
  };
  return new Proxy(source, {
    get(target, property) {
      if (Object.hasOwn(methods, property)) return methods[property];
      const value = Reflect.get(target, property);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

// Runs `sql` on the source and mirrors `mirrorSql` in its place. For statements that must
// address source rows by their auto-increment id; the mirror copy uses the natural key.
async function queryWithMirror(connection, sql, params, mirrorSql, mirrorParams) {
  const state = connection?.[STATE];
  if (!state) return connection.query(sql, params);
  state.skipNext = true;
  let result;
  try {
    result = await connection.query(sql, params);
  } finally {
    state.skipNext = false;
  }
  const statement = format(mirrorSql, mirrorParams);
  if (state.inTransaction) state.buffer.push(statement);
  else record([statement]);
  return result;
}

// Queues a statement for the mirror alone, to restore rows the source already holds but the
// mirror never received (written before mirroring began, or an outbox entry that was lost).
function queueForMirror(sql, values) {
  record([format(sql, values)]);
}

// Runs a write on the source only, for source-only tables the mirror schema does not have. A
// missing mirror table is retried in place, so mirroring such a write would stall the outbox.
async function querySourceOnly(connection, sql, params) {
  const state = connection?.[STATE];
  if (!state) return connection.query(sql, params);
  state.skipNext = true;
  try {
    return await connection.query(sql, params);
  } finally {
    state.skipNext = false;
  }
}

function deadLetterable(error) {
  return DATA_ERRORS.has(error?.code);
}

function statementQueries(statements) {
  const queries = [];
  let current = [];
  let size = 0;
  for (const statement of statements) {
    if (current.length && size + statement.length > APPLY_QUERY_BYTES) {
      queries.push(current.join(";\n"));
      current = [];
      size = 0;
    }
    current.push(statement);
    size += statement.length + 2;
  }
  if (current.length) queries.push(current.join(";\n"));
  return queries;
}

// Entries are applied in one transaction, so a failure leaves none of them on the mirror.
async function applyEntries(entries) {
  const connection = await getMirrorPool().getConnection();
  try {
    await connection.beginTransaction();
    try {
      for (const query of statementQueries(entries.flatMap((entry) => entry.sql))) await connection.query(query);
      await connection.commit();
    } catch (error) {
      await connection.rollback().catch(() => {});
      throw error;
    }
  } finally {
    connection.release();
  }
}

// A single worker must own the outbox: a second process popping entries could skip one
// that was never applied (e.g. two processes overlapping during a PM2 restart).
async function holdLock(redis) {
  const key = `${outboxKey()}:lock`;
  const ttl = integer("MIRROR_LOCK_TTL_MS", 30000, { min: 5000 });
  lockToken ||= crypto.randomUUID();
  if ((await redis.get(key)) === lockToken) {
    await redis.pExpire(key, ttl);
    return true;
  }
  return (await redis.set(key, lockToken, { NX: true, PX: ttl })) === "OK";
}

async function drainOutbox() {
  const redis = await require("./redis").getRedisClient();
  if (!redis?.isOpen) throw new Error("Redis unavailable");
  if (!(await holdLock(redis))) return { applied: 0, more: false };
  const key = outboxKey();
  const batch = integer("MIRROR_APPLY_BATCH", 200, { min: 1 });
  let applied = 0;
  while (applied < batch && workerRunning) {
    const raws = await redis.lRange(key, 0, batch - applied - 1);
    if (!raws.length) return { applied, more: false };
    const entries = raws.map((raw) => {
      try {
        return JSON.parse(raw);
      } catch {
        return null;
      }
    });
    if (entries.every(Boolean)) {
      try {
        await applyEntries(entries);
        stats.applied += entries.length;
        stats.lastAppliedAt = new Date().toISOString();
        await redis.lTrim(key, entries.length, -1);
        applied += entries.length;
        continue;
      } catch (error) {
        if (!deadLetterable(error)) throw error;
      }
    }
    // An entry was rejected or unreadable: replay the batch one entry at a time so only that
    // entry is dead-lettered.
    applied += await drainOneByOne(redis, key, raws.length);
  }
  return { applied, more: true };
}

async function drainOneByOne(redis, key, count) {
  let applied = 0;
  while (applied < count && workerRunning) {
    const raw = await redis.lIndex(key, 0);
    if (raw == null) return applied;
    let entry;
    try {
      entry = JSON.parse(raw);
      await applyEntries([entry]);
      stats.applied += 1;
      stats.lastAppliedAt = new Date().toISOString();
    } catch (error) {
      if (entry && !deadLetterable(error)) throw error;
      stats.deadLettered += 1;
      logger.error("[Mirror] statement rejected by the mirror; moved to dead letters", {
        error: error.message,
        code: error.code,
      });
      await redis
        .multi()
        .rPush(
          `${key}:dead`,
          JSON.stringify({ ...(entry || { raw }), error: error.message, code: error.code }),
        )
        .lTrim(`${key}:dead`, -DEAD_LETTER_LIMIT, -1)
        .exec();
    }
    await redis.lPop(key);
    applied += 1;
  }
  return applied;
}

function startMirrorWorker() {
  if (!mirrorEnabled() || workerRunning) return;
  workerRunning = true;
  const idleMs = integer("MIRROR_POLL_MS", 500, { min: 50 });
  const maxBackoffMs = integer("MIRROR_RETRY_MAX_MS", 30000, { min: 1000 });
  let backoffMs = 0;
  const loop = async () => {
    if (!workerRunning) return;
    let delay = idleMs;
    try {
      const { more } = await drainOutbox();
      backoffMs = 0;
      if (more) delay = 0;
    } catch (error) {
      backoffMs = Math.min(maxBackoffMs, backoffMs ? backoffMs * 2 : 1000);
      delay = backoffMs;
      if (stats.lastError !== error.message) {
        logger.error("[Mirror] apply failed; retrying", {
          error: error.message,
          code: error.code,
          retryInMs: delay,
        });
      }
      stats.lastError = error.message;
      stats.lastErrorAt = new Date().toISOString();
    }
    if (workerRunning) timer = setTimeout(() => void loop(), delay);
  };
  logger.info("[Mirror] worker started", { database: process.env.MIRROR_DB_DATABASE, outbox: outboxKey() });
  void loop();
}

async function getMirrorStatus() {
  if (!mirrorEnabled()) return { enabled: false };
  const status = { enabled: true, database: process.env.MIRROR_DB_DATABASE, ...stats };
  const redis = await require("./redis").getRedisClient();
  if (redis?.isOpen) {
    const key = outboxKey();
    const [backlog, deadLetters, oldest] = await Promise.all([
      redis.lLen(key),
      redis.lLen(`${key}:dead`),
      redis.lIndex(key, 0),
    ]);
    status.backlog = backlog;
    status.deadLetters = deadLetters;
    try {
      status.oldestQueuedAt = oldest ? JSON.parse(oldest).at : null;
    } catch {
      status.oldestQueuedAt = null;
    }
  }
  return status;
}

// Resolves once every committed write so far has reached the outbox.
function flushRecording() {
  return recording;
}

async function closeMirror() {
  workerRunning = false;
  clearTimeout(timer);
  await recording;
  if (lockToken) {
    try {
      const redis = await require("./redis").getRedisClient();
      const key = `${outboxKey()}:lock`;
      if (redis?.isOpen && (await redis.get(key)) === lockToken) await redis.del(key);
    } catch {
      // The lock expires on its own.
    }
  }
  if (!pool) return;
  const current = pool;
  pool = undefined;
  await current.end();
}

module.exports = {
  mirrorEnabled,
  getMirrorPool,
  wrapPool,
  queryWithMirror,
  queueForMirror,
  querySourceOnly,
  startMirrorWorker,
  drainOutbox,
  getMirrorStatus,
  flushRecording,
  closeMirror,
  outboxKey,
  isWrite,
};

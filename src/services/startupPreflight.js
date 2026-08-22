const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const redisStore = require("../config/redis");
const cronConfig = require("../config/cron");

const naturalKeys = [
  { table: "t_event", column: "eventid" },
  { table: "t_market", column: "marketid" },
  { table: "t_matchfancy", column: "fancyid" },
];

function present(value) {
  return value != null && String(value).trim() !== "";
}

function validUrl(value, protocols) {
  try {
    return protocols.includes(new URL(value).protocol);
  } catch {
    return false;
  }
}

function environmentErrors(env = process.env) {
  const errors = [];
  const required = ["SOURCE_DB_HOST"];
  for (const name of required) if (!present(env[name])) errors.push(`${name} is required`);
  if (!present(env.SOURCE_DB_USERNAME) && !present(env.SOURCE_DB_USER)) {
    errors.push("SOURCE_DB_USERNAME or SOURCE_DB_USER is required");
  }
  if (!present(env.SOURCE_DB_DATABASE) && !present(env.SOURCE_DB_NAME)) {
    errors.push("SOURCE_DB_DATABASE or SOURCE_DB_NAME is required");
  }
  if (!present(env.REDIS_URL) && !present(env.REDIS_HOST)) {
    errors.push("REDIS_URL or REDIS_HOST is required");
  }
  if (!present(env.PROVIDER_TOKEN) && !present(env.PROVIDER_X_API_KEY)) {
    errors.push("PROVIDER_TOKEN or PROVIDER_X_API_KEY is required");
  }
  if (!present(env.PROVIDER_SOCKET_URL)) errors.push("PROVIDER_SOCKET_URL is required");
  else if (!validUrl(env.PROVIDER_SOCKET_URL, ["http:", "https:", "ws:", "wss:"])) {
    errors.push("PROVIDER_SOCKET_URL must be a valid HTTP(S) or WS(S) URL");
  }
  if (present(env.PROVIDER_BASE_URL) && !validUrl(env.PROVIDER_BASE_URL, ["http:", "https:"])) {
    errors.push("PROVIDER_BASE_URL must be a valid HTTP(S) URL");
  }
  const port = Number(env.PORT || 3000);
  if (!Number.isInteger(port) || port < 1 || port > 65535) errors.push("PORT must be between 1 and 65535");
  return errors;
}

function cronErrors(config = cronConfig) {
  return Object.entries(config)
    .filter(([, value]) => value?.expression && !cron.validate(value.expression))
    .map(([name, value]) => `${name} cron expression is invalid: ${value.expression}`);
}

async function assertNaturalKeyIntegrity(connection) {
  for (const { table, column } of naturalKeys) {
    const [indexes] = await connection.query(
      `SELECT INDEX_NAME, SUB_PART,
              COUNT(*) OVER (PARTITION BY INDEX_NAME) index_columns
         FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?
          AND NON_UNIQUE=0 AND COLUMN_NAME=?`,
      [table, column],
    );
    const usable = indexes.find((index) => Number(index.index_columns) === 1);
    if (!usable) throw new Error(`${table}.${column} must have a single-column UNIQUE index`);

    const [[length]] = await connection.query(
      `SELECT MAX(CHAR_LENGTH(\`${column}\`)) maximum FROM \`${table}\``,
    );
    if (usable.SUB_PART != null && Number(length.maximum || 0) > Number(usable.SUB_PART)) {
      throw new Error(
        `${table}.${column} contains a value longer than its UNIQUE index prefix (${usable.SUB_PART})`,
      );
    }

    const [duplicates] = await connection.query(
      `SELECT \`${column}\`, COUNT(*) copies FROM \`${table}\`
        WHERE \`${column}\` IS NOT NULL GROUP BY \`${column}\` HAVING COUNT(*) > 1 LIMIT 1`,
    );
    if (duplicates.length) {
      throw new Error(`${table}.${column} contains duplicate value ${duplicates[0][column]}`);
    }
  }
}

async function runStartupPreflight() {
  const configurationErrors = [...environmentErrors(), ...cronErrors()];
  if (configurationErrors.length) throw new Error(`Invalid configuration: ${configurationErrors.join("; ")}`);

  const connection = await getSourcePool().getConnection();
  try {
    await connection.ping();
    await assertNaturalKeyIntegrity(connection);
  } finally {
    connection.release();
  }

  const redis = await redisStore.getRedisClient();
  if (!redis?.isOpen) throw new Error("Redis connection is required but unavailable");
  const pong = await redis.ping();
  if (pong !== "PONG") throw new Error(`Redis PING returned ${String(pong)}`);

  return { sourceDatabase: "ready", redis: "ready", schema: "ready", configuration: "ready" };
}

module.exports = {
  naturalKeys,
  environmentErrors,
  cronErrors,
  assertNaturalKeyIntegrity,
  runStartupPreflight,
};

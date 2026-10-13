const mysql = require("mysql2/promise");
const mysqlTypeCast = require("./mysqlTypeCast");
const mirrorDb = require("./mirrorDb");

let pool;

function getSourcePool() {
  if (pool) return pool;
  const source = mysql.createPool({
    host: process.env.SOURCE_DB_HOST,
    port: Number(process.env.SOURCE_DB_PORT || 3306),
    user: process.env.SOURCE_DB_USERNAME || process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    database: process.env.SOURCE_DB_DATABASE || process.env.SOURCE_DB_NAME,
    waitForConnections: true,
    connectionLimit: Number(process.env.SOURCE_DB_CONNECTION_LIMIT || 32),
    queueLimit: 0,
    // Preserve stored IST DATETIME values instead of converting them through the host timezone.
    dateStrings: true,
    typeCast: mysqlTypeCast,
  });
  // With a mirror configured, committed writes are also queued for the mirror database.
  pool = mirrorDb.mirrorEnabled() ? mirrorDb.wrapPool(source) : source;
  return pool;
}

async function checkSourceDbConnection() {
  const connection = await getSourcePool().getConnection();
  connection.release();
}

async function closeSourceDb() {
  if (!pool) return;
  await mirrorDb.flushRecording();
  const current = pool;
  pool = undefined;
  await current.end();
}

module.exports = { getSourcePool, checkSourceDbConnection, closeSourceDb };

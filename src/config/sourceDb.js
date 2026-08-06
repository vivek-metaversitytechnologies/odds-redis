const mysql = require("mysql2/promise");

let pool;

function getSourcePool() {
  if (pool) return pool;
  pool = mysql.createPool({
    host: process.env.SOURCE_DB_HOST,
    port: Number(process.env.SOURCE_DB_PORT || 3306),
    user: process.env.SOURCE_DB_USERNAME || process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    database: process.env.SOURCE_DB_DATABASE || process.env.SOURCE_DB_NAME,
    waitForConnections: true,
    connectionLimit: Number(process.env.SOURCE_DB_CONNECTION_LIMIT || 5),
    queueLimit: 0,
    // Preserve stored IST DATETIME values instead of converting them through the host timezone.
    dateStrings: true,
  });
  return pool;
}

async function checkSourceDbConnection() {
  const connection = await getSourcePool().getConnection();
  connection.release();
}

async function closeSourceDb() {
  if (!pool) return;
  const current = pool;
  pool = undefined;
  await current.end();
}

module.exports = { getSourcePool, checkSourceDbConnection, closeSourceDb };

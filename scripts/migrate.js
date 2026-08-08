const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const mysql = require("mysql2/promise");

require("dotenv").config({ quiet: true });

const migrationsDirectory = path.join(__dirname, "migrations");

function migrationFiles(directory = migrationsDirectory) {
  return fs
    .readdirSync(directory)
    .filter((name) => /^\d+.*\.sql$/i.test(name))
    .sort((left, right) => left.localeCompare(right));
}

function checksum(contents) {
  return crypto.createHash("sha256").update(contents).digest("hex");
}

function databaseConfig() {
  const config = {
    host: process.env.SOURCE_DB_HOST,
    port: Number(process.env.SOURCE_DB_PORT || 3306),
    user: process.env.SOURCE_DB_USERNAME || process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    database: process.env.SOURCE_DB_DATABASE || process.env.SOURCE_DB_NAME,
    multipleStatements: true,
  };
  const missing = Object.entries(config)
    .filter(([key, value]) => !["password", "multipleStatements"].includes(key) && !value)
    .map(([key]) => key);
  if (missing.length) throw new Error(`Missing database configuration: ${missing.join(", ")}`);
  return config;
}

async function migrate() {
  const connection = await mysql.createConnection(databaseConfig());
  let locked = false;
  try {
    const [[lock]] = await connection.query("SELECT GET_LOCK(?, 30) AS acquired", [
      "odds_socket_schema_migrations",
    ]);
    if (Number(lock.acquired) !== 1) throw new Error("Could not acquire the database migration lock");
    locked = true;
    await connection.query(`CREATE TABLE IF NOT EXISTS service_migrations (
      name VARCHAR(255) NOT NULL PRIMARY KEY,
      checksum CHAR(64) NOT NULL,
      applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )`);

    let applied = 0;
    let skipped = 0;
    for (const name of migrationFiles()) {
      const sql = fs.readFileSync(path.join(migrationsDirectory, name), "utf8");
      const digest = checksum(sql);
      const [existing] = await connection.query(
        "SELECT checksum FROM service_migrations WHERE name=? LIMIT 1",
        [name],
      );
      if (existing.length) {
        if (existing[0].checksum !== digest) throw new Error(`Applied migration was modified: ${name}`);
        skipped += 1;
        continue;
      }
      process.stdout.write(`Applying ${name}...\n`);
      await connection.query(sql);
      await connection.query("INSERT INTO service_migrations (name,checksum) VALUES (?,?)", [name, digest]);
      applied += 1;
      process.stdout.write(`Applied ${name}\n`);
    }
    process.stdout.write(`Migrations complete: ${applied} applied, ${skipped} skipped\n`);
    return { applied, skipped };
  } finally {
    if (locked) await connection.query("SELECT RELEASE_LOCK(?)", ["odds_socket_schema_migrations"]);
    await connection.end();
  }
}

if (require.main === module) {
  migrate().catch((error) => {
    process.stderr.write(`Migration failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { checksum, migrationFiles, databaseConfig, migrate };

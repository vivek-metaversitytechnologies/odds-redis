// Creates the service tables on an empty mirror database from the source database's own
// definitions (SHOW CREATE TABLE), then copies the migration ledger so `db:migrate -- --mirror`
// starts from the same point. Copies no data: the mirror receives writes from now on.
//
//   npm run db:bootstrap-mirror               create missing tables
//   npm run db:bootstrap-mirror -- --dry-run  print the DDL only
//   npm run db:bootstrap-mirror -- --innodb   create MyISAM source tables as InnoDB
const mysql = require("mysql2/promise");
const { databaseConfig } = require("./migrate");

require("dotenv").config({ quiet: true });

const TABLES = [
  "t_series",
  "t_event",
  "t_market",
  "t_matchfancy",
  "t_selectionid",
  "t_matchresult",
  "t_fancyresult",
  "t_matchabondendtie",
  "service_migrations",
];

// The auto-increment counter belongs to the source rows; the mirror starts its own.
function mirrorDdl(ddl, { innodb = false } = {}) {
  let result = ddl.replace(/\s+AUTO_INCREMENT=\d+/i, "");
  if (innodb) result = result.replace(/ENGINE=MyISAM/i, "ENGINE=InnoDB");
  return result;
}

async function tablesIn(connection) {
  const [rows] = await connection.query(
    "SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",
  );
  return new Set(rows.map((row) => row.name));
}

async function bootstrapMirror({
  dryRun = false,
  innodb = false,
  log = (line) => process.stdout.write(`${line}\n`),
} = {}) {
  const source = await mysql.createConnection(databaseConfig("SOURCE_DB"));
  const mirror = await mysql.createConnection(databaseConfig("MIRROR_DB"));
  try {
    const [[{ sourceDb }]] = await source.query("SELECT DATABASE() AS sourceDb");
    const [[{ mirrorDb }]] = await mirror.query("SELECT DATABASE() AS mirrorDb");
    const [[sourceServer]] = await source.query("SELECT @@hostname AS host, @@port AS port");
    const [[mirrorServer]] = await mirror.query("SELECT @@hostname AS host, @@port AS port");
    if (
      sourceDb === mirrorDb &&
      sourceServer.host === mirrorServer.host &&
      sourceServer.port === mirrorServer.port
    ) {
      throw new Error("MIRROR_DB_* points at the source database");
    }
    const sourceTables = await tablesIn(source);
    const mirrorTables = await tablesIn(mirror);
    const created = [];
    for (const table of TABLES) {
      if (!sourceTables.has(table)) {
        log(`skip ${table}: not present on the source`);
        continue;
      }
      if (mirrorTables.has(table)) {
        log(`skip ${table}: already present on the mirror`);
        continue;
      }
      const [[row]] = await source.query(`SHOW CREATE TABLE \`${table}\``);
      const ddl = mirrorDdl(row["Create Table"], { innodb });
      const references = [...ddl.matchAll(/REFERENCES `(\w+)`/gi)].map((match) => match[1]);
      const missing = references.filter((name) => !TABLES.includes(name));
      if (missing.length)
        throw new Error(`${table} references tables outside the mirror: ${missing.join(", ")}`);
      if (dryRun) {
        log(`${ddl};\n`);
        continue;
      }
      await mirror.query(ddl);
      created.push(table);
      log(`created ${table}`);
    }
    if (!dryRun && created.includes("service_migrations")) {
      const [ledger] = await source.query("SELECT name,checksum,applied_at FROM service_migrations");
      if (ledger.length) {
        await mirror.query("INSERT INTO service_migrations (name,checksum,applied_at) VALUES ?", [
          ledger.map((entry) => [entry.name, entry.checksum, entry.applied_at]),
        ]);
      }
      log(`copied ${ledger.length} migration ledger entries`);
    }
    return { created };
  } finally {
    await Promise.allSettled([source.end(), mirror.end()]);
  }
}

if (require.main === module) {
  bootstrapMirror({
    dryRun: process.argv.includes("--dry-run"),
    innodb: process.argv.includes("--innodb"),
  }).catch((error) => {
    process.stderr.write(`Mirror bootstrap failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { TABLES, mirrorDdl, bootstrapMirror };

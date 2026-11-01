// Prepares a mirror database that another application already uses (its own tables, columns and
// rows stay untouched) and copies the source rows into it. Schema changes are additive only:
// missing columns, the missing tables and the unique keys the service's upserts rely on.
//
//   node scripts/sync-mirror-db.js                  check only: prints the plan, changes nothing
//   node scripts/sync-mirror-db.js --schema         apply the additive schema changes
//   node scripts/sync-mirror-db.js --copy           copy every source row (mirror tables must be empty)
//   node scripts/sync-mirror-db.js --catchup=<ISO>  copy rows written since <ISO> (run after enabling)
//
// --copy and --catchup expect MIRROR_DB_* to name the new mirror. --copy also requires
// MIRROR_DB_ENABLED=false, so the outbox does not replay writes into a half-copied mirror.
const fs = require("node:fs");
const mysql = require("mysql2/promise");
const { createClient } = require("redis");
const { databaseConfig } = require("./migrate");
const { TABLES, mirrorDdl } = require("./bootstrap-mirror-db");

const MIRRORED = TABLES.filter((table) => table !== "service_migrations");
const BATCH_ROWS = 2000;
// Natural key per table: upserts address it, and catch-up skips rows already present by it.
const NATURAL_KEYS = {
  t_series: ["seriesid", "sportid"],
  t_event: ["eventid"],
  t_market: ["marketid"],
  t_matchfancy: ["fancyid"],
  t_selectionid: ["selectionid", "marketid"],
  t_matchresult: ["marketid"],
  t_fancyresult: ["fancyid"],
  t_matchabondendtie: ["marketid"],
};
// Unique keys the service's ON DUPLICATE KEY upserts need on the mirror.
const REQUIRED_UNIQUE = { t_selectionid: ["selectionid", "marketid"] };

const quote = (name) => `\`${name}\``;

function columnDefinitions(createTable) {
  const definitions = new Map();
  for (const line of createTable.split("\n")) {
    const match = line.match(/^\s+`([^`]+)`\s+(.+?),?$/);
    if (match) definitions.set(match[1], match[2]);
  }
  return definitions;
}

// The other application inserts into these tables without our columns, so an added column may
// not be NOT NULL without a default.
function addableDefinition(definition) {
  if (/\bNOT NULL\b/i.test(definition) && !/\bDEFAULT\b/i.test(definition) && !/AUTO_INCREMENT/i.test(definition)) {
    return definition.replace(/\bNOT NULL\b/i, "NULL");
  }
  return definition;
}

async function columnsOf(connection, table) {
  const [rows] = await connection.query(
    "SELECT COLUMN_NAME name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
    [table],
  );
  return rows.map((row) => row.name);
}

async function uniqueKeys(connection, table) {
  const [rows] = await connection.query(
    `SELECT INDEX_NAME name, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) cols
       FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND NON_UNIQUE=0 GROUP BY INDEX_NAME`,
    [table],
  );
  return rows.map((row) => row.cols);
}

async function schemaPlan(source, mirror) {
  const statements = [];
  for (const table of MIRRORED) {
    const [[row]] = await source.query(`SHOW CREATE TABLE ${quote(table)}`);
    const createTable = row["Create Table"];
    const mirrorColumns = await columnsOf(mirror, table);
    if (!mirrorColumns.length) {
      statements.push(mirrorDdl(createTable, { innodb: true }));
      continue;
    }
    const present = new Set(mirrorColumns.map((name) => name.toLowerCase()));
    const additions = [...columnDefinitions(createTable)]
      .filter(([name]) => !present.has(name.toLowerCase()))
      .map(([name, definition]) => `ADD COLUMN ${quote(name)} ${addableDefinition(definition)}`);
    if (additions.length) statements.push(`ALTER TABLE ${quote(table)} ${additions.join(", ")}`);
    const required = REQUIRED_UNIQUE[table];
    if (required && !(await uniqueKeys(mirror, table)).includes(required.join(","))) {
      statements.push(
        `ALTER TABLE ${quote(table)} ADD UNIQUE KEY ${quote(`uq_${required.join("_")}`)} (${required.map(quote).join(",")})`,
      );
    }
  }
  return statements;
}

function insertSql(table, columns, rows, { upsert }) {
  const list = columns.map(quote).join(",");
  let sql = `INSERT INTO ${quote(table)} (${list}) VALUES ?`;
  if (upsert) sql += ` ON DUPLICATE KEY UPDATE ${columns.map((c) => `${quote(c)}=VALUES(${quote(c)})`).join(",")}`;
  return [sql, [rows.map((row) => columns.map((column) => row[column]))]];
}

// Columns copied: every source column except the source's auto-increment id; the mirror numbers
// its own rows, which keeps them clear of any rows the other application inserts.
async function copyColumns(source, mirror, table) {
  const mirrorColumns = new Set((await columnsOf(mirror, table)).map((name) => name.toLowerCase()));
  const sourceColumns = (await columnsOf(source, table)).filter((name) => name !== "id");
  const missing = sourceColumns.filter((name) => !mirrorColumns.has(name.toLowerCase()));
  if (missing.length) throw new Error(`${table} lacks ${missing.join(",")} on the mirror; run --schema first`);
  return sourceColumns;
}

async function copyTable(source, mirror, table, { where = "1=1", params = [], upsert = false, skipExisting = false }) {
  const columns = await copyColumns(source, mirror, table);
  const key = NATURAL_KEYS[table];
  let lastId = 0;
  let copied = 0;
  for (;;) {
    const [rows] = await source.query(
      `SELECT id,${columns.map(quote).join(",")} FROM ${quote(table)} WHERE id>? AND (${where}) ORDER BY id LIMIT ${BATCH_ROWS}`,
      [lastId, ...params],
    );
    if (!rows.length) break;
    lastId = rows.at(-1).id;
    let batch = rows;
    if (skipExisting) {
      const [present] = await mirror.query(
        `SELECT ${key.map(quote).join(",")} FROM ${quote(table)} WHERE (${key.map(quote).join(",")}) IN (?)`,
        [rows.map((row) => key.map((column) => row[column]))],
      );
      const seen = new Set(present.map((row) => key.map((column) => String(row[column])).join("|")));
      batch = rows.filter((row) => !seen.has(key.map((column) => String(row[column])).join("|")));
    }
    if (batch.length) await mirror.query(...insertSql(table, columns, batch, { upsert }));
    copied += batch.length;
  }
  return copied;
}

async function main() {
  const args = process.argv.slice(2);
  const catchup = args.find((arg) => arg.startsWith("--catchup="))?.split("=")[1];
  const mode = args.includes("--schema") ? "schema" : args.includes("--copy") ? "copy" : catchup ? "catchup" : "check";
  const options = { dateStrings: true };
  const source = await mysql.createConnection({ ...databaseConfig("SOURCE_DB"), ...options });
  const mirror = await mysql.createConnection({ ...databaseConfig("MIRROR_DB"), ...options });
  try {
    const [[s]] = await source.query("SELECT DATABASE() db, @@hostname host");
    const [[m]] = await mirror.query("SELECT DATABASE() db, @@hostname host");
    const [[{ cipher }]] = await mirror.query("SELECT VARIABLE_VALUE cipher FROM performance_schema.session_status WHERE VARIABLE_NAME='Ssl_cipher'").catch(() => [[{ cipher: null }]]);
    console.log("source", s.db, "@", s.host, "| mirror", m.db, "@", m.host, "| TLS", cipher || "off");
    if (s.db === m.db && s.host === m.host) throw new Error("MIRROR_DB_* points at the source database");

    if (mode === "check" || mode === "schema") {
      const statements = await schemaPlan(source, mirror);
      if (!statements.length) console.log("schema: nothing to change");
      for (const statement of statements) {
        console.log(`${mode === "schema" ? "applying" : "planned"}: ${statement.slice(0, 400)}${statement.length > 400 ? " ..." : ""}`);
        if (mode === "schema") await mirror.query(statement);
      }
      for (const table of MIRRORED) {
        const [[{ n }]] = await mirror.query(`SELECT COUNT(*) n FROM ${quote(table)}`).catch(() => [[{ n: "absent" }]]);
        console.log(`mirror ${table}: ${n} rows`);
      }
      if (mode === "check") console.log("check only; nothing changed");
      return;
    }

    if (mode === "copy") {
      if (/^MIRROR_DB_ENABLED=true/m.test(fs.readFileSync(".env", "utf8"))) {
        throw new Error("set MIRROR_DB_ENABLED=false and restart odds-redis first");
      }
      for (const table of MIRRORED) {
        const [[{ n }]] = await mirror.query(`SELECT COUNT(*) n FROM ${quote(table)}`);
        if (n > 0) throw new Error(`mirror ${table} already has ${n} rows; --copy needs empty tables`);
      }
      const startedAt = new Date(Date.now() - 60000).toISOString().slice(0, 19).replace("T", " ");
      for (const table of MIRRORED) {
        const started = Date.now();
        const copied = await copyTable(source, mirror, table, {});
        const [[{ a }]] = await source.query(`SELECT COUNT(*) a FROM ${quote(table)}`);
        const [[{ b }]] = await mirror.query(`SELECT COUNT(*) b FROM ${quote(table)}`);
        console.log(`copied ${table}: ${copied} rows, source ${a}, mirror ${b} (${Date.now() - started}ms)`);
      }
      // Queued writes predate the copy, which already holds their effect; replayed afterwards they
      // could overwrite newer copied rows.
      const redis = createClient({ url: process.env.REDIS_URL });
      await redis.connect();
      const outbox = process.env.MIRROR_OUTBOX_KEY || `Mirror-Outbox-Rs:${process.env.MIRROR_DB_DATABASE || "mirror"}`;
      console.log(`cleared ${await redis.lLen(outbox)} queued mirror writes`);
      await redis.del([outbox, `${outbox}:dead`]);
      await redis.quit();
      console.log(`done. Enable the mirror, restart odds-redis, then run: node scripts/sync-mirror-db.js --catchup='${startedAt}'`);
      return;
    }

    // Catch-up: rows changed on the source between the copy and the mirror being enabled.
    // Tables with an updatedon column are upserted by their unique key; the others only gain
    // rows the mirror does not have yet.
    for (const table of MIRRORED) {
      const columns = await columnsOf(source, table);
      const hasUpdatedOn = columns.includes("updatedon");
      const uniqueOnMirror = (await uniqueKeys(mirror, table)).includes(NATURAL_KEYS[table].join(","));
      const copied = hasUpdatedOn && uniqueOnMirror
        ? await copyTable(source, mirror, table, { where: "updatedon>=?", params: [catchup], upsert: true })
        : await copyTable(source, mirror, table, { where: columns.includes("createdon") ? "createdon>=?" : columns.includes("date") ? "`date`>=?" : "1=1", params: columns.includes("createdon") || columns.includes("date") ? [catchup] : [], skipExisting: true });
      console.log(`caught up ${table}: ${copied} rows`);
    }
  } finally {
    await Promise.allSettled([source.end(), mirror.end()]);
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error("FAILED:", error.message);
    process.exitCode = 1;
  });
}

module.exports = { columnDefinitions, addableDefinition, insertSql };

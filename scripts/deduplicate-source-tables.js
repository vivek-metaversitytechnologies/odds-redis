const mysql = require("mysql2/promise");

require("dotenv").config({ quiet: true });

const definitions = [
  { table: "t_event", key: "eventid", unique: "uq_event_eventid" },
  { table: "t_market", key: "marketid", unique: "uq_market_marketid" },
  { table: "t_matchfancy", key: "fancyid", unique: "uq_fancy_fancyid" },
];

function databaseConfig() {
  return {
    host: process.env.SOURCE_DB_HOST,
    port: Number(process.env.SOURCE_DB_PORT || 3306),
    user: process.env.SOURCE_DB_USERNAME || process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    database: process.env.SOURCE_DB_DATABASE || process.env.SOURCE_DB_NAME,
  };
}

function stamp() {
  return new Date().toISOString().replace(/\D/g, "").slice(0, 14);
}

async function tableStats(connection, { table, key }) {
  const [[row]] = await connection.query(
    `SELECT COUNT(*) total, COUNT(DISTINCT \`${key}\`) unique_ids,
            SUM(\`${key}\` IS NULL) null_ids FROM \`${table}\``,
  );
  return {
    total: Number(row.total),
    uniqueIds: Number(row.unique_ids),
    nullIds: Number(row.null_ids),
  };
}

async function assertSafeSchema(connection) {
  const [triggers] = await connection.query(
    `SELECT EVENT_OBJECT_TABLE table_name, TRIGGER_NAME trigger_name
       FROM information_schema.TRIGGERS
      WHERE TRIGGER_SCHEMA=DATABASE()
        AND EVENT_OBJECT_TABLE IN ('t_event','t_market','t_matchfancy')`,
  );
  if (triggers.length) {
    throw new Error(`Refusing rebuild because table triggers exist: ${JSON.stringify(triggers)}`);
  }
  for (const definition of definitions) {
    const [columns] = await connection.query(`SHOW COLUMNS FROM \`${definition.table}\``);
    if (!columns.some((column) => column.Field === "id")) {
      throw new Error(`${definition.table} has no id column`);
    }
    if (!columns.some((column) => column.Field === definition.key)) {
      throw new Error(`${definition.table} has no ${definition.key} column`);
    }
  }
}

async function deduplicate({ execute = false } = {}) {
  const connection = await mysql.createConnection(databaseConfig());
  let locked = false;
  try {
    await assertSafeSchema(connection);
    const before = {};
    for (const definition of definitions) {
      before[definition.table] = await tableStats(connection, definition);
    }
    process.stdout.write(`${JSON.stringify({ execute, before }, null, 2)}\n`);
    if (!execute) {
      process.stdout.write("Dry run only. Re-run with --execute while the service is stopped.\n");
      return { before };
    }

    const [[lock]] = await connection.query("SELECT GET_LOCK(?, 30) acquired", [
      "odds_socket_natural_id_deduplication",
    ]);
    if (Number(lock.acquired) !== 1) throw new Error("Could not acquire the deduplication lock");
    locked = true;

    const suffix = stamp();
    const swaps = [];
    const result = {};
    for (const definition of definitions) {
      const { table, key, unique } = definition;
      const clean = `${table}_dedupe_${suffix}`;
      const backup = `${table}_backup_${suffix}`;
      process.stdout.write(`Building ${clean} from ${table}...\n`);
      await connection.query(`CREATE TABLE \`${clean}\` LIKE \`${table}\``);
      await connection.query(`ALTER TABLE \`${clean}\` ADD UNIQUE KEY \`${unique}\` (\`${key}\`)`);
      await connection.query(
        `INSERT INTO \`${clean}\`
         SELECT source.* FROM \`${table}\` source
         INNER JOIN (
           SELECT MAX(id) keep_id FROM \`${table}\`
           WHERE \`${key}\` IS NOT NULL GROUP BY \`${key}\`
         ) latest ON latest.keep_id=source.id`,
      );
      await connection.query(
        `INSERT INTO \`${clean}\` SELECT * FROM \`${table}\` WHERE \`${key}\` IS NULL`,
      );
      const cleanStats = await tableStats(connection, { table: clean, key });
      const expected = before[table].uniqueIds + before[table].nullIds;
      if (cleanStats.total !== expected) {
        throw new Error(`${clean} validation failed: expected ${expected}, found ${cleanStats.total}`);
      }
      result[table] = { clean, backup, before: before[table], after: cleanStats };
      swaps.push(`\`${table}\` TO \`${backup}\``, `\`${clean}\` TO \`${table}\``);
    }

    process.stdout.write("Validation passed; atomically swapping all tables...\n");
    await connection.query(`RENAME TABLE ${swaps.join(", ")}`);
    process.stdout.write(`${JSON.stringify({ completed: true, result }, null, 2)}\n`);
    process.stdout.write("Backups retained. Do not delete them until application verification is complete.\n");
    return result;
  } finally {
    if (locked) {
      await connection.query("SELECT RELEASE_LOCK(?)", ["odds_socket_natural_id_deduplication"]);
    }
    await connection.end();
  }
}

if (require.main === module) {
  deduplicate({ execute: process.argv.includes("--execute") }).catch((error) => {
    process.stderr.write(`Deduplication failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { definitions, tableStats, assertSafeSchema, deduplicate };

const mysql = require("mysql2/promise");
const { databaseConfig } = require("./migrate");

require("dotenv").config({ quiet: true });

const lockName = "odds_socket_schema_migrations";

async function inspectMigrationLock({ force = false } = {}) {
  const connection = await mysql.createConnection(databaseConfig());
  try {
    const [[lock]] = await connection.query("SELECT IS_USED_LOCK(?) AS owner", [lockName]);
    const owner = Number(lock.owner);
    if (!Number.isInteger(owner) || owner <= 0) {
      process.stdout.write("Migration lock is free.\n");
      return { locked: false };
    }

    const [sessions] = await connection.query(
      `SELECT ID,USER,HOST,DB,COMMAND,TIME,STATE,LEFT(INFO,300) AS INFO
         FROM information_schema.PROCESSLIST WHERE ID=?`,
      [owner],
    );
    const session = sessions[0] || { ID: owner };
    process.stdout.write(`Migration lock owner: ${JSON.stringify(session)}\n`);
    if (!force) {
      process.stdout.write("Inspect the owner above. Use --force to terminate only this lock owner.\n");
      return { locked: true, owner, session };
    }

    await connection.query(`KILL CONNECTION ${owner}`);
    process.stdout.write(`Terminated migration lock connection ${owner}.\n`);
    return { locked: false, releasedOwner: owner };
  } finally {
    await connection.end();
  }
}

if (require.main === module) {
  inspectMigrationLock({ force: process.argv.includes("--force") }).catch((error) => {
    process.stderr.write(`Migration lock inspection failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { inspectMigrationLock, lockName };

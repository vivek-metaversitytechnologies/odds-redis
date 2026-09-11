require("dotenv").config({ quiet: true });
const db = require("../src/config/sourceDb");
const { supportsFancyNameRepair, genericNameSql, repairableIdSql, isFallbackFancyName, resolveFancyName, repairFancyNames } = require("../src/services/fancyNameService");

async function repairOne(pool, marketId) {
  const [rows] = await pool.query(
    "SELECT name FROM t_matchfancy WHERE fancyid=? UNION ALL SELECT fancyname AS name FROM t_fancyresult WHERE fancyid=?",
    [marketId, marketId],
  );
  if (!rows.length) throw new Error("Fancy market not found");
  const knownNames = [...new Set(rows.map((row) => row.name).filter((name) => !isFallbackFancyName(name)))];
  // Reuse a descriptive stored name when one table is already correct.
  const seed = knownNames.length === 1 ? knownNames[0] : "Fancy2";
  const name = await resolveFancyName(marketId, seed);
  if (isFallbackFancyName(name)) return { marketId, skipped: true, reason: "No unambiguous provider name" };
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    await repairFancyNames(connection, marketId, name);
    await connection.commit();
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally { connection.release(); }
  return { marketId, name, repaired: true };
}

async function repairAll(pool, report = console.log, pause = () => new Promise((resolve) => setTimeout(resolve, 500))) {
  let cursor = "";
  const summary = { checked: 0, repaired: 0, skipped: 0, failed: 0 };
  while (true) {
    const [rows] = await pool.query(
      `SELECT fancyid FROM t_matchfancy
         WHERE fancyid > ? AND ${repairableIdSql("fancyid")}
           AND ${genericNameSql("name")}
       UNION
       SELECT fancyid FROM t_fancyresult
         WHERE fancyid > ? AND ${repairableIdSql("fancyid")}
           AND ${genericNameSql("fancyname")}
       ORDER BY fancyid LIMIT 100`,
      [cursor, cursor],
    );
    if (!rows.length) break;
    for (const row of rows) {
      cursor = String(row.fancyid);
      summary.checked += 1;
      try {
        const result = await repairOne(pool, cursor);
        summary[result.skipped ? "skipped" : "repaired"] += 1;
        report(JSON.stringify(result));
      } catch (error) {
        summary.failed += 1;
        report(JSON.stringify({ marketId: cursor, failed: true, error: error.message }));
      }
      await pause();
    }
  }
  report(JSON.stringify({ summary }));
  return summary;
}

async function main() {
  const marketId = process.argv[2];
  if (marketId === "--all") {
    const summary = await repairAll(db.getSourcePool());
    return summary.failed ? 1 : 0;
  }
  if (!marketId || !supportsFancyNameRepair(marketId)) throw new Error("Usage: node scripts/repair-fancy-name.js <fancy-market-id> | --all");
  const pool = db.getSourcePool();
  const result = await repairOne(pool, marketId);
  console.log(JSON.stringify(result));
  if (result.skipped) return 1;
  const [fancies] = await pool.query("SELECT fancyid,name,status FROM t_matchfancy WHERE fancyid=?", [marketId]);
  const [results] = await pool.query("SELECT fancyid,fancyname,result,resultstatus FROM t_fancyresult WHERE fancyid=?", [marketId]);
  console.log(JSON.stringify({ fancies, results }, null, 2));
  return 0;
}

if (require.main === module) {
  main().then(async (code) => { await db.closeSourceDb(); process.exit(code); }).catch(async (error) => {
    console.error(error.message);
    await db.closeSourceDb();
    process.exit(1);
  });
}

module.exports = { repairOne, repairAll };

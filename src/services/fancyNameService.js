const provider = require("./providerApi");
const logger = require("../utils/logger");

function isGenericSessionName(name) {
  return ["", "fancy2"].includes(String(name || "").trim().toLowerCase().replace(/[\s-]+/g, ""));
}

async function resolveSessionName(marketId, name) {
  if (!String(marketId).toUpperCase().endsWith("-F2") || !isGenericSessionName(name)) return name;
  try {
    const response = await provider.runners(marketId, { source: "fancy-name-repair", retries: 0 });
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    const names = [...new Set(rows
      .filter((row) => !row.marketId || String(row.marketId) === String(marketId))
      .map((row) => String(row.name ?? row.nation ?? "").trim())
      .filter((value) => !isGenericSessionName(value)))];
    return names.length === 1 ? names[0] : name;
  } catch (error) {
    logger.warn("[FancyName] runner name unavailable", { marketId, error: error.message });
    return name;
  }
}

async function repairSessionNames(connection, marketId, name) {
  if (!String(marketId).toUpperCase().endsWith("-F2") || isGenericSessionName(name)) return;
  // Only metadata changes: never reopen a market or alter its settlement.
  await connection.execute(
    "UPDATE t_matchfancy SET name=? WHERE fancyid=? AND (name IS NULL OR LOWER(REPLACE(REPLACE(TRIM(name),' ',''),'-','')) IN ('','fancy2'))",
    [name, marketId],
  );
  await connection.execute(
    "UPDATE t_fancyresult SET fancyname=? WHERE fancyid=? AND (fancyname IS NULL OR LOWER(REPLACE(REPLACE(TRIM(fancyname),' ',''),'-','')) IN ('','fancy2'))",
    [name, marketId],
  );
}

module.exports = { isGenericSessionName, resolveSessionName, repairSessionNames };

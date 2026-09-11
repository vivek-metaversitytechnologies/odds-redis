const provider = require("./providerApi");
const logger = require("../utils/logger");

const REPAIRABLE_SUFFIXES = ["F2", "F3", "OE", "KD", "MT", "CC"];
const FALLBACK_NAMES = ["", "fancy2", "othermarket", "oddeven", "khado", "meter", "cricketcasino"];

function supportsFancyNameRepair(marketId) {
  return REPAIRABLE_SUFFIXES.some((suffix) => String(marketId).toUpperCase().endsWith(`-${suffix}`));
}

function genericNameSql(column) {
  return `(${column} IS NULL OR LOWER(REPLACE(REPLACE(TRIM(${column}),' ',''),'-','')) IN (${FALLBACK_NAMES.map((name) => `'${name}'`).join(",")}))`;
}

function repairableIdSql(column) {
  return `(${REPAIRABLE_SUFFIXES.map((suffix) => `${column} LIKE '%-${suffix}'`).join(" OR ")})`;
}

function isFallbackFancyName(name) {
  return FALLBACK_NAMES.includes(String(name || "").trim().toLowerCase().replace(/[\s-]+/g, ""));
}

async function resolveFancyName(marketId, name) {
  if (!supportsFancyNameRepair(marketId) || !isFallbackFancyName(name)) return name;
  try {
    const response = await provider.runners(marketId, { source: "fancy-name-repair", retries: 0 });
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    const names = [...new Set(rows
      .filter((row) => !row.marketId || String(row.marketId) === String(marketId))
      .map((row) => String(row.name ?? row.nation ?? "").trim())
      .filter((value) => !isFallbackFancyName(value)))];
    return names.length === 1 ? names[0] : name;
  } catch (error) {
    logger.warn("[FancyName] runner name unavailable", { marketId, error: error.message });
    return name;
  }
}

async function repairFancyNames(connection, marketId, name) {
  if (!supportsFancyNameRepair(marketId) || isFallbackFancyName(name)) return;
  // Only metadata changes: never reopen a market or alter its settlement.
  await connection.execute(
    `UPDATE t_matchfancy SET name=? WHERE fancyid=? AND ${genericNameSql("name")}`,
    [name, marketId],
  );
  await connection.execute(
    `UPDATE t_fancyresult SET fancyname=? WHERE fancyid=? AND ${genericNameSql("fancyname")}`,
    [name, marketId],
  );
}

module.exports = { supportsFancyNameRepair, genericNameSql, repairableIdSql, isFallbackFancyName, resolveFancyName, repairFancyNames };

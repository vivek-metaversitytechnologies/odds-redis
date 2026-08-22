const fs = require("node:fs/promises");
const path = require("node:path");
const { integer } = require("../config/env");

function parseJsonObjects(text) {
  const records = [];
  let start = -1;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (start < 0) {
      if (char === "{") {
        start = index;
        depth = 1;
      }
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') inString = true;
    else if (char === "{") depth += 1;
    else if (char === "}") depth -= 1;
    if (depth === 0) {
      try {
        records.push(JSON.parse(text.slice(start, index + 1)));
      } catch {
        /* skip malformed records */
      }
      start = -1;
    }
  }
  return records;
}

async function latestLogFile(directory, prefix) {
  let files;
  try {
    files = await fs.readdir(directory);
  } catch {
    return null;
  }
  const matching = files
    .filter((file) => file.startsWith(prefix) && file.endsWith(".log"))
    .sort()
    .reverse();
  return matching.length ? path.join(directory, matching[0]) : null;
}

function normalize(record, source) {
  const type = record.type || record.message || "log.entry";
  const level =
    record.level ||
    (record.ok === false || String(type).includes("error") || String(type).includes("failed")
      ? "error"
      : "info");
  return { ...record, source, type, level };
}

const LOG_CACHE_MAX_RECORDS = integer("LOG_READER_CACHE_MAX_RECORDS", 2000, { min: 100 });
const sourceCache = new Map();

async function findLogFile(source) {
  const provider = source === "provider";
  const directory = path.resolve(
    provider ? process.env.PROVIDER_LOG_DIR || "logs/provider" : process.env.LOG_DIR || "logs",
  );
  return latestLogFile(directory, provider ? "provider-http-" : "application-");
}

// Bounded cache for the frequently-polled log list: it never needs more than
// `limit` (<=500) most recent records, so only the tail is retained — caching the
// full parsed array of a 20-25MB log would hold far more heap than the source file.
async function readSource(source) {
  const file = await findLogFile(source);
  if (!file) return { source, file: null, records: [] };
  const stats = await fs.stat(file);
  const cached = sourceCache.get(source);
  if (cached && cached.file === file && cached.mtimeMs === stats.mtimeMs && cached.size === stats.size) {
    return cached.result;
  }
  const text = await fs.readFile(file, "utf8");
  const records = parseJsonObjects(text).map((record) => normalize(record, source));
  const bounded = records.length > LOG_CACHE_MAX_RECORDS ? records.slice(-LOG_CACHE_MAX_RECORDS) : records;
  const result = { source, file: path.basename(file), records: bounded };
  sourceCache.set(source, { file, mtimeMs: stats.mtimeMs, size: stats.size, result });
  return result;
}

// Marketid lookups must search the whole file — a match can sit well outside the
// bounded tail readSource() caches — so this always reads and parses fresh.
async function readSourceFull(source) {
  const file = await findLogFile(source);
  if (!file) return { source, file: null, records: [] };
  const text = await fs.readFile(file, "utf8");
  return {
    source,
    file: path.basename(file),
    records: parseJsonObjects(text).map((record) => normalize(record, source)),
  };
}

async function readLogs({ source = "all", limit = 100 } = {}) {
  const sources = source === "all" ? ["application", "provider"] : [source];
  const results = await Promise.all(sources.map(readSource));
  const records = results
    .flatMap((result) => result.records)
    .sort((left, right) => new Date(right.timestamp || 0) - new Date(left.timestamp || 0))
    .slice(0, Math.max(1, Math.min(500, Number(limit) || 100)));
  return { records, files: Object.fromEntries(results.map((result) => [result.source, result.file])) };
}

function containsMarketId(value, marketId, visited = new Set()) {
  if (!value || typeof value !== "object" || visited.has(value)) return false;
  visited.add(value);
  if (value.mid != null && String(value.mid) === String(marketId)) return true;
  return Object.values(value).some((item) => containsMarketId(item, marketId, visited));
}

async function readRawSocketLogs({ marketId, limit = 20 } = {}) {
  const result = await readSourceFull("provider");
  const boundedLimit = Math.max(1, Math.min(100, Number(limit) || 20));
  const records = result.records
    .filter((record) => record.type === "provider.socket.raw" && containsMarketId(record.payload, marketId))
    .sort((left, right) => new Date(right.timestamp || 0) - new Date(left.timestamp || 0))
    .slice(0, boundedLimit);
  return { marketId: String(marketId), records, file: result.file };
}

module.exports = { parseJsonObjects, readLogs, readRawSocketLogs, containsMarketId };

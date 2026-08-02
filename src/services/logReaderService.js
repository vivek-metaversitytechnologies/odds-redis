const fs = require("node:fs/promises");
const path = require("node:path");

function parseJsonObjects(text) {
  const records = [];
  let start = -1; let depth = 0; let inString = false; let escaped = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (start < 0) {
      if (char === "{") { start = index; depth = 1; }
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
      try { records.push(JSON.parse(text.slice(start, index + 1))); } catch { /* skip malformed records */ }
      start = -1;
    }
  }
  return records;
}

async function latestLogFile(directory, prefix) {
  let files;
  try { files = await fs.readdir(directory); } catch { return null; }
  const matching = files.filter((file) => file.startsWith(prefix) && file.endsWith(".log")).sort().reverse();
  return matching.length ? path.join(directory, matching[0]) : null;
}

function normalize(record, source) {
  const type = record.type || record.message || "log.entry";
  const level = record.level || (record.ok === false || String(type).includes("error") || String(type).includes("failed") ? "error" : "info");
  return { ...record, source, type, level };
}

async function readSource(source) {
  const provider = source === "provider";
  const directory = path.resolve(provider ? process.env.PROVIDER_LOG_DIR || "logs/provider" : process.env.LOG_DIR || "logs");
  const file = await latestLogFile(directory, provider ? "provider-http-" : "application-");
  if (!file) return { source, file: null, records: [] };
  const text = await fs.readFile(file, "utf8");
  return { source, file: path.basename(file), records: parseJsonObjects(text).map((record) => normalize(record, source)) };
}

async function readLogs({ source = "all", limit = 100 } = {}) {
  const sources = source === "all" ? ["application", "provider"] : [source];
  const results = await Promise.all(sources.map(readSource));
  const records = results.flatMap((result) => result.records)
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
  const result = await readSource("provider");
  const boundedLimit = Math.max(1, Math.min(100, Number(limit) || 20));
  const records = result.records
    .filter((record) => record.type === "provider.socket.raw"
      && containsMarketId(record.payload, marketId))
    .sort((left, right) => new Date(right.timestamp || 0) - new Date(left.timestamp || 0))
    .slice(0, boundedLimit);
  return { marketId: String(marketId), records, file: result.file };
}

module.exports = { parseJsonObjects, readLogs, readRawSocketLogs, containsMarketId };

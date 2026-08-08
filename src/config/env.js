function integer(name, fallback, { min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER } = {}) {
  const raw = process.env[name];
  if (raw == null || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) return fallback;
  return value;
}

function boolean(name, fallback = false) {
  const raw = process.env[name];
  if (raw == null || raw === "") return fallback;
  if (["true", "1", "yes", "on"].includes(String(raw).toLowerCase())) return true;
  if (["false", "0", "no", "off"].includes(String(raw).toLowerCase())) return false;
  return fallback;
}

function csvIntegers(name, fallback = []) {
  const raw = process.env[name];
  const values = String(raw == null || raw === "" ? fallback.join(",") : raw)
    .split(",")
    .map((value) => Number(value.trim()))
    .filter(Number.isInteger);
  return [...new Set(values)];
}

module.exports = { integer, boolean, csvIntegers };

const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;

function utcDate(value) {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  if (value == null || value === "") return null;
  const text = String(value).trim();
  const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(text);
  const normalized = hasZone ? text : `${text.replace(" ", "T")}Z`;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function utcToIstSql(value) {
  const parsed = utcDate(value);
  if (!parsed) return null;
  return new Date(parsed.getTime() + IST_OFFSET_MS).toISOString().slice(0, 19).replace("T", " ");
}

module.exports = { utcToIstSql };

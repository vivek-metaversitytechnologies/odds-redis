// Normalize boolean BIT columns once at the DB boundary. Larger BIT fields and
// binary columns retain mysql2's native representation.
function mysqlTypeCast(field, next) {
  if (field.type !== "BIT" || field.length !== 1) return next();
  const value = field.buffer();
  if (value === null) return null;
  return value[0] === 0 ? 0 : 1;
}

module.exports = mysqlTypeCast;

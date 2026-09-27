function limitNumber(value) {
  if (!["number", "string"].includes(typeof value) || String(value).trim() === "") return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

// Only the provider's max stake (`mas`) is applied. minbet is a fixed business rule: it is set to
// 100 when discovery creates the market and is never changed by the settings API or the room.
function providerLimits(settings) {
  const max = limitNumber(settings?.mas);
  return max === null ? {} : { providerMaxBet: max };
}

async function persistLimits(connection, rows, fancy = false) {
  const supplied = rows.filter((row) => row.providerMaxBet != null);
  if (!supplied.length) return;
  const table = fancy ? "t_matchfancy" : "t_market";
  const id = fancy ? "fancyid" : "marketid";
  const selects = supplied.map(() => "SELECT ? AS marketid, ? AS eventid, ? AS maxbet").join(" UNION ALL ");
  await connection.query(
    `UPDATE ${table} AS target JOIN (${selects}) AS limits_update
       ON target.${id}=limits_update.marketid AND target.eventid=limits_update.eventid
     SET target.maxbet=limits_update.maxbet`,
    supplied.flatMap((row) => [row.marketId, row.eventId, row.providerMaxBet]),
  );
}

module.exports = { providerLimits, persistLimits };

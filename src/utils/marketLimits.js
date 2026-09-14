function limitNumber(value) {
  if (!["number", "string"].includes(typeof value) || String(value).trim() === "") return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

function providerLimits(settings) {
  const min = limitNumber(settings?.ms);
  const max = limitNumber(settings?.mas);
  return {
    ...(min === null ? {} : { providerMinBet: min }),
    ...(max === null ? {} : { providerMaxBet: max }),
  };
}

async function persistLimits(connection, rows, fancy = false) {
  const supplied = rows.filter((row) => row.providerMinBet != null || row.providerMaxBet != null);
  if (!supplied.length) return;
  const table = fancy ? "t_matchfancy" : "t_market";
  const id = fancy ? "fancyid" : "marketid";
  const selects = supplied
    .map(() => "SELECT ? AS marketid, ? AS eventid, ? AS minbet, ? AS maxbet")
    .join(" UNION ALL ");
  await connection.query(
    `UPDATE ${table} AS target JOIN (${selects}) AS limits_update
       ON target.${id}=limits_update.marketid AND target.eventid=limits_update.eventid
     SET target.minbet=COALESCE(limits_update.minbet,target.minbet),
         target.maxbet=COALESCE(limits_update.maxbet,target.maxbet)`,
    supplied.flatMap((row) => [
      row.marketId,
      row.eventId,
      row.providerMinBet ?? null,
      row.providerMaxBet ?? null,
    ]),
  );
}

module.exports = { providerLimits, persistLimits };

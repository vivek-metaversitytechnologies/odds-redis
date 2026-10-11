// Primary markets decide whether an event is still running. The toss market and a numbered
// secondary bookmaker (Bookmaker2) settle long before the match ends, so neither may close it.
const NUMBERED_BOOKMAKER = /bookmaker\s*\d+$/;

function isPrimaryMarketName(name) {
  const normalized = String(name || "").trim().toLowerCase();
  return normalized === "match odds" || (normalized.includes("bookmaker") && !NUMBERED_BOOKMAKER.test(normalized));
}

// Vendor/discovery rows carry a type; Match Odds rows are not always named.
function isPrimaryMarket(market) {
  if (market?.marketType === "match-odd") return true;
  if (market?.marketType !== "bookmaker" || /-BM2$/i.test(String(market?.marketId || ""))) return false;
  return isPrimaryMarketName(market?.marketName);
}

// SQL equivalent of isPrimaryMarketName for t_market.marketname.
const PRIMARY_MARKET_SQL =
  "(LOWER(marketname)='match odds' OR (LOWER(marketname) LIKE '%bookmaker%' AND LOWER(marketname) NOT REGEXP 'bookmaker[[:space:]]*[0-9]+$'))";

module.exports = { isPrimaryMarketName, isPrimaryMarket, PRIMARY_MARKET_SQL };

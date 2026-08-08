const FANCY_MARKET_TYPES = new Set([
  "session",
  "khado",
  "odd-even",
  "cricket-casino",
  "ball-by-ball",
  "other-market",
  "meter",
]);

const REGULAR_MARKET_TYPES = [
  "bookmaker",
  "tied-match",
  "match-odd",
  "winner-market",
  "TOSS",
  "super-over",
  "goals",
  "line-market",
  "completed-match",
];

const FANCY_MARKET_REQUESTS = ["session", "khado", "odd-even", "cricket-casino", "ball-by-ball"].map(
  (type) => [type],
);

const MARKET_TYPES = [...FANCY_MARKET_TYPES, ...REGULAR_MARKET_TYPES];
const DISCOVERABLE_MARKET_TYPES = new Set(MARKET_TYPES.map((type) => String(type).toLowerCase()));

module.exports = {
  FANCY_MARKET_TYPES,
  REGULAR_MARKET_TYPES,
  FANCY_MARKET_REQUESTS,
  MARKET_TYPES,
  DISCOVERABLE_MARKET_TYPES,
};

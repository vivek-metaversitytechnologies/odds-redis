const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

const loggers = new Map();
let sequence = 0;

function filenamePart(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function fileKey(marketName, marketId) {
  const name = filenamePart(marketName);
  const id = filenamePart(marketId) || "unknown";
  return name ? `${name}-${id}` : id;
}

function getLogger(marketName, marketId) {
  const key = fileKey(marketName, marketId);
  if (!loggers.has(key)) {
    loggers.set(key, winston.createLogger({
      level: "info",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf((info) =>
          JSON.stringify({ timestamp: info.timestamp, type: info.type, ...info.details }),
        ),
      ),
      transports: [
        new winston.transports.DailyRotateFile({
          dirname: path.resolve("logs/line-market"),
          filename: `line-market-${key}-%DATE%.log`,
          datePattern: "YYYY-MM-DD",
          maxSize: "25m",
          maxFiles: "7d",
          zippedArchive: false,
        }),
      ],
      exitOnError: false,
    }));
  }
  return loggers.get(key);
}

function writeLineMarketObservation(source, details = {}) {
  sequence += 1;
  getLogger(details.marketName, details.marketId).info("line-market", {
    type: "market.observed",
    details: { sequence, source, observedAt: new Date().toISOString(), ...details },
  });
}

async function closeLineMarketLog() {
  for (const logger of loggers.values()) logger.close();
  loggers.clear();
  sequence = 0;
}

module.exports = { writeLineMarketObservation, closeLineMarketLog };

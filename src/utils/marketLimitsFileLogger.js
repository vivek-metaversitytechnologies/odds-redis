const path = require("node:path");
const winston = require("winston");
const logger = require("./logger");
require("winston-daily-rotate-file");

let instance;

function readable(value) {
  if (value === null || value === undefined || value === "") return "not-provided";
  return String(value).replaceAll(/\s+/g, " ").trim();
}

function getLogger() {
  if (instance) return instance;
  const transport = new winston.transports.DailyRotateFile({
    dirname: path.resolve(process.env.MARKET_LIMITS_LOG_DIR || "logs/market-limits"),
    filename: "market-limits-%DATE%.log",
    datePattern: "YYYY-MM-DD",
    maxSize: process.env.MARKET_LIMITS_LOG_MAX_SIZE || "25m",
    maxFiles: process.env.MARKET_LIMITS_LOG_MAX_FILES || "14d",
    zippedArchive: false,
  });
  instance = winston.createLogger({
    level: "info",
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf((info) => {
        const details = info.details || {};
        return `${info.timestamp} | MARKET LIMIT UPDATE | event=${readable(details.eventId)}` +
          ` | market=${readable(details.marketId)} | minbet=${readable(details.minbet)}` +
          ` | maxbet=${readable(details.maxbet)}`;
      }),
    ),
    transports: [transport],
    exitOnError: false,
  });
  instance.on("error", (error) => logger.error("[MarketLimitsLog] write failed", { error: error.message }));
  return instance;
}

function writeMarketLimitsLog(item) {
  if (String(process.env.MARKET_LIMITS_LOG_TO_FILE || "true").toLowerCase() !== "true") return;
  // Record received values before validation or persistence, including partial updates.
  try {
    getLogger().info("market.limits.received", {
      details: {
        eventId: item?.eid ?? null,
        marketId: item?.mid ?? null,
        minbet: item?.settings?.ms ?? null,
        maxbet: item?.settings?.mas ?? null,
      },
    });
  } catch (error) {
    logger.error("[MarketLimitsLog] write failed", { error: error.message });
  }
}

async function closeMarketLimitsLog() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  // Winston's finish can precede the rotating transport's underlying file flush.
  const flushed = current.transports.map(
    (transport) =>
      new Promise((resolve) => {
        transport.logStream.once("finish", resolve);
        transport.logStream.once("error", resolve);
      }),
  );
  await new Promise((resolve) => {
    current.once("finish", resolve);
    current.once("error", resolve);
    current.end();
  });
  current.close();
  await Promise.all(flushed);
}

module.exports = { writeMarketLimitsLog, closeMarketLimitsLog };

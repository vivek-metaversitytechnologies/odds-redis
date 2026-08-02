const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

let instance;

function enabled() {
  return String(process.env.PROVIDER_LOG_TO_FILE || "true").toLowerCase() === "true";
}

function getLogger() {
  if (instance) return instance;
  instance = winston.createLogger({
    level: "info",
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf((info) => JSON.stringify({
        timestamp: info.timestamp,
        type: info.type,
        ...info.details,
      }, null, 2)),
    ),
    transports: [new winston.transports.DailyRotateFile({
      dirname: path.resolve(process.env.PROVIDER_LOG_DIR || "logs/provider"),
      filename: "provider-http-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      maxSize: process.env.PROVIDER_LOG_MAX_SIZE || "25m",
      maxFiles: process.env.PROVIDER_LOG_MAX_FILES || "14d",
      zippedArchive: false,
    })],
    exitOnError: false,
  });
  return instance;
}

function writeProviderLog(type, details) {
  if (enabled()) getLogger().info("provider-http", { type, details });
}

async function closeProviderLog() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  current.close();
}

module.exports = { writeProviderLog, closeProviderLog };

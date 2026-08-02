const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

function metadata(info) {
  return Object.fromEntries(Object.entries(info).filter(([key]) =>
    !["level", "message", "timestamp", "splat"].includes(key)));
}

const consoleFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.colorize({ level: true }),
  winston.format.printf((info) => {
    const meta = metadata(info);
    return `${info.timestamp} [${info.level}] ${info.message}${Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : ""}`;
  }),
);

const prettyFileFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.printf((info) => JSON.stringify({
    timestamp: info.timestamp,
    level: info.level,
    message: info.message,
    ...metadata(info),
  }, null, 2)),
);

const transports = String(process.env.LOG_TO_FILE || "false").toLowerCase() === "true"
  ? [new winston.transports.DailyRotateFile({
      dirname: path.resolve(process.env.LOG_DIR || "logs"),
      filename: "application-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      maxSize: process.env.LOG_MAX_SIZE || "20m",
      maxFiles: process.env.LOG_MAX_FILES || "14d",
      format: prettyFileFormat,
    })]
  : [new winston.transports.Console({ format: consoleFormat })];

const instance = winston.createLogger({
  level: String(process.env.LOG_LEVEL || "info").toLowerCase(),
  transports,
  exitOnError: false,
});

module.exports = {
  debug: (message, meta) => instance.debug(message, meta),
  info: (message, meta) => instance.info(message, meta),
  warn: (message, meta) => instance.warn(message, meta),
  error: (message, meta) => instance.error(message, meta),
  close: async () => { instance.close(); },
};

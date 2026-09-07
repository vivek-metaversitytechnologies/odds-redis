const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

let instance;

function enabled() {
  return String(process.env.BALL_BY_BALL_LOG_TO_FILE || "true").toLowerCase() === "true";
}

function getLogger() {
  if (instance) return instance;
  instance = winston.createLogger({
    level: "info",
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf((info) =>
        JSON.stringify({ timestamp: info.timestamp, type: info.type, ...info.details }),
      ),
    ),
    transports: [
      new winston.transports.DailyRotateFile({
        dirname: path.resolve(process.env.BALL_BY_BALL_LOG_DIR || "logs/ball-by-ball"),
        filename: "ball-by-ball-%DATE%.log",
        datePattern: "YYYY-MM-DD",
        maxSize: process.env.BALL_BY_BALL_LOG_MAX_SIZE || "25m",
        maxFiles: process.env.BALL_BY_BALL_LOG_MAX_FILES || "7d",
        zippedArchive: false,
      }),
    ],
    exitOnError: false,
  });
  return instance;
}

function writeBallByBallLog(type, details = {}) {
  if (enabled()) getLogger().info("ball-by-ball", { type, details });
}

async function closeBallByBallLog() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  current.close();
}

module.exports = { writeBallByBallLog, closeBallByBallLog };

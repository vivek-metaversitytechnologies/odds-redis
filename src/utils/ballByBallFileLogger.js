const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

let instance;
const ballLineInstances = new Map();
let sequence = 0;

function loggerOptions(filename) {
  return {
    level: "info",
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf((info) =>
        JSON.stringify({ timestamp: info.timestamp, type: info.type, ...info.details }),
      ),
    ),
    transports: [
      new winston.transports.DailyRotateFile({
        dirname: path.resolve("logs/ball-by-ball"),
        filename,
        datePattern: "YYYY-MM-DD",
        maxSize: "25m",
        maxFiles: "7d",
        zippedArchive: false,
      }),
    ],
    exitOnError: false,
  };
}

function getLogger() {
  if (instance) return instance;
  instance = winston.createLogger(loggerOptions("ball-by-ball-%DATE%.log"));
  return instance;
}

function ballLineFileKey(ballLine) {
  const normalized = String(ballLine ?? "unknown").trim();
  return /^\d+(?:\.\d+)?$/.test(normalized) ? normalized : "unknown";
}

function getBallLineLogger(ballLine) {
  const key = ballLineFileKey(ballLine);
  if (!ballLineInstances.has(key)) {
    ballLineInstances.set(key, winston.createLogger(loggerOptions(`ball-line-${key}-%DATE%.log`)));
  }
  return ballLineInstances.get(key);
}

function writeBallByBallLog(type, details = {}) {
  sequence += 1;
  getLogger().info("ball-by-ball", { type, details: { sequence, ...details } });
}

// API discovery and Socket.IO ticks for one ball line share one append-only file.
// `sequence` preserves their process-local chronology even when vendor timestamps
// are missing or arrive out of order.
function writeBallByBallObservation(source, details = {}) {
  sequence += 1;
  getBallLineLogger(details.ballLine).info("ball-by-ball", {
    type: "market.observed",
    details: {
      sequence,
      ballLine: details.ballLine ?? null,
      source,
      observedAt: new Date().toISOString(),
      ...details,
    },
  });
}

async function closeBallByBallLog() {
  const current = instance;
  instance = undefined;
  current?.close();
  for (const logger of ballLineInstances.values()) logger.close();
  ballLineInstances.clear();
  sequence = 0;
}

module.exports = { writeBallByBallLog, writeBallByBallObservation, closeBallByBallLog };

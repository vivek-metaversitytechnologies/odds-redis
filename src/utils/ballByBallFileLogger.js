const path = require("node:path");
const winston = require("winston");
require("winston-daily-rotate-file");

let instance;
let sequence = 0;

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
        dirname: path.resolve("logs/ball-by-ball"),
        filename: "ball-by-ball-%DATE%.log",
        datePattern: "YYYY-MM-DD",
        maxSize: "25m",
        maxFiles: "7d",
        zippedArchive: false,
      }),
    ],
    exitOnError: false,
  });
  return instance;
}

function writeBallByBallLog(type, details = {}) {
  sequence += 1;
  getLogger().info("ball-by-ball", { type, details: { sequence, ...details } });
}

// API discovery and Socket.IO ticks share this one append-only stream. `sequence`
// is assigned at observation time, so records retain their process-local chronology
// even when the provider timestamps are missing or arrive out of order.
function writeBallByBallObservation(source, details = {}) {
  writeBallByBallLog("market.observed", {
    source,
    observedAt: new Date().toISOString(),
    ...details,
  });
}

async function closeBallByBallLog() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  sequence = 0;
  current.close();
}

module.exports = { writeBallByBallLog, writeBallByBallObservation, closeBallByBallLog };

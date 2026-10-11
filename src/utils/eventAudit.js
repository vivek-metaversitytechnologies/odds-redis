const path = require("node:path");
const winston = require("winston");
const logger = require("./logger");
require("winston-daily-rotate-file");

// Event lifecycle audit, written as JSON Lines independently of LOG_LEVEL so that every
// decision that closes, keeps or removes an event can be reconstructed afterwards. Volume is
// low: one line per terminal observation, retirement, carry-over decision or Redis removal.
//
// Actions:
//   terminal.observed   lifecycle evidence that an event finished (counts toward confirmation)
//   terminal.override   eventSync closes an event the vendor still reports as not game-over
//   retired             socket game-over on a primary market retired the event in the database
//   redis.removed       event payload, score or active-match entry actually deleted from Redis
//   carry.kept          event missing from the vendor's today feed kept alive (unfinished primary)
//   carry.dropped       such an event not kept: no unfinished primary market at the vendor

let instance;

function enabled() {
  return String(process.env.EVENT_AUDIT_ENABLED || "true").toLowerCase() === "true";
}

function getLogger() {
  if (instance) return instance;
  const transport = new winston.transports.DailyRotateFile({
    dirname: path.resolve(process.env.EVENT_AUDIT_LOG_DIR || "logs/event-lifecycle"),
    filename: "event-lifecycle-%DATE%.jsonl",
    datePattern: "YYYY-MM-DD",
    maxSize: process.env.EVENT_AUDIT_LOG_MAX_SIZE || "50m",
    maxFiles: process.env.EVENT_AUDIT_LOG_MAX_FILES || "30d",
    zippedArchive: false,
  });
  instance = winston.createLogger({
    level: "info",
    format: winston.format.printf((info) => JSON.stringify(info.record)),
    transports: [transport],
    exitOnError: false,
  });
  instance.on("error", (error) => logger.error("[EventAudit] write failed", { error: error.message }));
  return instance;
}

function audit(action, eventId, fields = {}) {
  if (!enabled()) return;
  try {
    getLogger().info("event", {
      record: { ts: new Date().toISOString(), action, eventId: eventId == null ? null : String(eventId), ...fields },
    });
  } catch (error) {
    logger.error("[EventAudit] write failed", { error: error.message });
  }
}

module.exports = { audit };

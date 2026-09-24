const path = require("node:path");
const winston = require("winston");
const logger = require("./logger");
require("winston-daily-rotate-file");

let instance;

function readable(value) {
  if (value === null || value === undefined || value === "") return "not-provided";
  return String(value).replaceAll(/\s+/g, " ").trim();
}

function runnerSummary(runners) {
  if (!Array.isArray(runners) || !runners.length) return "  - none";
  return runners.map((runner) =>
    `  - ${readable(runner?.na ?? runner?.name)} (ID ${readable(runner?.rid ?? runner?.id)})` +
      ` | ${readable(runner?.s ?? runner?.status)}` +
      ` | Back ${readable(runner?.b1)} @ ${readable(runner?.bs1)}` +
      ` | Lay ${readable(runner?.l1)} @ ${readable(runner?.ls1)}`,
  ).join("\n");
}

function getLogger() {
  if (instance) return instance;
  const transport = new winston.transports.DailyRotateFile({
    dirname: path.resolve(process.env.BALL_BY_BALL_LOG_DIR || "logs/ball-by-ball"),
    filename: "ball-by-ball-%DATE%.log",
    datePattern: "YYYY-MM-DD",
    maxSize: process.env.BALL_BY_BALL_LOG_MAX_SIZE || "25m",
    maxFiles: process.env.BALL_BY_BALL_LOG_MAX_FILES || "14d",
    zippedArchive: false,
  });
  instance = winston.createLogger({
    level: "info",
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf((info) => {
        const details = info.details || {};
        const common = [
          "================================================================================",
          `BALL BY BALL ${info.message} — ${info.timestamp}`,
          `Event       : ${readable(details.eventId)}`,
          `Market      : ${readable(details.marketId)}`,
          `Name        : ${readable(details.name)}`,
          `Status      : ${readable(details.status)}`,
        ];
        if (info.message === "DISCOVERY") {
          return [...common,
            `Ball line   : ${readable(details.ballLine)}`,
            `Active      : ${readable(details.active)}`,
            `Game over   : ${readable(details.gameOver)}`,
            `Provider at : ${readable(details.providerUpdatedAt)}`,
            "",
          ].join("\n");
        }
        return [...common,
          `Game over   : ${readable(details.gameOver)}`,
          `Result      : ${readable(details.result)}`,
          `Provider at : ${readable(details.providerTimestamp)}`,
          "Runners:",
          runnerSummary(details.runners),
          "",
        ].join("\n");
      }),
    ),
    transports: [transport],
    exitOnError: false,
  });
  instance.on("error", (error) => logger.error("[BallByBallLog] write failed", { error: error.message }));
  return instance;
}

function enabled() {
  return String(process.env.BALL_BY_BALL_LOG_TO_FILE || "true").toLowerCase() === "true";
}

function writeBallByBallDiscoveryLog(item) {
  if (!enabled()) return;
  try {
    getLogger().info("DISCOVERY", { details: {
      eventId: item?.eventId ?? item?.eid ?? null,
      marketId: item?.id ?? item?.mid ?? null,
      name: item?.name ?? item?.na ?? null,
      status: item?.status ?? item?.sb ?? null,
      ballLine: item?.ballLine ?? null,
      active: item?.isActive ?? null,
      gameOver: item?.gameOver ?? null,
      providerUpdatedAt: item?.updatedAt ?? item?.t ?? null,
    } });
  } catch (error) {
    logger.error("[BallByBallLog] write failed", { error: error.message });
  }
}

function writeBallByBallSocketLog(item) {
  if (!enabled() || !/-BB$/i.test(String(item?.mid || ""))) return;
  try {
    getLogger().info("SOCKET", { details: {
      eventId: item?.eid ?? null,
      marketId: item?.mid ?? null,
      name: item?.na ?? null,
      status: item?.s ?? item?.sb ?? null,
      gameOver: item?.go ?? null,
      result: item?.res ?? null,
      providerTimestamp: item?.t ?? null,
      runners: item?.r,
    } });
  } catch (error) {
    logger.error("[BallByBallLog] write failed", { error: error.message });
  }
}

async function closeBallByBallLog() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  const flushed = current.transports.map((transport) => new Promise((resolve) => {
    transport.logStream.once("finish", resolve);
    transport.logStream.once("error", resolve);
  }));
  await new Promise((resolve) => {
    current.once("finish", resolve);
    current.once("error", resolve);
    current.end();
  });
  current.close();
  await Promise.all(flushed);
}

module.exports = { writeBallByBallDiscoveryLog, writeBallByBallSocketLog, closeBallByBallLog };

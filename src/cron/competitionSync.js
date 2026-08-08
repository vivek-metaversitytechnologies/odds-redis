const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");

let running = false;
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function supportedSportIds() {
  return new Set(
    String(process.env.SPORT_IDS || "1,2,4")
      .split(",")
      .map((value) => Number(value.trim()))
      .filter(Number.isFinite),
  );
}

function competitionRows(response) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  const sports = supportedSportIds();
  return rows
    .map((item) => ({
      seriesId: Number(item?.id),
      seriesName: String(item?.name || "").trim(),
      sportId: Number(item?.sportId),
    }))
    .filter((item) => Number.isInteger(item.seriesId) && item.seriesName && sports.has(item.sportId));
}

async function upsertCompetitions(competitions) {
  if (!competitions.length) return { inserted: 0, updated: 0 };
  const connection = await getSourcePool().getConnection();
  let inserted = 0;
  let updated = 0;
  try {
    await connection.beginTransaction();
    const ids = competitions.map((competition) => competition.seriesId);
    const [existingRows] = await connection.query(
      `SELECT seriesid FROM t_series WHERE seriesid IN (${ids.map(() => "?").join(",")})`,
      ids,
    );
    const existing = new Set(existingRows.map((row) => Number(row.seriesid)));
    for (const competition of competitions) {
      if (existing.has(competition.seriesId)) {
        await connection.execute(
          `UPDATE t_series SET seriesname = ?, sportid = ?, isactive = ?, status = ?, updatedon = NOW()
           WHERE seriesid = ?`,
          [competition.seriesName, competition.sportId, true, true, competition.seriesId],
        );
        updated += 1;
        continue;
      }
      await connection.execute(
        `INSERT INTO t_series
          (adminid, appid, createdon, isactive, marketcount, seriesid, seriesname, sportid, status, updatedon)
         VALUES (?, ?, NOW(), ?, ?, ?, ?, ?, ?, NOW())`,
        ["1", 1, true, "0", competition.seriesId, competition.seriesName, competition.sportId, true],
      );
      inserted += 1;
    }
    await connection.commit();
    return { inserted, updated };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function syncCompetitions() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const response = await provider.competitions();
    const competitions = competitionRows(response);
    const persisted = await upsertCompetitions(competitions);
    const received = Array.isArray(response?.data)
      ? response.data.length
      : Array.isArray(response)
        ? response.length
        : 0;
    const result = {
      skipped: false,
      received,
      supported: competitions.length,
      sportIds: [...supportedSportIds()],
      ...persisted,
    };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger.info("[CompetitionSync] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[CompetitionSync] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startCompetitionSync() {
  const { expression } = cronConfig.competition;
  const task = cron.schedule(expression, () => void syncCompetitions().catch(() => {}));
  logger.info("[CompetitionSync] scheduled", { expression, sportIds: [...supportedSportIds()] });
  return task;
}

function getCompetitionSyncStatus() {
  return { ...state, sportIds: [...supportedSportIds()] };
}

module.exports = {
  supportedSportIds,
  competitionRows,
  upsertCompetitions,
  syncCompetitions,
  startCompetitionSync,
  getCompetitionSyncStatus,
};

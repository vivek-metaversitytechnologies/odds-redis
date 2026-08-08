const { getSourcePool } = require("../config/sourceDb");
const Event = require("../models/Event");

function validId(id) {
  return /^\d+$/.test(id) && Number(id) > 0;
}

async function list(req, res, next) {
  try {
    const [rows] = await getSourcePool().query("SELECT * FROM t_event ORDER BY id DESC");
    res.json({ status: "ok", data: rows.map(Event.fromRow) });
  } catch (error) {
    next(error);
  }
}

async function get(req, res, next) {
  try {
    if (!validId(req.params.id))
      return res.status(400).json({ status: "error", message: "Invalid event id" });
    const [rows] = await getSourcePool().query("SELECT * FROM t_event WHERE id = ?", [req.params.id]);
    if (!rows.length) return res.status(404).json({ status: "error", message: "Event not found" });
    res.json({ status: "ok", data: Event.fromRow(rows[0]) });
  } catch (error) {
    next(error);
  }
}

module.exports = { list, get };

const logger = require("../utils/logger");

function notFound(req, res) {
  res.status(404).json({ status: "error", message: "Route not found" });
}

function errorHandler(error, req, res, next) {
  if (res.headersSent) return next(error);
  logger.error("Request failed", { method: req.method, path: req.originalUrl, error: error.message });
  res
    .status(error.statusCode || 500)
    .json({ status: "error", message: error.statusCode ? error.message : "Internal Server Error" });
}

module.exports = { notFound, errorHandler };

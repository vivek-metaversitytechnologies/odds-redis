const crypto = require("node:crypto");

const COOKIE_NAME = "odds_admin_session";

function password() {
  return String(process.env.ADMIN_PANEL_PASSWORD || "");
}
function sessionSeconds() {
  const value = Number(process.env.ADMIN_PANEL_SESSION_SECONDS || 43200);
  return Number.isFinite(value) ? Math.max(300, value) : 43200;
}

function cookies(req) {
  return Object.fromEntries(
    String(req.headers.cookie || "")
      .split(";")
      .map((part) => {
        const index = part.indexOf("=");
        return index < 0
          ? [part.trim(), ""]
          : [part.slice(0, index).trim(), decodeURIComponent(part.slice(index + 1))];
      })
      .filter(([key]) => key),
  );
}

function signature(value) {
  return crypto.createHmac("sha256", password()).update(value).digest("base64url");
}

function createSession() {
  const expires = String(Math.floor(Date.now() / 1000) + sessionSeconds());
  return `${expires}.${signature(expires)}`;
}

function validSession(req) {
  const configured = password();
  if (!configured) return false;
  const token = cookies(req)[COOKIE_NAME];
  const [expires, supplied] = String(token || "").split(".");
  if (!expires || !supplied || Number(expires) <= Math.floor(Date.now() / 1000)) return false;
  const expected = signature(expires);
  const left = Buffer.from(supplied);
  const right = Buffer.from(expected);
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function secureRequest(req) {
  return (
    req.secure ||
    String(req.get("x-forwarded-proto") || "")
      .split(",")[0]
      .trim() === "https"
  );
}

function setSessionCookie(req, res) {
  const attributes = [
    `${COOKIE_NAME}=${encodeURIComponent(createSession())}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Strict",
    `Max-Age=${sessionSeconds()}`,
  ];
  if (secureRequest(req)) attributes.push("Secure");
  res.setHeader("Set-Cookie", attributes.join("; "));
}

function clearSessionCookie(req, res) {
  const attributes = [`${COOKIE_NAME}=`, "Path=/", "HttpOnly", "SameSite=Strict", "Max-Age=0"];
  if (secureRequest(req)) attributes.push("Secure");
  res.setHeader("Set-Cookie", attributes.join("; "));
}

function matchesPassword(candidate) {
  const configured = Buffer.from(password());
  const supplied = Buffer.from(String(candidate || ""));
  return (
    configured.length > 0 &&
    configured.length === supplied.length &&
    crypto.timingSafeEqual(configured, supplied)
  );
}

function hasInternalKey(req) {
  const required = process.env.INTERNAL_API_KEY;
  if (!required) return false;
  const supplied = req.get("x-internal-api-key");
  const left = Buffer.from(String(required));
  const right = Buffer.from(String(supplied || ""));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function requireAdminPage(req, res, next) {
  if (!password()) return res.status(503).send("ADMIN_PANEL_PASSWORD is not configured");
  if (validSession(req)) return next();
  const nextPath = encodeURIComponent(req.originalUrl || "/admin/");
  return res.redirect(302, `/admin/login/?next=${nextPath}`);
}

function requireAdminApi(req, res, next) {
  if (validSession(req) || hasInternalKey(req)) return next();
  return res.status(401).json({ status: "error", message: "Admin authentication required" });
}

module.exports = {
  COOKIE_NAME,
  createSession,
  validSession,
  matchesPassword,
  hasInternalKey,
  setSessionCookie,
  clearSessionCookie,
  requireAdminPage,
  requireAdminApi,
};

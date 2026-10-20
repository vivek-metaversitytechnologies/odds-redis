const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const mirrorDb = require("../src/config/mirrorDb");

// Source fancy rows by fancyid: { isactive, status }.
const sourceFancies = new Map();
db.getSourcePool = () => ({
  query: async (sql, params) => {
    const ids = params.slice(0, -1);
    return [ids.filter((id) => sourceFancies.get(id)?.isactive === false).map((fancyid) => ({ fancyid, status: sourceFancies.get(fancyid).status }))];
  },
});
const { reconcileMirrorFancies } = require("../src/cron/mirrorFancyReconcile");

function mockMirror(t, activeOnMirror) {
  const queued = [];
  t.mock.method(mirrorDb, "mirrorEnabled", () => true);
  t.mock.method(mirrorDb, "getMirrorPool", () => ({
    query: async () => [activeOnMirror.map((fancyid) => ({ fancyid }))],
  }));
  t.mock.method(mirrorDb, "queueForMirror", (sql, values) => queued.push({ sql, values }));
  return queued;
}

test("fancies deactivated on the source are deactivated on the mirror through the outbox", async (t) => {
  sourceFancies.clear();
  sourceFancies.set("4.1-F2", { isactive: false, status: "CLOSE" });
  sourceFancies.set("4.2-F2", { isactive: false, status: "SUSPENDED" });
  sourceFancies.set("4.3-F2", { isactive: true, status: "OPEN" });
  const queued = mockMirror(t, ["4.1-F2", "4.2-F2", "4.3-F2", "4.4-F2"]);

  const result = await reconcileMirrorFancies();

  assert.equal(result.queued, 2);
  assert.deepEqual(result.byStatus, { CLOSE: 1, unchanged: 1 });
  // A terminal status is copied along; a socket-managed one is left to the socket.
  assert.deepEqual(queued, [
    {
      sql: "UPDATE t_matchfancy SET isactive=?,status=? WHERE fancyid IN (?) AND isactive=?",
      values: [false, "CLOSE", ["4.1-F2"], true],
    },
    {
      sql: "UPDATE t_matchfancy SET isactive=? WHERE fancyid IN (?) AND isactive=?",
      values: [false, ["4.2-F2"], true],
    },
  ]);
  for (const { sql } of queued) assert.doesNotMatch(sql, /\bid\s*(=|IN)/);
});

test("nothing is queued when the mirror agrees with the source", async (t) => {
  sourceFancies.clear();
  sourceFancies.set("4.3-F2", { isactive: true, status: "OPEN" });
  const queued = mockMirror(t, ["4.3-F2"]);
  const result = await reconcileMirrorFancies();
  assert.equal(result.queued, 0);
  assert.equal(queued.length, 0);
});

test("the run is skipped while mirroring is disabled", async (t) => {
  t.mock.method(mirrorDb, "mirrorEnabled", () => false);
  assert.deepEqual(await reconcileMirrorFancies(), { skipped: true, reason: "mirror-disabled" });
});

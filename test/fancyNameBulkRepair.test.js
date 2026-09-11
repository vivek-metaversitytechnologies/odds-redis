const test = require("node:test");
const assert = require("node:assert/strict");
const provider = require("../src/services/providerApi");
const { repairOne, repairAll } = require("../scripts/repair-fancy-name");

function connection() {
  const calls = [];
  return {
    calls,
    beginTransaction: async () => calls.push("begin"),
    execute: async (_sql, params) => calls.push(params),
    commit: async () => calls.push("commit"),
    rollback: async () => calls.push("rollback"),
    release: () => calls.push("release"),
  };
}

test("bulk name repair reuses a correct result name without calling the provider", async (t) => {
  t.mock.method(provider, "runners", () => { throw new Error("unexpected provider call"); });
  const c = connection();
  const pool = { query: async () => [[{ name: "Fancy2" }, { name: "Only 15 Over Run SL-W" }]], getConnection: async () => c };
  const result = await repairOne(pool, "4.1-F2");
  assert.equal(result.name, "Only 15 Over Run SL-W");
  assert.equal(provider.runners.mock.callCount(), 0);
  assert.deepEqual(c.calls, ["begin", [result.name, "4.1-F2"], [result.name, "4.1-F2"], "commit", "release"]);
});

test("bulk repair advances past unresolved records and continues after transaction failures", async (t) => {
  t.mock.method(provider, "runners", async () => ({ data: [] }));
  const cursors = [];
  const c = connection();
  const pool = {
    query: async (sql, params) => {
      if (sql.includes("ORDER BY")) {
        cursors.push(params[0]);
        if (params[0] === "") return [[{ fancyid: "4.1-F2" }, { fancyid: "4.2-F2" }]];
        if (params[0] === "4.2-F2") return [[{ fancyid: "4.3-F2" }]];
        return [[]];
      }
      return [[{ name: params[0] === "4.1-F2" ? "Fancy2" : "Real name" }]];
    },
    getConnection: async () => {
      if (cursors.length === 1) return { ...c, execute: async () => { throw new Error("write failed"); } };
      return c;
    },
  };
  const summary = await repairAll(pool, () => {}, async () => {});
  assert.deepEqual(summary, { checked: 3, skipped: 1, failed: 1, repaired: 1 });
  assert.deepEqual(cursors, ["", "4.2-F2", "4.3-F2"]);
  assert.ok(c.calls.includes("rollback"));
});

test("bulk scan covers remaining fancy families in both tables", async () => {
  let query;
  await repairAll({ query: async (sql) => { query = sql; return [[]]; } }, () => {}, async () => {});
  for (const suffix of ["F2", "F3", "OE", "KD", "MT", "CC"]) {
    assert.equal(query.split(`LIKE '%-${suffix}'`).length - 1, 2);
  }
  for (const name of ["fancy2", "othermarket", "oddeven", "khado", "meter", "cricketcasino"]) {
    assert.equal(query.split(`'${name}'`).length - 1, 2);
  }
  assert.ok(!query.includes("%-BB"), "ball numbers retain their dedicated metadata handling");
});

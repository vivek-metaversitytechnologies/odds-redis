const test = require("node:test");
const assert = require("node:assert/strict");
const provider = require("../src/services/providerApi");

const fancyId = (index) => `11.${String(900925862394 + index)}-CC`;
const regularId = (index) => `1.${262893095 + index}`;
const charsOf = (batch) => batch.reduce((total, id) => total + id.length + 3, 0);

test("result batches never exceed the id count or id-length limits", () => {
  const regular = provider.resultIdBatches(Array.from({ length: 1200 }, (_, index) => regularId(index)));
  assert.ok(regular.every((batch) => batch.length <= provider.RESULTS_MAX_IDS_PER_REQUEST));
  assert.deepEqual(regular.map((batch) => batch.length), [500, 500, 200]);

  // Long fancy ids hit the character budget before the count cap (the case that failed at ~999).
  const fancies = provider.resultIdBatches(Array.from({ length: 1000 }, (_, index) => fancyId(index)));
  assert.ok(fancies.every((batch) => charsOf(batch) <= provider.RESULTS_MAX_ID_CHARS_PER_REQUEST));
  assert.ok(fancies[0].length < provider.RESULTS_MAX_IDS_PER_REQUEST);
  assert.equal(fancies.flat().length, 1000);
  assert.deepEqual(provider.resultIdBatches([]), []);
});

test("provider.results splits an oversized id list and merges the rows", async (t) => {
  const bodies = [];
  t.mock.method(global, "fetch", async (_url, init) => {
    const { mids } = JSON.parse(init.body);
    bodies.push(mids.length);
    return new Response(JSON.stringify({ data: mids.map((marketId) => ({ marketId, result: "1" })) }), { status: 200 });
  });
  const ids = Array.from({ length: 1100 }, (_, index) => fancyId(index));

  const response = await provider.results({ mids: ids });

  assert.ok(bodies.length >= 3);
  assert.ok(bodies.every((count) => count <= provider.RESULTS_MAX_IDS_PER_REQUEST));
  assert.equal(response.data.length, 1100);
  assert.deepEqual(new Set(response.data.map((row) => row.marketId)).size, 1100);
});

test("a small id list is one untouched request", async (t) => {
  const bodies = [];
  t.mock.method(global, "fetch", async (_url, init) => {
    bodies.push(JSON.parse(init.body));
    return new Response(JSON.stringify({ data: [] }), { status: 200 });
  });
  await provider.results({ mids: ["1.1", "1.2"] });
  assert.deepEqual(bodies, [{ mids: ["1.1", "1.2"] }]);
});

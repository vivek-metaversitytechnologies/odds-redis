const test = require("node:test");
const assert = require("node:assert/strict");
const { retryDelay } = require("../src/services/pendingResultQueue");
const queue = require("../src/services/pendingResultQueue");
const redis = require("../src/config/redis");

test("pending results back off without expiring unresolved settlements", () => {
  assert.equal(retryDelay(1), 60000);
  assert.equal(retryDelay(2), 300000);
  assert.equal(retryDelay(3), 1800000);
  assert.equal(retryDelay(100), 1800000);
});

test("repeated deactivation preserves retry schedule and settlement clears queue state", async () => {
  const scores = new Map();
  const attempts = new Map();
  const firstQueued = new Map();
  const review = new Map();
  const fake = {
    isOpen: true,
    async eval(_script, { keys, arguments: args }) {
      const now = Number(args[0]);
      if (keys.length === 3) {
        for (const id of args.slice(1)) {
          if (review.has(id)) continue;
          if (!firstQueued.has(id)) firstQueued.set(id, String(now));
          if (!scores.has(id)) scores.set(id, now);
        }
        return 1;
      }
      const moved = [];
      for (const id of args.slice(2)) {
        if (!firstQueued.has(id) || now - Number(firstQueued.get(id)) < Number(args[1])) continue;
        review.set(id, { marketId: id, attempts: attempts.get(id) || 0 });
        scores.delete(id); firstQueued.delete(id); attempts.delete(id); moved.push(id);
      }
      return moved;
    },
    async hmGet(_key, ids) { return ids.map((id) => firstQueued.get(id)); },
    async zAdd(_key, entries, options) {
      for (const { value, score } of entries) {
        if (options.NX && scores.has(value)) continue;
        if (options.XX && !scores.has(value)) continue;
        scores.set(value, score);
      }
    },
    multi() {
      const ops = [];
      const tx = {
        hSetNX(_key, id, value) {
          ops.push(() => { if (!firstQueued.has(id)) firstQueued.set(id, value); });
          return tx;
        },
        zAdd(key, entries, options) { ops.push(() => fake.zAdd(key, entries, options)); return tx; },
        hIncrBy(_key, id) {
          ops.push(() => { attempts.set(id, (attempts.get(id) || 0) + 1); return attempts.get(id); });
          return tx;
        },
        zRem(_key, ids) { ops.push(() => ids.forEach((id) => scores.delete(id))); return tx; },
        hDel(key, ids) { ops.push(() => ids.forEach((id) => (key.endsWith(":firstQueuedAt") ? firstQueued : attempts).delete(id))); return tx; },
        async exec() { return ops.map((op) => op()); },
      };
      return tx;
    },
  };
  redis.__testing__.setRedisClient(fake);
  try {
    await queue.enqueue(["1.262087180", "1.262087180"]);
    assert.equal(scores.size, 1);
    await queue.defer(["1.262087180"]);
    const due = scores.get("1.262087180");
    assert.ok(due >= Date.now() + 59000);
    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.get("1.262087180"), due);
    firstQueued.set("1.262087180", String(Date.now() - 86400000));
    await queue.enqueue(["1.262087180"]);
    await queue.defer(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
    assert.equal(review.has("1.262087180"), true);
    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
    await queue.remove(["1.262087180"]);
    assert.equal(scores.size, 0);
    assert.equal(attempts.size, 0);
    assert.equal(firstQueued.size, 0);
  } finally { redis.__testing__.reset(); }
});

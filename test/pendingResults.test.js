const test = require("node:test");
const assert = require("node:assert/strict");
const { retryDelay } = require("../src/services/pendingResultQueue");
const queue = require("../src/services/pendingResultQueue");
const redis = require("../src/config/redis");

const KEY = "Pending-Regular-Results";
const ATTEMPTS_KEY = `${KEY}:attempts`;
const FIRST_QUEUED_KEY = `${KEY}:firstQueuedAt`;
const REVIEW_KEY = `${KEY}:review`;

test("retries stay on a flat ten-second cadence", () => {
  assert.equal(retryDelay(1), 10000);
  assert.equal(retryDelay(2), 10000);
  assert.equal(retryDelay(10), 10000);
});

function makeFakeRedis() {
  const scores = new Map();
  const attempts = new Map();
  const firstQueued = new Map();
  const review = new Map();
  const fake = {
    isOpen: true,
    async eval(_script, { keys, arguments: args }) {
      if (keys[1] === FIRST_QUEUED_KEY && keys[2] === REVIEW_KEY) {
        // enqueue
        const now = args[0];
        for (const id of args.slice(1)) {
          if (review.has(id)) continue;
          if (!firstQueued.has(id)) firstQueued.set(id, now);
          if (!scores.has(id)) scores.set(id, Number(now));
        }
        return 1;
      }
      // moveExpired: keys = [key, attempts, firstQueuedAt]
      const maxAttempts = Number(args[0]);
      const moved = [];
      for (const id of args.slice(1)) {
        if (Number(attempts.get(id) || 0) >= maxAttempts) {
          scores.delete(id); attempts.delete(id); firstQueued.delete(id);
          moved.push(id);
        }
      }
      return moved;
    },
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
        zAdd(k, entries, options) { ops.push(() => fake.zAdd(k, entries, options)); return tx; },
        hIncrBy(_key, id) {
          ops.push(() => { attempts.set(id, (attempts.get(id) || 0) + 1); return attempts.get(id); });
          return tx;
        },
        zRem(_key, ids) { ops.push(() => ids.forEach((id) => scores.delete(id))); return tx; },
        hDel(k, ids) { ops.push(() => ids.forEach((id) => (k === FIRST_QUEUED_KEY ? firstQueued : attempts).delete(id))); return tx; },
        async exec() { return ops.map((op) => op()); },
      };
      return tx;
    },
  };
  return { fake, scores, attempts, firstQueued, review };
}

test("a market retried every ten seconds is dropped after an hour of failed attempts", async () => {
  const { fake, scores, attempts } = makeFakeRedis();
  redis.__testing__.setRedisClient(fake);
  try {
    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.size, 1);

    for (let attempt = 1; attempt <= 359; attempt += 1) {
      await queue.defer(["1.262087180"]);
      assert.equal(attempts.get("1.262087180"), attempt);
      assert.ok(scores.has("1.262087180"), `still queued after attempt ${attempt}`);
      assert.equal(scores.get("1.262087180") - Date.now() >= retryDelay(attempt) - 1000, true);
    }

    // 360th failed attempt (1 hour at 10s intervals) exhausts the retry budget and drops the market entirely.
    await queue.defer(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
    assert.equal(attempts.has("1.262087180"), false);
  } finally { redis.__testing__.reset(); }
});

test("dropped markets are not tracked for manual review", async () => {
  const { fake, scores, attempts, review } = makeFakeRedis();
  redis.__testing__.setRedisClient(fake);
  try {
    await queue.enqueue(["1.262087180"]);
    for (let attempt = 1; attempt <= 360; attempt += 1) await queue.defer(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
    assert.equal(attempts.has("1.262087180"), false);
    assert.equal(review.size, 0);

    // A dropped market can be re-enqueued fresh (e.g. rediscovered by the backlog scan).
    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), true);
  } finally { redis.__testing__.reset(); }
});

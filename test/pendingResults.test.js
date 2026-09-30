const test = require("node:test");
const assert = require("node:assert/strict");
const { retryDelay } = require("../src/services/pendingResultQueue");
const queue = require("../src/services/pendingResultQueue");
const redis = require("../src/config/redis");

const KEY = "Pending-Regular-Results";
const FIRST_QUEUED_KEY = `${KEY}:firstQueuedAt`;
const REVIEW_KEY = `${KEY}:review`;

test("retries stay on a flat one-minute cadence", () => {
  assert.equal(retryDelay(1), 60000);
  assert.equal(retryDelay(2), 60000);
  assert.equal(retryDelay(10), 60000);
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
      // moveExpired: keys = [key, attempts, firstQueuedAt, review]; args = [max, now, ...ids]
      const maxAttempts = Number(args[0]);
      const moved = [];
      for (const id of args.slice(2)) {
        if (Number(attempts.get(id) || 0) >= maxAttempts) {
          scores.delete(id); attempts.delete(id); firstQueued.delete(id);
          review.set(id, JSON.stringify({ marketId: id, reason: "no-vendor-result" }));
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
    async hmGet(_key, ids) {
      return ids.map((id) => firstQueued.get(id) ?? null);
    },
    multi() {
      const ops = [];
      const tx = {
        hSetNX(_key, id, value) {
          ops.push(() => { if (!firstQueued.has(id)) firstQueued.set(id, value); });
          return tx;
        },
        zAdd(k, entries, options) { ops.push(() => fake.zAdd(k, entries, options)); return tx; },
        hIncrBy(_key, id, by) {
          ops.push(() => { attempts.set(id, (attempts.get(id) || 0) + by); return attempts.get(id); });
          return tx;
        },
        zRem(_key, ids) { ops.push(() => ids.forEach((id) => scores.delete(id))); return tx; },
        hDel(k, ids) { ops.push(() => ids.forEach((id) => (k === FIRST_QUEUED_KEY ? firstQueued : attempts).delete(id))); return tx; },
        hSet(k, id, value) { ops.push(() => { if (k === REVIEW_KEY) review.set(id, value); }); return tx; },
        async exec() { return ops.map((op) => op()); },
      };
      return tx;
    },
  };
  return { fake, scores, attempts, firstQueued, review };
}

test("a market retried every minute moves to review after 12 hours without a result", async () => {
  const { fake, scores, attempts, review } = makeFakeRedis();
  redis.__testing__.setRedisClient(fake);
  try {
    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.size, 1);

    for (let attempt = 1; attempt < queue.MAX_ATTEMPTS; attempt += 1) {
      await queue.defer(["1.262087180"]);
      assert.ok(scores.has("1.262087180"), `still queued after attempt ${attempt}`);
    }
    assert.equal(attempts.get("1.262087180"), queue.MAX_ATTEMPTS - 1);
    assert.ok(scores.get("1.262087180") - Date.now() >= retryDelay() - 1000);

    // The 720th failed attempt (12 hours at one-minute intervals) moves it to review for good.
    await queue.defer(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
    assert.equal(attempts.has("1.262087180"), false);
    assert.equal(JSON.parse(review.get("1.262087180")).reason, "no-vendor-result");
  } finally { redis.__testing__.reset(); }
});

test("a market in review is never queued again by the recovery scan", async () => {
  const { fake, scores, review } = makeFakeRedis();
  redis.__testing__.setRedisClient(fake);
  try {
    await queue.enqueue(["1.262087180"]);
    for (let attempt = 1; attempt <= queue.MAX_ATTEMPTS; attempt += 1) await queue.defer(["1.262087180"]);
    assert.equal(review.has("1.262087180"), true);

    await queue.enqueue(["1.262087180"]);
    assert.equal(scores.has("1.262087180"), false);
  } finally { redis.__testing__.reset(); }
});

test("polls before the event start do not spend the 12-hour budget", async () => {
  const { eventStartById } = queue.__testing__;
  const { fake, scores, attempts, review } = makeFakeRedis();
  redis.__testing__.setRedisClient(fake);
  try {
    // Bookmaker switched off by the vendor a day before the start (event 36111738).
    await queue.enqueue(["4.697831668661-BM"]);
    eventStartById.set("4.697831668661-BM", Date.now() + 24 * 60 * 60 * 1000);
    for (let attempt = 1; attempt <= queue.MAX_ATTEMPTS + 10; attempt += 1) await queue.defer(["4.697831668661-BM"]);
    assert.equal(attempts.get("4.697831668661-BM"), 0);
    assert.ok(scores.has("4.697831668661-BM"), "still polled every minute before the start");
    assert.equal(review.has("4.697831668661-BM"), false);

    // Once the event has started, every failed poll counts again.
    eventStartById.set("4.697831668661-BM", Date.now() - 1000);
    await queue.defer(["4.697831668661-BM"]);
    assert.equal(attempts.get("4.697831668661-BM"), 1);
  } finally {
    eventStartById.clear();
    redis.__testing__.reset();
  }
});

test("event start times are read as IST", () => {
  const { eventStartMs } = queue.__testing__;
  assert.equal(eventStartMs("2026-09-29 09:30:00.000000"), Date.parse("2026-09-29T04:00:00Z"));
  assert.equal(eventStartMs("2026-09-29 09:30:00"), Date.parse("2026-09-29T04:00:00Z"));
  assert.ok(Number.isNaN(eventStartMs(null)));
  assert.ok(Number.isNaN(eventStartMs("0000-00-00 garbage")));
});

test("the due-entry load reads each event start", () => {
  const source = require("node:fs").readFileSync(require("node:path").join(__dirname, "../src/services/pendingResultQueue.js"), "utf8");
  assert.match(source, /m\.sportid,e\.open_date AS opendate\s+FROM t_market m LEFT JOIN t_event e/);
});

test("only recently active or in-play regular markets are queued, and never line markets", () => {
  const source = require("node:fs").readFileSync(require("node:path").join(__dirname, "../src/services/pendingResultQueue.js"), "utf8");
  assert.equal(queue.ELIGIBLE_HOURS, 48);
  assert.match(source, /m\.updatedon >= DATE_SUB\(NOW\(\), INTERVAL \$\{ELIGIBLE_HOURS\} HOUR\)\s+OR \(COALESCE\(e\.in_play,0\)=1 AND e\.isactive=1\)/);
  assert.match(source, /NOT EXISTS \(SELECT 1 FROM t_matchfancy lf WHERE lf\.fancyid=m\.marketid\)/);
  // Both the recovery scan and the due-entry load apply the eligibility rule.
  assert.equal((source.match(/AND \$\{eligibleSql\}/g) || []).length, 2);
});

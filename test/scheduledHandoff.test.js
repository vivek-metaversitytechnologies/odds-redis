const test = require("node:test");
const assert = require("node:assert/strict");
const { scheduledHandoff } = require("../src/utils/scheduledHandoff");

test("missed cron ticks coalesce and run immediately when background releases", () => {
  let busy = true;
  let runs = 0;
  const worker = scheduledHandoff({ isBusy: () => busy, run: () => { busy = true; runs++; }, onError: assert.fail });
  worker.request();
  worker.request();
  assert.equal(worker.pending, true);
  assert.equal(runs, 0);
  busy = false;
  worker.drain();
  assert.equal(runs, 1);
  assert.equal(busy, true);
  assert.equal(worker.pending, false);
  worker.drain();
  assert.equal(runs, 1);
});

test("shutdown discards pending work and restart accepts ticks", () => {
  let busy = true;
  let runs = 0;
  const worker = scheduledHandoff({ isBusy: () => busy, run: () => { runs++; }, onError: assert.fail });
  worker.request();
  worker.stop();
  busy = false;
  worker.drain();
  worker.request();
  assert.equal(runs, 0);
  worker.start();
  worker.request();
  assert.equal(runs, 1);
});

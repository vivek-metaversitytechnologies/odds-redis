// Coalesce missed ticks into one priority run after the current owner releases.
function scheduledHandoff({ isBusy, run, onError }) {
  let pending = false;
  let stopped = false;
  function drain() {
    if (stopped || !pending || isBusy()) return;
    pending = false;
    try { void Promise.resolve(run()).catch(onError); }
    catch (error) { onError(error); }
  }
  return {
    request() { if (!stopped) { pending = true; drain(); } },
    drain,
    get pending() { return pending; },
    start() { stopped = false; },
    stop() { stopped = true; pending = false; },
  };
}

module.exports = { scheduledHandoff };

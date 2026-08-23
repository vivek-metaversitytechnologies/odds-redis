function isDeadlock(error) {
  return error?.code === "ER_LOCK_DEADLOCK" || Number(error?.errno) === 1213 || error?.sqlState === "40001";
}

async function retryDeadlock(operation, { attempts = 3, delayMs = 50 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (!isDeadlock(error) || attempt === attempts) throw error;
      await new Promise((resolve) => setTimeout(resolve, delayMs * attempt));
    }
  }
  throw lastError;
}

module.exports = { isDeadlock, retryDeadlock };

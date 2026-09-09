require("dotenv").config({ quiet: true });
const queue = require("../src/services/pendingResultQueue");
const redis = require("../src/config/redis");

queue.listReview(process.argv[2] || "0")
  .then((page) => console.log(JSON.stringify(page, null, 2)))
  .catch((error) => { console.error(error.message); process.exitCode = 1; })
  .finally(() => redis.closeRedis());

module.exports = {
  apps: [{
    name: "odds-redis",
    cwd: __dirname,
    script: "src/server.js",
    instances: 1,
    exec_mode: "fork",
    autorestart: true,
    restart_delay: 5000,
    max_memory_restart: "1G",
    kill_timeout: 90000,
    time: true,
    env: {
      NODE_ENV: "production",
    },
  }],
};

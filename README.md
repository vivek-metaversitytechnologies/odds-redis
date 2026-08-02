# Odds streaming service

Production-style Node.js service that discovers active markets in MySQL,
subscribes through the provider HTTP API, consumes Socket.IO ticks, and writes
the latest frontend-ready payload for each event to Redis.

## Setup

Requires Node.js 20+, read-only access to the source MySQL database, and Redis.

```bash
npm install
cp .env.example .env
npm start
```

Configure `.env` before starting. The service uses only the read-only source database
configured through the `SOURCE_DB_*` variables; application state is stored in Redis.
Redis reconnect errors are logged and ticks are counted as failed
until Redis becomes available.

Provider HTTP requests and responses are logged with credentials redacted.
Use `PROVIDER_LOG_PAYLOADS` to enable/disable bodies and
`PROVIDER_LOG_MAX_CHARS` to cap large response previews.
Raw provider Socket.IO market `tick` messages are logged before transformation when
`PROVIDER_LOG_SOCKET_PAYLOADS=true`.
Per-market provider, queue, Redis, and frontend-emit timings are logged when
`PROVIDER_LOG_SOCKET_TIMINGS=true`.
Pretty-printed daily files are written to `logs/provider/provider-http-YYYY-MM-DD.log`.
Logging uses Winston with daily rotation, size limits, and retention controls.

## Runtime flow

- On startup and according to `MARKET_SYNC_CRON`, query active `t_market` rows.
- Retry market IDs skipped by the provider every `PROVIDER_SKIPPED_RETRY_MS` (default: 1000 ms) until accepted.
- Persist and publish final `go: true` result ticks, then unsubscribe those markets from the provider.
- Route every provider request and retry through a shared process-wide limiter capped by `PROVIDER_MAX_REQUESTS_PER_MINUTE`.
- Subscribe new market IDs in bounded HTTP batches.
- Connect to the provider Socket.IO endpoint and replay subscriptions after a reconnect.
- Serialize incoming writes to preserve tick order and avoid unbounded Redis work.
- Store the grouped frontend payload at `Data-Rs:<eid>`.
- Convert bookmaker runner payloads using market metadata before storing them.
- Publish the complete Redis-backed event payload to subscribed frontends after each persisted update.
- Unsubscribe and close HTTP, Socket.IO, Redis, and MySQL during shutdown.

## API

- `GET /health`
- `GET /db-time`
- `GET /api/socket/status`
- `GET /admin/` - manual discovery and subscription console
- `GET /api/provider/sports`
- `GET /api/provider/competitions`
- `GET /api/provider/events`
- `POST /api/provider/markets`
- `GET /api/provider/markets/:marketId/runners`
- `GET /api/events`
- `GET /api/events/:id`

Run tests with `npm test`.

For development with automatic restarts, run `npm run dev`.

Frontends load their initial snapshot from the API, then join the Socket.IO room with
`subscribe:event` and receive update-only `tick` messages.

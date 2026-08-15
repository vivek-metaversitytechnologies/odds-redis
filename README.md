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
- Coalesce event ticks for 100 ms by default and skip identical Redis writes and frontend emissions.
- Store the grouped frontend payload at `Data-Rs:<eid>`.
- Convert bookmaker runner payloads using market metadata before storing them.
- Publish the complete Redis-backed event payload to subscribed frontends after each persisted update.
- Preserve provider registrations and Redis snapshots during a process restart; close only
  local HTTP, Socket.IO, Redis, and MySQL connections during shutdown.

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
- `POST /api/source/events/:eventId/unsubscribe` - temporarily unsubscribe every market for an event
- `GET /betfair_api/fancy/:eventId` - public frontend-ready Redis snapshot (legacy-compatible shape)
- `GET /betfair_api/fancy/score/:eventId` - latest provider HTML scorecard for an event
- `GET /betfair_api/active_match/:sportId` - public active-event dashboard list (legacy-compatible shape)

Run tests with `npm test`.

For development with automatic restarts, run `npm run dev`.

Frontends load their initial snapshot from the API, then join the Socket.IO room with
`subscribe:event` and receive update-only `tick` and `score` messages. The subscription
acknowledgement includes both the latest odds `snapshot` and latest `score`.

Market discovery uses a fast primary pass on `MARKET_DISCOVERY_CRON` and a full typed-family
pass every `MARKET_FULL_DISCOVERY_MS` (default: 30000). Unchanged definitions are not rewritten.
Events already in play or starting within `ACTIVE_EVENT_LEAD_MINUTES` (60 by default) use the
fast discovery and subscription lane. Later events use definition-only discovery every 10 minutes
through `FUTURE_MARKET_DISCOVERY_CRON`; they are subscribed when they cross into the active window.
Active discovery and live cleanup request one event at a time by default, preventing the provider's
market-response limit from truncating large cricket events. Future discovery remains batched.

The `/health` WebSocket section reports current queued ticks/events and active writes plus a rolling
60-second traffic window for provider ingestion, Redis persistence, and frontend forwarding counts
and bytes. The same figures appear on the admin overview.

`REDIS_EVENT_CLEANUP_CRON` (every 10 minutes by default) scans event snapshot and score keys and
removes events that are no longer active in `t_event`. The cleanup is fail-safe: a source database
query failure aborts the run before any Redis keys are deleted.
Event snapshots and scorecards also use sliding 24-hour TTLs by default; configure them with
`REDIS_EVENT_TTL_SECONDS` and `REDIS_SCORE_TTL_SECONDS`.
Empty runner responses are cached for `RUNNER_MISS_CACHE_MS` (default: 300000).

Apply all pending database migrations using the service environment:

```bash
npm run db:migrate
```

The runner uses a database lock, records checksums in `service_migrations`, and safely
skips migrations already applied by an earlier deployment.

If an interrupted remote connection retains the migration lock, inspect it and then
release only that named lock owner:

```bash
npm run db:migrate:lock
npm run db:migrate:lock -- --force
```

## Deployed server

The backend currently runs on port `5673` at:

```text
http://143.110.249.169:5673
```

Use the health endpoint for deployment verification:

```bash
curl -s http://143.110.249.169:5673/health
```

Use the socket status endpoint to inspect provider activity:

```bash
curl -s http://143.110.249.169:5673/api/socket/status
```

A healthy response reports `sourceDatabase: "connected"`, Redis `connected: true`,
provider WebSocket `connected: true`, and no increase in `failedTickCount`. The `/`
route intentionally returns `404 Route not found`; this does not indicate a failed deployment.

The public IP endpoint is for direct verification. Production frontend traffic should
use an HTTPS domain through Nginx rather than public unencrypted port `5673`.

## PM2

Run exactly one backend instance because subscription and completed-market state is
process-local:

```bash
npm install -g pm2@latest
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

Run the command printed by `pm2 startup`, then use `pm2 status` and
`pm2 logs odds-redis` to verify the process.

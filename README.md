# Odds streaming service

Production-style Node.js service that discovers active markets in MySQL,
subscribes through the provider HTTP API, consumes Socket.IO ticks, and writes
the latest frontend-ready payload for each event to Redis.

## Setup

Incoming WebSocket `market` limits are logged as one readable line per message in
`logs/market-limits/market-limits-YYYY-MM-DD.log`, with `timestamp`, `eventId`,
`marketId`, `minbet` (raw `settings.ms`) and `maxbet` (raw `settings.mas`). Missing
values are recorded as `not-provided`. These are received values, not confirmation of a
successful database write. Logging is enabled by default, independently of
`LOG_LEVEL`, `LOG_TO_FILE`, and `PROVIDER_LOG_TO_FILE`. Configure it with
`MARKET_LIMITS_LOG_TO_FILE`, `MARKET_LIMITS_LOG_DIR`, `MARKET_LIMITS_LOG_MAX_SIZE`
(default `25m`), and `MARKET_LIMITS_LOG_MAX_FILES` (default `14d`). Entries identify
whether an update was `RECEIVED`, `SAVED`, `RETRYING`, or `RECOVERED`. Failed or
omitted settings are fetched again every `MARKET_SETTINGS_RETRY_MS` (default five
seconds), and all subscribed market settings are refreshed after a socket reconnect.

Ball-by-Ball discovery records and live `-BB` socket ticks are logged separately as
readable lines in `logs/ball-by-ball/ball-by-ball-YYYY-MM-DD.log`. Configure rotation
with `BALL_BY_BALL_LOG_TO_FILE`, `BALL_BY_BALL_LOG_DIR`,
`BALL_BY_BALL_LOG_MAX_SIZE` (default `25m`), and `BALL_BY_BALL_LOG_MAX_FILES`
(default `14d`).

Requires Node.js 20+, read-only access to the source MySQL database, and Redis.

```bash
npm install
cp .env.example .env
npm start
```

Start the isolated public REST process separately with `npm run start:api`. It serves only
the four `/betfair_api` endpoints and its own `/health`; it does not start vendor ingestion,
cron jobs, MySQL access, or either Socket.IO server.

Configure `.env` before starting. The service uses only the read-only source database
configured through the `SOURCE_DB_*` variables; application state is stored in Redis.
Redis reconnect errors are logged and ticks are counted as failed
until Redis becomes available.

Provider HTTP requests and responses are logged with credentials redacted.
Use `PROVIDER_LOG_PAYLOADS` to enable/disable bodies and
`PROVIDER_LOG_MAX_CHARS` to cap large response previews.
Raw socket logging records only fancy markets with IDs ending in `-F2`, `-F3`, `-OE`,
`-KD`, `-MT`, `-BB`, or `-CC` that carry `go: true`, `rt: true`, or a nonempty `res`
(including numeric zero). Ordinary updates and non-fancy markets are skipped, including
unrelated entries in mixed packets. Each `provider.socket.raw` record includes the socket
event name and the complete matching market object with all original fields, without
HTTP preview truncation. No environment changes are required; this logging ignores
`PROVIDER_LOG_SOCKET_PAYLOADS` and `PROVIDER_LOG_TO_FILE`. Records are also available through
`GET /api/logs/socket?marketId=<marketId>&limit=20`.
Per-market provider, queue, Redis, and frontend-emit timings are logged when
`PROVIDER_LOG_SOCKET_TIMINGS=true`.
Pretty-printed daily files are written to `logs/provider/provider-http-YYYY-MM-DD.log`.
Logging uses Winston with daily rotation, size limits, and retention controls.

## Runtime flow

- On startup and according to `MARKET_SYNC_CRON`, query active `t_market` rows.
- Retry market IDs skipped by the provider every `PROVIDER_SKIPPED_RETRY_MS` (default: 1000 ms) until accepted.
- Persist and publish final `go: true` result ticks, then unsubscribe those markets from the provider.
- Route every provider request and retry through a shared process-wide limiter. The
  configured `PROVIDER_MAX_REQUESTS_PER_MINUTE` is hard-clamped to 800 requests per
  20 seconds, leaving headroom below the vendor's 1000-request cap.
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
- `GET /betfair_api/live_match` - only live (in-play) events, grouped by every sport in `SPORT_IDS`;
  `GET /betfair_api/live_match/:sportId` returns one sport. Each event row is exactly an
  `active_match` row (`matchId`, `matchName`, `openDate`, `inPlay`, `marketId`, `bm`, `GM`,
  `outright`, back/lay prices, `li`), with `inPlay` always true, ordered like `active_match`. The
  live list comes from the Redis event metadata, so a live event with no displayable market yet is
  still listed, with a null `marketId` and zero prices. Responds `503` if any requested sport has
  no event metadata in Redis, and `404` for a sport that is not configured.

The public API process reads all four endpoints from Redis. If the `Events-Rs:<sportId>`
metadata required by `active_match` is absent, it returns `503` instead of falling back to
MySQL. The original process retains its existing routes and database fallback during proxy
cutover; route production traffic to `PUBLIC_API_PORT` before removing that compatibility path.

Run tests with `npm test`.

For development with automatic restarts, run `npm run dev`.

Frontends load their initial snapshot from the API, then join the Socket.IO room with
`subscribe:event` and receive update-only `tick` and `score` messages. The subscription
acknowledgement includes both the latest odds `snapshot` and latest `score`.

Market discovery uses a fast primary pass on `MARKET_DISCOVERY_CRON` and a full typed-family
pass every `MARKET_FULL_DISCOVERY_MS` (default: 30000). Unchanged definitions are not rewritten.
Events already in play or starting within `ACTIVE_EVENT_LEAD_MINUTES` (60 by default) use the
fast discovery lane. Later events use definition-only discovery every 10 minutes through
`FUTURE_MARKET_DISCOVERY_CRON`.

Provider socket subscription has no fixed pre-match lead time. Every active, non-resulted
market is a subscription candidate, ordered cricket-first, in-play-first, then soonest-kickoff-first.
In-play markets are always subscribed. Not-yet-live future markets are admitted in that same
order only while the process has resource headroom (event-loop delay and heap usage, sampled by
`resourceMonitor`/`healthSupervisor`); the moment either looks unhealthy, remaining future markets
are simply deferred to the next `MARKET_SYNC_CRON` cycle rather than starving live coverage. This
lets far-future events pick up their first live tick — replacing the frontend-hidden `WAITING`
placeholder seeded at discovery time — as early as spare capacity allows, without a static cutoff.
Active discovery and live cleanup request one event at a time by default, preventing the provider's
market-response limit from truncating large cricket events. Future discovery remains batched.

The `/health` WebSocket section reports current queued ticks/events and active writes plus a rolling
60-second traffic window for provider ingestion, Redis persistence, and frontend forwarding counts
and bytes. The same figures appear on the admin overview.

`REDIS_EVENT_CLEANUP_CRON` (every 10 minutes by default) scans event snapshot and score keys and
removes events that are no longer active in `t_event`. The cleanup is fail-safe: a source database
query failure aborts the run before any Redis keys are deleted.

### Market trace (ball-by-ball, line and cricket-casino markets)

Every ball-by-ball, line and cricket-casino market's lifecycle is written to `logs/market-trace/market-trace-<date>.jsonl`,
one JSON object per line with `ts`, `kind` (`BB`/`LINE`/`CC` by default; `MARKET_TRACE_KINDS` can add `F2`, `KD`, `OE`, `F3`, `MT` for a short investigation, since those change state on nearly every ball), `stage`, `eventId`, `marketId` and the
reason for each decision:

| Stage | Written when |
|---|---|
| `discovery.row` | a vendor discovery row's state (active, game over, status, name, ball line) changes |
| `discovery.decision` | discovery treats the market as `new`, `changed`, `deactivate` or `retire` |
| `discovery.omitted` | a known market is missing from a successful discovery response (it is retained) |
| `db.upsert` | `t_matchfancy` is inserted/updated, with the flags written |
| `definition` | a placeholder is `added`/`removed` in the payload, or `blocked` by a terminal set |
| `tick.state` | a socket or price tick changes status, `go`, `rt`, result or runner states (prices included) |
| `tick.rejected` | a tick is dropped: `no-db-row` or `inactive-in-db` (throttled) |
| `tick.blocked` | a live-looking tick stays hidden by a terminal/unavailable set (throttled) |
| `visibility` | the market is `shown` or `hidden` in the event payload, with the reason (`go`, `s-false`, `rt`, `abandoned`, `bb-terminal-set`, `line-terminal-set`, `line-unavailable-set`) |
| `line.set` / `bb.terminal` | terminal or unavailable markers change |
| `bb.reopened` | a terminal ball-by-ball market is reopened by a live socket tick newer than its closure (the vendor reuses market ids) |
| `price.seed` | a line market's HTTP price seed fails or returns nothing (throttled) |
| `subscription` | subscribe/unsubscribe outcome |
| `result` | a result is persisted or rejected, or the socket reports game over |
| `socket.gameover` | **any** market type's socket game-over, with `kind` (F2, F3, OE, KD, MT, CC, BB, LINE, REGULAR) and the `res` it carried |

Repeated identical states are not rewritten, so the file stays small even at tick rates. Analyse it with:

```bash
node scripts/marketTrace.js anomalies [--days=1] [--kind=BB|LINE|CC] [--event=<id>]
node scripts/marketTrace.js event <eventId>
node scripts/marketTrace.js market <marketId>
```

The same reports are served over HTTP for remote analysis. Set `MARKET_TRACE_API_KEY` and send it as
`X-Market-Trace-Key`; that key opens this route only (the admin session or `X-Internal-API-Key`
also work). At most two reports are read at once; further requests get 429.

```bash
curl -H "X-Market-Trace-Key: $KEY" "https://<host>/api/market-trace?view=anomalies&days=1&kind=BB"
curl -H "X-Market-Trace-Key: $KEY" "https://<host>/api/market-trace?view=event&id=<eventId>"
curl -H "X-Market-Trace-Key: $KEY" "https://<host>/api/market-trace?view=market&id=<marketId>&limit=500"
curl -H "X-Market-Trace-Key: $KEY" "https://<host>/api/market-trace?view=recent&stage=visibility&limit=200"
curl -H "X-Market-Trace-Key: $KEY" "https://<host>/api/market-trace?view=files"
```

`anomalies` groups markets by issue (shown after terminal, discovery active after the socket closed
it, ticks before the DB row existed, discovered but never ticked, game over without a result,
blocked while live, repeatedly omitted, price seed failures, unresolved subscriptions) and reports
discovery→first-tick and game-over→result latency.

### Socket result settlement

A socket game-over tick (`go: true`) carries the market's result in `res`, in the format
settlement already parses (runs, a digit, `back`/`lay`, or the winning selection id). In production
55/55 game-overs matched the result the API later stored, across every family.

- **Fancies** (session, F3, odd-even, khado, meter, cricket-casino, ball-by-ball) settle from it
  immediately with `FANCY_SOCKET_SETTLEMENT=on` (default; `BALL_BY_BALL_SOCKET_SETTLEMENT` is the
  older name). `shadow` only traces the value (`result` stage, `socket-shadow`); `off` leaves them to
  the API poller. Settling from the socket takes seconds instead of the once-a-minute poll and removes
  the market from the poller's candidates, so fewer results API calls.
- **Line markets** settle from the socket with an immediate API fallback (their own path).
- **Regular markets** (match odds, bookmaker, goals) are only watched (`REGULAR_SOCKET_SETTLEMENT=shadow`):
  the winning selection id is recorded on the `socket.gameover` census line, nothing is written.

The API poller stays the fallback, and results are written with `INSERT ... WHERE NOT EXISTS`, so
whichever path lands first wins. Trace `result` records carry `source: socket | api`, and every
game-over's census line carries `socketSettlement` (`on`, `shadow`, `off` or `line`).

### Cricket-casino result chase

The vendor computes a cricket-casino result only when that market is requested in a small results
request; the poller's large batches return existing results but never create one (measured: a
450-id request missed it twice, a single-id request created it immediately). The casino chaser
(`CASINO_RESULT_CHASE_INTERVAL_MS`, default 20 s) asks for every unsettled casino market of in-play
cricket events in batches of `CHASE_BATCH_SIZE` (code constant) and settles what comes back through
the normal result path. Status: `/health` → `pipelines.casinoResultChase`.

### Closed-fancy result chase and backlog sweep

Session-style fancies (session, F3, other-market, odd-even, khado, meter) close on the socket with
`s=false` (the market's own `s`; runner `sb` is not used). The vendor produces their result 1.5-8
minutes later, and the result poller used to pick it up up to a minute after that (up to 3 minutes
when its sweep missed the market). Closed markets are remembered in memory and the closed-fancy
chaser (`CLOSED_FANCY_CHASE_INTERVAL_MS`, default 15 s) asks the results API for just those until
they settle, reopen, or pass `CLOSED_FANCY_CHASE_MAX_AGE_MS` (default 1 h). Status: `/health` →
`pipelines.closedFancyChase`.

Because live fancies no longer depend on it, the poller's large rotating backlog lanes (thousands of
long-idle markets) run only every `RESULT_BACKLOG_SWEEP_EVERY_RUNS` runs (default 5); the recent,
pending and fallback lanes still run every minute. `pipelines.results.lastResult.backlogSweep` shows
which kind of run it was.

### Pending regular-result queue

Closed regular markets (`t_market`) wait for their result in the Redis queue
`Pending-Regular-Results`, which the headroom worker polls. The vendor publishes regular results
within hours of the event start (measured: football max 4.2 h, tennis 7.8 h, cricket 8.1 h), so:

- only markets seen active in the last 48 hours (`t_market.updatedon`), or whose event is still in
  play on an active event (multi-day Tests; a stale `in_play` on an ended event does not count), are queued — by the hourly recovery scan and when due entries are loaded;
  older entries are dropped;
- line markets never enter (their result lives in `t_fancyresult`), and the regular poller lanes
  skip them too;
- an entry is retried every minute for 12 hours; without a result it moves to
  `Pending-Regular-Results:review` (`scripts/list-pending-result-review.js`) and is not queued
  again. Review entries are pruned after 14 days.

### Line market suspension

The socket suspends a line market briefly during play (observed 9-17 s) and keeps it `SUSPENDED`
once its over has finished until it is settled. A line market that stays `SUSPENDED` for
`LINE_MARKET_SUSPENDED_HIDE_MS` (default 30000) joins `Unavailable-LineMarket-Rs:<eventId>` and is
hidden; an `OPEN` socket tick shows it again. The 2-second HTTP price refresh carries no status,
so it keeps the socket's last market and runner status instead of falling back to the stored one.

### Stake-limit polling

The provider's `market` room is not a reliable source of stake limits on its own: it has pushed
placeholder limits (`minbet`/`maxbet` of 1) and later corrected them only in its settings API. Two
in-process pipelines therefore poll `POST /v1/markets/settings` for everything the frontend lists:

| Pipeline | Payload groups | Interval | Budget |
|---|---|---|---|
| `market` | `Odds`, `Bookmaker` | `LIMITS_MARKET_POLL_INTERVAL_MS` (30000) | `LIMITS_MARKET_POLL_MAX_REQUESTS_PER_MINUTE` (60) |
| `fancy` | every other group | `LIMITS_FANCY_POLL_INTERVAL_MS` (5000) | `LIMITS_FANCY_POLL_MAX_REQUESTS_PER_MINUTE` (240) |

Targets are the events in the active-match list (`SPORT_IDS`) and the markets their frontend
payloads show. Only a max stake that differs from the payload is written (DB, Redis, frontend tick).

`minbet` is a fixed business rule: discovery creates every market and fancy with `minbet=100`, and
nothing changes it afterwards. The provider's `ms` (from the settings API, the pollers or the
`market` room) is logged but never applied; only `mas` updates `maxbet`. If
the listed markets need more requests than a run's budget allows, a rotating cursor covers them
across runs; `lastRun.fullCycleMs` in `/health` (`pipelines.limitsPoll`) reports how long one full
pass takes. Runs are skipped while total provider traffic is above
`LIMITS_POLL_PROVIDER_HEADROOM_PERCENT` of the application cap, and markets the provider omits from
its response are not requested again for `LIMITS_POLL_UNSUPPORTED_TTL_MS`. The settings API answers
at most 50 markets per request (larger requests are silently truncated), so batch sizes are capped at
50; `node scripts/benchmarkMarketSettings.js` re-measures latency, the cap and ID-kind coverage.
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

Run exactly one ingestion instance because subscription and completed-market state is
process-local. The PM2 configuration also starts the independent `odds-public-api` process:

```bash
npm install -g pm2@latest
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

Run the command printed by `pm2 startup`, then use `pm2 status`, `pm2 logs odds-redis`, and
`pm2 logs odds-public-api` to verify both processes. Point the four `/betfair_api/` Nginx
locations to `127.0.0.1:$PUBLIC_API_PORT`; keep `/socket.io/` on the ingestion service port.

Line markets have a dedicated sequential discovery job every second, configurable with
`LINE_MARKET_DISCOVERY_CRON`. It checks active-window cricket events individually,
updates database and Redis definitions, and immediately subscribes to active markets.
Overlapping cycles are skipped. Explicit inactive/game-over flags remove markets;
omissions retain them. Provider latency and queue limits can extend a cycle beyond one second.

Repair generic fancy names in existing database rows with
`node scripts/repair-fancy-name.js --all`, or pass one market ID instead of `--all`.
The repair covers F2, F3, OE, KD, MT, and CC in both `t_matchfancy` and
`t_fancyresult`, including completed records. It preserves descriptive names and
settlement fields, skips missing or ambiguous provider names, and reports a summary.
Ball-by-ball retains its dedicated numbered-name discovery handling.

Event retirement selects active rows per event and commits at most 100 rows at a
time, avoiding rewrites of historical inactive markets. Retirement and fancy
discovery retry deadlocked transactions up to three times. Discovery repairs
fallback names after committing its market batch; it does not repeatedly repair
historical results for markets whose stored names are already descriptive. Use
the repair script above for that backfill.

Migration `003_fancy_result_lookup_index.sql` adds a non-unique index on
`t_fancyresult(fancyid(191))` unless a leading index covering at least 191
characters (or the full column) already exists. The prefix fits MyISAM’s
1,000-byte key limit with utf8mb4. It
preserves duplicate result history and the existing storage engine. On MyISAM,
index creation can block table access, so apply it during a maintenance window.
The code changes do not apply this migration automatically.

Socket game-over cleanup also commits at most 100 market rows at a time and
keeps Redis calls outside database transactions. Deadlock retries do not count
as additional event-terminal observations. Migration
`004_regular_result_lookup_indexes.sql` supplies MyISAM-safe regular and
exceptional result lookup indexes. See [the settlement database plan](docs/settlement-db-plan.md)
for the production findings, targeted migration rollout, and a separate InnoDB
conversion plan.

# Live Trip Updates Fan-Out — Ordered, Resumable, Back-Pressured

MoveInSync Case Study 4. Security desks, transport admins and employees watch trips live over WebSocket: cab
locations, trip status changes, delays and SOS alerts. This project is the publish-subscribe layer that delivers those
updates: one Spring Boot application run as several identical gateway replicas, with Redis Streams as the durable,
ordered event history and Redis Pub/Sub to tell gateways that new events exist.

## What it does

| Requirement | How |
|---|---|
| Topics per cab, trip and office | `trip:{id}`, `cab:{id}`, `office:{id}`, plus `sos:{office}` for security desks |
| Authorisation on every subscription, tenant isolation | Checked on every SUBSCRIBE/RESUME and re-checked every 30 s; tenant always taken from the authenticated identity |
| Per-topic ordering, at-least-once, deduplication | Redis assigns a sequence number per topic atomically; clients drop `seq ≤ last processed` |
| Resume after reconnect | `RESUME` with the last sequence number, on any gateway; replays exactly the missed events from a bounded buffer (1000 per topic) |
| Away too long | `RECOVERY_REQUIRED`; the client re-subscribes and gets the latest location per cab and live events. **No snapshot of trip status, delay or SOS state is provided** |
| Slow consumers | Bounded queue per connection; locations conflated to the latest per cab; status and SOS never dropped (an overflowing client is disconnected and resumes) |
| SOS priority | Own Redis connection and notification channel; served before locations inside each gateway |
| Many gateways, node crash | Every gateway learns of every event; clients of a failed gateway reconnect elsewhere and resume without losing reliable events |

Measured with 10,080 WebSocket clients, 2,000 publishing cabs and a gateway killed mid-run: 0 reliable events lost,
SOS p99 933 ms for connected clients; clients that were reconnecting after their gateway died received SOS after up to
4.3 s. Details and limits: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Documents

- [docs/DESIGN.md](docs/DESIGN.md) — architecture, event flow, guarantees, failure modes, trade-offs
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — WebSocket messages, authentication, resume, errors, close codes
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — load-test method and results

## Architecture

```
  publisher (Java EventPublisher) ──► Redis 7.4: per-topic sequence + stream (atomic Lua), latest location per cab
                                        │  Pub/Sub hints: notify:events, SOS on notify:events:sos
                                        ▼
            gateway-1 / gateway-2 / gateway-3   (read new events per topic, queue per connection)
                                        ▲
              screens ── WebSocket /ws ─┴── nginx :8080 (least connections)
```

## Getting started

### What you need

| To… | Install |
|---|---|
| Run the application | **Docker** with Docker Compose v2 (Docker Desktop on Windows/macOS), running. Nothing else: the application is compiled inside Docker. |
| Run the tests | Additionally **JDK 21**, with `JAVA_HOME` pointing to the JDK directory. Maven comes with the repository (Maven Wrapper). |
| Watch updates live | Any WebSocket client; the examples use [wscat](https://github.com/websockets/wscat) (needs Node.js: `npm install -g wscat`). |

Give Docker at least 4 GB of memory (the containers' limits add up to about 3.7 GB; the optional load generator adds
1.6 GB). Ports 8080–8083 and 16379 must be free.

### From a fresh clone to a running system

```bash
# 1. Get the code
git clone https://github.com/rawlx/moveinsync.git
cd moveinsync

# 2. Build and start Redis, three gateways and nginx in the background
#    (the first build downloads dependencies and takes a few minutes)
docker compose up --build -d

# 3. Wait until all three gateways show "(healthy)"
docker compose ps

# 4. Check that the system is ready
curl http://localhost:8080/actuator/health/readiness     # {"status":"UP",...}   (Windows: curl.exe)
```

Then go to [Try it](#try-it) to generate traffic and watch updates arrive.

To update an existing checkout later: `git pull`, then `docker compose up --build -d` again (it rebuilds the image).

### Endpoints

| Endpoint | |
|---|---|
| `ws://localhost:8080/ws` | WebSocket, load balanced across the gateways |
| `ws://localhost:8081/ws`, `8082`, `8083` | A specific gateway |
| `http://localhost:8080/actuator/health` | Health (readiness includes Redis) |
| `http://localhost:8080/actuator/prometheus` | Metrics |

### Stop

```bash
docker compose down        # stop and remove the containers
docker compose down -v     # ... and also delete the stored events (Redis volume)
```

The Compose stack runs in **development authentication mode**: it accepts fixed test tokens and must not be deployed
as is. Outside Compose the application default rejects every connection.

## Try it

Any WebSocket client works; the examples use [wscat](https://github.com/websockets/wscat) (`npm install -g wscat`).

**1. Generate live traffic.** The load generator publishes a simulated fleet of 2,000 cabs (2 tenants × 10 offices ×
100 cabs) through the event publisher, and connects its own clients:

```bash
docker compose --profile loadtest run --rm -e LOADTEST_CLIENTS=500 -e LOADTEST_DURATION_S=300 loadgen
```

It publishes for about five minutes (plus a short connect, warm-up and drain phase) and ends with a summary of its
checks. At this size its "≥ 10,000 connections" check
is reported as missed — expected, since it was told to use 500 clients (minimum 460).

**2. Watch while it runs**, with tokens of the simulated fleet:

```bash
wscat -c "ws://localhost:8080/ws?access_token=syn1-admin-1"
> {"type":"SUBSCRIBE","id":"m1","topic":"office:O1"}
```

| Token | Can subscribe to | Sees |
|---|---|---|
| `syn1-admin-1` | `office:O1`, and `trip:T1-*` / `cab:C1-*` of office O1 | Status, delay, SOS and locations of office O1's 100 cabs |
| `syn1-sec-1` | `sos:O1` | SOS alerts of office O1 |
| `syn1-emp-1-1-1` | `trip:T1-1` only | Updates of their own trip |

Pattern: `syn{tenant}-admin-{office}`, `syn{tenant}-sec-{office}`, `syn{tenant}-emp-{office}-{cab}-{passenger}` with
tenant 1–2, office 1–10, cab 1–100, passenger 1–4; trips are `T{office}-{cab}`, cabs `C{office}-{cab}`.

**3. Try the rules:**

```text
> {"type":"SUBSCRIBE","id":"m2","topic":"trip:T1-2"}          (with syn1-emp-1-1-1: another employee's trip)
< {"type":"ERROR","id":"m2","code":"FORBIDDEN","message":"Subscription not permitted","topic":"trip:T1-2"}

> {"type":"UNSUBSCRIBE","id":"m3","topic":"trip:T1-1"}
< {"type":"UNSUBSCRIBED","id":"m3","topic":"trip:T1-1"}
> {"type":"RESUME","id":"m4","topic":"trip:T1-1","lastSeq":<a seq you received>}
< ... the events you missed, in order ...
< {"type":"RESUME_COMPLETE","id":"m4","topic":"trip:T1-1","fromSeq":...,"toSeq":...,"replayed":3}
```

Connecting without a token is refused with HTTP 401. Fixed test tokens for a small hand-made directory (tenants `acme`
and `globex`, e.g. `dev-acme-emp-1` for `trip:T-1`) are listed in [docs/PROTOCOL.md](docs/PROTOCOL.md), together with
every message type, resume semantics, errors and close codes.

## Publishing events from code

There is no HTTP ingestion endpoint: events are published by Java code through `EventPublisher`, as the load
generator does. It routes the event to its topics from the directory, appends it durably to Redis, then notifies the
gateways. Example against the Compose stack's Redis (published on host port 16379) and the test directory:

```java
RedisClient redis = RedisClients.create("redis://localhost:16379", Duration.ofSeconds(2));
AppProperties.Retention retention =           // same values as the gateways' app.retention settings
        new AppProperties.Retention(1000, 100_000, Duration.ofHours(24), Duration.ofMinutes(5));
EventPublisher publisher = new EventPublisher(
        new RedisEventStore(redis, retention),
        new RedisNotificationPublisher(redis, "notify:events"),
        new RedisEventStore(redis, retention),                 // SOS lane: own connection ...
        new RedisNotificationPublisher(redis, RedisNotificationPublisher.priorityChannel("notify:events")), // ... and channel
        new EventRouter(DevFixtures.load("dev/dev-fixtures.json").directory()));

publisher.publish(new TripEvent("e-1", "acme", "T-1", "C-1", Instant.now(),
        new StatusPayload(TripStatus.PICKED_UP)));
publisher.publish(new TripEvent("e-2", "acme", "T-1", "C-1", Instant.now(),
        new SosPayload("S-1", SosRaiser.EMPLOYEE, new GeoPoint(12.97, 77.59))));
```

The event id is the idempotency key: publishing the same event again within 5 minutes returns the original sequence
numbers without a second append. An event whose trip, cab or office the directory does not confirm is rejected
(`RoutingException`) before anything is written.

## Tests

```bash
./mvnw -B -ntp test           # unit tests (no Docker needed)        Windows: .\mvnw.cmd -B -ntp test
./mvnw -B -ntp clean verify   # unit + integration tests (Docker)    Windows: .\mvnw.cmd -B -ntp clean verify
```

Integration tests start Redis in a container (Testcontainers). Smoke tests against the running Compose stack (start
it first with `docker compose up --build -d`):

```bash
./mvnw -B -ntp -pl gateway verify -Dit.test=ComposeSmokeIT -Dsmoke.compose=true -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false
```

(In PowerShell, quote each `-D…` argument.) Result at submission: 244 unit and 53 integration tests pass (3 skipped:
the smoke tests, which run only with `-Dsmoke.compose=true`); the 3 smoke tests pass against the stack.

The full load test with a gateway kill is driven by a PowerShell script (Windows):

```powershell
powershell -ExecutionPolicy Bypass -File deploy\loadtest\run-loadtest.ps1 -Clients 10100 -DurationSeconds 180 -KillAfterSeconds 60 -KillService gateway-2
```

It builds and starts the stack, runs the load generator, kills `gateway-2` during the run, restores it afterwards and
prints the result. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Configuration

Settings live in `gateway/src/main/resources/application.yml` and can be overridden with environment variables (Spring
relaxed binding, e.g. `APP_RETENTION_LOGMAXLENGTH=2000`).

| Property | Default | Meaning |
|---|---|---|
| `app.redis.uri` | `redis://localhost:6379` | Redis (`APP_REDIS_URI`) |
| `app.auth.mode` | `NONE` | `NONE` rejects every connection; `DEV` accepts the test tokens (`APP_AUTH_MODE`) |
| `app.retention.log-max-length` | `1000` | Replay buffer: events kept per topic |
| `app.retention.topic-key-ttl` | `24h` | A topic's history expires after this long without events |
| `app.retention.dedup-ttl` | `5m` | Window in which a republished event id is recognised |
| `app.outbox.critical-capacity` | `512` | Status/delay/SOS queue per connection (overflow ⇒ close `4008`, client resumes) |
| `app.outbox.location-capacity` | `2048` | Pending locations per connection (latest per cab) |
| `app.outbox.max-subscriptions-per-connection` | `50` | Subscription limit |
| `app.notifications.channel` | `notify:events` | Notification channel (SOS uses `notify:events:sos`) |
| `app.notifications.reconcile-interval` | `5s` | Periodic catch-up from Redis, independent of notifications |
| `app.auth.revalidate-interval` | `30s` | Re-authorisation of live subscriptions |
| `app.websocket.heartbeat-interval` / `idle-timeout` | `20s` / `60s` | Ping cadence / close `4010` after silence |
| `app.websocket.max-message-chars` | `4096` | Largest accepted client message |

## Project layout

```
gateway/src/main/java/com/moveinsync/tripupdates/
  domain/         events, payloads, topics, validation (plain Java)
  routing/        EventRouter: an event's topics from the directory
  publish/        EventPublisher: durable append, then notification
  store/          RedisEventStore and Lua scripts (resources/redis/): sequencing, history, latest locations
  notify/         Pub/Sub notification publisher and subscriber
  gateway/        subscriptions, per-topic feeds, per-connection queues and writers
  ws/             WebSocket endpoint, protocol, authentication handshake, heartbeat
  auth/           identities, roles, authorisation, test directory and tokens
  observability/  metrics
  config/         Spring configuration
  sim/, loadtest/ trip simulator and load generator
deploy/           nginx configuration, load-test script
docker-compose.yml, Dockerfile
```

## Limitations

- No authoritative snapshot of trip status, delay or SOS state after `RECOVERY_REQUIRED`.
- SOS delivery within 2 s was measured for connected clients; clients reconnecting after a gateway failure wait for
  their reconnect (2.6–6.4 s in the measured run).
- The load test ran on a single laptop and predates the dedicated SOS lane; the frozen-desk scenario was not
  load-tested.
- Development token authentication only; no production identity provider.
- Single Redis node; every gateway receives every notification; about 130 KiB of gateway memory per connection.

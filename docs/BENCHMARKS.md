# Benchmarks

One load test at the case-study target: at least 10,000 WebSocket connections, 2,000 publishing cabs, SOS latency,
and one gateway killed while traffic flows. All figures below were **measured in that run**, unless marked *derived*
(arithmetic on measured values) or *estimate*.

> **Note:** this run predates two later changes: the dedicated SOS lane (its own Redis connection and
> `notify:events:sos` channel) and gateways serving newly arrived status/SOS events before pending location reads.
> The SOS latency figures therefore describe the earlier code; the run **has not been repeated** since.

## Environment

| | |
|---|---|
| Host | Intel Core i5-8265U @ 1.60 GHz (4 cores / 8 threads), 11.9 GB RAM, Windows 11 |
| Docker | Docker Desktop, engine 29.5.3, WSL2 VM with 8 vCPUs and 5.73 GiB memory |
| Containers | 3 gateways (1000 MiB memory limit each, Java 21, virtual threads), Redis 7.4.12 (640 MiB limit, AOF every second, `maxmemory` 512 MB, `noeviction`), nginx 1.28.3, load generator (1600 MiB limit) |
| Sharing | **The load generator, the three gateways and Redis all ran on the same laptop and competed for its CPU.** Tail latencies include that contention. |

## Method

- **Load generator:** `loadtest/LoadTest`, running in its own container on the Compose network. Clients are real
  WebSocket connections with test tokens, spread round robin over the three gateways (directly, not through nginx).
  A client counts as connected only once the gateway confirmed its subscription (`SUBSCRIBED`).
- **Traffic:** a simulated fleet of 2 tenants × 10 offices × 100 cabs = 2,000 cabs and trips, published through the
  routed `EventPublisher` by 8 publisher threads: a location every 5 s per cab, a status change every 6th location
  tick, a delay with probability 0.02 per tick, and 2 SOS per second on random trips.
- **Clients:** per office 2 security users (`sos:` topic), 10 office admins (`office:`), 10 cab watchers (`cab:`);
  the rest are employees on their own trip; plus 20 probes that ask for another employee's trip and must be refused.
- **Correctness check:** every published reliable event is recorded with its topic sequence number. At the end, each
  client's received sequence numbers are compared with what was published on its topic after it subscribed. Clients
  accept only increasing sequence numbers, so a reordering would show up as a missing event.
- **SOS latency:** send and receive times are taken in the same process (`System.nanoTime()`), so no clock
  synchronisation is involved. It covers routing, the Redis append, the notification, the gateway's read, queueing,
  the WebSocket write and the client parsing the frame. Deliveries to a client that was reconnecting at that moment
  are reported separately.
- **Failure injection:** `gateway-2` was killed (`docker kill`) 60 s into steady traffic and restored after the run.
  Its clients reconnected, with backoff, to a surviving gateway chosen by client id, and sent `RESUME`.
- **Duration:** 180 s of steady traffic after a 15 s warm-up, then a 30 s drain.

Command (Windows PowerShell, from the repository root):

```powershell
powershell -ExecutionPolicy Bypass -File deploy\loadtest\run-loadtest.ps1 -Clients 10100 -DurationSeconds 180 -KillAfterSeconds 60 -KillService gateway-2
```

## Results

| Measure | Result |
|---|---|
| Connected and subscribed clients | **10,080** (plus 20 refused probes); 0 failed to connect; 3,360 / 3,361 / 3,359 per gateway |
| Publishing cabs | 2,000 |
| Events published | 91,767: 78,392 locations, 12,000 status changes, 982 delays, 393 SOS; 0 publish failures |
| Frames delivered to clients | 1,273,214 |
| Reliable events (status, delay, SOS) | 200,310 expected, **0 missing**, 0 unexpected, 0 duplicates |
| Tenant isolation | 0 events delivered across tenants; 20 of 20 unauthorised subscriptions refused |
| SOS deliveries | 786 of 786 (393 SOS × 2 security users of the office) |

Throughput (*derived*): publishing ran about 196 s (warm-up plus steady phase), so about 470 events/s published and
about 6,500 frames/s delivered on average; locations were about 400/s by construction (2,000 cabs every 5 s).

### SOS latency

| Clients | Deliveries | p50 | p95 | p99 | Max | Over 2 s |
|---|---|---|---|---|---|---|
| Connected | 779 | 8.1 ms | 96.0 ms | **933.3 ms** | 1,667 ms | 0 |
| Reconnecting after the gateway kill | 7 | 3,165 ms | 4,321 ms | 4,321 ms | 4,321 ms | 5 |
| All | 786 | 8.2 ms | 110.9 ms | 1,309.9 ms | 4,321 ms | 5 |

All five deliveries over 2 s went to clients that had lost `gateway-2` and had not yet completed their resume; all
five SOS were published within 2.8 s after the first disconnect. That their latency is the reconnect time is an
inference from this timing.

### Gateway failure

| Measure | Result |
|---|---|
| Clients disconnected by the kill | 3,361 |
| Outage per client (disconnect → `RESUME_COMPLETE`) | p50 4.51 s, p95 5.46 s, p99 5.90 s, max 6.43 s |
| All clients recovered | within 6.61 s of the first disconnect |
| After failover | 5,040 / 5,040 sessions on the two surviving gateways |
| Reliable events lost during recovery | 0 |

Clients of the two surviving gateways were not disconnected and kept receiving throughout.

### Resources

| Phase | Memory per gateway | CPU per gateway |
|---|---|---|
| Idle, no clients | 186–194 MiB | < 1 % |
| Connected, about 3,360 clients each | 611–638 MiB | 160–204 % of a core |
| Steady traffic | 641–653 MiB | 158–181 % |
| After failover, 5,040 clients each | 821–849 MiB **of the 1000 MiB limit** | 86–132 % |

The load generator used up to 1.35 GiB and 2.4–3.0 cores; Redis at most 37 MiB.

## Estimates (not measured)

- **Memory per connection** (*derived*): (connected − idle gateway memory) ÷ clients ≈ 130 KiB per subscribed
  connection, consistent at about 3,360 and 5,040 clients. This is an upper bound: it includes memory not yet
  reclaimed by the garbage collector and traffic buffers.
- **Capacity per gateway** (*estimate*): about 6,000–7,000 connections per 1 GB gateway at that rate, before leaving
  headroom for absorbing a failed gateway's clients.
- **Case-study scale** (*estimate*): 200,000 screens would need tens of gateways of this size. Every gateway receives
  every notification, so notification work per gateway grows with the total event rate; all appends go to one Redis
  node. Neither limit was measured.

## Limitations

- Everything ran on one laptop; tail latencies include CPU contention with the load generator.
- The run predates the SOS lane and has not been repeated (see the note at the top).
- SOS within 2 s was not met for clients whose gateway failed: they waited for their reconnect.
- The frozen-desk scenario (one desk stops reading while others are measured) was not part of the load test; it is
  covered only by automated functional tests.
- After the failover the survivors ran at 82–85 % of their memory limit; a second failure at that load was not tested.
- No run at the case-study scale; no micro-benchmarks of individual components.

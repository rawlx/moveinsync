# WebSocket Protocol — `/ws`

JSON text frames over one WebSocket per client. Source of truth: `ws/ProtocolCodec`, `ws/ProtocolService`,
`ws/ErrorCode`, `gateway/CloseReason`. The example frames below were captured from the Compose stack (sequence
numbers and ids differ per run).

## Connecting and authentication

- URL: `ws://<host>:8080/ws` (nginx) or a gateway directly (`8081`–`8083` in Compose).
- Credential: `Authorization: Bearer <token>` header, or the `access_token` query parameter for browsers, which cannot
  set headers on a WebSocket (`ws://localhost:8080/ws?access_token=dev-acme-emp-1`). The token is never logged; nginx
  logs the path without the query string.
- No or invalid credential: the upgrade is refused with HTTP **401** and no session exists.
- `app.auth.mode=NONE` (the application default) rejects every connection. `DEV` (used by Compose and tests) accepts
  the well-known tokens of `gateway/src/main/resources/dev/dev-fixtures.json`, plus the generated load-test tokens when
  the synthetic fleet is enabled. There is no production identity provider; `ConnectionAuthenticator` is the seam.
- Browser origins: cross-origin browser connections are refused unless `app.websocket.allowed-origin-patterns` is set.

Dev tokens (fixtures):

| Token | Tenant | Role | May read |
|---|---|---|---|
| `dev-acme-emp-1` | acme | EMPLOYEE | `trip:T-1` (passenger) |
| `dev-acme-emp-2` | acme | EMPLOYEE | `trip:T-2` |
| `dev-acme-admin-o1` | acme | OFFICE_ADMIN | `office:O-1`, and trips and cabs of office O-1 |
| `dev-acme-security-o1` | acme | SECURITY | `sos:O-1` (explicit grant) |
| `dev-globex-emp-1` | globex | EMPLOYEE | `trip:T-1` of tenant globex |
| `dev-globex-admin-o1` | globex | OFFICE_ADMIN | `office:O-1` of tenant globex |

## Authorization and tenant isolation

- Tenant, role and permissions come only from the authenticated identity, never from a message. A topic name is always
  resolved inside the caller's tenant, so `trip:T-1` of acme and of globex are different topics.
- Every SUBSCRIBE and RESUME is authorized (`auth/TopicAuthorizer`):
  EMPLOYEE — own trips only; OFFICE_ADMIN — `office:O` of administered offices and `trip:`/`cab:` topics of those
  offices; SECURITY — explicitly granted `sos:O` topics only. Nobody may read another tenant's data.
- A forbidden topic and a topic that does not exist get the same `FORBIDDEN` answer (no existence leak).
- Live subscriptions are re-checked every 30 s (`app.auth.revalidate-interval`); a withdrawn permission removes the
  subscription and its pending events and sends `ERROR SUBSCRIPTION_REVOKED`.

## Topics

`trip:{id}`, `cab:{id}`, `office:{id}`, `sos:{officeId}`; ids match `[A-Za-z0-9_-]{1,64}`. Routing (who receives an
event) is derived from the directory, never from the publisher: STATUS and DELAY go to the trip, its cab and its
office; SOS additionally to `sos:{office}`; LOCATION with a trip to the trip, cab and office, without a trip to the cab
and office.

## Client → gateway

```json
{"type":"SUBSCRIBE",   "id":"m1", "topic":"trip:T-1"}
{"type":"RESUME",      "id":"m2", "topic":"trip:T-1", "lastSeq":1793338576578028}
{"type":"UNSUBSCRIBE", "id":"m3", "topic":"trip:T-1"}
{"type":"ACK",                    "topic":"trip:T-1", "seq":1793338576578029}
```

- `id`: `[A-Za-z0-9_-]{1,64}`, echoed in the reply (optional on ACK). `lastSeq` ≥ 0 and `seq` ≥ 1 are integers.
- Unknown fields or types, invalid values and frames over 4096 characters get an `ERROR`; the connection stays open.
- At most 50 subscriptions per connection (`TOO_MANY_SUBSCRIPTIONS`).
- `ACK` is optional and informational: the gateway records the highest acknowledged sequence per subscription and
  rejects ACKs for sequences it never delivered (`INVALID_ACK`); it does not drive redelivery. Resume always uses the
  client's own `lastSeq`.

## Gateway → client

```json
{"type":"SUBSCRIBED","id":"m1","topic":"trip:T-1","seq":1793338576578028}
{"type":"EVENT","topic":"trip:T-1","seq":1793338576578026,"publishedAt":"2026-10-30T06:06:24.014Z",
 "event":{"eventId":"e-1042","tenantId":"acme","type":"LOCATION","tripId":"T-1","cabId":"C-1",
          "occurredAt":"2026-10-30T06:06:23.995042100Z",
          "data":{"lat":12.98,"lon":77.70,"speedKph":22.7,"heading":107.2}}}
{"type":"RESUME_COMPLETE","id":"m7","topic":"trip:T-1","fromSeq":1793338576578028,"toSeq":1793338576578028,"replayed":0}
{"type":"UNSUBSCRIBED","id":"m5","topic":"trip:T-1"}
{"type":"ERROR","id":"m2","code":"FORBIDDEN","message":"Subscription not permitted","topic":"trip:T-2"}
```

(Coordinates shortened in the example.) `event.data` by type: STATUS `{"status":"PICKED_UP"}` (ASSIGNED,
DRIVER_EN_ROUTE, ARRIVED, PICKED_UP, DROPPED, COMPLETED, CANCELLED); DELAY `{"delayMinutes":4,"reasonCode":"TRAFFIC"}`;
SOS `{"sosId":…,"raisedBy":"EMPLOYEE",…,"lat":…,"lon":…}`; LOCATION `{"lat":…,"lon":…}` with optional
`speedKph` and `heading`. `publishedAt` is Redis time at append.

## Sequence numbers and deduplication

- Redis assigns one sequence per topic inside the append script (see [DESIGN.md](DESIGN.md)). Numbers increase
  strictly per topic but have gaps (locations consume numbers too). They are large integers (≈ 1.8 × 10¹⁵), still
  exactly representable as JSON/JavaScript numbers.
- **Reliable events** (STATUS, DELAY, SOS) arrive per topic in increasing `seq`. Delivery is at-least-once: after a
  reconnect an event may arrive again. A client keeps, per topic, the last reliable `seq` it processed and drops any
  reliable event with `seq ≤` that value.
- **LOCATION** events are latest-value data: they may be conflated, may arrive after `RESUME_COMPLETE`, and their
  `seq` can be lower than the last reliable `seq` (observed in the example above). Keep a separate last-seen `seq` per
  cab, apply a location only if it is newer, and **never use a location `seq` as the resume position**.

## Subscribe

`SUBSCRIBE` starts at the topic's current head: `SUBSCRIBED.seq` is the position (only events with a higher `seq`
follow); no history is replayed. The latest location per cab of the topic, if any, follows immediately, then live
events. Use `SUBSCRIBED.seq` as the initial `lastSeq`.

## Resume, replay and RESUME_COMPLETE

Send `RESUME` with the last reliable `seq` processed, on any gateway, for a topic not currently subscribed on that
connection (otherwise `ALREADY_SUBSCRIBED`). The gateway authorizes it like a SUBSCRIBE, then:

1. replays every retained reliable event with `seq > lastSeq` in order;
2. sends `RESUME_COMPLETE` (`fromSeq` = `lastSeq`, `toSeq` = position reached, `replayed` = count);
3. sends the latest location per cab;
4. continues with live events, filtered so that nothing at or below the position is sent twice.

The replay and the switch to live happen under one lock per topic, so no event published meanwhile is skipped or
duplicated on that subscription.

## RECOVERY_REQUIRED (expired or unknown history)

If `lastSeq` is older than the retained history (more than 1000 reliable events per topic, or the topic expired after
24 h without events) or newer than anything Redis knows, nothing is attached and the client receives:

```json
{"type":"ERROR","id":"m6","code":"RECOVERY_REQUIRED",
 "message":"Requested position is not in the retained history; reload state and SUBSCRIBE","topic":"trip:T-1",
 "recovery":{"requestedSeq":1,"oldestAvailableAfterSeq":1793338576578000,"headSeq":1793338576578028}}
```

**The system does not provide an authoritative snapshot of trip status, delay or SOS state.** The only recovery is a
new `SUBSCRIBE`, which delivers the latest location per cab and then live events. Reliable events missed beyond the
retained window are not reconstructed, and the client does not learn the trip's current status until the next STATUS
event. ("Reload state" in the message refers to the client's own data sources; the gateway offers no state endpoint.)

## Errors

| Code | When |
|---|---|
| `BAD_REQUEST` | Not JSON, missing, invalid or unknown field |
| `UNSUPPORTED_TYPE` | Unknown `type` |
| `MESSAGE_TOO_LARGE` | Frame over `app.websocket.max-message-chars` (4096) |
| `FORBIDDEN` | Topic not permitted, or does not exist |
| `ALREADY_SUBSCRIBED` / `NOT_SUBSCRIBED` | SUBSCRIBE/RESUME on a subscribed topic; UNSUBSCRIBE/ACK on an unsubscribed one |
| `TOO_MANY_SUBSCRIPTIONS` | More than 50 subscriptions on the connection |
| `INVALID_ACK` | ACK for a sequence never delivered on that subscription |
| `RECOVERY_REQUIRED` | RESUME position outside the retained history (above) |
| `SUBSCRIPTION_REVOKED` | Permission withdrawn for a live subscription |
| `UNAVAILABLE` | Event store unreachable; nothing attached, retry later |
| `INTERNAL` | Unexpected failure |

Error messages never echo the rejected input, internal details or other tenants' data.

## Close codes and heartbeat

| Code | Meaning | Client action |
|---|---|---|
| 1000 | Normal | — |
| 1001 | Gateway shutting down | Reconnect and RESUME |
| 1011 | Transport error | Reconnect and RESUME |
| 4008 | Slow consumer: the reliable lane (512 frames) overflowed | Reconnect and RESUME |
| 4009 | History needed by the gateway was trimmed (resync) | Reconnect; RESUME, or SUBSCRIBE after `RECOVERY_REQUIRED` |
| 4010 | Idle timeout: no traffic, not even a pong, for 60 s | Reconnect and RESUME |

The gateway sends a WebSocket ping every 20 s to connections without traffic; standard clients answer with a pong
automatically.

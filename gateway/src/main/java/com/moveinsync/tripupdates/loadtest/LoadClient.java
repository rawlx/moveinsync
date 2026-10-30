package com.moveinsync.tripupdates.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * One simulated screen: a real WebSocket connection with a dev token and one subscription. It behaves like the
 * reference client of the protocol: deduplicates reliable events by sequence, keeps the last processed reliable
 * sequence, and after losing its connection reconnects to the next gateway (round robin, backoff) and sends
 * {@code RESUME} from that sequence.
 *
 * <p>Callbacks of one connection run sequentially; state shared with the engine's reporting thread is guarded by
 * this object's monitor.
 */
final class LoadClient implements WebSocket.Listener {

    enum State { NEW, CONNECTING, SUBSCRIBED, RECOVERING, FAILED, CLOSED }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_INITIAL_ATTEMPTS = 3;

    private final ClientPlan.ClientSpec spec;
    private final LoadTest engine;
    private final StringBuilder partial = new StringBuilder();
    private State state = State.NEW;
    private WebSocket socket;
    private int gateway;
    private int attempts;
    private boolean everSubscribed;
    private boolean closingByUs;
    private long startSeq = -1;
    private long lastSeq = -1;
    private final LongList received = new LongList();
    private long duplicates;
    private long locations;
    private long tenantViolations;
    private long disconnectNanos;
    private int reconnects;

    LoadClient(ClientPlan.ClientSpec spec, LoadTest engine) {
        this.spec = spec;
        this.engine = engine;
        this.gateway = spec.gateway();
    }

    ClientPlan.ClientSpec spec() {
        return spec;
    }

    synchronized State state() {
        return state;
    }

    synchronized boolean everSubscribed() {
        return everSubscribed;
    }

    synchronized long startSeq() {
        return startSeq;
    }

    synchronized long[] receivedSeqs() {
        return received.toArray();
    }

    synchronized long duplicates() {
        return duplicates;
    }

    synchronized long locations() {
        return locations;
    }

    synchronized long tenantViolations() {
        return tenantViolations;
    }

    synchronized int reconnects() {
        return reconnects;
    }

    synchronized int gateway() {
        return gateway;
    }

    void connect() {
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            if (state != State.RECOVERING) {
                state = State.CONNECTING;
            }
            attempts++;
        }
        URI uri = URI.create(engine.gatewayUri(gateway()));
        engine.httpClient(spec.id()).newWebSocketBuilder()
                .header("Authorization", "Bearer " + spec.token())
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(uri, this)
                .whenComplete((ws, error) -> {
                    if (error != null) {
                        connectFailed(error);
                    }
                });
    }

    /** Closes the connection on purpose (end of test, or a probe that got its answer); no reconnect follows. */
    void close() {
        WebSocket current;
        synchronized (this) {
            closingByUs = true;
            if (state != State.FAILED) {
                state = State.CLOSED;
            }
            current = socket;
        }
        if (current != null) {
            current.sendClose(WebSocket.NORMAL_CLOSURE, "done").exceptionally(e -> null);
            current.abort();
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        String first;
        synchronized (this) {
            socket = webSocket;
            first = startSeq < 0
                    ? "{\"type\":\"SUBSCRIBE\",\"id\":\"s" + spec.id() + "\",\"topic\":\"" + spec.topic().name() + "\"}"
                    : "{\"type\":\"RESUME\",\"id\":\"r" + spec.id() + "\",\"topic\":\"" + spec.topic().name()
                            + "\",\"lastSeq\":" + lastSeq + "}";
        }
        webSocket.sendText(first, true);
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            String text = partial.toString();
            partial.setLength(0);
            try {
                handle(MAPPER.readTree(text), System.nanoTime());
            } catch (Exception e) {
                engine.error("CLIENT_PARSE_ERROR");
            }
        }
        webSocket.request(1);
        return null;
    }

    private void handle(JsonNode message, long now) {
        switch (message.path("type").asText()) {
            case "SUBSCRIBED" -> {
                synchronized (this) {
                    startSeq = message.path("seq").asLong();
                    lastSeq = startSeq;
                    state = State.SUBSCRIBED;
                    everSubscribed = true;
                    attempts = 0;
                }
                engine.subscribed(this);
            }
            case "RESUME_COMPLETE" -> {
                long outage;
                synchronized (this) {
                    state = State.SUBSCRIBED;
                    attempts = 0;
                    outage = now - disconnectNanos;
                }
                engine.recovered(this, outage);
            }
            case "EVENT" -> onEvent(message, now);
            case "ERROR" -> {
                String code = message.path("code").asText();
                if (spec.kind() == ClientPlan.Kind.PROBE) {
                    engine.probeAnswered(this, code);
                } else {
                    engine.error("ERROR_" + code);
                }
            }
            default -> engine.error("CLIENT_UNKNOWN_FRAME");
        }
    }

    private void onEvent(JsonNode message, long now) {
        String type = message.path("event").path("type").asText();
        long seq = message.path("seq").asLong();
        boolean sos;
        boolean recovering;
        int connectedTo;
        synchronized (this) {
            if (!spec.tenantId().equals(message.path("event").path("tenantId").asText())) {
                tenantViolations++;
            }
            if ("LOCATION".equals(type)) {
                locations++;
                engine.delivered();
                return;
            }
            if (seq <= lastSeq) {
                duplicates++;
                return;
            }
            lastSeq = seq;
            received.add(seq);
            recovering = state == State.RECOVERING;
            connectedTo = gateway;
            sos = "SOS".equals(type) && spec.kind() == ClientPlan.Kind.SECURITY_SOS;
        }
        engine.delivered();
        if (sos) {
            engine.sosReceived(message.path("event").path("eventId").asText(), spec.id(), connectedTo, now,
                    recovering);
        }
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        // 1006 abnormal closure, 1001 going away, 1011 server error: the gateway itself is in trouble.
        lost(webSocket, "close " + statusCode, statusCode == 1006 || statusCode == 1001 || statusCode == 1011);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        lost(webSocket, "error " + error.getClass().getSimpleName(), true);
    }

    /** @param gatewayFailure the loss says the gateway is unhealthy (not a protocol close such as 4008/4010) */
    private void lost(WebSocket webSocket, String why, boolean gatewayFailure) {
        int from;
        synchronized (this) {
            if (socket != webSocket || closingByUs || engine.stopping()) {
                return; // an old connection, or we are shutting down
            }
            socket = null;
            if (!everSubscribed) {
                // Never subscribed: treat like a failed connect attempt.
                state = State.CONNECTING;
            } else if (state != State.RECOVERING) {
                state = State.RECOVERING;
                disconnectNanos = System.nanoTime();
                reconnects++;
            }
            from = gateway;
        }
        if (gatewayFailure) {
            engine.gatewayFailed(from, why);
        }
        engine.disconnected(this, why);
        scheduleReconnect();
    }

    private void connectFailed(Throwable error) {
        boolean giveUp;
        int target;
        synchronized (this) {
            giveUp = !everSubscribed && attempts >= MAX_INITIAL_ATTEMPTS;
            if (giveUp) {
                state = State.FAILED;
            }
            target = gateway;
        }
        engine.gatewayFailed(target, "connect failed");
        engine.connectFailed(this, error);
        if (!giveUp) {
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        long delay;
        synchronized (this) {
            if (state == State.CLOSED || state == State.FAILED || engine.stopping()) {
                return;
            }
            // Spread over the healthy survivors by client id; never straight back to the gateway just lost.
            gateway = GatewaySelector.pick(spec.id(), gateway, engine.healthyGateways(), engine.gatewayCount());
            delay = Math.min(5_000, 200L * Math.max(1, attempts)) + (spec.id() % 200);
        }
        engine.scheduler().schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }
}

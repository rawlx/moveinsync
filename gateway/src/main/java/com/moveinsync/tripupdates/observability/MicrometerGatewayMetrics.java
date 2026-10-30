package com.moveinsync.tripupdates.observability;

import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.gateway.ClientOutbox;
import com.moveinsync.tripupdates.gateway.ClientSession;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.GatewayMetrics;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 * Micrometer meters for the on-call view (lag, queue depth, rejections, failures, retries, recovery). Cardinality is
 * bounded by construction: every tag value comes from an enum or a boolean, never from tenant, trip, cab, topic or
 * session identifiers.
 *
 * <pre>
 * tripupdates.delivery.lag{type}                       Timer, publication (Redis clock) → written to client
 * tripupdates.events.delivered{type}                   (count of the timer)
 * tripupdates.outbox.offers{result}                    CONFLATED | LOCATION_EVICTED | OVERFLOW
 * tripupdates.outbox.depth{lane}                       Gauge, sum over sessions (reliable | location)
 * tripupdates.outbox.depth.max{lane}                   Gauge, deepest single session
 * tripupdates.sessions.opened / .closed{reason}        Counters; tripupdates.sessions.active Gauge
 * tripupdates.feeds.active                             Gauge, locally subscribed topics
 * tripupdates.subscriptions.requests{request,outcome}  SUBSCRIBE | RESUME × outcome (rejections, recovery)
 * tripupdates.subscriptions.revoked                    Counter
 * tripupdates.feed.read.failures / .resyncs            Counters (retried via reconciliation / client resync)
 * tripupdates.reconciliations                          Counter
 * </pre>
 */
public final class MicrometerGatewayMetrics implements GatewayMetrics {

    private final MeterRegistry registry;
    private final Counter sessionsOpened;
    private final Map<CloseReason, Counter> sessionsClosed = new EnumMap<>(CloseReason.class);
    private final Map<EventType, Timer> lag = new EnumMap<>(EventType.class);
    private final Map<ClientOutbox.OfferResult, Counter> offers = new EnumMap<>(ClientOutbox.OfferResult.class);
    private final Counter feedReadFailures;
    private final Counter feedResyncs;
    private final Counter reconciliations;
    private final Counter revoked;
    private final Map<SubscriptionOutcome, Counter> subscribeRequests = new EnumMap<>(SubscriptionOutcome.class);
    private final Map<SubscriptionOutcome, Counter> resumeRequests = new EnumMap<>(SubscriptionOutcome.class);

    public MicrometerGatewayMetrics(MeterRegistry registry) {
        this.registry = registry;
        sessionsOpened = Counter.builder("tripupdates.sessions.opened").register(registry);
        for (CloseReason reason : CloseReason.values()) {
            sessionsClosed.put(reason, Counter.builder("tripupdates.sessions.closed")
                    .tag("reason", reason.name()).register(registry));
        }
        for (EventType type : EventType.values()) {
            lag.put(type, Timer.builder("tripupdates.delivery.lag")
                    .description("Time from publication in Redis until the event was written to the client")
                    .tag("type", type.name())
                    // Buckets only; quantiles are computed from them at query time (histogram_quantile). Client-side
                    // percentiles were dropped: their HdrHistogram recorder synchronises internally and pinned the
                    // virtual-thread writers that record into it (seen with -Djdk.tracePinnedThreads under load).
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofSeconds(30))
                    .register(registry));
        }
        for (ClientOutbox.OfferResult result : new ClientOutbox.OfferResult[] {ClientOutbox.OfferResult.CONFLATED,
                ClientOutbox.OfferResult.LOCATION_EVICTED, ClientOutbox.OfferResult.OVERFLOW}) {
            offers.put(result, Counter.builder("tripupdates.outbox.offers").tag("result", result.name())
                    .register(registry));
        }
        feedReadFailures = Counter.builder("tripupdates.feed.read.failures").register(registry);
        feedResyncs = Counter.builder("tripupdates.feed.resyncs").register(registry);
        reconciliations = Counter.builder("tripupdates.reconciliations").register(registry);
        revoked = Counter.builder("tripupdates.subscriptions.revoked").register(registry);
        for (SubscriptionOutcome outcome : SubscriptionOutcome.values()) {
            subscribeRequests.put(outcome, requestCounter("SUBSCRIBE", outcome));
            resumeRequests.put(outcome, requestCounter("RESUME", outcome));
        }
    }

    /** Registers the gauges that read live state from the registry (evaluated on scrape, O(sessions)). */
    public void bind(SubscriptionRegistry subscriptions) {
        Gauge.builder("tripupdates.sessions.active", subscriptions, r -> r.sessions().size()).register(registry);
        Gauge.builder("tripupdates.feeds.active", subscriptions, r -> r.feeds().size()).register(registry);
        depthGauges(subscriptions, "reliable", session -> session.outbox().reliableDepth());
        depthGauges(subscriptions, "location", session -> session.outbox().locationDepth());
    }

    @Override
    public void sessionOpened() {
        sessionsOpened.increment();
    }

    @Override
    public void sessionClosed(CloseReason reason) {
        sessionsClosed.get(reason).increment();
    }

    @Override
    public void eventDelivered(EventType type, Duration delay) {
        lag.get(type).record(delay);
    }

    @Override
    public void outboxOffer(ClientOutbox.OfferResult result) {
        Counter counter = offers.get(result);
        if (counter != null) {
            counter.increment();
        }
    }

    @Override
    public void feedReadFailed() {
        feedReadFailures.increment();
    }

    @Override
    public void feedResynced() {
        feedResyncs.increment();
    }

    @Override
    public void reconciliationRun() {
        reconciliations.increment();
    }

    @Override
    public void subscriptionRequest(boolean resume, SubscriptionOutcome outcome) {
        (resume ? resumeRequests : subscribeRequests).get(outcome).increment();
    }

    @Override
    public void subscriptionRevoked() {
        revoked.increment();
    }

    private Counter requestCounter(String request, SubscriptionOutcome outcome) {
        return Counter.builder("tripupdates.subscriptions.requests")
                .tag("request", request).tag("outcome", outcome.name()).register(registry);
    }

    private void depthGauges(SubscriptionRegistry subscriptions, String lane, ToIntFunction<ClientSession> depth) {
        Gauge.builder("tripupdates.outbox.depth", subscriptions,
                        r -> r.sessions().stream().mapToInt(depth).sum())
                .tag("lane", lane).register(registry);
        Gauge.builder("tripupdates.outbox.depth.max", subscriptions,
                        r -> r.sessions().stream().mapToInt(depth).max().orElse(0))
                .tag("lane", lane).register(registry);
    }
}

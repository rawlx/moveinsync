package com.moveinsync.tripupdates.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.ClientOutbox;
import com.moveinsync.tripupdates.gateway.ClientSession;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.GatewayEvents;
import com.moveinsync.tripupdates.gateway.GatewayMetrics;
import com.moveinsync.tripupdates.gateway.InMemoryEventStore;
import com.moveinsync.tripupdates.gateway.RecordingTransport;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class MicrometerGatewayMetricsTest {

    /** Our bounded tags only (no {@code phi}: client-side percentiles are not published). */
    private static final Set<String> ALLOWED_TAG_KEYS =
            Set.of("type", "reason", "result", "lane", "request", "outcome");

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final MicrometerGatewayMetrics metrics = new MicrometerGatewayMetrics(meters);

    @Test
    void countsAndTimesWithBoundedTags() {
        metrics.sessionOpened();
        metrics.sessionClosed(CloseReason.SLOW_CONSUMER);
        metrics.eventDelivered(EventType.SOS, Duration.ofMillis(12));
        metrics.eventDelivered(EventType.LOCATION, Duration.ofMillis(3));
        metrics.outboxOffer(ClientOutbox.OfferResult.CONFLATED);
        metrics.outboxOffer(ClientOutbox.OfferResult.OVERFLOW);
        metrics.outboxOffer(ClientOutbox.OfferResult.QUEUED); // not a tracked result: ignored
        metrics.feedReadFailed();
        metrics.feedResynced();
        metrics.reconciliationRun();
        metrics.subscriptionRequest(true, GatewayMetrics.SubscriptionOutcome.RECOVERY_REQUIRED);
        metrics.subscriptionRequest(false, GatewayMetrics.SubscriptionOutcome.FORBIDDEN);
        metrics.subscriptionRevoked();

        assertThat(meters.get("tripupdates.sessions.opened").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.sessions.closed").tag("reason", "SLOW_CONSUMER").counter().count())
                .isEqualTo(1);
        assertThat(meters.get("tripupdates.delivery.lag").tag("type", "SOS").timer().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.outbox.offers").tag("result", "OVERFLOW").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.subscriptions.requests").tag("request", "RESUME")
                .tag("outcome", "RECOVERY_REQUIRED").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.subscriptions.requests").tag("request", "SUBSCRIBE")
                .tag("outcome", "FORBIDDEN").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.subscriptions.revoked").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.feed.read.failures").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.feed.resyncs").counter().count()).isEqualTo(1);
        assertThat(meters.get("tripupdates.reconciliations").counter().count()).isEqualTo(1);
    }

    @Test
    void deliveryLagPublishesHistogramBucketsButNoClientSidePercentiles() {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerGatewayMetrics prometheusMetrics = new MicrometerGatewayMetrics(prometheus);
        prometheusMetrics.eventDelivered(EventType.SOS, Duration.ofMillis(12));
        prometheusMetrics.eventDelivered(EventType.SOS, Duration.ofMillis(800));

        String scrape = prometheus.scrape();
        assertThat(scrape).contains("tripupdates_delivery_lag_seconds_bucket{");
        assertThat(scrape).contains("tripupdates_delivery_lag_seconds_count{type=\"SOS\"} 2");
        assertThat(scrape).doesNotContain("quantile=");
        assertThat(prometheus.get("tripupdates.delivery.lag").tag("type", "SOS").timer().takeSnapshot()
                .percentileValues()).isEmpty();
    }

    @Test
    void gaugesReadLiveQueueDepthsAndNeverUseIdentifiersAsTags() {
        SubscriptionRegistry registry = new SubscriptionRegistry(new InMemoryEventStore(100), 100, Runnable::run,
                16, 16, metrics);
        metrics.bind(registry);
        RecordingTransport frozen = RecordingTransport.gated();
        ClientSession session = registry.open("session-secret-id", "tenant-secret", frozen);
        session.outbox().offer(GatewayEvents.sequenced(GatewayEvents.status("e1"), TopicId.trip("T-1"), 1));
        session.outbox().offer(GatewayEvents.sequenced(GatewayEvents.status("e2"), TopicId.trip("T-1"), 2));
        session.outbox().offer(GatewayEvents.sequenced(GatewayEvents.location("l1", "C-1", 1), TopicId.cab("C-1"), 3));

        assertThat(meters.get("tripupdates.sessions.active").gauge().value()).isEqualTo(1);
        // The writer holds one reliable event while blocked in send; the other is still queued.
        assertThat(meters.get("tripupdates.outbox.depth").tag("lane", "reliable").gauge().value())
                .isBetween(1.0, 2.0);
        assertThat(meters.get("tripupdates.outbox.depth.max").tag("lane", "location").gauge().value())
                .isBetween(0.0, 1.0);

        for (Meter meter : meters.getMeters()) {
            Set<String> keys = meter.getId().getTags().stream().map(Tag::getKey).collect(Collectors.toSet());
            assertThat(keys).as(meter.getId().getName()).isSubsetOf(ALLOWED_TAG_KEYS);
            assertThat(meter.getId().getTags()).extracting(Tag::getValue)
                    .noneMatch(value -> value.contains("secret") || value.contains("T-1") || value.contains("C-1"));
        }
        session.close(CloseReason.NORMAL);
    }
}

package com.moveinsync.tripupdates.loadtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LoadTestMeasurementTest {

    @Test
    void nearestRankPercentiles() {
        long[] sorted = IntStream.rangeClosed(1, 100).mapToLong(i -> i).toArray();
        assertThat(LatencyRecorder.percentile(sorted, 50)).isEqualTo(50);
        assertThat(LatencyRecorder.percentile(sorted, 95)).isEqualTo(95);
        assertThat(LatencyRecorder.percentile(sorted, 99)).isEqualTo(99);
        assertThat(LatencyRecorder.percentile(sorted, 100)).isEqualTo(100);
        assertThat(LatencyRecorder.percentile(sorted, 0)).isEqualTo(1);
        assertThat(LatencyRecorder.percentile(new long[] {7}, 99)).isEqualTo(7);
        assertThat(LatencyRecorder.percentile(new long[] {1, 2, 3}, 50)).isEqualTo(2);
        assertThatThrownBy(() -> LatencyRecorder.percentile(new long[0], 50)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void recorderSummarisesInMillisecondsAndCountsSamplesOverTheThreshold() {
        LatencyRecorder recorder = new LatencyRecorder(2_000);
        for (int ms : new int[] {5, 10, 15, 2_500, 20}) {
            recorder.record(TimeUnit.MILLISECONDS.toNanos(ms));
        }
        recorder.record(-5); // clock anomaly clamps to zero instead of producing a negative latency
        LatencyRecorder.Summary summary = recorder.summary();
        assertThat(summary.count()).isEqualTo(6);
        assertThat(summary.minMs()).isZero();
        assertThat(summary.p50Ms()).isEqualTo(10.0);
        assertThat(summary.p99Ms()).isEqualTo(2_500.0);
        assertThat(summary.maxMs()).isEqualTo(2_500.0);
        assertThat(summary.overThreshold()).isEqualTo(1);
        assertThat(new LatencyRecorder(2_000).summary().count()).isZero();
    }

    @Test
    void recorderIsSafeUnderConcurrentRecording() throws InterruptedException {
        LatencyRecorder recorder = new LatencyRecorder(2_000);
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = Thread.ofPlatform().start(() -> {
                for (int i = 0; i < 10_000; i++) {
                    recorder.record(i);
                }
            });
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(recorder.summary().count()).isEqualTo(80_000);
    }

    @Test
    void sequenceVerifierCountsMissingAndUnexpectedAfterTheStartPosition() {
        long[] published = {5, 10, 15, 20, 25};
        assertThat(SequenceVerifier.verify(10, published, new long[] {15, 20, 25}))
                .isEqualTo(new SequenceVerifier.Result(3, 3, 0, 0));
        assertThat(SequenceVerifier.verify(10, published, new long[] {15, 25}))
                .isEqualTo(new SequenceVerifier.Result(3, 2, 1, 0));
        assertThat(SequenceVerifier.verify(10, published, new long[] {15, 18, 20, 25}))
                .as("an event that was never published").isEqualTo(new SequenceVerifier.Result(3, 4, 0, 1));
        assertThat(SequenceVerifier.verify(0, published, new long[0]))
                .isEqualTo(new SequenceVerifier.Result(5, 0, 5, 0));
        assertThat(SequenceVerifier.verify(30, published, new long[0]).complete()).isTrue();
    }

    @Test
    void sosTrackerSeparatesLiveFromRecoveredAndCountsMissingAndDuplicates() {
        java.time.Instant anchor = java.time.Instant.parse("2026-10-30T10:00:00Z");
        long start = 1_000_000_000L; // arbitrary monotonic origin
        SosTracker tracker = new SosTracker(2_000, start, anchor);
        tracker.publishing("s1", start, 2);
        tracker.publishing("s2", start + TimeUnit.SECONDS.toNanos(10), 2);
        tracker.publishing("s3", start, 1);
        tracker.publishFailed("s3");
        tracker.received("s1", 1, 0, start + TimeUnit.MILLISECONDS.toNanos(30), false);
        tracker.received("s1", 2, 1, start + TimeUnit.MILLISECONDS.toNanos(40), false);
        tracker.received("s1", 2, 1, start + TimeUnit.MILLISECONDS.toNanos(90), false); // duplicate to client 2
        tracker.received("s2", 1, 2, start + TimeUnit.SECONDS.toNanos(14), true);         // after a reconnect
        tracker.received("unknown", 1, 0, 1, false);

        SosTracker.Summary summary = tracker.summary();
        assertThat(summary.published()).isEqualTo(2);
        assertThat(summary.publishFailures()).isEqualTo(1);
        assertThat(summary.expectedDeliveries()).isEqualTo(4);
        assertThat(summary.delivered()).isEqualTo(3);
        assertThat(summary.missing()).isEqualTo(1);
        assertThat(summary.duplicateDeliveries()).isEqualTo(1);
        assertThat(summary.live().count()).isEqualTo(2);
        assertThat(summary.recovered().count()).isEqualTo(1);
        assertThat(summary.recovered().overThreshold()).isEqualTo(1);
        assertThat(summary.all().count()).isEqualTo(3);
        // Only the 4 s delivery exceeds 2 s; it is kept individually with monotonic offsets and derived UTC times.
        assertThat(summary.slowDeliveries()).containsExactly(new SosTracker.SlowDelivery("s2", 1, 2, 10.0, 14.0,
                4_000.0, true, "2026-10-30T10:00:10Z", "2026-10-30T10:00:14Z"));
    }

    @Test
    void reconnectsSpreadDeterministicallyOverHealthySurvivorsOnly() {
        java.util.Set<Integer> allHealthy = java.util.Set.of(0, 1, 2);
        java.util.Set<Integer> withoutTwo = java.util.Set.of(0, 2);
        int[] perTarget = new int[3];
        for (int id = 1; id < 10_000; id += 3) { // the clients of gateway 1
            int target = GatewaySelector.pick(id, 1, withoutTwo, 3);
            assertThat(target).isNotEqualTo(1);
            assertThat(GatewaySelector.pick(id, 1, withoutTwo, 3)).as("deterministic").isEqualTo(target);
            perTarget[target]++;
        }
        assertThat(perTarget[1]).isZero();
        assertThat(Math.abs(perTarget[0] - perTarget[2])).as("balanced: %d vs %d", perTarget[0], perTarget[2])
                .isLessThanOrEqualTo(1);

        // Never back to the lost gateway while another one exists, even if it is still reported healthy.
        assertThat(GatewaySelector.pick(4, 1, allHealthy, 3)).isNotEqualTo(1);
        // An unhealthy survivor is skipped.
        for (int id = 0; id < 50; id++) {
            assertThat(GatewaySelector.pick(id, 1, java.util.Set.of(2), 3)).isEqualTo(2);
        }
        // Nothing known healthy: still try another gateway (the attempt itself is the check).
        assertThat(GatewaySelector.pick(7, 1, java.util.Set.of(), 3)).isIn(0, 2);
        // A single gateway can only reconnect to itself.
        assertThat(GatewaySelector.pick(7, 0, java.util.Set.of(), 1)).isZero();
    }

    @Test
    void failedGatewayBecomesEligibleOnlyAfterConsecutiveSuccessfulProbes() {
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        GatewayHealth health = new GatewayHealth(3, clock::get);
        clock.set(TimeUnit.SECONDS.toNanos(100));
        health.markDown(1, "close 1006");
        health.markDown(1, "close 1006"); // repeated reports do not add transitions
        assertThat(health.healthy()).containsExactly(0, 2);

        health.probeResult(1, true);
        assertThat(health.isUp(1)).as("one success is not enough").isFalse();
        health.probeResult(1, false);
        health.probeResult(1, true);
        assertThat(health.isUp(1)).as("a failure resets the count").isFalse();
        clock.set(TimeUnit.SECONDS.toNanos(130));
        health.probeResult(1, true);
        assertThat(health.isUp(1)).isTrue();
        health.probeResult(0, false); // probes of a healthy gateway are ignored
        assertThat(health.healthy()).containsExactly(0, 1, 2);
        assertThat(health.transitions()).extracting(GatewayHealth.Transition::state, GatewayHealth.Transition::atSeconds)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("DOWN", 100.0),
                        org.assertj.core.groups.Tuple.tuple("UP", 130.0));
    }

    @Test
    void histogramQuantilesInterpolateWithinPrometheusBuckets() {
        String body = """
                tripupdates_delivery_lag_seconds_bucket{node="g",type="SOS",le="0.01"} 50.0
                tripupdates_delivery_lag_seconds_bucket{node="g",type="SOS",le="0.1"} 90.0
                tripupdates_delivery_lag_seconds_bucket{node="g",type="SOS",le="1.0"} 100.0
                tripupdates_delivery_lag_seconds_bucket{node="g",type="SOS",le="+Inf"} 100.0
                tripupdates_delivery_lag_seconds_bucket{node="g",type="LOCATION",le="0.01"} 1.0
                tripupdates_delivery_lag_seconds_bucket{node="g",type="LOCATION",le="+Inf"} 1.0
                """;
        assertThat(GatewayProbe.histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\"", 0.5))
                .isCloseTo(0.01, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(GatewayProbe.histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\"", 0.7))
                .isCloseTo(0.055, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(GatewayProbe.histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\"", 0.99))
                .isCloseTo(0.91, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(GatewayProbe.histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"DELAY\"", 0.99))
                .as("no observations").isZero();
    }

    @Test
    void prometheusSumsSelectByNameAndLabels() {
        String body = """
                # HELP tripupdates_sessions_active
                tripupdates_sessions_active{node="gateway-1"} 3300.0
                tripupdates_sessions_active_extra 9.0
                jvm_memory_used_bytes{area="heap",id="G1 Eden Space",node="gateway-1"} 100.0
                jvm_memory_used_bytes{area="heap",id="G1 Old Gen",node="gateway-1"} 50.0
                jvm_memory_used_bytes{area="nonheap",id="Metaspace",node="gateway-1"} 70.0
                tripupdates_delivery_lag_seconds{node="gateway-1",type="SOS",quantile="0.99"} 0.012
                tripupdates_delivery_lag_seconds{node="gateway-1",type="LOCATION",quantile="0.99"} NaN
                """;
        assertThat(GatewayProbe.sum(body, "tripupdates_sessions_active", null)).isEqualTo(3300.0);
        assertThat(GatewayProbe.sum(body, "jvm_memory_used_bytes", "area=\"heap\"")).isEqualTo(150.0);
        assertThat(GatewayProbe.sum(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\",quantile=\"0.99\""))
                .isEqualTo(0.012);
        assertThat(GatewayProbe.sum(body, "tripupdates_delivery_lag_seconds", "type=\"LOCATION\"")).isZero();
        assertThat(GatewayProbe.sum(body, "missing_metric", null)).isZero();
    }

    @Test
    void configReadsEnvironmentWithDefaults() {
        LoadTestConfig defaults = LoadTestConfig.from(java.util.Map.of());
        assertThat(defaults.clients()).isEqualTo(10_000);
        assertThat(defaults.cabs()).isEqualTo(2_000);
        assertThat(defaults.gateways()).hasSize(3);
        LoadTestConfig custom = LoadTestConfig.from(java.util.Map.of("LOADTEST_CLIENTS", "40",
                "LOADTEST_GATEWAYS", "ws://a/ws, ws://b/ws", "LOADTEST_SOS_PER_SECOND", "0.5"));
        assertThat(custom.clients()).isEqualTo(40);
        assertThat(custom.gateways()).containsExactly("ws://a/ws", "ws://b/ws");
        assertThat(custom.sosPerSecond()).isEqualTo(0.5);
    }
}

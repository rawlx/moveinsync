package com.moveinsync.tripupdates.loadtest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end SOS latency: the publish time is taken with {@link System#nanoTime()} immediately before
 * {@code EventPublisher.publish} in this JVM, and the receive time with the same clock when a security client of this
 * JVM parses the frame. One monotonic clock, so no cross-process synchronisation is involved; the measurement
 * includes routing, the Redis append, the notification, the gateway's stream read, queueing, the WebSocket write,
 * the Docker network and the client's frame parsing.
 *
 * <p>Wall-clock times in the report are derived as {@code anchorUtc + (nanos - anchorNanos)} from one anchor taken at
 * the start of the test, so they can be correlated with external events (the orchestration script's kill time, taken
 * from the host clock; Docker Desktop keeps the VM clock synchronised with it, typically to milliseconds, which is an
 * assumption, not a measurement).
 *
 * <p>Deliveries that arrive while the receiving client is recovering (between losing its gateway and
 * {@code RESUME_COMPLETE}) are recorded separately: their latency includes the outage and reconnect. Every delivery
 * above the target is kept individually (ids and timings only, no payload).
 */
final class SosTracker {

    /**
     * One delivery over the target. Times in seconds since the test start (monotonic) and as derived UTC.
     *
     * @param gateway index of the gateway the receiving client was connected to when it received the event
     */
    record SlowDelivery(String eventId, int clientId, int gateway, double publishedAtSeconds, double receivedAtSeconds,
            double latencyMs, boolean recovering, String publishedAtUtc, String receivedAtUtc) {
    }

    record Summary(long published, long publishFailures, long expectedDeliveries, long delivered, long missing,
            long duplicateDeliveries, LatencyRecorder.Summary live, LatencyRecorder.Summary recovered,
            LatencyRecorder.Summary all, List<SlowDelivery> slowDeliveries) {
    }

    private record Sent(long nanos, int expectedRecipients) {
    }

    private final long targetNanos;
    private final long anchorNanos;
    private final Instant anchorUtc;
    private final ConcurrentHashMap<String, Sent> sent = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Integer>> receivedBy = new ConcurrentHashMap<>();
    private final LatencyRecorder live;
    private final LatencyRecorder recovered;
    private final LatencyRecorder all;
    private final List<SlowDelivery> slow = new ArrayList<>();
    private final AtomicLong publishFailures = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    SosTracker(long targetMillis, long anchorNanos, Instant anchorUtc) {
        this.targetNanos = targetMillis * 1_000_000;
        this.anchorNanos = anchorNanos;
        this.anchorUtc = anchorUtc;
        live = new LatencyRecorder(targetMillis);
        recovered = new LatencyRecorder(targetMillis);
        all = new LatencyRecorder(targetMillis);
    }

    /** Call immediately before publishing; {@code expectedRecipients} = subscribed security clients of the office. */
    void publishing(String eventId, long nanos, int expectedRecipients) {
        sent.put(eventId, new Sent(nanos, expectedRecipients));
    }

    void publishFailed(String eventId) {
        sent.remove(eventId);
        publishFailures.incrementAndGet();
    }

    void received(String eventId, int clientId, int gateway, long nanos, boolean recovering) {
        Sent original = sent.get(eventId);
        if (original == null) {
            return; // not one of ours (e.g. an earlier run's history)
        }
        if (!receivedBy.computeIfAbsent(eventId, id -> ConcurrentHashMap.newKeySet()).add(clientId)) {
            duplicates.incrementAndGet();
            return;
        }
        long latency = nanos - original.nanos();
        (recovering ? recovered : live).record(latency);
        all.record(latency);
        if (latency > targetNanos) {
            synchronized (slow) {
                slow.add(new SlowDelivery(eventId, clientId, gateway, seconds(original.nanos()), seconds(nanos),
                        Math.round(latency / 1e4) / 100.0, recovering, utc(original.nanos()), utc(nanos)));
            }
        }
    }

    Summary summary() {
        long expected = 0;
        long delivered = 0;
        long missing = 0;
        for (var entry : sent.entrySet()) {
            int got = receivedBy.getOrDefault(entry.getKey(), Set.of()).size();
            expected += entry.getValue().expectedRecipients();
            delivered += got;
            missing += Math.max(0, entry.getValue().expectedRecipients() - got);
        }
        List<SlowDelivery> slowCopy;
        synchronized (slow) {
            slowCopy = new ArrayList<>(slow);
        }
        slowCopy.sort(Comparator.comparingDouble(SlowDelivery::publishedAtSeconds));
        return new Summary(sent.size(), publishFailures.get(), expected, delivered, missing, duplicates.get(),
                live.summary(), recovered.summary(), all.summary(), List.copyOf(slowCopy));
    }

    private double seconds(long nanos) {
        return Math.round((nanos - anchorNanos) / 1e6) / 1000.0;
    }

    private String utc(long nanos) {
        return anchorUtc.plusNanos(nanos - anchorNanos).toString();
    }
}

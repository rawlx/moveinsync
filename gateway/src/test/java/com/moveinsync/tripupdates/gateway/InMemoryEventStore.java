package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.EventStore;
import com.moveinsync.tripupdates.store.EventStoreUnavailableException;
import com.moveinsync.tripupdates.store.TopicHistory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double with the same observable contract as {@code RedisEventStore} (per-topic sequences starting above a
 * base, bounded log with floor, gap reporting, latest location per cab, idempotent ordered appends). The Redis
 * implementation itself is covered by {@code RedisEventStoreIT}.
 */
public final class InMemoryEventStore implements EventStore {

    public static final long BASE = 1_000;

    private static final class TopicState {
        long seq = BASE;
        long floor = BASE;
        final TreeMap<Long, SequencedEvent> log = new TreeMap<>();
        final Map<String, SequencedEvent> locations = new HashMap<>();
    }

    private final int logMax;
    private final Map<TenantTopic, TopicState> topics = new HashMap<>();
    private final Map<String, AppendResult> appended = new HashMap<>();
    private final AtomicInteger reads = new AtomicInteger();
    private volatile boolean failing;

    public InMemoryEventStore(int logMax) {
        this.logMax = logMax;
    }

    public void failing(boolean failing) {
        this.failing = failing;
    }

    /** Number of readAfter / latestLocation calls. */
    public int reads() {
        return reads.get();
    }

    public synchronized int appendCount() {
        return appended.size();
    }

    @Override
    public synchronized AppendResult append(TripEvent event, List<TopicId> targets) {
        checkAvailable();
        String key = event.tenantId() + "/" + event.eventId();
        if (!event.type().isConflatable() && appended.containsKey(key)) {
            return new AppendResult(appended.get(key).events(), true);
        }
        Instant now = Instant.now();
        List<SequencedEvent> events = new ArrayList<>();
        for (TopicId topic : targets) {
            TopicState state = topics.computeIfAbsent(new TenantTopic(event.tenantId(), topic), t -> new TopicState());
            SequencedEvent sequenced = new SequencedEvent(event, topic, ++state.seq, now);
            if (event.type().isConflatable()) {
                state.locations.put(event.cabId(), sequenced);
            } else {
                state.log.put(sequenced.seq(), sequenced);
                while (state.log.size() > logMax) {
                    state.floor = state.log.pollFirstEntry().getKey();
                }
            }
            events.add(sequenced);
        }
        AppendResult result = new AppendResult(events, false);
        appended.put(key, result);
        return result;
    }

    @Override
    public synchronized TopicHistory readAfter(TenantTopic topic, long afterSeq, int limit) {
        reads.incrementAndGet();
        checkAvailable();
        TopicState state = topics.get(topic);
        long head = state == null ? 0 : state.seq;
        long floor = state == null ? 0 : state.floor;
        boolean gap = afterSeq < floor || afterSeq > head;
        List<SequencedEvent> events = new ArrayList<>();
        if (!gap && state != null) {
            for (SequencedEvent event : state.log.tailMap(afterSeq, false).values()) {
                if (events.size() == limit) {
                    break;
                }
                events.add(event);
            }
        }
        return new TopicHistory(topic, head, floor, gap, events);
    }

    @Override
    public synchronized long openTopic(TenantTopic topic) {
        checkAvailable();
        return topics.computeIfAbsent(topic, t -> new TopicState()).seq;
    }

    @Override
    public synchronized List<SequencedEvent> latestLocations(TenantTopic topic) {
        reads.incrementAndGet();
        checkAvailable();
        TopicState state = topics.get(topic);
        if (state == null) {
            return List.of();
        }
        List<SequencedEvent> latest = new ArrayList<>(state.locations.values());
        latest.sort(Comparator.comparingLong(SequencedEvent::seq));
        return latest;
    }

    @Override
    public synchronized Optional<SequencedEvent> latestLocation(TenantTopic topic, String cabId) {
        reads.incrementAndGet();
        checkAvailable();
        TopicState state = topics.get(topic);
        return state == null ? Optional.empty() : Optional.ofNullable(state.locations.get(cabId));
    }

    private void checkAvailable() {
        if (failing) {
            throw new EventStoreUnavailableException("store down (test)", null);
        }
    }
}

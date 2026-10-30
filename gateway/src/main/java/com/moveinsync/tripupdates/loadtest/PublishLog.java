package com.moveinsync.tripupdates.loadtest;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Sequence numbers of every reliable (non-location) event the load test published, per tenant topic. Thread-safe. */
final class PublishLog {

    private final ConcurrentHashMap<TenantTopic, LongList> reliableSeqs = new ConcurrentHashMap<>();

    void record(List<SequencedEvent> appended) {
        for (SequencedEvent event : appended) {
            if (!event.type().isConflatable()) {
                LongList seqs = reliableSeqs.computeIfAbsent(event.tenantTopic(), topic -> new LongList());
                synchronized (seqs) {
                    seqs.add(event.seq());
                }
            }
        }
    }

    /** Published reliable sequences of the topic, ascending. */
    long[] reliable(TenantTopic topic) {
        LongList seqs = reliableSeqs.get(topic);
        if (seqs == null) {
            return new long[0];
        }
        synchronized (seqs) {
            return seqs.toSortedArray();
        }
    }
}

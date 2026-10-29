package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One session's subscription to one tenant-scoped topic. Tracks what it has already been given, so an event is
 * handed to this subscription at most once even when it is fetched twice (duplicate notification, reconciliation,
 * replay overlapping live delivery): ordered events by topic sequence, locations by sequence per cab.
 *
 * <p>Delivery state is written only under the owning {@link TopicFeed}'s lock (deliveries for a topic are
 * serialised); {@link #deliveredSeq()} and the acknowledgement state may be read from other threads.
 */
public final class Subscription {

    private final ClientSession session;
    private final TenantTopic topic;
    private volatile long lastSeq;
    private final Map<String, Long> lastLocationSeqByCab = new HashMap<>(); // one entry per cab of the topic
    private final AtomicLong ackedSeq = new AtomicLong();

    Subscription(ClientSession session, TenantTopic topic) {
        this.session = session;
        this.topic = topic;
    }

    public ClientSession session() {
        return session;
    }

    public TenantTopic topic() {
        return topic;
    }

    /** Ordered events up to and including {@code seq} are not delivered to this subscription. */
    void startAfter(long seq) {
        lastSeq = seq;
        ackedSeq.set(seq);
    }

    long lastSeq() {
        return lastSeq;
    }

    /** Highest ordered sequence handed to the client's queue (or the start position). */
    public long deliveredSeq() {
        return lastSeq;
    }

    /** Highest sequence the client acknowledged as processed (or the start position). */
    public long ackedSeq() {
        return ackedSeq.get();
    }

    /**
     * Records a client acknowledgement. Idempotent: acknowledging the same or a lower sequence again changes nothing.
     *
     * @return false if {@code seq} was never delivered on this subscription (the acknowledgement is rejected)
     */
    public boolean acknowledge(long seq) {
        if (seq > lastSeq) {
            return false;
        }
        ackedSeq.accumulateAndGet(seq, Math::max);
        return true;
    }

    void deliver(SequencedEvent event) {
        if (event.type().isConflatable()) {
            Long last = lastLocationSeqByCab.get(event.event().cabId());
            if (last != null && event.seq() <= last) {
                return;
            }
            lastLocationSeqByCab.put(event.event().cabId(), event.seq());
        } else {
            if (event.seq() <= lastSeq) {
                return;
            }
            lastSeq = event.seq();
        }
        session.offer(event);
    }

    void resyncRequired() {
        session.close(CloseReason.RESYNC_REQUIRED);
    }
}

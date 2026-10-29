package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import java.util.HashMap;
import java.util.Map;

/**
 * One session's subscription to one tenant-scoped topic. Tracks what it has already been given, so an event is
 * handed to this subscription at most once even when it is fetched twice (duplicate notification, reconciliation).
 *
 * <p>All mutable state is guarded by the owning {@link TopicFeed}'s lock: deliveries for a topic are serialised.
 */
public final class Subscription {

    private final ClientSession session;
    private final TenantTopic topic;
    private long lastSeq;
    private final Map<String, Long> lastLocationSeqByCab = new HashMap<>();

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
    }

    long lastSeq() {
        return lastSeq;
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

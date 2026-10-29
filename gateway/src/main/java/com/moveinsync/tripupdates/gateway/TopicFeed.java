package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.store.EventStore;
import com.moveinsync.tripupdates.store.TopicHistory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers one tenant-scoped topic to this gateway's local subscriptions.
 *
 * <p><b>Notifications are hints.</b> The feed keeps a cursor (last ordered sequence it delivered). A notification
 * only says "the topic advanced"; the feed then reads every ordered event after its cursor from the durable stream
 * and delivers them in sequence order. Consequently:
 * <ul>
 *   <li>a duplicate or delayed notification (seq ≤ cursor) causes no read and no delivery;</li>
 *   <li>a missed notification is repaired by the next notification or reconciliation of the topic, because the
 *       read starts at the cursor, not at the notified sequence;</li>
 *   <li>notifications processed in any order or concurrently cannot reorder events: reads and deliveries for the
 *       topic happen under one lock, and the stream is read in sequence order.</li>
 * </ul>
 * If the stream no longer holds the history after the cursor (trimmed while this gateway was behind), the feed
 * cannot recover locally: it asks every subscription to resync (close with {@link CloseReason#RESYNC_REQUIRED}) and
 * moves its cursor to the head.
 *
 * <p>Work is coalesced: however many notifications arrive, at most one drain task per feed is queued, so the
 * pending work is bounded by the number of locally subscribed topics. Locations are processed after ordered
 * events in each drain, and only the latest location per cab is read.
 */
public final class TopicFeed {

    private static final Logger log = LoggerFactory.getLogger(TopicFeed.class);
    private static final long UNINITIALIZED = -1;

    private final TenantTopic topic;
    private final EventStore store;
    private final int pageSize;
    private final Executor executor;
    private final GatewayMetrics metrics;

    /** Serialises reads from the store and deliveries for this topic. */
    private final ReentrantLock lock = new ReentrantLock();
    private final CopyOnWriteArrayList<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private long cursor = UNINITIALIZED; // guarded by lock
    private final Map<String, Long> deliveredLocationSeqByCab = new HashMap<>(); // guarded by lock; one entry per cab
    private boolean retired; // guarded by lock

    private final Object pendingLock = new Object();
    private long pendingOrderedSeq; // guarded by pendingLock; 0 = none
    private boolean catchUpRequested; // guarded by pendingLock
    private final Map<String, Long> pendingLocationSeqByCab = new HashMap<>(); // guarded by pendingLock
    private final AtomicBoolean scheduled = new AtomicBoolean();

    TopicFeed(TenantTopic topic, EventStore store, int pageSize, Executor executor, GatewayMetrics metrics) {
        this.topic = topic;
        this.store = store;
        this.pageSize = pageSize;
        this.executor = executor;
        this.metrics = metrics;
    }

    public TenantTopic topic() {
        return topic;
    }

    /**
     * Attaches a subscription. Runs entirely under the feed lock, so no live delivery for this topic interleaves:
     * <ol>
     *   <li>The first attachment initialises the feed position to the topic's head via {@link EventStore#openTopic}
     *       (which also creates the topic's sequence base, so its creation is not mistaken for trimmed history).</li>
     *   <li>Without {@code resumeAfter} the subscription starts at the feed position (live only).</li>
     *   <li>With {@code resumeAfter} below the feed position, the ordered events in between are read from the durable
     *       stream and queued to this subscription only, in sequence order (replay).</li>
     *   <li>With {@code resumeAfter} at or above the feed position, the position is validated against the store and
     *       the subscription starts there; live events it has already processed are filtered by sequence.</li>
     *   <li>{@code confirmation} (if any) is queued after the replayed events and before any live event, then the
     *       latest location per cab is queued, then the subscription becomes live.</li>
     * </ol>
     * If the history needed for the replay is no longer retained, nothing is attached and
     * {@link AttachOutcome.HistoryUnavailable} is returned. If it disappears in the middle of a replay (trimmed
     * concurrently), the session has already received part of it and is closed with
     * {@link CloseReason#RESYNC_REQUIRED}.
     *
     * @return the outcome, or null if the feed was retired concurrently (the caller must use a fresh feed)
     */
    AttachOutcome attach(Subscription subscription, Long resumeAfter,
            Function<AttachOutcome.Attached, OutboundFrame.ControlMessage> confirmation) {
        lock.lock();
        try {
            if (retired) {
                return null;
            }
            if (cursor == UNINITIALIZED) {
                cursor = store.openTopic(topic);
            }
            long fromSeq = resumeAfter == null ? cursor : resumeAfter;
            long toSeq = cursor;
            int replayed = 0;
            if (resumeAfter != null && resumeAfter < cursor) {
                subscription.startAfter(resumeAfter);
                long at = resumeAfter;
                replay:
                while (at < cursor) {
                    TopicHistory history = store.readAfter(topic, at, pageSize);
                    if (history.gap()) {
                        if (replayed > 0) {
                            subscription.resyncRequired();
                        }
                        return new AttachOutcome.HistoryUnavailable(resumeAfter, history.floorSeq(), history.headSeq());
                    }
                    for (SequencedEvent event : history.events()) {
                        if (event.seq() > cursor) {
                            break replay; // newer than the feed position: delivered live after attaching
                        }
                        subscription.deliver(event);
                        replayed++;
                        at = event.seq();
                    }
                    if (history.events().size() < pageSize) {
                        break;
                    }
                }
            } else if (resumeAfter != null) {
                TopicHistory check = store.readAfter(topic, resumeAfter, 1);
                if (check.gap()) {
                    return new AttachOutcome.HistoryUnavailable(resumeAfter, check.floorSeq(), check.headSeq());
                }
                subscription.startAfter(resumeAfter);
                toSeq = resumeAfter;
            } else {
                subscription.startAfter(cursor);
            }
            // Read before confirming: if the store fails here, nothing has been confirmed or attached.
            List<SequencedEvent> latestLocations = store.latestLocations(topic);
            AttachOutcome.Attached attached = new AttachOutcome.Attached(fromSeq, toSeq, replayed);
            if (confirmation != null) {
                subscription.session().offerControl(confirmation.apply(attached));
            }
            for (SequencedEvent location : latestLocations) {
                subscription.deliver(location);
            }
            subscriptions.add(subscription);
            return attached;
        } finally {
            lock.unlock();
        }
    }

    /** @return true if this was the last subscription; the feed is then retired and must be discarded */
    boolean remove(Subscription subscription) {
        lock.lock();
        try {
            subscriptions.remove(subscription);
            if (subscriptions.isEmpty()) {
                retired = true;
            }
            return retired;
        } finally {
            lock.unlock();
        }
    }

    /** Retires the feed if nobody subscribed (used when initialisation failed). */
    boolean retireIfEmpty() {
        lock.lock();
        try {
            if (subscriptions.isEmpty()) {
                retired = true;
            }
            return retired;
        } finally {
            lock.unlock();
        }
    }

    public int subscriptionCount() {
        return subscriptions.size();
    }

    /** Records a notification and schedules a drain. Never blocks (safe on the Redis I/O thread). */
    void signal(long seq, EventType type, String cabId) {
        synchronized (pendingLock) {
            if (type.isConflatable()) {
                pendingLocationSeqByCab.merge(cabId, seq, Math::max);
            } else {
                pendingOrderedSeq = Math.max(pendingOrderedSeq, seq);
            }
        }
        schedule();
    }

    /** Reconciliation: read the stream from the cursor even without a notification. Never blocks. */
    void requestCatchUp() {
        synchronized (pendingLock) {
            catchUpRequested = true;
        }
        schedule();
    }

    private void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        while (true) {
            long orderedSeq;
            boolean force;
            Map<String, Long> locations;
            synchronized (pendingLock) {
                orderedSeq = pendingOrderedSeq;
                force = catchUpRequested;
                locations = pendingLocationSeqByCab.isEmpty() ? Map.of() : new HashMap<>(pendingLocationSeqByCab);
                pendingOrderedSeq = 0;
                catchUpRequested = false;
                pendingLocationSeqByCab.clear();
            }
            try {
                process(orderedSeq, force, locations);
            } catch (RuntimeException e) {
                // Store unavailable or invalid data. Ordered events are not lost: the cursor did not move past
                // them, and the next reconciliation retries. Pending locations are dropped (latest-value data).
                metrics.feedReadFailed();
                log.warn("Feed {} could not read from the event store; will retry on next reconciliation", topic, e);
                synchronized (pendingLock) {
                    catchUpRequested = true;
                }
                scheduled.set(false);
                return;
            }
            scheduled.set(false);
            synchronized (pendingLock) {
                if (pendingOrderedSeq == 0 && !catchUpRequested && pendingLocationSeqByCab.isEmpty()) {
                    return;
                }
            }
            if (!scheduled.compareAndSet(false, true)) {
                return; // another drain was scheduled concurrently and will pick the work up
            }
        }
    }

    private void process(long orderedSeq, boolean force, Map<String, Long> locations) {
        lock.lock();
        try {
            if (retired || cursor == UNINITIALIZED) {
                return;
            }
            if (force || orderedSeq > cursor) {
                catchUp();
            }
            for (Map.Entry<String, Long> location : locations.entrySet()) {
                deliverLatestLocation(location.getKey(), location.getValue());
            }
        } finally {
            lock.unlock();
        }
    }

    private void catchUp() {
        while (true) {
            TopicHistory history = store.readAfter(topic, cursor, pageSize);
            if (history.gap()) {
                log.warn("Feed {} lost history after seq {} (floor {}, head {}); resyncing {} subscriptions",
                        topic, cursor, history.floorSeq(), history.headSeq(), subscriptions.size());
                metrics.feedResynced();
                subscriptions.forEach(Subscription::resyncRequired);
                cursor = history.headSeq();
                return;
            }
            List<SequencedEvent> events = history.events();
            for (SequencedEvent event : events) {
                for (Subscription subscription : subscriptions) {
                    subscription.deliver(event);
                }
                cursor = event.seq();
            }
            if (events.size() < pageSize) {
                return;
            }
        }
    }

    private void deliverLatestLocation(String cabId, long notifiedSeq) {
        Long delivered = deliveredLocationSeqByCab.get(cabId);
        if (delivered != null && notifiedSeq <= delivered) {
            return; // duplicate or stale notification
        }
        Optional<SequencedEvent> latest = store.latestLocation(topic, cabId);
        if (latest.isEmpty() || (delivered != null && latest.get().seq() <= delivered)) {
            return;
        }
        deliveredLocationSeqByCab.put(cabId, latest.get().seq());
        for (Subscription subscription : subscriptions) {
            subscription.deliver(latest.get());
        }
    }

    /** Cursor for tests and diagnostics. */
    long cursor() {
        lock.lock();
        try {
            return cursor;
        } finally {
            lock.unlock();
        }
    }
}

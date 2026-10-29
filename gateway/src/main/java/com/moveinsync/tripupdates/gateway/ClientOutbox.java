package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded per-connection delivery queue with two lanes (ADR-0006):
 * <ul>
 *   <li><b>Reliable lane</b> (STATUS, DELAY, SOS): FIFO, capacity-bounded. Events are offered by each topic's feed
 *       in sequence order, so FIFO preserves per-topic order.</li>
 *   <li><b>Location lane</b>: one pending slot per (topic, cab); a newer location replaces the pending one
 *       (conflation). Capacity-bounded; when a new cab would exceed it, the oldest pending location is evicted,
 *       which is acceptable only because locations are latest-value data.</li>
 * </ul>
 * {@link #take()} always serves the reliable lane first, so an SOS (or status) never waits behind pending
 * locations, while reliable events keep their order among themselves. Protocol control messages
 * ({@link #offerControl}) share the reliable lane, so they stay ordered relative to reliable events.
 *
 * <p><b>Overflow policy for reliable events:</b> a reliable event is never dropped silently. When the reliable lane
 * is full, the outbox closes itself, discards everything still pending and returns {@link OfferResult#OVERFLOW};
 * the owner must then close the connection with {@link CloseReason#SLOW_CONSUMER}. The client reconnects and
 * resumes from its last processed sequence; the discarded events are still in the Redis streams. This keeps memory
 * per connection bounded by the two capacities.
 */
public final class ClientOutbox {

    public enum OfferResult {
        QUEUED,
        /** A pending location for the same cab was replaced (or the offered one was older and ignored). */
        CONFLATED,
        /** Queued, but the oldest pending location of another cab was evicted to stay within capacity. */
        LOCATION_EVICTED,
        /** Reliable lane full: the outbox is now closed and the connection must be closed. */
        OVERFLOW,
        /** The outbox was already closed; nothing was queued. */
        CLOSED
    }

    private record LocationKey(TopicId topic, String cabId) {
    }

    private final int reliableCapacity;
    private final int locationCapacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final ArrayDeque<OutboundFrame> reliable;
    private final LinkedHashMap<LocationKey, SequencedEvent> locations = new LinkedHashMap<>();
    private boolean closed;
    private long conflated;
    private long evicted;

    public ClientOutbox(int reliableCapacity, int locationCapacity) {
        if (reliableCapacity < 1 || locationCapacity < 1) {
            throw new IllegalArgumentException("capacities must be positive");
        }
        this.reliableCapacity = reliableCapacity;
        this.locationCapacity = locationCapacity;
        this.reliable = new ArrayDeque<>(Math.min(reliableCapacity, 64));
    }

    /** Never blocks. */
    public OfferResult offer(SequencedEvent event) {
        lock.lock();
        try {
            if (closed) {
                return OfferResult.CLOSED;
            }
            if (!event.type().isConflatable()) {
                if (reliable.size() >= reliableCapacity) {
                    closeLocked();
                    return OfferResult.OVERFLOW;
                }
                reliable.addLast(new OutboundFrame.EventFrame(event));
                notEmpty.signal();
                return OfferResult.QUEUED;
            }
            LocationKey key = new LocationKey(event.topic(), event.event().cabId());
            SequencedEvent pending = locations.remove(key);
            if (pending != null && pending.seq() >= event.seq()) {
                locations.put(key, pending); // keep the newer one already pending
                conflated++;
                return OfferResult.CONFLATED;
            }
            OfferResult result = OfferResult.QUEUED;
            if (pending != null) {
                conflated++;
                result = OfferResult.CONFLATED;
            } else if (locations.size() >= locationCapacity) {
                Iterator<LocationKey> oldest = locations.keySet().iterator();
                oldest.next();
                oldest.remove();
                evicted++;
                result = OfferResult.LOCATION_EVICTED;
            }
            locations.put(key, event);
            notEmpty.signal();
            return result;
        } finally {
            lock.unlock();
        }
    }

    /** Queues a protocol control message in the reliable lane. Never blocks; same overflow policy as events. */
    public OfferResult offerControl(OutboundFrame.ControlMessage message) {
        lock.lock();
        try {
            if (closed) {
                return OfferResult.CLOSED;
            }
            if (reliable.size() >= reliableCapacity) {
                closeLocked();
                return OfferResult.OVERFLOW;
            }
            reliable.addLast(new OutboundFrame.ControlFrame(message));
            notEmpty.signal();
            return OfferResult.QUEUED;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Next frame to send, reliable lane first; blocks while both lanes are empty.
     *
     * @return the next frame, or null once the outbox is closed (pending frames are then discarded)
     */
    public OutboundFrame take() throws InterruptedException {
        lock.lock();
        try {
            while (!closed && reliable.isEmpty() && locations.isEmpty()) {
                notEmpty.await();
            }
            if (closed) {
                return null;
            }
            if (!reliable.isEmpty()) {
                return reliable.pollFirst();
            }
            Iterator<SequencedEvent> oldest = locations.values().iterator();
            SequencedEvent next = oldest.next();
            oldest.remove();
            return new OutboundFrame.EventFrame(next);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            closeLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes every pending event of {@code topic} (reliable and location) after the client unsubscribed from it.
     * This is not a silent drop: the client asked to stop receiving the topic. Control messages are kept.
     *
     * @return the number of frames removed
     */
    public int discardTopic(TopicId topic) {
        lock.lock();
        try {
            int before = reliable.size() + locations.size();
            reliable.removeIf(frame -> frame instanceof OutboundFrame.EventFrame(SequencedEvent event)
                    && event.topic().equals(topic));
            locations.keySet().removeIf(key -> key.topic().equals(topic));
            return before - reliable.size() - locations.size();
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    public int reliableDepth() {
        lock.lock();
        try {
            return reliable.size();
        } finally {
            lock.unlock();
        }
    }

    public int locationDepth() {
        lock.lock();
        try {
            return locations.size();
        } finally {
            lock.unlock();
        }
    }

    public long conflatedCount() {
        lock.lock();
        try {
            return conflated;
        } finally {
            lock.unlock();
        }
    }

    public long evictedCount() {
        lock.lock();
        try {
            return evicted;
        } finally {
            lock.unlock();
        }
    }

    private void closeLocked() {
        closed = true;
        reliable.clear();
        locations.clear();
        notEmpty.signalAll();
    }
}

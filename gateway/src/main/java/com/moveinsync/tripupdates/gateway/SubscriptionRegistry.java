package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.store.EventStore;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * This gateway's local subscriptions, indexed by tenant-scoped topic. Thread-safe.
 *
 * <p>Tenant isolation is structural: a subscription's topic key is built from the session's own tenant, never from
 * client input, and notifications are routed by {@link TenantTopic}. A notification for tenant B therefore cannot
 * reach a session of tenant A even when both use the same topic name. (Authorization of the topic itself is a
 * separate concern, added with authentication.)
 */
public final class SubscriptionRegistry {

    private final EventStore store;
    private final int pageSize;
    private final Executor feedExecutor;
    private final int reliableCapacity;
    private final int locationCapacity;
    private final GatewayMetrics metrics;
    private final ConcurrentHashMap<TenantTopic, TopicFeed> feeds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClientSession, SessionSubscriptions> bySession = new ConcurrentHashMap<>();
    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();

    /**
     * One session's subscriptions. Changes are serialised by a {@link ReentrantLock}, not {@code synchronized}: the
     * critical section takes a feed lock and may read Redis, and on Java 21 a virtual thread that blocks inside a
     * {@code synchronized} block pins its carrier thread. Under many concurrent subscribes to the same topic that
     * starved every carrier and hung the gateway (found by the 10k-connection load test).
     */
    private static final class SessionSubscriptions {
        final ReentrantLock lock = new ReentrantLock();
        final Map<TopicId, Subscription> topics = new ConcurrentHashMap<>();
    }

    public SubscriptionRegistry(EventStore store, int pageSize, Executor feedExecutor, int reliableCapacity,
            int locationCapacity) {
        this(store, pageSize, feedExecutor, reliableCapacity, locationCapacity, GatewayMetrics.NOOP);
    }

    public SubscriptionRegistry(EventStore store, int pageSize, Executor feedExecutor, int reliableCapacity,
            int locationCapacity, GatewayMetrics metrics) {
        this.store = store;
        this.pageSize = pageSize;
        this.feedExecutor = feedExecutor;
        this.reliableCapacity = reliableCapacity;
        this.locationCapacity = locationCapacity;
        this.metrics = metrics;
    }

    /** Opens a session for an authenticated client of {@code tenantId} and starts its writer. */
    public ClientSession open(String sessionId, String tenantId, ClientTransport transport) {
        ClientSession session = new ClientSession(sessionId, tenantId,
                new ClientOutbox(reliableCapacity, locationCapacity), transport, this::removeAll, metrics);
        sessions.add(session);
        metrics.sessionOpened();
        session.startWriter();
        return session;
    }

    /**
     * Subscribes the session to a topic of its own tenant, receiving ordered events published after this call.
     * Subscribing again to the same topic returns the existing subscription (no duplicate delivery).
     *
     * @throws IllegalStateException if the session is closed
     * @throws com.moveinsync.tripupdates.store.EventStoreException if the topic position cannot be read
     */
    public Subscription subscribe(ClientSession session, TopicId topic) {
        AttachOutcome outcome = attach(session, topic, null, Integer.MAX_VALUE, null);
        if (outcome instanceof AttachOutcome.HistoryUnavailable unavailable) {
            throw new IllegalStateException("Unexpected history gap at feed position " + unavailable.requestedSeq());
        }
        return subscription(session, topic);
    }

    /**
     * Attaches a subscription of the session's own tenant (see {@link TopicFeed#attach} for ordering guarantees).
     * The topic key is built from the session's tenant, never from client input. Authorization must have been
     * checked by the caller.
     *
     * @param resumeAfter  null for live-only; otherwise the last sequence the client processed
     * @param confirmation control message queued after the replay and before live events (may be null)
     * @throws IllegalStateException if the session is closed
     * @throws com.moveinsync.tripupdates.store.EventStoreException if the store cannot be read (nothing attached)
     */
    public AttachOutcome attach(ClientSession session, TopicId topic, Long resumeAfter, int maxSubscriptions,
            Function<AttachOutcome.Attached, OutboundFrame.ControlMessage> confirmation) {
        TenantTopic key = new TenantTopic(session.tenantId(), topic);
        SessionSubscriptions mine = bySession.computeIfAbsent(session, s -> new SessionSubscriptions());
        mine.lock.lock();
        try {
            if (session.isClosed()) {
                bySession.remove(session, mine);
                throw new IllegalStateException("Session " + session.id() + " is closed");
            }
            if (mine.topics.containsKey(topic)) {
                return new AttachOutcome.AlreadySubscribed();
            }
            if (mine.topics.size() >= maxSubscriptions) {
                return new AttachOutcome.LimitReached(maxSubscriptions);
            }
            Subscription subscription = new Subscription(session, key);
            while (true) {
                TopicFeed feed = feeds.computeIfAbsent(key,
                        k -> new TopicFeed(k, store, pageSize, feedExecutor, metrics));
                AttachOutcome outcome;
                try {
                    outcome = feed.attach(subscription, resumeAfter, confirmation);
                } catch (RuntimeException e) {
                    if (feed.retireIfEmpty()) {
                        feeds.remove(key, feed);
                    }
                    throw e;
                }
                if (outcome == null) {
                    feeds.remove(key, feed); // retired concurrently; retry with a fresh feed
                    continue;
                }
                if (outcome instanceof AttachOutcome.Attached) {
                    mine.topics.put(topic, subscription);
                } else if (feed.retireIfEmpty()) {
                    feeds.remove(key, feed);
                }
                return outcome;
            }
        } finally {
            mine.lock.unlock();
        }
    }

    /** The session's subscription to a topic, or null. */
    public Subscription subscription(ClientSession session, TopicId topic) {
        SessionSubscriptions mine = bySession.get(session);
        return mine == null ? null : mine.topics.get(topic);
    }

    /**
     * Removes the subscription and every event of that topic still pending in the session's queue (including
     * pending locations), so nothing of the topic is sent after the caller's {@code UNSUBSCRIBED}. Detaching takes the
     * feed lock, so no delivery of the topic can be in flight once the purge runs.
     *
     * @return true if the session was subscribed to the topic
     */
    public boolean unsubscribe(ClientSession session, TopicId topic) {
        SessionSubscriptions mine = bySession.get(session);
        if (mine == null) {
            return false;
        }
        mine.lock.lock();
        try {
            Subscription subscription = mine.topics.remove(topic);
            if (subscription == null) {
                return false;
            }
            detach(subscription);
            session.outbox().discardTopic(topic);
            return true;
        } finally {
            mine.lock.unlock();
        }
    }

    /** Removes every subscription of the session (called when it closes). */
    public void removeAll(ClientSession session) {
        sessions.remove(session);
        SessionSubscriptions mine = bySession.remove(session);
        if (mine == null) {
            return;
        }
        mine.lock.lock();
        try {
            mine.topics.values().forEach(this::detach);
            mine.topics.clear();
        } finally {
            mine.lock.unlock();
        }
    }

    /** The feed of a tenant-scoped topic, or null if no local session subscribes to it. */
    public TopicFeed feed(TenantTopic topic) {
        return feeds.get(topic);
    }

    public Collection<TopicFeed> feeds() {
        return List.copyOf(feeds.values());
    }

    /** Open sessions of this gateway. */
    public Collection<ClientSession> sessions() {
        return List.copyOf(sessions);
    }

    public List<TopicId> topicsOf(ClientSession session) {
        SessionSubscriptions mine = bySession.get(session);
        return mine == null ? List.of() : List.copyOf(mine.topics.keySet());
    }

    private void detach(Subscription subscription) {
        TopicFeed feed = feeds.get(subscription.topic());
        if (feed != null && feed.remove(subscription)) {
            feeds.remove(subscription.topic(), feed);
        }
    }
}

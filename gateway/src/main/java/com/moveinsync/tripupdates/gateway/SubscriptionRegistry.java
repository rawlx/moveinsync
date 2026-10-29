package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.store.EventStore;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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
    private final ConcurrentHashMap<TenantTopic, TopicFeed> feeds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ClientSession, Map<TopicId, Subscription>> bySession = new ConcurrentHashMap<>();

    public SubscriptionRegistry(EventStore store, int pageSize, Executor feedExecutor, int reliableCapacity,
            int locationCapacity) {
        this.store = store;
        this.pageSize = pageSize;
        this.feedExecutor = feedExecutor;
        this.reliableCapacity = reliableCapacity;
        this.locationCapacity = locationCapacity;
    }

    /** Opens a session for an authenticated client of {@code tenantId} and starts its writer. */
    public ClientSession open(String sessionId, String tenantId, ClientTransport transport) {
        ClientSession session = new ClientSession(sessionId, tenantId,
                new ClientOutbox(reliableCapacity, locationCapacity), transport, this::removeAll);
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
        Map<TopicId, Subscription> mine = bySession.computeIfAbsent(session, s -> new ConcurrentHashMap<>());
        synchronized (mine) {
            if (session.isClosed()) {
                bySession.remove(session, mine);
                throw new IllegalStateException("Session " + session.id() + " is closed");
            }
            if (mine.containsKey(topic)) {
                return new AttachOutcome.AlreadySubscribed();
            }
            if (mine.size() >= maxSubscriptions) {
                return new AttachOutcome.LimitReached(maxSubscriptions);
            }
            Subscription subscription = new Subscription(session, key);
            while (true) {
                TopicFeed feed = feeds.computeIfAbsent(key, k -> new TopicFeed(k, store, pageSize, feedExecutor));
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
                    mine.put(topic, subscription);
                } else if (feed.retireIfEmpty()) {
                    feeds.remove(key, feed);
                }
                return outcome;
            }
        }
    }

    /** The session's subscription to a topic, or null. */
    public Subscription subscription(ClientSession session, TopicId topic) {
        Map<TopicId, Subscription> mine = bySession.get(session);
        return mine == null ? null : mine.get(topic);
    }

    /** @return true if the session was subscribed to the topic */
    public boolean unsubscribe(ClientSession session, TopicId topic) {
        Map<TopicId, Subscription> mine = bySession.get(session);
        if (mine == null) {
            return false;
        }
        synchronized (mine) {
            Subscription subscription = mine.remove(topic);
            if (subscription == null) {
                return false;
            }
            detach(subscription);
            return true;
        }
    }

    /** Removes every subscription of the session (called when it closes). */
    public void removeAll(ClientSession session) {
        Map<TopicId, Subscription> mine = bySession.remove(session);
        if (mine == null) {
            return;
        }
        synchronized (mine) {
            mine.values().forEach(this::detach);
            mine.clear();
        }
    }

    /** The feed of a tenant-scoped topic, or null if no local session subscribes to it. */
    public TopicFeed feed(TenantTopic topic) {
        return feeds.get(topic);
    }

    public Collection<TopicFeed> feeds() {
        return List.copyOf(feeds.values());
    }

    public List<TopicId> topicsOf(ClientSession session) {
        Map<TopicId, Subscription> mine = bySession.get(session);
        return mine == null ? List.of() : List.copyOf(mine.keySet());
    }

    private void detach(Subscription subscription) {
        TopicFeed feed = feeds.get(subscription.topic());
        if (feed != null && feed.remove(subscription)) {
            feeds.remove(subscription.topic(), feed);
        }
    }
}

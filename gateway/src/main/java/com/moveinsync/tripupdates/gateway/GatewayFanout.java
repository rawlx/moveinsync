package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.notify.NotificationListener;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Routes notifications to local topic feeds and reconciles feeds with the durable streams.
 *
 * <p>Delivery guarantee offered here: Pub/Sub is best effort, so a notification may be lost (gateway disconnected
 * from Redis, publish failure after the append). Every locally subscribed topic is therefore re-read from its cursor
 * (a) whenever the notification subscription becomes active again and (b) every {@code reconcileInterval}. A
 * durable ordered event is thus delivered to a live local subscription at the latest one reconcile interval after it
 * was appended, provided Redis is reachable and the history is still retained; beyond retention the subscription is
 * resynced instead. Locations are latest-value data and are not reconciled: a lost location notification is
 * superseded by the next location of that cab.
 */
public final class GatewayFanout implements NotificationListener, AutoCloseable {

    private final SubscriptionRegistry registry;
    private final ScheduledExecutorService ticker;

    public GatewayFanout(SubscriptionRegistry registry, Duration reconcileInterval) {
        this.registry = registry;
        this.ticker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .name("fanout-reconcile").daemon(true).factory());
        long millis = reconcileInterval.toMillis();
        ticker.scheduleWithFixedDelay(this::reconcileAll, millis, millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onNotification(EventNotification notification) {
        for (EventNotification.TopicSeq topicSeq : notification.topics()) {
            TopicFeed feed = registry.feed(new TenantTopic(notification.tenantId(), topicSeq.topic()));
            if (feed != null) {
                feed.signal(topicSeq.seq(), notification.type(), notification.cabId());
            }
        }
    }

    @Override
    public void onSubscribed() {
        reconcileAll();
    }

    /** Re-reads every locally subscribed topic from its cursor. Idempotent; never blocks. */
    public void reconcileAll() {
        registry.feeds().forEach(TopicFeed::requestCatchUp);
    }

    @Override
    public void close() {
        ticker.shutdownNow();
    }
}

package com.moveinsync.tripupdates.publish;

import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.notify.NotificationPublishException;
import com.moveinsync.tripupdates.notify.NotificationPublisher;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.EventStore;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes an event: durable append first, notification second. The notification is sent only after the append
 * succeeded, and a failed notification never causes a second append.
 */
public final class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final EventStore store;
    private final NotificationPublisher notifications;
    private final AtomicLong notificationFailures = new AtomicLong();

    public EventPublisher(EventStore store, NotificationPublisher notifications) {
        this.store = store;
        this.notifications = notifications;
    }

    /**
     * Appends the event and then notifies gateways. Exceptions from the append propagate unchanged (see
     * {@link EventStore#append}); no notification is sent for an append that did not succeed. A duplicate append
     * (idempotent retry) is re-notified, which repairs a notification lost by the original attempt.
     */
    public PublishOutcome publish(TripEvent event, List<TopicId> topics) {
        AppendResult appended = store.append(event, topics);
        return new PublishOutcome(appended, notify(appended));
    }

    /** Resends the notification for an already durable append. Never appends. Returns whether it was sent. */
    public boolean renotify(AppendResult appended) {
        return notify(appended);
    }

    /** Number of notifications that could not be published since start. */
    public long notificationFailures() {
        return notificationFailures.get();
    }

    private boolean notify(AppendResult appended) {
        EventNotification notification = EventNotification.of(appended);
        try {
            notifications.publish(notification);
            return true;
        } catch (NotificationPublishException e) {
            notificationFailures.incrementAndGet();
            log.warn("Notification for durable event {} of tenant {} was not published; gateways will recover it "
                    + "from the stream", notification.eventId(), notification.tenantId(), e);
            return false;
        }
    }
}

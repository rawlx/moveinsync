package com.moveinsync.tripupdates.notify;

/** Best-effort broadcast of {@link EventNotification}s to every gateway. */
public interface NotificationPublisher {

    /** @throws NotificationPublishException if the notification could not be handed to the broker */
    void publish(EventNotification notification);
}

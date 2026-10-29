package com.moveinsync.tripupdates.notify;

/**
 * Receives notifications. Both methods are called on the Redis client's I/O thread and must not block.
 */
public interface NotificationListener {

    void onNotification(EventNotification notification);

    /**
     * The subscription to the notification channel became active, initially or after a reconnect. Notifications
     * published while it was inactive are lost, so the listener should reconcile with the durable streams.
     */
    void onSubscribed();
}

package com.moveinsync.tripupdates.notify;

/** A notification could not be published. The event itself is already durable; only the hint was lost. */
public final class NotificationPublishException extends RuntimeException {

    public NotificationPublishException(String eventId, Throwable cause) {
        super("Notification for event " + eventId + " was not published", cause);
    }
}

package com.moveinsync.tripupdates.publish;

import com.moveinsync.tripupdates.store.AppendResult;

/**
 * Result of a publish. The event is durable whenever an outcome is returned.
 *
 * @param notified false when the Pub/Sub notification could not be sent. The event is still delivered: gateways
 *                 find it in the stream on their next reconciliation or on the next notification for the topic.
 *                 {@link EventPublisher#renotify} can resend the hint without appending again.
 */
public record PublishOutcome(AppendResult appended, boolean notified) {
}

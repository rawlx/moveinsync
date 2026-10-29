package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.EventType;
import java.time.Duration;

/**
 * Observation hooks of the delivery pipeline, so the core stays independent of the metrics library. Every argument
 * is drawn from a small fixed set (enums, booleans); implementations must never add tenant, trip, cab, topic or
 * client identifiers as metric labels.
 */
public interface GatewayMetrics {

    GatewayMetrics NOOP = new GatewayMetrics() {
    };

    /** Outcome of a SUBSCRIBE or RESUME request (bounded set, see the protocol layer). */
    enum SubscriptionOutcome {
        ATTACHED,
        FORBIDDEN,
        ALREADY_SUBSCRIBED,
        LIMIT_REACHED,
        RECOVERY_REQUIRED,
        UNAVAILABLE
    }

    default void sessionOpened() {
    }

    default void sessionClosed(CloseReason reason) {
    }

    /** An event frame was written to a client; {@code lag} is from Redis publication to the end of the write. */
    default void eventDelivered(EventType type, Duration lag) {
    }

    /** A queue offer that did not simply enqueue (conflation, eviction, overflow). */
    default void outboxOffer(ClientOutbox.OfferResult result) {
    }

    /** A feed could not read the event store; it retries on the next reconciliation. */
    default void feedReadFailed() {
    }

    /** A feed lost history beyond its position and asked its subscriptions to resync. */
    default void feedResynced() {
    }

    default void reconciliationRun() {
    }

    default void subscriptionRequest(boolean resume, SubscriptionOutcome outcome) {
    }

    /** A live subscription was removed because the identity is no longer authorised for the topic. */
    default void subscriptionRevoked() {
    }
}

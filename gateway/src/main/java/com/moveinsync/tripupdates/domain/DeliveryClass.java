package com.moveinsync.tripupdates.domain;

/** How an event is delivered (ADR-0002). */
public enum DeliveryClass {
    /** Strict per-topic order, stored in the replay log, never dropped. */
    ORDERED,
    /** Only the latest value per cab matters; may be conflated; not replayed. */
    LATEST_VALUE
}

package com.moveinsync.tripupdates.domain;

/** Event types and their delivery semantics (ADR-0002). */
public enum EventType {
    LOCATION(DeliveryClass.LATEST_VALUE, false, false),
    STATUS(DeliveryClass.ORDERED, false, true),
    DELAY(DeliveryClass.ORDERED, false, true),
    SOS(DeliveryClass.ORDERED, true, true);

    private final DeliveryClass deliveryClass;
    private final boolean priority;
    private final boolean requiresTrip;

    EventType(DeliveryClass deliveryClass, boolean priority, boolean requiresTrip) {
        this.deliveryClass = deliveryClass;
        this.priority = priority;
        this.requiresTrip = requiresTrip;
    }

    public DeliveryClass deliveryClass() {
        return deliveryClass;
    }

    public boolean isConflatable() {
        return deliveryClass == DeliveryClass.LATEST_VALUE;
    }

    /** SOS: measured for latency and never queued behind location traffic. */
    public boolean isPriority() {
        return priority;
    }

    /** Whether the event must name the trip it belongs to (location may be published without one). */
    public boolean requiresTrip() {
        return requiresTrip;
    }
}

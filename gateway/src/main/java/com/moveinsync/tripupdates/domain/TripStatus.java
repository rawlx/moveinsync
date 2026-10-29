package com.moveinsync.tripupdates.domain;

/** Trip lifecycle states carried by STATUS events. */
public enum TripStatus {
    ASSIGNED,
    DRIVER_EN_ROUTE,
    ARRIVED,
    PICKED_UP,
    DROPPED,
    COMPLETED,
    CANCELLED;

    /** Terminal states end the trip; snapshots drop the trip's transient state (delay, SOS) on these. */
    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED;
    }
}

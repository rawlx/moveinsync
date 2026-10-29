package com.moveinsync.tripupdates.routing;

/**
 * An event cannot be routed because the directory does not confirm the relationships it claims. The message names the
 * reason only (no identifiers or payload), so it is safe to log or return.
 */
public final class RoutingException extends RuntimeException {

    public enum Reason {
        /** The trip does not exist in the event's tenant. */
        UNKNOWN_TRIP,
        /** The cab does not exist in the event's tenant. */
        UNKNOWN_CAB,
        /** The trip exists but is served by another cab. */
        CAB_NOT_ON_TRIP,
        /** The office recorded for the trip or cab does not exist (inconsistent directory data). */
        UNKNOWN_OFFICE
    }

    private final Reason reason;

    public RoutingException(Reason reason) {
        super("Event cannot be routed: " + reason);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

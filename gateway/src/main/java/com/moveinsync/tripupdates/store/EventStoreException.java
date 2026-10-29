package com.moveinsync.tripupdates.store;

/**
 * Base class for event store failures. Messages contain identifiers only, never event payloads.
 */
public class EventStoreException extends RuntimeException {

    public EventStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    /** True when the failed append may nevertheless have been applied; only a retry with the same event resolves it. */
    public boolean mayHaveBeenApplied() {
        return false;
    }
}

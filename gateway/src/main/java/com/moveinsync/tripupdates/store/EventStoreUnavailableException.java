package com.moveinsync.tripupdates.store;

/** Redis could not be reached, so the command was not sent: nothing was written. */
public final class EventStoreUnavailableException extends EventStoreException {

    public EventStoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

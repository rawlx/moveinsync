package com.moveinsync.tripupdates.store;

/**
 * The append was sent but no reply arrived (timeout or connection lost). It may have been applied. The store does
 * not retry; the caller may retry with the same event, which is idempotent by event id for ordered events.
 */
public final class AppendOutcomeUnknownException extends EventStoreException {

    private final String eventId;

    public AppendOutcomeUnknownException(String eventId, Throwable cause) {
        super("Outcome of append for event " + eventId + " is unknown", cause);
        this.eventId = eventId;
    }

    public String eventId() {
        return eventId;
    }

    @Override
    public boolean mayHaveBeenApplied() {
        return true;
    }
}

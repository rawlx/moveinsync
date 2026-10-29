package com.moveinsync.tripupdates.store;

/** The event id was already appended, within the dedup window, with different content or topics. Nothing was written. */
public final class EventIdConflictException extends EventStoreException {

    private final String eventId;

    public EventIdConflictException(String eventId) {
        super("Event id " + eventId + " was already used for a different event", null);
        this.eventId = eventId;
    }

    public String eventId() {
        return eventId;
    }
}

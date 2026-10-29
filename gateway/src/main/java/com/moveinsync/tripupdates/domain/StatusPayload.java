package com.moveinsync.tripupdates.domain;

/** Trip status change. */
public record StatusPayload(TripStatus status) implements EventPayload {

    public StatusPayload {
        if (status == null) {
            throw new DomainValidationException("data.status", "is required");
        }
    }

    @Override
    public EventType type() {
        return EventType.STATUS;
    }
}

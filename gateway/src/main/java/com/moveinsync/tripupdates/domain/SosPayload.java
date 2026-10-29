package com.moveinsync.tripupdates.domain;

/** An SOS alert. The position is where the alert was raised; it is redacted from {@link #toString()}. */
public record SosPayload(String sosId, SosRaiser raisedBy, GeoPoint position) implements EventPayload {

    public SosPayload {
        Identifiers.require(sosId, "data.sosId");
        if (raisedBy == null) {
            throw new DomainValidationException("data.raisedBy", "is required");
        }
        if (position == null) {
            throw new DomainValidationException("data.position", "is required");
        }
    }

    @Override
    public EventType type() {
        return EventType.SOS;
    }

    @Override
    public String toString() {
        return "SosPayload[sosId=" + sosId + ", raisedBy=" + raisedBy + ", position=redacted]";
    }
}

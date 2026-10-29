package com.moveinsync.tripupdates.domain;

import java.time.Instant;

/**
 * A validated event before sequencing. Topic, per-topic sequence number and {@code publishedAt} do not exist yet:
 * they are assigned atomically by the event store and carried by {@link SequencedEvent}.
 *
 * @param eventId    publisher-supplied idempotency key
 * @param tenantId   taken from the authenticated principal, never from the request body
 * @param tripId     required for STATUS, DELAY and SOS; optional for LOCATION (null when the cab has no trip)
 * @param cabId      always required
 * @param occurredAt when the publisher observed the event
 */
public record TripEvent(
        String eventId,
        String tenantId,
        String tripId,
        String cabId,
        Instant occurredAt,
        EventPayload payload) {

    public TripEvent {
        Identifiers.require(eventId, "eventId");
        Identifiers.require(tenantId, "tenantId");
        Identifiers.require(cabId, "cabId");
        if (payload == null) {
            throw new DomainValidationException("data", "is required");
        }
        if (tripId == null) {
            if (payload.type().requiresTrip()) {
                throw new DomainValidationException("tripId", "is required for " + payload.type());
            }
        } else {
            Identifiers.require(tripId, "tripId");
        }
        if (occurredAt == null) {
            throw new DomainValidationException("occurredAt", "is required");
        }
    }

    public EventType type() {
        return payload.type();
    }

    public boolean hasTrip() {
        return tripId != null;
    }
}

package com.moveinsync.tripupdates.domain;

import java.time.Instant;

/**
 * An event as delivered on one topic: the {@link TripEvent} plus the fields the event store assigns atomically
 * when it appends the event (ADR-0001). One published event produces one {@code SequencedEvent} per target topic,
 * each with that topic's own sequence number.
 *
 * <p>Consistency rules: a trip topic must name the event's trip, a cab topic the event's cab. Office membership
 * comes from the directory and cannot be checked here.
 */
public record SequencedEvent(TripEvent event, TopicId topic, long seq, Instant publishedAt) {

    public SequencedEvent {
        if (event == null) {
            throw new DomainValidationException("event", "is required");
        }
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
        if (seq < 1) {
            throw new DomainValidationException("seq", "must be >= 1");
        }
        if (publishedAt == null) {
            throw new DomainValidationException("publishedAt", "is required");
        }
        requireDeliverableOn(event, topic);
    }

    /**
     * Checks that {@code event} may be delivered on {@code topic}: a trip topic must name the event's trip, a cab
     * topic the event's cab, and an SOS topic carries SOS events only. Used before sequencing so that an
     * inconsistent append never reaches the store.
     */
    public static void requireDeliverableOn(TripEvent event, TopicId topic) {
        switch (topic.kind()) {
            case TRIP -> {
                if (!topic.id().equals(event.tripId())) {
                    throw new DomainValidationException("topic", "trip topic does not match the event's trip");
                }
            }
            case CAB -> {
                if (!topic.id().equals(event.cabId())) {
                    throw new DomainValidationException("topic", "cab topic does not match the event's cab");
                }
            }
            case OFFICE -> {
                // Office membership is resolved from the directory during routing.
            }
            case SOS -> {
                if (event.type() != EventType.SOS) {
                    throw new DomainValidationException("topic", "sos topic carries SOS events only");
                }
            }
        }
    }

    /** The tenant-scoped key this event is delivered under. */
    public TenantTopic tenantTopic() {
        return new TenantTopic(event.tenantId(), topic);
    }

    public EventType type() {
        return event.type();
    }
}

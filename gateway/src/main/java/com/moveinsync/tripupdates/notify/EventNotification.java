package com.moveinsync.tripupdates.notify;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.Identifiers;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.store.AppendResult;
import java.util.List;

/**
 * A hint that an event was durably appended: which tenant-scoped topics advanced, to which sequence, for which event.
 * It carries no payload. Receivers read the canonical event from the event store, so a lost, duplicated or delayed
 * notification can delay delivery but never change what is delivered.
 */
public record EventNotification(String tenantId, String eventId, EventType type, String cabId, List<TopicSeq> topics) {

    public EventNotification {
        Identifiers.require(tenantId, "tenantId");
        Identifiers.require(eventId, "eventId");
        Identifiers.require(cabId, "cabId");
        if (type == null) {
            throw new DomainValidationException("type", "is required");
        }
        if (topics == null || topics.isEmpty()) {
            throw new DomainValidationException("topics", "must not be empty");
        }
        topics = List.copyOf(topics);
    }

    /** The notification for a durable append (also for a duplicate append, which re-announces the original seqs). */
    public static EventNotification of(AppendResult appended) {
        TripEvent event = appended.events().get(0).event();
        List<TopicSeq> topics = appended.events().stream()
                .map(sequenced -> new TopicSeq(sequenced.topic(), sequenced.seq()))
                .toList();
        return new EventNotification(event.tenantId(), event.eventId(), event.type(), event.cabId(), topics);
    }

    /** One topic's sequence number for the event. */
    public record TopicSeq(TopicId topic, long seq) {

        public TopicSeq {
            if (topic == null) {
                throw new DomainValidationException("topic", "is required");
            }
            if (seq < 1) {
                throw new DomainValidationException("seq", "must be >= 1");
            }
        }
    }
}

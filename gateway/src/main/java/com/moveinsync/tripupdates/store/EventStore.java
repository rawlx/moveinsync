package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import java.util.List;

/**
 * Durable, per-topic sequenced event storage. Sequence numbers are assigned by the store, atomically with the
 * append, so they are unique and increasing per topic across every publisher and gateway (ADR-0001).
 */
public interface EventStore {

    /**
     * Assigns one sequence number per topic and persists the event on every topic in a single atomic operation.
     * Ordered events are idempotent by {@code eventId}: a retry with identical content returns the original
     * result with {@link AppendResult#duplicate()} set.
     *
     * @param topics target topics of the event's tenant, in a stable order; trip/cab topics must match the event
     * @throws com.moveinsync.tripupdates.domain.DomainValidationException if the input is invalid (nothing written)
     * @throws EventIdConflictException       if the event id was already used for different content
     * @throws AppendOutcomeUnknownException  if the append may or may not have been applied; retry with the same event
     * @throws AppendRejectedException        if Redis refused the append; nothing was written
     * @throws EventStoreUnavailableException if Redis could not be reached; nothing was written
     */
    AppendResult append(TripEvent event, List<TopicId> topics);

    /**
     * Reads ordered events with {@code seq > afterSeq}, together with the information needed to detect that the
     * requested history is no longer complete.
     */
    TopicHistory readAfter(TenantTopic topic, long afterSeq, int limit);

    /** The latest location per cab on a topic, ordered by sequence number. */
    List<SequencedEvent> latestLocations(TenantTopic topic);
}

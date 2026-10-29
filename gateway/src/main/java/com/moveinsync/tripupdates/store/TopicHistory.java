package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import java.util.List;

/**
 * Ordered events of one topic after a requested position, read atomically with the topic's bounds.
 *
 * @param headSeq  last sequence number assigned on the topic (0 if the topic does not exist)
 * @param floorSeq highest sequence number no longer available in the log (trimmed, or before the topic's start)
 * @param gap      true when the events after the requested position are no longer complete
 *                 ({@code afterSeq < floorSeq}) or the position is unknown to the store ({@code afterSeq > headSeq},
 *                 e.g. after data loss); callers must fall back to a snapshot. {@code events} is then empty.
 * @param events   ordered events only (locations are latest-value and never in the log), at most the requested limit
 */
public record TopicHistory(TenantTopic topic, long headSeq, long floorSeq, boolean gap, List<SequencedEvent> events) {

    public TopicHistory {
        events = List.copyOf(events);
    }
}

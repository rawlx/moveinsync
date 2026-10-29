package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import java.util.List;

/**
 * Outcome of an append: one {@link SequencedEvent} per target topic, in the order the topics were given.
 *
 * @param duplicate true when the event id had already been appended with identical content; the events are the
 *                  original ones and nothing new was written
 */
public record AppendResult(List<SequencedEvent> events, boolean duplicate) {

    public AppendResult {
        events = List.copyOf(events);
    }
}

package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.SequencedEvent;

/**
 * One frame queued for a client: an event, or a protocol control message (acknowledgement, error, resume marker).
 * Control messages travel in the reliable lane together with reliable events, so a client sees, for example,
 * {@code SUBSCRIBED} before the first event of that subscription and {@code RESUME_COMPLETE} after the last replayed
 * event.
 */
public sealed interface OutboundFrame {

    record EventFrame(SequencedEvent event) implements OutboundFrame {
    }

    record ControlFrame(ControlMessage message) implements OutboundFrame {
    }

    /** Marker for protocol control messages; their wire format is owned by the protocol layer. */
    interface ControlMessage {
    }
}

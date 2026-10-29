package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.OutboundFrame;

/**
 * Control messages the gateway sends (events are sent as {@code EVENT} frames, see {@link ProtocolCodec}). They are
 * queued in the client's reliable lane, in order with events.
 */
public sealed interface ServerMessage extends OutboundFrame.ControlMessage {

    /** Live from now on: the subscription's position is {@code seq}; every later ordered event follows. */
    record Subscribed(String id, TopicId topic, long seq) implements ServerMessage {
    }

    record Unsubscribed(String id, TopicId topic) implements ServerMessage {
    }

    /**
     * Sent after the last replayed event and before the first live one: every ordered event with
     * {@code fromSeq < seq <= toSeq} has been queued ({@code replayed} of them; sequence numbers have gaps because
     * locations share the sequence but are not replayed). Later events are live.
     */
    record ResumeComplete(String id, TopicId topic, long fromSeq, long toSeq, int replayed) implements ServerMessage {
    }

    /**
     * @param id       the id of the client message that failed, if it could be read
     * @param topic    the topic concerned, if known
     * @param recovery for {@link ErrorCode#RECOVERY_REQUIRED}: the retained window (may be null)
     */
    record Error(String id, ErrorCode code, String message, TopicId topic, Recovery recovery)
            implements ServerMessage {

        public static Error of(String id, ErrorCode code, String message, TopicId topic) {
            return new Error(id, code, message, topic, null);
        }
    }

    /** Retained history: ordered events with {@code oldestAvailableAfterSeq < seq <= headSeq} can be replayed. */
    record Recovery(long requestedSeq, long oldestAvailableAfterSeq, long headSeq) {
    }
}

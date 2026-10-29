package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.domain.TopicId;

/**
 * Messages a client may send (JSON text frames, see {@link ProtocolCodec} for the exact format). {@code id} is a
 * client-chosen message id echoed in the reply; it is required except on {@code ACK}.
 */
public sealed interface ClientMessage {

    String id();

    TopicId topic();

    /** Live subscription: events published after the subscription is confirmed. */
    record Subscribe(String id, TopicId topic) implements ClientMessage {
    }

    record Unsubscribe(String id, TopicId topic) implements ClientMessage {
    }

    /** Subscribe and replay every ordered event with {@code seq > lastSeq} before going live. */
    record Resume(String id, TopicId topic, long lastSeq) implements ClientMessage {
    }

    /** The client finished processing the event with this sequence on this topic. Idempotent. */
    record Ack(String id, TopicId topic, long seq) implements ClientMessage {
    }
}

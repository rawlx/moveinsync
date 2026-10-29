package com.moveinsync.tripupdates.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.Identifiers;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import com.moveinsync.tripupdates.store.EventJson;
import java.util.Iterator;
import java.util.Set;

/**
 * JSON text-frame protocol of {@code /ws}.
 *
 * <h2>Client → gateway</h2>
 * <pre>
 * {"type":"SUBSCRIBE",   "id":"m1", "topic":"trip:T-1"}
 * {"type":"RESUME",      "id":"m2", "topic":"trip:T-1", "lastSeq":1761732000123004}
 * {"type":"UNSUBSCRIBE", "id":"m3", "topic":"trip:T-1"}
 * {"type":"ACK",                    "topic":"trip:T-1", "seq":1761732000123005}     ("id" optional)
 * </pre>
 * {@code id}: {@code [A-Za-z0-9_-]{1,64}}. {@code topic}: {@code trip|cab|office|sos:{id}}. {@code lastSeq} ≥ 0,
 * {@code seq} ≥ 1, integers. Unknown fields, unknown types and frames longer than the configured limit are rejected
 * with an {@code ERROR}; the connection stays open. Tenant, role and permissions are never read from messages.
 *
 * <h2>Gateway → client</h2>
 * <pre>
 * {"type":"SUBSCRIBED",      "id":"m1", "topic":"trip:T-1", "seq":1761732000123000}
 * {"type":"EVENT", "topic":"trip:T-1", "seq":1761732000123001, "publishedAt":"2026-10-29T10:00:00.123Z",
 *  "event":{"eventId":"e1","tenantId":"acme","type":"STATUS","tripId":"T-1","cabId":"C-1",
 *           "occurredAt":"2026-10-29T10:00:00Z","data":{"status":"PICKED_UP"}}}
 * {"type":"RESUME_COMPLETE", "id":"m2", "topic":"trip:T-1", "fromSeq":…, "toSeq":…, "replayed":2}
 * {"type":"UNSUBSCRIBED",    "id":"m3", "topic":"trip:T-1"}
 * {"type":"ERROR", "id":"m2", "code":"RECOVERY_REQUIRED", "message":"…", "topic":"trip:T-1",
 *  "recovery":{"requestedSeq":…, "oldestAvailableAfterSeq":…, "headSeq":…}}
 * </pre>
 * Ordering per topic: {@code SUBSCRIBED} or the replayed events and {@code RESUME_COMPLETE} come before any live
 * event of that subscription; reliable events (STATUS, DELAY, SOS) arrive in sequence order. LOCATION events are
 * latest-value: they may be conflated, may arrive after {@code RESUME_COMPLETE} and do not take part in the ordered
 * sequence. Delivery is at-least-once across reconnects: clients deduplicate by {@code (topic, seq)} and resume with
 * the last ordered {@code seq} they processed.
 */
public final class ProtocolCodec {

    private static final Set<String> SUBSCRIBE_FIELDS = Set.of("type", "id", "topic");
    private static final Set<String> RESUME_FIELDS = Set.of("type", "id", "topic", "lastSeq");
    private static final Set<String> ACK_FIELDS = Set.of("type", "id", "topic", "seq");

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventJson eventJson = new EventJson();

    /** @throws ProtocolException if the frame is not a valid client message */
    public ClientMessage parse(String text) {
        JsonNode node;
        try {
            node = mapper.readTree(text);
        } catch (JsonProcessingException e) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, "Message is not valid JSON", null);
        }
        if (node == null || !node.isObject()) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, "Message must be a JSON object", null);
        }
        String id = readableId(node);
        JsonNode type = node.get("type");
        if (type == null || !type.isTextual()) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, "type: is required", id);
        }
        try {
            return switch (type.asText()) {
                case "SUBSCRIBE" -> {
                    checkFields(node, SUBSCRIBE_FIELDS, id);
                    yield new ClientMessage.Subscribe(requiredId(node), topic(node));
                }
                case "UNSUBSCRIBE" -> {
                    checkFields(node, SUBSCRIBE_FIELDS, id);
                    yield new ClientMessage.Unsubscribe(requiredId(node), topic(node));
                }
                case "RESUME" -> {
                    checkFields(node, RESUME_FIELDS, id);
                    yield new ClientMessage.Resume(requiredId(node), topic(node), sequence(node, "lastSeq", 0, id));
                }
                case "ACK" -> {
                    checkFields(node, ACK_FIELDS, id);
                    String ackId = node.has("id") ? requiredId(node) : null;
                    yield new ClientMessage.Ack(ackId, topic(node), sequence(node, "seq", 1, id));
                }
                default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_TYPE, "type: is not supported", id);
            };
        } catch (DomainValidationException e) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, e.getMessage(), id);
        }
    }

    /** Wire form of a frame queued for a client. */
    public String render(OutboundFrame frame) {
        ObjectNode node = switch (frame) {
            case OutboundFrame.EventFrame(SequencedEvent event) -> {
                ObjectNode out = mapper.createObjectNode();
                out.put("type", "EVENT");
                out.put("topic", event.topic().name());
                out.put("seq", event.seq());
                out.put("publishedAt", event.publishedAt().toString());
                out.set("event", eventJson.toTree(event.event()));
                yield out;
            }
            case OutboundFrame.ControlFrame(OutboundFrame.ControlMessage message) -> control(message);
        };
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Frame could not be serialized", e);
        }
    }

    private ObjectNode control(OutboundFrame.ControlMessage message) {
        if (!(message instanceof ServerMessage server)) {
            throw new IllegalArgumentException("Unsupported control message " + message.getClass().getName());
        }
        ObjectNode out = mapper.createObjectNode();
        switch (server) {
            case ServerMessage.Subscribed subscribed -> {
                out.put("type", "SUBSCRIBED");
                out.put("id", subscribed.id());
                out.put("topic", subscribed.topic().name());
                out.put("seq", subscribed.seq());
            }
            case ServerMessage.Unsubscribed unsubscribed -> {
                out.put("type", "UNSUBSCRIBED");
                out.put("id", unsubscribed.id());
                out.put("topic", unsubscribed.topic().name());
            }
            case ServerMessage.ResumeComplete complete -> {
                out.put("type", "RESUME_COMPLETE");
                out.put("id", complete.id());
                out.put("topic", complete.topic().name());
                out.put("fromSeq", complete.fromSeq());
                out.put("toSeq", complete.toSeq());
                out.put("replayed", complete.replayed());
            }
            case ServerMessage.Error error -> {
                out.put("type", "ERROR");
                if (error.id() != null) {
                    out.put("id", error.id());
                }
                out.put("code", error.code().name());
                out.put("message", error.message());
                if (error.topic() != null) {
                    out.put("topic", error.topic().name());
                }
                if (error.recovery() != null) {
                    ObjectNode recovery = out.putObject("recovery");
                    recovery.put("requestedSeq", error.recovery().requestedSeq());
                    recovery.put("oldestAvailableAfterSeq", error.recovery().oldestAvailableAfterSeq());
                    recovery.put("headSeq", error.recovery().headSeq());
                }
            }
        }
        return out;
    }

    private static void checkFields(JsonNode node, Set<String> allowed, String id) {
        Iterator<String> names = node.fieldNames();
        while (names.hasNext()) {
            if (!allowed.contains(names.next())) {
                throw new ProtocolException(ErrorCode.BAD_REQUEST, "Message contains an unsupported field", id);
            }
        }
    }

    private static String requiredId(JsonNode node) {
        JsonNode id = node.get("id");
        if (id == null || !id.isTextual()) {
            throw new DomainValidationException("id", "is required");
        }
        return Identifiers.require(id.asText(), "id");
    }

    /** The message id if it is present and valid (used to correlate error replies), else null. */
    private static String readableId(JsonNode node) {
        JsonNode id = node.get("id");
        return id != null && id.isTextual() && Identifiers.isValid(id.asText()) ? id.asText() : null;
    }

    private static TopicId topic(JsonNode node) {
        JsonNode topic = node.get("topic");
        if (topic == null || !topic.isTextual()) {
            throw new DomainValidationException("topic", "is required");
        }
        return TopicId.parse(topic.asText());
    }

    private static long sequence(JsonNode node, String field, long min, String id) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < min) {
            throw new ProtocolException(ErrorCode.BAD_REQUEST, field + ": must be an integer >= " + min, id);
        }
        return value.longValue();
    }
}

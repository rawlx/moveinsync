package com.moveinsync.tripupdates.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.tripupdates.domain.DelayPayload;
import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.EventPayload;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.LocationPayload;
import com.moveinsync.tripupdates.domain.SosPayload;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * JSON form of a {@link TripEvent} as stored in Redis:
 * {@code {"eventId","tenantId","type","tripId"?,"cabId","occurredAt","data":{...}}} with flat {@code lat}/{@code lon}
 * in {@code data}. Decoding goes through the domain constructors, so stored data is re-validated.
 */
final class EventJson {

    private final ObjectMapper mapper = new ObjectMapper();

    String encode(TripEvent event) {
        ObjectNode node = mapper.createObjectNode();
        node.put("eventId", event.eventId());
        node.put("tenantId", event.tenantId());
        node.put("type", event.type().name());
        if (event.hasTrip()) {
            node.put("tripId", event.tripId());
        }
        node.put("cabId", event.cabId());
        node.put("occurredAt", event.occurredAt().toString());
        ObjectNode data = node.putObject("data");
        switch (event.payload()) {
            case LocationPayload location -> {
                data.put("lat", location.position().lat());
                data.put("lon", location.position().lon());
                if (location.speedKph() != null) {
                    data.put("speedKph", location.speedKph());
                }
                if (location.heading() != null) {
                    data.put("heading", location.heading());
                }
            }
            case StatusPayload status -> data.put("status", status.status().name());
            case DelayPayload delay -> {
                data.put("delayMinutes", delay.delayMinutes());
                data.put("reasonCode", delay.reasonCode());
            }
            case SosPayload sos -> {
                data.put("sosId", sos.sosId());
                data.put("raisedBy", sos.raisedBy().name());
                data.put("lat", sos.position().lat());
                data.put("lon", sos.position().lon());
            }
        }
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Event could not be serialized", e);
        }
    }

    TripEvent decode(String json) {
        JsonNode node;
        try {
            node = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new DomainValidationException("event", "is not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw new DomainValidationException("event", "must be a JSON object");
        }
        EventType type = enumValue(EventType.class, node, "type");
        JsonNode data = node.get("data");
        if (data == null || !data.isObject()) {
            throw new DomainValidationException("data", "is required");
        }
        return new TripEvent(
                text(node, "eventId"),
                text(node, "tenantId"),
                text(node, "tripId"),
                text(node, "cabId"),
                instant(node, "occurredAt"),
                payload(type, data));
    }

    private static EventPayload payload(EventType type, JsonNode data) {
        return switch (type) {
            case LOCATION -> new LocationPayload(
                    point(data), optionalNumber(data, "speedKph"), optionalNumber(data, "heading"));
            case STATUS -> new StatusPayload(enumValue(TripStatus.class, data, "status"));
            case DELAY -> new DelayPayload(integer(data, "delayMinutes"), text(data, "reasonCode"));
            case SOS -> new SosPayload(
                    text(data, "sosId"), enumValue(SosRaiser.class, data, "raisedBy"), point(data));
        };
    }

    private static GeoPoint point(JsonNode data) {
        return new GeoPoint(number(data, "lat"), number(data, "lon"));
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new DomainValidationException(field, "must be a string");
        }
        return value.asText();
    }

    private static double number(JsonNode node, String field) {
        Double value = optionalNumber(node, field);
        if (value == null) {
            throw new DomainValidationException("data." + field, "is required");
        }
        return value;
    }

    private static Double optionalNumber(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isNumber()) {
            throw new DomainValidationException("data." + field, "must be a number");
        }
        return value.doubleValue();
    }

    private static int integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new DomainValidationException("data." + field, "is required");
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new DomainValidationException("data." + field, "must be an integer");
        }
        return value.intValue();
    }

    private static Instant instant(JsonNode node, String field) {
        String value = text(node, field);
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new DomainValidationException(field, "must be an ISO-8601 instant");
        }
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, JsonNode node, String field) {
        String value = text(node, field);
        if (value == null) {
            throw new DomainValidationException(field, "is required");
        }
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            throw new DomainValidationException(field, "has an unsupported value");
        }
    }
}

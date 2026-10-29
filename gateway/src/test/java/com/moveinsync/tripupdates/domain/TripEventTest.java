package com.moveinsync.tripupdates.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TripEventTest {

    static final Instant OCCURRED = Instant.parse("2026-10-29T10:00:00Z");
    static final GeoPoint POSITION = new GeoPoint(12.9716, 77.5946);

    static EventPayload payloadOf(EventType type) {
        return switch (type) {
            case LOCATION -> new LocationPayload(POSITION, 30.0, 90.0);
            case STATUS -> new StatusPayload(TripStatus.PICKED_UP);
            case DELAY -> new DelayPayload(10, "TRAFFIC");
            case SOS -> new SosPayload("SOS-1", SosRaiser.EMPLOYEE, POSITION);
        };
    }

    static TripEvent event(EventType type) {
        return new TripEvent("evt-1", "acme", "T-A1", "C-A1", OCCURRED, payloadOf(type));
    }

    @ParameterizedTest
    @EnumSource(EventType.class)
    void acceptsValidEventOfEveryType(EventType type) {
        TripEvent event = event(type);
        assertThat(event.type()).isEqualTo(type);
        assertThat(event.tenantId()).isEqualTo("acme");
        assertThat(event.hasTrip()).isTrue();
    }

    @Test
    void typeIsDerivedFromPayload() {
        TripEvent event = new TripEvent("evt-1", "acme", "T-A1", "C-A1", OCCURRED, new DelayPayload(3, "WEATHER"));
        assertThat(event.type()).isEqualTo(EventType.DELAY);
    }

    @Test
    void locationMayOmitTrip() {
        TripEvent event = new TripEvent("evt-1", "acme", null, "C-A1", OCCURRED, payloadOf(EventType.LOCATION));
        assertThat(event.hasTrip()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"STATUS", "DELAY", "SOS"})
    void tripScopedTypesRequireTrip(EventType type) {
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", null, "C-A1", OCCURRED, payloadOf(type)))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("tripId: is required for " + type);
    }

    @Test
    void rejectsMissingRequiredFields() {
        EventPayload status = payloadOf(EventType.STATUS);
        assertThatThrownBy(() -> new TripEvent(null, "acme", "T-A1", "C-A1", OCCURRED, status))
                .hasMessage("eventId: is required");
        assertThatThrownBy(() -> new TripEvent("evt-1", null, "T-A1", "C-A1", OCCURRED, status))
                .hasMessage("tenantId: is required");
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", "T-A1", null, OCCURRED, status))
                .hasMessage("cabId: is required");
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", "T-A1", "C-A1", null, status))
                .hasMessage("occurredAt: is required");
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", "T-A1", "C-A1", OCCURRED, null))
                .hasMessage("data: is required");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "evt 1", "evt/1", "evt:1"})
    void rejectsMalformedIdentifiers(String bad) {
        EventPayload status = payloadOf(EventType.STATUS);
        assertThatThrownBy(() -> new TripEvent(bad, "acme", "T-A1", "C-A1", OCCURRED, status))
                .extracting(e -> ((DomainValidationException) e).field()).isEqualTo("eventId");
        assertThatThrownBy(() -> new TripEvent("evt-1", bad, "T-A1", "C-A1", OCCURRED, status))
                .extracting(e -> ((DomainValidationException) e).field()).isEqualTo("tenantId");
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", bad, "C-A1", OCCURRED, status))
                .extracting(e -> ((DomainValidationException) e).field()).isEqualTo("tripId");
        assertThatThrownBy(() -> new TripEvent("evt-1", "acme", "T-A1", bad, OCCURRED, status))
                .extracting(e -> ((DomainValidationException) e).field()).isEqualTo("cabId");
    }

    @Test
    void acceptsUuidEventId() {
        TripEvent event = new TripEvent("7c0e5a7e-1b8f-4a63-9f39-2d1e8b9c4a10", "acme", "T-A1", "C-A1",
                OCCURRED, payloadOf(EventType.STATUS));
        assertThat(event.eventId()).hasSize(36);
    }

    @Test
    void toStringDoesNotLeakPositions() {
        assertThat(event(EventType.SOS).toString()).doesNotContain("12.97").doesNotContain("77.59");
        assertThat(event(EventType.LOCATION).toString()).doesNotContain("12.97").doesNotContain("77.59");
    }
}

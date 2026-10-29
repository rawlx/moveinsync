package com.moveinsync.tripupdates.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.LocationPayload;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EventJsonTest {

    private final EventJson json = new EventJson();

    @Test
    void roundTripsEveryEventType() {
        for (TripEvent event : new TripEvent[] {
                TestEvents.status("e1", TripStatus.PICKED_UP),
                TestEvents.delay("e2", 15),
                TestEvents.sos("e3"),
                TestEvents.location("e4", "T-1", "C-1", 12.5),
                TestEvents.location("e5", null, "C-1", -33.8688)}) {
            assertThat(json.decode(json.encode(event))).isEqualTo(event);
        }
    }

    @Test
    void roundTripsLocationWithoutOptionalFields() {
        TripEvent event = new TripEvent("e1", "acme", null, "C-1", TestEvents.OCCURRED,
                new LocationPayload(new GeoPoint(1.25, -2.5), null, null));
        String encoded = json.encode(event);
        assertThat(encoded).doesNotContain("speedKph").doesNotContain("heading").doesNotContain("tripId");
        assertThat(json.decode(encoded)).isEqualTo(event);
    }

    @Test
    void usesFlatWireFields() {
        assertThat(json.encode(TestEvents.status("e1", TripStatus.ARRIVED))).isEqualTo(
                "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"STATUS\",\"tripId\":\"T-1\",\"cabId\":\"C-1\","
                        + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{\"status\":\"ARRIVED\"}}");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not json",
            "[]",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"TELEPORT\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{}}",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"STATUS\",\"tripId\":\"T-1\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"yesterday\",\"data\":{\"status\":\"ARRIVED\"}}",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"STATUS\",\"tripId\":\"T-1\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\"}",
            "{\"eventId\":7,\"tenantId\":\"acme\",\"type\":\"STATUS\",\"tripId\":\"T-1\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{\"status\":\"ARRIVED\"}}",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"DELAY\",\"tripId\":\"T-1\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{\"delayMinutes\":1.5,\"reasonCode\":\"X\"}}",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"LOCATION\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{\"lat\":\"12\",\"lon\":77}}",
            "{\"eventId\":\"e1\",\"tenantId\":\"acme\",\"type\":\"LOCATION\",\"cabId\":\"C-1\","
                    + "\"occurredAt\":\"2026-10-29T10:00:00Z\",\"data\":{\"lat\":95,\"lon\":77}}",
    })
    void rejectsMalformedOrInvalidJson(String input) {
        assertThatThrownBy(() -> json.decode(input)).isInstanceOf(DomainValidationException.class);
    }
}

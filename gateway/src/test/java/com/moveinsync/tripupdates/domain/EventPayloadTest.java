package com.moveinsync.tripupdates.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EventPayloadTest {

    private static final GeoPoint BANGALORE = new GeoPoint(12.9716, 77.5946);

    @Nested
    class GeoPoints {

        @ParameterizedTest
        @CsvSource({"-90,-180", "90,180", "0,0"})
        void acceptsBoundaryCoordinates(double lat, double lon) {
            assertThat(new GeoPoint(lat, lon).lat()).isEqualTo(lat);
        }

        @ParameterizedTest
        @CsvSource({"90.0001,0,lat", "-90.0001,0,lat", "NaN,0,lat", "Infinity,0,lat",
                "0,180.0001,lon", "0,-180.0001,lon", "0,NaN,lon"})
        void rejectsOutOfRangeCoordinates(double lat, double lon, String field) {
            assertThatThrownBy(() -> new GeoPoint(lat, lon))
                    .isInstanceOf(DomainValidationException.class)
                    .extracting(e -> ((DomainValidationException) e).field())
                    .isEqualTo(field);
        }

        @Test
        void neverRevealsCoordinates() {
            assertThat(BANGALORE.toString()).doesNotContain("12.97").doesNotContain("77.59");
            assertThatThrownBy(() -> new GeoPoint(123.456789, 0)).message().doesNotContain("123.456789");
        }
    }

    @Nested
    class Location {

        @Test
        void acceptsFullAndMinimalPayloads() {
            assertThat(new LocationPayload(BANGALORE, 42.5, 359.9).type()).isEqualTo(EventType.LOCATION);
            assertThat(new LocationPayload(BANGALORE, null, null).speedKph()).isNull();
        }

        @Test
        void requiresPosition() {
            assertThatThrownBy(() -> new LocationPayload(null, 10.0, 10.0))
                    .isInstanceOf(DomainValidationException.class)
                    .hasMessage("data.position: is required");
        }

        @ParameterizedTest
        @ValueSource(doubles = {-0.1, 300.1, Double.NaN, Double.POSITIVE_INFINITY})
        void rejectsInvalidSpeed(double speed) {
            assertThatThrownBy(() -> new LocationPayload(BANGALORE, speed, null))
                    .isInstanceOf(DomainValidationException.class)
                    .extracting(e -> ((DomainValidationException) e).field())
                    .isEqualTo("data.speedKph");
        }

        @ParameterizedTest
        @ValueSource(doubles = {-1, 360, 720, Double.NaN})
        void rejectsInvalidHeading(double heading) {
            assertThatThrownBy(() -> new LocationPayload(BANGALORE, null, heading))
                    .isInstanceOf(DomainValidationException.class)
                    .extracting(e -> ((DomainValidationException) e).field())
                    .isEqualTo("data.heading");
        }

        @Test
        void toStringIsRedacted() {
            assertThat(new LocationPayload(BANGALORE, 42.0, 90.0).toString()).isEqualTo("LocationPayload[redacted]");
        }
    }

    @Nested
    class Status {

        @Test
        void acceptsEveryStatus() {
            for (TripStatus status : TripStatus.values()) {
                assertThat(new StatusPayload(status).type()).isEqualTo(EventType.STATUS);
            }
        }

        @Test
        void requiresStatus() {
            assertThatThrownBy(() -> new StatusPayload(null))
                    .isInstanceOf(DomainValidationException.class)
                    .hasMessage("data.status: is required");
        }
    }

    @Nested
    class Delay {

        @Test
        void acceptsBoundaryValues() {
            assertThat(new DelayPayload(0, "TRAFFIC").type()).isEqualTo(EventType.DELAY);
            assertThat(new DelayPayload(DelayPayload.MAX_DELAY_MINUTES, "VEHICLE_BREAKDOWN").delayMinutes())
                    .isEqualTo(1440);
        }

        @ParameterizedTest
        @ValueSource(ints = {-1, 1441, Integer.MIN_VALUE})
        void rejectsOutOfRangeDelay(int minutes) {
            assertThatThrownBy(() -> new DelayPayload(minutes, "TRAFFIC"))
                    .isInstanceOf(DomainValidationException.class)
                    .extracting(e -> ((DomainValidationException) e).field())
                    .isEqualTo("data.delayMinutes");
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "traffic", "_TRAFFIC", "TRAFFIC JAM", "1TRAFFIC",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFG"})
        void rejectsMalformedReasonCode(String reason) {
            assertThatThrownBy(() -> new DelayPayload(5, reason))
                    .isInstanceOf(DomainValidationException.class)
                    .extracting(e -> ((DomainValidationException) e).field())
                    .isEqualTo("data.reasonCode");
        }

        @Test
        void requiresReasonCode() {
            assertThatThrownBy(() -> new DelayPayload(5, null))
                    .isInstanceOf(DomainValidationException.class)
                    .hasMessage("data.reasonCode: is required");
        }
    }

    @Nested
    class Sos {

        @Test
        void acceptsValidPayload() {
            SosPayload sos = new SosPayload("SOS-1", SosRaiser.EMPLOYEE, BANGALORE);
            assertThat(sos.type()).isEqualTo(EventType.SOS);
            assertThat(sos.type().isPriority()).isTrue();
        }

        @Test
        void rejectsMissingFields() {
            assertThatThrownBy(() -> new SosPayload(null, SosRaiser.DRIVER, BANGALORE))
                    .hasMessage("data.sosId: is required");
            assertThatThrownBy(() -> new SosPayload("SOS 1", SosRaiser.DRIVER, BANGALORE))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> new SosPayload("SOS-1", null, BANGALORE))
                    .hasMessage("data.raisedBy: is required");
            assertThatThrownBy(() -> new SosPayload("SOS-1", SosRaiser.DRIVER, null))
                    .hasMessage("data.position: is required");
        }

        @Test
        void toStringKeepsIdsButRedactsPosition() {
            String text = new SosPayload("SOS-1", SosRaiser.EMPLOYEE, BANGALORE).toString();
            assertThat(text).contains("SOS-1", "EMPLOYEE", "redacted").doesNotContain("12.97").doesNotContain("77.59");
        }
    }
}

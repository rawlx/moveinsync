package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.domain.DelayPayload;
import com.moveinsync.tripupdates.domain.EventPayload;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.LocationPayload;
import com.moveinsync.tripupdates.domain.SosPayload;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import java.time.Instant;
import java.util.List;

/** Event fixtures for store tests. */
final class TestEvents {

    static final Instant OCCURRED = Instant.parse("2026-10-29T10:00:00Z");
    static final GeoPoint POSITION = new GeoPoint(12.9716, 77.5946);

    private TestEvents() {
    }

    static TripEvent status(String tenant, String eventId, String tripId, String cabId, TripStatus status) {
        return new TripEvent(eventId, tenant, tripId, cabId, OCCURRED, new StatusPayload(status));
    }

    static TripEvent status(String eventId, TripStatus status) {
        return status("acme", eventId, "T-1", "C-1", status);
    }

    static TripEvent delay(String eventId, int minutes) {
        return new TripEvent(eventId, "acme", "T-1", "C-1", OCCURRED, new DelayPayload(minutes, "TRAFFIC"));
    }

    static TripEvent sos(String eventId) {
        return new TripEvent(eventId, "acme", "T-1", "C-1", OCCURRED,
                new SosPayload("SOS-" + eventId, SosRaiser.EMPLOYEE, POSITION));
    }

    static TripEvent location(String eventId, String tripId, String cabId, double lat) {
        EventPayload payload = new LocationPayload(new GeoPoint(lat, 77.5946), 30.0, 90.0);
        return new TripEvent(eventId, "acme", tripId, cabId, OCCURRED, payload);
    }

    /** trip:T-1, cab:C-1, office:O-1 */
    static List<TopicId> tripCabOffice() {
        return List.of(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"));
    }
}

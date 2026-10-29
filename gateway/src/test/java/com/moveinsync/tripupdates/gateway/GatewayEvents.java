package com.moveinsync.tripupdates.gateway;

import com.moveinsync.tripupdates.domain.DelayPayload;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.LocationPayload;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.SosPayload;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import java.time.Instant;
import java.util.List;

/** Event fixtures for gateway tests. Trip T-1 runs on cab C-1 in office O-1. */
public final class GatewayEvents {

    public static final Instant OCCURRED = Instant.parse("2026-10-29T10:00:00Z");
    public static final TopicId TRIP = TopicId.trip("T-1");
    public static final TopicId CAB = TopicId.cab("C-1");
    public static final TopicId OFFICE = TopicId.office("O-1");
    public static final List<TopicId> ALL = List.of(TRIP, CAB, OFFICE);

    private GatewayEvents() {
    }

    public static TripEvent status(String tenant, String eventId) {
        return new TripEvent(eventId, tenant, "T-1", "C-1", OCCURRED, new StatusPayload(TripStatus.DRIVER_EN_ROUTE));
    }

    public static TripEvent status(String eventId) {
        return status("acme", eventId);
    }

    public static TripEvent delay(String eventId) {
        return new TripEvent(eventId, "acme", "T-1", "C-1", OCCURRED, new DelayPayload(5, "TRAFFIC"));
    }

    public static TripEvent sos(String eventId) {
        return new TripEvent(eventId, "acme", "T-1", "C-1", OCCURRED,
                new SosPayload("S-" + eventId, SosRaiser.EMPLOYEE, new GeoPoint(12.9, 77.6)));
    }

    /** A location of {@code cabId} with no trip (deliverable on cab and office topics). */
    public static TripEvent location(String eventId, String cabId, double lat) {
        return new TripEvent(eventId, "acme", null, cabId, OCCURRED,
                new LocationPayload(new GeoPoint(lat, 77.6), null, null));
    }

    /** A sequenced event for outbox tests that do not need a store. */
    public static SequencedEvent sequenced(TripEvent event, TopicId topic, long seq) {
        return new SequencedEvent(event, topic, seq, OCCURRED);
    }
}

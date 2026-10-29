package com.moveinsync.tripupdates.routing;

import com.moveinsync.tripupdates.auth.TopicDirectory;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides the destination topics of a published event from the directory, never from the caller:
 * <table>
 *   <caption>Destinations</caption>
 *   <tr><th>Event</th><th>Topics</th></tr>
 *   <tr><td>STATUS, DELAY (always have a trip)</td><td>{@code trip:T}, {@code cab:C}, {@code office:O}</td></tr>
 *   <tr><td>SOS</td><td>{@code trip:T}, {@code cab:C}, {@code office:O}, {@code sos:O}</td></tr>
 *   <tr><td>LOCATION with a trip</td><td>{@code trip:T}, {@code cab:C}, {@code office:O}</td></tr>
 *   <tr><td>LOCATION without a trip</td><td>{@code cab:C}, {@code office:O} (O = the cab's office)</td></tr>
 * </table>
 * O is the trip's office when the event names a trip. Every lookup is in the event's own tenant, so ids of another
 * tenant are simply unknown. The router does not guess: a trip not in the directory, a cab that does not serve the
 * named trip, or an office that does not exist rejects the event with {@link RoutingException}. A location without a
 * trip is never attached to a trip, because the directory has no notion of a cab's current trip.
 *
 * <p>Plain Java (no Spring, no Redis); the result is ordered, duplicate-free and checked with
 * {@link SequencedEvent#requireDeliverableOn} before it is returned.
 */
public final class EventRouter {

    private final TopicDirectory directory;

    public EventRouter(TopicDirectory directory) {
        this.directory = directory;
    }

    /**
     * @throws RoutingException if the directory does not confirm the event's trip, cab or office
     */
    public List<TopicId> route(TripEvent event) {
        String tenant = event.tenantId();
        String officeId;
        Set<TopicId> topics = new LinkedHashSet<>();
        if (event.hasTrip()) {
            TopicDirectory.Trip trip = directory.trip(tenant, event.tripId())
                    .orElseThrow(() -> new RoutingException(RoutingException.Reason.UNKNOWN_TRIP));
            if (!trip.cabId().equals(event.cabId())) {
                throw new RoutingException(RoutingException.Reason.CAB_NOT_ON_TRIP);
            }
            officeId = trip.officeId();
            topics.add(TopicId.trip(event.tripId()));
        } else {
            officeId = directory.cabOffice(tenant, event.cabId())
                    .orElseThrow(() -> new RoutingException(RoutingException.Reason.UNKNOWN_CAB));
        }
        if (!directory.officeExists(tenant, officeId)) {
            throw new RoutingException(RoutingException.Reason.UNKNOWN_OFFICE);
        }
        topics.add(TopicId.cab(event.cabId()));
        topics.add(TopicId.office(officeId));
        if (event.type() == EventType.SOS) {
            topics.add(TopicId.sos(officeId));
        }
        List<TopicId> routed = List.copyOf(topics);
        routed.forEach(topic -> SequencedEvent.requireDeliverableOn(event, topic));
        return routed;
    }
}

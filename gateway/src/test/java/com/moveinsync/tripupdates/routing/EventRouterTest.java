package com.moveinsync.tripupdates.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.auth.TopicDirectory;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EventRouterTest {

    private static final GeoPoint POSITION = new GeoPoint(12.9, 77.6);
    private final EventRouter router = new EventRouter(DevFixtures.load("dev/dev-fixtures.json").directory());

    private static TripEvent event(String tenant, String tripId, String cabId, EventPayload payload) {
        return new TripEvent("e1", tenant, tripId, cabId, Instant.parse("2026-10-30T08:00:00Z"), payload);
    }

    @Test
    void statusAndDelayGoToTripCabAndTheTripsOffice() {
        List<TopicId> expected = List.of(TopicId.trip("T-2"), TopicId.cab("C-2"), TopicId.office("O-2"));
        assertThat(router.route(event("acme", "T-2", "C-2", new StatusPayload(TripStatus.PICKED_UP))))
                .containsExactlyElementsOf(expected);
        assertThat(router.route(event("acme", "T-2", "C-2", new DelayPayload(5, "TRAFFIC"))))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void sosAlsoGoesToTheOfficesSecurityTopic() {
        assertThat(router.route(event("acme", "T-1", "C-1", new SosPayload("S1", SosRaiser.EMPLOYEE, POSITION))))
                .containsExactly(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"), TopicId.sos("O-1"));
    }

    @Test
    void locationWithATripFollowsTheTripAndWithoutOneOnlyTheCabAndItsOffice() {
        LocationPayload location = new LocationPayload(POSITION, 20.0, 90.0);
        assertThat(router.route(event("acme", "T-1", "C-1", location)))
                .containsExactly(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"));
        assertThat(router.route(event("acme", null, "C-2", location)))
                .as("no trip is guessed for a cab").containsExactly(TopicId.cab("C-2"), TopicId.office("O-2"));
    }

    @Test
    void routesAreDuplicateFree() {
        for (EventPayload payload : List.of(new StatusPayload(TripStatus.ARRIVED), new DelayPayload(1, "TRAFFIC"),
                new SosPayload("S1", SosRaiser.DRIVER, POSITION), new LocationPayload(POSITION, null, null))) {
            List<TopicId> topics = router.route(event("acme", "T-1", "C-1", payload));
            assertThat(new HashSet<>(topics)).hasSameSizeAs(topics);
        }
    }

    @Test
    void unknownTripIsRejected() {
        assertThatThrownBy(() -> router.route(event("acme", "T-404", "C-1", new StatusPayload(TripStatus.ARRIVED))))
                .isInstanceOfSatisfying(RoutingException.class,
                        e -> assertThat(e.reason()).isEqualTo(RoutingException.Reason.UNKNOWN_TRIP))
                .message().doesNotContain("T-404");
    }

    @Test
    void cabThatDoesNotServeTheTripIsRejected() {
        assertThatThrownBy(() -> router.route(event("acme", "T-1", "C-2", new StatusPayload(TripStatus.ARRIVED))))
                .isInstanceOfSatisfying(RoutingException.class,
                        e -> assertThat(e.reason()).isEqualTo(RoutingException.Reason.CAB_NOT_ON_TRIP));
    }

    @Test
    void unknownCabOfATriplessLocationIsRejected() {
        assertThatThrownBy(() -> router.route(event("acme", null, "C-404", new LocationPayload(POSITION, null, null))))
                .isInstanceOfSatisfying(RoutingException.class,
                        e -> assertThat(e.reason()).isEqualTo(RoutingException.Reason.UNKNOWN_CAB));
    }

    @ParameterizedTest
    @ValueSource(strings = {"globex", "initech"})
    void lookupsStayInTheEventsTenant(String tenant) {
        // acme's T-2 / C-2 do not exist for globex (which has its own T-1) or for an unknown tenant.
        assertThatThrownBy(() -> router.route(event(tenant, "T-2", "C-2", new StatusPayload(TripStatus.ARRIVED))))
                .isInstanceOf(RoutingException.class);
        assertThatThrownBy(() -> router.route(event(tenant, null, "C-2", new LocationPayload(POSITION, null, null))))
                .isInstanceOf(RoutingException.class);
    }

    @Test
    void sameIdsInAnotherTenantRouteWithThatTenantsData() {
        // globex has its own T-1 on C-1 in O-1: routed, and the topics are tenant-scoped by the store.
        assertThat(router.route(event("globex", "T-1", "C-1", new StatusPayload(TripStatus.ARRIVED))))
                .containsExactly(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"));
    }

    @Test
    void inconsistentDirectoryDataIsRejectedRatherThanGuessed() {
        TopicDirectory broken = new InMemoryTopicDirectory(Map.of("acme", new InMemoryTopicDirectory.Tenant(
                Set.of("O-1"), Map.of("C-9", "O-9"),
                Map.of("T-9", new TopicDirectory.Trip("O-9", "C-9", Set.of("emp-9"))))));
        EventRouter brokenRouter = new EventRouter(broken);

        assertThatThrownBy(() -> brokenRouter.route(event("acme", "T-9", "C-9", new StatusPayload(TripStatus.ARRIVED))))
                .isInstanceOfSatisfying(RoutingException.class,
                        e -> assertThat(e.reason()).isEqualTo(RoutingException.Reason.UNKNOWN_OFFICE));
        assertThatThrownBy(() -> brokenRouter.route(event("acme", null, "C-9", new LocationPayload(POSITION, null, null))))
                .isInstanceOf(RoutingException.class);
    }
}

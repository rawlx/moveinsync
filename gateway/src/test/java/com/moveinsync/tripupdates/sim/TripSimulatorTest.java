package com.moveinsync.tripupdates.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import com.moveinsync.tripupdates.routing.EventRouter;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TripSimulatorTest {

    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-30T08:00:00Z"), ZoneOffset.UTC);

    private static TripSimulator simulator(long seed) {
        return new TripSimulator(TripSimulator.fleetOf(FIXTURES.directory()), seed, "run1", CLOCK, 2, 0.3);
    }

    private static List<TripEvent> run(TripSimulator simulator, int ticks) {
        List<TripEvent> events = new ArrayList<>();
        for (int i = 0; i < ticks; i++) {
            events.addAll(simulator.tick());
        }
        return events;
    }

    @Test
    void fleetCoversEveryTripOfEveryTenantInAStableOrder() {
        assertThat(TripSimulator.fleetOf(FIXTURES.directory())).containsExactly(
                new TripSimulator.FleetTrip("acme", "T-1", "C-1", "O-1"),
                new TripSimulator.FleetTrip("acme", "T-2", "C-2", "O-2"),
                new TripSimulator.FleetTrip("globex", "T-1", "C-1", "O-1"));
    }

    @Test
    void sameSeedProducesTheSameEvents() {
        assertThat(run(simulator(42), 15)).isEqualTo(run(simulator(42), 15));
        assertThat(run(simulator(42), 15)).isNotEqualTo(run(simulator(43), 15));
    }

    @Test
    void producesEveryEventTypeAndOnlyEventsTheRouterAccepts() {
        TripSimulator simulator = simulator(7);
        List<TripEvent> events = run(simulator, 20);
        events.add(simulator.sos(simulator.fleet().get(0), SosRaiser.EMPLOYEE));
        EventRouter router = new EventRouter(FIXTURES.directory());

        Set<EventType> types = EnumSet.noneOf(EventType.class);
        Set<String> ids = new HashSet<>();
        for (TripEvent event : events) {
            types.add(event.type());
            assertThat(ids.add(event.eventId())).as("unique id").isTrue();
            assertThat(router.route(event)).isNotEmpty();
        }
        assertThat(types).containsExactlyInAnyOrder(EventType.values());
        assertThat(events).extracting(TripEvent::tenantId).contains("acme", "globex");
    }

    @Test
    void statusFollowsTheTripLifecycleAndCabsKeepReportingAfterCompletion() {
        List<TripEvent> events = run(simulator(1), 20);
        for (TripSimulator.FleetTrip trip : TripSimulator.fleetOf(FIXTURES.directory())) {
            List<TripStatus> statuses = events.stream()
                    .filter(e -> e.tenantId().equals(trip.tenantId()) && trip.tripId().equals(e.tripId()))
                    .filter(e -> e.payload() instanceof StatusPayload)
                    .map(e -> ((StatusPayload) e.payload()).status())
                    .toList();
            assertThat(statuses).containsExactly(TripStatus.ASSIGNED, TripStatus.DRIVER_EN_ROUTE, TripStatus.ARRIVED,
                    TripStatus.PICKED_UP, TripStatus.DROPPED, TripStatus.COMPLETED);
        }
        List<TripEvent> lastTick = events.subList(events.size() - 3, events.size());
        assertThat(lastTick).allSatisfy(e -> {
            assertThat(e.type()).isEqualTo(EventType.LOCATION);
            assertThat(e.hasTrip()).as("trip completed").isFalse();
        });
    }

    @Test
    void scriptedEventsAreOnlyAllowedForFleetTrips() {
        TripSimulator simulator = simulator(1);
        assertThat(simulator.delay(simulator.fleet().get(1), 9, "WEATHER").type()).isEqualTo(EventType.DELAY);
        assertThatThrownBy(() -> simulator.status(new TripSimulator.FleetTrip("acme", "T-9", "C-9", "O-9"),
                TripStatus.ARRIVED)).isInstanceOf(IllegalArgumentException.class);
    }
}

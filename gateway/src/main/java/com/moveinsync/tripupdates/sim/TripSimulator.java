package com.moveinsync.tripupdates.sim;

import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.domain.DelayPayload;
import com.moveinsync.tripupdates.domain.EventPayload;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.LocationPayload;
import com.moveinsync.tripupdates.domain.SosPayload;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic generator of realistic trip traffic for tests, smoke runs and load tests. Events are plain
 * {@link TripEvent}s, to be published through {@code EventPublisher.publish(TripEvent)} (which routes them).
 *
 * <p>Each {@link #tick()} advances every simulated trip by one step: its cab reports a LOCATION (a short move near
 * its start point), the trip's STATUS advances every {@code statusEveryTicks} ticks through ASSIGNED → … → COMPLETED,
 * and a DELAY is reported with {@code delayProbability}. Once a trip is COMPLETED its cab keeps reporting locations
 * without a trip. SOS events are raised explicitly with {@link #sos}.
 *
 * <p>Given the same fleet, seed and run id, the sequence of events (ids, types, payloads, order) is identical;
 * only {@code occurredAt} follows the supplied clock. Event ids are {@code {runId}-{n}}, so separate runs against
 * the same Redis do not collide with each other's idempotency records.
 */
public final class TripSimulator {

    /** One trip of the fleet; must match the directory used by the gateways. */
    public record FleetTrip(String tenantId, String tripId, String cabId, String officeId) {
    }

    private static final TripStatus[] PROGRESSION = {TripStatus.ASSIGNED, TripStatus.DRIVER_EN_ROUTE,
            TripStatus.ARRIVED, TripStatus.PICKED_UP, TripStatus.DROPPED, TripStatus.COMPLETED};
    private static final String[] DELAY_REASONS = {"TRAFFIC", "WEATHER", "ROAD_CLOSURE", "VEHICLE_ISSUE"};

    private static final class TripState {
        final FleetTrip trip;
        final double baseLat;
        final double baseLon;
        int step;
        int statusIndex = -1;

        TripState(FleetTrip trip, double baseLat, double baseLon) {
            this.trip = trip;
            this.baseLat = baseLat;
            this.baseLon = baseLon;
        }

        boolean onTrip() {
            return statusIndex < PROGRESSION.length - 1;
        }
    }

    private final List<TripState> trips = new ArrayList<>();
    private final Random random;
    private final String runId;
    private final Clock clock;
    private final int statusEveryTicks;
    private final double delayProbability;
    private long counter;
    private long ticks;

    public TripSimulator(List<FleetTrip> fleet, long seed, String runId, Clock clock, int statusEveryTicks,
            double delayProbability) {
        if (fleet.isEmpty()) {
            throw new IllegalArgumentException("fleet must not be empty");
        }
        if (statusEveryTicks < 1 || delayProbability < 0 || delayProbability > 1) {
            throw new IllegalArgumentException("invalid simulation parameters");
        }
        this.random = new Random(seed);
        this.runId = runId;
        this.clock = clock;
        this.statusEveryTicks = statusEveryTicks;
        this.delayProbability = delayProbability;
        for (FleetTrip trip : fleet) {
            // Somewhere in Bengaluru, a few kilometres apart.
            trips.add(new TripState(trip, 12.90 + random.nextDouble() * 0.15, 77.55 + random.nextDouble() * 0.15));
        }
    }

    /** Every trip of a directory, in a stable order (tenant, then trip id). */
    public static List<FleetTrip> fleetOf(InMemoryTopicDirectory directory) {
        List<FleetTrip> fleet = new ArrayList<>();
        directory.tenants().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(tenant ->
                tenant.getValue().trips().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(trip ->
                        fleet.add(new FleetTrip(tenant.getKey(), trip.getKey(), trip.getValue().cabId(),
                                trip.getValue().officeId()))));
        return List.copyOf(fleet);
    }

    public List<FleetTrip> fleet() {
        return trips.stream().map(state -> state.trip).toList();
    }

    /** Advances the simulation by one step and returns the events it produced, in publication order. */
    public List<TripEvent> tick() {
        ticks++;
        List<TripEvent> events = new ArrayList<>();
        for (TripState state : trips) {
            state.step++;
            boolean advance = state.onTrip() && (state.statusIndex < 0 || ticks % statusEveryTicks == 0);
            if (advance) {
                state.statusIndex++;
                events.add(event(state, state.trip.tripId(), new StatusPayload(PROGRESSION[state.statusIndex])));
            }
            if (state.onTrip() && state.statusIndex >= 1 && random.nextDouble() < delayProbability) {
                events.add(event(state, state.trip.tripId(), new DelayPayload(1 + random.nextInt(20),
                        DELAY_REASONS[random.nextInt(DELAY_REASONS.length)])));
            }
            events.add(event(state, state.onTrip() ? state.trip.tripId() : null, new LocationPayload(position(state),
                    10 + random.nextDouble() * 40, random.nextDouble() * 359.9)));
        }
        return events;
    }

    /** An SOS on the given trip (raised during the trip, from the cab's current position). */
    public TripEvent sos(FleetTrip trip, SosRaiser raisedBy) {
        TripState state = stateOf(trip);
        return event(state, trip.tripId(), new SosPayload(runId + "-sos-" + (counter + 1), raisedBy, position(state)));
    }

    /** A specific status change on the given trip (for scripted scenarios). */
    public TripEvent status(FleetTrip trip, TripStatus status) {
        return event(stateOf(trip), trip.tripId(), new StatusPayload(status));
    }

    /** A delay on the given trip (for scripted scenarios). */
    public TripEvent delay(FleetTrip trip, int minutes, String reasonCode) {
        return event(stateOf(trip), trip.tripId(), new DelayPayload(minutes, reasonCode));
    }

    private TripEvent event(TripState state, String tripId, EventPayload payload) {
        counter++;
        return new TripEvent(runId + "-" + counter, state.trip.tenantId(), tripId, state.trip.cabId(),
                clock.instant(), payload);
    }

    private static GeoPoint position(TripState state) {
        double angle = state.step / 20.0;
        return new GeoPoint(state.baseLat + 0.002 * Math.sin(angle) + 0.0001 * state.step,
                state.baseLon + 0.002 * Math.cos(angle));
    }

    private TripState stateOf(FleetTrip trip) {
        return trips.stream().filter(state -> state.trip.equals(trip)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("trip is not part of this simulation"));
    }
}

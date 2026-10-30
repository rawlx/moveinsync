package com.moveinsync.tripupdates.loadtest;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.sim.SyntheticFleet;
import com.moveinsync.tripupdates.sim.TripSimulator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides, deterministically, which identity each simulated client uses, what it subscribes to and which gateway it
 * connects to first (round robin). Every planned subscription is one the authorizer permits, except the probes,
 * which deliberately request a topic they are not allowed to read and must be refused.
 */
final class ClientPlan {

    enum Kind {
        /** Employee on their own trip topic. */
        EMPLOYEE_TRIP,
        /** Office admin (security desk) on the office topic. */
        ADMIN_OFFICE,
        /** Office admin watching one cab of the office. */
        ADMIN_CAB,
        /** Security user on the office's SOS topic. */
        SECURITY_SOS,
        /** Authorisation probe: a subscription that must be refused. */
        PROBE
    }

    record ClientSpec(int id, Kind kind, String token, String tenantId, TopicId topic, int gateway) {

        TenantTopic tenantTopic() {
            return new TenantTopic(tenantId, topic);
        }
    }

    private ClientPlan() {
    }

    static List<ClientSpec> build(SyntheticFleet fleet, LoadTestConfig config) {
        Map<String, List<TripSimulator.FleetTrip>> tripsByOffice = new LinkedHashMap<>();
        for (TripSimulator.FleetTrip trip : fleet.trips()) {
            tripsByOffice.computeIfAbsent(trip.tenantId() + "/" + trip.officeId(), key -> new ArrayList<>()).add(trip);
        }
        List<ClientSpec> plan = new ArrayList<>();
        int gateways = config.gateways().size();
        for (List<TripSimulator.FleetTrip> officeTrips : tripsByOffice.values()) {
            TripSimulator.FleetTrip first = officeTrips.get(0);
            String tenant = first.tenantId();
            String office = first.officeId();
            for (int i = 0; i < config.securityPerOffice(); i++) {
                add(plan, Kind.SECURITY_SOS, SyntheticFleet.securityToken(tenant, office), tenant,
                        TopicId.sos(office), gateways);
            }
            for (int i = 0; i < config.adminsPerOffice(); i++) {
                add(plan, Kind.ADMIN_OFFICE, SyntheticFleet.adminToken(tenant, office), tenant,
                        TopicId.office(office), gateways);
            }
            for (int i = 0; i < config.cabWatchersPerOffice(); i++) {
                add(plan, Kind.ADMIN_CAB, SyntheticFleet.adminToken(tenant, office), tenant,
                        TopicId.cab(officeTrips.get(i % officeTrips.size()).cabId()), gateways);
            }
        }
        List<TripSimulator.FleetTrip> trips = fleet.trips();
        int passengers = fleet.spec().passengersPerTrip();
        for (int i = 0; i < config.probes(); i++) {
            // An employee asking for another trip of their own tenant: must be FORBIDDEN.
            TripSimulator.FleetTrip own = trips.get(i % trips.size());
            TripSimulator.FleetTrip other = trips.get((i + 1) % trips.size());
            if (!other.tenantId().equals(own.tenantId()) || other.tripId().equals(own.tripId())) {
                other = trips.get((i + 2) % trips.size());
            }
            add(plan, Kind.PROBE, fleet.employeeToken(own, 1), own.tenantId(), TopicId.trip(other.tripId()), gateways);
        }
        int employees = config.clients() - plan.size();
        if (employees < 0) {
            throw new IllegalArgumentException("clients must cover security, admin, cab-watcher and probe clients");
        }
        for (int i = 0; i < employees; i++) {
            TripSimulator.FleetTrip trip = trips.get(i % trips.size());
            int passenger = 1 + (i / trips.size()) % passengers; // beyond passengers*trips, identities are reused
            add(plan, Kind.EMPLOYEE_TRIP, fleet.employeeToken(trip, passenger), trip.tenantId(),
                    TopicId.trip(trip.tripId()), gateways);
        }
        return List.copyOf(plan);
    }

    /**
     * Clients that join after a gateway failure, all pinned to {@code gateway} (the restored replica), with ids
     * starting at {@code firstId}. Per office, in a repeating block of ten: one security user (SOS topic), two
     * office admins, two cab watchers and five employees, so every topic kind is exercised on the restored gateway.
     */
    static List<ClientSpec> buildLate(SyntheticFleet fleet, int count, int gateway, int firstId) {
        Map<String, List<TripSimulator.FleetTrip>> tripsByOffice = new LinkedHashMap<>();
        for (TripSimulator.FleetTrip trip : fleet.trips()) {
            tripsByOffice.computeIfAbsent(trip.tenantId() + "/" + trip.officeId(), key -> new ArrayList<>()).add(trip);
        }
        List<List<TripSimulator.FleetTrip>> offices = new ArrayList<>(tripsByOffice.values());
        List<ClientSpec> late = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<TripSimulator.FleetTrip> officeTrips = offices.get((i / 10) % offices.size());
            TripSimulator.FleetTrip trip = officeTrips.get((i / (10 * offices.size())) % officeTrips.size());
            String tenant = trip.tenantId();
            String office = trip.officeId();
            int id = firstId + i;
            ClientSpec spec = switch (i % 10) {
                case 0 -> new ClientSpec(id, Kind.SECURITY_SOS, SyntheticFleet.securityToken(tenant, office), tenant,
                        TopicId.sos(office), gateway);
                case 1, 2 -> new ClientSpec(id, Kind.ADMIN_OFFICE, SyntheticFleet.adminToken(tenant, office), tenant,
                        TopicId.office(office), gateway);
                case 3, 4 -> new ClientSpec(id, Kind.ADMIN_CAB, SyntheticFleet.adminToken(tenant, office), tenant,
                        TopicId.cab(trip.cabId()), gateway);
                default -> new ClientSpec(id, Kind.EMPLOYEE_TRIP, fleet.employeeToken(trip, 1), tenant,
                        TopicId.trip(trip.tripId()), gateway);
            };
            late.add(spec);
        }
        return List.copyOf(late);
    }

    private static void add(List<ClientSpec> plan, Kind kind, String token, String tenant, TopicId topic,
            int gateways) {
        int id = plan.size();
        plan.add(new ClientSpec(id, kind, token, tenant, topic, id % gateways));
    }
}

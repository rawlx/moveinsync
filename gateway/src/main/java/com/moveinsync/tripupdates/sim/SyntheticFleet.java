package com.moveinsync.tripupdates.sim;

import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.auth.Role;
import com.moveinsync.tripupdates.auth.TopicDirectory;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DEVELOPMENT AND LOAD TEST ONLY. A deterministic fleet generated from a few numbers, so the gateways (directory and
 * dev tokens) and a load generator (publishers and clients) agree on the same data without shipping a large fixture
 * file. Ids only, no personal data.
 *
 * <pre>
 * tenant   syn{t}                         t = 1..tenants
 * office   O{o}                           o = 1..officesPerTenant
 * cab/trip C{o}-{c} / T{o}-{c}            c = 1..cabsPerOffice, one trip per cab
 * tokens   syn{t}-emp-{o}-{c}-{p}         EMPLOYEE, passenger p (1..passengersPerTrip) of T{o}-{c}
 *          syn{t}-admin-{o}               OFFICE_ADMIN of O{o}
 *          syn{t}-sec-{o}                 SECURITY, granted sos:O{o}
 * </pre>
 * The same ids are reused in every tenant on purpose, so tenant isolation is exercised by identical ids.
 */
public final class SyntheticFleet {

    public record Spec(int tenants, int officesPerTenant, int cabsPerOffice, int passengersPerTrip) {

        public Spec {
            if (tenants < 1 || officesPerTenant < 1 || cabsPerOffice < 1 || passengersPerTrip < 1) {
                throw new IllegalArgumentException("synthetic fleet dimensions must be positive");
            }
        }

        public int cabs() {
            return tenants * officesPerTenant * cabsPerOffice;
        }
    }

    private final Spec spec;
    private final InMemoryTopicDirectory directory;
    private final Map<String, Identity> identities;
    private final List<TripSimulator.FleetTrip> trips;

    private SyntheticFleet(Spec spec, InMemoryTopicDirectory directory, Map<String, Identity> identities,
            List<TripSimulator.FleetTrip> trips) {
        this.spec = spec;
        this.directory = directory;
        this.identities = identities;
        this.trips = trips;
    }

    public static SyntheticFleet generate(Spec spec) {
        Map<String, InMemoryTopicDirectory.Tenant> tenants = new HashMap<>();
        Map<String, Identity> identities = new LinkedHashMap<>();
        List<TripSimulator.FleetTrip> trips = new ArrayList<>();
        for (int t = 1; t <= spec.tenants(); t++) {
            String tenant = tenantId(t);
            Set<String> offices = new HashSet<>();
            Map<String, String> cabOffices = new HashMap<>();
            Map<String, TopicDirectory.Trip> tenantTrips = new HashMap<>();
            for (int o = 1; o <= spec.officesPerTenant(); o++) {
                String office = "O" + o;
                offices.add(office);
                identities.put(adminToken(tenant, office),
                        new Identity("admin-" + o, tenant, Role.OFFICE_ADMIN, Set.of(office), Set.of()));
                identities.put(securityToken(tenant, office), new Identity("sec-" + o, tenant, Role.SECURITY, Set.of(),
                        Set.of(TopicId.sos(office))));
                for (int c = 1; c <= spec.cabsPerOffice(); c++) {
                    String cab = "C" + o + "-" + c;
                    String trip = "T" + o + "-" + c;
                    cabOffices.put(cab, office);
                    Set<String> passengers = new HashSet<>();
                    for (int p = 1; p <= spec.passengersPerTrip(); p++) {
                        String subject = "e" + o + "-" + c + "-" + p;
                        passengers.add(subject);
                        identities.put(employeeToken(tenant, o, c, p),
                                new Identity(subject, tenant, Role.EMPLOYEE, Set.of(), Set.of()));
                    }
                    tenantTrips.put(trip, new TopicDirectory.Trip(office, cab, passengers));
                    trips.add(new TripSimulator.FleetTrip(tenant, trip, cab, office));
                }
            }
            tenants.put(tenant, new InMemoryTopicDirectory.Tenant(offices, cabOffices, tenantTrips));
        }
        return new SyntheticFleet(spec, new InMemoryTopicDirectory(tenants), Map.copyOf(identities),
                List.copyOf(trips));
    }

    public static String tenantId(int t) {
        return "syn" + t;
    }

    public static String employeeToken(String tenant, int office, int cab, int passenger) {
        return tenant + "-emp-" + office + "-" + cab + "-" + passenger;
    }

    public static String adminToken(String tenant, String officeId) {
        return tenant + "-admin-" + officeId.substring(1);
    }

    public static String securityToken(String tenant, String officeId) {
        return tenant + "-sec-" + officeId.substring(1);
    }

    /** Office number and cab number of a synthetic trip or cab id ({@code T3-17} / {@code C3-17}). */
    static int[] officeAndCab(String id) {
        String[] parts = id.substring(1).split("-");
        return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
    }

    /** Token of passenger {@code passenger} (1-based) of a fleet trip. */
    public String employeeToken(TripSimulator.FleetTrip trip, int passenger) {
        int[] ids = officeAndCab(trip.tripId());
        return employeeToken(trip.tenantId(), ids[0], ids[1], passenger);
    }

    public Spec spec() {
        return spec;
    }

    public InMemoryTopicDirectory directory() {
        return directory;
    }

    /** Dev token → identity. */
    public Map<String, Identity> identities() {
        return identities;
    }

    /** Every trip, ordered by tenant, office, cab. */
    public List<TripSimulator.FleetTrip> trips() {
        return trips;
    }
}

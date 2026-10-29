package com.moveinsync.tripupdates.auth;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** {@link TopicDirectory} over fixed in-memory data (development fixtures and tests). */
public final class InMemoryTopicDirectory implements TopicDirectory {

    /** One tenant's reference data. */
    public record Tenant(Set<String> offices, Map<String, String> cabOffices, Map<String, Trip> trips) {

        public Tenant {
            offices = Set.copyOf(offices);
            cabOffices = Map.copyOf(cabOffices);
            trips = Map.copyOf(trips);
        }
    }

    private final Map<String, Tenant> tenants;

    public InMemoryTopicDirectory(Map<String, Tenant> tenants) {
        this.tenants = Map.copyOf(tenants);
    }

    /** The data per tenant (immutable), e.g. to build a simulated fleet that matches this directory. */
    public Map<String, Tenant> tenants() {
        return tenants;
    }

    @Override
    public Optional<Trip> trip(String tenantId, String tripId) {
        return tenant(tenantId).map(tenant -> tenant.trips().get(tripId));
    }

    @Override
    public Optional<String> cabOffice(String tenantId, String cabId) {
        return tenant(tenantId).map(tenant -> tenant.cabOffices().get(cabId));
    }

    @Override
    public boolean officeExists(String tenantId, String officeId) {
        return tenant(tenantId).map(tenant -> tenant.offices().contains(officeId)).orElse(false);
    }

    private Optional<Tenant> tenant(String tenantId) {
        return Optional.ofNullable(tenants.get(tenantId));
    }
}

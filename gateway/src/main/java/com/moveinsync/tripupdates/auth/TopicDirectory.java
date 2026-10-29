package com.moveinsync.tripupdates.auth;

import java.util.Optional;
import java.util.Set;

/**
 * Tenant-scoped reference data needed to authorise topic subscriptions. Every lookup takes the caller's tenant, so an
 * id of another tenant is simply not found.
 */
public interface TopicDirectory {

    record Trip(String officeId, String cabId, Set<String> passengers) {

        public Trip {
            passengers = Set.copyOf(passengers);
        }
    }

    Optional<Trip> trip(String tenantId, String tripId);

    /** The office a cab belongs to. */
    Optional<String> cabOffice(String tenantId, String cabId);

    boolean officeExists(String tenantId, String officeId);
}

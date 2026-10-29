package com.moveinsync.tripupdates.auth;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.Identifiers;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.Set;

/**
 * An authenticated client, as established by a {@link ConnectionAuthenticator} from server-side data. Tenant, role,
 * offices and grants never come from client messages.
 *
 * @param subject   the user's id (employees: matched against trip passengers)
 * @param officeIds offices administered (OFFICE_ADMIN only)
 * @param grants    explicitly granted topics (SECURITY only)
 */
public record Identity(String subject, String tenantId, Role role, Set<String> officeIds, Set<TopicId> grants) {

    public Identity {
        Identifiers.require(subject, "subject");
        Identifiers.require(tenantId, "tenantId");
        if (role == null) {
            throw new DomainValidationException("role", "is required");
        }
        officeIds = officeIds == null ? Set.of() : Set.copyOf(officeIds);
        officeIds.forEach(office -> Identifiers.require(office, "officeId"));
        grants = grants == null ? Set.of() : Set.copyOf(grants);
    }

    @Override
    public String toString() {
        return "Identity[" + subject + ", tenant=" + tenantId + ", role=" + role + "]";
    }
}

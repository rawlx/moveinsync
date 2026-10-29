package com.moveinsync.tripupdates.auth;

import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TopicKind;
import java.util.Optional;

/**
 * Decides whether an identity may read a topic. Evaluated on every SUBSCRIBE and RESUME, not only at login.
 * <ul>
 *   <li>All lookups are in the identity's own tenant: a topic of another tenant does not exist for this caller.</li>
 *   <li>EMPLOYEE: {@code trip:T} only if the employee is a passenger of T.</li>
 *   <li>OFFICE_ADMIN: {@code office:O} for administered offices, and {@code trip:T} / {@code cab:C} whose office is
 *       administered.</li>
 *   <li>SECURITY: only {@code sos:O} topics explicitly granted, for offices that exist.</li>
 * </ul>
 * Unknown topics and forbidden topics yield the same answer, so a denial reveals nothing about existence.
 */
public final class TopicAuthorizer {

    private final TopicDirectory directory;

    public TopicAuthorizer(TopicDirectory directory) {
        this.directory = directory;
    }

    public boolean mayRead(Identity identity, TopicId topic) {
        String tenant = identity.tenantId();
        return switch (identity.role()) {
            case EMPLOYEE -> switch (topic.kind()) {
                case TRIP -> directory.trip(tenant, topic.id())
                        .map(trip -> trip.passengers().contains(identity.subject()))
                        .orElse(false);
                case CAB, OFFICE, SOS -> false;
            };
            case OFFICE_ADMIN -> {
                Optional<String> office = switch (topic.kind()) {
                    case OFFICE -> directory.officeExists(tenant, topic.id()) ? Optional.of(topic.id()) : Optional.empty();
                    case TRIP -> directory.trip(tenant, topic.id()).map(TopicDirectory.Trip::officeId);
                    case CAB -> directory.cabOffice(tenant, topic.id());
                    case SOS -> Optional.empty();
                };
                yield office.map(identity.officeIds()::contains).orElse(false);
            }
            case SECURITY -> topic.kind() == TopicKind.SOS
                    && identity.grants().contains(topic)
                    && directory.officeExists(tenant, topic.id());
        };
    }
}

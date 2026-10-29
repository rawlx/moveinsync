package com.moveinsync.tripupdates.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.domain.TopicId;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TopicAuthorizerTest {

    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");
    private final TopicAuthorizer authorizer = new TopicAuthorizer(FIXTURES.directory());

    private static Identity identity(String token) {
        return FIXTURES.authenticator().authenticate(token).orElseThrow();
    }

    private boolean may(String token, String topic) {
        return authorizer.mayRead(identity(token), TopicId.parse(topic));
    }

    @Test
    void employeeMayReadOnlyTheirOwnTrip() {
        assertThat(may("dev-acme-emp-1", "trip:T-1")).isTrue();
        assertThat(may("dev-acme-emp-1", "trip:T-2")).as("another employee's trip").isFalse();
        assertThat(may("dev-acme-emp-1", "trip:T-404")).as("unknown trip").isFalse();
        assertThat(may("dev-acme-emp-1", "cab:C-1")).isFalse();
        assertThat(may("dev-acme-emp-1", "office:O-1")).isFalse();
        assertThat(may("dev-acme-emp-1", "sos:O-1")).isFalse();
    }

    @Test
    void officeAdminMayReadOnlyTheirOfficesTopicsTripsAndCabs() {
        assertThat(may("dev-acme-admin-o1", "office:O-1")).isTrue();
        assertThat(may("dev-acme-admin-o1", "trip:T-1")).isTrue();
        assertThat(may("dev-acme-admin-o1", "cab:C-1")).isTrue();
        assertThat(may("dev-acme-admin-o1", "office:O-2")).isFalse();
        assertThat(may("dev-acme-admin-o1", "trip:T-2")).as("trip of office O-2").isFalse();
        assertThat(may("dev-acme-admin-o1", "cab:C-2")).isFalse();
        assertThat(may("dev-acme-admin-o1", "sos:O-1")).as("SOS topics are for security users").isFalse();
        assertThat(may("dev-acme-admin-o1", "office:O-9")).as("unknown office").isFalse();
    }

    @Test
    void securityUserMayReadOnlyExplicitlyGrantedSosTopics() {
        assertThat(may("dev-acme-security-o1", "sos:O-1")).isTrue();
        assertThat(may("dev-acme-security-o1", "sos:O-2")).as("not granted").isFalse();
        assertThat(may("dev-acme-security-o1", "office:O-1")).isFalse();
        assertThat(may("dev-acme-security-o1", "trip:T-1")).isFalse();
    }

    @Test
    void identicalIdsOfAnotherTenantAreNeverVisible() {
        // globex has its own trip T-1 / office O-1; acme users cannot see them and vice versa.
        assertThat(may("dev-globex-emp-1", "trip:T-1")).as("globex's own T-1").isTrue();
        assertThat(may("dev-acme-emp-1", "trip:T-1")).as("acme's own T-1").isTrue();
        Identity acmeEmployeeClaimingGlobexTrip = new Identity("g-emp-1", "acme", Role.EMPLOYEE, Set.of(), Set.of());
        assertThat(authorizer.mayRead(acmeEmployeeClaimingGlobexTrip, TopicId.trip("T-1")))
                .as("passenger of globex T-1, but authenticated in acme").isFalse();
        assertThat(may("dev-globex-admin-o1", "trip:T-2")).as("acme-only trip").isFalse();
    }

    @Test
    void securityGrantForAnOfficeThatDoesNotExistIsIgnored() {
        Identity security = new Identity("s", "acme", Role.SECURITY, Set.of(), Set.of(TopicId.sos("O-9")));
        assertThat(authorizer.mayRead(security, TopicId.sos("O-9"))).isFalse();
    }

    @Test
    void devTokensMapToServerSideIdentitiesOnly() {
        assertThat(FIXTURES.authenticator().authenticate("dev-acme-emp-1"))
                .hasValueSatisfying(identity -> {
                    assertThat(identity.tenantId()).isEqualTo("acme");
                    assertThat(identity.role()).isEqualTo(Role.EMPLOYEE);
                });
        assertThat(FIXTURES.authenticator().authenticate("unknown")).isEmpty();
        assertThat(FIXTURES.authenticator().authenticate(null)).isEmpty();
        assertThat(FIXTURES.authenticator().authenticate("")).isEmpty();
    }
}

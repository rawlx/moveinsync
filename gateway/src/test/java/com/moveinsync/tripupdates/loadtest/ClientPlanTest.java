package com.moveinsync.tripupdates.loadtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.auth.DevTokenAuthenticator;
import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.routing.EventRouter;
import com.moveinsync.tripupdates.sim.SyntheticFleet;
import com.moveinsync.tripupdates.sim.TripSimulator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ClientPlanTest {

    private static LoadTestConfig config(int clients) {
        return LoadTestConfig.from(Map.of("LOADTEST_CLIENTS", Integer.toString(clients)));
    }

    @Test
    void fullScalePlanHasTenThousandClientsAllAuthorisedExceptTheProbes() {
        SyntheticFleet fleet = SyntheticFleet.generate(new SyntheticFleet.Spec(2, 10, 100, 4));
        List<ClientPlan.ClientSpec> plan = ClientPlan.build(fleet, config(10_000));
        TopicAuthorizer authorizer = new TopicAuthorizer(fleet.directory());
        DevTokenAuthenticator authenticator = new DevTokenAuthenticator(fleet.identities());

        assertThat(plan).hasSize(10_000);
        Map<ClientPlan.Kind, Long> kinds = plan.stream()
                .collect(Collectors.groupingBy(ClientPlan.ClientSpec::kind, Collectors.counting()));
        assertThat(kinds).containsEntry(ClientPlan.Kind.SECURITY_SOS, 40L)
                .containsEntry(ClientPlan.Kind.ADMIN_OFFICE, 200L)
                .containsEntry(ClientPlan.Kind.ADMIN_CAB, 200L)
                .containsEntry(ClientPlan.Kind.PROBE, 20L)
                .containsEntry(ClientPlan.Kind.EMPLOYEE_TRIP, 9_540L);
        for (ClientPlan.ClientSpec spec : plan) {
            Identity identity = authenticator.authenticate(spec.token()).orElseThrow();
            assertThat(identity.tenantId()).isEqualTo(spec.tenantId());
            assertThat(authorizer.mayRead(identity, spec.topic()))
                    .as("%s %s %s", spec.kind(), spec.token(), spec.topic())
                    .isEqualTo(spec.kind() != ClientPlan.Kind.PROBE);
        }
        Map<Integer, Long> perGateway = plan.stream()
                .collect(Collectors.groupingBy(ClientPlan.ClientSpec::gateway, Collectors.counting()));
        assertThat(perGateway.values()).allSatisfy(count -> assertThat(count).isBetween(3_333L, 3_334L));
    }

    @Test
    void syntheticFleetIsDeterministicAndEveryEventItProducesIsRoutable() {
        SyntheticFleet fleet = SyntheticFleet.generate(new SyntheticFleet.Spec(2, 2, 3, 2));
        assertThat(fleet.trips()).hasSize(12);
        assertThat(fleet.identities()).hasSize(2 * 2 * (2 + 3 * 2));
        assertThat(SyntheticFleet.generate(new SyntheticFleet.Spec(2, 2, 3, 2)).trips()).isEqualTo(fleet.trips());

        EventRouter router = new EventRouter(fleet.directory());
        TripSimulator simulator = new TripSimulator(fleet.trips(), 1, "t", java.time.Clock.systemUTC(), 1, 0.5);
        for (int i = 0; i < 8; i++) {
            simulator.tick().forEach(event -> assertThat(router.route(event)).isNotEmpty());
        }
        assertThat(fleet.trips()).extracting(TripSimulator.FleetTrip::tenantId).containsOnly("syn1", "syn2");
    }

    @Test
    void planRejectsTooFewClientsForTheFixedRoles() {
        SyntheticFleet fleet = SyntheticFleet.generate(new SyntheticFleet.Spec(2, 10, 100, 4));
        assertThatThrownBy(() -> ClientPlan.build(fleet, config(100))).isInstanceOf(IllegalArgumentException.class);
    }
}

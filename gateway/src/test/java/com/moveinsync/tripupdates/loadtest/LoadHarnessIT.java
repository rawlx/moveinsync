package com.moveinsync.tripupdates.loadtest;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The load-test harness at small scale against a real gateway and Redis: clients connect, publishers and the SOS
 * injector run, every server-side session is dropped mid-run (as a gateway failure would), clients reconnect and
 * RESUME, and the harness's own verification must find every reliable event and every SOS delivered.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LoadHarnessIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("app.redis.uri", LoadHarnessIT::redisUri);
        registry.add("app.auth.mode", () -> "DEV");
        registry.add("app.auth.synthetic-fleet.tenants", () -> "2");
        registry.add("app.auth.synthetic-fleet.offices-per-tenant", () -> "2");
        registry.add("app.auth.synthetic-fleet.cabs-per-office", () -> "3");
        registry.add("app.auth.synthetic-fleet.passengers-per-trip", () -> "2");
    }

    static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private SubscriptionRegistry registry;

    @Test
    void harnessMeasuresAndVerifiesDeliveryAcrossAForcedDisconnectOfEveryClient() throws Exception {
        LoadTestConfig config = new LoadTestConfig("it-" + System.currentTimeMillis(), "target/loadtest-it",
                List.of("ws://localhost:" + port + "/ws"), redisUri(),
                2, 2, 3, 2,
                44, 2, 2, 1, 4,
                200, 50, 2,
                Duration.ofSeconds(30), Duration.ofSeconds(2), Duration.ofSeconds(8), Duration.ofSeconds(6),
                Duration.ofMillis(400), 2, 0.3, 4.0, 3, Duration.ofSeconds(2), 7L, 0, 0, Duration.ofSeconds(30));

        LoadReport report = new LoadTest(config).run(
                () -> registry.sessions().forEach(session -> session.close(CloseReason.SHUTDOWN)),
                Duration.ofSeconds(3));
        System.out.println(report.summary());

        assertThat(report.establishedAtConnected()).isEqualTo(40);
        assertThat(report.clientsFailedToConnect()).isZero();
        assertThat(report.probeAnswers()).containsEntry("FORBIDDEN", 4L);
        assertThat(report.totalPublished()).isPositive();
        assertThat(report.publishedByType()).allSatisfy((type, count) -> assertThat(count).as(type).isPositive());
        assertThat(report.publishFailures()).isZero();
        report.clients().forEach((kind, result) -> {
            assertThat(result.missingReliable).as("%s missing", kind).isZero();
            assertThat(result.unexpectedReliable).as("%s unexpected", kind).isZero();
            assertThat(result.tenantViolations).as("%s tenant", kind).isZero();
        });
        assertThat(report.clients().get(ClientPlan.Kind.EMPLOYEE_TRIP).expectedReliable).isPositive();
        assertThat(report.clients().get(ClientPlan.Kind.EMPLOYEE_TRIP).reconnects).as("forced disconnect")
                .isPositive();
        assertThat(report.reconnectOutage().count()).isPositive();
        assertThat(report.sos().published()).isPositive();
        assertThat(report.sos().missing()).isZero();
        assertThat(report.sos().delivered()).isEqualTo(report.sos().expectedDeliveries());
        assertThat(report.sos().live().count()).isPositive();
        assertThat(report.firstDisconnectAtSeconds()).isNotNull();
    }
}

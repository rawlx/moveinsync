package com.moveinsync.tripupdates.ws;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.routing.EventRouter;
import com.moveinsync.tripupdates.sim.TripSimulator;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.RedisClients;
import com.moveinsync.tripupdates.store.RedisEventStore;
import io.lettuce.core.RedisClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * End-to-end smoke test against the running Compose stack ({@code docker compose up --build -d}). Opt-in:
 * <pre>
 * mvnw -pl gateway verify -Dit.test=ComposeSmokeIT -Dsmoke.compose=true -Dsurefire.skip=true
 * </pre>
 * Events are published from this JVM through {@link EventPublisher} (with {@link EventRouter} and
 * {@link TripSimulator}) into the stack's Redis, exactly as any publisher process would; WebSocket clients connect to
 * specific gateway replicas (host ports 8081-8083) and through nginx (8080) with dev tokens.
 * Ports can be overridden with {@code smoke.nginx}, {@code smoke.gateways} and {@code smoke.redis}.
 */
@EnabledIfSystemProperty(named = "smoke.compose", matches = "true")
class ComposeSmokeIT {

    private static final int NGINX = Integer.getInteger("smoke.nginx", 8080);
    private static final int[] GATEWAYS = Arrays.stream(System.getProperty("smoke.gateways", "8081,8082,8083")
            .split(",")).mapToInt(Integer::parseInt).toArray();
    private static final String REDIS = System.getProperty("smoke.redis", "redis://localhost:16379");
    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");
    private static final TripSimulator.FleetTrip ACME_T1 = new TripSimulator.FleetTrip("acme", "T-1", "C-1", "O-1");
    private static final TenantTopic ACME_TRIP = new TenantTopic("acme", TopicId.trip("T-1"));
    private static final TenantTopic ACME_OFFICE = new TenantTopic("acme", TopicId.office("O-1"));
    private static final TenantTopic ACME_SOS = new TenantTopic("acme", TopicId.sos("O-1"));

    private static RedisClient client;
    private static RedisEventStore store;
    private static RedisNotificationPublisher notifications;
    private static EventPublisher publisher;
    private static TripSimulator simulator;

    @BeforeAll
    static void connect() {
        client = RedisClients.create(REDIS, Duration.ofSeconds(2));
        store = new RedisEventStore(client, new AppProperties.Retention(1000, 100_000, Duration.ofHours(24),
                Duration.ofMinutes(5)));
        notifications = new RedisNotificationPublisher(client, "notify:events");
        publisher = new EventPublisher(store, notifications, new EventRouter(FIXTURES.directory()));
        // Fixed seed, unique run id: the same traffic every run without colliding with earlier runs' event ids.
        simulator = new TripSimulator(TripSimulator.fleetOf(FIXTURES.directory()), 20261030L,
                "smoke-" + System.currentTimeMillis(), Clock.systemUTC(), 1, 0.5);
    }

    @AfterAll
    static void disconnect() {
        notifications.close();
        store.close();
        client.shutdown();
    }

    /** Publishes through the router and returns the per-topic sequenced copies. */
    private static List<SequencedEvent> publish(TripEvent event) {
        AppendResult appended = publisher.publish(event).appended();
        return appended.events();
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(5))
                        .build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String subscribe(String id, String topic) {
        return "{\"type\":\"SUBSCRIBE\",\"id\":\"" + id + "\",\"topic\":\"" + topic + "\"}";
    }

    /** Reads frames until {@code reliable} non-location EVENTs arrived; returns all EVENT frames read. */
    private static List<JsonNode> readEvents(WsTestClient ws, int reliable) throws InterruptedException {
        List<JsonNode> events = new ArrayList<>();
        int seen = 0;
        while (seen < reliable) {
            JsonNode frame = ws.next();
            assertThat(frame.get("type").asText()).as(frame.toString()).isEqualTo("EVENT");
            events.add(frame);
            if (!"LOCATION".equals(frame.at("/event/type").asText())) {
                seen++;
            }
        }
        return events;
    }

    private static List<Long> reliableSeqs(List<JsonNode> frames) {
        return frames.stream().filter(f -> !"LOCATION".equals(f.at("/event/type").asText()))
                .map(f -> f.get("seq").asLong()).toList();
    }

    /** Sequences of the reliable events published on one tenant topic, in publication order. */
    private static List<Long> expectedReliable(List<List<SequencedEvent>> published, TenantTopic topic) {
        return published.stream().flatMap(List::stream)
                .filter(e -> e.tenantTopic().equals(topic) && !e.type().isConflatable())
                .map(SequencedEvent::seq).toList();
    }

    @Test
    void everyReplicaAndTheLoadBalancerAreReadyWithRedis() throws Exception {
        for (int port : GATEWAYS) {
            HttpResponse<String> readiness = get(port, "/actuator/health/readiness");
            assertThat(readiness.statusCode()).as("gateway on %d", port).isEqualTo(200);
            assertThat(readiness.body()).contains("\"redis\":{\"status\":\"UP\"");
        }
        assertThat(get(NGINX, "/actuator/health/readiness").statusCode()).isEqualTo(200);
        assertThat(get(NGINX, "/actuator/prometheus").body()).contains("tripupdates_sessions_active");
    }

    @Test
    void eventsReachAuthorisedSubscribersOnEveryReplicaAndNobodyElse() throws Exception {
        WsTestClient employee = WsTestClient.connect(GATEWAYS[0], "dev-acme-emp-1");
        WsTestClient desk = WsTestClient.connect(GATEWAYS[1], "dev-acme-admin-o1");
        WsTestClient security = WsTestClient.connect(GATEWAYS[2], "dev-acme-security-o1");
        WsTestClient otherEmployee = WsTestClient.connect(NGINX, "dev-acme-emp-2");
        WsTestClient globex = WsTestClient.connect(NGINX, "dev-globex-emp-1");
        employee.send(subscribe("e1", "trip:T-1"));
        desk.send(subscribe("d1", "office:O-1"));
        security.send(subscribe("s1", "sos:O-1"));
        otherEmployee.send(subscribe("o1", "trip:T-1")); // acme's T-1 is not theirs
        globex.send(subscribe("g1", "trip:T-1"));        // globex's own T-1
        for (WsTestClient ws : List.of(employee, desk, security, globex)) {
            assertThat(ws.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        }
        JsonNode denied = otherEmployee.next();
        assertThat(denied.get("code").asText()).isEqualTo("FORBIDDEN");

        // Simulated traffic for every fleet trip (both tenants), then a scripted delay and an SOS on acme T-1.
        List<List<SequencedEvent>> published = new ArrayList<>();
        for (int tick = 0; tick < 3; tick++) {
            for (TripEvent event : simulator.tick()) {
                published.add(publish(event));
            }
        }
        published.add(publish(simulator.delay(ACME_T1, 6, "TRAFFIC")));
        published.add(publish(simulator.sos(ACME_T1, SosRaiser.EMPLOYEE)));

        List<Long> tripExpected = expectedReliable(published, ACME_TRIP);
        List<JsonNode> employeeEvents = readEvents(employee, tripExpected.size());
        assertThat(reliableSeqs(employeeEvents)).as("employee on gateway 1").containsExactlyElementsOf(tripExpected);
        assertThat(employeeEvents).extracting(f -> f.get("seq").asLong()).isSorted();
        assertThat(employeeEvents).allSatisfy(f -> assertThat(f.at("/event/tenantId").asText()).isEqualTo("acme"));

        List<Long> officeExpected = expectedReliable(published, ACME_OFFICE);
        assertThat(reliableSeqs(readEvents(desk, officeExpected.size()))).as("desk on gateway 2")
                .containsExactlyElementsOf(officeExpected);

        List<JsonNode> alerts = readEvents(security, 1);
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).at("/event/type").asText()).isEqualTo("SOS");
        assertThat(alerts.get(0).get("seq").asLong()).isEqualTo(expectedReliable(published, ACME_SOS).get(0));
        assertThat(security.poll(500)).as("security receives SOS only").isNull();

        TenantTopic globexTrip = new TenantTopic("globex", TopicId.trip("T-1"));
        List<Long> globexExpected = expectedReliable(published, globexTrip);
        List<JsonNode> globexEvents = readEvents(globex, globexExpected.size());
        assertThat(reliableSeqs(globexEvents)).containsExactlyElementsOf(globexExpected);
        assertThat(globexEvents).allSatisfy(f -> assertThat(f.at("/event/tenantId").asText()).isEqualTo("globex"));
        JsonNode extra;
        while ((extra = globex.poll(500)) != null) {
            assertThat(extra.at("/event/tenantId").asText()).as("no acme data for globex").isEqualTo("globex");
        }
        assertThat(otherEmployee.poll(500)).as("forbidden client receives nothing").isNull();

        for (WsTestClient ws : List.of(employee, desk, security, otherEmployee, globex)) {
            ws.close();
        }
    }

    @Test
    void droppedClientResumesOnAnotherReplicaWithoutLosingReliableEvents() throws Exception {
        WsTestClient first = WsTestClient.connect(GATEWAYS[0], "dev-acme-admin-o1");
        first.send(subscribe("d1", "office:O-1"));
        assertThat(first.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        publish(simulator.status(ACME_T1, TripStatus.DRIVER_EN_ROUTE));
        long lastProcessed = reliableSeqs(readEvents(first, 1)).get(0);
        first.abort(); // connection drop

        List<List<SequencedEvent>> missed = List.of(
                publish(simulator.status(ACME_T1, TripStatus.ARRIVED)),
                publish(simulator.delay(ACME_T1, 3, "WEATHER")),
                publish(simulator.sos(ACME_T1, SosRaiser.DRIVER)));

        WsTestClient resumed = WsTestClient.connect(GATEWAYS[2], "dev-acme-admin-o1");
        resumed.send("{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"office:O-1\",\"lastSeq\":" + lastProcessed + "}");
        List<JsonNode> replay = resumed.next(4);
        assertThat(replay.subList(0, 3)).extracting(f -> f.at("/event/type").asText())
                .containsExactly("STATUS", "DELAY", "SOS");
        assertThat(replay.subList(0, 3)).extracting(f -> f.get("seq").asLong())
                .containsExactlyElementsOf(expectedReliable(missed, ACME_OFFICE));
        assertThat(replay.get(3).get("type").asText()).isEqualTo("RESUME_COMPLETE");

        List<SequencedEvent> live = publish(simulator.status(ACME_T1, TripStatus.PICKED_UP));
        assertThat(reliableSeqs(readEvents(resumed, 1)))
                .containsExactlyElementsOf(expectedReliable(List.of(live), ACME_OFFICE));
        resumed.close();
    }
}

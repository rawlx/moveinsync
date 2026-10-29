package com.moveinsync.tripupdates.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.store.AppendResult;
import java.net.http.WebSocketHandshakeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
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

/** {@code /ws} end to end: real Tomcat, real Redis, dev authentication, JDK WebSocket client. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketEndpointIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    private static final List<TopicId> T1_TOPICS =
            List.of(TopicId.trip("T-1"), TopicId.cab("C-1"), TopicId.office("O-1"));
    private static final AtomicInteger IDS = new AtomicInteger();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("app.redis.uri", () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        registry.add("app.auth.mode", () -> "DEV");
        registry.add("app.notifications.reconcile-interval", () -> "1h");
        registry.add("app.outbox.critical-capacity", () -> "64");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private EventPublisher publisher;

    @Autowired
    private SubscriptionRegistry registry;

    @Autowired
    private TripUpdatesWebSocketHandler handler;

    private static TripEvent status(String tenant, String tripId, String cabId) {
        return new TripEvent("e-" + IDS.incrementAndGet(), tenant, tripId, cabId, Instant.now(),
                new StatusPayload(TripStatus.DRIVER_EN_ROUTE));
    }

    private AppendResult publishAcmeT1() {
        return publisher.publish(status("acme", "T-1", "C-1"), T1_TOPICS).appended();
    }

    private static String subscribe(String id, String topic) {
        return "{\"type\":\"SUBSCRIBE\",\"id\":\"" + id + "\",\"topic\":\"" + topic + "\"}";
    }

    @Test
    void handshakeWithoutOrWithAnUnknownTokenIsRejected() {
        for (String token : new String[] {null, "not-a-token", "dev-acme-emp-1x"}) {
            assertThatThrownBy(() -> WsTestClient.connect(port, token))
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(WebSocketHandshakeException.class)
                    .satisfies(e -> assertThat(((WebSocketHandshakeException) e.getCause()).getResponse().statusCode())
                            .isEqualTo(401));
        }
    }

    @Test
    void connectSubscribeAndReceiveEventsInOrderThenDisconnectCleansUp() throws Exception {
        int connectionsBefore = handler.connectionCount();
        WsTestClient employee = WsTestClient.connect(port, "dev-acme-emp-1");
        employee.send(subscribe("m1", "trip:T-1"));
        JsonNode subscribed = employee.next();
        assertThat(subscribed.get("type").asText()).isEqualTo("SUBSCRIBED");
        assertThat(subscribed.get("id").asText()).isEqualTo("m1");

        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            seqs.add(publishAcmeT1().events().get(0).seq());
        }
        List<JsonNode> events = employee.next(5);
        assertThat(events).extracting(e -> e.get("type").asText()).containsOnly("EVENT");
        assertThat(events).extracting(e -> e.get("seq").asLong()).containsExactlyElementsOf(seqs);
        assertThat(events.get(0).at("/event/data/status").asText()).isEqualTo("DRIVER_EN_ROUTE");
        assertThat(handler.connectionCount()).isEqualTo(connectionsBefore + 1);

        employee.close();
        await().atMost(Duration.ofSeconds(10)).until(() -> handler.connectionCount() == connectionsBefore
                && registry.feed(new TenantTopic("acme", TopicId.trip("T-1"))) == null);
    }

    @Test
    void malformedMessagesGetErrorsAndTheConnectionStaysOpen() throws Exception {
        WsTestClient employee = WsTestClient.connect(port, "dev-acme-emp-1");
        employee.send("hello");
        employee.send("{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\",\"tenantId\":\"globex\"}");
        employee.send("{\"type\":\"RESUME\",\"id\":\"m2\",\"topic\":\"trip:T-1\",\"lastSeq\":-4}");
        employee.send("{\"type\":\"PING\",\"id\":\"m3\"}");
        employee.send("{\"type\":\"SUBSCRIBE\",\"id\":\"m4\",\"topic\":\"" + "y".repeat(5000) + "\"}");

        assertThat(employee.next(5)).extracting(m -> m.get("code").asText()).containsExactly(
                "BAD_REQUEST", "BAD_REQUEST", "BAD_REQUEST", "UNSUPPORTED_TYPE", "MESSAGE_TOO_LARGE");
        employee.send(subscribe("m5", "trip:T-1"));
        assertThat(employee.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        employee.close();
    }

    @Test
    void unauthorizedAndCrossTenantTopicsAreForbiddenWithoutLeakingExistence() throws Exception {
        WsTestClient employee = WsTestClient.connect(port, "dev-acme-emp-1");
        employee.send(subscribe("m1", "trip:T-2"));   // exists, another employee's
        employee.send(subscribe("m2", "trip:T-999")); // does not exist
        employee.send(subscribe("m3", "sos:O-1"));
        List<JsonNode> errors = employee.next(3);
        assertThat(errors).extracting(e -> e.get("code").asText()).containsOnly("FORBIDDEN");
        assertThat(errors).extracting(e -> e.get("message").asText()).containsOnly("Subscription not permitted");

        WsTestClient security = WsTestClient.connect(port, "dev-acme-security-o1");
        security.send(subscribe("s1", "office:O-1"));
        assertThat(security.next().get("code").asText()).isEqualTo("FORBIDDEN");
        security.send(subscribe("s2", "sos:O-1"));
        assertThat(security.next().get("type").asText()).isEqualTo("SUBSCRIBED");

        // globex has a trip with the same id; its employee only ever sees globex events.
        WsTestClient globex = WsTestClient.connect(port, "dev-globex-emp-1");
        globex.send(subscribe("g1", "trip:T-1"));
        assertThat(globex.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        publishAcmeT1();
        AppendResult globexEvent = publisher.publish(status("globex", "T-1", "C-1"), T1_TOPICS).appended();
        JsonNode received = globex.next();
        assertThat(received.at("/event/eventId").asText()).isEqualTo(globexEvent.events().get(0).event().eventId());
        assertThat(received.at("/event/tenantId").asText()).isEqualTo("globex");
        assertThat(globex.poll(300)).isNull();

        employee.close();
        security.close();
        globex.close();
    }

    @Test
    void duplicateSubscribeUnsubscribeAndAcks() throws Exception {
        WsTestClient admin = WsTestClient.connect(port, "dev-acme-admin-o1");
        admin.send(subscribe("m1", "office:O-1"));
        admin.send(subscribe("m2", "office:O-1"));
        assertThat(admin.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        assertThat(admin.next().get("code").asText()).isEqualTo("ALREADY_SUBSCRIBED");

        long seq = publishAcmeT1().events().get(2).seq(); // office topic
        assertThat(admin.next().get("seq").asLong()).isEqualTo(seq);
        admin.send("{\"type\":\"ACK\",\"topic\":\"office:O-1\",\"seq\":" + seq + "}");
        admin.send("{\"type\":\"ACK\",\"topic\":\"office:O-1\",\"seq\":" + seq + "}");
        admin.send("{\"type\":\"ACK\",\"id\":\"a1\",\"topic\":\"office:O-1\",\"seq\":" + (seq + 100) + "}");
        JsonNode invalid = admin.next();
        assertThat(invalid.get("code").asText()).isEqualTo("INVALID_ACK");
        assertThat(invalid.get("id").asText()).isEqualTo("a1");

        admin.send("{\"type\":\"UNSUBSCRIBE\",\"id\":\"m3\",\"topic\":\"office:O-1\"}");
        assertThat(admin.next().get("type").asText()).isEqualTo("UNSUBSCRIBED");
        publishAcmeT1();
        assertThat(admin.poll(300)).as("no events after UNSUBSCRIBED").isNull();
        admin.close();
    }

    @Test
    void resumeReplaysMissedEventsInOrderThenCompletesThenGoesLive() throws Exception {
        long lastProcessed = publishAcmeT1().events().get(0).seq();
        List<Long> missed = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            missed.add(publishAcmeT1().events().get(0).seq()); // client offline
        }

        WsTestClient employee = WsTestClient.connect(port, "dev-acme-emp-1");
        employee.send("{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"trip:T-1\",\"lastSeq\":" + lastProcessed + "}");
        List<JsonNode> replay = employee.next(4);
        assertThat(replay.subList(0, 3)).extracting(e -> e.get("seq").asLong()).containsExactlyElementsOf(missed);
        JsonNode complete = replay.get(3);
        assertThat(complete.get("type").asText()).isEqualTo("RESUME_COMPLETE");
        assertThat(complete.get("id").asText()).isEqualTo("r1");
        assertThat(complete.get("replayed").asInt()).isEqualTo(3);
        assertThat(complete.get("toSeq").asLong()).isEqualTo(missed.get(2));

        long live = publishAcmeT1().events().get(0).seq();
        assertThat(employee.next().get("seq").asLong()).isEqualTo(live);
        assertThat(employee.poll(300)).as("no duplicates").isNull();
        employee.close();
    }

    @Test
    void resumeFromExpiredOrUnknownHistoryRequiresRecovery() throws Exception {
        long head = publishAcmeT1().events().get(0).seq();
        WsTestClient employee = WsTestClient.connect(port, "dev-acme-emp-1");
        employee.send("{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"trip:T-1\",\"lastSeq\":1}");
        employee.send("{\"type\":\"RESUME\",\"id\":\"r2\",\"topic\":\"trip:T-1\",\"lastSeq\":" + (head + 1_000_000) + "}");

        for (JsonNode error : employee.next(2)) {
            assertThat(error.get("code").asText()).isEqualTo("RECOVERY_REQUIRED");
            assertThat(error.get("topic").asText()).isEqualTo("trip:T-1");
            assertThat(error.at("/recovery/headSeq").asLong()).isGreaterThanOrEqualTo(head);
        }
        employee.send(subscribe("m1", "trip:T-1"));
        assertThat(employee.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        employee.close();
    }

    @Test
    void frozenClientIsDisconnectedWhileOthersKeepReceiving() throws Exception {
        WsTestClient frozen = WsTestClient.connect(port, "dev-acme-admin-o1", true);
        WsTestClient healthy = WsTestClient.connect(port, "dev-acme-admin-o1");
        frozen.send(subscribe("f1", "office:O-1"));
        healthy.send(subscribe("h1", "office:O-1"));
        assertThat(healthy.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        TenantTopic office = new TenantTopic("acme", TopicId.office("O-1"));
        await().atMost(Duration.ofSeconds(10)).until(() -> registry.feed(office).subscriptionCount() == 2);

        int total = 4000; // enough to fill the frozen client's TCP buffers and its 64-slot reliable lane
        for (int i = 0; i < total; i++) {
            publishAcmeT1();
        }
        List<JsonNode> received = healthy.next(total);
        assertThat(received).extracting(e -> e.get("seq").asLong()).isSorted().doesNotHaveDuplicates();

        await().atMost(Duration.ofSeconds(30)).until(() -> registry.feed(office).subscriptionCount() == 1);
        healthy.close();
        frozen.abort();
    }

    @Test
    void manyConcurrentClientsEachReceiveTheirTopicInOrder() throws Exception {
        List<WsTestClient> clients = new ArrayList<>();
        List<Thread> connectors = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            connectors.add(Thread.ofVirtual().start(() -> {
                WsTestClient client = WsTestClient.connect(port, "dev-acme-emp-1");
                client.send(subscribe("m1", "trip:T-1"));
                synchronized (clients) {
                    clients.add(client);
                }
            }));
        }
        for (Thread connector : connectors) {
            connector.join();
        }
        for (WsTestClient client : clients) {
            assertThat(client.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        }
        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            seqs.add(publishAcmeT1().events().get(0).seq());
        }
        for (WsTestClient client : clients) {
            assertThat(client.next(10)).extracting(e -> e.get("seq").asLong()).containsExactlyElementsOf(seqs);
            client.close();
        }
    }
}

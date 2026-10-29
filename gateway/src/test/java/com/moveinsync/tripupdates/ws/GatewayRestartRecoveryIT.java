package com.moveinsync.tripupdates.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.DelayPayload;
import com.moveinsync.tripupdates.domain.EventPayload;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.GeoPoint;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.SosPayload;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.StatusPayload;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.GatewayFanout;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import com.moveinsync.tripupdates.gateway.RecordingTransport;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.notify.RedisNotificationSubscriber;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.RedisClients;
import com.moveinsync.tripupdates.store.RedisEventStore;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Gateway restart: a client served by gateway A loses it; STATUS, DELAY and SOS events are published while it is
 * gone; a brand-new gateway B (empty in-memory state, as after a restart or on another node) serves the client's
 * RESUME from Redis Streams. Uses the real store, real Pub/Sub and the protocol layer; the WebSocket adapter itself is
 * covered by WebSocketEndpointIT.
 */
@Testcontainers
class GatewayRestartRecoveryIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    private static final Duration WAIT = Duration.ofSeconds(15);
    private static final String CHANNEL = "notify:events";
    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");
    private static final AtomicInteger IDS = new AtomicInteger();
    private static final TopicId OFFICE = TopicId.office("O-1");
    private static final TopicId SOS = TopicId.sos("O-1");

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private RedisClient client;
    private RedisEventStore store;
    private EventPublisher publisher;

    /** One gateway instance with its own in-memory state. */
    private final class Gateway implements AutoCloseable {
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        final SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, executor, 512, 64);
        final GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));
        final RedisNotificationSubscriber subscriber = new RedisNotificationSubscriber(client, CHANNEL, fanout);
        final ProtocolService protocol = new ProtocolService(registry, new TopicAuthorizer(FIXTURES.directory()),
                new ProtocolCodec(), 4096, 10);

        Gateway() {
            subscriber.start();
        }

        ProtocolService.Connection connect(String token, RecordingTransport transport) {
            Identity identity = FIXTURES.authenticator().authenticate(token).orElseThrow();
            return new ProtocolService.Connection(identity,
                    registry.open("s" + IDS.incrementAndGet(), identity.tenantId(), transport));
        }

        /** Stops this gateway: its client connections drop and nothing it held in memory survives. */
        @Override
        public void close() {
            registry.sessions().forEach(session -> session.close(CloseReason.SHUTDOWN));
            subscriber.close();
            fanout.close();
            executor.shutdownNow();
        }
    }

    @BeforeEach
    void setUp() {
        client = RedisClients.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                Duration.ofSeconds(2));
        try (var admin = client.connect()) {
            admin.sync().flushall();
        }
        store = new RedisEventStore(client, new AppProperties.Retention(1000, 100_000, Duration.ofHours(24),
                Duration.ofMinutes(5)));
        RedisNotificationPublisher notifications = new RedisNotificationPublisher(client, CHANNEL);
        closeables.add(notifications);
        publisher = new EventPublisher(store, notifications);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
        store.close();
        client.shutdown();
    }

    private Gateway startGateway() {
        Gateway gateway = new Gateway();
        closeables.add(gateway);
        return gateway;
    }

    private AppendResult publish(EventPayload payload) {
        TripEvent event = new TripEvent("e" + IDS.incrementAndGet(), "acme", "T-1", "C-1", Instant.now(), payload);
        List<TopicId> topics = payload instanceof SosPayload
                ? List.of(TopicId.trip("T-1"), TopicId.cab("C-1"), OFFICE, SOS)
                : List.of(TopicId.trip("T-1"), TopicId.cab("C-1"), OFFICE);
        return publisher.publish(event, topics).appended();
    }

    private static long seqOn(AppendResult result, TopicId topic) {
        return result.events().stream().filter(e -> e.topic().equals(topic)).findFirst().orElseThrow().seq();
    }

    @Test
    void reliableEventsPublishedWhileTheGatewayWasDownAreRecoveredInOrderOnAFreshGateway() {
        Gateway a = startGateway();
        RecordingTransport deskOnA = RecordingTransport.open();
        RecordingTransport securityOnA = RecordingTransport.open();
        ProtocolService.Connection desk = a.connect("dev-acme-admin-o1", deskOnA);
        ProtocolService.Connection security = a.connect("dev-acme-security-o1", securityOnA);
        a.protocol.handle(desk, "{\"type\":\"SUBSCRIBE\",\"id\":\"d1\",\"topic\":\"office:O-1\"}");
        a.protocol.handle(security, "{\"type\":\"SUBSCRIBE\",\"id\":\"s1\",\"topic\":\"sos:O-1\"}");
        await().atMost(WAIT).until(() -> deskOnA.controls().size() == 1 && securityOnA.controls().size() == 1);
        long securityStart = ((ServerMessage.Subscribed) securityOnA.controls().get(0)).seq();

        AppendResult before = publish(new StatusPayload(TripStatus.DRIVER_EN_ROUTE));
        await().atMost(WAIT).until(() -> deskOnA.sent().size() == 1);
        long deskLastProcessed = deskOnA.sent().get(0).seq();
        assertThat(deskLastProcessed).isEqualTo(seqOn(before, OFFICE));

        a.close(); // gateway A goes away

        List<AppendResult> missed = List.of(
                publish(new StatusPayload(TripStatus.ARRIVED)),
                publish(new DelayPayload(7, "TRAFFIC")),
                publish(new SosPayload("SOS-1", SosRaiser.EMPLOYEE, new GeoPoint(12.9, 77.6))));

        Gateway b = startGateway(); // fresh instance: no feeds, no cursors
        RecordingTransport deskOnB = RecordingTransport.open();
        RecordingTransport securityOnB = RecordingTransport.open();
        ProtocolService.Connection deskAgain = b.connect("dev-acme-admin-o1", deskOnB);
        ProtocolService.Connection securityAgain = b.connect("dev-acme-security-o1", securityOnB);
        b.protocol.handle(deskAgain,
                "{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"office:O-1\",\"lastSeq\":" + deskLastProcessed + "}");
        b.protocol.handle(securityAgain,
                "{\"type\":\"RESUME\",\"id\":\"r2\",\"topic\":\"sos:O-1\",\"lastSeq\":" + securityStart + "}");

        await().atMost(WAIT).until(() -> deskOnB.frames().size() == 4 && securityOnB.frames().size() == 2);
        assertThat(deskOnB.sent()).extracting(SequencedEvent::type)
                .containsExactly(EventType.STATUS, EventType.DELAY, EventType.SOS);
        assertThat(deskOnB.seqs()).containsExactlyElementsOf(missed.stream().map(r -> seqOn(r, OFFICE)).toList());
        assertThat(((OutboundFrame.ControlFrame) deskOnB.frames().get(3)).message())
                .isInstanceOfSatisfying(ServerMessage.ResumeComplete.class, c -> assertThat(c.replayed()).isEqualTo(3));
        assertThat(securityOnB.sent()).extracting(SequencedEvent::type).containsExactly(EventType.SOS);

        // Live delivery continues on B after the handoff, without duplicates.
        AppendResult live = publish(new SosPayload("SOS-2", SosRaiser.DRIVER, new GeoPoint(12.9, 77.6)));
        await().atMost(WAIT).until(() -> deskOnB.sent().size() == 4 && securityOnB.sent().size() == 2);
        assertThat(deskOnB.sent().get(3).seq()).isEqualTo(seqOn(live, OFFICE));
        assertThat(securityOnB.sent().get(1).seq()).isEqualTo(seqOn(live, SOS));
        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> deskOnB.sent().size() == 4);
    }
}

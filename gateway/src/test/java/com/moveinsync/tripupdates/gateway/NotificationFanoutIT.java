package com.moveinsync.tripupdates.gateway;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.ALL;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.CAB;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.OFFICE;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.TRIP;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.location;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.sos;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.notify.RedisNotificationSubscriber;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.publish.PublishOutcome;
import com.moveinsync.tripupdates.store.RedisClients;
import com.moveinsync.tripupdates.store.RedisEventStore;
import io.lettuce.core.KillArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Redis Pub/Sub notifications with Redis Streams as the source of truth, across two gateways sharing one Redis.
 * Reconciliation ticks are disabled (1 h) so that any recovery observed comes from the mechanism under test.
 */
@Testcontainers
class NotificationFanoutIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    private static final Duration WAIT = Duration.ofSeconds(15);
    private static final String CHANNEL = "notify:events";
    private static final AppProperties.Retention RETENTION =
            new AppProperties.Retention(1000, 100_000, Duration.ofHours(24), Duration.ofMinutes(5));

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private RedisClient client;
    private StatefulRedisConnection<String, String> admin;
    private RedisEventStore store;
    private EventPublisher publisher;

    /** One gateway node: its own registry, fanout and notification subscription. */
    private record Gateway(SubscriptionRegistry registry, GatewayFanout fanout) {
    }

    @BeforeEach
    void setUp() {
        client = RedisClients.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                Duration.ofSeconds(2));
        admin = client.connect();
        admin.sync().flushall();
        store = new RedisEventStore(client, RETENTION);
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
        admin.close();
        client.shutdown();
    }

    private Gateway startGateway() {
        return startGateway(CHANNEL);
    }

    /** A gateway subscribed to the given notification channels, each on its own Pub/Sub connection. */
    private Gateway startGateway(String... channels) {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, executor, 512, 64);
        GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));
        closeables.add(executor::shutdownNow);
        closeables.add(fanout);
        for (String channel : channels) {
            RedisNotificationSubscriber subscriber = new RedisNotificationSubscriber(client, channel, fanout);
            subscriber.start();
            closeables.add(subscriber);
        }
        return new Gateway(registry, fanout);
    }

    /** A publisher with the SOS lane: its own store connection and the priority notification channel. */
    private EventPublisher lanedPublisher() {
        RedisNotificationPublisher notifications = new RedisNotificationPublisher(client, CHANNEL);
        RedisNotificationPublisher priority = new RedisNotificationPublisher(client,
                RedisNotificationPublisher.priorityChannel(CHANNEL));
        RedisEventStore priorityStore = new RedisEventStore(client, RETENTION);
        closeables.add(notifications);
        closeables.add(priority);
        closeables.add(priorityStore::close);
        return new EventPublisher(store, notifications, priorityStore, priority, null);
    }

    @Test
    void sosTravelsOnItsOwnChannelAndStillArrivesAfterTheEarlierStatusOfItsTopic() {
        // Gateway P hears only the SOS channel: anything it delivers was triggered by an SOS notification.
        Gateway onlySos = startGateway(RedisNotificationPublisher.priorityChannel(CHANNEL));
        Gateway both = startGateway(CHANNEL, RedisNotificationPublisher.priorityChannel(CHANNEL));
        RecordingTransport sosOnly = RecordingTransport.open();
        RecordingTransport employee = RecordingTransport.open();
        RecordingTransport otherTenant = RecordingTransport.open();
        onlySos.registry().subscribe(onlySos.registry().open("p", "acme", sosOnly), TRIP);
        both.registry().subscribe(both.registry().open("emp", "acme", employee), TRIP);
        both.registry().subscribe(both.registry().open("other", "globex", otherTenant), TRIP);
        EventPublisher laned = lanedPublisher();

        laned.publish(status("s1"), ALL);    // default channel: gateway P does not hear it
        laned.publish(location("l1", "C-1", 1.0), List.of(CAB, OFFICE));
        laned.publish(sos("sos"), ALL);      // SOS channel: P catches its trip feed up from the stream
        await().atMost(WAIT).until(() -> sosOnly.sent().size() == 2);
        assertThat(sosOnly.eventIds()).as("the earlier status first, then the SOS").containsExactly("s1", "sos");
        assertThat(sosOnly.seqs()).isSorted().doesNotHaveDuplicates();

        laned.publish(status("s2"), ALL);    // default channel again: not heard by P
        await().atMost(WAIT).until(() -> employee.sent().size() == 3);
        assertThat(employee.eventIds()).containsExactly("s1", "sos", "s2");
        assertThat(employee.seqs()).isSorted().doesNotHaveDuplicates();
        await().during(Duration.ofMillis(500)).atMost(WAIT).until(() -> sosOnly.sent().size() == 2);
        assertThat(otherTenant.sent()).as("tenant isolation").isEmpty();
    }

    @Test
    void eventPublishedThroughOneGatewayReachesSubscribersOnAnotherInOrder() {
        startGateway(); // gateway A: publishes, has no subscribers
        Gateway b = startGateway();
        RecordingTransport employee = RecordingTransport.open();
        RecordingTransport desk = RecordingTransport.open();
        b.registry().subscribe(b.registry().open("emp", "acme", employee), TRIP);
        b.registry().subscribe(b.registry().open("desk", "acme", desk), OFFICE);

        publisher.publish(status("e1"), ALL);
        publisher.publish(location("l1", "C-1", 1.0), List.of(CAB, OFFICE));
        publisher.publish(sos("e2"), ALL);
        publisher.publish(status("e3"), ALL);

        await().atMost(WAIT).until(() -> employee.sent().size() == 3 && desk.sent().size() == 4);
        assertThat(employee.eventIds()).containsExactly("e1", "e2", "e3");
        assertThat(employee.seqs()).isSorted();
        assertThat(desk.eventIds()).containsExactlyInAnyOrder("e1", "e2", "e3", "l1");
        assertThat(desk.eventIds().stream().filter(id -> id.startsWith("e")).toList())
                .containsExactly("e1", "e2", "e3");
    }

    @Test
    void publishFailureAfterDurableAppendIsRecoveredFromTheStream() throws IOException {
        Gateway b = startGateway();
        RecordingTransport employee = RecordingTransport.open();
        b.registry().subscribe(b.registry().open("emp", "acme", employee), TRIP);

        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        RedisClient brokenBroker = RedisClients.create("redis://127.0.0.1:" + closedPort, Duration.ofMillis(300));
        RedisNotificationPublisher broken = new RedisNotificationPublisher(brokenBroker, CHANNEL);
        closeables.add(brokenBroker::shutdown);
        closeables.add(broken);
        EventPublisher publisherWithBrokenNotifications = new EventPublisher(store, broken);

        PublishOutcome outcome = publisherWithBrokenNotifications.publish(status("e1"), ALL);

        assertThat(outcome.notified()).isFalse();
        assertThat(outcome.appended().duplicate()).isFalse();
        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> employee.sent().isEmpty());

        b.fanout().reconcileAll();

        await().atMost(WAIT).until(() -> employee.eventIds().equals(List.of("e1")));
        assertThat(admin.sync().xlen("t:acme:log:trip:T-1")).as("appended exactly once").isEqualTo(1);
    }

    @Test
    void reconnectingThePubSubConnectionRecoversNotificationsMissedMeanwhile() {
        Gateway b = startGateway();
        RecordingTransport employee = RecordingTransport.open();
        b.registry().subscribe(b.registry().open("emp", "acme", employee), TRIP);

        // Durable appends whose notifications this gateway never saw (as during a Pub/Sub outage).
        store.append(status("missed-1"), ALL);
        store.append(status("missed-2"), ALL);
        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> employee.sent().isEmpty());

        // Drop the gateway's Pub/Sub connection; Lettuce reconnects and re-subscribes, which triggers recovery.
        long killed = admin.sync().clientKill(KillArgs.Builder.typePubsub());
        assertThat(killed).isPositive();

        await().atMost(WAIT).until(() -> employee.eventIds().equals(List.of("missed-1", "missed-2")));
        publisher.publish(status("after"), ALL);
        await().atMost(WAIT).until(() -> employee.eventIds().equals(List.of("missed-1", "missed-2", "after")));
    }

    @Test
    void duplicateNotificationsOverRealPubSubDeliverOnce() {
        Gateway b = startGateway();
        RecordingTransport employee = RecordingTransport.open();
        b.registry().subscribe(b.registry().open("emp", "acme", employee), TRIP);

        PublishOutcome outcome = publisher.publish(status("e1"), ALL);
        publisher.renotify(outcome.appended());
        publisher.renotify(outcome.appended());
        publisher.publish(status("e1"), ALL); // idempotent retry: re-notifies, no new append

        await().atMost(WAIT).until(() -> employee.sent().size() == 1);
        await().during(Duration.ofMillis(500)).atMost(WAIT).until(() -> employee.sent().size() == 1);
        assertThat(employee.eventIds()).containsExactly("e1");
    }
}

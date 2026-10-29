package com.moveinsync.tripupdates.ws;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.ALL;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.auth.TopicDirectory;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.GatewayFanout;
import com.moveinsync.tripupdates.gateway.InMemoryEventStore;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import com.moveinsync.tripupdates.gateway.RecordingTransport;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.notify.EventNotification;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConnectionMaintenanceTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");

    /** A directory whose data can be replaced, as a real directory changes when trips are reassigned. */
    private static final class ChangingDirectory implements TopicDirectory {
        final AtomicReference<TopicDirectory> current = new AtomicReference<>(FIXTURES.directory());

        @Override
        public Optional<Trip> trip(String tenantId, String tripId) {
            return current.get().trip(tenantId, tripId);
        }

        @Override
        public Optional<String> cabOffice(String tenantId, String cabId) {
            return current.get().cabOffice(tenantId, cabId);
        }

        @Override
        public boolean officeExists(String tenantId, String officeId) {
            return current.get().officeExists(tenantId, officeId);
        }
    }

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventStore store = new InMemoryEventStore(1000);
    private final SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, executor, 512, 64);
    private final GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));
    private final ChangingDirectory directory = new ChangingDirectory();
    private final ProtocolService protocol = new ProtocolService(registry, new TopicAuthorizer(directory),
            new ProtocolCodec(), 4096, 10);
    private final List<ProtocolService.Connection> connections = new CopyOnWriteArrayList<>();
    private final AtomicLong now = new AtomicLong(System.currentTimeMillis());
    private final ConnectionMaintenance maintenance =
            new ConnectionMaintenance(() -> List.copyOf(connections), protocol, Duration.ofSeconds(60), now::get);

    @AfterEach
    void tearDown() {
        maintenance.close();
        fanout.close();
        executor.shutdownNow();
    }

    private ProtocolService.Connection connect(String token, RecordingTransport transport) {
        Identity identity = FIXTURES.authenticator().authenticate(token).orElseThrow();
        ProtocolService.Connection connection = new ProtocolService.Connection(identity,
                registry.open("s-" + connections.size(), identity.tenantId(), transport));
        connections.add(connection);
        return connection;
    }

    @Test
    void liveConnectionsArePingedAndSilentOnesAreClosedAfterTheIdleTimeout() {
        RecordingTransport active = RecordingTransport.open();
        RecordingTransport silent = RecordingTransport.open();
        ProtocolService.Connection activeConnection = connect("dev-acme-emp-1", active);
        ProtocolService.Connection silentConnection = connect("dev-acme-emp-2", silent);
        activeConnection.touchAt(0);
        silentConnection.touchAt(0);

        now.set(Duration.ofSeconds(59).toMillis());
        assertThat(maintenance.heartbeat()).isZero();
        await().atMost(WAIT).until(() -> active.controls().contains(Heartbeat.PING)
                && silent.controls().contains(Heartbeat.PING));

        activeConnection.touchAt(Duration.ofSeconds(50).toMillis()); // its pong arrived
        now.set(Duration.ofSeconds(61).toMillis());
        assertThat(maintenance.heartbeat()).isEqualTo(1);

        await().atMost(WAIT).until(() -> silent.closeReason() == CloseReason.IDLE_TIMEOUT);
        assertThat(silentConnection.session().closeReason()).isEqualTo(CloseReason.IDLE_TIMEOUT);
        assertThat(activeConnection.session().isClosed()).isFalse();
    }

    @Test
    void revokedSubscriptionsAreRemovedWithTheirPendingEventsAndTheClientIsTold() {
        RecordingTransport transport = RecordingTransport.open();
        ProtocolService.Connection employee = connect("dev-acme-emp-1", transport);
        protocol.handle(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        await().atMost(WAIT).until(() -> transport.controls().size() == 1);
        assertThat(maintenance.revalidate()).as("still permitted").isZero();

        // Trip T-1 is reassigned to another passenger.
        directory.current.set(new InMemoryTopicDirectory(Map.of("acme", new InMemoryTopicDirectory.Tenant(
                Set.of("O-1", "O-2"), Map.of("C-1", "O-1"),
                Map.of("T-1", new TopicDirectory.Trip("O-1", "C-1", Set.of("emp-9")))))));

        assertThat(maintenance.revalidate()).isEqualTo(1);
        await().atMost(WAIT).until(() -> transport.controls().size() == 2);
        ServerMessage.Error revoked = (ServerMessage.Error) transport.controls().get(1);
        assertThat(revoked.code()).isEqualTo(ErrorCode.SUBSCRIPTION_REVOKED);
        assertThat(revoked.topic()).isEqualTo(TopicId.trip("T-1"));
        assertThat(registry.topicsOf(employee.session())).isEmpty();
        assertThat(registry.feeds()).isEmpty();

        fanout.onNotification(EventNotification.of(store.append(status("after"), ALL)));
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> transport.sent().isEmpty());
        assertThat(maintenance.revalidate()).as("idempotent").isZero();

        protocol.handle(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m2\",\"topic\":\"trip:T-1\"}");
        await().atMost(WAIT).until(() -> transport.controls().size() == 3);
        assertThat(((ServerMessage.Error) transport.controls().get(2)).code()).isEqualTo(ErrorCode.FORBIDDEN);
    }

    @Test
    void closedConnectionsAreSkipped() {
        RecordingTransport transport = RecordingTransport.open();
        ProtocolService.Connection connection = connect("dev-acme-emp-1", transport);
        connection.session().close(CloseReason.NORMAL);
        now.addAndGet(Duration.ofHours(1).toMillis());

        assertThat(maintenance.heartbeat()).isZero();
        assertThat(maintenance.revalidate()).isZero();
        assertThat(transport.frames()).noneMatch(frame -> frame instanceof OutboundFrame.ControlFrame);
    }
}

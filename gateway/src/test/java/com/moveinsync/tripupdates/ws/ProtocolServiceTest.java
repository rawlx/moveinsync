package com.moveinsync.tripupdates.ws;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.ALL;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.TRIP;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.location;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.GatewayFanout;
import com.moveinsync.tripupdates.gateway.InMemoryEventStore;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import com.moveinsync.tripupdates.gateway.RecordingTransport;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.store.AppendResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ProtocolServiceTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final DevFixtures FIXTURES = DevFixtures.load("dev/dev-fixtures.json");

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventStore store = new InMemoryEventStore(1000);
    private final SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, executor, 512, 64);
    private final GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));
    private final ProtocolService protocol = new ProtocolService(registry,
            new TopicAuthorizer(FIXTURES.directory()), new ProtocolCodec(), 4096, 3);

    @AfterEach
    void tearDown() {
        fanout.close();
        executor.shutdownNow();
    }

    private record Client(ProtocolService.Connection connection, RecordingTransport transport) {
    }

    private Client connect(String token) {
        Identity identity = FIXTURES.authenticator().authenticate(token).orElseThrow();
        RecordingTransport transport = RecordingTransport.open();
        return new Client(new ProtocolService.Connection(identity,
                registry.open("s-" + token, identity.tenantId(), transport)), transport);
    }

    private void send(Client client, String json) {
        protocol.handle(client.connection(), json);
    }

    private AppendResult publish(TripEvent event) {
        AppendResult result = store.append(event, ALL);
        fanout.onNotification(EventNotification.of(result));
        return result;
    }

    private static long tripSeq(AppendResult result) {
        return result.events().get(0).seq();
    }

    private static ServerMessage.Error lastError(Client client) {
        List<OutboundFrame.ControlMessage> controls = client.transport().controls();
        return (ServerMessage.Error) controls.get(controls.size() - 1);
    }

    private void awaitControls(Client client, int count) {
        await().atMost(WAIT).until(() -> client.transport().controls().size() == count);
    }

    @Test
    void subscribeConfirmsBeforeAnyLiveEvent() {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        publish(status("e1"));

        await().atMost(WAIT).until(() -> employee.transport().frames().size() == 2);
        List<OutboundFrame> frames = employee.transport().frames();
        assertThat(frames.get(0)).isInstanceOfSatisfying(OutboundFrame.ControlFrame.class, frame ->
                assertThat(frame.message()).isInstanceOf(ServerMessage.Subscribed.class));
        assertThat(employee.transport().eventIds()).containsExactly("e1");
    }

    @Test
    void malformedAndUnsupportedMessagesGetStructuredErrorsAndTheConnectionStaysUsable() {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{oops");
        send(employee, "{\"type\":\"PUBLISH\",\"id\":\"m1\"}");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m2\",\"topic\":\"trip:T-1\",\"tenantId\":\"globex\"}");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m3\",\"topic\":\"" + "x".repeat(5000) + "\"}");
        awaitControls(employee, 4);

        assertThat(employee.transport().controls()).extracting(c -> ((ServerMessage.Error) c).code())
                .containsExactly(ErrorCode.BAD_REQUEST, ErrorCode.UNSUPPORTED_TYPE, ErrorCode.BAD_REQUEST,
                        ErrorCode.MESSAGE_TOO_LARGE);
        assertThat(employee.transport().controls()).extracting(c -> ((ServerMessage.Error) c).id())
                .containsExactly(null, "m1", "m2", null);
        assertThat(employee.connection().session().isClosed()).isFalse();

        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m4\",\"topic\":\"trip:T-1\"}");
        awaitControls(employee, 5);
        assertThat(employee.transport().controls().get(4)).isInstanceOf(ServerMessage.Subscribed.class);
    }

    @Test
    void forbiddenAndUnknownTopicsGetTheSameAnswerAndNothingIsAttached() {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-2\"}");   // another employee's
        send(employee, "{\"type\":\"RESUME\",\"id\":\"m2\",\"topic\":\"trip:T-404\",\"lastSeq\":0}"); // unknown
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m3\",\"topic\":\"office:O-1\"}"); // wrong role
        awaitControls(employee, 3);

        assertThat(employee.transport().controls()).allSatisfy(control -> {
            ServerMessage.Error error = (ServerMessage.Error) control;
            assertThat(error.code()).isEqualTo(ErrorCode.FORBIDDEN);
            assertThat(error.message()).isEqualTo("Subscription not permitted");
        });
        assertThat(registry.feeds()).isEmpty();
    }

    @Test
    void tenantIsTakenFromTheIdentityNotTheTopic() {
        Client globex = connect("dev-globex-emp-1");
        send(globex, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        awaitControls(globex, 1);

        publish(status("acme", "acme-1"));       // acme's T-1
        publish(status("globex", "globex-1"));   // globex's T-1

        await().atMost(WAIT).until(() -> globex.transport().sent().size() == 1);
        assertThat(globex.transport().eventIds()).containsExactly("globex-1");
        assertThat(registry.feed(new TenantTopic("globex", TRIP))).isNotNull();
        assertThat(registry.feed(new TenantTopic("acme", TRIP))).isNull();
    }

    @Test
    void duplicateSubscribeIsRejectedAndUnsubscribeCleansUp() {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m2\",\"topic\":\"trip:T-1\"}");
        send(employee, "{\"type\":\"RESUME\",\"id\":\"m3\",\"topic\":\"trip:T-1\",\"lastSeq\":0}");
        awaitControls(employee, 3);
        assertThat(((ServerMessage.Error) employee.transport().controls().get(1)).code())
                .isEqualTo(ErrorCode.ALREADY_SUBSCRIBED);
        assertThat(((ServerMessage.Error) employee.transport().controls().get(2)).code())
                .isEqualTo(ErrorCode.ALREADY_SUBSCRIBED);

        publish(status("e1"));
        await().atMost(WAIT).until(() -> employee.transport().sent().size() == 1);

        send(employee, "{\"type\":\"UNSUBSCRIBE\",\"id\":\"m4\",\"topic\":\"trip:T-1\"}");
        send(employee, "{\"type\":\"UNSUBSCRIBE\",\"id\":\"m5\",\"topic\":\"trip:T-1\"}");
        awaitControls(employee, 5);
        assertThat(employee.transport().controls().get(3)).isEqualTo(new ServerMessage.Unsubscribed("m4", TRIP));
        assertThat(lastError(employee).code()).isEqualTo(ErrorCode.NOT_SUBSCRIBED);
        assertThat(registry.feeds()).isEmpty();

        publish(status("e2"));
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> employee.transport().sent().size() == 1);
    }

    @Test
    void subscriptionLimitIsEnforced() {
        ProtocolService limited = new ProtocolService(registry, new TopicAuthorizer(FIXTURES.directory()),
                new ProtocolCodec(), 4096, 2);
        Client admin = connect("dev-acme-admin-o1");
        limited.handle(admin.connection(), "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"office:O-1\"}");
        limited.handle(admin.connection(), "{\"type\":\"SUBSCRIBE\",\"id\":\"m2\",\"topic\":\"trip:T-1\"}");
        limited.handle(admin.connection(), "{\"type\":\"SUBSCRIBE\",\"id\":\"m3\",\"topic\":\"cab:C-1\"}");
        awaitControls(admin, 3);
        assertThat(lastError(admin).code()).isEqualTo(ErrorCode.TOO_MANY_SUBSCRIPTIONS);
        assertThat(registry.topicsOf(admin.connection().session())).hasSize(2);

        limited.handle(admin.connection(), "{\"type\":\"UNSUBSCRIBE\",\"id\":\"m4\",\"topic\":\"trip:T-1\"}");
        limited.handle(admin.connection(), "{\"type\":\"SUBSCRIBE\",\"id\":\"m5\",\"topic\":\"cab:C-1\"}");
        awaitControls(admin, 5);
        assertThat(admin.transport().controls().get(4)).isInstanceOf(ServerMessage.Subscribed.class);
    }

    @Test
    void resumeReplaysExactlyTheMissedEventsThenCompletesThenGoesLive() {
        long first = tripSeq(store.append(status("e1"), ALL));
        store.append(status("e2"), ALL);
        store.append(location("l1", "C-1", 1.0), List.of(TopicId.cab("C-1"), TopicId.office("O-1")));
        long third = tripSeq(store.append(status("e3"), ALL));

        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"trip:T-1\",\"lastSeq\":" + first + "}");
        awaitControls(employee, 1);
        publish(status("e4"));

        await().atMost(WAIT).until(() -> employee.transport().sent().size() == 3);
        List<OutboundFrame> frames = employee.transport().frames();
        assertThat(frames).hasSize(4);
        assertThat(((OutboundFrame.EventFrame) frames.get(0)).event().event().eventId()).isEqualTo("e2");
        assertThat(((OutboundFrame.EventFrame) frames.get(1)).event().event().eventId()).isEqualTo("e3");
        assertThat(((OutboundFrame.ControlFrame) frames.get(2)).message())
                .isEqualTo(new ServerMessage.ResumeComplete("r1", TRIP, first, third, 2));
        assertThat(((OutboundFrame.EventFrame) frames.get(3)).event().event().eventId()).isEqualTo("e4");
    }

    @Test
    void resumeAtTheHeadReplaysNothingAndDeduplicatesLiveEventsAlreadyProcessed() {
        Client admin = connect("dev-acme-admin-o1");
        send(admin, "{\"type\":\"SUBSCRIBE\",\"id\":\"s1\",\"topic\":\"office:O-1\"}"); // feed exists, cursor at head
        awaitControls(admin, 1);
        long e1 = publish(status("e1")).events().get(2).seq(); // office seq
        await().atMost(WAIT).until(() -> admin.transport().sent().size() == 1);

        // A second client of the same feed that already processed e1 (e.g. on another gateway) resumes after it.
        Client other = connect("dev-acme-admin-o1");
        send(other, "{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"office:O-1\",\"lastSeq\":" + e1 + "}");
        awaitControls(other, 1);
        publish(status("e2"));

        await().atMost(WAIT).until(() -> other.transport().sent().size() == 1);
        assertThat(other.transport().eventIds()).containsExactly("e2");
        assertThat(((ServerMessage.ResumeComplete) other.transport().controls().get(0)).replayed()).isZero();
    }

    @Test
    void expiredOrUnknownHistoryRequiresExplicitRecovery() {
        InMemoryEventStore small = new InMemoryEventStore(2);
        SubscriptionRegistry smallRegistry = new SubscriptionRegistry(small, 2, executor, 512, 64);
        ProtocolService smallProtocol = new ProtocolService(smallRegistry,
                new TopicAuthorizer(FIXTURES.directory()), new ProtocolCodec(), 4096, 10);
        long first = tripSeq(small.append(status("e1"), ALL));
        for (int i = 2; i <= 5; i++) {
            small.append(status("e" + i), ALL); // log keeps only the last 2
        }
        Identity identity = FIXTURES.authenticator().authenticate("dev-acme-emp-1").orElseThrow();
        RecordingTransport transport = RecordingTransport.open();
        ProtocolService.Connection connection = new ProtocolService.Connection(identity,
                smallRegistry.open("s1", "acme", transport));

        smallProtocol.handle(connection, "{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"trip:T-1\",\"lastSeq\":" + first + "}");
        smallProtocol.handle(connection, "{\"type\":\"RESUME\",\"id\":\"r2\",\"topic\":\"trip:T-1\",\"lastSeq\":999999999}");
        await().atMost(WAIT).until(() -> transport.controls().size() == 2);

        ServerMessage.Error expired = (ServerMessage.Error) transport.controls().get(0);
        assertThat(expired.code()).isEqualTo(ErrorCode.RECOVERY_REQUIRED);
        assertThat(expired.recovery().requestedSeq()).isEqualTo(first);
        assertThat(expired.recovery().oldestAvailableAfterSeq()).isGreaterThan(first);
        ServerMessage.Error unknown = (ServerMessage.Error) transport.controls().get(1);
        assertThat(unknown.code()).isEqualTo(ErrorCode.RECOVERY_REQUIRED);
        assertThat(transport.sent()).as("no partial replay").isEmpty();
        assertThat(smallRegistry.feeds()).isEmpty();
        assertThat(connection.session().isClosed()).isFalse();
    }

    @Test
    void replayLiveHandoffUnderConcurrentPublishingHasNoGapsDuplicatesOrReordering() throws Exception {
        long start = tripSeq(store.append(status("e0"), ALL));
        for (int i = 1; i <= 200; i++) {
            store.append(status("pre-" + i), ALL);
        }
        AtomicBoolean publishing = new AtomicBoolean(true);
        List<String> published = new java.util.concurrent.CopyOnWriteArrayList<>();
        Thread publisher = Thread.ofPlatform().start(() -> {
            for (int i = 1; publishing.get() && i <= 300; i++) {
                publish(status("live-" + i));
                published.add("live-" + i);
            }
        });
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"RESUME\",\"id\":\"r1\",\"topic\":\"trip:T-1\",\"lastSeq\":" + start + "}");
        publisher.join();
        publishing.set(false);
        fanout.reconcileAll();

        int expected = 200 + published.size();
        await().atMost(WAIT).until(() -> employee.transport().sent().size() == expected);
        List<SequencedEvent> events = employee.transport().sent();
        assertThat(events).extracting(SequencedEvent::seq).isSorted().doesNotHaveDuplicates();
        List<String> ids = events.stream().map(e -> e.event().eventId()).toList();
        List<String> wanted = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            wanted.add("pre-" + i);
        }
        wanted.addAll(published);
        assertThat(ids).containsExactlyElementsOf(wanted);

        // RESUME_COMPLETE sits exactly between the replayed events (seq <= toSeq) and live ones (seq > toSeq).
        List<OutboundFrame> frames = employee.transport().frames();
        int marker = -1;
        for (int i = 0; i < frames.size(); i++) {
            if (frames.get(i) instanceof OutboundFrame.ControlFrame) {
                marker = i;
            }
        }
        ServerMessage.ResumeComplete complete =
                (ServerMessage.ResumeComplete) ((OutboundFrame.ControlFrame) frames.get(marker)).message();
        for (int i = 0; i < frames.size(); i++) {
            if (frames.get(i) instanceof OutboundFrame.EventFrame(SequencedEvent event)) {
                assertThat(event.seq() <= complete.toSeq()).as("frame %d", i).isEqualTo(i < marker);
            }
        }
        assertThat(complete.replayed()).isEqualTo(marker);
    }

    @Test
    void acknowledgementsAreIdempotentAndValidated() {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        awaitControls(employee, 1);
        long seq = tripSeq(publish(status("e1")));
        await().atMost(WAIT).until(() -> employee.transport().sent().size() == 1);

        for (int i = 0; i < 3; i++) {
            send(employee, "{\"type\":\"ACK\",\"topic\":\"trip:T-1\",\"seq\":" + seq + "}");
        }
        assertThat(registry.subscription(employee.connection().session(), TRIP).ackedSeq()).isEqualTo(seq);
        assertThat(employee.transport().controls()).as("valid ACKs get no reply").hasSize(1);

        send(employee, "{\"type\":\"ACK\",\"id\":\"a1\",\"topic\":\"trip:T-1\",\"seq\":" + (seq + 1) + "}");
        send(employee, "{\"type\":\"ACK\",\"id\":\"a2\",\"topic\":\"cab:C-1\",\"seq\":1}");
        awaitControls(employee, 3);
        assertThat(((ServerMessage.Error) employee.transport().controls().get(1)).code()).isEqualTo(ErrorCode.INVALID_ACK);
        assertThat(lastError(employee).code()).isEqualTo(ErrorCode.NOT_SUBSCRIBED);
        assertThat(registry.subscription(employee.connection().session(), TRIP).ackedSeq()).isEqualTo(seq);
    }

    @Test
    void concurrentAcksFromManyThreadsKeepTheHighest() throws Exception {
        Client employee = connect("dev-acme-emp-1");
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            seqs.add(tripSeq(publish(status("e" + i))));
        }
        await().atMost(WAIT).until(() -> employee.transport().sent().size() == 50);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            threads.add(Thread.ofPlatform().start(() -> {
                for (long seq : seqs) {
                    send(employee, "{\"type\":\"ACK\",\"topic\":\"trip:T-1\",\"seq\":" + seq + "}");
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(registry.subscription(employee.connection().session(), TRIP).ackedSeq())
                .isEqualTo(seqs.get(seqs.size() - 1));
        assertThat(employee.transport().controls()).hasSize(1);
    }

    @Test
    void storeOutageIsReportedAsUnavailableAndNothingIsAttached() {
        Client employee = connect("dev-acme-emp-1");
        store.failing(true);
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        awaitControls(employee, 1);
        assertThat(lastError(employee).code()).isEqualTo(ErrorCode.UNAVAILABLE);
        assertThat(registry.feeds()).isEmpty();

        store.failing(false);
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m2\",\"topic\":\"trip:T-1\"}");
        awaitControls(employee, 2);
        assertThat(employee.transport().controls().get(1)).isInstanceOf(ServerMessage.Subscribed.class);
    }

    @Test
    void messagesOnAClosedSessionAreIgnored() {
        Client employee = connect("dev-acme-emp-1");
        employee.connection().session().close(CloseReason.NORMAL);
        send(employee, "{\"type\":\"SUBSCRIBE\",\"id\":\"m1\",\"topic\":\"trip:T-1\"}");
        assertThat(registry.feeds()).isEmpty();
    }
}

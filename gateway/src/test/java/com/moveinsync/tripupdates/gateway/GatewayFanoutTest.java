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

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.store.AppendResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GatewayFanoutTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventStore store = new InMemoryEventStore(1000);
    private SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, executor, 512, 64);
    private GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));

    @AfterEach
    void tearDown() {
        fanout.close();
        executor.shutdownNow();
    }

    private AppendResult publish(TripEvent event, List<TopicId> topics) {
        AppendResult result = store.append(event, topics);
        fanout.onNotification(EventNotification.of(result));
        return result;
    }

    @Test
    void notifiedEventsAreReadFromTheStoreAndDeliveredInOrder() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);

        for (int i = 1; i <= 5; i++) {
            publish(status("e" + i), ALL);
        }

        await().atMost(WAIT).until(() -> client.sent().size() == 5);
        assertThat(client.eventIds()).containsExactly("e1", "e2", "e3", "e4", "e5");
        assertThat(client.seqs()).isSorted();
    }

    @Test
    void onlyEventsAfterSubscribingAreDelivered() {
        publish(status("before"), ALL);
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);
        publish(status("after"), ALL);

        await().atMost(WAIT).until(() -> client.sent().size() == 1);
        assertThat(client.eventIds()).containsExactly("after");
    }

    @Test
    void duplicateAndDelayedNotificationsDeliverOnceAndCauseNoExtraReads() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);
        AppendResult first = publish(status("e1"), ALL);
        AppendResult second = publish(status("e2"), ALL);
        await().atMost(WAIT).until(() -> client.sent().size() == 2);
        int readsBefore = store.reads();

        for (int i = 0; i < 5; i++) {
            fanout.onNotification(EventNotification.of(second)); // duplicate
            fanout.onNotification(EventNotification.of(first));  // delayed (older)
        }

        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> client.sent().size() == 2);
        assertThat(client.eventIds()).containsExactly("e1", "e2");
        assertThat(store.reads()).as("seq <= cursor needs no read").isEqualTo(readsBefore);
    }

    @Test
    void missedNotificationIsRecoveredByTheNextNotification() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);

        store.append(status("missed-1"), ALL); // appended, notification lost
        store.append(status("missed-2"), ALL);
        publish(status("e3"), ALL);

        await().atMost(WAIT).until(() -> client.sent().size() == 3);
        assertThat(client.eventIds()).containsExactly("missed-1", "missed-2", "e3");
    }

    @Test
    void lastMissedNotificationIsRecoveredByReconciliation() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);
        store.append(status("missed"), ALL);

        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> client.sent().isEmpty());
        fanout.reconcileAll();
        await().atMost(WAIT).until(() -> client.sent().size() == 1);

        fanout.reconcileAll(); // idempotent
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> client.sent().size() == 1);
    }

    @Test
    void resubscribingToNotificationsTriggersReconciliation() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);
        store.append(status("during-outage"), ALL);

        fanout.onSubscribed();

        await().atMost(WAIT).until(() -> client.eventIds().equals(List.of("during-outage")));
    }

    @Test
    void periodicReconciliationRecoversWithoutAnyNotification() {
        fanout.close();
        fanout = new GatewayFanout(registry, Duration.ofMillis(50));
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);

        store.append(status("silent"), ALL);

        await().atMost(WAIT).until(() -> client.eventIds().equals(List.of("silent")));
    }

    @Test
    void storeOutageDelaysButDoesNotLoseOrderedEvents() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), TRIP);
        AppendResult appended = store.append(status("e1"), ALL);

        store.failing(true);
        fanout.onNotification(EventNotification.of(appended));
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> client.sent().isEmpty());

        store.failing(false);
        fanout.reconcileAll();
        await().atMost(WAIT).until(() -> client.eventIds().equals(List.of("e1")));
    }

    @Test
    void notificationsAreRoutedOnlyToTheNotifiedTenant() {
        RecordingTransport acme = RecordingTransport.open();
        RecordingTransport globex = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", acme), TRIP);
        registry.subscribe(registry.open("g", "globex", globex), TRIP);

        publish(status("globex", "g1"), ALL);
        publish(status("acme", "a1"), ALL);

        await().atMost(WAIT).until(() -> acme.sent().size() == 1 && globex.sent().size() == 1);
        assertThat(acme.sent()).allSatisfy(e -> assertThat(e.event().tenantId()).isEqualTo("acme"));
        assertThat(globex.sent()).allSatisfy(e -> assertThat(e.event().tenantId()).isEqualTo("globex"));
    }

    @Test
    void eachSubscriptionGetsItsTopicsCopyOnceAndUnsubscribedTopicsStop() {
        RecordingTransport client = RecordingTransport.open();
        ClientSession session = registry.open("a", "acme", client);
        registry.subscribe(session, TRIP);
        registry.subscribe(session, OFFICE);

        publish(status("e1"), ALL);
        await().atMost(WAIT).until(() -> client.sent().size() == 2);
        assertThat(client.sent()).extracting(SequencedEvent::topic).containsExactlyInAnyOrder(TRIP, OFFICE);

        registry.unsubscribe(session, OFFICE);
        publish(status("e2"), ALL);
        await().atMost(WAIT).until(() -> client.sent().size() == 3);
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> client.sent().size() == 3);
        assertThat(client.sent().get(2).topic()).isEqualTo(TRIP);
    }

    @Test
    void concurrentNotificationsNeverReorderOrDuplicateReliableEvents() throws Exception {
        RecordingTransport tripClient = RecordingTransport.open();
        RecordingTransport officeClient = RecordingTransport.open();
        registry.subscribe(registry.open("t", "acme", tripClient), TRIP);
        registry.subscribe(registry.open("o", "acme", officeClient), OFFICE);
        int threads = 8;
        int perThread = 50;
        List<Thread> publishers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            publishers.add(Thread.ofPlatform().start(() -> {
                for (int i = 0; i < perThread; i++) {
                    AppendResult result = store.append(status("t" + thread + "-" + i), ALL);
                    EventNotification notification = EventNotification.of(result);
                    if (ThreadLocalRandom.current().nextInt(4) != 0) { // some notifications are lost
                        fanout.onNotification(notification);
                    }
                    if (ThreadLocalRandom.current().nextBoolean()) { // some are duplicated
                        fanout.onNotification(notification);
                    }
                }
            }));
        }
        for (Thread publisher : publishers) {
            publisher.join();
        }
        fanout.reconcileAll(); // recovers notifications lost at the very end

        int total = threads * perThread;
        await().atMost(WAIT).until(() -> tripClient.sent().size() == total && officeClient.sent().size() == total);
        for (RecordingTransport client : List.of(tripClient, officeClient)) {
            List<Long> seqs = client.seqs();
            assertThat(seqs).isSorted().doesNotHaveDuplicates().hasSize(total);
        }
    }

    @Test
    void latestLocationIsReadFromTheStoreAndDuplicatesAreIgnored() {
        RecordingTransport client = RecordingTransport.open();
        registry.subscribe(registry.open("a", "acme", client), OFFICE);

        AppendResult first = publish(location("l1", "C-1", 1.0), List.of(CAB, OFFICE));
        await().atMost(WAIT).until(() -> client.sent().size() == 1);
        store.append(location("l2", "C-1", 2.0), List.of(CAB, OFFICE)); // newer, its notification lost
        fanout.onNotification(EventNotification.of(first)); // stale duplicate: no read, no delivery
        int reads = store.reads();
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> client.sent().size() == 1);
        assertThat(store.reads()).isEqualTo(reads);

        publish(location("l3", "C-1", 3.0), List.of(CAB, OFFICE));
        await().atMost(WAIT).until(() -> client.sent().size() == 2);
        assertThat(client.eventIds()).containsExactly("l1", "l3");
    }

    @Test
    void sosOvertakesPendingLocationsButNotEarlierStatusOfTheSameTopic() {
        RecordingTransport desk = RecordingTransport.gated();
        ClientSession session = registry.open("desk", "acme", desk);
        registry.subscribe(session, OFFICE);

        publish(status("warm-up"), ALL); // the writer takes it and blocks in send: the desk is frozen
        publish(location("l1", "C-1", 1.0), List.of(CAB, OFFICE));
        publish(location("l2", "C-2", 1.0), List.of(TopicId.cab("C-2"), OFFICE));
        publish(status("s2"), ALL);
        publish(location("l3", "C-3", 1.0), List.of(TopicId.cab("C-3"), OFFICE));
        publish(sos("sos"), ALL);
        await().atMost(WAIT).until(() -> session.outbox().reliableDepth() == 2 && session.outbox().locationDepth() == 3);

        desk.release(100);

        await().atMost(WAIT).until(() -> desk.sent().size() == 6);
        assertThat(desk.eventIds()).containsExactly("warm-up", "s2", "sos", "l1", "l2", "l3");
    }

    @Test
    void slowClientIsDisconnectedWithoutDelayingOthers() {
        fanout.close();
        registry = new SubscriptionRegistry(store, 1000, executor, 8, 64);
        fanout = new GatewayFanout(registry, Duration.ofHours(1));
        RecordingTransport frozen = RecordingTransport.gated();
        RecordingTransport healthy = RecordingTransport.open();
        ClientSession frozenSession = registry.open("frozen", "acme", frozen);
        registry.subscribe(frozenSession, TRIP);
        registry.subscribe(registry.open("healthy", "acme", healthy), TRIP);

        for (int i = 1; i <= 30; i++) {
            publish(status("e" + i), ALL);
        }

        await().atMost(WAIT).until(() -> healthy.sent().size() == 30);
        assertThat(healthy.seqs()).isSorted().doesNotHaveDuplicates();
        await().atMost(WAIT).until(() -> frozen.closeReason() == CloseReason.SLOW_CONSUMER);
        assertThat(frozenSession.closeReason()).isEqualTo(CloseReason.SLOW_CONSUMER);
        await().atMost(WAIT).until(() -> registry.feed(new TenantTopic("acme", TRIP)).subscriptionCount() == 1);
        assertThat(registry.topicsOf(frozenSession)).isEmpty();
    }

    @Test
    void unsubscribePurgesPendingEventsAndLocationsOfThatTopicOnly() {
        RecordingTransport desk = RecordingTransport.gated();
        ClientSession session = registry.open("desk", "acme", desk);
        registry.subscribe(session, OFFICE);
        registry.subscribe(session, TRIP);

        // The writer takes one copy of warm-up (trip or office: the two feeds drain independently) and blocks.
        publish(status("warm-up"), ALL);
        publish(status("e2"), ALL);
        publish(location("l1", "C-1", 1.0), List.of(CAB, OFFICE));
        await().atMost(WAIT).until(() -> session.outbox().reliableDepth() == 3 && session.outbox().locationDepth() == 1);

        assertThat(registry.unsubscribe(session, OFFICE)).isTrue();
        assertThat(session.outbox().locationDepth()).as("pending office location purged").isZero();

        publish(status("e3"), ALL);
        desk.release(100);
        await().atMost(WAIT).until(() -> desk.sent().stream().filter(e -> e.topic().equals(TRIP)).count() == 3);
        await().during(Duration.ofMillis(200)).atMost(WAIT).until(() -> desk.sent().size() <= 4);
        assertThat(desk.sent().stream().filter(e -> e.topic().equals(TRIP)).map(e -> e.event().eventId()))
                .containsExactly("warm-up", "e2", "e3");
        assertThat(desk.sent().stream().filter(e -> e.topic().equals(OFFICE)).map(e -> e.event().eventId()).toList())
                .as("only the frame already in the writer's hands may still be an office event")
                .isSubsetOf(List.of("warm-up"));
        assertThat(desk.sent()).noneMatch(e -> e.type().isConflatable());
    }

    @Test
    void historyTrimmedBeyondTheCursorForcesResync() {
        InMemoryEventStore small = new InMemoryEventStore(3);
        SubscriptionRegistry smallRegistry = new SubscriptionRegistry(small, 3, executor, 512, 64);
        GatewayFanout smallFanout = new GatewayFanout(smallRegistry, Duration.ofHours(1));
        try {
            RecordingTransport client = RecordingTransport.open();
            ClientSession session = smallRegistry.open("a", "acme", client);
            smallRegistry.subscribe(session, TRIP);
            AppendResult last = null;
            for (int i = 1; i <= 6; i++) {
                last = small.append(status("e" + i), ALL); // all notifications lost; log keeps only 3
            }
            smallFanout.onNotification(EventNotification.of(last));

            await().atMost(WAIT).until(() -> client.closeReason() == CloseReason.RESYNC_REQUIRED);
            assertThat(client.sent()).isEmpty();
        } finally {
            smallFanout.close();
        }
    }
}

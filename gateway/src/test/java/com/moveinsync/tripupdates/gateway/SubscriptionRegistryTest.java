package com.moveinsync.tripupdates.gateway;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.CAB;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.OFFICE;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.TRIP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class SubscriptionRegistryTest {

    private final InMemoryEventStore store = new InMemoryEventStore(1000);
    private final SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, Runnable::run, 64, 64);

    @Test
    void subscribeAndUnsubscribeManageOneFeedPerTenantTopic() {
        ClientSession a = registry.open("a", "acme", RecordingTransport.open());
        ClientSession b = registry.open("b", "acme", RecordingTransport.open());
        TenantTopic trip = new TenantTopic("acme", TRIP);

        registry.subscribe(a, TRIP);
        registry.subscribe(b, TRIP);
        registry.subscribe(a, CAB);

        assertThat(registry.feed(trip).subscriptionCount()).isEqualTo(2);
        assertThat(registry.feeds()).hasSize(2);
        assertThat(registry.topicsOf(a)).containsExactlyInAnyOrder(TRIP, CAB);

        assertThat(registry.unsubscribe(a, TRIP)).isTrue();
        assertThat(registry.unsubscribe(a, TRIP)).isFalse();
        assertThat(registry.feed(trip).subscriptionCount()).isEqualTo(1);

        assertThat(registry.unsubscribe(b, TRIP)).isTrue();
        assertThat(registry.feed(trip)).as("last subscriber gone").isNull();
        assertThat(registry.unsubscribe(b, OFFICE)).isFalse();
    }

    @Test
    void subscribingTwiceIsIdempotent() {
        ClientSession a = registry.open("a", "acme", RecordingTransport.open());
        Subscription first = registry.subscribe(a, TRIP);
        assertThat(registry.subscribe(a, TRIP)).isSameAs(first);
        assertThat(registry.feed(new TenantTopic("acme", TRIP)).subscriptionCount()).isEqualTo(1);
    }

    @Test
    void sameTopicNameInTwoTenantsUsesSeparateFeeds() {
        ClientSession acme = registry.open("a", "acme", RecordingTransport.open());
        ClientSession globex = registry.open("g", "globex", RecordingTransport.open());

        registry.subscribe(acme, TRIP);
        registry.subscribe(globex, TRIP);

        TopicFeed acmeFeed = registry.feed(new TenantTopic("acme", TRIP));
        TopicFeed globexFeed = registry.feed(new TenantTopic("globex", TRIP));
        assertThat(acmeFeed).isNotNull().isNotSameAs(globexFeed);
        assertThat(acmeFeed.subscriptionCount()).isEqualTo(1);
        assertThat(globexFeed.subscriptionCount()).isEqualTo(1);
        assertThat(registry.feed(new TenantTopic("initech", TRIP))).isNull();
    }

    @Test
    void closingASessionRemovesItsSubscriptionsAndBlocksNewOnes() {
        RecordingTransport transport = RecordingTransport.open();
        ClientSession a = registry.open("a", "acme", transport);
        registry.subscribe(a, TRIP);
        registry.subscribe(a, OFFICE);

        a.close(CloseReason.NORMAL);

        await().atMost(Duration.ofSeconds(5)).until(() -> registry.feeds().isEmpty());
        assertThat(transport.closeReason()).isEqualTo(CloseReason.NORMAL);
        assertThat(registry.topicsOf(a)).isEmpty();
        assertThatThrownBy(() -> registry.subscribe(a, TRIP)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedFeedInitialisationLeavesNoFeedBehind() {
        ClientSession a = registry.open("a", "acme", RecordingTransport.open());
        store.failing(true);
        assertThatThrownBy(() -> registry.subscribe(a, TRIP)).isInstanceOf(RuntimeException.class);
        assertThat(registry.feeds()).isEmpty();
        store.failing(false);
        registry.subscribe(a, TRIP);
        assertThat(registry.feeds()).hasSize(1);
    }

    @Test
    void concurrentSubscribeAndUnsubscribeLeaveAConsistentRegistry() throws InterruptedException {
        List<ClientSession> sessions = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            sessions.add(registry.open("s" + i, "acme", RecordingTransport.open()));
        }
        List<TopicId> topics = List.of(TRIP, CAB, OFFICE, TopicId.office("O-2"));
        List<Thread> threads = new ArrayList<>();
        for (ClientSession session : sessions) {
            threads.add(Thread.ofPlatform().start(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < 2_000; i++) {
                    TopicId topic = topics.get(random.nextInt(topics.size()));
                    if (random.nextBoolean()) {
                        registry.subscribe(session, topic);
                    } else {
                        registry.unsubscribe(session, topic);
                    }
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join();
        }

        // Every feed's subscriber count equals the number of sessions that think they are subscribed.
        for (TopicId topic : topics) {
            long subscribed = sessions.stream().filter(s -> registry.topicsOf(s).contains(topic)).count();
            TopicFeed feed = registry.feed(new TenantTopic("acme", topic));
            assertThat(feed == null ? 0 : feed.subscriptionCount()).as(topic.name()).isEqualTo((int) subscribed);
        }
        sessions.forEach(registry::removeAll);
        assertThat(registry.feeds()).isEmpty();
    }
}

package com.moveinsync.tripupdates.gateway;

import static com.moveinsync.tripupdates.gateway.GatewayEvents.ALL;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.OFFICE;
import static com.moveinsync.tripupdates.gateway.GatewayEvents.status;
import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.notify.EventNotification;
import com.moveinsync.tripupdates.store.AppendResult;
import com.moveinsync.tripupdates.store.EventStore;
import com.moveinsync.tripupdates.store.TopicHistory;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression test for the hang found by the 10k-connection load test: a feed drain holds the topic lock while blocked
 * on a (slow) store read, and many virtual threads subscribe to the same topic at once. If subscribing waited for the
 * feed lock inside a {@code synchronized} block, every carrier thread would be pinned by a waiting subscriber and the
 * drain could never be resumed to release the lock.
 */
class VirtualThreadPinningTest {

    /** Delegates to the in-memory store, with every read slowed down like a busy Redis. */
    private static final class SlowStore implements EventStore {
        private final InMemoryEventStore delegate = new InMemoryEventStore(1000);

        @Override
        public AppendResult append(TripEvent event, List<TopicId> topics) {
            return delegate.append(event, topics);
        }

        @Override
        public TopicHistory readAfter(TenantTopic topic, long afterSeq, int limit) {
            pause();
            return delegate.readAfter(topic, afterSeq, limit);
        }

        @Override
        public long openTopic(TenantTopic topic) {
            pause();
            return delegate.openTopic(topic);
        }

        @Override
        public List<SequencedEvent> latestLocations(TenantTopic topic) {
            pause();
            return delegate.latestLocations(topic);
        }

        @Override
        public Optional<SequencedEvent> latestLocation(TenantTopic topic, String cabId) {
            return delegate.latestLocation(topic, cabId);
        }

        private static void pause() {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void manyConcurrentSubscribersToOneTopicWhileItsFeedIsReadingCompleteWithoutStarvation() throws Exception {
        SlowStore store = new SlowStore();
        ExecutorService feedExecutor = Executors.newVirtualThreadPerTaskExecutor();
        SubscriptionRegistry registry = new SubscriptionRegistry(store, 1000, feedExecutor, 64, 64);
        GatewayFanout fanout = new GatewayFanout(registry, Duration.ofHours(1));
        try {
            registry.subscribe(registry.open("first", "acme", RecordingTransport.open()), OFFICE);
            int subscribers = Runtime.getRuntime().availableProcessors() * 25; // far more than carrier threads
            CountDownLatch done = new CountDownLatch(subscribers);
            AtomicInteger failures = new AtomicInteger();
            // Not try-with-resources: close() would wait forever for deadlocked threads instead of failing.
            ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
            try {
                for (int i = 0; i < 20; i++) { // keep the feed draining (lock held across slow reads)
                    fanout.onNotification(EventNotification.of(store.append(status("e" + i), ALL)));
                }
                for (int i = 0; i < subscribers; i++) {
                    int id = i;
                    clients.submit(() -> {
                        try {
                            registry.subscribe(registry.open("s" + id, "acme", RecordingTransport.open()), OFFICE);
                        } catch (RuntimeException e) {
                            failures.incrementAndGet();
                        } finally {
                            done.countDown();
                        }
                    });
                }
                assertThat(done.await(60, TimeUnit.SECONDS)).as("all subscribers completed").isTrue();
            } finally {
                clients.shutdownNow();
            }
            assertThat(failures.get()).isZero();
            assertThat(registry.feed(new TenantTopic("acme", OFFICE)).subscriptionCount()).isEqualTo(subscribers + 1);
        } finally {
            // Leave nothing running for later tests in this JVM: stop notifications, close every session (which
            // unregisters it and ends its writer) and wait for in-flight feed drains to finish.
            fanout.close();
            registry.sessions().forEach(session -> session.close(CloseReason.SHUTDOWN));
            feedExecutor.shutdown();
            assertThat(feedExecutor.awaitTermination(30, TimeUnit.SECONDS)).as("feed drains finished").isTrue();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> registry.feeds().isEmpty());
        }
    }
}

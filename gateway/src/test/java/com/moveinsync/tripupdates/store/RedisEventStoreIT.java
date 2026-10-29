package com.moveinsync.tripupdates.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Runs the real Lua scripts against Redis 7.4 in a container. */
@Testcontainers
class RedisEventStoreIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    static RedisClient client;
    static StatefulRedisConnection<String, String> inspect;
    static RedisCommands<String, String> redis;

    private final List<RedisEventStore> stores = new ArrayList<>();
    private RedisEventStore store;

    static String uri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    static AppProperties.Retention retention(int logMax) {
        return new AppProperties.Retention(logMax, 100_000, Duration.ofHours(24), Duration.ofMinutes(5));
    }

    @BeforeAll
    static void connect() {
        client = RedisClients.create(uri(), Duration.ofSeconds(5));
        inspect = client.connect();
        redis = inspect.sync();
    }

    @AfterAll
    static void disconnect() {
        inspect.close();
        client.shutdown();
    }

    @BeforeEach
    void cleanRedis() {
        redis.flushall();
        store = newStore(1000);
    }

    @AfterEach
    void closeStores() {
        stores.forEach(RedisEventStore::close);
    }

    RedisEventStore newStore(int logMax) {
        RedisEventStore created = new RedisEventStore(client, retention(logMax));
        stores.add(created);
        return created;
    }

    static TenantTopic acme(TopicId topic) {
        return new TenantTopic("acme", topic);
    }

    @Nested
    class Sequencing {

        @Test
        void sequentialAppendsIncreaseByOnePerTopicIndependently() {
            List<AppendResult> results = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                results.add(store.append(TestEvents.status("e" + i, TripStatus.ARRIVED), TestEvents.tripCabOffice()));
            }
            for (int topic = 0; topic < 3; topic++) {
                long first = results.get(0).events().get(topic).seq();
                for (int i = 0; i < 5; i++) {
                    assertThat(results.get(i).events().get(topic).seq()).isEqualTo(first + i);
                }
            }
            // A topic that receives only some events advances only for those.
            AppendResult cabOnly = store.append(TestEvents.location("l1", null, "C-1", 1.0),
                    List.of(TopicId.cab("C-1")));
            assertThat(cabOnly.events().get(0).seq()).isEqualTo(results.get(4).events().get(1).seq() + 1);
        }

        @Test
        void resultCarriesTopicSeqEventAndRedisAssignedPublicationTime() {
            Instant before = Instant.now().minusSeconds(5);
            TripEvent event = TestEvents.sos("e1");
            AppendResult result = store.append(event, TestEvents.tripCabOffice());
            Instant after = Instant.now().plusSeconds(5);

            assertThat(result.duplicate()).isFalse();
            assertThat(result.events()).extracting(SequencedEvent::topic).containsExactlyElementsOf(
                    TestEvents.tripCabOffice());
            assertThat(result.events()).allSatisfy(sequenced -> {
                assertThat(sequenced.event()).isEqualTo(event);
                assertThat(sequenced.publishedAt()).isBetween(before, after);
                assertThat(sequenced.publishedAt()).isEqualTo(result.events().get(0).publishedAt());
            });
        }

        @Test
        void persistsTheCompleteEventPerTopic() {
            TripEvent event = TestEvents.delay("e1", 12);
            AppendResult result = store.append(event, TestEvents.tripCabOffice());

            for (SequencedEvent appended : result.events()) {
                TopicHistory history = store.readAfter(appended.tenantTopic(), appended.seq() - 1, 10);
                assertThat(history.gap()).isFalse();
                assertThat(history.headSeq()).isEqualTo(appended.seq());
                assertThat(history.events()).containsExactly(appended);
            }
            assertThat(redis.xlen(RedisKeys.BUS_CRITICAL)).isEqualTo(1);
        }

        @Test
        void sequencesAreLargeButSafeForJsonIntegers() {
            long seq = store.append(TestEvents.status("e1", TripStatus.ASSIGNED), TestEvents.tripCabOffice())
                    .events().get(0).seq();
            assertThat(seq).isGreaterThan(1_700_000_000_000_000L).isLessThan(1L << 53);
        }

        @Test
        void topicKeysGetTheConfiguredTtl() {
            store.append(TestEvents.status("e1", TripStatus.ASSIGNED), TestEvents.tripCabOffice());
            TenantTopic trip = acme(TopicId.trip("T-1"));
            for (String key : List.of(RedisKeys.seq(trip), RedisKeys.log(trip), RedisKeys.floor(trip))) {
                assertThat(redis.ttl(key)).isBetween(Duration.ofHours(23).toSeconds(), Duration.ofHours(24).toSeconds());
            }
            assertThat(redis.ttl(RedisKeys.dedup("acme", "e1"))).isBetween(1L, Duration.ofMinutes(5).toSeconds());
        }

        @Test
        void recreatedTopicNeverReusesSequenceNumbers() throws InterruptedException {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            long old = 0;
            for (int i = 0; i < 3; i++) {
                old = store.append(TestEvents.status("e" + i, TripStatus.ARRIVED), List.of(TopicId.trip("T-1")))
                        .events().get(0).seq();
            }
            // Simulate TTL expiry of the whole topic.
            redis.del(RedisKeys.seq(trip), RedisKeys.log(trip), RedisKeys.floor(trip), RedisKeys.loc(trip));
            Thread.sleep(5);

            long fresh = store.append(TestEvents.status("e9", TripStatus.ARRIVED), List.of(TopicId.trip("T-1")))
                    .events().get(0).seq();

            assertThat(fresh).isGreaterThan(old);
            TopicHistory resumeFromOld = store.readAfter(trip, old, 10);
            assertThat(resumeFromOld.gap()).isTrue();
            assertThat(resumeFromOld.events()).isEmpty();
        }
    }

    @Nested
    class Concurrency {

        @Test
        void concurrentPublishersOnTwoConnectionsNeverDuplicateOrSkipSequences() throws Exception {
            int threads = 16;
            int perThread = 200;
            int total = threads * perThread;
            RedisEventStore gatewayA = newStore(10_000);
            RedisEventStore gatewayB = newStore(10_000);
            ConcurrentLinkedQueue<AppendResult> results = new ConcurrentLinkedQueue<>();
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                RedisEventStore target = t % 2 == 0 ? gatewayA : gatewayB;
                int thread = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        TripEvent event = TestEvents.status("t" + thread + "-" + i, TripStatus.DRIVER_EN_ROUTE);
                        results.add(target.append(event, TestEvents.tripCabOffice()));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertThat(results).hasSize(total).noneMatch(AppendResult::duplicate);
            for (int topic = 0; topic < 3; topic++) {
                List<Long> seqs = new ArrayList<>();
                for (AppendResult result : results) {
                    seqs.add(result.events().get(topic).seq());
                }
                Collections.sort(seqs);
                long first = seqs.get(0);
                assertThat(seqs).as("dense and unique on topic %d", topic)
                        .containsExactlyElementsOf(LongStream.range(first, first + total).boxed().toList());
            }
            // Each event id was appended exactly once to every log and to the bus.
            TenantTopic trip = acme(TopicId.trip("T-1"));
            assertThat(redis.xlen(RedisKeys.log(trip))).isEqualTo(total);
            assertThat(redis.xlen(RedisKeys.BUS_CRITICAL)).isEqualTo(total);
            // Log order equals sequence order: read it back in pages.
            List<SequencedEvent> log = new ArrayList<>();
            long cursor = store.readAfter(trip, 0, 1).floorSeq();
            while (log.size() < total) {
                TopicHistory page = gatewayA.readAfter(trip, cursor, 1000);
                log.addAll(page.events());
                cursor = page.events().get(page.events().size() - 1).seq();
            }
            assertThat(log).extracting(SequencedEvent::seq).isSorted().doesNotHaveDuplicates();
        }
    }

    @Nested
    class Isolation {

        @Test
        void sameTopicNamesInDifferentTenantsAreIndependent() {
            TripEvent acmeEvent = TestEvents.status("acme", "e1", "T-1", "C-1", TripStatus.ARRIVED);
            TripEvent globexEvent = TestEvents.status("globex", "e1", "T-1", "C-1", TripStatus.CANCELLED);

            store.append(acmeEvent, List.of(TopicId.trip("T-1")));
            store.append(TestEvents.status("acme", "e2", "T-1", "C-1", TripStatus.PICKED_UP),
                    List.of(TopicId.trip("T-1")));
            AppendResult globex = store.append(globexEvent, List.of(TopicId.trip("T-1")));

            // Same event id in another tenant is not a duplicate.
            assertThat(globex.duplicate()).isFalse();
            TenantTopic globexTrip = new TenantTopic("globex", TopicId.trip("T-1"));
            TopicHistory globexHistory = store.readAfter(globexTrip, globex.events().get(0).seq() - 1, 10);
            assertThat(globexHistory.events()).extracting(SequencedEvent::event).containsExactly(globexEvent);

            TenantTopic acmeTrip = acme(TopicId.trip("T-1"));
            TopicHistory acmeHistory = store.readAfter(acmeTrip, store.readAfter(acmeTrip, 0, 1).floorSeq(), 10);
            assertThat(acmeHistory.events()).hasSize(2)
                    .allSatisfy(e -> assertThat(e.event().tenantId()).isEqualTo("acme"));
        }

        @Test
        void topicsOfTheSameTenantDoNotShareHistory() {
            store.append(TestEvents.status("acme", "e1", "T-1", "C-1", TripStatus.ARRIVED),
                    List.of(TopicId.trip("T-1")));
            store.append(TestEvents.status("acme", "e2", "T-2", "C-1", TripStatus.ARRIVED),
                    List.of(TopicId.trip("T-2")));

            TenantTopic t1 = acme(TopicId.trip("T-1"));
            TopicHistory history = store.readAfter(t1, store.readAfter(t1, 0, 1).floorSeq(), 10);
            assertThat(history.events()).extracting(e -> e.event().eventId()).containsExactly("e1");
            assertThat(redis.exists(RedisKeys.seq(acme(TopicId.office("T-1"))))).isZero();
        }
    }

    @Nested
    class Idempotency {

        @Test
        void retryWithSameEventReturnsOriginalResultWithoutAppending() {
            TripEvent event = TestEvents.status("e1", TripStatus.PICKED_UP);
            AppendResult first = store.append(event, TestEvents.tripCabOffice());
            AppendResult retry = newStore(1000).append(event, TestEvents.tripCabOffice());

            assertThat(retry.duplicate()).isTrue();
            assertThat(retry.events()).isEqualTo(first.events());
            assertThat(redis.xlen(RedisKeys.log(acme(TopicId.trip("T-1"))))).isEqualTo(1);
            assertThat(redis.xlen(RedisKeys.BUS_CRITICAL)).isEqualTo(1);
            assertThat(redis.get(RedisKeys.seq(acme(TopicId.trip("T-1")))))
                    .isEqualTo(Long.toString(first.events().get(0).seq()));
        }

        @Test
        void sameEventIdWithDifferentContentIsAConflictAndWritesNothing() {
            store.append(TestEvents.status("e1", TripStatus.PICKED_UP), TestEvents.tripCabOffice());
            Map<String, String> before = snapshot();

            assertThatThrownBy(() -> store.append(TestEvents.status("e1", TripStatus.CANCELLED),
                    TestEvents.tripCabOffice()))
                    .isInstanceOf(EventIdConflictException.class)
                    .satisfies(e -> assertThat(((EventStoreException) e).mayHaveBeenApplied()).isFalse());
            assertThatThrownBy(() -> store.append(TestEvents.status("e1", TripStatus.PICKED_UP),
                    List.of(TopicId.trip("T-1"))))
                    .as("same content, different topics")
                    .isInstanceOf(EventIdConflictException.class);

            assertThat(snapshot()).isEqualTo(before);
        }

        @Test
        void eventIdCanBeReusedAfterTheDedupWindow() {
            TripEvent event = TestEvents.status("e1", TripStatus.PICKED_UP);
            AppendResult first = store.append(event, TestEvents.tripCabOffice());
            redis.del(RedisKeys.dedup("acme", "e1")); // what TTL expiry does

            AppendResult later = store.append(event, TestEvents.tripCabOffice());
            assertThat(later.duplicate()).isFalse();
            assertThat(later.events().get(0).seq()).isEqualTo(first.events().get(0).seq() + 1);
        }
    }

    @Nested
    class Validation {

        @Test
        void invalidInputNeverReachesRedis() {
            store.append(TestEvents.status("e0", TripStatus.ASSIGNED), TestEvents.tripCabOffice());
            Map<String, String> before = snapshot();
            TripEvent event = TestEvents.status("e1", TripStatus.ARRIVED);

            assertThatThrownBy(() -> store.append(event, List.of(TopicId.trip("T-OTHER"))))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.append(event, List.of(TopicId.cab("C-OTHER"))))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.append(event, List.of()))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.append(event, null))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.append(event, List.of(TopicId.trip("T-1"), TopicId.trip("T-1"))))
                    .isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.append(null, TestEvents.tripCabOffice()))
                    .isInstanceOf(DomainValidationException.class);
            List<TopicId> tooMany = new ArrayList<>();
            for (int i = 0; i <= RedisEventStore.MAX_TOPICS_PER_EVENT; i++) {
                tooMany.add(TopicId.office("O-" + i));
            }
            assertThatThrownBy(() -> store.append(event, tooMany)).isInstanceOf(DomainValidationException.class);
            TripEvent locationWithoutTrip = TestEvents.location("l1", null, "C-1", 1.0);
            assertThatThrownBy(() -> store.append(locationWithoutTrip, List.of(TopicId.trip("T-1"))))
                    .isInstanceOf(DomainValidationException.class);

            assertThat(snapshot()).isEqualTo(before);
        }

        @Test
        void corruptTopicStateIsRejectedBeforeAnyWrite() {
            redis.set(RedisKeys.seq(acme(TopicId.cab("C-1"))), "not-a-number");
            Map<String, String> before = snapshot();

            assertThatThrownBy(() -> store.append(TestEvents.status("e1", TripStatus.ARRIVED),
                    TestEvents.tripCabOffice()))
                    .isInstanceOf(AppendRejectedException.class)
                    .satisfies(e -> assertThat(((EventStoreException) e).mayHaveBeenApplied()).isFalse());
            assertThatThrownBy(() -> store.append(TestEvents.location("l1", "T-1", "C-1", 1.0),
                    TestEvents.tripCabOffice()))
                    .isInstanceOf(AppendRejectedException.class);

            // trip:T-1 comes first in the key order and was not incremented; nothing reached the bus.
            assertThat(snapshot()).isEqualTo(before);
        }

        @Test
        void logAheadOfCounterIsRejectedBeforeAnyWrite() {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            store.append(TestEvents.status("e1", TripStatus.ARRIVED), List.of(TopicId.trip("T-1")));
            redis.set(RedisKeys.seq(trip), "1"); // counter behind the log, e.g. partial data loss
            Map<String, String> before = snapshot();

            assertThatThrownBy(() -> store.append(TestEvents.status("e2", TripStatus.ARRIVED),
                    List.of(TopicId.trip("T-1"))))
                    .isInstanceOf(AppendRejectedException.class);
            assertThat(snapshot()).isEqualTo(before);
        }

        @Test
        void readRejectsInvalidArguments() {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            assertThatThrownBy(() -> store.readAfter(trip, -1, 10)).isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.readAfter(trip, 0, 0)).isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.readAfter(trip, 0, 1001)).isInstanceOf(DomainValidationException.class);
            assertThatThrownBy(() -> store.readAfter(null, 0, 10)).isInstanceOf(DomainValidationException.class);
        }
    }

    @Nested
    class Retention {

        @Test
        void logIsTrimmedExactlyAndTrimmedHistoryIsReportedAsAGap() {
            RedisEventStore small = newStore(5);
            TenantTopic trip = acme(TopicId.trip("T-1"));
            List<Long> seqs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                seqs.add(small.append(TestEvents.status("e" + i, TripStatus.ARRIVED), List.of(TopicId.trip("T-1")))
                        .events().get(0).seq());
            }

            assertThat(redis.xlen(RedisKeys.log(trip))).isEqualTo(5);
            TopicHistory complete = small.readAfter(trip, seqs.get(2), 5);
            assertThat(complete.floorSeq()).isEqualTo(seqs.get(2));
            assertThat(complete.headSeq()).isEqualTo(seqs.get(7));
            assertThat(complete.gap()).isFalse();
            assertThat(complete.events()).extracting(SequencedEvent::seq).containsExactlyElementsOf(seqs.subList(3, 8));

            TopicHistory trimmed = small.readAfter(trip, seqs.get(1), 5);
            assertThat(trimmed.gap()).isTrue();
            assertThat(trimmed.events()).isEmpty();
        }

        @Test
        void unknownPositionIsAGapAndHeadPositionIsComplete() {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            long head = store.append(TestEvents.status("e1", TripStatus.ARRIVED), List.of(TopicId.trip("T-1")))
                    .events().get(0).seq();

            TopicHistory atHead = store.readAfter(trip, head, 10);
            assertThat(atHead.gap()).isFalse();
            assertThat(atHead.events()).isEmpty();

            assertThat(store.readAfter(trip, head + 1, 10).gap()).as("ahead of head").isTrue();
            assertThat(store.readAfter(trip, 0, 10).gap()).as("before the topic started").isTrue();
        }

        @Test
        void unknownTopicHasNoHistoryAndNoGapAtZero() {
            TopicHistory history = store.readAfter(acme(TopicId.trip("NEW")), 0, 10);
            assertThat(history.headSeq()).isZero();
            assertThat(history.floorSeq()).isZero();
            assertThat(history.gap()).isFalse();
            assertThat(history.events()).isEmpty();
        }

        @Test
        void readHonoursTheLimitForPaging() {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            for (int i = 0; i < 5; i++) {
                store.append(TestEvents.status("e" + i, TripStatus.ARRIVED), List.of(TopicId.trip("T-1")));
            }
            long floor = store.readAfter(trip, 0, 1).floorSeq();
            TopicHistory page = store.readAfter(trip, floor, 2);
            assertThat(page.events()).extracting(SequencedEvent::seq).containsExactly(floor + 1, floor + 2);
        }
    }

    @Nested
    class Locations {

        @Test
        void locationsShareTheTopicSequenceButNeverEnterTheLog() {
            TenantTopic trip = acme(TopicId.trip("T-1"));
            long status = store.append(TestEvents.status("e1", TripStatus.PICKED_UP), TestEvents.tripCabOffice())
                    .events().get(0).seq();
            AppendResult first = store.append(TestEvents.location("l1", "T-1", "C-1", 1.0), TestEvents.tripCabOffice());
            AppendResult second = store.append(TestEvents.location("l2", "T-1", "C-1", 2.0), TestEvents.tripCabOffice());

            assertThat(first.events().get(0).seq()).isEqualTo(status + 1);
            assertThat(second.events().get(0).seq()).isEqualTo(status + 2);
            assertThat(first.events()).allSatisfy(e -> assertThat(e.type()).isEqualTo(EventType.LOCATION));
            assertThat(redis.xlen(RedisKeys.log(trip))).isEqualTo(1);
            assertThat(redis.xlen(RedisKeys.BUS_CRITICAL)).isEqualTo(1);

            // Only the latest location per cab is kept, with its own sequence number.
            assertThat(store.latestLocations(trip)).containsExactly(second.events().get(0));
            TopicHistory history = store.readAfter(trip, status - 1, 10);
            assertThat(history.headSeq()).isEqualTo(status + 2);
            assertThat(history.events()).extracting(e -> e.event().eventId()).containsExactly("e1");
        }

        @Test
        void latestLocationIsKeptPerCab() {
            TenantTopic office = acme(TopicId.office("O-1"));
            store.append(TestEvents.location("l1", null, "C-1", 1.0), List.of(TopicId.office("O-1")));
            AppendResult c2 = store.append(TestEvents.location("l2", null, "C-2", 2.0), List.of(TopicId.office("O-1")));
            AppendResult c1 = store.append(TestEvents.location("l3", null, "C-1", 3.0), List.of(TopicId.office("O-1")));

            assertThat(store.latestLocations(office)).containsExactly(c2.events().get(0), c1.events().get(0));
        }
    }

    /** All keys with their values (type-aware), for "nothing changed" assertions. */
    static Map<String, String> snapshot() {
        Map<String, String> state = new HashMap<>();
        for (String key : redis.keys("*")) {
            String type = redis.type(key);
            String value = switch (type) {
                case "string" -> redis.get(key);
                case "hash" -> new TreeMap<>(redis.hgetall(key)).toString();
                case "stream" -> redis.xrange(key, Range.create("-", "+")).toString();
                default -> type;
            };
            state.put(key, type + "=" + value);
        }
        return state;
    }
}

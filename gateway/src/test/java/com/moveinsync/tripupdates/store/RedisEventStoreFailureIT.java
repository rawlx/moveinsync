package com.moveinsync.tripupdates.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.domain.TripStatus;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Failure injection: Redis paused (no reply → unknown outcome), unreachable, and with an empty script cache. */
@Testcontainers
class RedisEventStoreFailureIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    private static final AppProperties.Retention RETENTION =
            new AppProperties.Retention(1000, 100_000, Duration.ofHours(24), Duration.ofMinutes(5));
    private static final TenantTopic TRIP = new TenantTopic("acme", TopicId.trip("T-1"));

    private RedisClient client;
    private StatefulRedisConnection<String, String> inspect;
    private RedisCommands<String, String> redis;
    private RedisEventStore store;

    @BeforeEach
    void setUp() {
        client = RedisClients.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                Duration.ofMillis(500));
        inspect = client.connect();
        redis = inspect.sync();
        redis.flushall();
        store = new RedisEventStore(client, RETENTION);
    }

    @AfterEach
    void tearDown() {
        unpause();
        store.close();
        inspect.close();
        client.shutdown();
    }

    @Test
    void timeoutIsReportedAsUnknownOutcomeAndRetryWithSameEventIsAppliedOnce() {
        TripEvent event = TestEvents.status("e1", TripStatus.PICKED_UP);
        store.append(TestEvents.status("e0", TripStatus.ASSIGNED), TestEvents.tripCabOffice()); // warm up

        pause();
        assertThatThrownBy(() -> store.append(event, TestEvents.tripCabOffice()))
                .isInstanceOf(AppendOutcomeUnknownException.class)
                .satisfies(e -> assertThat(((EventStoreException) e).mayHaveBeenApplied()).isTrue());
        unpause();

        // The paused command was already on the wire, so Redis executes it after unpausing. The caller cannot
        // know that; retrying with the same event must not append it a second time.
        AppendResult retry = store.append(event, TestEvents.tripCabOffice());

        assertThat(retry.duplicate()).isTrue();
        assertThat(redis.xlen(RedisKeys.log(TRIP))).isEqualTo(2);
        assertThat(redis.xlen(RedisKeys.BUS_CRITICAL)).isEqualTo(2);
        long floor = store.readAfter(TRIP, 0, 1).floorSeq();
        assertThat(store.readAfter(TRIP, floor, 10).events())
                .extracting(e -> e.event().eventId()).containsExactly("e0", "e1");
    }

    @Test
    void unreachableRedisIsReportedAsUnavailableAndNothingWasApplied() throws IOException {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        RedisClient unreachable = RedisClients.create("redis://127.0.0.1:" + closedPort, Duration.ofMillis(500));
        RedisEventStore offline = new RedisEventStore(unreachable, RETENTION);
        try {
            assertThatThrownBy(() -> offline.append(TestEvents.status("e1", TripStatus.ARRIVED),
                    TestEvents.tripCabOffice()))
                    .isInstanceOf(EventStoreUnavailableException.class)
                    .satisfies(e -> assertThat(((EventStoreException) e).mayHaveBeenApplied()).isFalse());
            assertThatThrownBy(() -> offline.readAfter(TRIP, 0, 10))
                    .isInstanceOf(EventStoreUnavailableException.class);
        } finally {
            offline.close();
            unreachable.shutdown();
        }
    }

    @Test
    void appendSucceedsAfterTheScriptCacheIsFlushed() {
        store.append(TestEvents.status("e1", TripStatus.ARRIVED), TestEvents.tripCabOffice());
        redis.scriptFlush(); // what a Redis restart does to the script cache

        AppendResult result = store.append(TestEvents.status("e2", TripStatus.PICKED_UP), TestEvents.tripCabOffice());

        assertThat(result.duplicate()).isFalse();
        assertThat(redis.xlen(RedisKeys.log(TRIP))).isEqualTo(2);
    }

    @Test
    void outOfMemoryRejectsTheAppendWithoutWriting() {
        store.append(TestEvents.status("e1", TripStatus.ARRIVED), TestEvents.tripCabOffice());
        long logLength = redis.xlen(RedisKeys.log(TRIP));
        String seq = redis.get(RedisKeys.seq(TRIP));
        redis.configSet("maxmemory-policy", "noeviction");
        redis.configSet("maxmemory", "1");
        try {
            assertThatThrownBy(() -> store.append(TestEvents.status("e2", TripStatus.PICKED_UP),
                    TestEvents.tripCabOffice()))
                    .isInstanceOf(AppendRejectedException.class)
                    .satisfies(e -> assertThat(((EventStoreException) e).mayHaveBeenApplied()).isFalse());
        } finally {
            redis.configSet("maxmemory", "0");
        }
        assertThat(redis.xlen(RedisKeys.log(TRIP))).isEqualTo(logLength);
        assertThat(redis.get(RedisKeys.seq(TRIP))).isEqualTo(seq);
    }

    private static void pause() {
        DockerClientFactory.instance().client().pauseContainerCmd(REDIS.getContainerId()).exec();
    }

    private static void unpause() {
        var docker = DockerClientFactory.instance().client();
        Boolean paused = docker.inspectContainerCmd(REDIS.getContainerId()).exec().getState().getPaused();
        if (Boolean.TRUE.equals(paused)) {
            docker.unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
    }
}

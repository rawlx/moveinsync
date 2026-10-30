package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.Identifiers;
import com.moveinsync.tripupdates.domain.SequencedEvent;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link EventStore} on Redis Streams. Each append is one Lua script, so sequence assignment, log append, trimming,
 * bus append and the idempotency record are atomic (Redis runs scripts serially). The store never retries an
 * append; the only resend is EVAL after a NOSCRIPT reply, which means the script did not run.
 *
 * <p>The connection is opened lazily on first use, so the application can start while Redis is still coming up.
 */
public final class RedisEventStore implements EventStore, AutoCloseable {

    static final int MAX_TOPICS_PER_EVENT = 8;

    private static final Script APPEND_ORDERED = Script.load("redis/append-ordered.lua");
    private static final Script APPEND_LOCATION = Script.load("redis/append-location.lua");
    private static final Script READ_AFTER = Script.load("redis/read-after.lua");
    private static final Script OPEN_TOPIC = Script.load("redis/open-topic.lua");

    private final RedisClient client;
    private final AppProperties.Retention retention;
    private final EventJson json = new EventJson();
    private final ReentrantLock connectLock = new ReentrantLock();
    private volatile StatefulRedisConnection<String, String> connection;

    public RedisEventStore(RedisClient client, AppProperties.Retention retention) {
        this.client = client;
        this.retention = retention;
    }

    @Override
    public AppendResult append(TripEvent event, List<TopicId> topics) {
        List<TenantTopic> targets = validate(event, topics);
        String body = json.encode(event);
        List<Object> reply;
        try {
            reply = event.type().isConflatable() ? appendLocation(event, targets, body) : appendOrdered(event, targets, body);
        } catch (RedisCommandExecutionException e) {
            throw new AppendRejectedException("Redis rejected the append of event " + event.eventId(), e);
        } catch (RedisCommandTimeoutException e) {
            throw new AppendOutcomeUnknownException(event.eventId(), e);
        } catch (RedisException e) {
            if (wasNotSent(e)) {
                throw new EventStoreUnavailableException("Redis is unavailable", e);
            }
            throw new AppendOutcomeUnknownException(event.eventId(), e);
        }
        return toResult(event, targets, reply);
    }

    @Override
    public TopicHistory readAfter(TenantTopic topic, long afterSeq, int limit) {
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
        if (afterSeq < 0) {
            throw new DomainValidationException("afterSeq", "must be >= 0");
        }
        if (limit < 1 || limit > retention.logMaxLength()) {
            throw new DomainValidationException("limit", "must be between 1 and " + retention.logMaxLength());
        }
        List<Object> reply = read(() -> eval(READ_AFTER,
                new String[] {RedisKeys.seq(topic), RedisKeys.floor(topic), RedisKeys.log(topic)},
                Long.toString(afterSeq), Integer.toString(limit)));
        long head = (Long) reply.get(0);
        long floor = (Long) reply.get(1);
        boolean gap = afterSeq < floor || afterSeq > head;
        List<SequencedEvent> events = new ArrayList<>();
        for (Object entry : (List<?>) reply.get(2)) {
            List<?> idAndFields = (List<?>) entry;
            String id = (String) idAndFields.get(0);
            Map<String, String> fields = fields((List<?>) idAndFields.get(1));
            long seq = Long.parseLong(id.substring(0, id.indexOf('-')));
            events.add(decode(topic, fields.get("event"), seq, fields.get("publishedAt")));
        }
        return new TopicHistory(topic, head, floor, gap, events);
    }

    @Override
    public long openTopic(TenantTopic topic) {
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
        List<Object> reply = read(() -> eval(OPEN_TOPIC,
                new String[] {RedisKeys.seq(topic), RedisKeys.floor(topic)},
                Long.toString(retention.topicKeyTtl().toSeconds())));
        return (Long) reply.get(0);
    }

    @Override
    public List<SequencedEvent> latestLocations(TenantTopic topic) {
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
        Map<String, String> byCab = read(() -> connection().sync().hgetall(RedisKeys.loc(topic)));
        List<SequencedEvent> latest = new ArrayList<>();
        for (String value : byCab.values()) {
            latest.add(decodeLocation(topic, value));
        }
        latest.sort(Comparator.comparingLong(SequencedEvent::seq));
        return latest;
    }

    @Override
    public Optional<SequencedEvent> latestLocation(TenantTopic topic, String cabId) {
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
        Identifiers.require(cabId, "cabId");
        String value = read(() -> connection().sync().hget(RedisKeys.loc(topic), cabId));
        return value == null ? Optional.empty() : Optional.of(decodeLocation(topic, value));
    }

    private SequencedEvent decodeLocation(TenantTopic topic, String value) {
        int first = value.indexOf('|');
        int second = value.indexOf('|', first + 1);
        if (first < 0 || second < 0) {
            throw new EventStoreException("Stored location on " + topic + " is malformed", null);
        }
        try {
            return decode(topic, value.substring(second + 1), Long.parseLong(value.substring(0, first)),
                    value.substring(first + 1, second));
        } catch (NumberFormatException e) {
            throw new EventStoreException("Stored location on " + topic + " is malformed", e);
        }
    }

    @Override
    public void close() {
        StatefulRedisConnection<String, String> current = connection;
        if (current != null) {
            current.close();
        }
    }

    private List<TenantTopic> validate(TripEvent event, List<TopicId> topics) {
        if (event == null) {
            throw new DomainValidationException("event", "is required");
        }
        if (topics == null || topics.isEmpty()) {
            throw new DomainValidationException("topics", "must not be empty");
        }
        if (topics.size() > MAX_TOPICS_PER_EVENT) {
            throw new DomainValidationException("topics", "must not exceed " + MAX_TOPICS_PER_EVENT);
        }
        if (new HashSet<>(topics).size() != topics.size()) {
            throw new DomainValidationException("topics", "must not contain duplicates");
        }
        List<TenantTopic> targets = new ArrayList<>(topics.size());
        for (TopicId topic : topics) {
            if (topic == null) {
                throw new DomainValidationException("topics", "must not contain null");
            }
            SequencedEvent.requireDeliverableOn(event, topic);
            targets.add(new TenantTopic(event.tenantId(), topic));
        }
        return targets;
    }

    private List<Object> appendOrdered(TripEvent event, List<TenantTopic> targets, String body) {
        List<String> keys = new ArrayList<>();
        keys.add(RedisKeys.dedup(event.tenantId(), event.eventId()));
        keys.add(RedisKeys.BUS_CRITICAL);
        addTopicKeys(keys, targets);
        List<String> args = new ArrayList<>(List.of(
                event.tenantId(),
                body,
                Integer.toString(retention.logMaxLength()),
                Integer.toString(retention.busMaxLength()),
                Long.toString(retention.topicKeyTtl().toSeconds()),
                Long.toString(retention.dedupTtl().toSeconds())));
        targets.forEach(target -> args.add(target.topic().name()));
        List<Object> reply = eval(APPEND_ORDERED, keys.toArray(String[]::new), args.toArray(String[]::new));
        if ("CONFLICT".equals(reply.get(0))) {
            throw new EventIdConflictException(event.eventId());
        }
        return reply;
    }

    private List<Object> appendLocation(TripEvent event, List<TenantTopic> targets, String body) {
        List<String> keys = new ArrayList<>();
        addTopicKeys(keys, targets);
        return eval(APPEND_LOCATION, keys.toArray(String[]::new),
                body, event.cabId(), Long.toString(retention.topicKeyTtl().toSeconds()));
    }

    private static void addTopicKeys(List<String> keys, List<TenantTopic> targets) {
        for (TenantTopic target : targets) {
            keys.add(RedisKeys.seq(target));
            keys.add(RedisKeys.log(target));
            keys.add(RedisKeys.floor(target));
            keys.add(RedisKeys.loc(target));
        }
    }

    private AppendResult toResult(TripEvent event, List<TenantTopic> targets, List<Object> reply) {
        Object status = reply.get(0);
        if ((!"OK".equals(status) && !"DUP".equals(status)) || reply.size() != targets.size() + 2) {
            // The script ran but answered unexpectedly; whether it wrote is not known.
            throw new AppendOutcomeUnknownException(event.eventId(),
                    new IllegalStateException("Unexpected append reply"));
        }
        Instant publishedAt = Instant.ofEpochMilli((Long) reply.get(1));
        List<SequencedEvent> events = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            events.add(new SequencedEvent(event, targets.get(i).topic(), (Long) reply.get(i + 2), publishedAt));
        }
        return new AppendResult(events, "DUP".equals(status));
    }

    private SequencedEvent decode(TenantTopic topic, String body, long seq, String publishedAtMs) {
        try {
            TripEvent event = json.decode(body);
            if (!event.tenantId().equals(topic.tenantId())) {
                throw new DomainValidationException("tenantId", "does not match the topic's tenant");
            }
            return new SequencedEvent(event, topic.topic(), seq, Instant.ofEpochMilli(Long.parseLong(publishedAtMs)));
        } catch (DomainValidationException | NumberFormatException | NullPointerException e) {
            throw new EventStoreException("Stored event on " + topic + " at seq " + seq + " is invalid", e);
        }
    }

    private static Map<String, String> fields(List<?> flat) {
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            map.put((String) flat.get(i), (String) flat.get(i + 1));
        }
        return map;
    }

    private <T> T read(Supplier<T> command) {
        try {
            return command.get();
        } catch (RedisCommandExecutionException e) {
            throw new EventStoreException("Redis rejected the read", e);
        } catch (RedisException e) {
            throw new EventStoreUnavailableException("Redis is unavailable", e);
        }
    }

    private List<Object> eval(Script script, String[] keys, String... args) {
        RedisCommands<String, String> commands = connection().sync();
        try {
            return commands.evalsha(script.sha1(), ScriptOutputType.MULTI, keys, args);
        } catch (RedisNoScriptException e) {
            // NOSCRIPT: the script did not run (e.g. Redis restarted). EVAL runs it and caches it again.
            return commands.eval(script.source(), ScriptOutputType.MULTI, keys, args);
        }
    }

    private StatefulRedisConnection<String, String> connection() {
        StatefulRedisConnection<String, String> current = connection;
        if (current == null) {
            connectLock.lock(); // not synchronized: connecting blocks, which would pin a virtual thread
            try {
                current = connection;
                if (current == null) {
                    current = client.connect();
                    connection = current;
                }
            } finally {
                connectLock.unlock();
            }
        }
        return current;
    }

    /**
     * Failures that guarantee the command never reached Redis: connecting failed, or the client rejected the
     * command while disconnected ({@code DisconnectedBehavior.REJECT_COMMANDS}).
     */
    private static boolean wasNotSent(RedisException e) {
        if (e instanceof RedisConnectionException) {
            return true;
        }
        String message = e.getMessage();
        return message != null && message.contains("not connected") && message.contains("rejected");
    }

    private record Script(String source, String sha1) {

        static Script load(String resource) {
            try (InputStream in = RedisEventStore.class.getClassLoader().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IllegalStateException("Missing script " + resource);
                }
                String source = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                byte[] digest = MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8));
                return new Script(source, HexFormat.of().formatHex(digest));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

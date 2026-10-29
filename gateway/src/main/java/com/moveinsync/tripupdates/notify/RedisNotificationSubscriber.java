package com.moveinsync.tripupdates.notify;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes to the notification channel. Lettuce re-subscribes automatically after a reconnect; the SUBSCRIBE
 * confirmation then reaches {@link NotificationListener#onSubscribed()}, which is the trigger for recovering
 * notifications missed during the outage.
 */
public final class RedisNotificationSubscriber implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisNotificationSubscriber.class);

    private final RedisClient client;
    private final String channel;
    private final NotificationListener listener;
    private final NotificationCodec codec = new NotificationCodec();
    private final AtomicLong malformed = new AtomicLong();
    private volatile StatefulRedisPubSubConnection<String, String> connection;

    public RedisNotificationSubscriber(RedisClient client, String channel, NotificationListener listener) {
        this.client = client;
        this.channel = channel;
        this.listener = listener;
    }

    /** Connects and subscribes; throws if Redis is unreachable. */
    public void start() {
        StatefulRedisPubSubConnection<String, String> pubSub = client.connectPubSub();
        pubSub.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String from, String message) {
                EventNotification notification;
                try {
                    notification = codec.decode(message);
                } catch (DomainValidationException e) {
                    malformed.incrementAndGet();
                    log.warn("Ignored malformed notification on {}: {}", from, e.getMessage());
                    return;
                }
                listener.onNotification(notification);
            }

            @Override
            public void subscribed(String to, long count) {
                listener.onSubscribed();
            }
        });
        pubSub.sync().subscribe(channel);
        connection = pubSub;
    }

    public long malformedCount() {
        return malformed.get();
    }

    @Override
    public void close() {
        StatefulRedisPubSubConnection<String, String> current = connection;
        if (current != null) {
            current.close();
        }
    }
}

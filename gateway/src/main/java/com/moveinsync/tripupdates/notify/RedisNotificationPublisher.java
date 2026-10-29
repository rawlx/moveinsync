package com.moveinsync.tripupdates.notify;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * Publishes notifications with Redis {@code PUBLISH}. Pub/Sub is at-most-once: a gateway that is disconnected
 * while a message is published never sees it. That is acceptable only because notifications are hints and gateways
 * reconcile against the durable streams (see {@code GatewayFanout}).
 */
public final class RedisNotificationPublisher implements NotificationPublisher, AutoCloseable {

    private final RedisClient client;
    private final String channel;
    private final NotificationCodec codec = new NotificationCodec();
    private final Object connectLock = new Object();
    private volatile StatefulRedisConnection<String, String> connection;

    public RedisNotificationPublisher(RedisClient client, String channel) {
        this.client = client;
        this.channel = channel;
    }

    @Override
    public void publish(EventNotification notification) {
        String message = codec.encode(notification);
        try {
            connection().sync().publish(channel, message);
        } catch (RedisException e) {
            throw new NotificationPublishException(notification.eventId(), e);
        }
    }

    @Override
    public void close() {
        StatefulRedisConnection<String, String> current = connection;
        if (current != null) {
            current.close();
        }
    }

    private StatefulRedisConnection<String, String> connection() {
        StatefulRedisConnection<String, String> current = connection;
        if (current == null) {
            synchronized (connectLock) {
                current = connection;
                if (current == null) {
                    current = client.connect();
                    connection = current;
                }
            }
        }
        return current;
    }
}

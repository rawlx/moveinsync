package com.moveinsync.tripupdates.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * {@code PING}s Redis on its own connection (bounded by {@code app.redis.command-timeout}). Part of the readiness group,
 * so a gateway that cannot reach its event store is not sent traffic; liveness does not depend on Redis, so the
 * gateway is not restarted for an outage it can recover from. Details expose the error type only.
 */
final class RedisHealthIndicator implements HealthIndicator, AutoCloseable {

    private final RedisClient client;
    private volatile StatefulRedisConnection<String, String> connection;

    RedisHealthIndicator(RedisClient client) {
        this.client = client;
    }

    @Override
    public Health health() {
        try {
            return "PONG".equals(connection().sync().ping()) ? Health.up().build() : Health.down().build();
        } catch (RedisException e) {
            return Health.down().withDetail("error", e.getClass().getSimpleName()).build();
        }
    }

    @Override
    public void close() {
        StatefulRedisConnection<String, String> current = connection;
        if (current != null) {
            current.close();
        }
    }

    private synchronized StatefulRedisConnection<String, String> connection() {
        if (connection == null) {
            connection = client.connect();
        }
        return connection;
    }
}

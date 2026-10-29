package com.moveinsync.tripupdates.store;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import java.time.Duration;

/**
 * Creates Lettuce clients with explicit failure behaviour: every command is bounded by {@code commandTimeout}, and
 * commands issued while disconnected fail immediately instead of being queued and sent later.
 *
 * <p>Lettuce re-sends commands that were in flight when a connection dropped once it reconnects. For ordered
 * appends this is safe because the append script is idempotent by event id; a re-sent location append only
 * produces a newer sequence number for the same latest value.
 */
public final class RedisClients {

    private RedisClients() {
    }

    public static RedisClient create(String uri, Duration commandTimeout) {
        RedisClient client = RedisClient.create(RedisURI.create(uri));
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(commandTimeout))
                .socketOptions(SocketOptions.builder().connectTimeout(commandTimeout).build())
                .build());
        return client;
    }
}

package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.gateway.CloseReason;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic upkeep of open connections:
 * <ul>
 *   <li><b>Heartbeat</b> every {@code heartbeatInterval}: a connection with no inbound traffic (text frames or pongs)
 *       for {@code idleTimeout} is closed with {@link CloseReason#IDLE_TIMEOUT}; every other connection gets a
 *       WebSocket ping, which a live client answers with a pong. This detects half-open connections and clients that
 *       stopped reading within roughly {@code idleTimeout + heartbeatInterval}.</li>
 *   <li><b>Authorization refresh</b> every {@code revalidateInterval}: every live subscription is re-checked
 *       ({@link ProtocolService#revalidate}); revoked ones are removed. A permission change therefore takes effect
 *       within one interval.</li>
 * </ul>
 * Both tasks only enqueue frames or close sessions; neither blocks on a client.
 */
public final class ConnectionMaintenance implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionMaintenance.class);

    private final Supplier<List<ProtocolService.Connection>> connections;
    private final ProtocolService protocol;
    private final Duration idleTimeout;
    private final LongSupplier clock;
    private final ScheduledExecutorService scheduler;

    public ConnectionMaintenance(Supplier<List<ProtocolService.Connection>> connections, ProtocolService protocol,
            Duration heartbeatInterval, Duration idleTimeout, Duration revalidateInterval) {
        this(connections, protocol, idleTimeout, System::currentTimeMillis);
        schedule(this::heartbeat, heartbeatInterval);
        schedule(this::revalidate, revalidateInterval);
    }

    /** Unscheduled instance for tests; call {@link #heartbeat()} and {@link #revalidate()} directly. */
    ConnectionMaintenance(Supplier<List<ProtocolService.Connection>> connections, ProtocolService protocol,
            Duration idleTimeout, LongSupplier clock) {
        this.connections = connections;
        this.protocol = protocol;
        this.idleTimeout = idleTimeout;
        this.clock = clock;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("connection-maintenance").daemon(true).factory());
    }

    /** @return the number of connections closed as idle */
    int heartbeat() {
        long now = clock.getAsLong();
        int closed = 0;
        for (ProtocolService.Connection connection : connections.get()) {
            if (connection.session().isClosed()) {
                continue;
            }
            if (now - connection.lastInboundMillis() > idleTimeout.toMillis()) {
                connection.session().close(CloseReason.IDLE_TIMEOUT);
                closed++;
            } else {
                connection.session().offerControl(Heartbeat.PING);
            }
        }
        return closed;
    }

    /** @return the number of subscriptions revoked */
    int revalidate() {
        int revoked = 0;
        for (ProtocolService.Connection connection : connections.get()) {
            if (!connection.session().isClosed()) {
                revoked += protocol.revalidate(connection);
            }
        }
        return revoked;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private void schedule(Runnable task, Duration interval) {
        long millis = interval.toMillis();
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                log.warn("Connection maintenance task failed; it will run again", e); // keep the schedule alive
            }
        }, millis, millis, TimeUnit.MILLISECONDS);
    }
}

package com.moveinsync.tripupdates.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Heartbeat over a real WebSocket: the gateway pings every 200 ms and closes connections silent for 1 s. The JDK
 * client answers pings automatically while it reads; a client that stops reading stops answering.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketHeartbeatIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.12-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("app.redis.uri", () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        registry.add("app.auth.mode", () -> "DEV");
        registry.add("app.websocket.heartbeat-interval", () -> "200ms");
        registry.add("app.websocket.idle-timeout", () -> "1s");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TripUpdatesWebSocketHandler handler;

    @Autowired
    private MeterRegistry meters;

    private double idleClosures() {
        return meters.get("tripupdates.sessions.closed").tag("reason", "IDLE_TIMEOUT").counter().count();
    }

    @Test
    void respondingClientStaysConnectedAndUnresponsiveClientIsClosedAsIdle() throws Exception {
        double idleBefore = idleClosures();
        WsTestClient responsive = WsTestClient.connect(port, "dev-acme-emp-1");
        WsTestClient unresponsive = WsTestClient.connect(port, "dev-acme-emp-2", true);
        unresponsive.send("{\"type\":\"SUBSCRIBE\",\"id\":\"u1\",\"topic\":\"trip:T-2\"}"); // consumes its only demand
        assertThat(unresponsive.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        await().atMost(Duration.ofSeconds(5)).until(() -> handler.connectionCount() == 2);

        await().atMost(Duration.ofSeconds(10)).until(() -> handler.connectionCount() == 1);
        assertThat(idleClosures() - idleBefore).isEqualTo(1);

        Thread.sleep(2_000); // well past the idle timeout: pongs keep the responsive client alive
        assertThat(handler.connectionCount()).isEqualTo(1);
        responsive.send("{\"type\":\"SUBSCRIBE\",\"id\":\"r1\",\"topic\":\"trip:T-1\"}");
        assertThat(responsive.next().get("type").asText()).isEqualTo("SUBSCRIBED");
        assertThat(responsive.closed()).isNotDone();

        responsive.close();
        unresponsive.abort();
    }
}

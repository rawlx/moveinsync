package com.moveinsync.tripupdates;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.auth.ConnectionAuthenticator;
import com.moveinsync.tripupdates.config.AppProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Starts the real servlet container (WebSocket support needs one) without Redis. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TripUpdatesApplicationTest {

    @Autowired
    private AppProperties properties;

    @Autowired
    private ConnectionAuthenticator authenticator;

    @Test
    void contextLoadsAndBindsDocumentedDefaults() {
        assertThat(properties.redis().commandTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(properties.outbox().criticalCapacity()).isEqualTo(512);
        assertThat(properties.outbox().locationCapacity()).isEqualTo(2048);
        assertThat(properties.outbox().pendingCapacity()).isEqualTo(1000);
        assertThat(properties.retention().logMaxLength()).isEqualTo(1000);
        assertThat(properties.retention().topicKeyTtl()).isEqualTo(Duration.ofHours(24));
        assertThat(properties.retention().dedupTtl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(properties.websocket().maxMessageChars()).isEqualTo(4096);
    }

    @Test
    void authenticationFailsClosedByDefault() {
        assertThat(properties.auth().mode()).isEqualTo(AppProperties.AuthMode.NONE);
        assertThat(authenticator.authenticate("dev-acme-emp-1")).isEmpty();
    }
}

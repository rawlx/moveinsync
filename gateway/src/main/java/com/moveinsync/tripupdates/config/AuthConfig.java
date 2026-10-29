package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.auth.ConnectionAuthenticator;
import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.auth.TopicDirectory;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Selects the connection authenticator and directory. Fails closed: unless {@code app.auth.mode=dev}, no identity
 * provider is configured and every WebSocket handshake is rejected. {@code dev} loads well-known test tokens and
 * reference data from {@code dev/dev-fixtures.json}; it is for local runs and tests only.
 */
@Configuration(proxyBeanMethods = false)
class AuthConfig {

    static final String DEV_FIXTURES = "dev/dev-fixtures.json";
    private static final Logger log = LoggerFactory.getLogger(AuthConfig.class);

    @Bean
    ConnectionAuthenticator connectionAuthenticator(AppProperties properties) {
        if (properties.auth().mode() == AppProperties.AuthMode.DEV) {
            log.warn("DEVELOPMENT authentication enabled: well-known dev tokens are accepted. Never use in production.");
            return DevFixtures.load(DEV_FIXTURES).authenticator();
        }
        log.warn("No identity provider configured (app.auth.mode={}): all WebSocket connections are rejected",
                properties.auth().mode());
        return token -> Optional.empty();
    }

    @Bean
    TopicDirectory topicDirectory(AppProperties properties) {
        if (properties.auth().mode() == AppProperties.AuthMode.DEV) {
            return DevFixtures.load(DEV_FIXTURES).directory();
        }
        return new InMemoryTopicDirectory(Map.of());
    }
}

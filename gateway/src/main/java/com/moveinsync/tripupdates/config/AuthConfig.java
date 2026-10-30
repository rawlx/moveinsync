package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.auth.ConnectionAuthenticator;
import com.moveinsync.tripupdates.auth.DevFixtures;
import com.moveinsync.tripupdates.auth.DevTokenAuthenticator;
import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.auth.InMemoryTopicDirectory;
import com.moveinsync.tripupdates.auth.TopicDirectory;
import com.moveinsync.tripupdates.sim.SyntheticFleet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Selects the connection authenticator and directory. Fails closed: unless {@code app.auth.mode=dev}, no identity
 * provider is configured and every WebSocket handshake is rejected. {@code dev} loads well-known test tokens and
 * reference data from {@code dev/dev-fixtures.json}, plus, if {@code app.auth.synthetic-fleet} is configured, a
 * generated {@link SyntheticFleet} for load tests. Both are for local runs and tests only.
 */
@Configuration(proxyBeanMethods = false)
class AuthConfig {

    static final String DEV_FIXTURES = "dev/dev-fixtures.json";
    private static final Logger log = LoggerFactory.getLogger(AuthConfig.class);

    /** Dev tokens and directory data (empty unless mode is DEV). */
    record DevData(Map<String, Identity> identities, InMemoryTopicDirectory directory) {
    }

    @Bean
    DevData devData(AppProperties properties) {
        if (properties.auth().mode() != AppProperties.AuthMode.DEV) {
            return new DevData(Map.of(), new InMemoryTopicDirectory(Map.of()));
        }
        DevFixtures fixtures = DevFixtures.load(DEV_FIXTURES);
        Map<String, Identity> identities = new HashMap<>(fixtures.identities());
        Map<String, InMemoryTopicDirectory.Tenant> tenants = new HashMap<>(fixtures.directory().tenants());
        AppProperties.SyntheticFleet synthetic = properties.auth().syntheticFleet();
        if (synthetic != null && synthetic.enabled()) {
            SyntheticFleet fleet = SyntheticFleet.generate(new SyntheticFleet.Spec(synthetic.tenants(),
                    synthetic.officesPerTenant(), synthetic.cabsPerOffice(), synthetic.passengersPerTrip()));
            identities.putAll(fleet.identities());
            tenants.putAll(fleet.directory().tenants());
            log.warn("DEVELOPMENT synthetic fleet loaded: {} cabs, {} dev tokens", fleet.spec().cabs(),
                    fleet.identities().size());
        }
        return new DevData(identities, new InMemoryTopicDirectory(tenants));
    }

    @Bean
    ConnectionAuthenticator connectionAuthenticator(AppProperties properties, DevData devData) {
        if (properties.auth().mode() == AppProperties.AuthMode.DEV) {
            log.warn("DEVELOPMENT authentication enabled: well-known dev tokens are accepted. Never use in production.");
            return new DevTokenAuthenticator(devData.identities());
        }
        log.warn("No identity provider configured (app.auth.mode={}): all WebSocket connections are rejected",
                properties.auth().mode());
        return token -> Optional.empty();
    }

    @Bean
    TopicDirectory topicDirectory(DevData devData) {
        return devData.directory();
    }
}

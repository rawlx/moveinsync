package com.moveinsync.tripupdates.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripupdates.domain.TopicId;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DEVELOPMENT AND TEST ONLY. Loads dev tokens and directory data from a classpath JSON resource. The data contains
 * ids only (no names, phone numbers, e-mail addresses or home locations).
 */
public final class DevFixtures {

    private final Map<String, Identity> identitiesByToken;
    private final InMemoryTopicDirectory directory;

    private DevFixtures(Map<String, Identity> identitiesByToken, InMemoryTopicDirectory directory) {
        this.identitiesByToken = identitiesByToken;
        this.directory = directory;
    }

    public static DevFixtures load(String resource) {
        try (InputStream in = DevFixtures.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Missing dev fixtures " + resource);
            }
            JsonNode root = new ObjectMapper().readTree(in);
            Map<String, InMemoryTopicDirectory.Tenant> tenants = new HashMap<>();
            root.path("tenants").properties().forEach(tenant -> tenants.put(tenant.getKey(), tenant(tenant.getValue())));
            Map<String, Identity> identities = new HashMap<>();
            root.path("tokens").properties().forEach(token -> identities.put(token.getKey(), identity(token.getValue())));
            return new DevFixtures(identities, new InMemoryTopicDirectory(tenants));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public DevTokenAuthenticator authenticator() {
        return new DevTokenAuthenticator(identitiesByToken);
    }

    /** Dev token → identity. */
    public Map<String, Identity> identities() {
        return Map.copyOf(identitiesByToken);
    }

    public InMemoryTopicDirectory directory() {
        return directory;
    }

    private static InMemoryTopicDirectory.Tenant tenant(JsonNode node) {
        Map<String, String> cabs = new HashMap<>();
        node.path("cabs").properties().forEach(cab -> cabs.put(cab.getKey(), cab.getValue().path("office").asText()));
        Map<String, TopicDirectory.Trip> trips = new HashMap<>();
        node.path("trips").properties().forEach(trip -> trips.put(trip.getKey(), new TopicDirectory.Trip(
                trip.getValue().path("office").asText(),
                trip.getValue().path("cab").asText(),
                strings(trip.getValue().path("passengers")))));
        return new InMemoryTopicDirectory.Tenant(strings(node.path("offices")), cabs, trips);
    }

    private static Identity identity(JsonNode node) {
        Set<TopicId> grants = new HashSet<>();
        strings(node.path("grants")).forEach(grant -> grants.add(TopicId.parse(grant)));
        return new Identity(node.path("subject").asText(), node.path("tenant").asText(),
                Role.valueOf(node.path("role").asText()), strings(node.path("offices")), grants);
    }

    private static Set<String> strings(JsonNode array) {
        Set<String> values = new HashSet<>();
        array.forEach(value -> values.add(value.asText()));
        return values;
    }
}

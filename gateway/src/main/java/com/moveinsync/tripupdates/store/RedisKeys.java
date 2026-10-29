package com.moveinsync.tripupdates.store;

import com.moveinsync.tripupdates.domain.TenantTopic;

/**
 * Redis key scheme. Every topic key is {@code t:{tenantId}:{kind}:{topic}}, e.g. {@code t:acme:seq:trip:T-1}.
 * Tenant and topic ids cannot contain {@code ':'} (see {@code Identifiers}), so keys of different tenants, key
 * kinds or topics can never collide.
 */
final class RedisKeys {

    /** Global stream of ordered events consumed by every gateway (ADR-0005). */
    static final String BUS_CRITICAL = "bus:critical";

    private RedisKeys() {
    }

    static String seq(TenantTopic topic) {
        return topicKey(topic, "seq");
    }

    static String log(TenantTopic topic) {
        return topicKey(topic, "log");
    }

    static String floor(TenantTopic topic) {
        return topicKey(topic, "floor");
    }

    static String loc(TenantTopic topic) {
        return topicKey(topic, "loc");
    }

    static String dedup(String tenantId, String eventId) {
        return "t:" + tenantId + ":dedup:" + eventId;
    }

    private static String topicKey(TenantTopic topic, String kind) {
        return "t:" + topic.tenantId() + ":" + kind + ":" + topic.topic().name();
    }
}

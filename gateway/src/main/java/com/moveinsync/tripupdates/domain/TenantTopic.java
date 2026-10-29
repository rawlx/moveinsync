package com.moveinsync.tripupdates.domain;

/**
 * A topic scoped to one tenant. Two tenants may use the same topic name; their {@code TenantTopic}s are never
 * equal, which is what keeps registries and storage keys tenant-isolated.
 */
public record TenantTopic(String tenantId, TopicId topic) {

    public TenantTopic {
        Identifiers.require(tenantId, "tenantId");
        if (topic == null) {
            throw new DomainValidationException("topic", "is required");
        }
    }

    @Override
    public String toString() {
        return tenantId + "/" + topic.name();
    }
}

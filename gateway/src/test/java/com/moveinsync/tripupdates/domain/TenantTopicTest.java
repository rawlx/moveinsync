package com.moveinsync.tripupdates.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TenantTopicTest {

    @Test
    void sameTopicNameInDifferentTenantsIsADifferentKey() {
        TenantTopic acme = new TenantTopic("acme", TopicId.trip("T-1"));
        TenantTopic globex = new TenantTopic("globex", TopicId.trip("T-1"));

        assertThat(acme).isNotEqualTo(globex);
        assertThat(acme).isEqualTo(new TenantTopic("acme", TopicId.parse("trip:T-1")));
        assertThat(acme.toString()).isEqualTo("acme/trip:T-1");
    }

    @Test
    void requiresValidTenantAndTopic() {
        assertThatThrownBy(() -> new TenantTopic(null, TopicId.trip("T-1")))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("tenantId: is required");
        assertThatThrownBy(() -> new TenantTopic("ac me", TopicId.trip("T-1")))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new TenantTopic("acme", null))
                .isInstanceOf(DomainValidationException.class)
                .hasMessage("topic: is required");
    }
}

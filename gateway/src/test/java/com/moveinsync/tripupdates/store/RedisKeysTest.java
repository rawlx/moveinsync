package com.moveinsync.tripupdates.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RedisKeysTest {

    @Test
    void keysAreTenantScoped() {
        TenantTopic topic = new TenantTopic("acme", TopicId.trip("T-1"));
        assertThat(RedisKeys.seq(topic)).isEqualTo("t:acme:seq:trip:T-1");
        assertThat(RedisKeys.log(topic)).isEqualTo("t:acme:log:trip:T-1");
        assertThat(RedisKeys.floor(topic)).isEqualTo("t:acme:floor:trip:T-1");
        assertThat(RedisKeys.loc(topic)).isEqualTo("t:acme:loc:trip:T-1");
        assertThat(RedisKeys.dedup("acme", "evt-1")).isEqualTo("t:acme:dedup:evt-1");
    }

    @Test
    void noCollisionsAcrossTenantsKindsAndIds() {
        Set<String> keys = new HashSet<>();
        int expected = 0;
        for (String tenant : List.of("acme", "globex", "acme-trip")) {
            for (TopicId topic : List.of(TopicId.trip("1"), TopicId.cab("1"), TopicId.office("1"),
                    TopicId.trip("seq"), TopicId.trip("1-1"))) {
                TenantTopic tenantTopic = new TenantTopic(tenant, topic);
                keys.addAll(List.of(RedisKeys.seq(tenantTopic), RedisKeys.log(tenantTopic),
                        RedisKeys.floor(tenantTopic), RedisKeys.loc(tenantTopic)));
                expected += 4;
            }
            keys.add(RedisKeys.dedup(tenant, "1"));
            expected++;
        }
        assertThat(keys).hasSize(expected).doesNotContain(RedisKeys.BUS_CRITICAL);
    }
}

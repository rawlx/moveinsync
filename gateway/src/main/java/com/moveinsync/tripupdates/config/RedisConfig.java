package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.store.EventStore;
import com.moveinsync.tripupdates.store.RedisClients;
import com.moveinsync.tripupdates.store.RedisEventStore;
import io.lettuce.core.RedisClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class RedisConfig {

    @Bean(destroyMethod = "shutdown")
    RedisClient redisClient(AppProperties properties) {
        return RedisClients.create(properties.redis().uri(), properties.redis().commandTimeout());
    }

    @Bean(destroyMethod = "close")
    EventStore eventStore(RedisClient redisClient, AppProperties properties) {
        return new RedisEventStore(redisClient, properties.retention());
    }

    /** Health contributor {@code redis}, included in the readiness group (application.yml). */
    @Bean(destroyMethod = "close")
    RedisHealthIndicator redisHealthIndicator(RedisClient redisClient) {
        return new RedisHealthIndicator(redisClient);
    }
}

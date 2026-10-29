package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.gateway.GatewayFanout;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.notify.RedisNotificationSubscriber;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.store.EventStore;
import io.lettuce.core.RedisClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class FanoutConfig {

    @Bean(destroyMethod = "close")
    RedisNotificationPublisher notificationPublisher(RedisClient redisClient, AppProperties properties) {
        return new RedisNotificationPublisher(redisClient, properties.notifications().channel());
    }

    @Bean
    EventPublisher eventPublisher(EventStore eventStore, RedisNotificationPublisher notificationPublisher) {
        return new EventPublisher(eventStore, notificationPublisher);
    }

    /** Runs feed drains; at most one task per locally subscribed topic is queued (see TopicFeed). */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService feedExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    SubscriptionRegistry subscriptionRegistry(EventStore eventStore, ExecutorService feedExecutor,
            AppProperties properties) {
        return new SubscriptionRegistry(eventStore, properties.retention().logMaxLength(), feedExecutor,
                properties.outbox().criticalCapacity(), properties.outbox().locationCapacity());
    }

    @Bean(destroyMethod = "close")
    GatewayFanout gatewayFanout(SubscriptionRegistry subscriptionRegistry, AppProperties properties) {
        return new GatewayFanout(subscriptionRegistry, properties.notifications().reconcileInterval());
    }

    @Bean
    NotificationSubscriberLifecycle notificationSubscriber(RedisClient redisClient, GatewayFanout gatewayFanout,
            AppProperties properties) {
        return new NotificationSubscriberLifecycle(
                new RedisNotificationSubscriber(redisClient, properties.notifications().channel(), gatewayFanout));
    }
}

package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.gateway.GatewayFanout;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.notify.RedisNotificationSubscriber;
import com.moveinsync.tripupdates.observability.MicrometerGatewayMetrics;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.store.EventStore;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class FanoutConfig {

    @Bean
    MicrometerGatewayMetrics gatewayMetrics(MeterRegistry meterRegistry) {
        return new MicrometerGatewayMetrics(meterRegistry);
    }

    @Bean(destroyMethod = "close")
    RedisNotificationPublisher notificationPublisher(RedisClient redisClient, AppProperties properties) {
        return new RedisNotificationPublisher(redisClient, properties.notifications().channel());
    }

    @Bean
    EventPublisher eventPublisher(EventStore eventStore, RedisNotificationPublisher notificationPublisher,
            MeterRegistry meterRegistry) {
        EventPublisher publisher = new EventPublisher(eventStore, notificationPublisher);
        FunctionCounter.builder("tripupdates.notifications.publish.failures", publisher,
                        EventPublisher::notificationFailures)
                .description("Notifications not published after a durable append (recovered by reconciliation)")
                .register(meterRegistry);
        return publisher;
    }

    /** Runs feed drains; at most one task per locally subscribed topic is queued (see TopicFeed). */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService feedExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    SubscriptionRegistry subscriptionRegistry(EventStore eventStore, ExecutorService feedExecutor,
            AppProperties properties, MicrometerGatewayMetrics gatewayMetrics) {
        SubscriptionRegistry registry = new SubscriptionRegistry(eventStore, properties.retention().logMaxLength(),
                feedExecutor, properties.outbox().criticalCapacity(), properties.outbox().locationCapacity(),
                gatewayMetrics);
        gatewayMetrics.bind(registry);
        return registry;
    }

    @Bean(destroyMethod = "close")
    GatewayFanout gatewayFanout(SubscriptionRegistry subscriptionRegistry, AppProperties properties,
            MicrometerGatewayMetrics gatewayMetrics) {
        return new GatewayFanout(subscriptionRegistry, properties.notifications().reconcileInterval(), gatewayMetrics);
    }

    @Bean
    RedisNotificationSubscriber redisNotificationSubscriber(RedisClient redisClient, GatewayFanout gatewayFanout,
            AppProperties properties, MeterRegistry meterRegistry) {
        RedisNotificationSubscriber subscriber =
                new RedisNotificationSubscriber(redisClient, properties.notifications().channel(), gatewayFanout);
        FunctionCounter.builder("tripupdates.notifications.malformed", subscriber,
                        RedisNotificationSubscriber::malformedCount)
                .register(meterRegistry);
        return subscriber;
    }

    @Bean
    NotificationSubscriberLifecycle notificationSubscriber(RedisNotificationSubscriber redisNotificationSubscriber) {
        return new NotificationSubscriberLifecycle(redisNotificationSubscriber);
    }
}

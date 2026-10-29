package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.auth.TopicAuthorizer;
import com.moveinsync.tripupdates.auth.TopicDirectory;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import com.moveinsync.tripupdates.ws.ProtocolCodec;
import com.moveinsync.tripupdates.ws.ProtocolService;
import com.moveinsync.tripupdates.ws.TripUpdatesWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration(proxyBeanMethods = false)
class ProtocolConfig {

    @Bean
    ProtocolCodec protocolCodec() {
        return new ProtocolCodec();
    }

    @Bean
    TopicAuthorizer topicAuthorizer(TopicDirectory topicDirectory) {
        return new TopicAuthorizer(topicDirectory);
    }

    @Bean
    ProtocolService protocolService(SubscriptionRegistry subscriptionRegistry, TopicAuthorizer topicAuthorizer,
            ProtocolCodec protocolCodec, AppProperties properties) {
        return new ProtocolService(subscriptionRegistry, topicAuthorizer, protocolCodec,
                properties.websocket().maxMessageChars(), properties.outbox().maxSubscriptionsPerConnection());
    }

    @Bean
    TripUpdatesWebSocketHandler tripUpdatesWebSocketHandler(SubscriptionRegistry subscriptionRegistry,
            ProtocolService protocolService, ProtocolCodec protocolCodec) {
        return new TripUpdatesWebSocketHandler(subscriptionRegistry, protocolService, protocolCodec);
    }

    /**
     * Container-level frame limit, twice the protocol limit: frames between the two get a MESSAGE_TOO_LARGE error,
     * larger frames are refused by the container (close code 1009) before reaching the application.
     */
    @Bean
    ServletServerContainerFactoryBean webSocketContainer(AppProperties properties) {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(properties.websocket().maxMessageChars() * 2);
        container.setMaxBinaryMessageBufferSize(1024);
        return container;
    }
}

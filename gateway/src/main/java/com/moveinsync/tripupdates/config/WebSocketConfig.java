package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.auth.ConnectionAuthenticator;
import com.moveinsync.tripupdates.ws.AuthHandshakeInterceptor;
import com.moveinsync.tripupdates.ws.TripUpdatesWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistration;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** Registers {@code /ws}: handshake authentication, then {@link TripUpdatesWebSocketHandler}. */
@Configuration(proxyBeanMethods = false)
@EnableWebSocket
class WebSocketConfig implements WebSocketConfigurer {

    private final AppProperties properties;
    private final TripUpdatesWebSocketHandler handler;
    private final ConnectionAuthenticator authenticator;

    WebSocketConfig(AppProperties properties, TripUpdatesWebSocketHandler handler,
            ConnectionAuthenticator authenticator) {
        this.properties = properties;
        this.handler = handler;
        this.authenticator = authenticator;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry handlers) {
        WebSocketHandlerRegistration registration = handlers.addHandler(handler, "/ws")
                .addInterceptors(new AuthHandshakeInterceptor(authenticator));
        if (!properties.websocket().allowedOriginPatterns().isEmpty()) {
            registration.setAllowedOriginPatterns(properties.websocket().allowedOriginPatterns().toArray(String[]::new));
        }
    }
}

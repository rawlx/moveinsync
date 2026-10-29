package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.auth.Identity;
import com.moveinsync.tripupdates.gateway.ClientSession;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.SubscriptionRegistry;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Thin adapter between Spring WebSocket sessions and the gateway: opens a {@link ClientSession} per connection,
 * passes text frames to {@link ProtocolService}, and closes the session (which unregisters every subscription and
 * releases its queue) when the connection ends.
 */
public final class TripUpdatesWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(TripUpdatesWebSocketHandler.class);

    private final SubscriptionRegistry registry;
    private final ProtocolService protocol;
    private final ProtocolCodec codec;
    private final ConcurrentHashMap<String, ProtocolService.Connection> connections = new ConcurrentHashMap<>();

    public TripUpdatesWebSocketHandler(SubscriptionRegistry registry, ProtocolService protocol, ProtocolCodec codec) {
        this.registry = registry;
        this.protocol = protocol;
        this.codec = codec;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession webSocket) throws Exception {
        if (!(webSocket.getAttributes().get(AuthHandshakeInterceptor.IDENTITY_ATTRIBUTE) instanceof Identity identity)) {
            webSocket.close(CloseStatus.POLICY_VIOLATION); // unreachable when the interceptor is installed
            return;
        }
        String key = webSocket.getId();
        ClientSession session = registry.open("ws-" + key.replaceAll("[^A-Za-z0-9_-]", ""), identity.tenantId(),
                new WebSocketClientTransport(webSocket, codec, () -> connections.remove(key)));
        connections.put(webSocket.getId(), new ProtocolService.Connection(identity, session));
        log.debug("Connection {} opened for {} ({})", session.id(), identity.subject(), identity.tenantId());
    }

    @Override
    protected void handlePongMessage(WebSocketSession webSocket, PongMessage message) {
        ProtocolService.Connection connection = connections.get(webSocket.getId());
        if (connection != null) {
            connection.touch();
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession webSocket, TextMessage message) {
        ProtocolService.Connection connection = connections.get(webSocket.getId());
        if (connection != null) {
            protocol.handle(connection, message.getPayload());
        }
    }

    @Override
    public void handleTransportError(WebSocketSession webSocket, Throwable exception) {
        close(webSocket, CloseReason.TRANSPORT_ERROR);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession webSocket, CloseStatus status) {
        close(webSocket, CloseReason.NORMAL);
    }

    /** Number of open connections (diagnostics, tests). */
    public int connectionCount() {
        return connections.size();
    }

    /** Snapshot of the open connections, for periodic maintenance (heartbeat, authorization refresh). */
    public List<ProtocolService.Connection> connections() {
        return List.copyOf(connections.values());
    }

    private void close(WebSocketSession webSocket, CloseReason reason) {
        ProtocolService.Connection connection = connections.remove(webSocket.getId());
        if (connection != null) {
            connection.session().close(reason);
        }
    }
}

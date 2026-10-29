package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.auth.ConnectionAuthenticator;
import com.moveinsync.tripupdates.auth.Identity;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Authenticates the WebSocket handshake. The bearer credential is read from {@code Authorization: Bearer …} or, for
 * browsers (which cannot set headers on a WebSocket), from the {@code access_token} query parameter. Without a valid
 * credential the upgrade is refused with 401 and no session exists. The credential is never logged.
 */
public final class AuthHandshakeInterceptor implements HandshakeInterceptor {

    public static final String IDENTITY_ATTRIBUTE = "tripupdates.identity";
    private static final Logger log = LoggerFactory.getLogger(AuthHandshakeInterceptor.class);

    private final ConnectionAuthenticator authenticator;

    public AuthHandshakeInterceptor(ConnectionAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
            Map<String, Object> attributes) {
        Optional<Identity> identity = authenticator.authenticate(credential(request));
        if (identity.isEmpty()) {
            log.info("Rejected WebSocket handshake from {}: missing or invalid credential", request.getRemoteAddress());
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
        attributes.put(IDENTITY_ATTRIBUTE, identity.get());
        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
            Exception exception) {
    }

    private static String credential(ServerHttpRequest request) {
        List<String> authorization = request.getHeaders().get("Authorization");
        if (authorization != null && !authorization.isEmpty()) {
            String value = authorization.get(0);
            return value.regionMatches(true, 0, "Bearer ", 0, 7) ? value.substring(7).trim() : null;
        }
        return UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams().getFirst("access_token");
    }
}

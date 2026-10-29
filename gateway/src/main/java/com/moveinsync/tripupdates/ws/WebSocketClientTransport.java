package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.gateway.ClientTransport;
import com.moveinsync.tripupdates.gateway.CloseReason;
import com.moveinsync.tripupdates.gateway.OutboundFrame;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * {@link ClientTransport} over a Spring {@link WebSocketSession}. {@link #send} is called only by the connection's
 * writer thread (WebSocket sessions do not allow concurrent sends) and blocks while the client's network is slow.
 */
final class WebSocketClientTransport implements ClientTransport {

    private static final Logger log = LoggerFactory.getLogger(WebSocketClientTransport.class);

    private static final byte[] PING_PAYLOAD = {'h', 'b'};

    private final WebSocketSession session;
    private final ProtocolCodec codec;
    private final Runnable onClose;

    /**
     * @param onClose run first when the gateway closes the connection, before the WebSocket close itself, which can
     *                block while the writer is stuck on a frozen client
     */
    WebSocketClientTransport(WebSocketSession session, ProtocolCodec codec, Runnable onClose) {
        this.session = session;
        this.codec = codec;
        this.onClose = onClose;
    }

    @Override
    public void send(OutboundFrame frame) throws IOException {
        if (frame instanceof OutboundFrame.ControlFrame(Heartbeat heartbeat)) {
            session.sendMessage(new PingMessage(ByteBuffer.wrap(PING_PAYLOAD)));
            return;
        }
        session.sendMessage(new TextMessage(codec.render(frame)));
    }

    @Override
    public void close(CloseReason reason) {
        onClose.run();
        if (!session.isOpen()) {
            return;
        }
        try {
            session.close(new CloseStatus(reason.code(), reason.name()));
        } catch (IOException | RuntimeException e) {
            log.debug("Closing WebSocket session {} failed", session.getId(), e);
        }
    }
}

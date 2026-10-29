package com.moveinsync.tripupdates.gateway;

/**
 * The client-facing side of a connection (a WebSocket session in production, a recorder in tests).
 * {@link #send} may block on a slow network; it is only ever called from the connection's own writer thread.
 */
public interface ClientTransport {

    void send(OutboundFrame frame) throws Exception;

    /** Closes the connection. Must not block indefinitely; it is used to unblock a writer stuck in {@link #send}. */
    void close(CloseReason reason);
}

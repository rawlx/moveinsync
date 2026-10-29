package com.moveinsync.tripupdates.gateway;

/** Why a client connection is closed; the code is the WebSocket close code sent to the client. */
public enum CloseReason {
    NORMAL(1000),
    SHUTDOWN(1001),
    TRANSPORT_ERROR(1011),
    /** The client's reliable queue overflowed. It must reconnect and resume from its last processed sequence. */
    SLOW_CONSUMER(4008),
    /** The gateway could not recover the topic history it needed. The client must reconnect and resume. */
    RESYNC_REQUIRED(4009),
    /** No traffic (not even a pong to the gateway's heartbeat ping) within the idle timeout: presumed dead. */
    IDLE_TIMEOUT(4010);

    private final int code;

    CloseReason(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}

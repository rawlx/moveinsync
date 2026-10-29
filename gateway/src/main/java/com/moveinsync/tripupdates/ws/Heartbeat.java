package com.moveinsync.tripupdates.ws;

import com.moveinsync.tripupdates.gateway.OutboundFrame;

/**
 * Gateway-initiated heartbeat, sent as a WebSocket ping control frame (not a JSON message). Browsers and standard
 * clients answer pings with pongs automatically; a client that stops reading, or whose connection is half-open,
 * stops answering and is closed after the idle timeout. Queued in the reliable lane like every control message, so
 * it is never sent concurrently with another frame.
 */
enum Heartbeat implements OutboundFrame.ControlMessage {
    PING
}

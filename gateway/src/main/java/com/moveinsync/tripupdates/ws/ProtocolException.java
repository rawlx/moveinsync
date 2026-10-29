package com.moveinsync.tripupdates.ws;

/** A client message that cannot be accepted. The message text is safe to send to the client. */
public final class ProtocolException extends RuntimeException {

    private final ErrorCode code;
    private final String messageId;

    public ProtocolException(ErrorCode code, String message, String messageId) {
        super(message);
        this.code = code;
        this.messageId = messageId;
    }

    public ErrorCode code() {
        return code;
    }

    /** The offending message's id, if it could be read. */
    public String messageId() {
        return messageId;
    }
}

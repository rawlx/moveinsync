package com.moveinsync.tripupdates.ws;

/** Codes of {@code ERROR} frames. Messages accompanying them never contain internal details or other tenants' data. */
public enum ErrorCode {
    /** Not JSON, missing/invalid/unknown field. */
    BAD_REQUEST,
    /** Unknown {@code type}. */
    UNSUPPORTED_TYPE,
    /** Frame longer than the configured maximum. */
    MESSAGE_TOO_LARGE,
    /** Topic not permitted for this identity. Also returned for topics that do not exist. */
    FORBIDDEN,
    ALREADY_SUBSCRIBED,
    NOT_SUBSCRIBED,
    TOO_MANY_SUBSCRIPTIONS,
    /** ACK for a sequence that was never delivered on that subscription. */
    INVALID_ACK,
    /** RESUME position not available in the retained history; the client must recover (no snapshot yet). */
    RECOVERY_REQUIRED,
    /** Event store temporarily unavailable; retry later. */
    UNAVAILABLE,
    INTERNAL
}

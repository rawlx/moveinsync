package com.moveinsync.tripupdates.domain;

/**
 * Thrown when a domain value is invalid. The message names the field and the rule only; it never echoes the
 * rejected value, so it is safe to log or return to clients (no payload or personal data).
 */
public final class DomainValidationException extends RuntimeException {

    private final String field;
    private final String reason;

    public DomainValidationException(String field, String reason) {
        super(field + ": " + reason);
        this.field = field;
        this.reason = reason;
    }

    public String field() {
        return field;
    }

    public String reason() {
        return reason;
    }
}

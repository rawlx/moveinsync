package com.moveinsync.tripupdates.domain;

import java.util.regex.Pattern;

/**
 * Validation for opaque identifiers (tenant, trip, cab, office, event, SOS ids).
 * Restricting ids to {@code [A-Za-z0-9_-]{1,64}} keeps them safe inside topic names, Redis keys and JSON.
 */
public final class Identifiers {

    public static final int MAX_LENGTH = 64;
    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9_-]{1," + MAX_LENGTH + "}");

    private Identifiers() {
    }

    public static boolean isValid(String value) {
        return value != null && VALID.matcher(value).matches();
    }

    public static String require(String value, String field) {
        if (value == null) {
            throw new DomainValidationException(field, "is required");
        }
        if (!isValid(value)) {
            throw new DomainValidationException(field, "must match [A-Za-z0-9_-]{1," + MAX_LENGTH + "}");
        }
        return value;
    }
}

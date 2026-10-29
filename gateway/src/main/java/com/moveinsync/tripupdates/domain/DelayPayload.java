package com.moveinsync.tripupdates.domain;

import java.util.regex.Pattern;

/** Expected delay of a trip, with a machine-readable reason code (e.g. {@code TRAFFIC}); no free text. */
public record DelayPayload(int delayMinutes, String reasonCode) implements EventPayload {

    static final int MAX_DELAY_MINUTES = 24 * 60;
    private static final Pattern REASON_CODE = Pattern.compile("[A-Z][A-Z_]{0,31}");

    public DelayPayload {
        if (delayMinutes < 0 || delayMinutes > MAX_DELAY_MINUTES) {
            throw new DomainValidationException("data.delayMinutes", "must be between 0 and " + MAX_DELAY_MINUTES);
        }
        if (reasonCode == null) {
            throw new DomainValidationException("data.reasonCode", "is required");
        }
        if (!REASON_CODE.matcher(reasonCode).matches()) {
            throw new DomainValidationException("data.reasonCode", "must match [A-Z][A-Z_]{0,31}");
        }
    }

    @Override
    public EventType type() {
        return EventType.DELAY;
    }
}

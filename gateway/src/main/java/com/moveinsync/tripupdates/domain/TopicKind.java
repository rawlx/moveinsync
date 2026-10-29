package com.moveinsync.tripupdates.domain;

import java.util.Optional;

/**
 * Topic families. TRIP, CAB and OFFICE are from the case study; SOS ({@code sos:{officeId}}) carries only the SOS
 * alerts of one office, so security users can be authorised for alerts without seeing all office traffic.
 * The prefix is the client-visible, lower-case name part.
 */
public enum TopicKind {
    TRIP("trip"),
    CAB("cab"),
    OFFICE("office"),
    SOS("sos");

    private final String prefix;

    TopicKind(String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }

    public static Optional<TopicKind> fromPrefix(String prefix) {
        for (TopicKind kind : values()) {
            if (kind.prefix.equals(prefix)) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }
}

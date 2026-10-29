package com.moveinsync.tripupdates.domain;

import java.util.Optional;

/** The three topic families from the case study. The prefix is the client-visible, lower-case name part. */
public enum TopicKind {
    TRIP("trip"),
    CAB("cab"),
    OFFICE("office");

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

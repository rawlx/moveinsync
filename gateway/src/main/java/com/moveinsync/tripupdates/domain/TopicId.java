package com.moveinsync.tripupdates.domain;

/**
 * A client-visible topic name: {@code {kind}:{id}}, e.g. {@code trip:T-A1}. It never contains the tenant;
 * see {@link TenantTopic} for the tenant-scoped key.
 */
public record TopicId(TopicKind kind, String id) {

    private static final char SEPARATOR = ':';

    public TopicId {
        if (kind == null) {
            throw new DomainValidationException("topic.kind", "is required");
        }
        Identifiers.require(id, "topic.id");
    }

    public static TopicId trip(String tripId) {
        return new TopicId(TopicKind.TRIP, tripId);
    }

    public static TopicId cab(String cabId) {
        return new TopicId(TopicKind.CAB, cabId);
    }

    public static TopicId office(String officeId) {
        return new TopicId(TopicKind.OFFICE, officeId);
    }

    /** Parses {@code kind:id}; the kind must be one of the lower-case prefixes and the id a valid identifier. */
    public static TopicId parse(String name) {
        if (name == null) {
            throw new DomainValidationException("topic", "is required");
        }
        int separator = name.indexOf(SEPARATOR);
        if (separator < 0) {
            throw new DomainValidationException("topic", "must have the form {kind}:{id}");
        }
        TopicKind kind = TopicKind.fromPrefix(name.substring(0, separator))
                .orElseThrow(() -> new DomainValidationException("topic.kind", "must be one of trip, cab, office"));
        return new TopicId(kind, name.substring(separator + 1));
    }

    /** The canonical wire name, e.g. {@code trip:T-A1}. */
    public String name() {
        return kind.prefix() + SEPARATOR + id;
    }

    @Override
    public String toString() {
        return name();
    }
}

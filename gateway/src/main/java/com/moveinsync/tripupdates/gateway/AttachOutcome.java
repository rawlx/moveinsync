package com.moveinsync.tripupdates.gateway;

/** Result of attaching a subscription to a topic (see {@link SubscriptionRegistry#attach}). */
public sealed interface AttachOutcome {

    /**
     * The subscription is live. Ordered events with {@code seq > fromSeq} and {@code seq <= toSeq} were replayed
     * ({@code replayed} of them) and are queued before the confirmation; everything after {@code toSeq} is
     * delivered live, in order, after the confirmation.
     */
    record Attached(long fromSeq, long toSeq, int replayed) implements AttachOutcome {
    }

    /**
     * The requested position cannot be served from the retained history: it is older than the retained window
     * ({@code requested < floorSeq}) or unknown to the store ({@code requested > headSeq}, e.g. after data loss).
     * Nothing was attached. Snapshot fallback would plug in here; it is not implemented, so the client must be told
     * explicitly that recovery is required.
     */
    record HistoryUnavailable(long requestedSeq, long floorSeq, long headSeq) implements AttachOutcome {
    }

    /** The session already subscribes to the topic; nothing changed. */
    record AlreadySubscribed() implements AttachOutcome {
    }

    /** The session reached its subscription limit; nothing changed. */
    record LimitReached(int limit) implements AttachOutcome {
    }
}

package com.moveinsync.tripupdates.loadtest;

/**
 * Compares the reliable events a client accepted with what was published on its topic after it subscribed.
 * Delivery is at-least-once, so duplicates are expected and dropped by the client before this check; what must not
 * happen is a published event that never arrived (missing) or an event nobody published (unexpected).
 */
final class SequenceVerifier {

    record Result(int expected, int received, int missing, int unexpected) {

        boolean complete() {
            return missing == 0 && unexpected == 0;
        }
    }

    private SequenceVerifier() {
    }

    /**
     * @param startSeq  the subscription's start position: events with {@code seq <= startSeq} are not expected
     * @param published ascending published sequences of the topic
     * @param received  ascending, distinct sequences the client accepted (it only accepts {@code seq > last})
     */
    static Result verify(long startSeq, long[] published, long[] received) {
        int expected = 0;
        int missing = 0;
        int matched = 0;
        int r = 0;
        for (long seq : published) {
            if (seq <= startSeq) {
                continue;
            }
            expected++;
            while (r < received.length && received[r] < seq) {
                r++;
            }
            if (r < received.length && received[r] == seq) {
                matched++;
                r++;
            } else {
                missing++;
            }
        }
        return new Result(expected, received.length, missing, received.length - matched);
    }
}

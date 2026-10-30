package com.moveinsync.tripupdates.loadtest;

/**
 * Records durations (nanoseconds) and reports exact nearest-rank percentiles over all samples. Exact rather than
 * histogram-approximated because sample counts are modest (thousands); thread-safe.
 */
final class LatencyRecorder {

    /** Percentiles in milliseconds; all zero when there are no samples. */
    record Summary(int count, double minMs, double p50Ms, double p95Ms, double p99Ms, double maxMs, double meanMs,
            int overThreshold, double thresholdMs) {
    }

    private final LongList samples = new LongList();
    private final long thresholdNanos;

    /** @param thresholdMillis samples above this are counted separately (e.g. the 2 s SOS target) */
    LatencyRecorder(long thresholdMillis) {
        this.thresholdNanos = thresholdMillis * 1_000_000;
    }

    synchronized void record(long nanos) {
        samples.add(Math.max(0, nanos));
    }

    synchronized Summary summary() {
        long[] sorted = samples.toSortedArray();
        if (sorted.length == 0) {
            return new Summary(0, 0, 0, 0, 0, 0, 0, 0, thresholdNanos / 1e6);
        }
        long sum = 0;
        int over = 0;
        for (long sample : sorted) {
            sum += sample;
            if (sample > thresholdNanos) {
                over++;
            }
        }
        return new Summary(sorted.length, ms(sorted[0]), ms(percentile(sorted, 50)), ms(percentile(sorted, 95)),
                ms(percentile(sorted, 99)), ms(sorted[sorted.length - 1]), ms(sum / (double) sorted.length), over,
                thresholdNanos / 1e6);
    }

    /** Nearest-rank percentile of an ascending array: the smallest value with at least p% of samples ≤ it. */
    static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            throw new IllegalArgumentException("no samples");
        }
        if (p <= 0) {
            return sorted[0];
        }
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.min(sorted.length, Math.max(1, rank)) - 1];
    }

    private static double ms(double nanos) {
        return Math.round(nanos / 1e3) / 1e3;
    }
}

package com.moveinsync.tripupdates.loadtest;

import java.util.Arrays;

/** Growable array of primitive longs (avoids boxing for large sample and sequence sets). Not thread-safe. */
final class LongList {

    private long[] values = new long[16];
    private int size;

    void add(long value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, size * 2);
        }
        values[size++] = value;
    }

    int size() {
        return size;
    }

    long[] toArray() {
        return Arrays.copyOf(values, size);
    }

    long[] toSortedArray() {
        long[] copy = toArray();
        Arrays.sort(copy);
        return copy;
    }
}

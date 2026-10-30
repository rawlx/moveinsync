package com.moveinsync.tripupdates.loadtest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The load generator's view of which gateways may receive (re)connections. A gateway is marked down when a client
 * loses its connection to it abnormally or cannot connect; it becomes eligible again only after
 * {@link #REQUIRED_SUCCESSES} consecutive successful readiness probes, so clients are not sent back to a gateway that
 * is still starting or flapping. Every transition is recorded with its time. Thread-safe.
 */
final class GatewayHealth {

    static final int REQUIRED_SUCCESSES = 2;

    /** A state change, with seconds since the start of the test. */
    record Transition(int gateway, String state, double atSeconds, String reason) {
    }

    private final boolean[] up;
    private final int[] consecutiveSuccesses;
    private final List<Transition> transitions = new ArrayList<>();
    private final LongSupplier elapsedNanos;

    GatewayHealth(int gateways, LongSupplier elapsedNanos) {
        this.up = new boolean[gateways];
        this.consecutiveSuccesses = new int[gateways];
        this.elapsedNanos = elapsedNanos;
        Arrays.fill(up, true);
    }

    synchronized void markDown(int gateway, String reason) {
        consecutiveSuccesses[gateway] = 0;
        if (up[gateway]) {
            up[gateway] = false;
            transitions.add(new Transition(gateway, "DOWN", seconds(), reason));
        }
    }

    /** Result of a readiness probe of a gateway that is down; ignored while it is up. */
    synchronized void probeResult(int gateway, boolean ready) {
        if (up[gateway]) {
            return;
        }
        if (!ready) {
            consecutiveSuccesses[gateway] = 0;
            return;
        }
        if (++consecutiveSuccesses[gateway] >= REQUIRED_SUCCESSES) {
            up[gateway] = true;
            transitions.add(new Transition(gateway, "UP", seconds(),
                    REQUIRED_SUCCESSES + " consecutive readiness probes"));
        }
    }

    synchronized boolean isUp(int gateway) {
        return up[gateway];
    }

    synchronized Set<Integer> healthy() {
        Set<Integer> healthy = new LinkedHashSet<>();
        for (int g = 0; g < up.length; g++) {
            if (up[g]) {
                healthy.add(g);
            }
        }
        return healthy;
    }

    synchronized List<Transition> transitions() {
        return List.copyOf(transitions);
    }

    private double seconds() {
        return Math.round(elapsedNanos.getAsLong() / 1e7) / 100.0;
    }
}

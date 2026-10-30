package com.moveinsync.tripupdates.loadtest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Chooses the gateway a client reconnects to: deterministically by client id, among the gateways currently considered
 * healthy, never the one it just lost. Clients displaced from one gateway therefore spread evenly over the survivors
 * (ids of one gateway are congruent mod the gateway count, and are re-spread mod the survivor count) instead of all
 * moving to the "next" gateway.
 */
final class GatewaySelector {

    private GatewaySelector() {
    }

    /**
     * @param lost    the gateway the client was connected to (or failed to reach)
     * @param healthy gateways currently eligible for (re)connection
     * @return the gateway to try next. If no other gateway is healthy, another gateway is still tried (the health
     *         view may be stale; the connect attempt itself is the check), and only with a single gateway is
     *         {@code lost} returned.
     */
    static int pick(int clientId, int lost, Set<Integer> healthy, int gateways) {
        List<Integer> candidates = new ArrayList<>();
        for (int g = 0; g < gateways; g++) {
            if (g != lost && healthy.contains(g)) {
                candidates.add(g);
            }
        }
        if (candidates.isEmpty()) {
            for (int g = 0; g < gateways; g++) {
                if (g != lost) {
                    candidates.add(g);
                }
            }
        }
        if (candidates.isEmpty()) {
            return lost;
        }
        return candidates.get(Math.floorMod(clientId, candidates.size()));
    }
}

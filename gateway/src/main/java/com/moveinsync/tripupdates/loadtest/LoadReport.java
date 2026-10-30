package com.moveinsync.tripupdates.loadtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Everything a load-test run measured, plus pass/fail evaluation against the case-study targets. */
public record LoadReport(
        LoadTestConfig config,
        String startedAtUtc,
        int cabs,
        Map<ClientPlan.Kind, KindResult> clients,
        Map<String, Long> publishedByType,
        long publishFailures,
        long notificationFailures,
        long lateTicks,
        long deliveredFrames,
        long connectFailures,
        int clientsFailedToConnect,
        Map<String, Long> errors,
        Map<String, Long> probeAnswers,
        Map<String, Long> disconnectReasons,
        SosTracker.Summary sos,
        LatencyRecorder.Summary reconnectOutage,
        Double firstDisconnectAtSeconds,
        Double recoveryWindowSeconds,
        List<GatewayHealth.Transition> gatewayTransitions,
        FailbackResult failback,
        Map<Integer, Map<String, Long>> perGateway,
        Map<String, Object> phases,
        List<Map<String, Object>> timeline) {

    /** Per client kind: delivery and sequence verification totals. Mutable while the report is assembled. */
    public static final class KindResult {
        public int planned;
        public int subscribed;
        public int neverSubscribed;
        public int incompleteClients;
        public long expectedReliable;
        public long receivedReliable;
        public long missingReliable;
        public long unexpectedReliable;
        public long duplicates;
        public long locations;
        public long reconnects;
        public long tenantViolations;
    }

    public record Check(String target, String measured, boolean met) {
    }

    /**
     * Clients connected to a restored gateway after its failure. Times are seconds since the test start;
     * {@code allSubscribedAtSeconds} is null if not every late client subscribed before the timeout.
     */
    public record FailbackResult(int gateway, int requested, int launched, int subscribed, int onRestoredGateway,
            long reconnected, Double eligibleAtSeconds, Double launchedAtSeconds, Double allSubscribedAtSeconds,
            long expectedReliable, long receivedReliable, long missingReliable, long unexpectedReliable,
            long duplicates, long locations, long tenantViolations) {
    }

    /** Non-probe clients confirmed subscribed at the CONNECTED phase. */
    public int establishedAtConnected() {
        Object connected = phases.get("CONNECTED");
        return connected instanceof Map<?, ?> map ? (Integer) map.get("subscribedClients") : 0;
    }

    public long totalPublished() {
        return publishedByType.values().stream().mapToLong(Long::longValue).sum();
    }

    public List<Check> checks() {
        List<Check> checks = new ArrayList<>();
        int established = establishedAtConnected();
        checks.add(new Check(">= 10,000 established, subscribed WebSocket clients", established + " confirmed SUBSCRIBED",
                established >= 10_000));
        checks.add(new Check(">= 2,000 publishing cabs", cabs + " cabs publishing", cabs >= 2_000));
        long missing = clients.values().stream().mapToLong(k -> k.missingReliable).sum();
        long unexpected = clients.values().stream().mapToLong(k -> k.unexpectedReliable).sum();
        checks.add(new Check("no reliable (STATUS/DELAY/SOS) event lost per subscription",
                missing + " missing, " + unexpected + " unexpected", missing == 0 && unexpected == 0));
        checks.add(new Check("every SOS delivered to every subscribed security client",
                sos.missing() + " of " + sos.expectedDeliveries() + " missing", sos.missing() == 0
                        && sos.expectedDeliveries() > 0));
        checks.add(new Check("SOS p99 < 2 s (live deliveries)", sos.live().p99Ms() + " ms over "
                + sos.live().count() + " samples", sos.live().count() > 0 && sos.live().p99Ms() < 2_000));
        long violations = clients.values().stream().mapToLong(k -> k.tenantViolations).sum()
                + (failback == null ? 0 : failback.tenantViolations());
        checks.add(new Check("no cross-tenant event delivered", violations + " violations", violations == 0));
        long forbidden = probeAnswers.getOrDefault("FORBIDDEN", 0L);
        int probes = clients.get(ClientPlan.Kind.PROBE).planned;
        checks.add(new Check("unauthorised subscriptions refused", forbidden + " of " + probes + " FORBIDDEN",
                forbidden == probes));
        if (failback != null) {
            checks.add(new Check("restored gateway accepts new subscriptions",
                    failback.subscribed() + " of " + failback.requested() + " late clients subscribed, "
                            + failback.onRestoredGateway() + " on gateway " + failback.gateway(),
                    failback.requested() > 0 && failback.subscribed() == failback.requested()
                            && failback.onRestoredGateway() == failback.requested()));
            checks.add(new Check("late clients: no reliable event lost",
                    failback.missingReliable() + " missing, " + failback.unexpectedReliable() + " unexpected of "
                            + failback.expectedReliable() + " expected",
                    failback.expectedReliable() > 0 && failback.missingReliable() == 0
                            && failback.unexpectedReliable() == 0));
        }
        return checks;
    }

    public boolean passed() {
        return checks().stream().allMatch(Check::met);
    }

    /** Writes {@code result.json} and {@code summary.txt} into {@code dir}. */
    public void write(Path dir) throws IOException {
        Files.createDirectories(dir);
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT);
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("checks", checks());
        json.put("passed", passed());
        json.put("report", this);
        mapper.writeValue(dir.resolve("result.json").toFile(), json);
        Files.writeString(dir.resolve("summary.txt"), summary());
    }

    public String summary() {
        StringBuilder out = new StringBuilder();
        out.append("Load test ").append(config.runId()).append('\n');
        out.append(String.format("fleet: %d cabs (%d tenants x %d offices x %d cabs), clients planned %d, gateways %s%n",
                cabs, config.tenants(), config.officesPerTenant(), config.cabsPerOffice(), config.clients(),
                config.gateways()));
        out.append(String.format("duration %ss (+%ss warmup, +%ss drain), location every %d ms, SOS %.2f/s, "
                        + "%d publisher threads%n", config.duration().toSeconds(), config.warmup().toSeconds(),
                config.drain().toSeconds(), config.locationInterval().toMillis(), config.sosPerSecond(),
                config.publisherThreads()));
        out.append(String.format("established (SUBSCRIBED) at CONNECTED: %d; failed to connect: %d; connect errors: %d%n",
                establishedAtConnected(), clientsFailedToConnect, connectFailures));
        out.append(String.format("published: %d total %s; publish failures %d; notification failures %d; late ticks %d%n",
                totalPublished(), publishedByType, publishFailures, notificationFailures, lateTicks));
        out.append(String.format("delivered frames (events received by clients): %d%n", deliveredFrames));
        clients.forEach((kind, r) -> out.append(String.format(
                "  %-14s planned %5d subscribed %5d never %4d | reliable expected %7d received %7d missing %d "
                        + "unexpected %d dup %d | locations %d | reconnects %d | incomplete clients %d | "
                        + "tenant violations %d%n", kind, r.planned, r.subscribed, r.neverSubscribed, r.expectedReliable,
                r.receivedReliable, r.missingReliable, r.unexpectedReliable, r.duplicates, r.locations, r.reconnects,
                r.incompleteClients, r.tenantViolations)));
        out.append(String.format("SOS: published %d (failures %d), deliveries expected %d delivered %d missing %d "
                        + "duplicates %d%n", sos.published(), sos.publishFailures(), sos.expectedDeliveries(),
                sos.delivered(), sos.missing(), sos.duplicateDeliveries()));
        out.append("  all       ").append(format(sos.all())).append('\n');
        out.append("  live      ").append(format(sos.live())).append('\n');
        out.append("  recovered ").append(format(sos.recovered())).append('\n');
        out.append(String.format("  deliveries over %.0f ms: %d (individually listed; times in s since start %s)%n",
                sos.all().thresholdMs(), sos.slowDeliveries().size(), startedAtUtc));
        for (SosTracker.SlowDelivery slow : sos.slowDeliveries()) {
            out.append(String.format("    %s client %d gw %d published t=%.3fs (%s) received t=%.3fs latency %.1f ms "
                            + "%s, %s%n", slow.eventId(), slow.clientId(), slow.gateway(), slow.publishedAtSeconds(),
                    slow.publishedAtUtc(), slow.receivedAtSeconds(), slow.latencyMs(),
                    slow.recovering() ? "during the client's recovery" : "live", relativeToFailure(slow)));
        }
        out.append("reconnect outage (disconnect -> RESUME_COMPLETE): ").append(format(reconnectOutage)).append('\n');
        out.append(String.format("first disconnect at %ss; recovery window %ss; disconnect reasons %s%n",
                firstDisconnectAtSeconds, recoveryWindowSeconds, disconnectReasons));
        out.append("gateway health transitions (load generator's view): ").append(gatewayTransitions).append('\n');
        out.append("per gateway (index): ").append(perGateway).append('\n');
        sos.byGateway().forEach((gateway, s) -> out.append("  SOS received via gateway ").append(gateway)
                .append(": ").append(format(s)).append('\n'));
        if (failback != null) {
            out.append("failback: ").append(failback).append('\n');
        }
        out.append("probe answers: ").append(probeAnswers).append("; errors: ").append(errors).append('\n');
        out.append("checks:\n");
        for (Check check : checks()) {
            out.append(String.format("  [%s] %s -> %s%n", check.met() ? "MET" : "MISSED", check.target(),
                    check.measured()));
        }
        out.append("overall: ").append(passed() ? "PASSED" : "NOT PASSED").append('\n');
        return out.toString();
    }

    /** Position of a slow delivery relative to the observed failure window (measured, not a cause). */
    private String relativeToFailure(SosTracker.SlowDelivery slow) {
        if (firstDisconnectAtSeconds == null) {
            return "no gateway failure observed";
        }
        double end = firstDisconnectAtSeconds + (recoveryWindowSeconds == null ? 0 : recoveryWindowSeconds);
        String where = slow.publishedAtSeconds() < firstDisconnectAtSeconds
                ? (slow.receivedAtSeconds() >= firstDisconnectAtSeconds ? "published before, received during/after"
                        : "entirely before")
                : slow.publishedAtSeconds() <= end ? "published within" : "published after";
        return String.format("%s the failure window [%.3f s, %.3f s] (published %+.3f s from first disconnect)",
                where, firstDisconnectAtSeconds, end, slow.publishedAtSeconds() - firstDisconnectAtSeconds);
    }

    private static String format(LatencyRecorder.Summary s) {
        return String.format("n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms mean=%.1fms >%.0fms: %d", s.count(),
                s.p50Ms(), s.p95Ms(), s.p99Ms(), s.maxMs(), s.meanMs(), s.thresholdMs(), s.overThreshold());
    }
}

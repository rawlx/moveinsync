package com.moveinsync.tripupdates.loadtest;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Load-test parameters, read from environment variables ({@code LOADTEST_*}) with defaults sized for the case-study
 * targets. The fleet dimensions must match the gateways' {@code app.auth.synthetic-fleet} settings.
 */
public record LoadTestConfig(
        String runId,
        String resultsDir,
        List<String> gateways,
        String redisUri,
        int tenants,
        int officesPerTenant,
        int cabsPerOffice,
        int passengersPerTrip,
        int clients,
        int securityPerOffice,
        int adminsPerOffice,
        int cabWatchersPerOffice,
        int probes,
        int connectsPerSecond,
        int maxConcurrentConnects,
        int httpClients,
        Duration connectTimeout,
        Duration warmup,
        Duration duration,
        Duration drain,
        Duration locationInterval,
        int statusEveryTicks,
        double delayProbability,
        double sosPerSecond,
        int publisherThreads,
        Duration sampleInterval,
        long seed,
        int lateClients,
        int lateGateway,
        Duration lateTimeout) {

    public static LoadTestConfig fromEnvironment() {
        return from(System.getenv());
    }

    static LoadTestConfig from(Map<String, String> env) {
        Function<String, String> get = name -> env.get("LOADTEST_" + name);
        return new LoadTestConfig(
                string(get, "RUN_ID", "run-" + Instant.now().toString().replaceAll("[^0-9]", "").substring(0, 14)),
                string(get, "RESULTS_DIR", "/results"),
                Arrays.stream(string(get, "GATEWAYS",
                        "ws://gateway-1:8080/ws,ws://gateway-2:8080/ws,ws://gateway-3:8080/ws").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList(),
                string(get, "REDIS_URI", "redis://redis:6379"),
                integer(get, "TENANTS", 2),
                integer(get, "OFFICES_PER_TENANT", 10),
                integer(get, "CABS_PER_OFFICE", 100),
                integer(get, "PASSENGERS_PER_TRIP", 4),
                integer(get, "CLIENTS", 10_000),
                integer(get, "SECURITY_PER_OFFICE", 2),
                integer(get, "ADMINS_PER_OFFICE", 10),
                integer(get, "CAB_WATCHERS_PER_OFFICE", 10),
                integer(get, "PROBES", 20),
                integer(get, "CONNECTS_PER_SECOND", 400),
                integer(get, "MAX_CONCURRENT_CONNECTS", 200),
                integer(get, "HTTP_CLIENTS", 8),
                Duration.ofSeconds(integer(get, "CONNECT_TIMEOUT_S", 180)),
                Duration.ofSeconds(integer(get, "WARMUP_S", 15)),
                Duration.ofSeconds(integer(get, "DURATION_S", 180)),
                Duration.ofSeconds(integer(get, "DRAIN_S", 20)),
                Duration.ofMillis(integer(get, "LOCATION_INTERVAL_MS", 5000)),
                integer(get, "STATUS_EVERY_TICKS", 6),
                decimal(get, "DELAY_PROBABILITY", 0.02),
                decimal(get, "SOS_PER_SECOND", 2.0),
                integer(get, "PUBLISHER_THREADS", 8),
                Duration.ofSeconds(integer(get, "SAMPLE_INTERVAL_S", 5)),
                Long.parseLong(string(get, "SEED", "20261030")),
                integer(get, "LATE_CLIENTS", 0),
                integer(get, "LATE_GATEWAY", 1),
                Duration.ofSeconds(integer(get, "LATE_TIMEOUT_S", 180)));
    }

    public LoadTestConfig {
        gateways = List.copyOf(gateways);
        if (gateways.isEmpty() || clients < 1 || publisherThreads < 1 || httpClients < 1 || sosPerSecond < 0
                || lateClients < 0 || (lateClients > 0 && (lateGateway < 0 || lateGateway >= gateways.size()))) {
            throw new IllegalArgumentException("invalid load-test configuration");
        }
    }

    public int cabs() {
        return tenants * officesPerTenant * cabsPerOffice;
    }

    private static String string(Function<String, String> get, String name, String fallback) {
        String value = get.apply(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int integer(Function<String, String> get, String name, int fallback) {
        return Integer.parseInt(string(get, name, Integer.toString(fallback)));
    }

    private static double decimal(Function<String, String> get, String name, double fallback) {
        return Double.parseDouble(string(get, name, Double.toString(fallback)));
    }
}

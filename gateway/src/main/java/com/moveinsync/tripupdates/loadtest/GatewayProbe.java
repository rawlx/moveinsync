package com.moveinsync.tripupdates.loadtest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads each gateway's {@code /actuator/prometheus} and extracts the values the report needs (sessions, heap, CPU,
 * queue depths, failures). A gateway that does not answer is reported as {@code up=0}.
 */
final class GatewayProbe {

    private final List<String> metricsUris;
    private final List<String> readinessUris;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    GatewayProbe(List<String> webSocketUris) {
        this.metricsUris = webSocketUris.stream()
                .map(uri -> uri.replaceFirst("^ws", "http").replaceFirst("/ws$", "/actuator/prometheus"))
                .toList();
        this.readinessUris = webSocketUris.stream()
                .map(uri -> uri.replaceFirst("^ws", "http").replaceFirst("/ws$", "/actuator/health/readiness"))
                .toList();
    }

    /** True if the gateway answers its readiness endpoint with UP (Redis reachable, application ready). */
    boolean ready(int gateway) {
        try {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(readinessUris.get(gateway)))
                    .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 && response.body().contains("\"status\":\"UP\"");
        } catch (Exception e) {
            return false;
        }
    }

    /** Per gateway (by index) a map of metric → value. */
    List<Map<String, Double>> snapshot() {
        return metricsUris.stream().map(this::scrape).toList();
    }

    private Map<String, Double> scrape(String uri) {
        Map<String, Double> values = new LinkedHashMap<>();
        String body;
        try {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(uri))
                    .timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                values.put("up", 0.0);
                return values;
            }
            body = response.body();
        } catch (Exception e) {
            values.put("up", 0.0);
            return values;
        }
        values.put("up", 1.0);
        values.put("sessions_active", sum(body, "tripupdates_sessions_active", null));
        values.put("feeds_active", sum(body, "tripupdates_feeds_active", null));
        values.put("heap_used_bytes", sum(body, "jvm_memory_used_bytes", "area=\"heap\""));
        values.put("nonheap_used_bytes", sum(body, "jvm_memory_used_bytes", "area=\"nonheap\""));
        values.put("process_cpu_usage", sum(body, "process_cpu_usage", null));
        values.put("live_threads", sum(body, "jvm_threads_live_threads", null));
        values.put("outbox_reliable_depth", sum(body, "tripupdates_outbox_depth", "lane=\"reliable\""));
        values.put("outbox_reliable_depth_max", sum(body, "tripupdates_outbox_depth_max", "lane=\"reliable\""));
        values.put("outbox_location_depth", sum(body, "tripupdates_outbox_depth", "lane=\"location\""));
        values.put("feed_read_failures", sum(body, "tripupdates_feed_read_failures_total", null));
        values.put("feed_resyncs", sum(body, "tripupdates_feed_resyncs_total", null));
        values.put("reconciliations", sum(body, "tripupdates_reconciliations_total", null));
        values.put("closed_slow_consumer", sum(body, "tripupdates_sessions_closed_total", "reason=\"SLOW_CONSUMER\""));
        values.put("closed_idle_timeout", sum(body, "tripupdates_sessions_closed_total", "reason=\"IDLE_TIMEOUT\""));
        values.put("closed_transport_error",
                sum(body, "tripupdates_sessions_closed_total", "reason=\"TRANSPORT_ERROR\""));
        values.put("outbox_overflows", sum(body, "tripupdates_outbox_offers_total", "result=\"OVERFLOW\""));
        values.put("locations_conflated", sum(body, "tripupdates_outbox_offers_total", "result=\"CONFLATED\""));
        values.put("delivered_events", sum(body, "tripupdates_delivery_lag_seconds_count", null));
        // From the Prometheus histogram buckets (cumulative since the gateway started).
        values.put("gateway_sos_lag_p50_seconds",
                histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\"", 0.50));
        values.put("gateway_sos_lag_p99_seconds",
                histogramQuantile(body, "tripupdates_delivery_lag_seconds", "type=\"SOS\"", 0.99));
        return values;
    }

    /**
     * Quantile estimated from Prometheus histogram buckets ({@code name_bucket{le=...}}), with linear interpolation
     * inside the bucket, as Prometheus' {@code histogram_quantile} does. Returns 0 when there are no observations.
     */
    static double histogramQuantile(String body, String name, String labelFragment, double quantile) {
        TreeMap<Double, Double> buckets = new TreeMap<>();
        String bucketName = name + "_bucket";
        for (String line : body.split("\n")) {
            if (!line.startsWith(bucketName + "{") || (labelFragment != null && !line.contains(labelFragment))) {
                continue;
            }
            int le = line.indexOf("le=\"");
            if (le < 0) {
                continue;
            }
            String bound = line.substring(le + 4, line.indexOf('"', le + 4));
            double upper = "+Inf".equals(bound) ? Double.POSITIVE_INFINITY : Double.parseDouble(bound);
            double count = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1).trim());
            buckets.merge(upper, count, Double::sum);
        }
        if (buckets.isEmpty()) {
            return 0;
        }
        double total = buckets.lastEntry().getValue();
        if (total <= 0) {
            return 0;
        }
        double rank = quantile * total;
        double lowerBound = 0;
        double lowerCount = 0;
        for (var bucket : buckets.entrySet()) {
            if (bucket.getValue() >= rank) {
                if (bucket.getKey().isInfinite()) {
                    return lowerBound; // above the highest finite bucket: report its bound, as Prometheus does
                }
                double inBucket = bucket.getValue() - lowerCount;
                return inBucket <= 0 ? bucket.getKey()
                        : lowerBound + (bucket.getKey() - lowerBound) * (rank - lowerCount) / inBucket;
            }
            lowerBound = bucket.getKey();
            lowerCount = bucket.getValue();
        }
        return lowerBound;
    }

    /** Sum of the samples of {@code name} whose label set contains {@code labelFragment} (null = any). */
    static double sum(String body, String name, String labelFragment) {
        double total = 0;
        for (String line : body.split("\n")) {
            if (line.startsWith("#") || !line.startsWith(name)) {
                continue;
            }
            int space = line.lastIndexOf(' ');
            String key = line.substring(0, space);
            String metric = key.contains("{") ? key.substring(0, key.indexOf('{')) : key;
            if (!metric.equals(name) || (labelFragment != null && !key.contains(labelFragment))) {
                continue;
            }
            try {
                double value = Double.parseDouble(line.substring(space + 1).trim());
                if (Double.isFinite(value)) { // empty percentile gauges report NaN
                    total += value;
                }
            } catch (NumberFormatException ignored) {
                // malformed sample: skip
            }
        }
        return total;
    }
}

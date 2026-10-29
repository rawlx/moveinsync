package com.moveinsync.tripupdates.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Externalised limits and connection settings. Values are documented in docs/TECHNICAL_SPEC.md.
 */
@Validated
@ConfigurationProperties("app")
public record AppProperties(
        @NotBlank String nodeId,
        @Valid @NotNull Redis redis,
        @Valid @NotNull Outbox outbox,
        @Valid @NotNull Retention retention,
        @Valid @NotNull Notifications notifications,
        @Valid @NotNull Auth auth,
        @Valid @NotNull WebSocket websocket) {

    /** {@code NONE}: no identity provider, every connection is rejected. {@code DEV}: test tokens only. */
    public enum AuthMode {
        NONE,
        DEV
    }

    public record Auth(@NotNull AuthMode mode) {
    }

    /**
     * @param maxMessageChars       longest accepted client frame
     * @param allowedOriginPatterns browser origins allowed to connect; empty means same-origin only
     */
    public record WebSocket(@Min(256) int maxMessageChars, @NotNull List<String> allowedOriginPatterns) {
    }

    /** {@code commandTimeout} bounds every Redis command; a timed-out append has an unknown outcome. */
    public record Redis(@NotBlank String uri, @NotNull Duration commandTimeout) {
    }

    /** Per-connection bounds (ADR-0006). */
    public record Outbox(
            @Min(1) int criticalCapacity,
            @Min(1) int locationCapacity,
            @Min(1) int pendingCapacity,
            @Min(1) int maxSubscriptionsPerConnection,
            @NotNull Duration sendTimeout) {
    }

    /** Redis retention (ADR-0003). */
    public record Retention(
            @Min(1) int logMaxLength,
            @Min(1) int busMaxLength,
            @NotNull Duration topicKeyTtl,
            @NotNull Duration dedupTtl) {
    }

    /**
     * Pub/Sub notifications. {@code reconcileInterval} bounds how long a durable event can wait for delivery when
     * its notification was lost.
     */
    public record Notifications(@NotBlank String channel, @NotNull Duration reconcileInterval) {
    }
}

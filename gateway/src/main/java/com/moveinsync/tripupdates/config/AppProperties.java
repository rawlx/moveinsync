package com.moveinsync.tripupdates.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
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
        @Valid @NotNull Retention retention) {

    public record Redis(@NotBlank String uri) {
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
}

package com.moveinsync.tripupdates.loadtest;

import ch.qos.logback.classic.Level;
import java.nio.file.Path;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the load generator (not a Spring application). Runs inside the gateway image on the Compose network,
 * started with Spring Boot's {@code PropertiesLauncher}; see {@code deploy/loadtest/run-loadtest.ps1}. Configuration
 * comes from {@code LOADTEST_*} environment variables ({@link LoadTestConfig}). Writes {@code result.json} and
 * {@code summary.txt} to {@code $LOADTEST_RESULTS_DIR/$LOADTEST_RUN_ID}.
 *
 * <p>Exit code: 0 all targets met, 2 completed but at least one target missed, 1 the run itself failed.
 */
public final class LoadTestMain {

    private LoadTestMain() {
    }

    public static void main(String[] args) {
        // No Spring logging configuration here: keep library logging (Lettuce, Netty) quiet.
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
        LoadTestConfig config = LoadTestConfig.fromEnvironment();
        LoadTest.log("config %s", config);
        try {
            LoadReport report = new LoadTest(config).run();
            report.write(Path.of(config.resultsDir(), config.runId()));
            System.out.println(report.summary());
            System.exit(report.passed() ? 0 : 2);
        } catch (Exception e) {
            LoadTest.log("load test failed: %s", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}

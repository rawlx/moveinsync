package com.moveinsync.tripupdates.loadtest;

import com.moveinsync.tripupdates.config.AppProperties;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.SosRaiser;
import com.moveinsync.tripupdates.domain.TenantTopic;
import com.moveinsync.tripupdates.domain.TopicId;
import com.moveinsync.tripupdates.domain.TripEvent;
import com.moveinsync.tripupdates.notify.RedisNotificationPublisher;
import com.moveinsync.tripupdates.publish.EventPublisher;
import com.moveinsync.tripupdates.publish.PublishOutcome;
import com.moveinsync.tripupdates.routing.EventRouter;
import com.moveinsync.tripupdates.sim.SyntheticFleet;
import com.moveinsync.tripupdates.sim.TripSimulator;
import com.moveinsync.tripupdates.store.RedisClients;
import com.moveinsync.tripupdates.store.RedisEventStore;
import io.lettuce.core.RedisClient;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The load-test engine: connects the planned clients, publishes simulated traffic through the routed
 * {@link EventPublisher}, injects SOS events, samples the gateways, and verifies every client's reliable sequence
 * history at the end. Connections are counted as established only when the gateway confirmed the subscription
 * ({@code SUBSCRIBED}); the gateways' own {@code tripupdates_sessions_active} is reported alongside.
 *
 * <p>Failure injection is external (the orchestration script kills a gateway container); the engine observes it
 * through the clients' disconnects and recoveries. {@link #run(Runnable, Duration)} also accepts an in-process hook
 * for tests.
 */
public final class LoadTest {

    private final LoadTestConfig config;
    private final SyntheticFleet fleet;
    private final List<LoadClient> clients = new ArrayList<>();
    /** Clients launched against the restored gateway after a failure (see {@link #failback()}). */
    private final List<LoadClient> lateClients = new CopyOnWriteArrayList<>();
    private volatile Double lateEligibleAtSeconds;
    private volatile Double lateLaunchedAtSeconds;
    private volatile Double lateAllSubscribedAtSeconds;
    private final HttpClient[] httpClients;
    private final ExecutorService callbackExecutor;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final GatewayProbe probe;
    private final PublishLog publishLog = new PublishLog();
    private final SosTracker sos;
    private final GatewayHealth health;
    private final LatencyRecorder reconnects = new LatencyRecorder(10_000);
    private final Map<TenantTopic, List<LoadClient>> securityByTopic = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> errors = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> probeAnswers = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> disconnectReasons = new ConcurrentHashMap<>();
    private final Map<EventType, LongAdder> publishedByType = new EnumMap<>(EventType.class);
    private final LongAdder delivered = new LongAdder();
    private final LongAdder connectFailures = new LongAdder();
    private final LongAdder publishFailures = new LongAdder();
    private final LongAdder notificationFailures = new LongAdder();
    private final LongAdder lateTicks = new LongAdder();
    private final AtomicLong firstDisconnectNanos = new AtomicLong();
    private final AtomicLong lastRecoveryNanos = new AtomicLong();
    private final Set<Integer> pendingFirstOutcome = ConcurrentHashMap.newKeySet();
    private final Semaphore connectPermits;
    private final List<Map<String, Object>> timeline = new CopyOnWriteArrayList<>();
    private final Map<String, Object> phases = new LinkedHashMap<>();
    private final long startNanos = System.nanoTime();
    private final Instant startUtc = Instant.now();
    private volatile int notLaunched;
    private volatile boolean stopping;
    private volatile boolean publishing;
    private volatile String phase = "START";

    private RedisClient redis;
    private RedisEventStore store;
    private RedisNotificationPublisher notifications;
    private RedisEventStore priorityStore;
    private RedisNotificationPublisher priorityNotifications;
    private EventPublisher publisher;

    public LoadTest(LoadTestConfig config) {
        this.config = config;
        this.fleet = SyntheticFleet.generate(new SyntheticFleet.Spec(config.tenants(), config.officesPerTenant(),
                config.cabsPerOffice(), config.passengersPerTrip()));
        this.probe = new GatewayProbe(config.gateways());
        this.sos = new SosTracker(2_000, startNanos, startUtc);
        this.health = new GatewayHealth(config.gateways().size(), () -> System.nanoTime() - startNanos);
        this.connectPermits = new Semaphore(config.maxConcurrentConnects());
        this.callbackExecutor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
        this.httpClients = new HttpClient[config.httpClients()];
        for (int i = 0; i < httpClients.length; i++) {
            httpClients[i] = HttpClient.newBuilder().executor(callbackExecutor)
                    .connectTimeout(Duration.ofSeconds(10)).build();
        }
        for (EventType type : EventType.values()) {
            publishedByType.put(type, new LongAdder());
        }
    }

    /** Runs the full test without an in-process failure hook. */
    public LoadReport run() throws InterruptedException {
        return run(null, null);
    }

    /**
     * @param midRunHook optional action run {@code hookAfter} after the steady phase begins (tests only)
     */
    public LoadReport run(Runnable midRunHook, Duration hookAfter) throws InterruptedException {
        try {
            openPublisher();
            scheduler.scheduleWithFixedDelay(this::probeDownGateways, 1, 1, TimeUnit.SECONDS);
            mark("START", probe.snapshot());
            connectAll();
            mark("CONNECTED", probe.snapshot());
            List<Thread> publishers = startPublishers();
            sleep(config.warmup());
            mark("STEADY", probe.snapshot());
            var sampler = scheduler.scheduleAtFixedRate(this::sample, 0, config.sampleInterval().toMillis(),
                    TimeUnit.MILLISECONDS);
            if (midRunHook != null) {
                scheduler.schedule(midRunHook, hookAfter.toMillis(), TimeUnit.MILLISECONDS);
            }
            Thread failback = config.lateClients() > 0
                    ? Thread.ofPlatform().name("failback").start(this::failback) : null;
            sleep(config.duration());
            if (failback != null) {
                failback.interrupt();
                failback.join(10_000);
            }
            publishing = false;
            for (Thread thread : publishers) {
                thread.join(30_000);
            }
            mark("PUBLISHING_STOPPED", probe.snapshot());
            sleep(config.drain());
            sampler.cancel(false);
            sample();
            mark("DRAINED", probe.snapshot());
            return report();
        } finally {
            shutdown();
        }
    }

    // ---- setup --------------------------------------------------------------------------------------------------

    private void openPublisher() {
        redis = RedisClients.create(config.redisUri(), Duration.ofSeconds(2));
        store = new RedisEventStore(redis, new AppProperties.Retention(1000, 100_000, Duration.ofHours(24),
                Duration.ofMinutes(5)));
        notifications = new RedisNotificationPublisher(redis, "notify:events");
        // The same SOS lane as the gateways' own publisher: separate store connection and notification channel.
        priorityStore = new RedisEventStore(redis, new AppProperties.Retention(1000, 100_000, Duration.ofHours(24),
                Duration.ofMinutes(5)));
        priorityNotifications = new RedisNotificationPublisher(redis,
                RedisNotificationPublisher.priorityChannel("notify:events"));
        publisher = new EventPublisher(store, notifications, priorityStore, priorityNotifications,
                new EventRouter(fleet.directory()));
    }

    private void connectAll() throws InterruptedException {
        for (ClientPlan.ClientSpec spec : ClientPlan.build(fleet, config)) {
            LoadClient client = new LoadClient(spec, this);
            clients.add(client);
            if (spec.kind() == ClientPlan.Kind.SECURITY_SOS) {
                securityByTopic.computeIfAbsent(spec.tenantTopic(), t -> new CopyOnWriteArrayList<>()).add(client);
            }
        }
        long intervalNanos = 1_000_000_000L / Math.max(1, config.connectsPerSecond());
        // One deadline for the whole connect phase: if the gateways stop answering, the ramp stops instead of
        // waiting forever for a permit, and the clients never launched are reported.
        long deadline = System.nanoTime() + config.connectTimeout().toNanos();
        long next = System.nanoTime();
        for (LoadClient client : clients) {
            if (!connectPermits.tryAcquire(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                notLaunched = clients.size() - clients.indexOf(client);
                log("connect phase deadline reached: %d clients not launched", notLaunched);
                break;
            }
            pendingFirstOutcome.add(client.spec().id());
            client.connect();
            next += intervalNanos;
            long wait = next - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            }
        }
        while (!pendingFirstOutcome.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(250);
        }
        log("connect phase done: subscribed=%d failed=%d pending=%d notLaunched=%d", countSubscribed(), countState(
                LoadClient.State.FAILED), pendingFirstOutcome.size(), notLaunched);
    }

    private void firstOutcome(LoadClient client) {
        if (pendingFirstOutcome.remove(client.spec().id())) {
            connectPermits.release();
        }
    }

    /**
     * Failback: waits until the load generator has seen the configured gateway fail (a DOWN transition) and become
     * eligible again (an UP transition after consecutive successful readiness probes), then connects the late
     * clients to that gateway only. Existing clients are not moved: redistribution is not part of the design.
     */
    private void failback() {
        int gateway = config.lateGateway();
        long deadline = System.nanoTime() + config.lateTimeout().toNanos();
        try {
            while (!restoredAfterFailure(gateway)) {
                if (System.nanoTime() > deadline || !publishing) {
                    log("failback: gateway %d was not seen failing and recovering; late clients not launched",
                            gateway);
                    return;
                }
                Thread.sleep(250);
            }
            lateEligibleAtSeconds = seconds(System.nanoTime() - startNanos);
            List<ClientPlan.ClientSpec> specs = ClientPlan.buildLate(fleet, config.lateClients(), gateway,
                    clients.size());
            lateLaunchedAtSeconds = seconds(System.nanoTime() - startNanos);
            log("failback: gateway %d eligible at %.2fs, launching %d late clients", gateway, lateEligibleAtSeconds,
                    specs.size());
            for (ClientPlan.ClientSpec spec : specs) {
                LoadClient client = new LoadClient(spec, this);
                lateClients.add(client);
                if (spec.kind() == ClientPlan.Kind.SECURITY_SOS) {
                    securityByTopic.computeIfAbsent(spec.tenantTopic(), t -> new CopyOnWriteArrayList<>())
                            .add(client);
                }
                client.connect();
                Thread.sleep(10); // about 100 connects per second
            }
            while (System.nanoTime() < deadline && publishing) {
                if (lateClients.stream().allMatch(LoadClient::everSubscribed)) {
                    lateAllSubscribedAtSeconds = seconds(System.nanoTime() - startNanos);
                    log("failback: all %d late clients subscribed at %.2fs", lateClients.size(),
                            lateAllSubscribedAtSeconds);
                    return;
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean restoredAfterFailure(int gateway) {
        boolean down = false;
        for (GatewayHealth.Transition transition : health.transitions()) {
            if (transition.gateway() == gateway) {
                if ("DOWN".equals(transition.state())) {
                    down = true;
                } else if (down && "UP".equals(transition.state())) {
                    return true;
                }
            }
        }
        return false;
    }

    // ---- publishing ---------------------------------------------------------------------------------------------

    private List<Thread> startPublishers() {
        publishing = true;
        List<List<TripSimulator.FleetTrip>> partitions = new ArrayList<>();
        for (int i = 0; i < config.publisherThreads(); i++) {
            partitions.add(new ArrayList<>());
        }
        List<TripSimulator.FleetTrip> trips = fleet.trips();
        for (int i = 0; i < trips.size(); i++) {
            partitions.get(i % partitions.size()).add(trips.get(i));
        }
        List<Thread> threads = new ArrayList<>();
        long interval = config.locationInterval().toNanos();
        for (int p = 0; p < partitions.size(); p++) {
            if (partitions.get(p).isEmpty()) {
                continue;
            }
            TripSimulator simulator = new TripSimulator(partitions.get(p), config.seed() + p,
                    config.runId() + "-p" + p, Clock.systemUTC(), config.statusEveryTicks(),
                    config.delayProbability());
            long offset = interval * p / partitions.size();
            threads.add(Thread.ofPlatform().name("publisher-" + p).start(() -> publishLoop(simulator, offset)));
        }
        if (config.sosPerSecond() > 0) {
            threads.add(Thread.ofPlatform().name("sos-injector").start(this::sosLoop));
        }
        return threads;
    }

    private void publishLoop(TripSimulator simulator, long offsetNanos) {
        long interval = config.locationInterval().toNanos();
        long next = System.nanoTime() + offsetNanos;
        while (publishing) {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(Math.min(wait, 200_000_000L));
                } catch (InterruptedException e) {
                    return;
                }
                continue;
            }
            if (-wait > interval) {
                lateTicks.increment();
            }
            for (TripEvent event : simulator.tick()) {
                if (!publishing) {
                    return;
                }
                publishOne(event);
            }
            next += interval;
        }
    }

    private void sosLoop() {
        TripSimulator simulator = new TripSimulator(fleet.trips(), config.seed() - 1, config.runId() + "-sos",
                Clock.systemUTC(), 1_000_000, 0);
        Random random = new Random(config.seed());
        long interval = (long) (1_000_000_000L / config.sosPerSecond());
        long next = System.nanoTime();
        while (publishing) {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    return;
                }
            }
            TripSimulator.FleetTrip trip = fleet.trips().get(random.nextInt(fleet.trips().size()));
            TripEvent event = simulator.sos(trip, SosRaiser.EMPLOYEE);
            TenantTopic sosTopic = new TenantTopic(trip.tenantId(), TopicId.sos(trip.officeId()));
            sos.publishing(event.eventId(), System.nanoTime(), expectedSecurityRecipients(sosTopic));
            if (!publishOne(event)) {
                sos.publishFailed(event.eventId());
            }
            next += interval;
        }
    }

    private int expectedSecurityRecipients(TenantTopic sosTopic) {
        int count = 0;
        for (LoadClient client : securityByTopic.getOrDefault(sosTopic, List.of())) {
            LoadClient.State state = client.state();
            if (client.everSubscribed() && state != LoadClient.State.FAILED && state != LoadClient.State.CLOSED) {
                count++;
            }
        }
        return count;
    }

    private boolean publishOne(TripEvent event) {
        try {
            PublishOutcome outcome = publisher.publish(event);
            publishLog.record(outcome.appended().events());
            publishedByType.get(event.type()).increment();
            if (!outcome.notified()) {
                notificationFailures.increment();
            }
            return true;
        } catch (RuntimeException e) {
            publishFailures.increment();
            error("PUBLISH_" + e.getClass().getSimpleName());
            return false;
        }
    }

    // ---- callbacks from clients ---------------------------------------------------------------------------------

    String gatewayUri(int index) {
        return config.gateways().get(index);
    }

    int gatewayCount() {
        return config.gateways().size();
    }

    HttpClient httpClient(int clientId) {
        return httpClients[clientId % httpClients.length];
    }

    ScheduledExecutorService scheduler() {
        return scheduler;
    }

    boolean stopping() {
        return stopping;
    }

    void subscribed(LoadClient client) {
        firstOutcome(client);
    }

    void probeAnswered(LoadClient client, String code) {
        probeAnswers.computeIfAbsent(code, c -> new LongAdder()).increment();
        firstOutcome(client);
        client.close();
    }

    void recovered(LoadClient client, long outageNanos) {
        reconnects.record(outageNanos);
        lastRecoveryNanos.set(System.nanoTime());
    }

    void disconnected(LoadClient client, String why) {
        disconnectReasons.computeIfAbsent(why, w -> new LongAdder()).increment();
        if (client.everSubscribed()) {
            firstDisconnectNanos.compareAndSet(0, System.nanoTime());
        }
    }

    Set<Integer> healthyGateways() {
        return health.healthy();
    }

    /** A client saw a gateway-level failure (abnormal close or failed connect). */
    void gatewayFailed(int gateway, String reason) {
        health.markDown(gateway, reason);
    }

    /** Readiness probes for gateways marked down; they become eligible again after consecutive successes. */
    private void probeDownGateways() {
        for (int g = 0; g < config.gateways().size(); g++) {
            if (!health.isUp(g)) {
                health.probeResult(g, probe.ready(g));
            }
        }
    }

    void connectFailed(LoadClient client, Throwable error) {
        connectFailures.increment();
        error("CONNECT_" + rootCause(error).getClass().getSimpleName());
        if (client.state() == LoadClient.State.FAILED) {
            firstOutcome(client);
        }
    }

    void delivered() {
        delivered.increment();
    }

    void sosReceived(String eventId, int clientId, int gateway, long nanos, boolean recovering) {
        sos.received(eventId, clientId, gateway, nanos, recovering);
    }

    void error(String code) {
        errors.computeIfAbsent(code, c -> new LongAdder()).increment();
    }

    // ---- sampling and reporting ---------------------------------------------------------------------------------

    private void mark(String name, List<Map<String, Double>> gateways) {
        phase = name;
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("atUtc", Instant.now().toString());
        entry.put("elapsedSeconds", seconds(System.nanoTime() - startNanos));
        entry.put("subscribedClients", countSubscribed());
        entry.put("recoveringClients", countState(LoadClient.State.RECOVERING));
        entry.put("clientsNotLaunched", notLaunched);
        entry.put("loadGeneratorHeapUsedBytes", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        entry.put("gateways", gateways);
        phases.put(name, entry);
        // Marker lines for the orchestration script.
        log("LOADTEST PHASE %s subscribed=%d sessionsOnGateways=%.0f", name, countSubscribed(),
                gateways.stream().mapToDouble(g -> g.getOrDefault("sessions_active", 0.0)).sum());
    }

    private void sample() {
        try {
            List<Map<String, Double>> gateways = probe.snapshot();
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("elapsedSeconds", seconds(System.nanoTime() - startNanos));
            point.put("phase", phase);
            point.put("subscribedClients", countSubscribed());
            point.put("recoveringClients", countState(LoadClient.State.RECOVERING));
            point.put("publishedEvents", totalPublished());
            point.put("deliveredEvents", delivered.sum());
            point.put("sosLiveP99Ms", sos.summary().live().p99Ms());
            List<Double> sessions = new ArrayList<>();
            List<Double> heaps = new ArrayList<>();
            for (Map<String, Double> gateway : gateways) {
                sessions.add(gateway.getOrDefault("sessions_active", -1.0));
                heaps.add(gateway.getOrDefault("heap_used_bytes", -1.0));
            }
            point.put("gatewaySessions", sessions);
            point.put("gatewayHeapUsedBytes", heaps);
            timeline.add(point);
            log("t=%.0fs %s subscribed=%d recovering=%d published=%d delivered=%d gatewaySessions=%s", (double)
                    point.get("elapsedSeconds"), phase, point.get("subscribedClients"), point.get("recoveringClients"),
                    totalPublished(), delivered.sum(), sessions);
        } catch (RuntimeException e) {
            error("SAMPLER_" + e.getClass().getSimpleName());
        }
    }

    private LoadReport report() {
        Map<ClientPlan.Kind, LoadReport.KindResult> byKind = new EnumMap<>(ClientPlan.Kind.class);
        for (ClientPlan.Kind kind : ClientPlan.Kind.values()) {
            byKind.put(kind, new LoadReport.KindResult());
        }
        for (LoadClient client : clients) {
            LoadReport.KindResult result = byKind.get(client.spec().kind());
            result.planned++;
            if (!client.everSubscribed()) {
                if (client.spec().kind() != ClientPlan.Kind.PROBE) {
                    result.neverSubscribed++;
                }
                continue;
            }
            result.subscribed++;
            result.reconnects += client.reconnects();
            result.duplicates += client.duplicates();
            result.locations += client.locations();
            result.tenantViolations += client.tenantViolations();
            SequenceVerifier.Result verified = SequenceVerifier.verify(client.startSeq(),
                    publishLog.reliable(client.spec().tenantTopic()), client.receivedSeqs());
            result.expectedReliable += verified.expected();
            result.receivedReliable += verified.received();
            result.missingReliable += verified.missing();
            result.unexpectedReliable += verified.unexpected();
            if (!verified.complete()) {
                result.incompleteClients++;
            }
        }
        Map<String, Long> published = new LinkedHashMap<>();
        LoadReport.FailbackResult failbackResult = config.lateClients() == 0 ? null : failbackResult();
        Map<Integer, Map<String, Long>> perGateway = new TreeMap<>();
        for (int g = 0; g < config.gateways().size(); g++) {
            Map<String, Long> row = new LinkedHashMap<>();
            row.put("initialClients", 0L);
            row.put("initialClientsReconnected", 0L);
            row.put("finalClients", 0L);
            perGateway.put(g, row);
        }
        for (LoadClient client : allClients()) {
            if (client.spec().kind() == ClientPlan.Kind.PROBE || !client.everSubscribed()) {
                continue;
            }
            if (!lateClients.contains(client)) {
                perGateway.get(client.spec().gateway()).merge("initialClients", 1L, Long::sum);
                if (client.reconnects() > 0) {
                    perGateway.get(client.spec().gateway()).merge("initialClientsReconnected", 1L, Long::sum);
                }
            }
            perGateway.get(client.gateway()).merge("finalClients", 1L, Long::sum);
        }
        publishedByType.forEach((type, count) -> published.put(type.name(), count.sum()));
        long first = firstDisconnectNanos.get();
        long last = lastRecoveryNanos.get();
        return new LoadReport(config, startUtc.toString(), fleet.spec().cabs(), byKind, published,
                publishFailures.sum(),
                notificationFailures.sum(), lateTicks.sum(), delivered.sum(), connectFailures.sum(),
                countState(LoadClient.State.FAILED), sums(errors), sums(probeAnswers), sums(disconnectReasons),
                sos.summary(), reconnects.summary(),
                first == 0 ? null : seconds(first - startNanos),
                first == 0 || last < first ? null : seconds(last - first),
                health.transitions(), failbackResult, perGateway, phases, timeline);
    }

    private LoadReport.FailbackResult failbackResult() {
        int planned = lateClients.size();
        int subscribed = 0;
        int onGateway = 0;
        long expected = 0;
        long received = 0;
        long missing = 0;
        long unexpected = 0;
        long duplicates = 0;
        long locations = 0;
        long violations = 0;
        long reconnected = 0;
        for (LoadClient client : lateClients) {
            if (!client.everSubscribed()) {
                continue;
            }
            subscribed++;
            if (client.gateway() == config.lateGateway()) {
                onGateway++;
            }
            reconnected += client.reconnects() > 0 ? 1 : 0;
            duplicates += client.duplicates();
            locations += client.locations();
            violations += client.tenantViolations();
            SequenceVerifier.Result verified = SequenceVerifier.verify(client.startSeq(),
                    publishLog.reliable(client.spec().tenantTopic()), client.receivedSeqs());
            expected += verified.expected();
            received += verified.received();
            missing += verified.missing();
            unexpected += verified.unexpected();
        }
        return new LoadReport.FailbackResult(config.lateGateway(), config.lateClients(), planned, subscribed,
                onGateway, reconnected, lateEligibleAtSeconds, lateLaunchedAtSeconds, lateAllSubscribedAtSeconds,
                expected, received, missing, unexpected, duplicates, locations, violations);
    }

    private List<LoadClient> allClients() {
        List<LoadClient> all = new ArrayList<>(clients);
        all.addAll(lateClients);
        return all;
    }

    private static Map<String, Long> sums(Map<String, LongAdder> counters) {
        Map<String, Long> out = new LinkedHashMap<>();
        counters.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(entry -> out.put(entry.getKey(), entry.getValue().sum()));
        return out;
    }

    private long totalPublished() {
        return publishedByType.values().stream().mapToLong(LongAdder::sum).sum();
    }

    private int countSubscribed() {
        return (int) allClients().stream().filter(c -> c.spec().kind() != ClientPlan.Kind.PROBE)
                .filter(c -> c.state() == LoadClient.State.SUBSCRIBED).count();
    }

    private int countState(LoadClient.State state) {
        return (int) allClients().stream().filter(c -> c.state() == state).count();
    }

    private void shutdown() {
        stopping = true;
        publishing = false;
        for (LoadClient client : allClients()) {
            try {
                client.close();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
        scheduler.shutdownNow();
        for (HttpClient http : httpClients) {
            http.shutdownNow();
        }
        callbackExecutor.shutdownNow();
        if (notifications != null) {
            notifications.close();
        }
        if (priorityNotifications != null) {
            priorityNotifications.close();
        }
        if (priorityStore != null) {
            priorityStore.close();
        }
        if (store != null) {
            store.close();
        }
        if (redis != null) {
            redis.shutdown();
        }
    }

    private static void sleep(Duration duration) throws InterruptedException {
        Thread.sleep(duration.toMillis());
    }

    private static double seconds(long nanos) {
        return Math.round(nanos / 1e7) / 100.0;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    static void log(String format, Object... args) {
        System.out.println("[loadtest] " + String.format(format, args));
    }
}

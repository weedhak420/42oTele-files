package telegram.files;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import telegram.files.repository.StatisticRecord;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMonitorVerticle extends AbstractVerticle {

    public static final String METRICS_UPDATE_ADDRESS = "metrics.performance.update";
    public static final String METRICS_QUERY_ADDRESS = "metrics.performance.query";
    public static final String METRICS_HISTORY_ADDRESS = "metrics.performance.history";
    public static final String METRICS_HEALTH_ADDRESS = "metrics.performance.health";
    public static final String ALERTS_QUERY_ADDRESS = "metrics.alerts.query";
    public static final String METRICS_RESOURCES_ADDRESS = "metrics.resources.query";
    public static final String METRICS_RECOMMENDATIONS_ADDRESS = "metrics.recommendations.query";

    private static final Log log = LogFactory.get();

    private final Deque<Boolean> downloadOutcomes = new ArrayDeque<>();
    private final Deque<Long> downloadSpeedSamples = new ArrayDeque<>();
    private final Deque<Long> responseTimeSamples = new ArrayDeque<>();
    private final Map<String, Integer> failedDownloadCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> downloadStartTimes = new ConcurrentHashMap<>();
    private final Set<String> activeDownloadIds = ConcurrentHashMap.newKeySet();
    private final Deque<JsonObject> historyBuffer = new ArrayDeque<>();
    private final Deque<JsonObject> alertBuffer = new ArrayDeque<>();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong eventBusInFlight = new AtomicLong();

    private volatile PerformanceMetrics currentMetrics = new PerformanceMetrics(0, 0, 0, 0, Map.of(), 0, 0, 0, System.currentTimeMillis());
    private volatile int queuedDownloads;
    private volatile long lastNetworkLatency;
    private long periodicMetricTimerId;
    private long snapshotTimerId;
    private long latencyProbeTimerId;

    @Override
    public void start(Promise<Void> startPromise) {
        registerInterceptors();
        initConsumers();
        periodicMetricTimerId = vertx.setPeriodic(10_000, id -> updateCurrentMetrics());
        snapshotTimerId = vertx.setPeriodic(Duration.ofMinutes(5).toMillis(), id -> persistSnapshot());
        latencyProbeTimerId = vertx.setPeriodic(10_000, id -> probeLatency());
        updateCurrentMetrics();
        startPromise.complete();
    }

    @Override
    public void stop() {
        if (periodicMetricTimerId != 0) {
            vertx.cancelTimer(periodicMetricTimerId);
        }
        if (snapshotTimerId != 0) {
            vertx.cancelTimer(snapshotTimerId);
        }
        if (latencyProbeTimerId != 0) {
            vertx.cancelTimer(latencyProbeTimerId);
        }
    }

    private void registerInterceptors() {
        vertx.eventBus().addInboundInterceptor(ctx -> {
            eventBusInFlight.incrementAndGet();
            ctx.next();
            eventBusInFlight.decrementAndGet();
        });
    }

    private void initConsumers() {
        vertx.eventBus().consumer(EventEnum.TELEGRAM_EVENT.address(), message -> handleTelegramEvent((JsonObject) message.body()));
        vertx.eventBus().consumer(METRICS_UPDATE_ADDRESS, message -> handleMetricUpdate((JsonObject) message.body()));
        vertx.eventBus().consumer(METRICS_QUERY_ADDRESS, message -> message.reply(buildMetricResponse()));
        vertx.eventBus().consumer(METRICS_HISTORY_ADDRESS, message -> fetchHistory()
                .onSuccess(message::reply)
                .onFailure(err -> message.fail(500, err.getMessage())));
        vertx.eventBus().consumer(METRICS_HEALTH_ADDRESS, message -> message.reply(buildHealthResponse()));
        vertx.eventBus().consumer(ALERTS_QUERY_ADDRESS, message -> message.reply(buildAlertResponse()));
        vertx.eventBus().consumer(METRICS_RESOURCES_ADDRESS, message -> message.reply(buildResources()));
        vertx.eventBus().consumer(METRICS_RECOMMENDATIONS_ADDRESS, message -> message.reply(buildRecommendationsResponse()));
    }

    private void handleMetricUpdate(JsonObject update) {
        String type = update.getString("type");
        if (StrUtil.isBlank(type)) {
            return;
        }
        switch (type) {
            case "downloadStart" -> handleDownloadStart(update);
            case "queueState" -> queuedDownloads = update.getInteger("queued", queuedDownloads);
            case "downloadFailure" -> handleDownloadFailure(update);
            case "apiCall" -> recordResponseTime(update.getLong("durationMs", 0L));
            case "cacheAccess" -> recordCacheAccess(update.getBoolean("hit", false));
            case "networkLatency" -> lastNetworkLatency = update.getLong("latency", lastNetworkLatency);
            default -> log.trace("Ignored unknown metric update type: %s".formatted(type));
        }
    }

    private void handleTelegramEvent(JsonObject message) {
        try {
            EventPayload payload = message.getJsonObject("payload").mapTo(EventPayload.class);
            if (payload.type() == EventPayload.TYPE_FILE_STATUS) {
                JsonObject data = JsonObject.mapFrom(payload.data());
                String status = data.getString("downloadStatus");
                String uniqueId = data.getString("uniqueId");
                if (StrUtil.isBlank(status) || StrUtil.isBlank(uniqueId)) {
                    return;
                }
                switch (status) {
                    case "completed" -> recordDownloadCompletion(uniqueId);
                    case "error" -> recordDownloadError(uniqueId);
                    default -> {
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse telegram event: {}", e.getMessage());
        }
    }

    private void handleDownloadStart(JsonObject update) {
        String uniqueId = update.getString("uniqueId");
        if (StrUtil.isBlank(uniqueId)) {
            return;
        }
        queuedDownloads = update.getInteger("queued", queuedDownloads);
        downloadStartTimes.put(uniqueId, System.currentTimeMillis());
        activeDownloadIds.add(uniqueId);
        updateCurrentMetrics();
    }

    private void handleDownloadFailure(JsonObject update) {
        String uniqueId = update.getString("uniqueId");
        if (StrUtil.isBlank(uniqueId)) {
            return;
        }
        int retryCount = update.getInteger("retryCount", 0);
        failedDownloadCounts.put(uniqueId, retryCount);
        recordOutcome(false);
        activeDownloadIds.remove(uniqueId);
        updateCurrentMetrics();
    }

    private void recordResponseTime(long durationMs) {
        if (durationMs <= 0) {
            return;
        }
        responseTimeSamples.add(durationMs);
        trimDeque(responseTimeSamples);
    }

    private void recordCacheAccess(boolean hit) {
        if (hit) {
            cacheHits.incrementAndGet();
        } else {
            cacheMisses.incrementAndGet();
        }
    }

    private void recordDownloadCompletion(String uniqueId) {
        Long startTime = downloadStartTimes.remove(uniqueId);
        recordOutcome(true);
        activeDownloadIds.remove(uniqueId);
        failedDownloadCounts.remove(uniqueId);
        if (startTime != null) {
            DataVerticle.fileRepository.getByUniqueId(uniqueId)
                    .onSuccess(fileRecord -> {
                        if (fileRecord != null && fileRecord.size() > 0) {
                            long duration = Math.max(1L, System.currentTimeMillis() - startTime);
                            long speed = (fileRecord.size() * 1000L) / duration;
                            downloadSpeedSamples.add(speed);
                            trimDeque(downloadSpeedSamples);
                        }
                        updateCurrentMetrics();
                    })
                    .onFailure(err -> log.warn("Failed to load file record for metrics: {}", err.getMessage()));
        } else {
            updateCurrentMetrics();
        }
    }

    private void recordDownloadError(String uniqueId) {
        recordOutcome(false);
        activeDownloadIds.remove(uniqueId);
        updateCurrentMetrics();
    }

    private void recordOutcome(boolean success) {
        downloadOutcomes.add(success);
        trimDeque(downloadOutcomes);
    }

    private void trimDeque(Deque<?> deque) {
        while (deque.size() > 100) {
            deque.poll();
        }
    }

    private void updateCurrentMetrics() {
        double successRate = calculateSuccessRate();
        double avgSpeed = average(downloadSpeedSamples);
        double avgResponse = average(responseTimeSamples);
        double hitRate = calculateCacheHitRate();
        PerformanceMetrics metrics = new PerformanceMetrics(
                successRate,
                avgSpeed,
                activeDownloadIds.size(),
                queuedDownloads,
                new HashMap<>(failedDownloadCounts),
                hitRate,
                avgResponse,
                lastNetworkLatency,
                System.currentTimeMillis()
        );
        currentMetrics = metrics;
    }

    private double calculateSuccessRate() {
        if (downloadOutcomes.isEmpty()) {
            return 100.0;
        }
        long successCount = downloadOutcomes.stream().filter(Boolean::booleanValue).count();
        return successCount * 100.0 / downloadOutcomes.size();
    }

    private double average(Deque<Long> samples) {
        if (samples.isEmpty()) {
            return 0;
        }
        return samples.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private double calculateCacheHitRate() {
        long hits = cacheHits.get();
        long misses = cacheMisses.get();
        if (hits + misses == 0) {
            return 0;
        }
        return hits * 100.0 / (hits + misses);
    }

    private JsonObject buildMetricResponse() {
        return new JsonObject()
                .put("timestamp", System.currentTimeMillis())
                .put("metrics", JsonObject.mapFrom(currentMetrics))
                .put("status", evaluateStatus(currentMetrics))
                .put("recommendations", buildRecommendations());
    }

    private JsonObject buildHealthResponse() {
        return new JsonObject()
                .put("timestamp", System.currentTimeMillis())
                .put("metrics", JsonObject.mapFrom(currentMetrics))
                .put("status", evaluateStatus(currentMetrics));
    }

    private Future<JsonObject> fetchHistory() {
        long end = System.currentTimeMillis();
        long start = end - Duration.ofDays(7).toMillis();
        return DataVerticle.statisticRepository.getRangeStatistics(StatisticRecord.Type.performance, 0, start, end)
                .map(records -> new JsonObject()
                        .put("timestamp", System.currentTimeMillis())
                        .put("metrics", historyBufferToJson(records))
                        .put("status", evaluateStatus(currentMetrics))
                        .put("recommendations", buildRecommendations()));
    }

    private List<JsonObject> historyBufferToJson(List<StatisticRecord> records) {
        List<JsonObject> combined = new ArrayList<>(historyBuffer);
        if (CollUtil.isNotEmpty(records)) {
            combined.addAll(records.stream()
                    .map(r -> new JsonObject(r.data()).put("timestamp", r.timestamp()))
                    .toList());
        }
        combined.sort((o1, o2) -> Long.compare(o1.getLong("timestamp"), o2.getLong("timestamp")));
        return combined;
    }

    private JsonObject buildAlertResponse() {
        return new JsonObject()
                .put("timestamp", System.currentTimeMillis())
                .put("metrics", new JsonObject())
                .put("status", evaluateStatus(currentMetrics))
                .put("recommendations", buildRecommendations())
                .put("alerts", new ArrayList<>(alertBuffer));
    }

    private JsonObject buildResources() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        JsonObject resources = new JsonObject()
                .put("heapUsed", heap.getUsed())
                .put("heapCommitted", heap.getCommitted())
                .put("nonHeapUsed", nonHeap.getUsed())
                .put("nonHeapCommitted", nonHeap.getCommitted())
                .put("threadCount", threadMXBean.getThreadCount())
                .put("peakThreadCount", threadMXBean.getPeakThreadCount())
                .put("eventBusInFlight", eventBusInFlight.get());
        return new JsonObject()
                .put("timestamp", System.currentTimeMillis())
                .put("metrics", resources)
                .put("status", evaluateStatus(currentMetrics))
                .put("recommendations", buildRecommendations());
    }

    private JsonObject buildRecommendationsResponse() {
        return new JsonObject()
                .put("timestamp", System.currentTimeMillis())
                .put("metrics", JsonObject.mapFrom(currentMetrics))
                .put("status", evaluateStatus(currentMetrics))
                .put("recommendations", buildRecommendations());
    }

    private List<String> buildRecommendations() {
        List<String> recommendations = new ArrayList<>();
        double speedMb = currentMetrics.averageDownloadSpeed() / (1024d * 1024);
        if (speedMb > 5) {
            recommendations.add("Increase concurrent download limit to leverage high bandwidth.");
        } else if (speedMb < 0.5) {
            recommendations.add("Decrease concurrent download limit to stabilize low bandwidth connections.");
        }
        if (currentMetrics.downloadSuccessRate() < 90) {
            recommendations.add("Review network stability or retry settings to improve download success rate.");
        }
        if (currentMetrics.queuedDownloads() > currentMetrics.activeDownloads()) {
            recommendations.add("Consider lowering history scan interval to reduce queue backlog.");
        }
        if (currentMetrics.cacheHitRate() < 50) {
            recommendations.add("Warm chat cache or increase cache size to improve hit rate.");
        }
        return recommendations;
    }

    private String evaluateStatus(PerformanceMetrics metrics) {
        if (metrics.downloadSuccessRate() < 80 || metrics.averageDownloadSpeed() < 100 * 1024) {
            return "critical";
        }
        if (metrics.downloadSuccessRate() < 90 || metrics.averageDownloadSpeed() < 300 * 1024) {
            return "degraded";
        }
        return "healthy";
    }

    private void persistSnapshot() {
        JsonObject data = JsonObject.mapFrom(currentMetrics);
        data.put("status", evaluateStatus(currentMetrics));
        historyBuffer.add(data.copy());
        trimDeque(historyBuffer);
        DataVerticle.statisticRepository.create(new StatisticRecord(
                Convert.toStr(0),
                StatisticRecord.Type.performance,
                System.currentTimeMillis(),
                data.encode()
        ));
        cleanupOldHistory();
        evaluateAlerts();
    }

    private void evaluateAlerts() {
        String status = evaluateStatus(currentMetrics);
        if (Objects.equals(status, "critical")) {
            JsonObject alert = new JsonObject()
                    .put("timestamp", System.currentTimeMillis())
                    .put("status", status)
                    .put("metrics", JsonObject.mapFrom(currentMetrics));
            alertBuffer.add(alert);
            trimDeque(alertBuffer);
            vertx.eventBus().publish(EventEnum.SYSTEM_ALERT.address(), alert);
            DataVerticle.statisticRepository.create(new StatisticRecord(
                    Convert.toStr(0),
                    StatisticRecord.Type.systemAlert,
                    System.currentTimeMillis(),
                    alert.encode()
            ));
        }
    }

    private void cleanupOldHistory() {
        long cutoff = System.currentTimeMillis() - Duration.ofDays(7).toMillis();
        DataVerticle.statisticRepository.deleteOlderThan(StatisticRecord.Type.performance, cutoff)
                .onFailure(err -> log.warn("Failed to cleanup performance metrics: {}", err.getMessage()));
        DataVerticle.statisticRepository.deleteOlderThan(StatisticRecord.Type.systemAlert, cutoff)
                .onFailure(err -> log.warn("Failed to cleanup alerts: {}", err.getMessage()));
    }

    private void probeLatency() {
        TelegramVerticles.getAll().stream().findFirst()
                .ifPresent(verticle -> verticle.ping()
                        .onSuccess(seconds -> vertx.eventBus().publish(METRICS_UPDATE_ADDRESS,
                                new JsonObject()
                                        .put("type", "networkLatency")
                                        .put("latency", Math.round(seconds * 1000))))
                        .onFailure(err -> log.trace("Latency probe failed: {}", err.getMessage())));
    }
}

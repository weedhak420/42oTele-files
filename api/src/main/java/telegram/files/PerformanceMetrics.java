package telegram.files;

import java.util.Map;

public record PerformanceMetrics(double downloadSuccessRate,
                                 double averageDownloadSpeed,
                                 int activeDownloads,
                                 int queuedDownloads,
                                 Map<String, Integer> failedDownloads,
                                 double cacheHitRate,
                                 double averageResponseTime,
                                 long networkLatency,
                                 long timestamp) {
}

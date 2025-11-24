package telegram.files.repository.segmented;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepository;

/**
 * Aggregation and reporting oriented queries for file records.
 */
public class FileStatisticsRepository {

    private final FileRepository delegate;

    public FileStatisticsRepository(FileRepository delegate) {
        this.delegate = delegate;
    }

    public Future<JsonObject> getDownloadStatistics(long telegramId) {
        return delegate.getDownloadStatistics(telegramId);
    }

    public Future<JsonObject> getDownloadStatistics() {
        return delegate.getDownloadStatistics();
    }

    public Future<JsonArray> getCompletedRangeStatistics(long id, long startTime, long endTime, int timeRange) {
        return delegate.getCompletedRangeStatistics(id, startTime, endTime, timeRange);
    }

    public Future<Integer> countByStatus(long telegramId, FileRecord.DownloadStatus downloadStatus) {
        return delegate.countByStatus(telegramId, downloadStatus);
    }

    public Future<JsonObject> countWithType(long telegramId, long chatId) {
        return delegate.countWithType(telegramId, chatId);
    }
}

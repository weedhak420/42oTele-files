package telegram.files.repository;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;

public interface StatisticRepository {
    Future<Void> create(StatisticRecord record);

    Future<Void> batchCreate(List<StatisticRecord> records);

    void startBufferedWriter(Vertx vertx);

    Future<List<StatisticRecord>> getRangeStatistics(StatisticRecord.Type type,
                                                     long relatedId,
                                                     long startTime,
                                                     long endTime);

    Future<Void> deleteOlderThan(StatisticRecord.Type type, long cutoffTimestamp);
}

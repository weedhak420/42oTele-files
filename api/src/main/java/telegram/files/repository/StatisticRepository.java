package telegram.files.repository;

import io.vertx.core.Future;

import java.util.List;

public interface StatisticRepository {
    Future<Void> create(StatisticRecord record);

    Future<List<StatisticRecord>> getRangeStatistics(StatisticRecord.Type type,
                                                     long relatedId,
                                                     long startTime,
                                                     long endTime);

    Future<Void> deleteOlderThan(StatisticRecord.Type type, long cutoffTimestamp);
}

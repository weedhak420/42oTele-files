package telegram.files.repository.impl;

import cn.hutool.core.collection.IterUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.cache.CacheProvider;
import telegram.files.repository.StatisticRecord;
import telegram.files.repository.StatisticRepository;

import java.util.List;
import java.util.Map;

public class StatisticRepositoryImpl extends AbstractSqlRepository implements StatisticRepository {

    private static final Log log = LogFactory.get();
    private final Cache<String, Object> statisticsCache = CacheProvider.statisticsCache();

    public StatisticRepositoryImpl(SqlClient sqlClient) {
        super(sqlClient);
    }

    @Override
    public Future<Void> create(StatisticRecord record) {
        return SqlTemplate
                .forUpdate(sqlClient, """
                        INSERT INTO statistic_record(related_id, type, timestamp, data)
                        VALUES (#{related_id}, #{type}, #{timestamp}, #{data})
                        """)
                .mapFrom(StatisticRecord.PARAM_MAPPER)
                .execute(record)
                .onSuccess(r -> log.trace("Successfully created statistic record: %s".formatted(record.relatedId())))
                .onFailure(
                        err -> log.error("Failed to create statistic record: %s".formatted(err.getMessage()))
                )
                .onSuccess(r -> statisticsCache.invalidate(cachePrefix(record.type(), record.relatedId())))
                .mapEmpty();
    }

    @Override
    public Future<List<StatisticRecord>> getRangeStatistics(StatisticRecord.Type type,
                                                            long relatedId,
                                                            long startTime,
                                                            long endTime) {
        String cacheKey = cacheKey(type, relatedId, startTime, endTime);
        Object cached = statisticsCache.getIfPresent(cacheKey);
        if (cached instanceof List<?> cachedList) {
            return Future.succeededFuture((List<StatisticRecord>) cachedList);
        }
        return SqlTemplate
                .forQuery(sqlClient, """
                        SELECT *
                        FROM statistic_record
                        WHERE type = #{type}
                          AND related_id = #{relatedId}
                          AND timestamp >= #{startTime}
                          AND timestamp <= #{endTime}
                        ORDER BY timestamp
                        """)
                .mapTo(StatisticRecord.ROW_MAPPER)
                .execute(Map.of(
                        "type", type.name(),
                        "relatedId", Convert.toStr(relatedId),
                        "startTime", startTime,
                        "endTime", endTime
                ))
                .map(rows -> {
                    List<StatisticRecord> list = IterUtil.toList(rows);
                    statisticsCache.put(cacheKey, list);
                    return list;
                })
                .onFailure(
                        err -> log.error("Failed to get range statistics: %s".formatted(err.getMessage()))
                );
    }

    private String cacheKey(StatisticRecord.Type type, long relatedId, long startTime, long endTime) {
        return cachePrefix(type, relatedId) + startTime + ":" + endTime;
    }

    private String cachePrefix(StatisticRecord.Type type, long relatedId) {
        return "%s:%s:".formatted(type.name(), relatedId);
    }
}

package telegram.files.repository.impl;

import cn.hutool.core.collection.IterUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.repository.StatisticRecord;
import telegram.files.repository.StatisticRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StatisticRepositoryImpl extends AbstractSqlRepository implements StatisticRepository {

    private static final Log log = LogFactory.get();
    private final List<StatisticRecord> buffer = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean bufferingEnabled = false;

    public StatisticRepositoryImpl(SqlClient sqlClient) {
        super(sqlClient);
    }

    @Override
    public Future<Void> create(StatisticRecord record) {
        if (bufferingEnabled) {
            buffer.add(record);
            return Future.succeededFuture();
        }
        return doInsert(record);
    }

    @Override
    public Future<Void> batchCreate(List<StatisticRecord> records) {
        if (records == null || records.isEmpty()) {
            return Future.succeededFuture();
        }
        return SqlTemplate
                .forUpdate(sqlClient, """
                        INSERT INTO statistic_record(related_id, type, timestamp, data)
                        VALUES (#{related_id}, #{type}, #{timestamp}, #{data})
                        """)
                .mapFrom(StatisticRecord.PARAM_MAPPER)
                .executeBatch(records)
                .onSuccess(r -> log.trace("Batch inserted %d statistic records".formatted(records.size())))
                .onFailure(err -> log.error("Failed to batch insert statistics: %s".formatted(err.getMessage())))
                .mapEmpty();
    }

    @Override
    public void startBufferedWriter(Vertx vertx) {
        bufferingEnabled = true;
        vertx.setPeriodic(Duration.ofMinutes(1).toMillis(), id -> flushBuffer());
    }

    private Future<Void> doInsert(StatisticRecord record) {
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
                .mapEmpty();
    }

    private Future<Void> flushBuffer() {
        List<StatisticRecord> snapshot;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return Future.succeededFuture();
            }
            snapshot = List.copyOf(buffer);
            buffer.clear();
        }
        return batchCreate(snapshot);
    }

    @Override
    public Future<List<StatisticRecord>> getRangeStatistics(StatisticRecord.Type type,
                                                            long relatedId,
                                                            long startTime,
                                                            long endTime) {
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
                .map(IterUtil::toList)
                .onFailure(
                        err -> log.error("Failed to get range statistics: %s".formatted(err.getMessage()))
                );
    }

    @Override
    public Future<Void> deleteOlderThan(StatisticRecord.Type type, long cutoffTimestamp) {
        return SqlTemplate
                .forUpdate(sqlClient, """
                        DELETE
                        FROM statistic_record
                        WHERE type = #{type}
                          AND timestamp < #{cutoff}
                        """)
                .execute(Map.of(
                        "type", type.name(),
                        "cutoff", cutoffTimestamp
                ))
                .onFailure(err -> log.error("Failed to cleanup statistic records: %s".formatted(err.getMessage())))
                .mapEmpty();
    }
}

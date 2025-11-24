package telegram.files.repository.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.IterUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.repository.ConfigurationHistoryRecord;
import telegram.files.repository.ConfigurationHistoryRepository;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConfigurationHistoryRepositoryImpl extends AbstractSqlRepository implements ConfigurationHistoryRepository {

    private static final Log log = LogFactory.get();

    public ConfigurationHistoryRepositoryImpl(SqlClient sqlClient) {
        super(sqlClient);
    }

    @Override
    public Future<Void> append(ConfigurationHistoryRecord record) {
        return SqlTemplate
                .forUpdate(sqlClient, """
                        INSERT INTO configuration_history_record(category, config_key, user, old_value, new_value, timestamp)
                        VALUES (#{category}, #{config_key}, #{user}, #{old_value}, #{new_value}, #{timestamp})
                        """)
                .mapFrom(ConfigurationHistoryRecord.PARAM_MAPPER)
                .execute(record)
                // Normalize to a void future before failure handling
                .mapEmpty()
                .onSuccess(r -> log.trace("Added configuration history for %s/%s".formatted(record.category(), record.key())))
                .onFailure(err -> log.error("Failed to add configuration history: %s".formatted(err.getMessage())));
    }

    @Override
    public Future<List<ConfigurationHistoryRecord>> list(int limit) {
        String query = """
                SELECT * FROM configuration_history_record
                ORDER BY timestamp DESC
                LIMIT #{limit}
                """;
        return SqlTemplate
                .forQuery(sqlClient, query)
                .mapTo(ConfigurationHistoryRecord.ROW_MAPPER)
                .execute(Collections.singletonMap("limit", limit))
                .map(IterUtil::toList)
                .onSuccess(r -> log.trace("Fetched %s configuration history entries".formatted(r.size())))
                .onFailure(err -> log.error("Failed to fetch configuration history: %s".formatted(err.getMessage())));
    }

    @Override
    public Future<ConfigurationHistoryRecord> getLastChange(String category, String key) {
        String query = """
                SELECT * FROM configuration_history_record
                WHERE category = #{category} AND config_key = #{config_key}
                ORDER BY timestamp DESC
                LIMIT 1
                """;
        return SqlTemplate
                .forQuery(sqlClient, query)
                .mapTo(ConfigurationHistoryRecord.ROW_MAPPER)
                .execute(Map.of("category", category, "config_key", key))
                .map(rs -> CollUtil.isEmpty(rs) ? null : rs.iterator().next())
                .onFailure(err -> log.error("Failed to fetch last configuration change: %s".formatted(err.getMessage())));
    }
}

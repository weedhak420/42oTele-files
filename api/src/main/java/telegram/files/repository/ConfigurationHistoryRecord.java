package telegram.files.repository;

import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;
import telegram.files.Config;

import java.util.Map;

public record ConfigurationHistoryRecord(Long id,
                                          String category,
                                          String key,
                                          String user,
                                          String oldValue,
                                          String newValue,
                                          long timestamp) {

    private static final String KEY_FIELD = Config.isMysql() ? "`config_key`" : "config_key";

    private static final String ID_FIELD = Config.isMysql() ? "id BIGINT PRIMARY KEY AUTO_INCREMENT" :
            Config.isPostgres() ? "id BIGSERIAL PRIMARY KEY" : "id INTEGER PRIMARY KEY AUTOINCREMENT";

    public static final String SCHEME = """
            CREATE TABLE IF NOT EXISTS configuration_history_record
            (
                %s,
                category    VARCHAR(128) NOT NULL,
                %s          VARCHAR(255) NOT NULL,
                user        VARCHAR(255),
                old_value   TEXT,
                new_value   TEXT,
                timestamp   BIGINT NOT NULL
            )
            """.formatted(ID_FIELD, KEY_FIELD);

    public static class ConfigurationHistoryDefinition implements Definition {
        @Override
        public String getScheme() {
            return SCHEME;
        }
    }

    public static final RowMapper<ConfigurationHistoryRecord> ROW_MAPPER = row -> new ConfigurationHistoryRecord(
            row.getLong("id"),
            row.getString("category"),
            row.getString("config_key"),
            row.getString("user"),
            row.getString("old_value"),
            row.getString("new_value"),
            row.getLong("timestamp"));

    public static final TupleMapper<ConfigurationHistoryRecord> PARAM_MAPPER = TupleMapper.mapper(record -> Map.ofEntries(
            Map.entry("id", record.id()),
            Map.entry("category", record.category()),
            Map.entry("config_key", record.key()),
            Map.entry("user", record.user()),
            Map.entry("old_value", record.oldValue()),
            Map.entry("new_value", record.newValue()),
            Map.entry("timestamp", record.timestamp())
    ));
}

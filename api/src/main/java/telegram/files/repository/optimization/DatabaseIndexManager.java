package telegram.files.repository.optimization;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ensures frequently queried columns are indexed to avoid full scans.
 */
public class DatabaseIndexManager {

    private static final Log log = LogFactory.get();
    private final SqlClient sqlClient;

    public DatabaseIndexManager(SqlClient sqlClient) {
        this.sqlClient = sqlClient;
    }

    public Future<Void> ensureIndexes() {
        List<Future<Void>> futures = List.of(
                ensureIndex("idx_file_record_chat_id", "file_record", "chat_id"),
                ensureIndex("idx_file_record_message_id", "file_record", "message_id"),
                ensureIndex("idx_file_record_download_status", "file_record", "download_status"),
                ensureIndex("idx_file_record_start_date", "file_record", "start_date"),
                ensureIndex("idx_file_record_chat_download", "file_record", "chat_id, download_status")
        );
        return CompositeFuture.all(futures)
                .mapEmpty();
    }

    private Future<Void> ensureIndex(String name, String table, String columns) {
        if (Config.isMysql()) {
            return ensureIndexMySql(name, table, columns);
        }
        String createStatement = "CREATE INDEX IF NOT EXISTS %s ON %s (%s)".formatted(name, table, columns);
        return sqlClient
                .query(createStatement)
                .execute()
                .mapEmpty()
                .onSuccess(r -> log.debug("Index ensured: %s".formatted(name)))
                .onFailure(err -> log.error("Failed to ensure index %s: %s".formatted(name, err.getMessage())));
    }

    private Future<Void> ensureIndexMySql(String name, String table, String columns) {
        String existsQuery = """
                SELECT COUNT(1) as cnt
                FROM information_schema.statistics
                WHERE table_schema = #{schema}
                  AND table_name = #{table}
                  AND index_name = #{name}
                """;
        Map<String, Object> params = new HashMap<>();
        params.put("schema", Config.DB_NAME);
        params.put("table", table);
        params.put("name", name);
        return SqlTemplate
                .forQuery(sqlClient, existsQuery)
                .mapTo(row -> row.getLong("cnt"))
                .execute(params)
                .map(CollUtil::getFirst)
                .compose(count -> {
                    if (count != null && count > 0) {
                        return Future.succeededFuture();
                    }
                    String createStatement = "CREATE INDEX %s ON %s (%s)".formatted(name, table, columns);
                    return sqlClient.query(createStatement)
                            .execute()
                            .mapEmpty();
                })
                .onSuccess(r -> log.debug("Index ensured: %s".formatted(name)))
                .onFailure(err -> log.error("Failed to ensure index %s: %s".formatted(name, err.getMessage())));
    }
}

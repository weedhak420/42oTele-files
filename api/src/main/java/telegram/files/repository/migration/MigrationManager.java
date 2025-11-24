package telegram.files.repository.migration;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.repository.SchemaVersionRecord;

import java.util.*;
import java.util.stream.Collectors;

public class MigrationManager {

    private static final Log log = LogFactory.get();

    private static final List<MigrationScript> SCRIPTS = List.of(
            new MigrationScript(
                    "0.2.5-indexes",
                    "Add composite indexes for download lookups",
                    new String[]{
                            "CREATE INDEX IF NOT EXISTS idx_file_record_tg_status_chat ON file_record(telegram_id, download_status, chat_id);",
                            "CREATE INDEX IF NOT EXISTS idx_file_record_unique_status ON file_record(unique_id, download_status);"
                    },
                    new String[]{
                            "DROP INDEX IF EXISTS idx_file_record_tg_status_chat;",
                            "DROP INDEX IF EXISTS idx_file_record_unique_status;"
                    }
            )
    );

    public static Future<Void> applyMigrations(Pool pool) {
        return pool.query(SchemaVersionRecord.SCHEME)
                .execute()
                .compose(v -> pool.query("SELECT version FROM schema_version")
                        .execute()
                        .map(rs -> rs.stream().map(row -> row.getString("version")).collect(Collectors.toSet())))
                .compose(applied -> sequentialApply(pool, applied))
                .onSuccess(v -> log.info("Database migrations applied"))
                .onFailure(err -> log.error("Database migration failed", err))
                .mapEmpty();
    }

    public static Future<Void> rollbackTo(Pool pool, String targetVersion) {
        Set<String> applied = SCRIPTS.stream()
                .map(MigrationScript::version)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<MigrationScript> toRollback = new ArrayList<>();
        for (int i = SCRIPTS.size() - 1; i >= 0; i--) {
            MigrationScript script = SCRIPTS.get(i);
            if (script.version().equals(targetVersion)) {
                break;
            }
            toRollback.add(script);
        }
        Future<Void> chain = Future.succeededFuture();
        for (MigrationScript script : toRollback) {
            chain = chain.compose(v -> applySqlBatch(pool, Arrays.asList(script.downSql()))
                    .compose(v2 -> removeHistory(pool, script)));
        }
        return chain;
    }

    private static Future<Void> sequentialApply(Pool pool, Set<String> appliedVersions) {
        Future<Void> chain = Future.succeededFuture();
        for (MigrationScript script : SCRIPTS) {
            if (appliedVersions.contains(script.version())) {
                continue;
            }
            chain = chain.compose(v -> applySqlBatch(pool, Arrays.asList(script.upSql()))
                    .compose(v2 -> insertHistory(pool, script)));
        }
        return chain;
    }

    private static Future<Void> applySqlBatch(Pool pool, List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return Future.succeededFuture();
        }
        Future<Void> chain = Future.succeededFuture();
        for (String sql : sqls) {
            chain = chain.compose(v -> pool.query(sql).execute().mapEmpty());
        }
        return chain;
    }

    private static Future<Void> insertHistory(Pool pool, MigrationScript script) {
        return SqlTemplate
                .forUpdate(pool, """
                        INSERT INTO schema_version(version, description, applied_at)
                        VALUES (#{version}, #{description}, #{applied_at})
                        """)
                .mapFrom(SchemaVersionRecord.PARAM_MAPPER)
                .execute(new SchemaVersionRecord(script.version(), script.description(), System.currentTimeMillis()))
                .mapEmpty();
    }

    private static Future<Void> removeHistory(Pool pool, MigrationScript script) {
        return SqlTemplate
                .forUpdate(pool, """
                        DELETE FROM schema_version WHERE version = #{version}
                        """)
                .execute(Map.of("version", script.version()))
                .mapEmpty();
    }
}

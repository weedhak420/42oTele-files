package telegram.files;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import telegram.files.repository.FileRepository;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseMaintenanceService {

    private static final Log log = LogFactory.get();
    private final Pool pool;
    private final FileRepository fileRepository;
    private final Vertx vertx;
    private final AtomicLong lastVacuum = new AtomicLong(0);

    public DatabaseMaintenanceService(Pool pool, FileRepository fileRepository, Vertx vertx) {
        this.pool = pool;
        this.fileRepository = fileRepository;
        this.vertx = vertx;
    }

    public void start() {
        vertx.setPeriodic(Duration.ofHours(6).toMillis(), id -> runVacuum());
        vertx.setPeriodic(Duration.ofHours(1).toMillis(), id -> cleanupOrphans());
        vertx.setPeriodic(Duration.ofHours(24).toMillis(), id -> backupDatabase());
    }

    public long getLastVacuum() {
        return lastVacuum.get();
    }

    public long getDatabaseSize() {
        File dbFile = new File(DataVerticle.getDataPath());
        return dbFile.exists() ? dbFile.length() : 0L;
    }

    private void runVacuum() {
        if (!Config.isSqlite()) {
            return;
        }
        pool.query("VACUUM;")
                .execute()
                .compose(v -> pool.query("ANALYZE;").execute())
                .onSuccess(v -> {
                    lastVacuum.set(System.currentTimeMillis());
                    log.info("SQLite VACUUM/ANALYZE completed");
                })
                .onFailure(err -> log.warn("VACUUM failed", err));
    }

    private void cleanupOrphans() {
        fileRepository.deleteOrphanedRecords()
                .onFailure(err -> log.warn("Orphan cleanup failed", err));
    }

    private void backupDatabase() {
        if (!Config.isSqlite()) {
            return;
        }
        String source = DataVerticle.getDataPath();
        File dbFile = new File(source);
        if (!dbFile.exists()) {
            return;
        }
        File backup = new File(source + ".bak");
        vertx.fileSystem()
                .copy(source, backup.getAbsolutePath())
                .onSuccess(v -> log.info("Database backup refreshed"))
                .onFailure(err -> log.warn("Database backup failed", err));
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("sizeBytes", getDatabaseSize())
                .put("lastVacuumMs", lastVacuum.get());
    }
}

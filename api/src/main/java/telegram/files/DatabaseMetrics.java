package telegram.files;

import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;

public record DatabaseMetrics(int active, int idle, int maxPoolSize, long sizeBytes, long lastVacuumMs) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("active", active)
                .put("idle", idle)
                .put("maxPoolSize", maxPoolSize)
                .put("sizeBytes", sizeBytes)
                .put("lastVacuumMs", lastVacuumMs);
    }

    public static DatabaseMetrics from(HikariDataSource dataSource, Pool pool, long sizeBytes, long lastVacuum) {
        int maxSize = pool instanceof io.vertx.sqlclient.impl.PoolImpl impl ? impl.options().getMaxSize() : Config.DB_POOL_SIZE;
        int activeConnections = dataSource != null ? dataSource.getHikariPoolMXBean().getActiveConnections() : 0;
        int idleConnections = dataSource != null ? dataSource.getHikariPoolMXBean().getIdleConnections() : 0;
        return new DatabaseMetrics(activeConnections, idleConnections, maxSize, sizeBytes, lastVacuum);
    }
}

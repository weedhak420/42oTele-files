package telegram.files;

import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonObject;

public record DatabaseMetrics(int active, int idle, int maxPoolSize, long sizeBytes, long lastVacuumMs) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("active", active)
                .put("idle", idle)
                .put("maxPoolSize", maxPoolSize)
                .put("sizeBytes", sizeBytes)
                .put("lastVacuumMs", lastVacuumMs);
    }

    public static DatabaseMetrics from(HikariDataSource dataSource, int configuredPoolSize, long sizeBytes, long lastVacuum) {
        int maxSize = configuredPoolSize;
        int activeConnections = dataSource != null ? dataSource.getHikariPoolMXBean().getActiveConnections() : 0;
        int idleConnections = dataSource != null ? dataSource.getHikariPoolMXBean().getIdleConnections() : 0;
        return new DatabaseMetrics(activeConnections, idleConnections, maxSize, sizeBytes, lastVacuum);
    }
}

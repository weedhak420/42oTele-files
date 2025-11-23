package telegram.files.repository.optimization;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages database indexes for performance optimization.
 */
public class DatabaseIndexManager {
    private static final Logger log = LoggerFactory.getLogger(DatabaseIndexManager.class);
    
    private final SqlClient sqlClient;
    
    public DatabaseIndexManager(SqlClient sqlClient) {
        this.sqlClient = sqlClient;
    }
    
    /**
     * Ensures all required indexes exist in the database.
     * 
     * @return Future that completes when all indexes are created
     */
    public Future<Void> ensureAllIndexes() {
        List<Future<Void>> futures = new ArrayList<>();
        
        // Files table indexes
        futures.add(ensureIndex("idx_file_chat_id", "files", "chat_id"));
        futures.add(ensureIndex("idx_file_status", "files", "status"));
        futures.add(ensureIndex("idx_file_created_at", "files", "created_at"));
        futures.add(ensureIndex("idx_file_type", "files", "file_type"));
        futures.add(ensureCompositeIndex("idx_file_chat_status", "files", "chat_id", "status"));
        
        // FIX: Use CompositeFuture.all(List) instead of array
        return CompositeFuture.all(new ArrayList<>(futures))
                .mapEmpty();
    }
    
    /**
     * Creates a single-column index if it doesn't exist.
     */
    private Future<Void> ensureIndex(String name, String table, String column) {
        String sql = "CREATE INDEX IF NOT EXISTS %s ON %s(%s)".formatted(name, table, column);
        
        // FIX: Add .mapEmpty() to convert Future<RowSet> to Future<Void>
        return sqlClient.query(sql)
                .execute()
                .mapEmpty()
                .onSuccess(v -> log.info("Index {} ensured", name))
                .onFailure(err -> log.error("Failed to ensure index {}: {}", name, err.getMessage()));
    }
    
    /**
     * Creates a composite (multi-column) index if it doesn't exist.
     */
    private Future<Void> ensureCompositeIndex(String name, String table, String... columns) {
        String columnList = String.join(", ", columns);
        String sql = "CREATE INDEX IF NOT EXISTS %s ON %s(%s)".formatted(name, table, columnList);
        
        // FIX: Add .mapEmpty() to convert Future<RowSet> to Future<Void>
        return sqlClient.query(sql)
                .execute()
                .mapEmpty()
                .onSuccess(v -> log.info("Composite index {} ensured", name))
                .onFailure(err -> log.error("Failed to ensure index {}: {}", name, err.getMessage()));
    }
}

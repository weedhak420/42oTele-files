package telegram.files.repository;

import io.vertx.core.Future;

import java.util.List;

public interface ConfigurationHistoryRepository {

    Future<Void> append(ConfigurationHistoryRecord record);

    Future<List<ConfigurationHistoryRecord>> list(int limit);

    Future<ConfigurationHistoryRecord> getLastChange(String category, String key);
}

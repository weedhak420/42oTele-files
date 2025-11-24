package telegram.files.service;

import io.vertx.core.Future;
import org.jooq.lambda.tuple.Tuple3;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepositoryFacade;
import telegram.files.repository.segmented.FileCommandRepository;
import telegram.files.repository.segmented.FileQueryRepository;
import telegram.files.repository.segmented.FileSearchRepository;
import telegram.files.repository.segmented.FileStatisticsRepository;

import java.util.List;
import java.util.Map;

/**
 * Business logic facade for file operations. Delegates to segmented
 * repositories to keep orchestration logic isolated from raw persistence
 * concerns.
 */
public class FileService {

    private final FileQueryRepository queryRepository;
    private final FileCommandRepository commandRepository;
    private final FileStatisticsRepository statisticsRepository;
    private final FileSearchRepository searchRepository;

    public FileService(FileRepositoryFacade facade) {
        this.queryRepository = facade.queries();
        this.commandRepository = facade.commands();
        this.statisticsRepository = facade.statistics();
        this.searchRepository = facade.search();
    }

    public Future<FileRecord> createFile(FileRecord record) {
        return commandRepository.create(record);
    }

    public Future<Boolean> createIfMissing(FileRecord record) {
        return commandRepository.createIfNotExist(record);
    }

    public Future<Tuple3<List<FileRecord>, Long, Long>> listFiles(long chatId, Map<String, String> filter) {
        return queryRepository.getFiles(chatId, filter);
    }

    public Future<Tuple3<List<FileRecord>, Long, Long>> search(long chatId, Map<String, String> filter) {
        return searchRepository.search(chatId, filter);
    }

    public Future<Void> updateTags(String uniqueId, String tags) {
        return commandRepository.updateTags(uniqueId, tags);
    }

    public Future<FileRecord> getByUniqueId(String uniqueId) {
        return queryRepository.getByUniqueId(uniqueId);
    }

    public Future<io.vertx.core.json.JsonObject> getDownloadStatistics(long telegramId) {
        return statisticsRepository.getDownloadStatistics(telegramId);
    }
}

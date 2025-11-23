package telegram.files.repository.segmented;

import io.vertx.core.Future;
import org.jooq.lambda.tuple.Tuple3;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepository;

import java.util.List;
import java.util.Map;

/**
 * Read-only access to {@link FileRecord} data.
 * This adapter delegates to the existing {@link FileRepository} while
 * preparing the codebase for a more granular repository split.
 */
public class FileQueryRepository {

    private final FileRepository delegate;

    public FileQueryRepository(FileRepository delegate) {
        this.delegate = delegate;
    }

    public Future<Tuple3<List<FileRecord>, Long, Long>> getFiles(long chatId, Map<String, String> filter) {
        return delegate.getFiles(chatId, filter);
    }

    public Future<Map<String, FileRecord>> getFilesByUniqueId(List<String> uniqueIds) {
        return delegate.getFilesByUniqueId(uniqueIds);
    }

    public Future<FileRecord> getByPrimaryKey(int fileId, String uniqueId) {
        return delegate.getByPrimaryKey(fileId, uniqueId);
    }

    public Future<FileRecord> getByUniqueId(String uniqueId) {
        return delegate.getByUniqueId(uniqueId);
    }

    public Future<FileRecord> getMainFileByThread(long telegramId, long threadChatId, long messageThreadId) {
        return delegate.getMainFileByThread(telegramId, threadChatId, messageThreadId);
    }

    public Future<String> getCaptionByMediaAlbumId(long mediaAlbumId) {
        return delegate.getCaptionByMediaAlbumId(mediaAlbumId);
    }

    public Future<Long> getReactionCountByMediaAlbumId(long mediaAlbumId) {
        return delegate.getReactionCountByMediaAlbumId(mediaAlbumId);
    }
}

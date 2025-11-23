package telegram.files.repository.segmented;

import io.vertx.core.Future;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepository;

/**
 * Handles create/update/delete operations for {@link FileRecord} entries.
 */
public class FileCommandRepository {

    private final FileRepository delegate;

    public FileCommandRepository(FileRepository delegate) {
        this.delegate = delegate;
    }

    public Future<FileRecord> create(FileRecord fileRecord) {
        return delegate.create(fileRecord);
    }

    public Future<Boolean> createIfNotExist(FileRecord fileRecord) {
        return delegate.createIfNotExist(fileRecord);
    }

    public Future<Void> updateTags(String uniqueId, String tags) {
        return delegate.updateTags(uniqueId, tags);
    }

    public Future<Void> deleteByUniqueId(String uniqueId) {
        return delegate.deleteByUniqueId(uniqueId);
    }

    public Future<Void> updateFileId(int fileId, String uniqueId) {
        return delegate.updateFileId(fileId, uniqueId);
    }

    public Future<Integer> updateAlbumDataByMediaAlbumId(long mediaAlbumId, String caption, long reactionCount) {
        return delegate.updateAlbumDataByMediaAlbumId(mediaAlbumId, caption, reactionCount);
    }

    public Future<io.vertx.core.json.JsonObject> updateDownloadStatus(int fileId,
                                                          String uniqueId,
                                                          String localPath,
                                                          FileRecord.DownloadStatus downloadStatus,
                                                          Long completionDate) {
        return delegate.updateDownloadStatus(fileId, uniqueId, localPath, downloadStatus, completionDate);
    }

    public Future<io.vertx.core.json.JsonObject> updateTransferStatus(String uniqueId,
                                                          FileRecord.TransferStatus transferStatus,
                                                          String localPath) {
        return delegate.updateTransferStatus(uniqueId, transferStatus, localPath);
    }
}

package telegram.files.repository.segmented;

import io.vertx.core.Future;
import org.jooq.lambda.tuple.Tuple3;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepository;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates advanced search and filtering use-cases for files.
 * Currently delegates to {@link FileRepository#getFiles(long, Map)} while the
 * richer search surface is being extracted.
 */
public class FileSearchRepository {

    private final FileRepository delegate;

    public FileSearchRepository(FileRepository delegate) {
        this.delegate = delegate;
    }

    public Future<Tuple3<List<FileRecord>, Long, Long>> search(long chatId, Map<String, String> filters) {
        return delegate.getFiles(chatId, filters);
    }
}

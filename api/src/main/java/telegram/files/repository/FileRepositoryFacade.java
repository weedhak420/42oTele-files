package telegram.files.repository;

import io.vertx.sqlclient.SqlClient;
import telegram.files.repository.impl.FileRepositoryImpl;
import telegram.files.repository.segmented.FileCommandRepository;
import telegram.files.repository.segmented.FileQueryRepository;
import telegram.files.repository.segmented.FileSearchRepository;
import telegram.files.repository.segmented.FileStatisticsRepository;

/**
 * Facade entry point that exposes specialized repositories while relying on the
 * existing {@link FileRepositoryImpl} for implementation. This keeps backward
 * compatibility with the previous monolithic repository while paving the way
 * for a cleaner split.
 */
public class FileRepositoryFacade {

    private final FileQueryRepository queryRepository;
    private final FileCommandRepository commandRepository;
    private final FileStatisticsRepository statisticsRepository;
    private final FileSearchRepository searchRepository;

    public FileRepositoryFacade(FileRepository baseRepository) {
        this.queryRepository = new FileQueryRepository(baseRepository);
        this.commandRepository = new FileCommandRepository(baseRepository);
        this.statisticsRepository = new FileStatisticsRepository(baseRepository);
        this.searchRepository = new FileSearchRepository(baseRepository);
    }

    public static FileRepositoryFacade create(SqlClient sqlClient) {
        return new FileRepositoryFacade(new FileRepositoryImpl(sqlClient));
    }

    public FileQueryRepository queries() {
        return queryRepository;
    }

    public FileCommandRepository commands() {
        return commandRepository;
    }

    public FileStatisticsRepository statistics() {
        return statisticsRepository;
    }

    public FileSearchRepository search() {
        return searchRepository;
    }
}

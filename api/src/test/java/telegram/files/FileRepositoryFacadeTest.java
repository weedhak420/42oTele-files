package telegram.files;

import io.vertx.core.Future;
import org.jooq.lambda.tuple.Tuple3;
import org.junit.jupiter.api.Test;
import telegram.files.repository.FileRecord;
import telegram.files.repository.FileRepository;
import telegram.files.repository.FileRepositoryFacade;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class FileRepositoryFacadeTest {

    @Test
    void delegatesThroughSegmentedAdapters() {
        FileRepository delegate = mock(FileRepository.class);
        FileRepositoryFacade facade = new FileRepositoryFacade(delegate);

        Map<String, String> filters = Map.of("type", "video");
        Tuple3<List<FileRecord>, Long, Long> tuple = new Tuple3<>(List.of(), 0L, 0L);
        when(delegate.getFiles(eq(1L), eq(filters))).thenReturn(Future.succeededFuture(tuple));

        FileRecord record = sampleRecord();
        when(delegate.create(record)).thenReturn(Future.succeededFuture(record));
        when(delegate.getDownloadStatistics(1L)).thenReturn(Future.succeededFuture(new io.vertx.core.json.JsonObject()));

        assertEquals(tuple, facade.queries().getFiles(1L, filters).result());
        assertEquals(record, facade.commands().create(record).result());
        assertNotNull(facade.statistics().getDownloadStatistics(1L).result());

        verify(delegate).getFiles(1L, filters);
        verify(delegate).create(record);
        verify(delegate).getDownloadStatistics(1L);
    }

    private FileRecord sampleRecord() {
        return new FileRecord(
                1,
                "unique",
                100L,
                200L,
                300L,
                0L,
                0,
                false,
                1024L,
                0L,
                "file",
                "text/plain",
                "name.txt",
                "thumb",
                "thumb-uid",
                "caption",
                "extra",
                "/tmp/name.txt",
                "idle",
                "idle",
                0L,
                null,
                "",
                0L,
                0L,
                0L
        );
    }
}

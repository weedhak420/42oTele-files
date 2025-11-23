package telegram.files;

import org.junit.jupiter.api.Test;
import telegram.files.autodownload.DownloadStateManager;
import telegram.files.autodownload.MessageWrapper;
import telegram.files.autodownload.WaitingScanThread;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DownloadStateManagerTest {

    @Test
    void addDrainAndRegisterQueues() {
        DownloadStateManager manager = new DownloadStateManager();
        MessageWrapper first = new MessageWrapper(null, false);
        MessageWrapper second = new MessageWrapper(null, true);

        assertTrue(manager.addWaitingMessages(1L, List.of(first), false, 2));
        assertFalse(manager.addWaitingMessages(1L, List.of(second), false, 0),
                "Should respect max queue size when not forced");

        assertTrue(manager.addWaitingMessages(1L, List.of(second), true, 0),
                "Force flag should bypass size restriction");
        assertEquals(2, manager.getWaitingMessages(1L).size());

        List<MessageWrapper> drained = manager.drainForDownload(1L, 2);
        assertEquals(List.of(first, second), drained, "Messages should drain in FIFO order");
        assertTrue(manager.getWaitingMessages(1L).isEmpty(), "Queue should be empty after drain");

        WaitingScanThread thread = new WaitingScanThread(1L, 2L, 3L);
        manager.registerWaitingThread(1L, thread);
        assertEquals(1, manager.getWaitingScanThreads().get(1L).size(),
                "Thread queue should accept registrations");
    }
}

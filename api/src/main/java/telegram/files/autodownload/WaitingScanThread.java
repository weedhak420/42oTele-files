package telegram.files.autodownload;

/**
 * Tracks deferred scanning work for threaded discussions that should be
 * processed after the primary download has started.
 */
public class WaitingScanThread {
    public long telegramId;
    public long threadChatId;
    public long messageThreadId;
    public String nextFileType;
    public long nextFromMessageId;
    public boolean isComplete;

    public WaitingScanThread(long telegramId, long threadChatId, long messageThreadId) {
        this.telegramId = telegramId;
        this.threadChatId = threadChatId;
        this.messageThreadId = messageThreadId;
    }
}

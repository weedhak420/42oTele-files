package telegram.files.autodownload;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * Centralizes shared download state so AutoDownloadVerticle can remain a
 * coordinator. The manager owns waiting queues and exposes small helpers for
 * adding and draining pending messages per Telegram account.
 */
public class DownloadStateManager {

    private static final Log log = LogFactory.get();

    private final Map<Long, LinkedList<MessageWrapper>> waitingDownloadMessages = new ConcurrentHashMap<>();
    private final Map<Long, LinkedList<WaitingScanThread>> waitingScanThreads = new ConcurrentHashMap<>();

    public Map<Long, LinkedList<MessageWrapper>> getWaitingDownloadMessages() {
        return waitingDownloadMessages;
    }

    /**
     * Returns the current waiting queue for a Telegram account without mutating it.
     *
     * @param telegramId the account identifier
     * @return waiting messages or an empty list when none are tracked
     */
    public LinkedList<MessageWrapper> getWaitingMessages(long telegramId) {
        return waitingDownloadMessages.getOrDefault(telegramId, new LinkedList<>());
    }

    public Map<Long, LinkedList<WaitingScanThread>> getWaitingScanThreads() {
        return waitingScanThreads;
    }

    /**
     * Adds messages to the per-account queue when capacity allows.
     *
     * @param telegramId     account identifier
     * @param messages       messages to append
     * @param force          bypass queue length checks when true
     * @param maxQueueLength maximum queue size when not forcing
     * @return {@code true} if messages were added; {@code false} when rejected
     */
    public boolean addWaitingMessages(long telegramId,
                                       List<MessageWrapper> messages,
                                       boolean force,
                                       int maxQueueLength) {
        if (CollUtil.isEmpty(messages)) {
            return false;
        }
        LinkedList<MessageWrapper> waitingMessages = waitingDownloadMessages.get(telegramId);
        if (waitingMessages == null) {
            waitingMessages = new LinkedList<>();
        }
        if (!force && waitingMessages.size() > maxQueueLength) {
            return false;
        }
        waitingMessages.addAll(messages);
        waitingDownloadMessages.put(telegramId, waitingMessages);
        log.debug("Add waiting download messages: %d".formatted(messages.size()));
        return true;
    }

    /**
     * Removes up to {@code limit} messages from the queue for download
     * execution.
     *
     * @param telegramId account identifier
     * @param limit      maximum number of messages to remove
     * @return drained messages in FIFO order
     */
    public List<MessageWrapper> drainForDownload(long telegramId, int limit) {
        LinkedList<MessageWrapper> messages = waitingDownloadMessages.get(telegramId);
        if (CollUtil.isEmpty(messages) || limit <= 0) {
            return List.of();
        }
        return IntStream.range(0, Math.min(limit, messages.size()))
                .mapToObj(i -> messages.poll())
                .toList();
    }

    /**
     * Tracks discovered thread downloads for later scanning by the verticle.
     *
     * @param telegramId account identifier
     * @param thread     thread metadata to enqueue
     */
    public void registerWaitingThread(long telegramId, WaitingScanThread thread) {
        waitingScanThreads.computeIfAbsent(telegramId, k -> new LinkedList<>()).add(thread);
    }
}

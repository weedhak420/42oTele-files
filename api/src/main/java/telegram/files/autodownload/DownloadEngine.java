package telegram.files.autodownload;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.drinkless.tdlib.TdApi;
import telegram.files.TelegramVerticle;
import telegram.files.repository.FileRecord;

import java.util.List;
import java.util.function.Consumer;

/**
 * Handles the execution aspects of starting downloads against the Telegram
 * client while keeping queue management in {@link DownloadStateManager}.
 */
public class DownloadEngine {

    private static final Log log = LogFactory.get();

    public void startDownloads(TelegramVerticle telegramVerticle,
                               long telegramId,
                               int surplusSize,
                               List<MessageWrapper> pending,
                               Consumer<WaitingScanThread> onThreadDiscovered) {
        if (CollUtil.isEmpty(pending) || surplusSize <= 0) {
            return;
        }

        log.debug("Download start! TelegramId: %d size: %d".formatted(telegramId, pending.size()));
        pending.forEach(messageWrapper -> {
            TdApi.Message message = messageWrapper.message();
            Integer fileId = telegram.files.TdApiHelp.getFileId(message);
            log.debug("Start download file: %s".formatted(fileId));
            telegramVerticle.startDownload(message.chatId, message.id, fileId)
                    .onSuccess(fileRecord -> handleThreadedDownload(telegramId, fileRecord, onThreadDiscovered))
                    .onFailure(e -> log.error("Download file failed! ChatId: %d MessageId:%d FileId:%d".formatted(message.chatId, message.id, fileId), e));
        });
    }

    private void handleThreadedDownload(long telegramId, FileRecord fileRecord, Consumer<WaitingScanThread> onThreadDiscovered) {
        if (fileRecord.threadChatId() != 0 && fileRecord.messageThreadId() != 0 && fileRecord.threadChatId() != fileRecord.chatId()) {
            onThreadDiscovered.accept(new WaitingScanThread(telegramId, fileRecord.threadChatId(), fileRecord.messageThreadId()));
        }
    }
}

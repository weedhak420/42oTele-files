package telegram.files;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.drinkless.tdlib.Client;
import org.drinkless.tdlib.TdApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TelegramUpdateHandler implements Client.ResultHandler {

    private static final Log log = LogFactory.get();

    private Consumer<TdApi.AuthorizationState> onAuthorizationStateUpdated;

    private Consumer<TdApi.UpdateFile> onFileUpdated;

    private Consumer<TdApi.UpdateFileDownloads> onFileDownloadsUpdated;

    private Consumer<TdApi.Object> onChatUpdated;

    private Consumer<TdApi.Message> onMessageReceived;

    private final Map<Integer, FileEventThrottle> fileEventThrottlers = new ConcurrentHashMap<>();

    private static class FileEventThrottle {
        private long lastEventTime;
        private long lastDownloadedSize;
        private static final long MIN_INTERVAL_MS = 1000L;
        private static final long MIN_SIZE_DELTA = 1_000_000L;

        boolean shouldSendEvent(TdApi.File file) {
            long now = System.currentTimeMillis();
            long currentSize = file.local.downloadedSize;

            if (file.local.isDownloadingCompleted || lastEventTime == 0) {
                lastEventTime = now;
                lastDownloadedSize = currentSize;
                return true;
            }

            boolean timeThrottled = (now - lastEventTime) >= MIN_INTERVAL_MS;
            boolean sizeThrottled = (currentSize - lastDownloadedSize) >= MIN_SIZE_DELTA;

            if (timeThrottled || sizeThrottled) {
                lastEventTime = now;
                lastDownloadedSize = currentSize;
                return true;
            }

            return false;
        }
    }

    @Override
    public void onResult(TdApi.Object object) {
        switch (object.getConstructor()) {
            case TdApi.UpdateAuthorizationState.CONSTRUCTOR:
                if (onAuthorizationStateUpdated != null)
                    onAuthorizationStateUpdated.accept(((TdApi.UpdateAuthorizationState) object).authorizationState);
                break;
            case TdApi.UpdateFile.CONSTRUCTOR:
                handleFileUpdated((TdApi.UpdateFile) object);
                break;
            case TdApi.UpdateFileDownload.CONSTRUCTOR:
                log.trace("File download update: %s".formatted(object));
                break;
            case TdApi.UpdateFileDownloads.CONSTRUCTOR:
                if (onFileDownloadsUpdated != null)
                    onFileDownloadsUpdated.accept((TdApi.UpdateFileDownloads) object);
                break;
            case TdApi.UpdateNewMessage.CONSTRUCTOR:
                if (onMessageReceived != null) {
                    onMessageReceived.accept(((TdApi.UpdateNewMessage) object).message);
                }
            case TdApi.UpdateNewChat.CONSTRUCTOR:
            case TdApi.UpdateChatTitle.CONSTRUCTOR:
            case TdApi.UpdateChatPhoto.CONSTRUCTOR:
            case TdApi.UpdateChatReadInbox.CONSTRUCTOR:
            case TdApi.UpdateChatLastMessage.CONSTRUCTOR:
            case TdApi.UpdateChatPosition.CONSTRUCTOR:
                if (onChatUpdated != null) {
                    onChatUpdated.accept(object);
                }
            default:
                log.trace("Unsupported telegram update: %s".formatted(object));
        }
    }

    private void handleFileUpdated(TdApi.UpdateFile update) {
        TdApi.File file = update.file;
        if (file != null && file.local != null && (file.local.isDownloadingActive || file.local.isDownloadingCompleted)) {
            FileEventThrottle throttle = fileEventThrottlers.computeIfAbsent(file.id, id -> new FileEventThrottle());
            boolean shouldSend = throttle.shouldSendEvent(file);

            if (!shouldSend) {
                return;
            }

            if (file.local.isDownloadingCompleted) {
                fileEventThrottlers.remove(file.id);
            }
        }

        if (onFileUpdated != null) {
            onFileUpdated.accept(update);
        }
    }

    public void setOnAuthorizationStateUpdated(Consumer<TdApi.AuthorizationState> onAuthorizationStateUpdated) {
        this.onAuthorizationStateUpdated = onAuthorizationStateUpdated;
    }

    public void setOnFileUpdated(Consumer<TdApi.UpdateFile> onFileUpdated) {
        this.onFileUpdated = onFileUpdated;
    }

    public void setOnFileDownloadsUpdated(Consumer<TdApi.UpdateFileDownloads> onFileDownloadsUpdated) {
        this.onFileDownloadsUpdated = onFileDownloadsUpdated;
    }

    public void setOnChatUpdated(Consumer<TdApi.Object> onChatUpdated) {
        this.onChatUpdated = onChatUpdated;
    }

    public void setOnMessageReceived(Consumer<TdApi.Message> onMessageReceived) {
        this.onMessageReceived = onMessageReceived;
    }
}

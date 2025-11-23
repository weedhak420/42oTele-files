package telegram.files.autodownload;

import org.drinkless.tdlib.TdApi;

/**
 * Lightweight wrapper around a {@link TdApi.Message} that captures whether the
 * message originated from historical backfill or a live update. Separating the
 * record into its own file allows download coordination utilities to share the
 * type without depending on {@link telegram.files.AutoDownloadVerticle} internals.
 */
public record MessageWrapper(TdApi.Message message, boolean isHistorical) {

    public TdApi.Message getMessage() {
        return this.message;
    }
}

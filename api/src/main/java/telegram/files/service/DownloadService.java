package telegram.files.service;

import io.vertx.core.Future;

/**
 * Coordinates download workflows and will later interact with a dedicated
 * download engine extracted from {@code AutoDownloadVerticle}.
 */
public class DownloadService {

    public Future<Void> scheduleDownloads() {
        return Future.succeededFuture();
    }
}

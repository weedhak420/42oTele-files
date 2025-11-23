package telegram.files.autodownload;

import io.vertx.core.Vertx;

import java.util.function.Consumer;

/**
 * Coordinates periodic download triggers. This wrapper makes it easier to unit
 * test scheduling behaviour without relying directly on Vert.x timers.
 */
public class DownloadScheduler {

    public long schedulePeriodic(Vertx vertx, long initialDelay, long periodMs, Consumer<Long> handler) {
        return vertx.setPeriodic(initialDelay, periodMs, handler::accept);
    }

    public void cancel(Vertx vertx, long timerId) {
        vertx.cancelTimer(timerId);
    }
}

package telegram.files.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import telegram.files.cache.CacheProvider;

import java.util.Optional;

/**
 * High level business coordination for Telegram specific workflows.
 * This service will eventually orchestrate authentication, chat navigation and
 * message processing once the responsibilities are extracted from
 * {@code TelegramVerticle}.
 */
public class TelegramService {

    private final Vertx vertx;

    public TelegramService(Vertx vertx) {
        this.vertx = vertx;
    }

    public Future<Void> ensureAuthenticated() {
        // Placeholder implementation until authentication is fully extracted.
        return Future.succeededFuture();
    }

    public Future<Void> cacheSession(String sessionKey, Object sessionPayload) {
        CacheProvider.sessionCache().put(sessionKey, sessionPayload);
        return Future.succeededFuture();
    }

    public <T> Future<Optional<T>> getCachedSession(String sessionKey, Class<T> type) {
        Object cached = CacheProvider.sessionCache().getIfPresent(sessionKey);
        if (cached == null || !type.isInstance(cached)) {
            return Future.succeededFuture(Optional.empty());
        }
        return Future.succeededFuture(Optional.of(type.cast(cached)));
    }
}

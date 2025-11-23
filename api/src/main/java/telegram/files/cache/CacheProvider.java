package telegram.files.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import telegram.files.repository.TelegramRecord;

import java.time.Duration;
import java.util.List;

/**
 * Centralized Caffeine cache factory to keep TTL and sizing consistent across
 * settings, chats, statistics and session caches.
 */
public final class CacheProvider {

    private static final Duration DEFAULT_TTL = Duration.ofMinutes(3);
    private static final long DEFAULT_SIZE = 512;

    private static final Cache<String, Object> SETTINGS_CACHE = newBuilder()
            .maximumSize(DEFAULT_SIZE)
            .build();

    private static final Cache<String, Object> STATISTICS_CACHE = newBuilder()
            .maximumSize(DEFAULT_SIZE)
            .build();

    private static final Cache<Long, List<TelegramRecord>> CHAT_CACHE = newBuilder()
            .maximumSize(64)
            .build();

    private static final Cache<String, Object> SESSION_CACHE = newBuilder()
            .maximumSize(DEFAULT_SIZE)
            .build();

    private CacheProvider() {
    }

    public static Cache<String, Object> settingsCache() {
        return SETTINGS_CACHE;
    }

    public static Cache<String, Object> statisticsCache() {
        return STATISTICS_CACHE;
    }

    public static Cache<Long, List<TelegramRecord>> chatCache() {
        return CHAT_CACHE;
    }

    public static Cache<String, Object> sessionCache() {
        return SESSION_CACHE;
    }

    private static Caffeine<Object, Object> newBuilder() {
        return Caffeine.newBuilder()
                .expireAfterWrite(DEFAULT_TTL);
    }
}

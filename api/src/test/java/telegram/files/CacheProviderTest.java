package telegram.files;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Test;
import telegram.files.cache.CacheProvider;
import telegram.files.repository.TelegramRecord;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CacheProviderTest {

    @Test
    void cachesShareSingletonsAndStoreValues() {
        Cache<String, Object> settingsFirst = CacheProvider.settingsCache();
        Cache<String, Object> settingsSecond = CacheProvider.settingsCache();
        assertSame(settingsFirst, settingsSecond, "Settings cache should be a singleton");

        settingsFirst.put("theme", "dark");
        assertEquals("dark", settingsSecond.getIfPresent("theme"));

        Cache<Long, List<TelegramRecord>> chats = CacheProvider.chatCache();
        chats.put(1L, List.of(new TelegramRecord(1L, "title")));
        assertEquals(1, chats.getIfPresent(1L).size());

        Cache<String, Object> sessionCache = CacheProvider.sessionCache();
        sessionCache.put("session", 42);
        assertEquals(42, CacheProvider.sessionCache().getIfPresent("session"));
    }
}

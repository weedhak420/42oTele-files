package telegram.files.repository.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.templates.SqlTemplate;
import telegram.files.Config;
import telegram.files.cache.CacheProvider;
import telegram.files.repository.TelegramRecord;
import telegram.files.repository.TelegramRepository;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TelegramRepositoryImpl extends AbstractSqlRepository implements TelegramRepository {

    private static final Log log = LogFactory.get();
    private static final long ALL_CHATS_CACHE_KEY = -1L;
    private final Cache<Long, List<TelegramRecord>> chatCache = CacheProvider.chatCache();

    public TelegramRepositoryImpl(SqlClient sqlClient) {
        super(sqlClient);
    }

    @Override
    public String getRootPath() {
        return Config.TELEGRAM_ROOT + File.separator + UUID.randomUUID();
    }

    @Override
    public Future<TelegramRecord> create(TelegramRecord telegramRecord) {
        return SqlTemplate
                .forUpdate(sqlClient, "INSERT INTO telegram_record(id, first_name, root_path, proxy) VALUES (#{id}, #{first_name}, #{root_path}, #{proxy})")
                .mapFrom(TelegramRecord.PARAM_MAPPER)
                .execute(telegramRecord)
                .map(r -> telegramRecord)
                .onSuccess(r -> log.trace("Successfully created telegram record: %s".formatted(telegramRecord.id())))
                .onFailure(
                        err -> log.error("Failed to create telegram record: %s".formatted(err.getMessage()))
                )
                .onSuccess(r -> evictChats());
    }

    @Override
    public Future<TelegramRecord> update(TelegramRecord telegramRecord) {
        return SqlTemplate
                .forUpdate(sqlClient, "UPDATE telegram_record SET first_name = #{first_name}, root_path = #{root_path}, proxy = #{proxy} WHERE id = #{id}")
                .mapFrom(TelegramRecord.PARAM_MAPPER)
                .execute(telegramRecord)
                .map(r -> telegramRecord)
                .onSuccess(r -> log.debug("Successfully updated telegram record: %s".formatted(telegramRecord.id())))
                .onFailure(
                        err -> log.error("Failed to update telegram record: %s".formatted(err.getMessage()))
                )
                .onSuccess(r -> evictChats());
    }

    @Override
    public Future<TelegramRecord> getById(long id) {
        List<TelegramRecord> cachedChats = chatCache.getIfPresent(ALL_CHATS_CACHE_KEY);
        if (CollUtil.isNotEmpty(cachedChats)) {
            return Future.succeededFuture(cachedChats.stream()
                    .filter(record -> record.id() == id)
                    .findFirst()
                    .orElse(null));
        }
        return SqlTemplate
                .forQuery(sqlClient, "SELECT * FROM telegram_record WHERE id = #{id} limit 1")
                .mapTo(TelegramRecord.ROW_MAPPER)
                .execute(MapUtil.of("id", id))
                .map(rs -> rs.size() == 0 ? null : rs.iterator().next());
    }

    @Override
    public Future<List<TelegramRecord>> getAll() {
        List<TelegramRecord> cached = chatCache.getIfPresent(ALL_CHATS_CACHE_KEY);
        if (CollUtil.isNotEmpty(cached)) {
            return Future.succeededFuture(cached);
        }
        return SqlTemplate
                .forQuery(sqlClient, "SELECT * FROM telegram_record ORDER BY id")
                .mapTo(TelegramRecord.ROW_MAPPER)
                .execute(Collections.emptyMap())
                .map(records -> {
                    List<TelegramRecord> chats = CollUtil.newArrayList(records);
                    chatCache.put(ALL_CHATS_CACHE_KEY, List.copyOf(chats));
                    chats.forEach(record -> chatCache.put(record.id(), List.of(record)));
                    return chats;
                });
    }

    private void evictChats() {
        chatCache.invalidate(ALL_CHATS_CACHE_KEY);
    }
}

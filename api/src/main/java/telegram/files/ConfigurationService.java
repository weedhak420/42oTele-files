package telegram.files;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import telegram.files.repository.ConfigurationHistoryRecord;
import telegram.files.repository.ConfigurationHistoryRepository;
import telegram.files.repository.SettingRecord;
import telegram.files.repository.SettingRepository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConfigurationService {

    private static final Log log = LogFactory.get();

    public record ConfigDefinition(String category,
                                   String key,
                                   Class<?> type,
                                   Object defaultValue,
                                   Number min,
                                   Number max,
                                   String unit,
                                   String description) {
        public String fullKey() {
            return category + "." + key;
        }
    }

    private final Map<String, ConfigDefinition> definitions = new LinkedHashMap<>();
    private final Map<String, Object> cache = new ConcurrentHashMap<>();
    private final SettingRepository settingRepository;
    private final ConfigurationHistoryRepository historyRepository;
    private final Vertx vertx;

    public ConfigurationService(Vertx vertx,
                                SettingRepository settingRepository,
                                ConfigurationHistoryRepository historyRepository) {
        this.vertx = vertx;
        this.settingRepository = settingRepository;
        this.historyRepository = historyRepository;
        registerDefaults();
    }

    public Future<Void> init() {
        List<String> keys = definitions.values().stream().map(ConfigDefinition::fullKey).toList();
        if (CollUtil.isEmpty(keys)) {
            return Future.succeededFuture();
        }
        return settingRepository.getByKeys(keys)
                .compose(records -> {
                    Map<String, SettingRecord> recordMap = records.stream()
                            .collect(Collectors.toMap(SettingRecord::key, Function.identity()));
                    definitions.values().forEach(def -> {
                        SettingRecord record = recordMap.get(def.fullKey());
                        Object value = record == null ? def.defaultValue : convertValue(def, record.value());
                        cache.put(def.fullKey(), Optional.ofNullable(value).orElse(def.defaultValue));
                    });
                    return Future.<Void>succeededFuture();
                })
                // Ensure the initialization chain returns Future<Void> before attaching failure handlers
                .mapEmpty()
                .onFailure(err -> log.error("Failed to initialize configuration service: {}", err.getMessage()))
                .mapEmpty();
    }

    private void registerDefaults() {
        // Auto download
        addDefinition(new ConfigDefinition("autoDownload", "historyScanInterval", Integer.class, 120_000, 10_000, 600_000L, "milliseconds", "Interval for scanning chat history"));
        addDefinition(new ConfigDefinition("autoDownload", "downloadInterval", Integer.class, 10_000, 1_000, 600_000L, "milliseconds", "Interval between download cycles"));
        addDefinition(new ConfigDefinition("autoDownload", "maxWaitingLength", Integer.class, 30, 1, 1_000, "count", "Maximum waiting queue length"));
        addDefinition(new ConfigDefinition("autoDownload", "maxConcurrentDownloads", Integer.class, 5, 1, 50, "count", "Concurrent downloads per account"));
        addDefinition(new ConfigDefinition("autoDownload", "enableAdaptiveThrottling", Boolean.class, true, null, null, "flag", "Enable adaptive throttling"));
        addDefinition(new ConfigDefinition("autoDownload", "retryAttempts", Integer.class, 3, 0, 10, "count", "Retry attempts for downloads"));
        addDefinition(new ConfigDefinition("autoDownload", "retryDelayMs", Long.class, 5_000L, 500L, 600_000L, "milliseconds", "Base retry delay"));

        // Performance
        addDefinition(new ConfigDefinition("performance", "enableCaching", Boolean.class, true, null, null, "flag", "Enable chat caching"));
        addDefinition(new ConfigDefinition("performance", "cacheSize", Integer.class, 1_000, 10, 10_000, "count", "Chat cache maximum size"));
        addDefinition(new ConfigDefinition("performance", "cacheExpirationMinutes", Integer.class, 5, 1, 120, "minutes", "Chat cache expiration"));
        addDefinition(new ConfigDefinition("performance", "batchSize", Integer.class, 100, 1, 1_000, "count", "Default batch size for operations"));
        addDefinition(new ConfigDefinition("performance", "enableMetrics", Boolean.class, true, null, null, "flag", "Enable metrics collection"));

        // Network
        addDefinition(new ConfigDefinition("network", "connectionTimeout", Integer.class, 30_000, 1_000, 120_000, "milliseconds", "Telegram connection timeout"));
        addDefinition(new ConfigDefinition("network", "keepAliveInterval", Long.class, 300_000L, 60_000L, 1_200_000L, "milliseconds", "Keep alive interval"));
        addDefinition(new ConfigDefinition("network", "maxRetries", Integer.class, 3, 0, 10, "count", "Max retries for TDLib calls"));
        addDefinition(new ConfigDefinition("network", "backoffMultiplier", Double.class, 2.0, 1.0, 10.0, "ratio", "Backoff multiplier for retries"));

        // Resource
        addDefinition(new ConfigDefinition("resource", "maxMemoryMb", Integer.class, 1_024, 128, 16_384, "megabytes", "Maximum allowed memory usage"));
        addDefinition(new ConfigDefinition("resource", "threadPoolSize", Integer.class, 10, 1, 200, "count", "Thread pool size"));
        addDefinition(new ConfigDefinition("resource", "databasePoolSize", Integer.class, 20, 1, 200, "count", "Database connection pool size"));
    }

    public static class AutoDownloadConfig {
        public int historyScanInterval = 120_000;
        public int downloadInterval = 10_000;
        public int maxWaitingLength = 30;
        public int maxConcurrentDownloads = 5;
        public boolean enableAdaptiveThrottling = true;
        public int retryAttempts = 3;
        public int retryDelayMs = 5_000;

        public AutoDownloadConfig() {
        }

        public AutoDownloadConfig(JsonObject json) {
            if (json != null) {
                this.historyScanInterval = json.getInteger("historyScanInterval", historyScanInterval);
                this.downloadInterval = json.getInteger("downloadInterval", downloadInterval);
                this.maxWaitingLength = json.getInteger("maxWaitingLength", maxWaitingLength);
                this.maxConcurrentDownloads = json.getInteger("maxConcurrentDownloads", maxConcurrentDownloads);
                this.enableAdaptiveThrottling = json.getBoolean("enableAdaptiveThrottling", enableAdaptiveThrottling);
                this.retryAttempts = json.getInteger("retryAttempts", retryAttempts);
                this.retryDelayMs = json.getInteger("retryDelayMs", retryDelayMs);
            }
        }

        public int historyScanInterval() {
            return historyScanInterval;
        }

        public int downloadInterval() {
            return downloadInterval;
        }

        public int maxWaitingLength() {
            return maxWaitingLength;
        }

        public int maxConcurrentDownloads() {
            return maxConcurrentDownloads;
        }

        public boolean enableAdaptiveThrottling() {
            return enableAdaptiveThrottling;
        }

        public int retryAttempts() {
            return retryAttempts;
        }

        public int retryDelayMs() {
            return retryDelayMs;
        }

        public JsonObject toJson() {
            return new JsonObject()
                    .put("historyScanInterval", historyScanInterval)
                    .put("downloadInterval", downloadInterval)
                    .put("maxWaitingLength", maxWaitingLength)
                    .put("maxConcurrentDownloads", maxConcurrentDownloads)
                    .put("enableAdaptiveThrottling", enableAdaptiveThrottling)
                    .put("retryAttempts", retryAttempts)
                    .put("retryDelayMs", retryDelayMs);
        }
    }

    private void addDefinition(ConfigDefinition definition) {
        definitions.put(definition.fullKey(), definition);
        cache.putIfAbsent(definition.fullKey(), definition.defaultValue);
    }

    public Map<String, ConfigDefinition> getDefinitions() {
        return Collections.unmodifiableMap(definitions);
    }

    public <T> T getValue(String category, String key, Class<T> type) {
        String fullKey = resolveKey(category, key);
        Object value = cache.getOrDefault(fullKey, definitions.get(fullKey) == null ? null : definitions.get(fullKey).defaultValue);
        return type.cast(value);
    }

    public JsonObject getAll() {
        JsonObject response = new JsonObject();
        definitions.values().forEach(def -> {
            JsonObject category = response.getJsonObject(def.category(), new JsonObject());
            category.put(def.key(), describe(def));
            response.put(def.category(), category);
        });
        return response;
    }

    public JsonObject getCategory(String category) {
        JsonObject response = new JsonObject();
        definitions.values().stream().filter(def -> def.category().equals(category)).forEach(def -> response.put(def.key(), describe(def)));
        return response;
    }

    public JsonObject getSchema() {
        return getAll();
    }

    public Future<JsonObject> update(String category, String key, Object rawValue, String user) {
        ConfigDefinition definition = definitions.get(resolveKey(category, key));
        if (definition == null) {
            return Future.failedFuture("Unknown configuration key: %s/%s".formatted(category, key));
        }
        Object value = Optional.ofNullable(rawValue).orElse(definition.defaultValue);
        value = coerceValue(definition, value);
        if (!ConfigValidator.validate(definition, value)) {
            return Future.failedFuture("Validation failed for %s/%s".formatted(category, key));
        }
        String fullKey = definition.fullKey();
        Object oldValue = cache.get(fullKey);
        return settingRepository.createOrUpdate(fullKey, Convert.toStr(value))
                .compose(r -> historyRepository.append(new ConfigurationHistoryRecord(null, category, key, user,
                        oldValue == null ? null : Convert.toStr(oldValue), Convert.toStr(value), System.currentTimeMillis())))
                .onSuccess(v -> {
                    cache.put(fullKey, value);
                    publishChange(definition, value, oldValue);
                })
                .map(describe(definition));
    }

    public Future<Void> reset(String user) {
        List<Future<?>> futures = new ArrayList<>();
        definitions.values().forEach(def -> futures.add(update(def.category(), def.key(), def.defaultValue, user)));
        return Future.all(futures).mapEmpty();
    }

    public Future<JsonObject> exportConfig(Set<String> categories) {
        JsonObject export = new JsonObject();
        definitions.values().stream()
                .filter(def -> categories == null || categories.isEmpty() || categories.contains(def.category()))
                .forEach(def -> {
                    JsonObject category = export.getJsonObject(def.category());
                    if (category == null) {
                        category = new JsonObject();
                        export.put(def.category(), category);
                    }
                    category.put(def.key(), cache.getOrDefault(def.fullKey(), def.defaultValue));
                });
        return Future.succeededFuture(export);
    }

    public Future<Void> importConfig(JsonObject payload, Set<String> categories, String user) {
        if (payload == null) {
            return Future.succeededFuture();
        }
        List<Future<?>> futures = new ArrayList<>();
        for (String category : payload.fieldNames()) {
            if (CollUtil.isNotEmpty(categories) && !categories.contains(category)) {
                continue;
            }
            JsonObject categoryObj = payload.getJsonObject(category);
            if (categoryObj == null) {
                continue;
            }
            for (String key : categoryObj.fieldNames()) {
                ConfigDefinition definition = definitions.get(resolveKey(category, key));
                if (definition == null) {
                    continue;
                }
                Object value = categoryObj.getValue(key);
                Object coerced = coerceValue(definition, value);
                if (!ConfigValidator.validate(definition, coerced)) {
                    return Future.failedFuture("Validation failed for %s/%s".formatted(category, key));
                }
                futures.add(update(category, key, coerced, user));
            }
        }
        return Future.all(futures).mapEmpty();
    }

    public Future<Void> saveProfile(String name, JsonObject settings, String user) {
        if (StrUtil.isBlank(name) || settings == null) {
            return Future.failedFuture("Profile name or settings missing");
        }
        return settingRepository.createOrUpdate(profileKey(name), settings.encode())
                .compose(ignore -> historyRepository.append(new ConfigurationHistoryRecord(null, "profile", name, user, null, settings.encode(), System.currentTimeMillis())))
                .mapEmpty();
    }

    public Future<Void> applyProfile(String name, String user) {
        return settingRepository.getByKeys(List.of(profileKey(name)))
                .compose(records -> {
                    if (CollUtil.isEmpty(records)) {
                        return Future.failedFuture("Profile %s not found".formatted(name));
                    }
                    JsonObject profile = new JsonObject(records.getFirst().value());
                    return importConfig(profile, Collections.emptySet(), user);
                });
    }

    public Future<List<ConfigurationHistoryRecord>> getHistory(int limit) {
        return historyRepository.list(limit);
    }

    public Future<JsonObject> rollback(String category, String key, String user) {
        return historyRepository.getLastChange(category, key)
                .compose(last -> {
                    if (last == null || last.oldValue() == null) {
                        return Future.failedFuture("No history to rollback for %s/%s".formatted(category, key));
                    }
                    ConfigDefinition definition = definitions.get(resolveKey(category, key));
                    Object value = coerceValue(definition, last.oldValue());
                    return update(category, key, value, user);
                });
    }

    private JsonObject describe(ConfigDefinition definition) {
        Object value = cache.getOrDefault(definition.fullKey(), definition.defaultValue);
        return new JsonObject()
                .put("value", value)
                .put("type", definition.type().getSimpleName().toLowerCase())
                .put("unit", definition.unit())
                .put("min", definition.min())
                .put("max", definition.max())
                .put("description", definition.description());
    }

    private Object coerceValue(ConfigDefinition definition, Object value) {
        if (value == null) {
            return definition.defaultValue();
        }
        if (definition.type() == Boolean.class) {
            return Convert.toBool(value, Convert.toBool(definition.defaultValue));
        }
        if (definition.type() == Integer.class) {
            return Convert.toInt(value, Convert.toInt(definition.defaultValue));
        }
        if (definition.type() == Long.class) {
            return Convert.toLong(value, Convert.toLong(definition.defaultValue));
        }
        if (definition.type() == Double.class) {
            return Convert.toDouble(value, Convert.toDouble(definition.defaultValue));
        }
        return value;
    }

    private Object convertValue(ConfigDefinition definition, String raw) {
        if (raw == null) {
            return definition.defaultValue();
        }
        return coerceValue(definition, raw);
    }

    private String resolveKey(String category, String key) {
        return category + "." + key;
    }

    private void publishChange(ConfigDefinition definition, Object value, Object oldValue) {
        JsonObject payload = new JsonObject()
                .put("category", definition.category())
                .put("key", definition.key())
                .put("value", value)
                .put("oldValue", oldValue);
        vertx.eventBus().publish(EventEnum.CONFIGURATION_CHANGE.address(definition.category()), payload);
    }

    private String profileKey(String name) {
        return "profile." + name;
    }
}

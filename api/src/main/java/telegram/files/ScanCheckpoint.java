package telegram.files;

import cn.hutool.core.util.StrUtil;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import telegram.files.repository.SettingKey;
import telegram.files.repository.SettingRepository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ScanCheckpoint {

    public String uniqueKey;

    public long chatId;

    public long lastScannedMessageId;

    public String lastFileType;

    public long timestamp;

    public ScanCheckpoint() {
    }

    public ScanCheckpoint(String uniqueKey, long chatId, long lastScannedMessageId, String lastFileType, long timestamp) {
        this.uniqueKey = uniqueKey;
        this.chatId = chatId;
        this.lastScannedMessageId = lastScannedMessageId;
        this.lastFileType = lastFileType;
        this.timestamp = timestamp;
    }

    public static Future<Map<String, ScanCheckpoint>> load(SettingRepository settingRepository) {
        return settingRepository.<String>getByKey(SettingKey.autoDownloadScanCheckpoint)
                .map(value -> {
                    if (StrUtil.isBlank(value)) {
                        return new ConcurrentHashMap<String, ScanCheckpoint>();
                    }
                    List<?> list = Json.decodeValue(value, List.class);
                    if (list == null) {
                        return new ConcurrentHashMap<String, ScanCheckpoint>();
                    }
                    return list.stream()
                            .map(json -> new JsonObject((Map<String, Object>) json).mapTo(ScanCheckpoint.class))
                            .collect(Collectors.toConcurrentMap(checkpoint -> checkpoint.uniqueKey, checkpoint -> checkpoint));
                });
    }

    public static Future<Void> save(SettingRepository settingRepository, Collection<ScanCheckpoint> checkpoints) {
        List<JsonObject> payload = checkpoints.stream()
                .map(JsonObject::mapFrom)
                .toList();
        return settingRepository.createOrUpdate(SettingKey.autoDownloadScanCheckpoint.name(), Json.encode(payload))
                .mapEmpty();
    }

    public void update(String nextFileType, long nextFromMessageId) {
        this.lastFileType = nextFileType;
        this.lastScannedMessageId = nextFromMessageId;
        this.timestamp = System.currentTimeMillis();
    }
}

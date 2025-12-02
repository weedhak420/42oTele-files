package telegram.files;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.drinkless.tdlib.TdApi;
import org.jooq.lambda.tuple.Tuple2;
import telegram.files.repository.FileRecord;
import telegram.files.repository.SettingAutoRecords;
import telegram.files.repository.SettingKey;
import telegram.files.repository.SettingTimeLimitedDownload;
import telegram.files.TelegramVerticle;
import telegram.files.TelegramVerticles;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class AutoDownloadVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    private static final int MAX_HISTORY_SCAN_TIME = 10 * 1000;

    private static final List<String> DEFAULT_FILE_TYPE_ORDER = List.of("photo", "video", "audio", "file");

    // telegramId -> messages
    private final Map<Long, Queue<MessageWrapper>> waitingDownloadMessages = new ConcurrentHashMap<>();

    // telegramId -> waiting scan threads
    private final Map<Long, LinkedList<WaitingScanThread>> waitingScanThreads = new ConcurrentHashMap<>();

    private final SettingAutoRecords autoRecords;

    private final AdaptiveThrottler adaptiveThrottler = new AdaptiveThrottler();

    private final Map<String, RetryContext> retryContexts = new ConcurrentHashMap<>();

    private final Map<String, ScanCheckpoint> scanCheckpoints = new ConcurrentHashMap<>();

    private final Set<String> activeDownloads = ConcurrentHashMap.newKeySet();

    private int limit;

    private int defaultLimit;

    private int historyScanInterval;

    private int downloadInterval;

    private int maxWaitingLength;

    private boolean adaptiveThrottlingEnabled;

    private int retryAttempts;

    private long retryDelayMs;

    private SettingTimeLimitedDownload timeLimited;

    private long historyScanTimerId;

    private long downloadTimerId;

    private long cleanupTimerId;

    private long concurrencyAdjustTimerId;

    private long stuckDownloadTimerId;

    private volatile int maxConcurrentDownloads;

    private volatile int currentConcurrentDownloads;

    private boolean adaptiveConcurrencyEnabled;

    private int downloadRetryAttempts;

    private long downloadTimeoutMs;

    public AutoDownloadVerticle() {
        this.autoRecords = AutomationsHolder.INSTANCE.autoRecords();
        AutomationsHolder.INSTANCE.registerOnRemoveListener(removedItems -> removedItems.forEach(item ->
                waitingDownloadMessages.getOrDefault(item.telegramId, new ConcurrentLinkedQueue<>())
                        .removeIf(m -> m.message().chatId == item.chatId)));
    }

    @Override
    public void start(Promise<Void> startPromise) {
        initAutoDownload()
                .compose(v -> this.initEventConsumer())
                .onSuccess(v -> {
                    restartHistoryScanTimer();
                    restartDownloadTimer();
                    startCleanupTimer();
                    startConcurrencyAdjuster();
                    startStuckDownloadMonitor();
                    adjustConcurrency();

                    log.info("""
                            Auto download verticle started!
                            |History scan interval: %s ms
                            |Download interval: %s ms
                            |Download limit: %s per telegram account!
                            |Time limit: %s
                            |Auto chats: %s
                            """.formatted(historyScanInterval,
                            downloadInterval,
                            limit,
                            timeLimited == null ? "" : Json.encode(timeLimited),
                            autoRecords.getDownloadEnabledItems().size()));

                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop() {
        log.info("Auto download verticle stopped!");
    }

    private void restartHistoryScanTimer() {
        if (historyScanTimerId != 0) {
            vertx.cancelTimer(historyScanTimerId);
        }
        historyScanTimerId = vertx.setPeriodic(0, historyScanInterval, id -> runHistoryScan());
    }

    private void restartDownloadTimer() {
        if (downloadTimerId != 0) {
            vertx.cancelTimer(downloadTimerId);
        }
        downloadTimerId = vertx.setPeriodic(0, downloadInterval, id -> {
            if (!isDownloadTime()) {
                log.debug("Auto download time limited! Skip download.");
                return;
            }
            waitingDownloadMessages.keySet().forEach(this::download);
        });
    }

    private void startCleanupTimer() {
        if (cleanupTimerId != 0) {
            vertx.cancelTimer(cleanupTimerId);
        }
        cleanupTimerId = vertx.setPeriodic(Duration.ofMinutes(10).toMillis(), id -> cleanupOldData());
    }

    private void startConcurrencyAdjuster() {
        if (!adaptiveConcurrencyEnabled) {
            return;
        }
        if (concurrencyAdjustTimerId != 0) {
            vertx.cancelTimer(concurrencyAdjustTimerId);
        }
        concurrencyAdjustTimerId = vertx.setPeriodic(Duration.ofMinutes(2).toMillis(), id -> adjustConcurrency());
    }

    private void startStuckDownloadMonitor() {
        if (stuckDownloadTimerId != 0) {
            vertx.cancelTimer(stuckDownloadTimerId);
        }
        stuckDownloadTimerId = vertx.setPeriodic(Duration.ofMinutes(1).toMillis(), id -> checkStuckDownloads());
    }

    private void runHistoryScan() {
        if (!isDownloadTime()) {
            log.debug("Auto download time limited! Skip scan history.");
            return;
        }

        autoRecords.getDownloadEnabledItems()
                .stream()
                .filter(auto -> auto.download.rule.downloadHistory
                                && auto.isNotComplete(SettingAutoRecords.HISTORY_DOWNLOAD_STATE))
                .forEach(auto -> {
                    if (isDownloadCommentEnabled(auto)
                        && CollUtil.isNotEmpty(waitingScanThreads.get(auto.telegramId))) {
                        addCommentMessage(auto);
                    } else {
                        if (auto.isNotComplete(SettingAutoRecords.HISTORY_DOWNLOAD_SCAN_STATE)) {
                            addHistoryMessage(auto);
                        } else {
                            Queue<MessageWrapper> messageWrappers = waitingDownloadMessages.get(auto.telegramId);
                            if (CollUtil.isEmpty(messageWrappers)
                                || messageWrappers.stream().noneMatch(MessageWrapper::isHistorical)) {
                                auto.complete(SettingAutoRecords.HISTORY_DOWNLOAD_STATE);
                                removeCheckpoint(auto.uniqueKey());
                            }
                        }
                    }
                });
    }

    private void cleanupOldData() {
        Instant now = Instant.now();
        Instant historyThreshold = now.minus(Duration.ofDays(7));
        waitingDownloadMessages.values()
                .forEach(queue -> queue.removeIf(wrapper -> wrapper.isHistorical()
                        && Instant.ofEpochSecond(wrapper.message().date).isBefore(historyThreshold)));

        Instant retryThreshold = now.minus(Duration.ofHours(1));
        retryContexts.entrySet().removeIf(entry -> Instant.ofEpochMilli(entry.getValue().getLastAttemptTime()).isBefore(retryThreshold));

        waitingDownloadMessages.forEach((telegramId, queue) -> {
            if (queue.size() > 100) {
                log.warn("Queue size for telegram %d exceeds 100, trimming oldest".formatted(telegramId));
                while (queue.size() > 100) {
                    queue.poll();
                }
            }
        });
    }

    private void checkStuckDownloads() {
        long now = System.currentTimeMillis();
        long hardTimeout = downloadTimeoutMs <= 0 ? Duration.ofMinutes(30).toMillis() : downloadTimeoutMs;
        long softTimeout = Duration.ofMinutes(5).toMillis();
        DataVerticle.fileRepository.getDownloadingFiles()
                .onSuccess(files -> files.forEach(file -> {
                    long startDate = file.startDate();
                    if (startDate <= 0) {
                        return;
                    }
                    long stuckDuration = now - startDate;
                    boolean isHardStuck = stuckDuration > hardTimeout;
                    boolean isSoftStuck = file.size() < 50_000_000L && stuckDuration > softTimeout;
                    if (isHardStuck || isSoftStuck) {
                        String reason = isHardStuck ? "hard timeout (30min)" : "soft timeout (5min, file < 50MB)";
                        log.warn("Detected stuck download: %s (%s, stuck %d min)".formatted(
                                file.fileName(), reason, stuckDuration / 60000));
                        resetStuckDownload(file, reason);
                    }
                }));
    }

    private void resetStuckDownload(FileRecord file, String reason) {
        Optional<TelegramVerticle> telegram = TelegramVerticles.get(file.telegramId());
        Future<Void> cancelFuture = telegram
                .map(t -> t.stopDownloadOnly(file.id(), file.uniqueId())
                        .onFailure(err -> log.warn("Failed to cancel stuck download for {}: {}", file.uniqueId(), err.getMessage())))
                .orElse(Future.succeededFuture());

        cancelFuture.compose(v -> DataVerticle.fileRepository.updateDownloadStatus(
                        file.id(),
                        file.uniqueId(),
                        null,
                        FileRecord.DownloadStatus.idle,
                        null))
                .onSuccess(v -> {
                    activeDownloads.remove(file.uniqueId());
                    retryContexts.remove(file.uniqueId());
                    log.info("Reset stuck download: {} ({})", file.fileName(), reason);
                })
                .onFailure(err -> log.error("Failed to reset stuck download {}: {}", file.uniqueId(), err.getMessage()));
    }

    private Future<Void> initAutoDownload() {
        ConfigurationService configurationService = DataVerticle.configurationService;
        this.historyScanInterval = configurationService.getValue("autoDownload", "historyScanInterval", Integer.class);
        this.downloadInterval = configurationService.getValue("autoDownload", "downloadInterval", Integer.class);
        this.maxWaitingLength = configurationService.getValue("autoDownload", "maxWaitingLength", Integer.class);
        this.limit = configurationService.getValue("autoDownload", "maxConcurrentDownloads", Integer.class);
        this.maxConcurrentDownloads = Math.max(1, limit);
        this.currentConcurrentDownloads = this.maxConcurrentDownloads;
        this.defaultLimit = this.limit;
        this.adaptiveThrottlingEnabled = configurationService.getValue("autoDownload", "enableAdaptiveThrottling", Boolean.class);
        Boolean adaptiveConcurrencySetting = Future.await(DataVerticle.settingRepository.getByKey(SettingKey.enableAdaptiveConcurrency));
        this.adaptiveConcurrencyEnabled = adaptiveConcurrencySetting == null ? this.adaptiveThrottlingEnabled : adaptiveConcurrencySetting;
        this.retryAttempts = configurationService.getValue("autoDownload", "retryAttempts", Integer.class);
        Integer configuredRetries = Future.await(DataVerticle.settingRepository.getByKey(SettingKey.downloadRetryAttempts));
        this.downloadRetryAttempts = configuredRetries == null ? this.retryAttempts : configuredRetries;
        this.retryDelayMs = configurationService.getValue("autoDownload", "retryDelayMs", Long.class);
        Long configuredTimeout = Future.await(DataVerticle.settingRepository.getByKey(SettingKey.downloadTimeout));
        this.downloadTimeoutMs = configuredTimeout == null ? 30 * 60 * 1000L : configuredTimeout;
        adaptiveThrottler.reset(limit);
        return DataVerticle.settingRepository.<ConfigurationService.AutoDownloadConfig>getByKey(SettingKey.autoDownload)
                .compose(this::applyAutoDownloadConfig)
                .compose(v -> DataVerticle.settingRepository.<SettingTimeLimitedDownload>getByKey(SettingKey.autoDownloadTimeLimited)
                        .onSuccess(value -> this.timeLimited = value)
                        .mapEmpty())
                .compose(v -> ScanCheckpoint.load(DataVerticle.settingRepository)
                        .onSuccess(scanCheckpoints::putAll)
                        .mapEmpty())
                // Ensure the composed init chain returns Future<Void> for consistent error handling
                .mapEmpty()
                .onFailure(e -> log.error("Get Auto download settings failed!", e))
                .mapEmpty();
    }

    private Future<Void> applyAutoDownloadConfig(ConfigurationService.AutoDownloadConfig config) {
        if (config != null) {
            this.historyScanInterval = Convert.toInt(config.historyScanInterval(), historyScanInterval);
            this.downloadInterval = Convert.toInt(config.downloadInterval(), downloadInterval);
            this.maxWaitingLength = Convert.toInt(config.maxWaitingLength(), maxWaitingLength);
            this.limit = Convert.toInt(config.maxConcurrentDownloads(), limit);
            this.maxConcurrentDownloads = Math.max(1, this.limit);
            this.currentConcurrentDownloads = Math.min(this.currentConcurrentDownloads, this.maxConcurrentDownloads);
            this.defaultLimit = this.limit;
            this.adaptiveThrottlingEnabled = Convert.toBool(config.enableAdaptiveThrottling(), adaptiveThrottlingEnabled);
            this.adaptiveConcurrencyEnabled = adaptiveConcurrencyEnabled || this.adaptiveThrottlingEnabled;
            this.retryAttempts = Convert.toInt(config.retryAttempts(), retryAttempts);
            this.downloadRetryAttempts = Math.max(downloadRetryAttempts, this.retryAttempts);
            this.retryDelayMs = Convert.toLong(config.retryDelayMs(), retryDelayMs);
            adaptiveThrottler.reset(limit);
        }
        return Future.succeededFuture();
    }

    private Future<Void> initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.CONFIGURATION_CHANGE.address("autoDownload"), message -> {
            JsonObject payload = (JsonObject) message.body();
            String key = payload.getString("key");
            Object value = payload.getValue("value");
            switch (key) {
                case "historyScanInterval" -> {
                    this.historyScanInterval = Convert.toInt(value, historyScanInterval);
                    restartHistoryScanTimer();
                }
                case "downloadInterval" -> {
                    this.downloadInterval = Convert.toInt(value, downloadInterval);
                    restartDownloadTimer();
                }
                case "maxWaitingLength" -> this.maxWaitingLength = Convert.toInt(value, maxWaitingLength);
                case "maxConcurrentDownloads" -> {
                    this.limit = Convert.toInt(value, limit);
                    adaptiveThrottler.reset(limit);
                }
                case "enableAdaptiveThrottling" -> this.adaptiveThrottlingEnabled = Convert.toBool(value, adaptiveThrottlingEnabled);
                case "retryAttempts" -> this.retryAttempts = Convert.toInt(value, retryAttempts);
                case "retryDelayMs" -> this.retryDelayMs = Convert.toLong(value, retryDelayMs);
                default -> log.debug("Ignored autoDownload config change for key {}", key);
            }
        });
        vertx.eventBus().consumer(EventEnum.SETTING_UPDATE.address(SettingKey.autoDownloadTimeLimited.name()), message -> {
            log.debug("Auto download time limit update: %s".formatted(message.body()));
            this.timeLimited = (SettingTimeLimitedDownload) SettingKey.autoDownloadTimeLimited.converter.apply((String) message.body());
        });
        vertx.eventBus().consumer(EventEnum.MESSAGE_RECEIVED.address(), message -> {
            log.trace("Auto download message received: %s".formatted(message.body()));
            this.onNewMessage((JsonObject) message.body());
        });
        return Future.succeededFuture();
    }

    private void addCommentMessage(SettingAutoRecords.Automation auto) {
        LinkedList<WaitingScanThread> scanThreads = waitingScanThreads.get(auto.telegramId);
        if (CollUtil.isEmpty(scanThreads)) {
            return;
        }
        scanThreads.removeIf(scanThread -> scanThread.isComplete);
        waitingScanThreads.get(auto.telegramId).forEach(scanThread -> {
            ScanParams scanParams = new ScanParams(auto.uniqueKey() + ":" + scanThread.messageThreadId,
                    auto.download.rule,
                    auto.telegramId,
                    scanThread.threadChatId,
                    scanThread.nextFileType,
                    scanThread.nextFromMessageId);
            scanParams.messageThreadId = scanThread.messageThreadId;
            addHistoryMessage(scanParams,
                    result -> {
                        scanThread.nextFileType = result.nextFileType;
                        scanThread.nextFromMessageId = result.nextFromMessageId;
                        if (result.isComplete) {
                            scanThread.isComplete = true;
                        }
                    },
                    System.currentTimeMillis()
            );
        });
    }

    private void addHistoryMessage(SettingAutoRecords.Automation auto) {
        addHistoryMessage(new ScanParams(auto.uniqueKey(),
                        auto.download.rule,
                        auto.telegramId,
                        auto.chatId,
                        auto.download.nextFileType,
                        auto.download.nextFromMessageId),
                result -> {
                    auto.download.nextFileType = result.nextFileType;
                    auto.download.nextFromMessageId = result.nextFromMessageId;
                    if (result.isComplete) {
                        auto.complete(SettingAutoRecords.HISTORY_DOWNLOAD_SCAN_STATE);
                    }
                },
                System.currentTimeMillis()
        );
    }

    private void addHistoryMessage(ScanParams params,
                                   Consumer<ScanResult> callback,
                                   long currentTimeMillis) {
        String uniqueKey = params.uniqueKey;
        long telegramId = params.telegramId;
        long chatId = params.chatId;
        long nextFromMessageId = params.nextFromMessageId;
        String nextFileType = params.nextFileType;
        Tuple2<String, List<String>> rule = handleRule(params.rule);
        if (StrUtil.isBlank(nextFileType)) {
            nextFileType = rule.v2.getFirst();
        }
        ScanCheckpoint checkpoint = scanCheckpoints.get(uniqueKey);
        if (checkpoint != null) {
            nextFromMessageId = Math.max(nextFromMessageId, checkpoint.lastScannedMessageId);
            if (StrUtil.isNotBlank(checkpoint.lastFileType)) {
                nextFileType = checkpoint.lastFileType;
            }
        }

        log.debug("Start scan history! TelegramId: %d ChatId: %d FileType: %s".formatted(telegramId, chatId, nextFileType));
        if (System.currentTimeMillis() - currentTimeMillis > MAX_HISTORY_SCAN_TIME) {
            log.debug("Scan history timeout! TelegramId: %d ChatId: %d".formatted(telegramId, chatId));
            callback.accept(new ScanResult(nextFileType, nextFromMessageId, false));
            return;
        }
        if (isExceedLimit(telegramId)) {
            log.debug("Scan history exceed per telegram account limit! TelegramId: %d ChatId: %d".formatted(telegramId, chatId));
            callback.accept(new ScanResult(nextFileType, nextFromMessageId, false));
            return;
        }

        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(telegramId);
        TdApi.SearchChatMessages searchChatMessages = new TdApi.SearchChatMessages();
        searchChatMessages.query = rule.v1;
        searchChatMessages.chatId = chatId;
        searchChatMessages.fromMessageId = nextFromMessageId;
        searchChatMessages.limit = Math.min(maxWaitingLength, Math.min(100, calculateOptimalChunkSize(telegramVerticle)));
        searchChatMessages.filter = TdApiHelp.getSearchMessagesFilter(nextFileType);
        searchChatMessages.messageThreadId = params.messageThreadId;
        final String fileTypeForCheckpoint = nextFileType;
        TdApi.FoundChatMessages foundChatMessages = Future.await(telegramVerticle.client.execute(searchChatMessages)
                .onFailure(r -> log.error("Search chat messages failed! TelegramId: %d ChatId: %d".formatted(telegramId, chatId), r))
        );
        if (foundChatMessages == null) {
            callback.accept(new ScanResult(nextFileType, nextFromMessageId, false));
            return;
        }
        if (foundChatMessages.messages.length == 0) {
            List<String> fileTypes = rule.v2;
            int nextTypeIndex = fileTypes.indexOf(nextFileType) + 1;
            if (nextTypeIndex < fileTypes.size()) {
                params.nextFileType = fileTypes.get(nextTypeIndex);
                params.nextFromMessageId = 0;
                log.debug("%s No more %s files found! Switch to %s".formatted(uniqueKey, nextFileType, params.nextFileType));
                updateCheckpoint(uniqueKey, chatId, params.nextFileType, params.nextFromMessageId);
                addHistoryMessage(params, callback, currentTimeMillis);
            } else {
                log.debug("%s No more history files found! TelegramId: %d ChatId: %d".formatted(uniqueKey, telegramId, chatId));
                removeCheckpoint(uniqueKey);
                callback.accept(new ScanResult(nextFileType, nextFromMessageId, true));
            }
        } else {
            log.info("Found %d historical messages to process for chat %d".formatted(
                    foundChatMessages.messages.length, chatId));
            DataVerticle.fileRepository.getFilesByUniqueId(TdApiHelp.getFileUniqueIds(Arrays.asList(foundChatMessages.messages)))
                    .onSuccess(existFiles -> {
                        List<TdApi.Message> messages = Stream.of(foundChatMessages.messages)
                                .filter(message -> {
                                    String uniqueId = TdApiHelp.getFileUniqueId(message);
                                    if (!existFiles.containsKey(uniqueId)) {
                                        return true;
                                    } else {
                                        FileRecord fileRecord = existFiles.get(uniqueId);
                                        return fileRecord.isDownloadStatus(FileRecord.DownloadStatus.idle);
                                    }
                                })
                                .toList();
                        if (CollUtil.isEmpty(messages)) {
                            params.nextFromMessageId = foundChatMessages.nextFromMessageId;
                            updateCheckpoint(uniqueKey, chatId, fileTypeForCheckpoint, params.nextFromMessageId);
                            addHistoryMessage(params, callback, currentTimeMillis);
                        } else {
                            boolean added = scanChunk(telegramId, messages, true, params.rule);
                            params.nextFromMessageId = foundChatMessages.nextFromMessageId;
                            updateCheckpoint(uniqueKey, chatId, fileTypeForCheckpoint, params.nextFromMessageId);
                            if (added) {
                                addHistoryMessage(params, callback, currentTimeMillis);
                            }
                        }
                    });
        }
    }

    private boolean scanChunk(long telegramId,
                              List<TdApi.Message> messages,
                              boolean isHistorical,
                              SettingAutoRecords.DownloadRule rule) {
        if (CollUtil.isEmpty(messages)) {
            return false;
        }
        int chunkSize = Math.max(1, Math.min(100, Math.min(maxWaitingLength, calculateOptimalChunkSize(telegramId))));
        boolean added = false;
        for (int i = 0; i < messages.size(); i += chunkSize) {
            List<TdApi.Message> chunk = messages.subList(i, Math.min(messages.size(), i + chunkSize));
            if (!addWaitingDownloadMessages(telegramId, chunk, false, isHistorical, rule)) {
                break;
            }
            added = true;
        }
        return added;
    }

    private Tuple2<String, List<String>> handleRule(SettingAutoRecords.DownloadRule rule) {
        String query = null;
        List<String> fileTypes = DEFAULT_FILE_TYPE_ORDER;
        if (rule != null) {
            if (StrUtil.isNotBlank(rule.query)) {
                query = rule.query;
            }
            if (CollUtil.isNotEmpty(rule.fileTypes)) {
                fileTypes = rule.fileTypes;
            }
        }
        return new Tuple2<>(query, fileTypes);
    }

    private void updateCheckpoint(String uniqueKey, long chatId, String nextFileType, long nextFromMessageId) {
        scanCheckpoints.compute(uniqueKey, (key, existing) -> {
            ScanCheckpoint checkpoint = existing == null
                    ? new ScanCheckpoint(uniqueKey, chatId, nextFromMessageId, nextFileType, System.currentTimeMillis())
                    : existing;
            checkpoint.update(nextFileType, nextFromMessageId);
            return checkpoint;
        });
        persistCheckpoints();
    }

    private void removeCheckpoint(String uniqueKey) {
        if (scanCheckpoints.remove(uniqueKey) != null) {
            persistCheckpoints();
        }
    }

    private void persistCheckpoints() {
        ScanCheckpoint.save(DataVerticle.settingRepository, scanCheckpoints.values())
                .onFailure(e -> log.error("Failed to persist scan checkpoints", e));
    }

    private boolean isDownloadTime() {
        if (timeLimited == null) {
            return true;
        }
        LocalTime now = LocalTime.now();

        LocalTime startTime = LocalTime.parse(timeLimited.startTime);
        LocalTime endTime = LocalTime.parse(timeLimited.endTime);
        if (startTime.equals(LocalTime.MIN) && endTime.equals(LocalTime.MIN)) {
            return true;
        }

        if (startTime.isAfter(endTime)) {
            return now.isAfter(startTime) || now.isBefore(endTime);
        } else {
            return now.isAfter(startTime) && now.isBefore(endTime);
        }
    }

    private boolean isExceedLimit(long telegramId) {
        Queue<MessageWrapper> waitingMessages = this.waitingDownloadMessages.get(telegramId);
        return getSurplusSize(telegramId) <= 0 || (waitingMessages != null && waitingMessages.size() > maxWaitingLength);
    }

    private int getSurplusSize(long telegramId) {
        Integer downloading = Future.await(DataVerticle.fileRepository.countByStatus(telegramId, FileRecord.DownloadStatus.downloading));
        int effectiveLimit = getEffectiveLimit(telegramId);
        return downloading == null ? effectiveLimit : Math.max(0, effectiveLimit - downloading);
    }

    private int getEffectiveLimit(long telegramId) {
        int configuredLimit = limit == 0 ? defaultLimit : limit;
        int adaptiveLimit = adaptiveConcurrencyEnabled ? currentConcurrentDownloads : configuredLimit;
        if (!adaptiveThrottlingEnabled) {
            return Math.min(adaptiveLimit, maxConcurrentDownloads);
        }
        if (telegramId == 0) {
            return Math.min(adaptiveThrottler.getCurrentLimit(adaptiveLimit), maxConcurrentDownloads);
        }
        return TelegramVerticles.get(telegramId)
                .map(telegramVerticle -> adaptiveThrottler.calculateLimit(getAverageSpeed(telegramVerticle), adaptiveLimit))
                .orElse(adaptiveThrottler.getCurrentLimit(adaptiveLimit));
    }

    private void adjustConcurrency() {
        if (!adaptiveConcurrencyEnabled) {
            return;
        }
        long avgSpeedBytes = calculateGlobalAverageSpeed();
        long avgSpeedMBps = avgSpeedBytes / 1_000_000L;
        int targetLimit;
        if (avgSpeedMBps > 10) {
            targetLimit = 20;
        } else if (avgSpeedMBps > 5) {
            targetLimit = 15;
        } else if (avgSpeedMBps > 2) {
            targetLimit = 10;
        } else if (avgSpeedMBps > 1) {
            targetLimit = 5;
        } else {
            targetLimit = 3;
        }

        targetLimit = Math.min(targetLimit, maxConcurrentDownloads);
        int updatedLimit = currentConcurrentDownloads;
        if (targetLimit > updatedLimit) {
            updatedLimit = Math.min(targetLimit, updatedLimit + 2);
        } else if (targetLimit < updatedLimit) {
            updatedLimit = Math.max(targetLimit, updatedLimit - 2);
        }
        updatedLimit = Math.max(1, updatedLimit);
        if (updatedLimit != currentConcurrentDownloads) {
            log.info("Adjusted concurrent downloads: %d (speed: %d MB/s)".formatted(updatedLimit, avgSpeedMBps));
        }
        currentConcurrentDownloads = updatedLimit;
    }

    private long calculateGlobalAverageSpeed() {
        List<TelegramVerticle> telegrams = TelegramVerticles.getAll();
        if (CollUtil.isEmpty(telegrams)) {
            return 0L;
        }
        return (long) telegrams.stream()
                .map(TelegramVerticle::getCurrentSpeedStats)
                .mapToLong(AvgSpeed.SpeedStats::avgSpeed)
                .average()
                .orElse(0);
    }

    private long getAverageSpeed(TelegramVerticle telegramVerticle) {
        return telegramVerticle.getCurrentSpeedStats().avgSpeed();
    }

    private int calculateOptimalChunkSize(long telegramId) {
        return TelegramVerticles.get(telegramId)
                .map(this::calculateOptimalChunkSize)
                .orElse(calculateOptimalChunkSizeBySpeed(0L));
    }

    private int calculateOptimalChunkSize(TelegramVerticle telegramVerticle) {
        return calculateOptimalChunkSizeBySpeed(getAverageSpeed(telegramVerticle));
    }

    private int calculateOptimalChunkSizeBySpeed(long avgSpeedBytesPerSecond) {
        if (avgSpeedBytesPerSecond > 2L * 1024 * 1024) {
            return 100;
        }
        if (avgSpeedBytesPerSecond > 500L * 1024) {
            return 50;
        }
        return 20;
    }

    private boolean isDownloadCommentEnabled(SettingAutoRecords.Automation auto) {
        if (!auto.download.enabled || !auto.download.rule.downloadCommentFiles) {
            return false;
        }
        return TelegramVerticles.get(auto.telegramId)
                .map(telegramVerticle -> telegramVerticle.getChat(auto.chatId))
                .map(chat -> chat.type.getConstructor() == TdApi.ChatTypeSupergroup.CONSTRUCTOR
                             && ((TdApi.ChatTypeSupergroup) chat.type).isChannel)
                .orElse(false);
    }

    private boolean addWaitingDownloadMessages(long telegramId,
                                               List<TdApi.Message> messages,
                                               boolean force,
                                               boolean isHistorical) {
        return addWaitingDownloadMessages(telegramId, messages, force, isHistorical, null);
    }

    private boolean addWaitingDownloadMessages(long telegramId,
                                               List<TdApi.Message> messages,
                                               boolean force,
                                               boolean isHistorical,
                                               SettingAutoRecords.DownloadRule rule) {
        if (CollUtil.isEmpty(messages)) {
            return false;
        }
        Queue<MessageWrapper> waitingMessages = this.waitingDownloadMessages.computeIfAbsent(
                telegramId,
                k -> new ConcurrentLinkedQueue<>()
        );
        if (!force && waitingMessages.size() > maxWaitingLength) {
            return false;
        } else {
            log.debug("Add waiting download messages: %d".formatted(messages.size()));
            TdApiHelp.filterUniqueMessages(messages).forEach(message -> {
                int priority = calculateFilePriority(message, rule);
                waitingMessages.add(new MessageWrapper(message, isHistorical, priority));
            });
            publishQueueSize(telegramId, waitingMessages.size());
            log.info("Added %d messages to download queue for telegram %d (total queue: %d)".formatted(
                    messages.size(), telegramId, waitingMessages.size()));
        }
        return true;
    }

    private int calculateFilePriority(TdApi.Message message, SettingAutoRecords.DownloadRule rule) {
        String fileType = TdApiHelp.getFileType(message);
        List<String> orderedTypes = rule != null && CollUtil.isNotEmpty(rule.fileTypes)
                ? rule.fileTypes
                : DEFAULT_FILE_TYPE_ORDER;
        int basePriority = orderedTypes.indexOf(fileType);
        if (basePriority < 0) {
            basePriority = orderedTypes.size();
        }

        long fileSize = TdApiHelp.getFileSize(message);
        if (fileSize < 10_000_000) {
            basePriority -= 1;
        }

        long messageAge = System.currentTimeMillis() - (message.date * 1000L);
        if (messageAge < 3_600_000) {
            basePriority -= 1;
        }

        return Math.max(0, basePriority);
    }

    private void download(long telegramId) {
        if (CollUtil.isEmpty(waitingDownloadMessages)) {
            log.debug("No waiting download messages map available");
            return;
        }
        Queue<MessageWrapper> messages = waitingDownloadMessages.get(telegramId);
        if (messages == null || messages.isEmpty()) {
            log.debug("No waiting download messages for telegramId: %d".formatted(telegramId));
            return;
        }
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(telegramId);
        int surplusSize = getSurplusSize(telegramId);
        log.info("Download queue size for telegram %d: %d messages".formatted(telegramId, messages.size()));
        log.info("Available download slots for telegram %d: %d".formatted(telegramId, surplusSize));
        if (surplusSize <= 0) {
            log.info("No available download slots for telegram %d (limit reached)".formatted(telegramId));
            return;
        }

        List<MessageWrapper> downloadMessages = new ArrayList<>();
        while (downloadMessages.size() < Math.min(surplusSize, messages.size()) && !messages.isEmpty()) {
            MessageWrapper wrapper = messages.poll();
            if (wrapper != null) {
                downloadMessages.add(wrapper);
            }
        }
        log.debug("Download start! TelegramId: %d size: %d".formatted(telegramId, downloadMessages.size()));

        downloadMessages.forEach(messageWrapper -> startDownloadWithRetry(telegramId, telegramVerticle, messageWrapper));
        log.debug("Remaining download messages: %d".formatted(messages.size()));
        publishQueueSize(telegramId, messages == null ? 0 : messages.size());
    }

    private void startDownloadWithRetry(long telegramId, TelegramVerticle telegramVerticle, MessageWrapper messageWrapper) {
        TdApi.Message message = messageWrapper.message();
        String uniqueId = TdApiHelp.getFileUniqueId(message);
        if (StrUtil.isBlank(uniqueId)) {
            log.warn("Skip download due to missing unique id. ChatId: %d MessageId:%d".formatted(message.chatId, message.id));
            return;
        }
        if (!activeDownloads.add(uniqueId)) {
            log.debug("Duplicate download detected, skip: %s".formatted(uniqueId));
            return;
        }
        publishMetricUpdate(new JsonObject()
                .put("type", "downloadStart")
                .put("uniqueId", uniqueId)
                .put("telegramId", telegramId)
                .put("queued", waitingDownloadMessages.getOrDefault(telegramId, new ConcurrentLinkedQueue<>()).size()));
        RetryContext context = retryContexts.computeIfAbsent(uniqueId, key -> new RetryContext());
        attemptDownload(telegramId, telegramVerticle, messageWrapper, uniqueId, context);
    }

    private void attemptDownload(long telegramId,
                                 TelegramVerticle telegramVerticle,
                                 MessageWrapper messageWrapper,
                                 String uniqueId,
                                 RetryContext context) {
        TdApi.Message message = messageWrapper.message();
        Integer fileId = TdApiHelp.getFileId(message);
        log.debug("Start download file: %s".formatted(fileId));
        telegramVerticle.startDownload(message.chatId, message.id, fileId)
                .onSuccess(fileRecord -> {
                    log.info("Start download file success! ChatId: %d MessageId:%d FileId:%d"
                            .formatted(message.chatId, message.id, fileId));
                    activeDownloads.remove(uniqueId);
                    retryContexts.remove(uniqueId);
                    if (fileRecord.threadChatId() != 0
                        && fileRecord.messageThreadId() != 0
                        && fileRecord.threadChatId() != fileRecord.chatId()) {
                        waitingScanThreads.computeIfAbsent(telegramId, k -> new LinkedList<>())
                                .add(new WaitingScanThread(telegramId, fileRecord.threadChatId(), fileRecord.messageThreadId()));
                    }
                })
                .onFailure(e -> handleDownloadFailure(telegramId, messageWrapper, uniqueId, context, e));
    }

    private void handleDownloadFailure(long telegramId,
                                       MessageWrapper messageWrapper,
                                       String uniqueId,
                                       RetryContext context,
                                       Throwable e) {
        log.error("Download file failed! ChatId: %d MessageId:%d UniqueId:%s"
                .formatted(messageWrapper.message().chatId, messageWrapper.message().id, uniqueId), e);
        activeDownloads.remove(uniqueId);
        context.recordFailure(e);
        if (isPermanentMessageFailure(e)) {
            markPermanentFailure(uniqueId, messageWrapper.message());
            retryContexts.remove(uniqueId);
            publishQueueSize(telegramId, waitingDownloadMessages.getOrDefault(telegramId, new ConcurrentLinkedQueue<>()).size());
            return;
        }
        if (!context.shouldRetry(downloadRetryAttempts)) {
            retryContexts.remove(uniqueId);
            return;
        }

        publishMetricUpdate(new JsonObject()
                .put("type", "downloadFailure")
                .put("uniqueId", uniqueId)
                .put("retryCount", context.getAttempts()));

        long delay = context.nextDelayMillis();
        vertx.setTimer(delay, id -> {
            waitingDownloadMessages.computeIfAbsent(telegramId, key -> new ConcurrentLinkedQueue<>())
                    .offer(messageWrapper);
            publishQueueSize(telegramId, waitingDownloadMessages
                    .getOrDefault(telegramId, new ConcurrentLinkedQueue<>())
                    .size());
        });
    }

    private boolean isPermanentMessageFailure(Throwable e) {
        if (e == null || e.getMessage() == null) {
            return false;
        }
        String msg = e.getMessage();
        return msg.contains("Not Found")
                || msg.contains("MSG_ID_INVALID")
                || msg.contains("message to be replied not found");
    }

    private void markPermanentFailure(String uniqueId, TdApi.Message message) {
        DataVerticle.fileRepository.getByUniqueId(uniqueId)
                .compose(record -> {
                    if (record == null) {
                        return Future.succeededFuture();
                    }
                    return DataVerticle.fileRepository.updateDownloadStatus(
                                    record.id(),
                                    uniqueId,
                                    record.localPath(),
                                    FileRecord.DownloadStatus.permanently_failed,
                                    System.currentTimeMillis())
                            .mapEmpty();
                })
                .onFailure(err -> log.error("Failed to mark message permanently failed. ChatId:{} MessageId:{} UniqueId:{}", message.chatId, message.id, uniqueId, err))
                .onSuccess(v -> log.warn("Message permanently unavailable, marked failed. ChatId:{} MessageId:{} UniqueId:{}", message.chatId, message.id, uniqueId));
    }

    private void onNewMessage(JsonObject jsonObject) {
        long telegramId = jsonObject.getLong("telegramId");
        long chatId = jsonObject.getLong("chatId");
        long messageId = jsonObject.getLong("messageId");
        autoRecords.getDownloadEnabledItems().stream()
                .filter(item -> item.telegramId == telegramId && item.chatId == chatId)
                .findFirst()
                .flatMap(item -> TelegramVerticles.get(telegramId))
                .ifPresent(telegramVerticle -> {
                    if (telegramVerticle.authorized) {
                        telegramVerticle.client.execute(new TdApi.GetMessage(chatId, messageId))
                                .onSuccess(message -> addWaitingDownloadMessages(telegramId, List.of(message), true, false))
                                .onFailure(e -> log.error("Auto download fail. Get message failed: %s".formatted(e.getMessage())));
                    }
                });
    }

    private void publishQueueSize(long telegramId, int queueSize) {
        publishMetricUpdate(new JsonObject()
                .put("type", "queueState")
                .put("telegramId", telegramId)
                .put("queued", queueSize)
                .put("active", activeDownloads.size()));
    }

    private void publishMetricUpdate(JsonObject payload) {
        vertx.eventBus().publish(PerformanceMonitorVerticle.METRICS_UPDATE_ADDRESS, payload);
    }

    private static class ScanParams {
        public String uniqueKey;

        public SettingAutoRecords.DownloadRule rule;

        public long telegramId;

        public long chatId;

        public long messageThreadId;

        public String nextFileType;

        public long nextFromMessageId;

        public ScanParams(String uniqueKey,
                          SettingAutoRecords.DownloadRule rule,
                          long telegramId,
                          long chatId,
                          String nextFileType,
                          long nextFromMessageId) {
            this.uniqueKey = uniqueKey;
            this.rule = rule;
            this.telegramId = telegramId;
            this.chatId = chatId;
            this.nextFileType = nextFileType;
            this.nextFromMessageId = nextFromMessageId;
        }
    }

    private static class ScanResult {
        public String nextFileType;

        public long nextFromMessageId;

        public boolean isComplete;

        public ScanResult(String nextFileType, long nextFromMessageId, boolean isComplete) {
            this.nextFileType = nextFileType;
            this.nextFromMessageId = nextFromMessageId;
            this.isComplete = isComplete;
        }
    }

    private static class WaitingScanThread {
        public long telegramId;

        public long threadChatId;

        public long messageThreadId;

        public String nextFileType;

        public long nextFromMessageId;

        public boolean isComplete;

        public WaitingScanThread(long telegramId, long threadChatId, long messageThreadId) {
            this.telegramId = telegramId;
            this.threadChatId = threadChatId;
            this.messageThreadId = messageThreadId;
        }
    }

    private record MessageWrapper(TdApi.Message message, boolean isHistorical, int priority)
            implements Comparable<MessageWrapper> {

        @Override
        public int compareTo(MessageWrapper other) {
            int priorityCompare = Integer.compare(this.priority, other.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            return Long.compare(other.message.date, this.message.date);
        }
    }
}

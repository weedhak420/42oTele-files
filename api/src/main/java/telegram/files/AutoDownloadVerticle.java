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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AutoDownloadVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    private static final int MAX_HISTORY_SCAN_TIME = 10 * 1000;

    private static final List<String> DEFAULT_FILE_TYPE_ORDER = List.of("photo", "video", "audio", "file");

    // telegramId -> messages
    private final Map<Long, PriorityQueue<MessageWrapper>> waitingDownloadMessages = new ConcurrentHashMap<>();

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

    public AutoDownloadVerticle() {
        this.autoRecords = AutomationsHolder.INSTANCE.autoRecords();
        AutomationsHolder.INSTANCE.registerOnRemoveListener(removedItems -> removedItems.forEach(item ->
                waitingDownloadMessages.getOrDefault(item.telegramId, new PriorityQueue<>(MessageWrapper.PRIORITY_COMPARATOR))
                        .removeIf(m -> m.message.chatId == item.chatId)));
    }

    @Override
    public void start(Promise<Void> startPromise) {
        initAutoDownload()
                .compose(v -> this.initEventConsumer())
                .onSuccess(v -> {
                    restartHistoryScanTimer();
                    restartDownloadTimer();
                    startCleanupTimer();

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
        cleanupTimerId = vertx.setPeriodic(Duration.ofHours(1).toMillis(), id -> cleanupOldData());
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
                            PriorityQueue<MessageWrapper> messageWrappers = waitingDownloadMessages.get(auto.telegramId);
                            if (CollUtil.isEmpty(messageWrappers)
                                || messageWrappers.stream().noneMatch(w -> w.isHistorical)) {
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
                .forEach(queue -> queue.removeIf(wrapper -> wrapper.isHistorical
                        && Instant.ofEpochSecond(wrapper.message.date).isBefore(historyThreshold)));

        Instant retryThreshold = now.minus(Duration.ofHours(1));
        retryContexts.entrySet().removeIf(entry -> Instant.ofEpochMilli(entry.getValue().getLastRetryTime()).isBefore(retryThreshold));
    }

    private Future<Void> initAutoDownload() {
        ConfigurationService configurationService = DataVerticle.configurationService;
        this.historyScanInterval = configurationService.getValue("autoDownload", "historyScanInterval", Integer.class);
        this.downloadInterval = configurationService.getValue("autoDownload", "downloadInterval", Integer.class);
        this.maxWaitingLength = configurationService.getValue("autoDownload", "maxWaitingLength", Integer.class);
        this.limit = configurationService.getValue("autoDownload", "maxConcurrentDownloads", Integer.class);
        this.defaultLimit = this.limit;
        this.adaptiveThrottlingEnabled = configurationService.getValue("autoDownload", "enableAdaptiveThrottling", Boolean.class);
        this.retryAttempts = configurationService.getValue("autoDownload", "retryAttempts", Integer.class);
        this.retryDelayMs = configurationService.getValue("autoDownload", "retryDelayMs", Long.class);
        adaptiveThrottler.reset(limit);
        return DataVerticle.settingRepository.<SettingTimeLimitedDownload>getByKey(SettingKey.autoDownloadTimeLimited)
                .onSuccess(value -> this.timeLimited = value)
                .compose(v -> ScanCheckpoint.load(DataVerticle.settingRepository)
                        .onSuccess(scanCheckpoints::putAll)
                        .mapEmpty())
                .onFailure(e -> log.error("Get Auto download settings failed!", e));
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
                            updateCheckpoint(uniqueKey, chatId, nextFileType, params.nextFromMessageId);
                            addHistoryMessage(params, callback, currentTimeMillis);
                        } else {
                            boolean added = scanChunk(telegramId, messages, true, rule.v2);
                            params.nextFromMessageId = foundChatMessages.nextFromMessageId;
                            updateCheckpoint(uniqueKey, chatId, nextFileType, params.nextFromMessageId);
                            if (added) {
                                addHistoryMessage(params, callback, currentTimeMillis);
                            }
                        }
                    });
        }
    }

    private boolean scanChunk(long telegramId, List<TdApi.Message> messages, boolean isHistorical, List<String> fileTypeOrder) {
        if (CollUtil.isEmpty(messages)) {
            return false;
        }
        int chunkSize = Math.max(1, Math.min(100, Math.min(maxWaitingLength, calculateOptimalChunkSize(telegramId))));
        boolean added = false;
        for (int i = 0; i < messages.size(); i += chunkSize) {
            List<TdApi.Message> chunk = messages.subList(i, Math.min(messages.size(), i + chunkSize));
            if (!addWaitingDownloadMessages(telegramId, chunk, false, isHistorical, fileTypeOrder)) {
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
        List<MessageWrapper> waitingMessages = this.waitingDownloadMessages.get(telegramId);
        return getSurplusSize(telegramId) <= 0 || (waitingMessages != null && waitingMessages.size() > maxWaitingLength);
    }

    private int getSurplusSize(long telegramId) {
        Integer downloading = Future.await(DataVerticle.fileRepository.countByStatus(telegramId, FileRecord.DownloadStatus.downloading));
        int effectiveLimit = getEffectiveLimit(telegramId);
        return downloading == null ? effectiveLimit : Math.max(0, effectiveLimit - downloading);
    }

    private int getEffectiveLimit(long telegramId) {
        int configuredLimit = limit == 0 ? defaultLimit : limit;
        if (!adaptiveThrottlingEnabled) {
            return configuredLimit;
        }
        if (telegramId == 0) {
            return adaptiveThrottler.getCurrentLimit(configuredLimit);
        }
        return TelegramVerticles.get(telegramId)
                .map(telegramVerticle -> adaptiveThrottler.calculateLimit(getAverageSpeed(telegramVerticle), configuredLimit))
                .orElse(adaptiveThrottler.getCurrentLimit(configuredLimit));
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
        return addWaitingDownloadMessages(telegramId, messages, force, isHistorical, DEFAULT_FILE_TYPE_ORDER);
    }

    private boolean addWaitingDownloadMessages(long telegramId,
                                               List<TdApi.Message> messages,
                                               boolean force,
                                               boolean isHistorical,
                                               List<String> fileTypeOrder) {
        if (CollUtil.isEmpty(messages)) {
            return false;
        }
        PriorityQueue<MessageWrapper> waitingMessages = this.waitingDownloadMessages.get(telegramId);
        if (waitingMessages == null) {
            waitingMessages = new PriorityQueue<>(MessageWrapper.PRIORITY_COMPARATOR);
        }
        if (!force && waitingMessages.size() > maxWaitingLength) {
            return false;
        } else {
            log.debug("Add waiting download messages: %d".formatted(messages.size()));
            waitingMessages.addAll(TdApiHelp.filterUniqueMessages(messages)
                    .stream()
                    .map(message -> new MessageWrapper(message, isHistorical, fileTypeOrder))
                    .toList()
            );
            publishQueueSize(telegramId, waitingMessages.size());
        }
        this.waitingDownloadMessages.put(telegramId, waitingMessages);
        return true;
    }

    private void download(long telegramId) {
        if (CollUtil.isEmpty(waitingDownloadMessages)) {
            return;
        }
        PriorityQueue<MessageWrapper> messages = waitingDownloadMessages.get(telegramId);
        if (CollUtil.isEmpty(messages)) {
            return;
        }
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(telegramId);
        int surplusSize = getSurplusSize(telegramId);
        if (surplusSize <= 0) {
            return;
        }

        List<MessageWrapper> downloadMessages = new ArrayList<>();
        IntStream.range(0, Math.min(surplusSize, messages.size()))
                .forEach(i -> {
                    MessageWrapper wrapper = messages.poll();
                    if (wrapper != null) {
                        downloadMessages.add(wrapper);
                    }
                });
        log.debug("Download start! TelegramId: %d size: %d".formatted(telegramId, downloadMessages.size()));

        downloadMessages.forEach(messageWrapper -> startDownloadWithRetry(telegramId, telegramVerticle, messageWrapper));
        log.debug("Remaining download messages: %d".formatted(messages.size()));
        publishQueueSize(telegramId, messages == null ? 0 : messages.size());
    }

    private void startDownloadWithRetry(long telegramId, TelegramVerticle telegramVerticle, MessageWrapper messageWrapper) {
        TdApi.Message message = messageWrapper.message;
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
                .put("queued", waitingDownloadMessages.getOrDefault(telegramId, new PriorityQueue<>(MessageWrapper.PRIORITY_COMPARATOR)).size()));
        RetryContext context = retryContexts.computeIfAbsent(uniqueId, key -> new RetryContext());
        attemptDownload(telegramId, telegramVerticle, messageWrapper, uniqueId, context);
    }

    private void attemptDownload(long telegramId,
                                 TelegramVerticle telegramVerticle,
                                 MessageWrapper messageWrapper,
                                 String uniqueId,
                                 RetryContext context) {
        TdApi.Message message = messageWrapper.message;
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
                .formatted(messageWrapper.message.chatId, messageWrapper.message.id, uniqueId), e);
        activeDownloads.remove(uniqueId);
        context.recordFailure(e);
        if (context.getRetryCount() > retryAttempts) {
            retryContexts.remove(uniqueId);
            return;
        }

        publishMetricUpdate(new JsonObject()
                .put("type", "downloadFailure")
                .put("uniqueId", uniqueId)
                .put("retryCount", context.getRetryCount()));

        long delay = retryDelayMs * (long) Math.pow(2, context.getRetryCount());
        vertx.setTimer(delay, id -> {
            waitingDownloadMessages.computeIfAbsent(telegramId, key -> new PriorityQueue<>(MessageWrapper.PRIORITY_COMPARATOR))
                    .offer(messageWrapper);
            publishQueueSize(telegramId, waitingDownloadMessages
                    .getOrDefault(telegramId, new PriorityQueue<>(MessageWrapper.PRIORITY_COMPARATOR))
                    .size());
        });
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

    private static class MessageWrapper {

        public static final Comparator<MessageWrapper> PRIORITY_COMPARATOR = Comparator
                .comparingInt(MessageWrapper::getFileTypeRank)
                .thenComparingLong(MessageWrapper::getFileSize)
                .thenComparing((MessageWrapper o1, MessageWrapper o2) -> Long.compare(o2.messageDate, o1.messageDate));

        private final TdApi.Message message;

        private final boolean isHistorical;

        private final int fileTypeRank;

        private final long fileSize;

        private final long messageDate;

        public MessageWrapper(TdApi.Message message, boolean isHistorical, List<String> fileTypeOrder) {
            this.message = message;
            this.isHistorical = isHistorical;
            this.fileTypeRank = calculateFileTypeRank(message, fileTypeOrder);
            this.fileSize = calculateFileSize(message);
            this.messageDate = Convert.toLong(message.date);
        }

        private int calculateFileTypeRank(TdApi.Message message, List<String> fileTypeOrder) {
            String fileType = getFileType(message);
            int index = fileTypeOrder.indexOf(fileType);
            return index >= 0 ? index : Integer.MAX_VALUE;
        }

        private String getFileType(TdApi.Message message) {
            return switch (message.content.getConstructor()) {
                case TdApi.MessagePhoto.CONSTRUCTOR -> "photo";
                case TdApi.MessageVideo.CONSTRUCTOR -> "video";
                case TdApi.MessageAudio.CONSTRUCTOR -> "audio";
                case TdApi.MessageDocument.CONSTRUCTOR -> "file";
                default -> "unknown";
            };
        }

        private long calculateFileSize(TdApi.Message message) {
            return TdApiHelp.getFileHandler(message)
                    .map(handler -> {
                        TdApi.File file = handler.getFile();
                        if (file == null) {
                            return Long.MAX_VALUE;
                        }
                        return file.size == 0 ? file.expectedSize : file.size;
                    })
                    .orElse(Long.MAX_VALUE);
        }

        private int getFileTypeRank() {
            return fileTypeRank;
        }

        private long getFileSize() {
            return fileSize;
        }
    }
}

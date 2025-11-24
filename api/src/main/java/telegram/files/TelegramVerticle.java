package telegram.files;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.codec.Base64;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.drinkless.tdlib.TdApi;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import telegram.files.repository.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TelegramVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    public TelegramClient client;

    private TelegramChats telegramChats;

    public boolean authorized = false;

    public TdApi.AuthorizationState lastAuthorizationState;

    public String rootPath;

    private String proxyName;

    private String rootId;

    private boolean needDelete = false;

    public TelegramRecord telegramRecord;

    private AvgSpeed avgSpeed = new AvgSpeed();

    private long avgSpeedPersistenceTimerId;

    private long lastFileEventTime;

    private long lastFileDownloadEventTime;

    private long lastActivityTime;

    private long keepAliveTimerId;

    private LoadingCache<Long, TdApi.Chat> chatCache;

    private long keepAliveInterval;

    private int connectionTimeoutMs;

    private int networkMaxRetries;

    private double backoffMultiplier;

    private boolean enableCaching;

    private int cacheSize;

    private int cacheExpirationMinutes;

    private boolean enableMetrics;

    private int batchSize;

    public TelegramVerticle(String rootPath) {
        this.rootPath = rootPath;
    }

    public TelegramVerticle(TelegramRecord telegramRecord) {
        this.telegramRecord = telegramRecord;
        this.rootPath = telegramRecord.rootPath();
        this.proxyName = telegramRecord.proxy();
    }

    public String getRootId() {
        if (StrUtil.isNotBlank(this.rootId)) return rootId;

        this.rootId = StrUtil.subAfter(this.rootPath, '-', true);
        return this.rootId;
    }

    public Object getId() {
        return telegramRecord == null ? this.getRootId() : telegramRecord.id();
    }

    public void setProxy(String proxyName) {
        this.proxyName = proxyName;
    }

    private void loadConfiguration() {
        ConfigurationService configurationService = DataVerticle.configurationService;
        this.enableCaching = configurationService.getValue("performance", "enableCaching", Boolean.class);
        this.cacheSize = configurationService.getValue("performance", "cacheSize", Integer.class);
        this.cacheExpirationMinutes = configurationService.getValue("performance", "cacheExpirationMinutes", Integer.class);
        this.batchSize = configurationService.getValue("performance", "batchSize", Integer.class);
        this.enableMetrics = configurationService.getValue("performance", "enableMetrics", Boolean.class);

        this.connectionTimeoutMs = configurationService.getValue("network", "connectionTimeout", Integer.class);
        this.keepAliveInterval = configurationService.getValue("network", "keepAliveInterval", Long.class);
        this.networkMaxRetries = configurationService.getValue("network", "maxRetries", Integer.class);
        this.backoffMultiplier = configurationService.getValue("network", "backoffMultiplier", Double.class);
    }

    private void initChatCache() {
        if (!enableCaching) {
            chatCache = null;
            return;
        }
        chatCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpirationMinutes, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public TdApi.Chat load(Long chatId) throws Exception {
                        TdApi.Chat chat = telegramChats.getChat(chatId);
                        if (chat != null) {
                            return chat;
                        }
                        return fetchChatBlocking(chatId);
                    }
                });
    }

    private TdApi.Chat fetchChatBlocking(long chatId) throws Exception {
        Promise<TdApi.Chat> promise = Promise.promise();
        executeWithContext(new TdApi.GetChat(chatId), "getChat", chatId, null, null)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);
        return promise.future().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private Future<Void> deleteFileAsync(String path) {
        if (StrUtil.isBlank(path)) {
            return Future.succeededFuture();
        }
        return vertx.fileSystem().exists(path)
                .compose(exists -> {
                    if (exists) {
                        return vertx.fileSystem().deleteRecursive(path);
                    }
                    return Future.succeededFuture();
                });
    }

    private Future<Boolean> existsAsync(String path) {
        if (StrUtil.isBlank(path)) {
            return Future.succeededFuture(false);
        }
        return vertx.fileSystem().exists(path).map(Boolean::valueOf);
    }

    private void setupKeepAlive() {
        if (keepAliveTimerId != 0) {
            vertx.cancelTimer(keepAliveTimerId);
        }
        keepAliveTimerId = vertx.setPeriodic(keepAliveInterval, id -> {
            if (System.currentTimeMillis() - lastActivityTime > keepAliveInterval) {
                executeWithContext(new TdApi.GetMe(), "keepAlive", null, null, null)
                        .onSuccess(r -> log.trace("[%s] keep-alive ping succeeded".formatted(getRootId())))
                        .onFailure(err -> log.warn("[%s] keep-alive ping failed: %s".formatted(getRootId(), err.getMessage())));
            }
        });
    }

    @Override
    public void start(Promise<Void> startPromise) {
        client = new TelegramClient();
        telegramChats = new TelegramChats(client);
        loadConfiguration();
        lastActivityTime = System.currentTimeMillis();
        initChatCache();
        TelegramUpdateHandler telegramUpdateHandler = new TelegramUpdateHandler();
        telegramUpdateHandler.setOnAuthorizationStateUpdated(this::onAuthorizationStateUpdated);
        telegramUpdateHandler.setOnFileUpdated(this::onFileUpdated);
        telegramUpdateHandler.setOnFileDownloadsUpdated(this::onFileDownloadsUpdated);
        telegramUpdateHandler.setOnChatUpdated(telegramChats::onChatUpdated);
        telegramUpdateHandler.setOnMessageReceived(this::onMessageReceived);

        client.initialize(telegramUpdateHandler, this::handleException, this::handleException);
        Future.all(initEventConsumer(), initAvgSpeed())
                .compose(r -> this.enableProxy(this.proxyName))
                .onSuccess(r -> {
                    setupKeepAlive();
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.close(false)
                .onComplete(stopPromise);
    }

    public Future<Void> close(boolean needDelete) {
        return executeWithContext(new TdApi.Close(), "close", null, null, null)
                .onSuccess(r -> {
                    log.info("[%s] Telegram account closed".formatted(this.getRootId()));
                    this.needDelete = needDelete;
                })
                .onFailure(e -> log.error("[%s] Failed to close telegram account: %s".formatted(this.getRootId(), e.getMessage())))
                .mapEmpty();
    }

    public boolean check() {
        if (StrUtil.isBlank(this.rootPath) || !vertx.fileSystem().existsBlocking(this.rootPath)) {
            log.error("[%s] Telegram account is invalid, root path: %s not exist.".formatted(this.getRootId(), this.rootPath));
            return false;
        }
        return true;
    }

    public Future<JsonObject> getTelegramAccount() {
        return Future.future(promise -> {
            if (!authorized) {
                JsonObject jsonObject = new JsonObject()
                        .put("id", this.getRootId())
                        .put("name", this.getRootId())
                        .put("phoneNumber", "")
                        .put("avatar", "")
                        .put("status", "inactive")
                        .put("rootPath", this.rootPath)
                        .put("isPremium", false)
                        .put("lastAuthorizationState", lastAuthorizationState)
                        .put("proxy", this.proxyName);
                if (this.telegramRecord != null) {
                    jsonObject.put("id", Convert.toStr(this.telegramRecord.id()))
                            .put("name", this.telegramRecord.firstName());
                }
                promise.complete(jsonObject);
                return;
            }
            executeWithContext(new TdApi.GetMe(), "getMe", null, null, null)
                    .onSuccess(user -> {
                        JsonObject result = new JsonObject()
                                .put("id", Convert.toStr(user.id))
                                .put("name", StrUtil.join(user.firstName, " ", user.lastName))
                                .put("phoneNumber", user.phoneNumber)
                                .put("avatar", Base64.encode((byte[]) BeanUtil.getProperty(user, "profilePhoto.minithumbnail.data")))
                                .put("status", "active")
                                .put("rootPath", this.rootPath)
                                .put("isPremium", user.isPremium)
                                .put("proxy", this.proxyName);
                        promise.complete(result);
                    })
                    .onFailure(e -> {
                        log.error("[%s] Failed to get telegram account: %s".formatted(this.getRootId(), e.getMessage()));
                        promise.fail(e);
                    });
        });
    }

    public Future<JsonArray> getChats(Long activatedChatId, String query, boolean archived) {
        return TelegramConverter.convertChat(this.telegramRecord.id(), telegramChats.getChatList(activatedChatId, query, 100, archived));
    }

    public TdApi.Chat getChat(long chatId) {
        if (chatCache == null) {
            return telegramChats.getChat(chatId);
        }
        try {
            TdApi.Chat cached = chatCache.getIfPresent(chatId);
            if (cached != null) {
                publishMetricUpdate(new JsonObject().put("type", "cacheAccess").put("hit", true));
                return cached;
            }
            TdApi.Chat chat = chatCache.get(chatId);
            publishMetricUpdate(new JsonObject().put("type", "cacheAccess").put("hit", false));
            return chat;
        } catch (ExecutionException e) {
            logErrorContext("getChat", chatId, null, null, e);
            return telegramChats.getChat(chatId);
        }
    }

    public Future<JsonObject> getChatFiles(long chatId, Map<String, String> filter) {
        boolean offline = Convert.toBool(filter.get("offline"), false);
        if (offline) {
            return FileRecordRetriever.getFiles(chatId, filter);
        } else {
            TdApi.SearchChatMessages searchChatMessages = new TdApi.SearchChatMessages();
            searchChatMessages.chatId = chatId;
            searchChatMessages.query = filter.get("search");
            searchChatMessages.fromMessageId = Convert.toLong(filter.get("fromMessageId"), 0L);
            searchChatMessages.offset = Convert.toInt(filter.get("offset"), 0);
            searchChatMessages.limit = Convert.toInt(filter.get("limit"), 20);
            searchChatMessages.filter = TdApiHelp.getSearchMessagesFilter(filter.get("type"));
            searchChatMessages.messageThreadId = Convert.toLong(filter.get("messageThreadId"), 0L);

            return (Objects.equals(filter.get("downloadStatus"), FileRecord.DownloadStatus.idle.name()) ?
                    this.getIdleChatFilesOptimized(searchChatMessages) :
                    executeWithContext(searchChatMessages, "searchChatMessages", searchChatMessages.chatId, searchChatMessages.fromMessageId, null))
                    .compose(t -> TelegramConverter.convertFiles(this.telegramRecord.id(), t));
        }
    }

    private Future<TdApi.FoundChatMessages> getIdleChatFilesOptimized(TdApi.SearchChatMessages searchChatMessages) {
        return DataVerticle.fileRepository.getIdleFilesByChatId(searchChatMessages.chatId)
                .compose(idleFiles -> {
                    Set<Long> idleMessageIds = idleFiles.stream().map(FileRecord::messageId).collect(Collectors.toSet());
                    if (idleMessageIds.isEmpty()) {
                        return Future.succeededFuture(new TdApi.FoundChatMessages(0, new TdApi.Message[]{}, 0));
                    }
                    return getIdleChatFiles(searchChatMessages, idleMessageIds, 0);
                });
    }

    private Future<TdApi.FoundChatMessages> getIdleChatFiles(TdApi.SearchChatMessages searchChatMessages, Set<Long> idleMessageIds, int seq) {
        if (seq != 0) {
            searchChatMessages.limit = 100;
        }
        return executeWithContext(searchChatMessages, "searchIdleChatMessages", searchChatMessages.chatId, searchChatMessages.fromMessageId, null)
                .compose(foundChatMessages -> {
                    TdApi.Message[] messages = Stream.of(foundChatMessages.messages)
                            .filter(message -> idleMessageIds.contains(message.id))
                            .toArray(TdApi.Message[]::new);
                    if (ArrayUtil.isEmpty(messages) && foundChatMessages.nextFromMessageId != 0) {
                        searchChatMessages.fromMessageId = foundChatMessages.nextFromMessageId;
                        return getIdleChatFiles(searchChatMessages, idleMessageIds, seq + 1);
                    } else {
                        foundChatMessages.messages = messages;
                        return Future.succeededFuture(foundChatMessages);
                    }
                });
    }

    public Future<JsonObject> getChatFilesCount(long chatId) {
        return Future.all(
                Stream.of(new TdApi.SearchMessagesFilterPhotoAndVideo(),
                                new TdApi.SearchMessagesFilterPhoto(),
                                new TdApi.SearchMessagesFilterVideo(),
                                new TdApi.SearchMessagesFilterAudio(),
                                new TdApi.SearchMessagesFilterDocument())
                        .map(filter -> executeWithContext(
                                new TdApi.GetChatMessageCount(chatId,
                                        filter,
                                        0,
                                        false),
                                "getChatMessageCount",
                                chatId,
                                null,
                                null)
                                .map(count -> new JsonObject()
                                        .put("type", TdApiHelp.getSearchMessagesFilterType(filter))
                                        .put("count", count.count)
                                )
                        )
                        .toList()
        ).map(counts -> {
            JsonObject result = new JsonObject();
            counts.<JsonObject>list().forEach(count -> result.put(count.getString("type"), count.getInteger("count")));
            return result;
        });
    }

    public Future<JsonObject> parseLink(String link) {
        return executeWithContext(new TdApi.GetMessageLinkInfo(link), "parseLink", null, null, null)
                .compose(messageLinkInfo -> {
                    if (messageLinkInfo.message == null) {
                        return Future.failedFuture("Message not found for link: " + link);
                    }
                    return FileRecordRetriever.getAlbumMessages(this.telegramRecord.id(), messageLinkInfo.message);
                })
                .compose(messages -> TelegramConverter.convertFiles(this.telegramRecord.id(), messages)
                        .map(files -> new JsonObject()
                                .put("files", files)
                                .put("count", files.size())
                                .put("size", files.size())
                                .put("nextFromMessageId", 0L) // No next message ID for link parsing
                        ));
    }

    public Future<Tuple2<String, String>> loadPreview(String uniqueId) {
        return DataVerticle.fileRepository
                .getByUniqueId(uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord == null || !fileRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)) {
                        return Future.failedFuture("File not found or not downloaded");
                    }
                    return existsAsync(fileRecord.localPath())
                            .compose(exists -> {
                                if (!exists) {
                                    return Future.failedFuture("File not found or not downloaded");
                                }
                                return Future.succeededFuture(Tuple.tuple(fileRecord.localPath(), fileRecord.mimeType()));
                            });
                });
    }

    public Future<FileRecord> startDownload(Long chatId, Long messageId, Integer fileId) {
        return Future.all(
                        executeWithContext(new TdApi.GetFile(fileId), "getFile", chatId, messageId, fileId),
                        executeWithContext(new TdApi.GetMessage(chatId, messageId), "getMessage", chatId, messageId, fileId),
                        executeWithContext(new TdApi.GetMessageThread(chatId, messageId), "getMessageThread", chatId, messageId, fileId, true, 0)
                )
                .compose(results -> {
                    TdApi.File file = results.resultAt(0);
                    TdApi.Message message = results.resultAt(1);
                    TdApi.MessageThreadInfo messageThreadInfo = results.resultAt(2);
                    if (file.local != null) {
                        if (file.local.isDownloadingCompleted) {
                            return syncFileDownloadStatus(file, message, messageThreadInfo)
                                    .compose(r -> DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId));
                        }
                        if (file.local.isDownloadingActive) {
                            return Future.failedFuture("File is downloading");
                        }
//                        return Future.failedFuture("Unknown file download status");
                    }

                    TdApiHelp.FileHandler<? extends TdApi.MessageContent> fileHandler = TdApiHelp.getFileHandler(message)
                            .orElseThrow(() -> VertxException.noStackTrace("not support message type"));
                    FileRecord fileRecord = fileHandler.convertFileRecord(telegramRecord.id()).withThreadInfo(messageThreadInfo);
                    return DataVerticle.fileRepository.createIfNotExist(fileRecord)
                            .compose(created -> {
                                if (!created) {
                                    return DataVerticle.fileRepository.updateFileId(fileRecord.id(), fileRecord.uniqueId());
                                }
                                return Future.succeededFuture();
                            })
                            .compose(ignore -> executeWithContext(new TdApi.AddFileToDownloads(fileId, chatId, messageId, 32), "addFileToDownloads", chatId, messageId, fileId))
                            .onSuccess(ignore -> {
                                sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                                        .put("fileId", fileId)
                                        .put("uniqueId", fileRecord.uniqueId())
                                        .put("downloadStatus", FileRecord.DownloadStatus.downloading)
                                ));

                                downloadThumbnail(chatId, messageId, fileHandler.convertThumbnailRecord(telegramRecord.id()));
                            })
                            .map(fileRecord)
                            .onSuccess(fr -> recordDownloadOutcome(fr.uniqueId(), true, "started"))
                            .onFailure(err -> recordDownloadOutcome(fileRecord.uniqueId(), false, err.getMessage()));
                });
    }

    public List<Future<FileRecord>> batchStartDownload(long chatId, Map<Long, Integer> messageFiles) {
        int batchLimit = calculateBatchLimit();
        List<Map.Entry<Long, Integer>> entries = new ArrayList<>(messageFiles.entrySet());
        List<Future<FileRecord>> futures = new ArrayList<>();
        for (int i = 0; i < entries.size(); i += batchLimit) {
            List<Future<FileRecord>> chunk = entries.subList(i, Math.min(entries.size(), i + batchLimit))
                    .stream()
                    .map(entry -> startDownload(chatId, entry.getKey(), entry.getValue()))
                    .toList();
            futures.addAll(chunk);
            Future.all(chunk).onFailure(err -> logErrorContext("batchStartDownload", chatId, null, null, err));
        }
        return futures;
    }

    private int calculateBatchLimit() {
        int configuredBatch = Math.max(1, batchSize);
        long avgBytesPerSecond = avgSpeed.getSpeedStats().avgSpeed();
        double mbPerSecond = avgBytesPerSecond / (1024d * 1024d);
        if (mbPerSecond > 5) {
            return Math.min(configuredBatch, 20);
        }
        if (mbPerSecond > 1) {
            return Math.min(configuredBatch, 10);
        }
        return Math.min(configuredBatch, 5);
    }

    public Future<Boolean> downloadThumbnail(Long chatId, Long messageId, FileRecord thumbnailRecord) {
        if (thumbnailRecord == null) {
            return Future.succeededFuture(false);
        }
        return DataVerticle.fileRepository.createIfNotExist(thumbnailRecord)
                .compose(created -> {
                    if (!created) {
                        return DataVerticle.fileRepository.updateFileId(thumbnailRecord.id(), thumbnailRecord.uniqueId());
                    }
                    return Future.succeededFuture();
                })
                .compose(ignore -> {
                    if (thumbnailRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)) {
                        return Future.succeededFuture(false);
                    }
                    return executeWithContext(new TdApi.AddFileToDownloads(thumbnailRecord.id(), chatId, messageId, 32), "addThumbnailToDownloads", chatId, messageId, thumbnailRecord.id())
                            .map(true);
                })
                .onSuccess(download -> {
                    if (download) {
                        log.debug("[%s] Download thumbnail: %s".formatted(this.getRootId(), thumbnailRecord.uniqueId()));
                    }
                });
    }

    public Future<Void> cancelDownload(Integer fileId) {
        return executeWithContext(new TdApi.GetFile(fileId), "getFile", null, null, fileId)
                .compose(file -> DataVerticle.fileRepository
                        .updateFileId(file.id, file.remote.uniqueId)
                        .map(file)
                )
                .compose(file -> {
                    if (file.local == null) {
                        return Future.failedFuture("File not started downloading");
                    }

                    return executeWithContext(new TdApi.CancelDownloadFile(fileId, false), "cancelDownload", null, null, fileId)
                            .map(file);
                })
                .compose(file -> executeWithContext(new TdApi.DeleteFile(fileId), "deleteFile", null, null, fileId).map(file))
                .compose(file -> DataVerticle.fileRepository.deleteByUniqueId(file.remote.uniqueId).map(file))
                .onSuccess(file ->
                        sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                                .put("fileId", fileId)
                                .put("uniqueId", file.remote.uniqueId)
                                .put("downloadStatus", FileRecord.DownloadStatus.idle)
                        )))
                .mapEmpty();
    }

    public Future<Void> togglePauseDownload(Integer fileId, boolean isPaused) {
        return executeWithContext(new TdApi.GetFile(fileId), "getFile", null, null, fileId)
                .compose(file -> DataVerticle.fileRepository
                        .updateFileId(file.id, file.remote.uniqueId)
                        .map(file)
                )
                .compose(file -> {
                    if (file.local == null) {
                        return Future.failedFuture("File not started downloading");
                    }
                    if (file.local.isDownloadingCompleted) {
                        return syncFileDownloadStatus(file, null, null).mapEmpty();
                    }
                    if (isPaused && !file.local.isDownloadingActive) {
                        return Future.failedFuture("File is not downloading");
                    }
                    if (!isPaused && file.local.isDownloadingActive) {
                        return Future.failedFuture("File is downloading");
                    }
                    if (!isPaused && !file.local.canBeDeleted) {
                        // Maybe the file is not exist, so we need to redownload it
                        return DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                                .compose(fileRecord ->
                                        executeWithContext(new TdApi.AddFileToDownloads(fileId, fileRecord.chatId(), fileRecord.messageId(), 32), "addFileToDownloads", fileRecord.chatId(), fileRecord.messageId(), fileId))
                                .mapEmpty();
                    }

                    return executeWithContext(new TdApi.ToggleDownloadIsPaused(fileId, isPaused), "toggleDownloadPaused", null, null, fileId);
                })
                .mapEmpty();
    }

    public Future<Void> removeFile(Integer fileId, String uniqueId) {
        return executeWithContext(new TdApi.GetFile(fileId), "getFile", null, null, fileId)
                .otherwise((TdApi.File) null)
                .compose(file -> DataVerticle.fileRepository
                        .getByUniqueId(uniqueId)
                        .map(fileRecord -> Tuple.tuple(file, fileRecord))
                )
                .compose(tuple2 -> {
                    TdApi.File file = tuple2.v1;
                    FileRecord fileRecord = tuple2.v2;
                    if (fileRecord == null) {
                        return Future.failedFuture("File not found");
                    }

                    Future<TdApi.File> deletionFuture = Future.succeededFuture(file);
                    if (fileRecord.isTransferStatus(FileRecord.TransferStatus.completed)) {
                        deletionFuture = deleteFileAsync(fileRecord.localPath())
                                .map(ignore -> file)
                                .onSuccess(ignore -> log.debug("[%s] Remove file success: %s".formatted(this.getRootId(), fileRecord.localPath())));
                    }

                    if (file != null && file.local != null && StrUtil.isNotBlank(file.local.path)) {
                        deletionFuture = executeWithContext(new TdApi.DeleteFile(fileId), "deleteFile", fileRecord.chatId(), fileRecord.messageId(), fileId)
                                .map(file);
                    } else if (!fileRecord.isTransferStatus(FileRecord.TransferStatus.completed)
                               && StrUtil.isNotBlank(fileRecord.localPath())) {
                        deletionFuture = deleteFileAsync(fileRecord.localPath())
                                .map(ignore -> file)
                                .onSuccess(ignore -> log.debug("[%s] Remove file success: %s".formatted(this.getRootId(), fileRecord.localPath())));
                    }
                    return deletionFuture;
                })
                .compose(file -> DataVerticle.fileRepository.deleteByUniqueId(uniqueId).map(file))
                .onSuccess(file -> sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                        .put("fileId", fileId)
                        .put("uniqueId", uniqueId)
                        .put("removed", true)
                )))
                .mapEmpty();
    }

    public Future<Void> updateAutoSettings(Long chatId, JsonObject params) {
        return DataVerticle.settingRepository.<SettingAutoRecords>getByKey(SettingKey.automation)
                .compose(settingAutoRecords -> {
                    if (settingAutoRecords == null) {
                        settingAutoRecords = new SettingAutoRecords();
                    }
                    SettingAutoRecords.Automation automation = params.mapTo(SettingAutoRecords.Automation.class);
                    boolean hasEnabled = automation.preload.enabled
                                         || automation.download.enabled
                                         || automation.transfer.enabled;

                    if (settingAutoRecords.exists(this.telegramRecord.id(), chatId) && !hasEnabled) {
                        settingAutoRecords.remove(this.telegramRecord.id(), chatId);
                    } else {
                        if (!hasEnabled) {
                            return Future.succeededFuture();
                        }
                        automation.telegramId = this.telegramRecord.id();
                        automation.chatId = chatId;
                        settingAutoRecords.add(automation);
                    }

                    return DataVerticle.settingRepository.createOrUpdate(SettingKey.automation.name(), Json.encode(settingAutoRecords))
                            .onSuccess(r -> vertx.eventBus().publish(EventEnum.AUTO_DOWNLOAD_UPDATE.name(), r.value()));
                })
                .mapEmpty();
    }

    public Future<JsonObject> getDownloadStatistics() {
        return Future.all(DataVerticle.fileRepository.getDownloadStatistics(this.telegramRecord.id()),
                executeWithContext(new TdApi.GetNetworkStatistics(), "getNetworkStatistics", null, null, null)
        ).map(r -> {
            JsonObject jsonObject = r.resultAt(0);
            TdApi.NetworkStatistics networkStatistics = r.resultAt(1);
            Tuple2<Long, Long> bytes = Arrays.stream(networkStatistics.entries)
                    .filter(e -> e instanceof TdApi.NetworkStatisticsEntryFile)
                    .map(e -> {
                        TdApi.NetworkStatisticsEntryFile entry = (TdApi.NetworkStatisticsEntryFile) e;
                        return Tuple.tuple(entry.sentBytes, entry.receivedBytes);
                    })
                    .reduce((a, b) -> Tuple.tuple(a.v1 + b.v1, a.v2 + b.v2))
                    .orElse(Tuple.tuple(0L, 0L));

            jsonObject.put("networkStatistics", JsonObject.of()
                    .put("sinceDate", networkStatistics.sinceDate)
                    .put("sentBytes", bytes.v1)
                    .put("receivedBytes", bytes.v2)
            );

            jsonObject.put("speedStats", avgSpeed.getSpeedStats());
            return jsonObject;
        });
    }

    public Future<JsonObject> getDownloadStatisticsByPhase(Integer timeRange) {
        // 1: 1 hour, 2: 1 day, 3: 1 week, 4: 1 month
        long endTime = System.currentTimeMillis();
        long startTime = switch (timeRange) {
            case 1 -> DateUtil.offsetHour(DateUtil.date(), -1).getTime();
            case 2 -> DateUtil.offsetDay(DateUtil.date(), -1).getTime();
            case 3 -> DateUtil.offsetWeek(DateUtil.date(), -1).getTime();
            case 4 -> DateUtil.offsetMonth(DateUtil.date(), -1).getTime();
            default -> throw new IllegalStateException("Unexpected value: " + timeRange);
        };

        return Future.all(
                        DataVerticle.statisticRepository.getRangeStatistics(StatisticRecord.Type.speed, this.telegramRecord.id(), startTime, endTime)
                                .map(statisticRecords -> TelegramConverter.convertRangedSpeedStats(statisticRecords, timeRange)),
                        DataVerticle.fileRepository.getCompletedRangeStatistics(this.telegramRecord.id(), startTime, endTime, timeRange)
                )
                .map(r -> new JsonObject()
                        .put("speedStats", r.resultAt(0))
                        .put("completedStats", r.resultAt(1))
                );
    }

    public Future<TdApi.Proxy> enableProxy(String proxyName) {
        if (StrUtil.isBlank(proxyName)) return Future.succeededFuture();
        return DataVerticle.settingRepository.<SettingProxyRecords>getByKey(SettingKey.proxys)
                .map(settingProxyRecords -> Optional.ofNullable(settingProxyRecords)
                        .flatMap(r -> r.getProxy(proxyName))
                        .orElseThrow(() -> VertxException.noStackTrace("Proxy %s not found".formatted(proxyName)))
                )
                .compose(proxy -> this.getTdProxy(proxy)
                        .map(r -> Tuple.tuple(proxy, r))
                )
                .compose(tuple -> {
                    SettingProxyRecords.Item proxy = tuple.v1;
                    TdApi.Proxy tdProxy = tuple.v2;
                    boolean edit = false;
                    if (tdProxy != null) {
                        if (tdProxy.isEnabled) {
                            return Future.succeededFuture(tdProxy);
                        }
                        edit = true;
                    }

                    TdApi.ProxyType proxyType;
                    switch (proxy.type) {
                        case "http" -> proxyType = new TdApi.ProxyTypeHttp(proxy.username, proxy.password, false);
                        case "socks5" -> proxyType = new TdApi.ProxyTypeSocks5(proxy.username, proxy.password);
                        case "mtproto" -> proxyType = new TdApi.ProxyTypeMtproto(proxy.secret);
                        case null, default -> {
                            return Future.failedFuture("Unsupported proxy type: %s".formatted(proxy.type));
                        }
                    }
                    return edit ? executeWithContext(new TdApi.EditProxy(tdProxy.id, proxy.server, proxy.port, true, proxyType), "editProxy", null, null, null)
                            : executeWithContext(new TdApi.AddProxy(proxy.server, proxy.port, true, proxyType), "addProxy", null, null, null);
                })
                .compose(r -> {
                    this.proxyName = proxyName;
                    if (this.telegramRecord != null) {
                        return DataVerticle.telegramRepository.update(this.telegramRecord.withProxy(proxyName))
                                .onSuccess(telegramRecord -> this.telegramRecord = telegramRecord)
                                .map(r);
                    } else {
                        return Future.succeededFuture(r);
                    }
                });
    }

    public Future<TdApi.Proxy> toggleProxy(JsonObject jsonObject) {
        String toggleProxyName = jsonObject.getString("proxyName");
        if (Objects.equals(toggleProxyName, this.proxyName)) {
            return Future.succeededFuture();
        }

        if (StrUtil.isBlank(toggleProxyName) && StrUtil.isNotBlank(this.proxyName)) {
            // disable proxy
            return executeWithContext(new TdApi.DisableProxy(), "disableProxy", null, null, null)
                    .compose(r -> {
                        this.proxyName = null;
                        if (this.telegramRecord != null) {
                            return DataVerticle.telegramRepository.update(this.telegramRecord.withProxy(null))
                                    .onSuccess(telegramRecord -> {
                                        this.telegramRecord = telegramRecord;
                                    })
                                    .mapEmpty();
                        }
                        return Future.succeededFuture();
                    });
        } else {
            return this.enableProxy(toggleProxyName);
        }
    }

    public Future<TdApi.Proxy> getTdProxy(SettingProxyRecords.Item proxy) {
        return executeWithContext(new TdApi.GetProxies(), "getProxies", null, null, null)
                .map(proxies -> Stream.of(proxies.proxies)
                        .filter(proxy::equalsTdProxy)
                        .findFirst()
                        .orElse(null));
    }

    public Future<TdApi.Proxy> getTdProxy() {
        return executeWithContext(new TdApi.GetProxies(), "getProxies", null, null, null)
                .map(proxies -> Stream.of(proxies.proxies)
                        .filter(p -> p.isEnabled)
                        .findFirst()
                        .orElse(null));
    }

    public Future<Double> ping() {
        return this.getTdProxy()
                .compose(proxy -> executeWithContext(new TdApi.PingProxy(proxy == null ? 0 : proxy.id), "pingProxy", null, null, null))
                .onSuccess(result -> publishMetricUpdate(new JsonObject()
                        .put("type", "networkLatency")
                        .put("latency", Math.round(result.seconds * 1000))))
                .map(r -> r.seconds);
    }

    public Future<String> execute(String method, Object params) {
        String code = RandomUtil.randomString(10);
        log.trace("[%s] Execute code: %s method: %s, params: %s".formatted(getRootId(), code, method, params));
        return Future.future(promise -> {
            TdApi.Function<?> func = TdApiHelp.getFunction(method, params);
            if (func == null) {
                promise.fail("Unsupported method: " + method);
                return;
            }
            client.getNativeClient().send(func, object -> {
                log.debug("[%s] Execute: [%s] Receive result: %s".formatted(getRootId(), code, object));
                handleDefaultResult(object, code);
            });
            promise.complete(code);
        });
    }

    private void sendEvent(EventPayload payload) {
        vertx.eventBus().publish(EventEnum.TELEGRAM_EVENT.address(),
                JsonObject.of("telegramId", this.getId(), "payload", JsonObject.mapFrom(payload)));
    }

    private void publishMetricUpdate(JsonObject payload) {
        if (!enableMetrics) {
            return;
        }
        if (payload == null) {
            return;
        }
        vertx.eventBus().publish(PerformanceMonitorVerticle.METRICS_UPDATE_ADDRESS, payload);
    }

    private void sendFileStatusHttpEvent(TdApi.File file, JsonObject fileUpdated) {
        if (fileUpdated == null || fileUpdated.isEmpty()) return;
        sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                .put("fileId", file.id)
                .put("uniqueId", file.remote.uniqueId)
                .put("downloadStatus", fileUpdated.getString("downloadStatus"))
                .put("localPath", fileUpdated.getString("localPath"))
                .put("completionDate", fileUpdated.getLong("completionDate"))
                .put("downloadedSize", file.local.downloadedSize)
        ));
    }

    private void handleAuthorizationResult(TdApi.Object object) {
        switch (object.getConstructor()) {
            case TdApi.Error.CONSTRUCTOR:
                sendEvent(EventPayload.build(EventPayload.TYPE_ERROR, object));
                break;
            case TdApi.Ok.CONSTRUCTOR:
                break;
            default:
                log.warn("[%s] Receive UpdateAuthorizationState with invalid authorization state%s".formatted(getRootId(), object));
        }
    }

    private void handleDefaultResult(TdApi.Object object, String code) {
        if (object.getConstructor() == TdApi.Error.CONSTRUCTOR) {
            sendEvent(EventPayload.build(EventPayload.TYPE_ERROR, code, object));
        } else {
            sendEvent(EventPayload.build(EventPayload.TYPE_METHOD_RESULT, code, object));
        }
    }

    private void handleException(Throwable e) {
        log.error(e);
    }

    public AvgSpeed.SpeedStats getCurrentSpeedStats() {
        return avgSpeed.getSpeedStats();
    }

    private void handleSaveAvgSpeed() {
        if (!authorized || telegramRecord == null) return;
        AvgSpeed.SpeedStats speedStats = avgSpeed.getSpeedStats();
        if (speedStats.avgSpeed() == 0
            && speedStats.minSpeed() == 0
            && speedStats.medianSpeed() == 0
            && speedStats.maxSpeed() == 0) {
            return;
        }
        JsonObject data = JsonObject.mapFrom(speedStats);
        data.remove("interval");
        DataVerticle.statisticRepository.create(new StatisticRecord(Convert.toStr(telegramRecord.id()),
                StatisticRecord.Type.speed,
                System.currentTimeMillis(),
                data.encode()));

        // Avoid speed not being updated for a long time
        avgSpeed.update(0, System.currentTimeMillis());
    }

    private Future<Void> initAvgSpeed() {
        return DataVerticle.settingRepository.<Integer>getByKey(SettingKey.avgSpeedInterval)
                .compose(interval -> {
                    if (Objects.equals(interval, avgSpeed.getSpeedStats().interval())) {
                        if (avgSpeedPersistenceTimerId == 0) {
                            avgSpeedPersistenceTimerId = vertx.setPeriodic(interval * 1000, id -> handleSaveAvgSpeed());
                        }
                        return Future.succeededFuture();
                    }

                    avgSpeed = new AvgSpeed(interval);
                    if (avgSpeedPersistenceTimerId != 0) {
                        vertx.cancelTimer(avgSpeedPersistenceTimerId);
                    }
                    avgSpeedPersistenceTimerId = vertx.setPeriodic(interval * 1000, id -> handleSaveAvgSpeed());
                    return Future.succeededFuture();
                });
    }

    private Future<Void> initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.SETTING_UPDATE.address(SettingKey.avgSpeedInterval.name()), message -> {
            log.debug("Avg Speed Interval update: %s".formatted(message.body()));
            this.initAvgSpeed();
        });

        vertx.eventBus().consumer(EventEnum.CONFIGURATION_CHANGE.address("network"), message -> {
            JsonObject payload = (JsonObject) message.body();
            String key = payload.getString("key");
            Object value = payload.getValue("value");
            switch (key) {
                case "connectionTimeout" -> this.connectionTimeoutMs = Convert.toInt(value, connectionTimeoutMs);
                case "keepAliveInterval" -> {
                    this.keepAliveInterval = Convert.toLong(value, keepAliveInterval);
                    setupKeepAlive();
                }
                case "maxRetries" -> this.networkMaxRetries = Convert.toInt(value, networkMaxRetries);
                case "backoffMultiplier" -> this.backoffMultiplier = Convert.toDouble(value, backoffMultiplier);
                default -> log.trace("Ignoring network config key {}", key);
            }
        });

        vertx.eventBus().consumer(EventEnum.CONFIGURATION_CHANGE.address("performance"), message -> {
            JsonObject payload = (JsonObject) message.body();
            String key = payload.getString("key");
            Object value = payload.getValue("value");
            switch (key) {
                case "enableCaching" -> {
                    this.enableCaching = Convert.toBool(value, enableCaching);
                    initChatCache();
                }
                case "cacheSize" -> {
                    this.cacheSize = Convert.toInt(value, cacheSize);
                    initChatCache();
                }
                case "cacheExpirationMinutes" -> {
                    this.cacheExpirationMinutes = Convert.toInt(value, cacheExpirationMinutes);
                    initChatCache();
                }
                case "enableMetrics" -> this.enableMetrics = Convert.toBool(value, enableMetrics);
                case "batchSize" -> this.batchSize = Convert.toInt(value, batchSize);
                default -> log.trace("Ignoring performance config key {}", key);
            }
        });

        return Future.succeededFuture();
    }

    private <R extends TdApi.Object> Future<R> executeWithContext(TdApi.Function<R> request,
                                                                  String operation,
                                                                  Long chatId,
                                                                  Long messageId,
                                                                  Integer fileId) {
        return executeWithContext(request, operation, chatId, messageId, fileId, false, 0);
    }

    private <R extends TdApi.Object> Future<R> executeWithContext(TdApi.Function<R> request,
                                                                  String operation,
                                                                  Long chatId,
                                                                  Long messageId,
                                                                  Integer fileId,
                                                                  boolean ignoreException,
                                                                  int retryCount) {
        Promise<R> promise = Promise.promise();
        long start = System.currentTimeMillis();
        lastActivityTime = start;
        Future<R> execution = ignoreException ? client.execute(request, true) : client.execute(request, connectionTimeoutMs, vertx);
        execution.onSuccess(result -> {
                    lastActivityTime = System.currentTimeMillis();
                    if (enableMetrics) {
                        recordApiMetrics(operation, System.currentTimeMillis() - start, true);
                        publishMetricUpdate(new JsonObject()
                                .put("type", "apiCall")
                                .put("operation", operation)
                                .put("durationMs", System.currentTimeMillis() - start)
                                .put("success", true));
                    }
                    promise.complete(result);
                })
                .onFailure(err -> {
                    lastActivityTime = System.currentTimeMillis();
                    logErrorContext(operation, chatId, messageId, fileId, err);
                    if (enableMetrics) {
                        recordApiMetrics(operation, System.currentTimeMillis() - start, false);
                        publishMetricUpdate(new JsonObject()
                                .put("type", "apiCall")
                                .put("operation", operation)
                                .put("durationMs", System.currentTimeMillis() - start)
                                .put("success", false));
                    }
                    if (isTransientError(err) && retryCount < networkMaxRetries) {
                        long delay = (long) Math.pow(backoffMultiplier, retryCount) * 500L;
                        vertx.setTimer(delay, id -> executeWithContext(request, operation, chatId, messageId, fileId, ignoreException, retryCount + 1)
                                .onComplete(promise));
                    } else {
                        promise.fail(err);
                    }
                });
        return promise.future();
    }

    private void logErrorContext(String operation, Long chatId, Long messageId, Integer fileId, Throwable err) {
        log.error("[%s] Operation %s failed (chatId=%s, messageId=%s, fileId=%s): %s".formatted(
                getRootId(),
                operation,
                chatId,
                messageId,
                fileId,
                err.getMessage()), err);
    }

    private boolean isTransientError(Throwable err) {
        if (err instanceof TelegramRunException tre) {
            int code = tre.getError().code;
            return code == 420 || code == 429 || code >= 500;
        }
        return false;
    }

    private void recordApiMetrics(String operation, long durationMs, boolean success) {
        if (!enableMetrics) {
            return;
        }
        if (telegramRecord == null) {
            return;
        }
        JsonObject data = new JsonObject()
                .put("operation", operation)
                .put("durationMs", durationMs)
                .put("success", success);
        DataVerticle.statisticRepository.create(new StatisticRecord(Convert.toStr(telegramRecord.id()),
                StatisticRecord.Type.apiCall,
                System.currentTimeMillis(),
                data.encode()));
        DataVerticle.statisticRepository.create(new StatisticRecord(Convert.toStr(telegramRecord.id()),
                StatisticRecord.Type.responseTime,
                System.currentTimeMillis(),
                data.encode()));
    }

    private void recordDownloadOutcome(String uniqueId, boolean success, String reason) {
        if (!enableMetrics) {
            return;
        }
        if (telegramRecord == null) {
            return;
        }
        JsonObject data = new JsonObject()
                .put("uniqueId", uniqueId)
                .put("success", success)
                .put("reason", reason);
        DataVerticle.statisticRepository.create(new StatisticRecord(Convert.toStr(telegramRecord.id()),
                StatisticRecord.Type.downloadOutcome,
                System.currentTimeMillis(),
                data.encode()));
    }

    private void onAuthorizationStateUpdated(TdApi.AuthorizationState authorizationState) {
        log.debug("[%s] Receive authorization state update: %s".formatted(getRootId(), authorizationState));
        this.lastAuthorizationState = authorizationState;
        switch (authorizationState.getConstructor()) {
            case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR:
                TdApi.SetTdlibParameters request = new TdApi.SetTdlibParameters();
                request.databaseDirectory = this.rootPath;
                request.useMessageDatabase = true;
                request.useFileDatabase = true;
                request.useChatInfoDatabase = true;
                request.useSecretChats = true;
                request.apiId = Config.TELEGRAM_API_ID;
                request.apiHash = Config.TELEGRAM_API_HASH;
                request.systemLanguageCode = "en";
                request.deviceModel = "Telegram Files";
                request.applicationVersion = Start.VERSION;
                log.trace("[%s] Send SetTdlibParameters: %s".formatted(getRootId(), request));

                executeWithContext(request, "setTdlibParameters", null, null, null).onSuccess(this::handleAuthorizationResult);
                break;
            case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitEmailAddress.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitEmailCode.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitRegistration.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR:
                sendEvent(EventPayload.build(EventPayload.TYPE_AUTHORIZATION, authorizationState));
                break;
            case TdApi.AuthorizationStateReady.CONSTRUCTOR:
                authorized = true;
                if (telegramRecord == null) {
                    executeWithContext(new TdApi.GetMe(), "getMe", null, null, null)
                            .compose(user ->
                                    DataVerticle.telegramRepository.create(new TelegramRecord(user.id, user.firstName, this.rootPath, this.proxyName))
                            )
                            .onSuccess(o -> {
                                telegramRecord = o;
                                log.info("[%s] %s Authorization Ready".formatted(getRootId(), this.telegramRecord.firstName()));
                            })
                            .onFailure(e -> log.error("[%s] Authorization Ready, but failed to create telegram record: %s".formatted(getRootId(), e.getMessage())));
                } else {
                    log.info("[%s] %s Authorization Ready".formatted(getRootId(), this.telegramRecord.firstName()));
                }
                sendEvent(EventPayload.build(EventPayload.TYPE_AUTHORIZATION, authorizationState));
                telegramChats.loadMainChatList();
                telegramChats.loadArchivedChatList();
                break;
            case TdApi.AuthorizationStateLoggingOut.CONSTRUCTOR:
                break;
            case TdApi.AuthorizationStateClosing.CONSTRUCTOR:
                break;
            case TdApi.AuthorizationStateClosed.CONSTRUCTOR:
                if (needDelete) {
                    vertx.fileSystem().deleteRecursive(this.rootPath)
                            .onSuccess(v -> log.info("[%s] Telegram account deleted".formatted(this.getRootId())))
                            .onFailure(e -> log.error("[%s] Failed to delete telegram account data: %s".formatted(this.getRootId(), e.getMessage())));
                }
                break;
            default:
                log.warn("[%s] Unsupported authorization state received:%s".formatted(this.getRootId(), authorizationState));
        }
    }

    private void onFileUpdated(TdApi.UpdateFile updateFile) {
        log.trace("📃[%s] Receive file update: %s".formatted(getRootId(), updateFile));
        TdApi.File file = updateFile.file;
        if (file != null) {
            String localPath = null;
            Long completionDate = null;
            if (file.local != null && file.local.isDownloadingCompleted) {
                localPath = file.local.path;
                completionDate = System.currentTimeMillis();
            }
            String finalLocalPath = localPath;
            Long finalCompletionDate = completionDate;
            DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                    .compose(fileRecord -> {
                        FileRecord.DownloadStatus downloadStatus = TdApiHelp.getDownloadStatus(file);
                        if (fileRecord == null) {
                            return Future.<Void>succeededFuture();
                        }
                        return existsAsync(fileRecord.localPath())
                                .compose(exists -> {
                                    if (fileRecord.isDownloadStatus(FileRecord.DownloadStatus.completed) &&
                                        fileRecord.isTransferStatus(FileRecord.TransferStatus.completed) &&
                                        exists) {
                                        return Future.succeededFuture();
                                    }
                                    FileRecord.DownloadStatus statusToPersist = downloadStatus == null ? FileRecord.DownloadStatus.idle : downloadStatus;
                                    return DataVerticle.fileRepository.updateDownloadStatus(file.id,
                                                    file.remote.uniqueId,
                                                    finalLocalPath,
                                                    statusToPersist,
                                                    finalCompletionDate)
                                            .onSuccess(r -> sendFileStatusHttpEvent(file, r))
                                            .mapEmpty();
                                });
                    });

            if (completionDate != null || lastFileEventTime == 0 || System.currentTimeMillis() - lastFileEventTime > 1000) {
                sendEvent(EventPayload.build(EventPayload.TYPE_FILE, updateFile));
                lastFileEventTime = System.currentTimeMillis();
            }
        }
    }

    private void onFileDownloadsUpdated(TdApi.UpdateFileDownloads updateFileDownloads) {
        log.trace("[%s] Receive file downloads update: %s".formatted(getRootId(), updateFileDownloads));
        avgSpeed.update(updateFileDownloads.downloadedSize, System.currentTimeMillis());
        if (lastFileDownloadEventTime == 0 || System.currentTimeMillis() - lastFileDownloadEventTime > 1000) {
            sendEvent(EventPayload.build(EventPayload.TYPE_FILE_DOWNLOAD, updateFileDownloads));
            lastFileDownloadEventTime = System.currentTimeMillis();
        }
    }

    private void onMessageReceived(TdApi.Message message) {
        log.trace("[%s] Receive message: %s".formatted(getRootId(), message));
        if (this.telegramRecord == null) {
            log.trace("[%s] Telegram record is null, can't handle message".formatted(getRootId()));
            return;
        }
        vertx.eventBus().publish(EventEnum.MESSAGE_RECEIVED.address(), JsonObject.of()
                .put("telegramId", telegramRecord.id())
                .put("chatId", message.chatId)
                .put("messageId", message.id)
        );
    }

    private Future<Void> syncFileDownloadStatus(TdApi.File file, TdApi.Message message, TdApi.MessageThreadInfo messageThreadInfo) {
        return DataVerticle.fileRepository
                .getByUniqueId(file.remote.uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord != null) {
                        return DataVerticle.fileRepository.updateDownloadStatus(
                                file.id,
                                file.remote.uniqueId,
                                file.local.path,
                                FileRecord.DownloadStatus.completed,
                                System.currentTimeMillis()
                        );
                    }

                    if (message == null) {
                        return Future.failedFuture("File not found");
                    }

                    fileRecord = TdApiHelp.getFileHandler(message)
                            .orElseThrow(() -> VertxException.noStackTrace("not support message type"))
                            .convertFileRecord(telegramRecord.id())
                            .withThreadInfo(messageThreadInfo);

                    return DataVerticle.fileRepository.create(fileRecord)
                            .compose(r -> DataVerticle.fileRepository.updateDownloadStatus(
                                    file.id,
                                    file.remote.uniqueId,
                                    file.local.path,
                                    FileRecord.DownloadStatus.completed,
                                    System.currentTimeMillis()
                            ));
                })
                .compose(r -> {
                    sendFileStatusHttpEvent(file, r);
                    if (r == null || r.isEmpty()) {
                        return Future.failedFuture("File is downloaded completed, but update status failed");
                    } else {
                        return Future.failedFuture("File is already downloaded successfully");
                    }
                });
    }
}

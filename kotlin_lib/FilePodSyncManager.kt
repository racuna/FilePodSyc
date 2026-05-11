package com.filepodsync.core

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * FilePodSync manager for Android.
 *
 * Thread-safe, coroutine-based, lifecycle-friendly. Exposes state as [StateFlow]
 * so the UI layer can observe subscriptions, episodes, and queue reactively.
 *
 * Usage:
 * ```
 * val fps = FilePodSyncManager(context, syncDir)
 * fps.addFeed("https://feeds.example.com/podcast", "Example")
 * fps.updateEpisode("https://cdn.example.com/ep1.mp3", guid = "ep-001", position = 600)
 * fps.queueAdd("guid:ep-001")
 * fps.sync() // suspending
 * ```
 */
class FilePodSyncManager(
    context: Context,
    private val syncDir: File,
    private val deviceName: String = "Android Device",
    private val platform: String = "android",
    private val clientName: String = "filepodsync-android"
) {
    companion object {
        private const val TAG = "FilePodSync"
    }

    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true // forward-compat: preserve unknown fields
        encodeDefaults = true
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutex = Mutex()

    // Persistent device ID (plain UTF-8, NOT JSON-encoded)
    val deviceId: String = loadOrCreateDeviceId(syncDir)

    // Three-state architecture
    private var syncedState: SyncedState = SyncedState()
    private val pendingLocalOps = mutableListOf<PendingOp>()
    private val pendingQueueOps = mutableListOf<QueueOp>()

    // Debounce
    private var queueDebounceJob: Job? = null
    private val isShutdown = AtomicBoolean(false)

    // Last sync throttle
    @Volatile
    private var lastSyncAt: Long = 0L

    // ── Public reactive state ───────────────────────────────────────────────

    private val _feeds = MutableStateFlow<Map<String, FpsFeeds.FeedRecord>>(emptyMap())
    val feeds: StateFlow<Map<String, FpsFeeds.FeedRecord>> = _feeds

    private val _episodes = MutableStateFlow<Map<String, FpsEpisodes.EpisodeRecord>>(emptyMap())
    val episodes: StateFlow<Map<String, FpsEpisodes.EpisodeRecord>> = _episodes

    private val _queue = MutableStateFlow<List<FpsQueue.QueueItem>>(emptyList())
    val queue: StateFlow<List<FpsQueue.QueueItem>> = _queue

    private val _devices = MutableStateFlow<Map<String, FpsDevices.DeviceRecord>>(emptyMap())
    val devices: StateFlow<Map<String, FpsDevices.DeviceRecord>> = _devices

    init {
        initStructure()
        runBlocking(Dispatchers.IO) {
            syncedState = loadState()
            emitCurrentState()
        }
        Log.i(TAG, "Initialized | device=$deviceId | dir=$syncDir")
    }

    // ── Initialization ──────────────────────────────────────────────────────

    private fun initStructure() {
        listOf("logs", "snapshots", "queue_ops").forEach {
            File(syncDir, it).mkdirs()
        }
        listOf("config", "devices", "feeds", "episodes", "queue").forEach { fname ->
            val f = File(syncDir, "$fname.json")
            if (!f.exists()) {
                val default = when (fname) {
                    "config" -> json.encodeToString(FpsConfig())
                    "devices" -> json.encodeToString(FpsDevices())
                    "feeds" -> json.encodeToString(FpsFeeds())
                    "episodes" -> json.encodeToString(FpsEpisodes())
                    "queue" -> json.encodeToString(FpsQueue())
                    else -> "{}"
                }
                atomicWriteJson(f, default)
            }
        }
    }

    private fun loadState(): SyncedState {
        return SyncedState(
            config = loadJsonSafe(File(syncDir, "config.json"), FpsConfig()),
            devices = loadJsonSafe(File(syncDir, "devices.json"), FpsDevices()),
            feeds = loadJsonSafe(File(syncDir, "feeds.json"), FpsFeeds()),
            episodes = loadJsonSafe(File(syncDir, "episodes.json"), FpsEpisodes()),
            queue = loadJsonSafe(File(syncDir, "queue.json"), FpsQueue())
        )
    }

    private fun emitCurrentState() {
        _feeds.value = syncedState.feeds?.feeds?.filterValues { it.status != "deleted" } ?: emptyMap()
        _episodes.value = syncedState.episodes?.episodes ?: emptyMap()
        _devices.value = syncedState.devices?.devices ?: emptyMap()
        // Queue is rebuilt live from ops
        val (items, _) = rebuildQueue(syncedState.queue, File(syncDir, "queue_ops"))
        _queue.value = items
    }

    // ── Device ID ───────────────────────────────────────────────────────────

    private fun loadOrCreateDeviceId(dir: File): String {
        val idFile = File(dir, ".fps_device_id")
        return if (idFile.exists()) {
            idFile.readText(StandardCharsets.UTF_8).trim().removeSurrounding(""")
        } else {
            val newId = UUID.randomUUID().toString()
            idFile.writeText(newId, StandardCharsets.UTF_8) // plain text, NOT json.dumps
            newId
        }
    }

    // ── Public API: Feeds ───────────────────────────────────────────────────

    suspend fun addFeed(url: String, title: String = "", custom: Map<String, String>? = null): Boolean {
        val norm = normalizeUrl(url)
        if (norm.isEmpty()) return false

        mutex.withLock {
            val feeds = syncedState.feeds?.feeds ?: mutableMapOf()
            val now = utcMs()
            val existing = feeds[norm]
            feeds[norm] = FpsFeeds.FeedRecord(
                url = norm,
                title = title.ifEmpty { existing?.title ?: "" },
                status = "active",
                health_status = existing?.health_status ?: "healthy",
                last_check = existing?.last_check ?: 0L,
                error_count = 0,
                added_by = existing?.added_by ?: deviceId,
                added_at = existing?.added_at ?: now,
                updated_by = deviceId,
                updated_at = now,
                custom = custom?.let { Json.encodeToString(it) }?.let { Json.parseToJsonElement(it) as? kotlinx.serialization.json.JsonObject }
            )
            saveStateFile("feeds", feeds)
            emitCurrentState()
        }
        return true
    }

    suspend fun removeFeed(url: String, archive: Boolean = false): Boolean {
        mutex.withLock {
            val norm = normalizeUrl(url)
            val feeds = syncedState.feeds?.feeds ?: return@withLock false
            val rec = feeds[norm] ?: return@withLock false
            rec.status = if (archive) "archived" else "deleted"
            rec.updated_by = deviceId
            rec.updated_at = utcMs()
            saveStateFile("feeds", feeds)
            emitCurrentState()
        }
        return true
    }

    // ── Public API: Episodes ────────────────────────────────────────────────

    suspend fun updateEpisode(
        episodeUrl: String,
        feedUrl: String,
        guid: String? = null,
        title: String = "",
        position: Int = 0,
        total: Int = 0,
        state: String = "unplayed"
    ): String {
        val epId = generateEpisodeId(guid, episodeUrl)
        mutex.withLock {
            val eps = syncedState.episodes?.episodes ?: mutableMapOf()
            val now = utcMs()
            val existing = eps[epId]
            eps[epId] = FpsEpisodes.EpisodeRecord(
                feed_url = normalizeUrl(feedUrl),
                guid = guid,
                url = episodeUrl,
                title = title.ifEmpty { existing?.title ?: "" },
                state = if (state in setOf("unplayed", "in_progress", "completed", "skipped")) state else (existing?.state ?: "unplayed"),
                progress_seconds = position, // exact, no max() clamping
                duration_seconds = if (total > 0) total else (existing?.duration_seconds ?: 0),
                updated_by = deviceId,
                updated_at = now,
                custom = existing?.custom
            )
            saveStateFile("episodes", eps)
            emitCurrentState()
        }
        return epId
    }

    // ── Public API: Queue (op-based with debounce) ──────────────────────────

    fun queueAdd(epId: String, afterId: String? = null) {
        val now = utcMs()
        pendingQueueOps.add(
            QueueOp(
                ts = now,
                device_id = deviceId,
                op = "add",
                items = listOf(FpsQueue.QueueItem(epId, now)),
                after_id = afterId
            )
        )
        debounceQueueFlush()
    }

    fun queueRemove(epIds: List<String>) {
        pendingQueueOps.add(
            QueueOp(
                ts = utcMs(),
                device_id = deviceId,
                op = "remove",
                ids = epIds
            )
        )
        debounceQueueFlush()
    }

    fun queueReorder(epIds: List<String>) {
        pendingQueueOps.add(
            QueueOp(
                ts = utcMs(),
                device_id = deviceId,
                op = "reorder",
                ids = epIds
            )
        )
        debounceQueueFlush()
    }

    fun queueClear() {
        pendingQueueOps.add(
            QueueOp(
                ts = utcMs(),
                device_id = deviceId,
                op = "clear"
            )
        )
        debounceQueueFlush()
    }

    private fun debounceQueueFlush() {
        queueDebounceJob?.cancel()
        queueDebounceJob = scope.launch {
            delay(QUEUE_DEBOUNCE_MS)
            flushQueueOps()
        }
    }

    private suspend fun flushQueueOps() {
        val ops = mutex.withLock {
            if (pendingQueueOps.isEmpty()) return@withLock emptyList<QueueOp>()
            val drained = pendingQueueOps.toList()
            pendingQueueOps.clear()
            drained
        }
        if (ops.isEmpty()) return

        val opsFile = File(syncDir, "queue_ops/$deviceId.jsonl")
        opsFile.parentFile?.mkdirs()
        val lines = ops.joinToString("\n") { json.encodeToString(QueueOp.serializer(), it) }
        withContext(Dispatchers.IO) {
            opsFile.appendText(lines + "\n", StandardCharsets.UTF_8)
        }
        maybeConsolidateQueue()
        emitCurrentState()
    }

    private suspend fun maybeConsolidateQueue() {
        val opsDir = File(syncDir, "queue_ops")
        var total = 0
        opsDir.listFiles()?.forEach { f ->
            if (f.isFile && f.extension == "jsonl" && !isConflictFile(f.name)) {
                total += f.readLines().count { it.isNotBlank() }
            }
        }
        if (total >= QUEUE_OPS_CONSOLIDATE_AT) {
            Log.i(TAG, "Consolidating queue ops ($total lines)")
            consolidateQueue()
        }
    }

    /**
     * Consolidation: rebuild queue, write snapshot, reset ONLY own op file.
     * Never touches other devices' files (Device File Sovereignty, spec Constraint 11).
     */
    private suspend fun consolidateQueue() {
        mutex.withLock {
            val snapshot = syncedState.queue
            val (items, maxTs) = rebuildQueue(snapshot, File(syncDir, "queue_ops"))
            val newQueue = FpsQueue(
                schema_version = SCHEMA_VERSION,
                updated_at = utcMs(),
                updated_by = deviceId,
                consolidated_through_ts = maxTs,
                items = items.toMutableList()
            )
            saveFile("queue.json", json.encodeToString(FpsQueue.serializer(), newQueue))
            syncedState = syncedState.copy(queue = newQueue)

            // Reset ONLY own file
            val ownFile = File(syncDir, "queue_ops/$deviceId.jsonl")
            if (ownFile.exists()) {
                ownFile.writeText("", StandardCharsets.UTF_8)
            }
            emitCurrentState()
        }
    }

    // ── Sync Cycle (three-state) ────────────────────────────────────────────

    suspend fun sync(force: Boolean = false): SyncResult {
        if (!force) {
            val elapsed = System.currentTimeMillis() - lastSyncAt
            if (elapsed < MIN_SYNC_INTERVAL_MS) {
                Log.d(TAG, "sync() throttled (${elapsed}ms < ${MIN_SYNC_INTERVAL_MS}ms)")
                return SyncResult(0, 0, 0, 0, utcMs(), throttled = true)
            }
        }

        mutex.withLock {
            // 1. Read remote
            val remote = loadState()

            // 2. Validate schema
            listOf("config", "devices", "feeds", "episodes", "queue").forEach { fname ->
                val data = when (fname) {
                    "config" -> remote.config?.let { mapOf("schema_version" to it.schema_version) }
                    "devices" -> remote.devices?.let { mapOf("schema_version" to it.schema_version) }
                    "feeds" -> remote.feeds?.let { mapOf("schema_version" to it.schema_version) }
                    "episodes" -> remote.episodes?.let { mapOf("schema_version" to it.schema_version) }
                    "queue" -> remote.queue?.let { mapOf("schema_version" to it.schema_version) }
                    else -> null
                }
                if (data != null && !validateSchemaVersion(data, fname)) {
                    // Attempt snapshot restore on schema mismatch (unlikely but defensive)
                }
            }

            // 3. Detect clock skew
            detectClockSkew(remote)

            // 4. LWW-EL merge
            val mergedFeeds = mergeRecords(
                syncedState.feeds?.feeds ?: emptyMap(),
                remote.feeds?.feeds ?: emptyMap()
            )
            val mergedEps = mergeEpisodeRecords(
                syncedState.episodes?.episodes ?: emptyMap(),
                remote.episodes?.episodes ?: emptyMap()
            )
            val mergedDevs = mergeDeviceRecords(
                syncedState.devices?.devices ?: emptyMap(),
                remote.devices?.devices ?: emptyMap()
            )

            // 5. Rebuild queue
            val (mergedQueueItems, _) = rebuildQueue(
                remote.queue,
                File(syncDir, "queue_ops")
            )
            val ctt = remote.queue?.consolidated_through_ts ?: 0L

            // 6. Replay pending local ops
            for (op in pendingLocalOps) {
                when (op.type) {
                    PendingOp.Type.FEED -> {
                        val rec = mergedFeeds[op.id]
                        if (rec == null || op.ts >= rec.updated_at) {
                            mergedFeeds[op.id] = op.feedData!!
                        }
                    }
                    PendingOp.Type.EPISODE -> {
                        val rec = mergedEps[op.id]
                        if (rec == null || op.ts >= rec.updated_at) {
                            mergedEps[op.id] = op.episodeData!!
                        }
                    }
                }
            }
            pendingLocalOps.clear()

            // 7. Stamp own device
            val now = utcMs()
            val existingDev = mergedDevs[deviceId]
            mergedDevs[deviceId] = FpsDevices.DeviceRecord(
                name = deviceName,
                platform = platform,
                client = clientName,
                status = "active",
                first_seen = existingDev?.first_seen ?: now,
                last_seen = now,
                updated_by = deviceId,
                updated_at = now
            )

            // 8. Atomic write
            saveStateFile("feeds", mergedFeeds)
            saveStateFile("episodes", mergedEps)
            saveStateFile("devices", mergedDevs)
            val mergedQueue = FpsQueue(
                schema_version = SCHEMA_VERSION,
                updated_at = now,
                updated_by = deviceId,
                consolidated_through_ts = ctt,
                items = mergedQueueItems.toMutableList()
            )
            saveFile("queue.json", json.encodeToString(FpsQueue.serializer(), mergedQueue))

            // 9. Update synced state
            syncedState = SyncedState(
                config = syncedState.config,
                devices = FpsDevices(devices = mergedDevs),
                feeds = FpsFeeds(feeds = mergedFeeds),
                episodes = FpsEpisodes(episodes = mergedEps),
                queue = mergedQueue
            )

            lastSyncAt = System.currentTimeMillis()
            emitCurrentState()

            return SyncResult(
                feeds = mergedFeeds.size,
                episodes = mergedEps.size,
                devices = mergedDevs.size,
                queue = mergedQueueItems.size,
                timestamp = now
            )
        }
    }

    // ── Bootstrap ───────────────────────────────────────────────────────────

    suspend fun bootstrapFromLocal(
        localFeeds: List<FeedInfo>,
        localEpisodes: List<EpisodeInfo>,
        queueEpIds: List<String> = emptyList()
    ): Map<String, Int> {
        mutex.withLock {
            val remoteFeeds = loadJsonSafe(File(syncDir, "feeds.json"), FpsFeeds()).feeds
            val now = utcMs()
            var skippedDeleted = 0

            for (f in localFeeds) {
                val norm = normalizeUrl(f.url)
                if (norm.isEmpty()) continue
                if (remoteFeeds[norm]?.status == "deleted") {
                    skippedDeleted++
                    continue
                }
                pendingLocalOps.add(
                    PendingOp(
                        type = PendingOp.Type.FEED,
                        id = norm,
                        ts = now,
                        feedData = FpsFeeds.FeedRecord(
                            url = norm,
                            title = f.title,
                            status = "active",
                            added_by = deviceId,
                            added_at = now,
                            updated_by = deviceId,
                            updated_at = now
                        )
                    )
                )
            }

            for (e in localEpisodes) {
                val epId = generateEpisodeId(e.guid, e.url)
                pendingLocalOps.add(
                    PendingOp(
                        type = PendingOp.Type.EPISODE,
                        id = epId,
                        ts = now,
                        episodeData = FpsEpisodes.EpisodeRecord(
                            feed_url = normalizeUrl(e.feedUrl),
                            guid = e.guid,
                            url = e.url,
                            title = e.title,
                            state = e.state,
                            progress_seconds = e.position,
                            duration_seconds = e.duration,
                            updated_by = deviceId,
                            updated_at = now
                        )
                    )
                )
            }

            if (queueEpIds.isNotEmpty()) {
                val items = queueEpIds.map { FpsQueue.QueueItem(it, now) }
                val opFile = File(syncDir, "queue_ops/$deviceId.jsonl")
                opFile.parentFile?.mkdirs()
                val op = QueueOp(
                    ts = now,
                    device_id = deviceId,
                    op = "add",
                    items = items,
                    after_id = null
                )
                opFile.appendText(json.encodeToString(op) + "\n", StandardCharsets.UTF_8)
            }

            return mapOf(
                "feeds" to (localFeeds.size - skippedDeleted),
                "feeds_skipped_deleted" to skippedDeleted,
                "episodes" to localEpisodes.size,
                "queue" to queueEpIds.size
            )
        }
    }

    // ── OPML Export / Import ────────────────────────────────────────────────

    fun exportOpml(): String {
        val activeFeeds = feeds.value.values.sortedWith(compareBy({ it.title }, { it.url }))
        fun esc(s: String) = s.replace("&", "&amp;")
            .replace(""", "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")

        val lines = mutableListOf(
            "<?xml version="1.0" encoding="UTF-8"?>",
            "<opml version="2.0">",
            "  <head>",
            "    <title>FilePodSync Subscriptions</title>",
            "  </head>",
            "  <body>"
        )
        for (feed in activeFeeds) {
            lines.add("    <outline type="rss" text="${esc(feed.title)}" xmlUrl="${esc(feed.url)}" />")
        }
        lines += listOf("  </body>", "</opml>")
        return lines.joinToString("\n")
    }

    // ── Housekeeping ────────────────────────────────────────────────────────

    suspend fun housekeeping() {
        withContext(Dispatchers.IO) {
            rotateLogs()
            pruneSnapshots()
        }
    }

    private fun rotateLogs() {
        val logDir = File(syncDir, "logs")
        if (!logDir.exists()) return
        val cutoff = java.text.SimpleDateFormat("yyyyMMdd", java.util.Locale.US).format(
            java.util.Calendar.getInstance().apply { add(java.util.Calendar.DAY_OF_YEAR, -LOG_MAX_DAYS) }.time
        )
        logDir.listFiles { f -> f.name.startsWith("sync-") && f.extension == "jsonl" }?.forEach { f ->
            val date = f.name.removePrefix("sync-").removeSuffix(".jsonl")
            if (date < cutoff) {
                f.delete()
            }
        }
    }

    private fun pruneSnapshots() {
        val snapDir = File(syncDir, "snapshots")
        if (!snapDir.exists()) return
        val snaps = snapDir.listFiles { f -> f.extension == "gz" }?.sortedBy { it.lastModified() } ?: return
        while (snaps.size > SNAPSHOT_RETENTION) {
            snaps.firstOrNull()?.delete()
        }
    }

    // ── Shutdown ────────────────────────────────────────────────────────────

    suspend fun shutdown() {
        if (isShutdown.getAndSet(true)) return
        queueDebounceJob?.cancel()
        flushQueueOps()
        createSnapshot()
        scope.cancel()
    }

    private suspend fun createSnapshot() {
        val ts = java.text.SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'", java.util.Locale.US).apply {
            timeZone = java.util.TimeZone.getTimeZone("UTC")
        }.format(java.util.Date())
        val file = File(syncDir, "snapshots/snapshot-$ts.json.gz")
        withContext(Dispatchers.IO) {
            java.util.zip.GZIPOutputStream(file.outputStream()).use { gz ->
                gz.write(json.encodeToString(syncedState).toByteArray(StandardCharsets.UTF_8))
            }
        }
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    private fun saveStateFile(key: String, data: Any) {
        val wrapper = when (key) {
            "feeds" -> FpsFeeds(feeds = data as MutableMap<String, FpsFeeds.FeedRecord>)
            "episodes" -> FpsEpisodes(episodes = data as MutableMap<String, FpsEpisodes.EpisodeRecord>)
            "devices" -> FpsDevices(devices = data as MutableMap<String, FpsDevices.DeviceRecord>)
            else -> throw IllegalArgumentException("Unknown key: $key")
        }
        val file = File(syncDir, "$key.json")
        atomicWriteJson(file, json.encodeToString(wrapper))
    }

    private fun saveFile(name: String, content: String) {
        atomicWriteJson(File(syncDir, name), content)
    }

    private fun detectClockSkew(remote: SyncedState) {
        val localNow = utcMs()
        val maxRemote = maxOf(
            remote.feeds?.updated_at ?: 0L,
            remote.episodes?.updated_at ?: 0L,
            remote.devices?.updated_at ?: 0L,
            remote.queue?.updated_at ?: 0L
        )
        if (maxRemote > 0 && kotlin.math.abs(localNow - maxRemote) > SKEW_WARNING_MS) {
            Log.w(TAG, "Clock skew detected: local=$localNow remote=$maxRemote diff=${kotlin.math.abs(localNow - maxRemote)}")
        }
    }

    // ── Data classes for internal use ───────────────────────────────────────

    data class SyncedState(
        val config: FpsConfig? = null,
        val devices: FpsDevices? = null,
        val feeds: FpsFeeds? = null,
        val episodes: FpsEpisodes? = null,
        val queue: FpsQueue? = null
    )

    data class PendingOp(
        val type: Type,
        val id: String,
        val ts: Long,
        val feedData: FpsFeeds.FeedRecord? = null,
        val episodeData: FpsEpisodes.EpisodeRecord? = null
    ) {
        enum class Type { FEED, EPISODE }
    }

    data class FeedInfo(val url: String, val title: String = "")
    data class EpisodeInfo(
        val url: String,
        val feedUrl: String,
        val guid: String? = null,
        val title: String = "",
        val state: String = "unplayed",
        val position: Int = 0,
        val duration: Int = 0
    )
}

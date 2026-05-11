package com.filepodsync.core

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

// ─────────────────────────────────────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

const val SCHEMA_VERSION = "1.3.0"
const val SCHEMA_COMPATIBLE_MAJOR = 1
const val QUEUE_DEBOUNCE_MS = 2_000L
const val QUEUE_OPS_CONSOLIDATE_AT = 50
const val SKEW_WARNING_MS = 300_000L
const val DEVICE_RETIREMENT_DAYS = 90
const val MIN_SYNC_INTERVAL_MS = 5_000L

// ─────────────────────────────────────────────────────────────────────────────
// JSON MODELS — strictly matching agent.md v1.3 schemas
// ─────────────────────────────────────────────────────────────────────────────

@Serializable
data class FpsConfig(
    val schema_version: String = SCHEMA_VERSION,
    val sync_interval_ms: Long = 1_800_000L,
    val capabilities: Capabilities = Capabilities(),
    val rotation: Rotation = Rotation()
) {
    @Serializable
    data class Capabilities(
        val queue_sync: Boolean = true,
        val tag_sync: Boolean = false,
        val snapshot_sync: Boolean = true,
        val dead_feed_tracking: Boolean = true
    )

    @Serializable
    data class Rotation(
        val log_max_days: Int = 30,
        val log_max_mb: Int = 10,
        val snapshot_retention: Int = 5,
        val queue_ops_consolidate_at: Int = QUEUE_OPS_CONSOLIDATE_AT
    )
}

@Serializable
data class FpsDevices(
    val schema_version: String = SCHEMA_VERSION,
    var updated_at: Long = 0L,
    var updated_by: String = "",
    val devices: MutableMap<String, DeviceRecord> = mutableMapOf()
) {
    @Serializable
    data class DeviceRecord(
        val name: String,
        val platform: String,
        val client: String,
        var status: String = "active",
        val first_seen: Long,
        var last_seen: Long,
        var updated_by: String = "",
        var updated_at: Long = 0L
    )
}

@Serializable
data class FpsFeeds(
    val schema_version: String = SCHEMA_VERSION,
    var updated_at: Long = 0L,
    var updated_by: String = "",
    val feeds: MutableMap<String, FeedRecord> = mutableMapOf()
) {
    @Serializable
    data class FeedRecord(
        val url: String,
        var title: String = "",
        var status: String = "active",           // active | archived | deleted
        var health_status: String = "healthy",   // healthy | stale | error
        var last_check: Long = 0L,
        var error_count: Int = 0,
        val added_by: String = "",
        val added_at: Long = 0L,
        var updated_by: String = "",
        var updated_at: Long = 0L,
        var custom: JsonObject? = null
    )
}

@Serializable
data class FpsEpisodes(
    val schema_version: String = SCHEMA_VERSION,
    var updated_at: Long = 0L,
    var updated_by: String = "",
    val episodes: MutableMap<String, EpisodeRecord> = mutableMapOf()
) {
    @Serializable
    data class EpisodeRecord(
        val feed_url: String = "",
        val guid: String? = null,
        val url: String = "",
        var title: String = "",
        var state: String = "unplayed",          // unplayed | in_progress | completed | skipped
        var progress_seconds: Int = 0,
        var duration_seconds: Int = 0,
        var updated_by: String = "",
        var updated_at: Long = 0L,
        var custom: JsonObject? = null
    )
}

@Serializable
data class FpsQueue(
    val schema_version: String = SCHEMA_VERSION,
    var updated_at: Long = 0L,
    var updated_by: String = "",
    var consolidated_through_ts: Long = 0L,
    val items: MutableList<QueueItem> = mutableListOf()
) {
    @Serializable
    data class QueueItem(
        val ep_id: String,
        val added_at: Long
    )
}

/**
 * Queue operation line (JSON Lines format).
 * Each line in queue_ops/<device-id>.jsonl is one of these.
 */
@Serializable
data class QueueOp(
    val ts: Long,
    val device_id: String,
    val op: String,          // add | remove | reorder | clear
    val items: List<FpsQueue.QueueItem>? = null,
    val ids: List<String>? = null,
    val after_id: String? = null
)

/**
 * Wrapper used for atomic file writes. Not serialized directly.
 */
data class SyncResult(
    val feeds: Int,
    val episodes: Int,
    val devices: Int,
    val queue: Int,
    val timestamp: Long,
    val throttled: Boolean = false
)

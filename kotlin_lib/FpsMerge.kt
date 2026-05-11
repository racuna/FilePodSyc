package com.filepodsync.core

// ─────────────────────────────────────────────────────────────────────────────
// LWW-EL MERGE (spec §MergeRecords)
// ─────────────────────────────────────────────────────────────────────────────

fun mergeRecords(
    local: Map<String, FpsFeeds.FeedRecord>,
    remote: Map<String, FpsFeeds.FeedRecord>
): MutableMap<String, FpsFeeds.FeedRecord> {
    val merged = local.toMutableMap()
    for ((key, rrec) in remote) {
        val lrec = local[key]
        if (lrec == null) {
            merged[key] = rrec
            continue
        }
        when {
            rrec.updated_at > lrec.updated_at -> merged[key] = rrec
            rrec.updated_at == lrec.updated_at && rrec.updated_by > lrec.updated_by -> merged[key] = rrec
        }
    }
    return merged
}

fun mergeEpisodeRecords(
    local: Map<String, FpsEpisodes.EpisodeRecord>,
    remote: Map<String, FpsEpisodes.EpisodeRecord>
): MutableMap<String, FpsEpisodes.EpisodeRecord> {
    val merged = local.toMutableMap()
    for ((key, rrec) in remote) {
        val lrec = local[key]
        if (lrec == null) {
            merged[key] = rrec
            continue
        }
        when {
            rrec.updated_at > lrec.updated_at -> merged[key] = rrec
            rrec.updated_at == lrec.updated_at && rrec.updated_by > lrec.updated_by -> merged[key] = rrec
        }
    }
    return merged
}

fun mergeDeviceRecords(
    local: Map<String, FpsDevices.DeviceRecord>,
    remote: Map<String, FpsDevices.DeviceRecord>
): MutableMap<String, FpsDevices.DeviceRecord> {
    val merged = local.toMutableMap()
    for ((key, rrec) in remote) {
        val lrec = local[key]
        if (lrec == null) {
            merged[key] = rrec
            continue
        }
        when {
            rrec.updated_at > lrec.updated_at -> merged[key] = rrec
            rrec.updated_at == lrec.updated_at && rrec.updated_by > lrec.updated_by -> merged[key] = rrec
        }
    }
    return merged
}

// ─────────────────────────────────────────────────────────────────────────────
// QUEUE RECONSTRUCTION (spec §RebuildQueue)
// ─────────────────────────────────────────────────────────────────────────────

fun rebuildQueue(
    snapshot: FpsQueue?,
    opsDir: File
): Pair<MutableList<FpsQueue.QueueItem>, Long> {
    val items = snapshot?.items?.toMutableList() ?: mutableListOf()
    val cutoff = snapshot?.consolidated_through_ts ?: 0L
    val ops = mutableListOf<QueueOp>()
    var maxTs = cutoff

    if (opsDir.exists()) {
        opsDir.listFiles()?.forEach { file ->
            if (!file.isFile || file.extension != "jsonl" || isConflictFile(file.name)) return@forEach
            try {
                file.readLines(StandardCharsets.UTF_8).forEach { line ->
                    if (line.isBlank()) return@forEach
                    val op = kotlinx.serialization.json.Json.decodeFromString(
                        QueueOp.serializer(), line
                    )
                    if (op.ts > cutoff) {
                        ops.add(op)
                        if (op.ts > maxTs) maxTs = op.ts
                    }
                }
            } catch (e: Exception) {
                android.util.Log.w("FilePodSync", "Failed to parse queue ops in ${file.name}: $e")
            }
        }
    }

    // Deterministic sort: (ts, device_id)
    ops.sortWith(compareBy({ it.ts }, { it.device_id }))

    for (op in ops) {
        applyQueueOp(items, op)
    }

    return Pair(items, maxTs)
}

fun applyQueueOp(items: MutableList<FpsQueue.QueueItem>, op: QueueOp) {
    when (op.op) {
        "add" -> {
            val newItems = op.items?.toMutableList() ?: return
            val afterId = op.after_id
            if (afterId == null) {
                items.addAll(newItems)
            } else {
                val idx = items.indexOfFirst { it.ep_id == afterId }
                if (idx >= 0) {
                    items.addAll(idx + 1, newItems)
                } else {
                    items.addAll(newItems)
                }
            }
        }
        "remove" -> {
            val rm = op.ids?.toSet() ?: return
            items.removeAll { it.ep_id in rm }
        }
        "reorder" -> {
            val order = op.ids ?: return
            val orderMap = order.withIndex().associate { it.value to it.index }
            val mentioned = items.filter { it.ep_id in orderMap }
                .sortedBy { orderMap[it.ep_id] ?: Int.MAX_VALUE }
            val rest = items.filter { it.ep_id !in orderMap }
            items.clear()
            items.addAll(mentioned)
            items.addAll(rest)
        }
        "clear" -> items.clear()
        else -> {
            // Unknown op type — silently skip (forward-compat, spec §Unknown op types)
            android.util.Log.w("FilePodSync", "Unknown queue op type: ${op.op}")
        }
    }
}

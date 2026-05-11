package com.filepodsync.core

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.atomic.AtomicLong

// ─────────────────────────────────────────────────────────────────────────────
// TIME
// ─────────────────────────────────────────────────────────────────────────────

fun utcMs(): Long = System.currentTimeMillis()

// ─────────────────────────────────────────────────────────────────────────────
// URL NORMALIZATION (spec §feeds.json)
// ─────────────────────────────────────────────────────────────────────────────

fun normalizeUrl(url: String): String {
    if (url.isBlank()) return ""
    val parsed = URL(url.trim())
    val scheme = parsed.protocol.lowercase()
    var host = parsed.host.lowercase()
    val port = parsed.port

    // Remove default ports
    if (port != -1) {
        if ((scheme == "http" && port == 80) || (scheme == "https" && port == 443)) {
            // host already lacks port
        } else {
            host = "$host:$port"
        }
    }

    var path = java.net.URLDecoder.decode(parsed.path, StandardCharsets.UTF_8)
    if (path.endsWith("/") && path.length > 1) {
        path = path.dropLast(1)
    }

    val query = parsed.query ?: ""
    val ref = parsed.ref ?: ""

    return buildString {
        append(scheme)
        append("://")
        append(host)
        append(path)
        if (query.isNotEmpty()) {
            append("?").append(query)
        }
        if (ref.isNotEmpty()) {
            append("#").append(ref)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EPISODE ID
// ─────────────────────────────────────────────────────────────────────────────

fun generateEpisodeId(guid: String?, url: String): String {
    val g = guid?.trim()
    if (!g.isNullOrEmpty() && g.lowercase() != "none") {
        return "guid:$g"
    }
    val hash = sha256Hex(normalizeUrl(url)).take(16)
    return "url:$hash"
}

fun sha256Hex(input: String): String {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = digest.digest(input.toByteArray(StandardCharsets.UTF_8))
    return bytes.joinToString("") { "%02x".format(it) }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFLICT FILE DETECTION (spec §File Filtering)
// ─────────────────────────────────────────────────────────────────────────────

fun isConflictFile(name: String): Boolean {
    val low = name.lowercase()
    return when {
        ".sync-conflict" in low -> true
        " (conflicted copy)" in low -> true
        Regex(""" \((\d+)\)\.(json|jsonl)$""").containsMatchIn(low) -> true
        low.endsWith(".tmp") -> true
        low.endsWith(".partial") -> true
        name.startsWith(".") -> true
        else -> false
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ATOMIC FILE WRITE
// ─────────────────────────────────────────────────────────────────────────────

fun atomicWriteJson(file: File, json: String) {
    val tmp = File(file.parentFile, file.name + ".tmp")
    tmp.writeText(json, StandardCharsets.UTF_8)
    if (!tmp.renameTo(file)) {
        // On some Android filesystems renameTo can fail across boundaries;
        // copy + delete as fallback.
        tmp.copyTo(file, overwrite = true)
        tmp.delete()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON SAFE LOAD
// ─────────────────────────────────────────────────────────────────────────────

inline fun <reified T> loadJsonSafe(file: File, fallback: T): T {
    if (!file.exists()) return fallback
    return try {
        kotlinx.serialization.json.Json.decodeFromString(serializer(), file.readText(StandardCharsets.UTF_8))
    } catch (e: Exception) {
        android.util.Log.w("FilePodSync", "Failed to load ${file.name}: $e")
        fallback
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEMA VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

fun validateSchemaVersion(data: Map<String, *>?, label: String): Boolean {
    val raw = data?.get("schema_version") as? String
    if (raw.isNullOrEmpty()) {
        android.util.Log.w("FilePodSync", "Missing schema_version in $label")
        return false
    }
    val major = try {
        raw.split(".")[0].toInt()
    } catch (e: Exception) {
        android.util.Log.w("FilePodSync", "Un-parseable schema_version $raw in $label")
        return false
    }
    if (major != SCHEMA_COMPATIBLE_MAJOR) {
        android.util.Log.e(
            "FilePodSync",
            "Schema major-version mismatch in $label: found $raw, compatible major is $SCHEMA_COMPATIBLE_MAJOR"
        )
        return false
    }
    return true
}

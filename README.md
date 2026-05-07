# FilePodSync (FPS) Specification v1.2

> A file-based, serverless podcast synchronization protocol that actually works across multiple devices.

[![Protocol](https://img.shields.io/badge/protocol-1.2-blue)](SPEC.md)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why FilePodSync?

Existing podcast sync relies on **gPodder.net**, **Nextcloud-gPodder**, or **oPodSync** — all server-dependent, fragile, or requiring technical expertise to self-host. FilePodSync replaces the server with a **folder** synced by Dropbox, Syncthing, Google Drive, Filen, iCloud, or your NAS.

**No API. No OAuth. No server maintenance. Just files.**

---

## What Changed in v1.2

v1.2 fixes critical concurrency and data-loss issues discovered in v1.1:

- **Queue is now operation-based** (`queue_ops/*.jsonl`). No more "last write wins" destroying offline queue additions.
- **GUID is first-class** everywhere. Episode identity uses RSS `<guid>` first, URL hash second.
- **URL normalization** prevents duplicate feeds from trivial URL variations.
- **Three-state sync architecture** (`synced` → `merged` → `local`) prevents local changes from being silently overwritten by stale remotes.
- **Clock-skew detection** warns when a device's clock is suspiciously off.
- **Auto-restore from snapshots** if a JSON file is corrupted by a sync conflict or crash.
- **Rewinds work** — progress is no longer clamped with `max()`.

---

## Directory Structure

```
FilePodSync/
├── config.json                 # Client capabilities & rotation policy
├── devices.json                # Device registry
├── feeds.json                  # Subscriptions (LWW-EL map)
├── episodes.json               # Episode states (LWW-EL map)
├── queue.json                  # Consolidated queue snapshot (auto-generated)
├── queue_ops/                  # Append-only queue operations per device
│   ├── <device-id>.jsonl
│   └── <device-id>.jsonl
├── logs/                       # Append-only sync logs
│   └── sync-YYYYMMDD.jsonl
└── snapshots/                  # Compressed disaster-recovery backups
    └── snapshot-<ts>.json.gz
```

> **Golden Rule:** A device MUST only append to its own `queue_ops/<device-id>.jsonl`. It MUST NOT modify another device's files, ever.

---

## Core Concepts

### 1. LWW-EL (Last-Write-Wins at Element Level)

For **feeds** and **episodes**, each record carries:
- `updated_at` — UTC timestamp in **milliseconds**.
- `updated_by` — UUID of the device that wrote it.

When merging, the record with the latest `updated_at` wins. If timestamps are identical, the lexicographically larger `updated_by` UUID wins (deterministic tie-breaker).

### 2. Queue Operations (Op-Based Merge)

The playback queue cannot use LWW safely: two devices adding episodes offline would silently lose one addition.

Instead, each device appends **operations** to its own log:

```jsonl
{"ts":1700000100000,"op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"op":"reorder","ids":["guid:xyz","guid:abc"]}
```

To reconstruct the queue:
1. Start from `queue.json` (snapshot) if it exists, otherwise `[]`.
2. Read **all** `queue_ops/*.jsonl` files.
3. Sort operations globally by `ts`.
4. Apply sequentially.

When the total pending operations exceed **50**, the client consolidates: writes the current queue into `queue.json` and truncates all `.jsonl` files.

### 3. Three-State Sync Architecture

To prevent a stale remote from overwriting fresh local changes:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Synced     │ ──► │   Merged    │ ──► │   Local     │
│  (disk)     │     │   (base)    │     │   (memory)  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       │  1. Read remote   │  2. Merge LWW     │  3. Apply
       │  2. Merge with    │     + Queue Ops   │     pending
       │     synced state  │                   │     local ops
```

- **Synced**: The state as it was after the last successful sync.
- **Merged**: `synced` merged with the current remote files.
- **Local**: `merged` + any user actions that happened since the last sync cycle.

This guarantees that a local "pause" made while offline is never lost when the device comes back online.

---

## File Schemas

### `config.json`

```json
{
  "schema_version": "1.2.0",
  "sync_interval_ms": 1800000,
  "capabilities": {
    "queue_sync": true,
    "tag_sync": false,
    "snapshot_sync": true,
    "dead_feed_tracking": true
  },
  "rotation": {
    "log_max_days": 30,
    "log_max_mb": 10,
    "snapshot_retention": 5,
    "queue_ops_consolidate_at": 50
  }
}
```

### `devices.json`

```json
{
  "schema_version": "1.2.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "devices": {
    "a1b2c3d4-e5f6-7890-abcd-ef1234567890": {
      "name": "Pixel 7",
      "platform": "android",
      "client": "antennapod",
      "first_seen": 1700000000000,
      "last_seen": 1700000000000
    }
  }
}
```

### `feeds.json`

```json
{
  "schema_version": "1.2.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "feeds": {
    "https://feeds.example.com/podcast": {
      "url": "https://feeds.example.com/podcast",
      "title": "Example Podcast",
      "status": "active",
      "health_status": "healthy",
      "last_check": 1700000000000,
      "error_count": 0,
      "added_by": "a1b2c3d4...",
      "added_at": 1700000000000,
      "updated_by": "a1b2c3d4...",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**URL Normalization:** Before using a URL as a key, clients MUST normalize it:
1. Lowercase the scheme and host.
2. Remove trailing slash from path.
3. Decode percent-encoding.
4. Remove default ports (`:80` for HTTP, `:443` for HTTPS).

### `episodes.json`

```json
{
  "schema_version": "1.2.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "episodes": {
    "guid:rss-guid-here": {
      "feed_url": "https://feeds.example.com/podcast",
      "guid": "rss-guid-here",
      "url": "https://cdn.example.com/ep1.mp3",
      "title": "Episode 1",
      "state": "in_progress",
      "progress_seconds": 1250,
      "duration_seconds": 3600,
      "updated_by": "a1b2c3d4...",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**Episode ID Generation:**
1. If the RSS `<guid>` exists and is non-empty, use `guid:<normalized_guid>`.
2. Otherwise, use `url:<sha256(normalized_url)[:16]>`.

This ensures that two clients referencing the same episode will always generate the same ID.

### `queue.json` (Snapshot)

Auto-generated. Clients may read it as an optimization, but the **canonical** queue state is always reconstructed from `queue_ops`.

```json
{
  "schema_version": "1.2.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "items": [
    {"ep_id": "guid:rss-guid-here", "added_at": 1700000000000}
  ]
}
```

### `queue_ops/<device-id>.jsonl`

Each line is a JSON object. Lines are appended; never modified in-place.

```jsonl
{"ts":1700000100000,"op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"op":"reorder","ids":["guid:xyz","guid:abc"]}
{"ts":1700000400000,"op":"clear"}
```

**Operations:**
- `add`: Insert `items` after `after_id` (`null` = append to end).
- `remove`: Delete items by `ep_id`.
- `reorder`: Replace entire queue order with `ids` (preserves items not in `ids` at the end).
- `clear`: Empty the queue.

---

## Merge Algorithm

### Feeds & Episodes (LWW-EL)

```python
def merge_records(local: dict, remote: dict) -> dict:
    merged = dict(local)
    for key, remote_record in remote.items():
        local_record = local.get(key)
        if not local_record:
            merged[key] = remote_record
            continue
        if remote_record["updated_at"] > local_record["updated_at"]:
            merged[key] = remote_record
        elif remote_record["updated_at"] == local_record["updated_at"]:
            if remote_record["updated_by"] > local_record["updated_by"]:
                merged[key] = remote_record
    return merged
```

### Queue (Operation Replay)

```python
def rebuild_queue(snapshot: dict, ops_dir: Path) -> list:
    items = list(snapshot.get("items", []))
    ops = []
    for f in ops_dir.glob("*.jsonl"):
        for line in f:
            ops.append(json.loads(line))
    ops.sort(key=lambda o: o["ts"])
    for op in ops:
        items = apply_op(items, op)
    return items
```

---

## Conflict & Edge Case Handling

| Scenario | Resolution |
|----------|------------|
| **Two devices add to queue offline** | Both `add` ops are replayed. Both episodes appear. |
| **Device A deletes feed, Device B adds episode to queue from that feed** | Feed deletion wins (LWW). The orphaned queue item remains but the client may gray it out. |
| **Syncthing conflict file** | Files matching `*.sync-conflict*` or `*.tmp` are **ignored entirely**. The main file is authoritative. |
| **Clock skew > 5 min** | Client logs a warning. Skewed timestamps still merge correctly locally, but the skewed device may incorrectly win conflicts during the skew window. |
| **New device with existing local state** | Client performs a **bootstrap**: reads local DB, converts to FPS format with fresh timestamps, writes to folder, then runs a normal sync. |
| **Corrupted JSON file** | Client attempts to restore from the latest `snapshots/*.json.gz`. If that fails, starts with empty state for that file. |
| **Rewinding progress** | Allowed. `progress_seconds` is the exact value from the most recent `updated_at`. No clamping. |

---

## Adding a New Device

1. Install the podcast client on the new device.
2. Point it to the existing `FilePodSync/` folder.
3. The client generates a new UUID and registers itself in `devices.json`.
4. **Bootstrap phase:** If the device already has local subscriptions/episodes, it writes them with current timestamps before reading the remote state.
5. Normal sync cycle begins. The device now has the union of all feeds and the merged episode states.

---

## Implementation Guide

### Kotlin (Android)

```kotlin
class FilePodSync(private val folder: File, private val deviceId: String) {
    private val gson = Gson()

    fun sync() {
        val remoteFeeds = loadJson(folder.resolve("feeds.json"))
        val remoteEps = loadJson(folder.resolve("episodes.json"))

        // Merge LWW-EL
        val mergedFeeds = mergeRecords(localFeeds, remoteFeeds)
        val mergedEps = mergeRecords(localEpisodes, remoteEps)

        // Rebuild queue from ops
        val queue = rebuildQueue(
            loadJson(folder.resolve("queue.json")),
            folder.resolve("queue_ops")
        )

        // Apply pending local ops with fresh timestamps
        applyLocalChanges(mergedFeeds, mergedEps, queue)

        // Atomic write
        atomicWrite(folder.resolve("feeds.json"), mergedFeeds)
        atomicWrite(folder.resolve("episodes.json"), mergedEps)
        flushQueueOps()
    }
}
```

### Python (Desktop / Terminal)

See `filepodsync.py` for a complete, production-oriented reference implementation.

---

## Sync Frequency

- **Write feeds/episodes:** Immediately on user action (subscribe, pause, completion).
- **Write queue ops:** Debounce for **2 seconds** after the last queue modification to avoid rapid successive operations.
- **Read / Full sync:** On app startup, resume from background, and every 60 seconds while active.
- **Consolidate queue ops:** When total pending ops > 50 or once per day.

---

## Privacy & Security

- **No credentials** in the sync folder. Only podcast metadata.
- **No audio files** are synced — only URLs and progress.
- For maximum privacy, use **Syncthing** (device-to-device) or an encrypted cloud provider (Filen, Cryptomator).

---

## Comparison

| Feature | gPodder.net | Nextcloud | oPodSync | **FilePodSync v1.2** |
|---|---|---|---|---|
| Server required | Public | Self-host | Self-host | **None** |
| Queue sync | ❌ | ❌ | ❌ | **✅ Op-based** |
| Offline edits merge safely | ❌ | ❌ | ❌ | **✅ Yes** |
| Dead feed detection | ❌ | ❌ | ❌ | **✅ Yes** |
| Setup complexity | Low | High | Medium | **Zero** |
| Conflict resolution | Server-side | Server-side | Server-side | **Deterministic LWW + Ops** |

---

## License

MIT. Implementations may use any license.

---

*FilePodSync — Because a folder is the only server you need.*

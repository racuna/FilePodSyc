# FilePodSync (FPS) Specification v1.3

> A file-based, serverless podcast synchronization protocol that actually works across multiple devices.

[![Protocol](https://img.shields.io/badge/protocol-1.3-blue)](SPEC.md)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## Interoperability Status

Current tested environments:

- LitePop ↔ LitePop
- Linux ↔ WSL2
- Syncthing transport
- Concurrent playback updates
- Queue synchronization
- Subscription synchronization

Status: Experimental but functional

---

## Why FilePodSync?

Existing podcast sync relies on **gPodder.net**, **Nextcloud-gPodder**, or **oPodSync** — all server-dependent, fragile, or requiring technical expertise to self-host. FilePodSync replaces the server with a **folder** synced by Dropbox, Syncthing, Google Drive, Filen, iCloud, or your NAS.

**No API. No OAuth. No server maintenance. Just files.**

You can try it with [LiTePop](https://github.com/racuna/litepop)!

---

## What Changed in v1.3

v1.3 fixes correctness issues and implementation ambiguities discovered in v1.2 client implementations:

- **Queue consolidation no longer touches other devices' files.** In v1.2, consolidation truncated all `.jsonl` files, including those belonging to other devices — a data-loss risk when another device was mid-write. Consolidation now only resets the local device's own op file, using a new `consolidated_through_ts` field in `queue.json` to skip already-applied ops on reconstruction.
- **Queue replay is now deterministic on timestamp ties.** All ops carry a `device_id` field. When two ops share the same `ts`, they are ordered by `(ts, device_id)`, producing identical results regardless of filesystem iteration order.
- **Conflict file detection covers all major sync providers.** v1.2 only filtered Syncthing-style names (`.sync-conflict`). v1.3 adds Dropbox (`(conflicted copy)`), Google Drive (` (1).json`), and generic patterns.
- **Log rotation uses filename dates, not filesystem `mtime`.** Sync providers routinely update `mtime` when verifying or transferring files, making `mtime`-based age calculations unreliable.
- **Bootstrap no longer resurrects deleted feeds.** If a feed exists in the remote folder with `status: "deleted"`, bootstrapping a new device with that feed locally will not override the deletion.
- **Snapshot restore preserves original timestamps.** In v1.2, restoring from a snapshot re-stamped all records with the current time, causing them to incorrectly win LWW conflicts against other devices' more recent edits.
- **Device records now track status.** Devices carry `status: "active" | "retired"` and their own `updated_at`/`updated_by` fields, enabling housekeeping of stale device entries.
- **Device ID is stored as plain UTF-8, not JSON-encoded.** `json.dumps` on a plain string wraps it in quotes, producing an invalid UUID when read back. The reference implementation and spec now use a direct `write_text` call.

---

## Directory Structure

```
FilePodSync/
├── config.json                 # Client capabilities & rotation policy
├── devices.json                # Device registry
├── feeds.json                  # Subscriptions (LWW-EL map)
├── episodes.json               # Episode states (LWW-EL map)
├── queue.json                  # Consolidated queue snapshot (auto-generated)
├── queue_ops/                  # Append-only queue operations, one file per device
│   ├── <device-uuid>.jsonl
│   └── <device-uuid>.jsonl
├── logs/                       # Append-only daily sync logs
│   └── sync-YYYYMMDD.jsonl
└── snapshots/                  # Compressed disaster-recovery backups
    └── snapshot-<ts>.json.gz
```

> **Golden Rule:** A device MUST only append to its own `queue_ops/<device-uuid>.jsonl`. It MUST NOT modify, truncate, or delete any other device's files — ever, including during queue consolidation.

---

## Core Concepts

### 1. LWW-EL (Last-Write-Wins at Element Level)

For **feeds**, **episodes**, and **devices**, each record carries two fields:

- `updated_at` — UTC timestamp in **milliseconds**.
- `updated_by` — UUID of the device that last wrote this record.

When merging two copies of the same record, the one with the later `updated_at` wins. If timestamps are identical, the lexicographically larger `updated_by` UUID wins — a deterministic, serverless tie-breaker that requires no coordination.

**What LWW-EL does not use:** the file-level `updated_at` field (the one at the root of `feeds.json`, `episodes.json`, etc.) is metadata about when the file was last written, not about when any individual record changed. It plays no role in merge decisions and must never be used as one.

### 2. Queue Operations (Op-Based Merge)

The playback queue cannot use LWW safely. If two devices add different episodes offline, a last-write-wins merge would silently discard one of them. Instead, each device appends **operations** to its own log file, and the queue is always reconstructed by replaying those operations in order.

```jsonl
{"ts":1700000100000,"device_id":"a1b2c3d4-...","op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"device_id":"a1b2c3d4-...","op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"device_id":"a1b2c3d4-...","op":"reorder","ids":["guid:xyz","guid:abc"]}
{"ts":1700000400000,"device_id":"a1b2c3d4-...","op":"clear"}
```

To reconstruct the queue:

1. Start from `queue.json` (snapshot) if it exists, otherwise an empty list.
2. Note the snapshot's `consolidated_through_ts` value (defaults to `0` if absent).
3. Read all `queue_ops/*.jsonl` files, skipping lines whose `ts` is at or below `consolidated_through_ts`.
4. Sort remaining operations by `(ts, device_id)` — the `device_id` field provides a deterministic tie-breaker when two ops land on the same millisecond.
5. Apply operations sequentially.

**Consolidation** happens when the total ops across all `.jsonl` files exceeds 50. The client rebuilds the queue, writes the result to `queue.json` with a `consolidated_through_ts` set to the latest op timestamp, and resets only its own op file. Other devices' files are never touched.

### 3. Three-State Sync Architecture

To prevent a stale remote from silently overwriting fresh local changes, the sync cycle maintains three distinct states:

```
Remote disk
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Read remote files                                        │
│  2. MergeRecords(synced_state, remote_state)  →  base       │
│  3. Replay pending_local_ops on top of base   →  final      │
│  4. AtomicWrite(final) to all four JSON files               │
│  5. synced_state = final  /  pending_local_ops.clear()      │
└─────────────────────────────────────────────────────────────┘
```

- **Synced state** — what this device last successfully wrote to disk. Acts as the "previous known good" baseline.
- **Merged base** — `synced_state` merged with whatever is currently on disk (which may include other devices' changes). Uses LWW-EL for feeds/episodes, op replay for the queue.
- **Final state** — merged base with any user actions taken since the last sync cycle (a pause, a subscribe, a queue reorder) layered on top.

This guarantees that a local "pause at 22:40" made while the device was offline survives the moment it reconnects and reads a remote file that is slightly newer.

---

## File Schemas

### `config.json`

```json
{
  "schema_version": "1.3.0",
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

`capabilities` allows partial clients to declare what they support. A client with `queue_sync: false` may omit `queue_ops/` entirely; other devices must tolerate its absence.

### `devices.json`

```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "devices": {
    "a1b2c3d4-e5f6-7890-abcd-ef1234567890": {
      "name": "Pixel 7",
      "platform": "android",
      "client": "antennapod",
      "status": "active",
      "first_seen": 1700000000000,
      "last_seen": 1700000000000,
      "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "updated_at": 1700000000000
    }
  }
}
```

**`status`** is `active` or `retired`. A client may mark a device `retired` if its `last_seen` is more than 90 days ago. Retired device records are kept for audit purposes; their `queue_ops/*.jsonl` files may be pruned once all their ops are older than the current `consolidated_through_ts`.

### `feeds.json`

```json
{
  "schema_version": "1.3.0",
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
      "added_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "added_at": 1700000000000,
      "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**Feed status values:** `active` | `archived` | `deleted`. Archiving hides the feed in the UI but preserves its episodes. Deletion is a soft-delete — the key and record are retained so the deletion propagates to all devices.

**URL Normalization:** Before using a URL as a dictionary key, clients MUST normalize it:

1. Lowercase the scheme and host.
2. Remove default ports (`:80` for HTTP, `:443` for HTTPS).
3. Decode percent-encoding in the path.
4. Remove a trailing slash from the path (unless the path is just `/`).
5. Preserve the query string and fragment.

`http://` and `https://` variants of the same URL are **distinct keys**. If a client detects a permanent redirect (HTTP 301) from the HTTP to the HTTPS version, it should write the HTTP entry as `status: "deleted"` and keep the HTTPS entry as `active`.

### `episodes.json`

```json
{
  "schema_version": "1.3.0",
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
      "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**Episode state values:** `unplayed` | `in_progress` | `completed` | `skipped`.

**Episode ID generation:**

1. If the RSS `<guid>` is present and non-empty, use `guid:<guid>`.
2. Otherwise, use `url:<first 16 hex chars of sha256(normalized_url)>`.

Two clients referencing the same episode will always generate the same ID, even if one found the episode via a different feed URL.

**`feed_url` is informative, not normative.** Some podcasts are cross-posted to multiple feeds with the same GUID. When this happens both clients will use the same episode ID; LWW-EL will decide which `feed_url` ends up stored. This is expected and does not affect playback state.

### `queue.json` (Snapshot)

```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "consolidated_through_ts": 1700000000000,
  "items": [
    {"ep_id": "guid:rss-guid-here", "added_at": 1700000000000}
  ]
}
```

This file is auto-generated by consolidation. The canonical queue is always the result of replaying `queue_ops`, not this snapshot alone. The snapshot exists as a performance optimization and a recovery baseline.

**`consolidated_through_ts`** is the timestamp of the latest op that was included when the snapshot was written. During reconstruction, ops at or below this timestamp are skipped. If the field is absent (snapshot written by a v1.2 client), treat it as `0` and replay all ops.

### `queue_ops/<device-uuid>.jsonl`

Each line is a complete, self-contained JSON object. Lines are appended only; never modified or deleted in-place. Each device writes exclusively to the file named after its own UUID.

```jsonl
{"ts":1700000100000,"device_id":"a1b2c3d4-...","op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"device_id":"a1b2c3d4-...","op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"device_id":"a1b2c3d4-...","op":"reorder","ids":["guid:xyz","guid:abc"]}
{"ts":1700000400000,"device_id":"a1b2c3d4-...","op":"clear"}
```

**Operations:**

- `add` — Insert `items` after the item with `ep_id == after_id`. If `after_id` is `null` or missing, append to the end.
- `remove` — Delete all items whose `ep_id` appears in `ids`.
- `reorder` — Reorder the queue to match `ids`. Items not present in `ids` retain their relative order and move to the end.
- `clear` — Empty the queue entirely.

Unknown operation types must be silently skipped, not rejected — this allows future op types to be added without breaking older clients.

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
    cutoff = snapshot.get("consolidated_through_ts", 0)
    ops = []
    for f in ops_dir.glob("*.jsonl"):
        if is_conflict_file(f.name):
            continue
        for line in f.read_text().splitlines():
            if line.strip():
                op = json.loads(line)
                if op["ts"] > cutoff:
                    ops.append(op)
    ops.sort(key=lambda o: (o["ts"], o.get("device_id", "")))
    for op in ops:
        items = apply_op(items, op)
    return items
```

---

## Conflict & Edge Case Handling

| Scenario | Resolution |
|---|---|
| Two devices add different episodes to the queue offline | Both `add` ops are replayed in `(ts, device_id)` order. Both episodes appear in the final queue. |
| Two devices reorder the queue offline | The op with the later `(ts, device_id)` wins. The earlier reorder is superseded. This is expected LWW behavior on the queue's shape, not its contents. |
| Device A deletes a feed; Device B adds an episode from it | Feed deletion wins (LWW). The episode record may remain in `episodes.json` as an orphan; client UIs may gray it out. |
| Device B bootstraps with a feed that Device A already deleted | The remote `deleted` record wins if its `updated_at` is more recent than the bootstrap timestamp. The feed is not resurrected. |
| Episode belongs to an archived feed | Archiving a feed does not freeze its episodes. Episode state updates continue to sync. Client UIs may choose to dim or hide such episodes. |
| Sync provider creates a conflict file (any provider) | Ignored entirely via `is_conflict_file()`. The canonical file is authoritative. See the provider compatibility table below. |
| Clock skew > 5 minutes | A warning is logged. The skewed device may incorrectly win LWW conflicts during the skew window. No automatic correction is applied. |
| Corrupted JSON file | Client attempts restore from the latest `snapshots/*.json.gz`, preserving original per-record `updated_at` values. If all snapshots fail, the file starts empty. Records are never re-stamped with the current time on restore. |
| New device with existing local subscriptions | See [Adding a New Device](#adding-a-new-device). |
| Device not seen for > 90 days | Any client may mark it `retired` in `devices.json`. Its op file may be pruned once all its ops are older than `consolidated_through_ts`. |

---

## Sync Provider Compatibility

FilePodSync has been designed around the conflict-file behaviors of the most common sync providers. The table below documents known behaviors and any caveats.

| Provider | Conflict naming | Notes |
|---|---|---|
| **Syncthing** | `filename.sync-conflict-DATE-VERSION.ext` | ✅ Recommended. Conflict file is always a copy; canonical file is preserved. |
| **Dropbox** | `filename (John's conflicted copy DATE).ext` | ✅ Good. Canonical file is preserved. |
| **Filen** | None (client-side E2EE prevents provider conflicts) | ✅ Ideal for privacy. End-to-end encrypted. |
| **Nextcloud / WebDAV** | Varies by client | ⚠️ Behavior depends on the desktop sync client used (e.g. Nextcloud Desktop may create `.sync-conflict` files). Test before deploying. |
| **Google Drive** | `filename (1).ext`, `filename (2).ext` | ⚠️ Google Drive **renames the original** and creates a numbered copy as the "new" file. The canonical `<uuid>.jsonl` or `feeds.json` may disappear. Clients should treat a missing canonical file as empty/new and recreate it on next sync. |
| **iCloud Drive** | `filename (conflicted copy DATE).ext` | ⚠️ iCloud may also rename the original. Same recovery behavior as Google Drive applies. |
| **NAS (SMB/NFS)** | None | ⚠️ No built-in conflict handling. If two devices mount the same share and write simultaneously, the last write wins at the OS level. Use atomic writes (`write .tmp → rename`) to minimize the window. |

A file must be ignored if its name matches any of these patterns:

- Contains `.sync-conflict`
- Contains ` (conflicted copy)`
- Matches `<name> (<number>).<ext>` (e.g. `feeds (1).json`)
- Ends with `.tmp`
- Ends with `.partial`
- Starts with `.`

---

## Adding a New Device

1. Install the podcast client on the new device.
2. Point it at the existing `FilePodSync/` folder (or create a new one if this is the first device).
3. The client generates a new UUID v4 and persists it to `.fps_device_id` as a plain text file.
4. **Bootstrap phase:** If the device already has local subscriptions and episode progress:
   - For each local feed, check if the remote folder already has that feed with `status: "deleted"`. If so, do not overwrite the deletion — the user removed it on another device intentionally.
   - Write all other local records to the folder with `updated_at = now` as pending operations.
5. Run one sync cycle. The device now holds the union of all feeds and merged episode states from every device that has used this folder.

A device that has never had local data simply registers itself and reads the current folder state — no bootstrap step needed.

---

## Migrating from gPodder / oPodSync

FilePodSync maps cleanly onto gPodder concepts:

| gPodder concept | FilePodSync equivalent |
|---|---|
| `subscriptions` | `feeds.json` |
| `episode_actions` | `episodes.json` |
| `timestamp` | `updated_at` (UTC milliseconds) |
| `device` identifier | `updated_by` (UUID v4) |
| `position` / `total` | `progress_seconds` / `duration_seconds` |
| Queue | `queue_ops/*.jsonl` (op-based; no gPodder equivalent) |

To migrate an existing gPodder setup:

1. Export your subscriptions as OPML from your podcast client.
2. Export episode actions from your gPodder server (or local database).
3. Run your FPS client's bootstrap import with that data — or any client that accepts OPML will create the `feeds.json` entries on first sync.

OPML export from FPS is deterministic: it includes all feeds where `status != "deleted"`, ordered by title.

---

## Implementation Guide

### Kotlin / Android

```kotlin
class FilePodSync(private val folder: File, private val deviceId: String) {

    fun sync() {
        val remoteFeeds  = loadJson(folder.resolve("feeds.json"))
        val remoteEps    = loadJson(folder.resolve("episodes.json"))
        val remoteDevs   = loadJson(folder.resolve("devices.json"))
        val remoteQueue  = loadJson(folder.resolve("queue.json"))

        // LWW-EL merge for maps
        val mergedFeeds = mergeRecords(syncedFeeds, remoteFeeds)
        val mergedEps   = mergeRecords(syncedEps,   remoteEps)
        val mergedDevs  = mergeRecords(syncedDevs,  remoteDevs)

        // Rebuild queue from ops (respects consolidated_through_ts)
        val mergedQueue = rebuildQueue(
            snapshot = remoteQueue,
            opsDir   = folder.resolve("queue_ops"),
            isConflictFile = ::isConflictFile
        )

        // Replay any in-memory pending local ops
        applyPendingOps(mergedFeeds, mergedEps)

        // Stamp this device as last_seen
        mergedDevs[deviceId]?.put("last_seen", utcMs())

        // Atomic write — all four files
        atomicWrite(folder.resolve("feeds.json"),   mergedFeeds)
        atomicWrite(folder.resolve("episodes.json"), mergedEps)
        atomicWrite(folder.resolve("devices.json"), mergedDevs)
        atomicWrite(folder.resolve("queue.json"),   mergedQueue)

        // Flush any pending queue ops to own file only
        flushQueueOps(folder.resolve("queue_ops").resolve("$deviceId.jsonl"))
    }

    private fun atomicWrite(file: File, data: Map<*, *>) {
        val tmp = File(file.parent, file.name + ".tmp")
        tmp.writeText(gson.toJson(data))
        tmp.renameTo(file)   // atomic on POSIX; best-effort on Windows
    }
}
```

### Python (Desktop / Terminal)

See `filepodsync.py` for the full reference implementation. Key points for implementors:

```python
# Device ID: plain text, no json.dumps
id_file.write_text(str(uuid.uuid4()), encoding="utf-8")

# Queue ops: always include device_id
op = {"ts": utc_ms(), "device_id": self.device_id, "op": "add", ...}

# Queue reconstruction: sort by (ts, device_id)
ops.sort(key=lambda o: (o["ts"], o.get("device_id", "")))

# Consolidation: only truncate own file
own_file = ops_dir / f"{self.device_id}.jsonl"
own_file.write_text("")   # reset own log only

# Log rotation: use filename date, not mtime
cutoff = (today - timedelta(days=max_days)).strftime("%Y%m%d")
for f in log_dir.glob("sync-????????.jsonl"):
    if f.stem[5:] < cutoff:
        f.unlink()
```

---

## Sync Frequency

- **Feeds and episodes** — write immediately on user action (subscribe, pause, mark complete, skip).
- **Queue ops** — debounce for **2 seconds** after the last queue modification, then write. On app shutdown, flush immediately without waiting.
- **Full sync cycle** — on app startup, on resume from background, and every 60 seconds while the app is in the foreground. Enforce a minimum of 5 seconds between full cycles to avoid hammering the sync provider.
- **Queue consolidation** — when total ops across all `.jsonl` files exceed `queue_ops_consolidate_at` (default 50), or once per day.
- **Snapshots** — after each successful sync cycle (optional but recommended).

---

## Known Limitations

FilePodSync deliberately trades some features for simplicity and zero server dependency. Before adopting it, consider whether these limitations apply to your use case:

**No encryption at rest.** The sync folder contains plaintext JSON. Anyone with access to the folder can read your subscription list and listening history. For sensitive use cases, use an end-to-end encrypted provider (Filen) or full-disk encryption on the sync volume (Cryptomator).

**No conflict UI.** LWW-EL is deterministic but silent. If two devices mark the same episode as completed and in-progress within the same millisecond, one result wins without any notification. In practice, millisecond-exact conflicts are rare, but they cannot be surfaced to the user with the current protocol. A future version could add a `conflicts` log file, but this would require clients to actively read and display it.

**`episodes.json` grows without bound.** There is no built-in retention policy for completed episodes. A feed with years of history will accumulate hundreds of entries. For very large libraries this can make sync cycles slower. A future minor version may introduce optional per-feed episode sharding (`episodes/<feed-hash>.json`).

**No support for authenticated feeds.** Feeds behind HTTP Basic Auth or cookie-based authentication cannot be stored in the sync folder — credentials must never be written there. Clients that subscribe to such feeds must store credentials locally and handle them outside FPS.

**Clock skew degrades correctness.** If one device's clock is significantly ahead of the others, it will win every LWW conflict during the skew window, regardless of which write was actually more recent. There is no automatic correction; the warning is informational only.

**Concurrent reorders are resolved by timestamp.** If two devices reorder the queue while offline, only one result survives — the one with the later timestamp wins. The other device's preferred order is silently lost. This is a known tradeoff of the op-based model.

---

## Privacy & Security

- **No credentials** are ever written to the sync folder. Only podcast metadata (feed URLs, titles, episode progress).
- **No audio files** are synced — only URLs and playback state.
- The folder may contain your full subscription and listening history. Treat it accordingly.
- For maximum privacy: use **Syncthing** (peer-to-peer, no cloud) or **Filen** (end-to-end encrypted cloud).
- For auditing: the `logs/` directory contains a per-device record of every sync cycle. Rotation is configurable.

---

## Comparison

| Feature | gPodder.net | Nextcloud | oPodSync | **FilePodSync v1.3** |
|---|---|---|---|---|
| Server required | Public | Self-host | Self-host | **None** |
| Queue sync | ❌ | ❌ | ❌ | **✅ Op-based** |
| Offline edits merge safely | ❌ | ❌ | ❌ | **✅ Yes** |
| Dead feed detection | ❌ | ❌ | ❌ | **✅ Yes** |
| Setup complexity | Low | High | Medium | **Zero** |
| Conflict resolution | Server-side | Server-side | Server-side | **Deterministic LWW + Ops** |
| End-to-end encryption | ❌ | Optional | ❌ | **Provider-dependent** |
| Works fully offline | ❌ | ❌ | ❌ | **✅ Yes** |

---

## License

MIT. Implementations may use any license.

---

*FilePodSync — Because a folder is the only server you need.*

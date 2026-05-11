# FilePodSync Agent Specification v1.3

Strict technical rules, JSON schemas, merge algorithms, validation constraints, and implementation patterns for FilePodSync clients.

## 🔒 Core Constraints

1. **Single Folder Dependency**: Client must only require read/write access to one root folder.
2. **No Central Server**: All sync is provider-mediated. Client never contacts external sync APIs directly.
3. **JSON-Only**: State files must be valid JSON. No binary formats except compressed snapshots.
4. **LWW-EL**: Last-Write-Wins at Element Level for feeds/episodes/devices. Deterministic, stateless merge. The file-level `updated_at` wrapper field is metadata only and MUST NOT be used as a merge tiebreaker for individual records.
5. **Op-Based Queue**: Queue uses append-only per-device operation logs to prevent data loss during concurrent offline edits.
6. **Idempotent Operations**: Reapplying sync must not corrupt state.
7. **Schema Versioning**: All JSON state files include `schema_version` (semver string). Clients must reject files whose major version differs from their own. Minor and patch version differences within the same major MUST be backwards-compatible; implementors MUST NOT introduce breaking changes in minor or patch bumps.
8. **Atomic Writes**: Always write to `.tmp` then `os.replace()`. Never overwrite in-place.
9. **URL Normalization**: All feed URLs used as dictionary keys MUST be normalized before comparison or hashing.
10. **GUID Priority**: Episode identity uses RSS `<guid>` first, SHA256(URL) second. Cross-client matching depends on this.
11. **Device File Sovereignty**: A device MUST only append to its own `queue_ops/<device-id>.jsonl`. It MUST NOT modify, truncate, or delete another device's files under any circumstance, including during queue consolidation.

## 📐 JSON Schemas

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

### `devices.json`
```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "devices": {
    "<uuid-v4>": {
      "name": "Litepop Terminal",
      "platform": "linux-desktop",
      "client": "litepop",
      "status": "active",
      "first_seen": 1700000000000,
      "last_seen": 1700000000000,
      "updated_by": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "updated_at": 1700000000000
    }
  }
}
```

**Device status values:** `active` | `retired`. A device that has not been seen for more than 90 days MAY be marked `retired` by any client. Retired devices are retained in `devices.json` for audit purposes but their `queue_ops/*.jsonl` files MAY be pruned during housekeeping.

### `feeds.json`
```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "feeds": {
    "<normalized-feed-url>": {
      "url": "https://...",
      "title": "...",
      "status": "active|archived|deleted",
      "health_status": "healthy|stale|error",
      "last_check": 1700000000000,
      "error_count": 0,
      "added_by": "<device-uuid>",
      "added_at": 1700000000000,
      "updated_by": "<device-uuid>",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**URL Normalization Rules:**
1. Lowercase scheme and host.
2. Remove default ports (`:80` for HTTP, `:443` for HTTPS).
3. Decode percent-encoding in path.
4. Remove trailing slash from path (if path length > 1).
5. Preserve query string and fragment.
6. HTTP and HTTPS variants of the same URL are treated as **distinct keys**. Clients that detect a permanent redirect (HTTP 301) from HTTP to HTTPS for the same path SHOULD store the HTTPS URL as canonical and write the HTTP entry with `status: "deleted"`.

### `episodes.json`
```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "episodes": {
    "<ep-id>": {
      "feed_url": "<normalized-feed-url>",
      "guid": "rss-guid-here",
      "url": "https://...",
      "title": "...",
      "state": "unplayed|in_progress|completed|skipped",
      "progress_seconds": 0,
      "duration_seconds": 0,
      "updated_by": "<device-uuid>",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

**Episode ID Generation:**
```
IF guid IS present AND non-empty AND stripped lowercase != "none":
  id = "guid:" + stripped_guid
ELSE:
  id = "url:" + hex(sha256(normalized_url))[0:16]
```

**`feed_url` field semantics:** This field is informative, not normative. When the same episode GUID appears in multiple feeds (cross-posted content), LWW-EL determines which `feed_url` is stored. Clients MUST NOT use `feed_url` to validate episode ownership; the episode record belongs to whoever last wrote it.

### `queue.json` (Snapshot)
```json
{
  "schema_version": "1.3.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "consolidated_through_ts": 1700000000000,
  "items": [
    {"ep_id": "<ep-id-1>", "added_at": 1700000000000},
    {"ep_id": "<ep-id-2>", "added_at": 1700000000000}
  ]
}
```

**`consolidated_through_ts`:** The UTC-ms timestamp of the latest op that was included when this snapshot was written. During queue reconstruction, ops with `ts <= consolidated_through_ts` MUST be skipped. This prevents double-application of ops that have already been folded into the snapshot, and allows safe partial truncation of op logs without reading every op file in full.

If `consolidated_through_ts` is absent (e.g. snapshot was written by a v1.2 client), treat it as `0` and replay all ops normally.

### `queue_ops/<device-uuid>.jsonl`
Each line is a self-contained JSON object. Append-only. A device MUST only write to the file whose name matches its own UUID.

```jsonl
{"ts":1700000100000,"device_id":"<uuid>","op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"device_id":"<uuid>","op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"device_id":"<uuid>","op":"reorder","ids":["guid:xyz","guid:abc"]}
{"ts":1700000400000,"device_id":"<uuid>","op":"clear"}
```

**`device_id` field:** Present in every op line. Used as a secondary sort key when two ops share the same `ts`, ensuring deterministic replay order across all clients regardless of filesystem iteration order.

**Operations:**
- `add`: Insert `items` after `after_id`. If `after_id` is null or missing, append to end.
- `remove`: Delete all items whose `ep_id` is in `ids`.
- `reorder`: Replace the order of items present in `ids`. Items not in `ids` retain their relative order and move to the end. This is a LWW operation: if two devices reorder concurrently, the one with the higher `ts` (or `device_id` on tie) wins.
- `clear`: Remove all items.

## 🧠 Merge Algorithms

### LWW-EL (Feeds, Episodes, Devices)

```text
FUNCTION MergeRecords(local_map, remote_map):
  merged = COPY(local_map)
  FOR EACH key, remote_record IN remote_map:
    local_record = local_map.get(key)
    IF local_record IS NULL:
      merged[key] = remote_record
      CONTINUE

    r_ts = remote_record.updated_at
    l_ts = local_record.updated_at

    IF r_ts > l_ts:
      merged[key] = remote_record
    ELSE IF r_ts == l_ts:
      IF remote_record.updated_by > local_record.updated_by:
        merged[key] = remote_record
  RETURN merged
```

**Key invariants:**
- The merge is commutative and associative. Applying it in any order produces the same result.
- The file-level wrapper `updated_at` is never read during merge. It is written after merge to record when the file was last touched by this device.
- When restoring from a snapshot, preserve original per-record `updated_at` values. Do NOT stamp records with the current time. This ensures the next sync cycle correctly resolves conflicts against other devices' concurrent writes.

### Queue Reconstruction

```text
FUNCTION RebuildQueue(snapshot, ops_dir):
  items = snapshot.items IF snapshot EXISTS ELSE []
  cutoff_ts = snapshot.consolidated_through_ts IF present ELSE 0
  ops = []

  FOR EACH file IN ops_dir MATCHING "*.jsonl":
    IF IsConflictFile(file.name): CONTINUE
    FOR EACH line IN file:
      op = PARSE_JSON(line)
      IF op.ts > cutoff_ts:          # skip already-consolidated ops
        ops.append(op)

  SORT ops BY (ts, device_id) ASCENDING  # deterministic on timestamp ties

  FOR EACH op IN ops:
    items = ApplyQueueOp(items, op)

  RETURN items

FUNCTION ApplyQueueOp(items, op):
  IF op.op == "add":
    new_items = op.items
    IF op.after_id IS NULL:
      items = items + new_items
    ELSE:
      idx = FIND_INDEX(items, ep_id == op.after_id)
      IF idx IS NULL: idx = LENGTH(items) - 1
      INSERT new_items AFTER idx

  ELSE IF op.op == "remove":
    items = FILTER(items, ep_id NOT IN op.ids)

  ELSE IF op.op == "reorder":
    order_map = {id: index FOR index, id IN op.ids}
    mentioned = FILTER(items, ep_id IN op.ids)
    not_mentioned = FILTER(items, ep_id NOT IN op.ids)
    SORT mentioned BY order_map[ep_id] ASCENDING
    items = mentioned + not_mentioned

  ELSE IF op.op == "clear":
    items = []

  RETURN items
```

### Three-State Sync Cycle

```text
FUNCTION SyncCycle():
  1. remote = ReadRemoteState()
  2. ValidateSchemaVersions(remote)      # reject major-version mismatch
  3. DetectClockSkew(remote)

  4. # Merge independent records (LWW-EL)
     base_feeds = MergeRecords(synced.feeds, remote.feeds)
     base_eps   = MergeRecords(synced.episodes, remote.episodes)
     base_devs  = MergeRecords(synced.devices, remote.devices)

  5. # Rebuild queue from ops (skipping already-consolidated ops)
     base_queue = RebuildQueue(remote.queue, queue_ops_dir)

  6. # Replay in-memory pending ops on top of merged base
     FOR EACH op IN pending_ops:
       IF op.type IN (feed_add, feed_remove, feed_archive):
         IF op.ts >= base_feeds[op.url].updated_at:
           ApplyFeedOp(base_feeds, op)
       ELSE IF op.type == ep_update:
         IF op.ts >= base_eps[op.ep_id].updated_at:
           ApplyEpisodeOp(base_eps, op)
     # Queue pending ops are written to queue_ops/<device-id>.jsonl
     # before SyncCycle runs; they are already included in step 5.

  7. # Register/update this device in the merged devices map
     base_devs[this.device_id].last_seen = GetUtcMs()

  8. # Atomic write — all four files written before returning
     AtomicWrite(feeds.json,   WrapMeta(base_feeds))
     AtomicWrite(episodes.json, WrapMeta(base_eps))
     AtomicWrite(devices.json, WrapMeta(base_devs))
     AtomicWrite(queue.json,   WrapMeta(base_queue))

  9. # Update internal synced state
     synced = {feeds: base_feeds, episodes: base_eps,
               devices: base_devs, queue: base_queue}
     pending_ops.clear()

  10. Housekeeping()
  11. LogSyncAction()
```

## ⚡ Implementation Requirements

### Atomic Writes
```python
def atomic_write(path, data):
    tmp = path + ".tmp"
    write_json(tmp, data)
    os.replace(tmp, path)
```

The `.tmp` file MUST be on the same filesystem/volume as the target so that `os.replace()` (or equivalent) is atomic. Never write the device ID file with `json.dumps`; write it as a plain UTF-8 string to avoid JSON quoting artifacts.

```python
# CORRECT — plain text, no JSON encoding
id_file.write_text(new_uuid, encoding="utf-8")
id = id_file.read_text(encoding="utf-8").strip()

# WRONG — json.dumps wraps the string in quotes
_atomic_write(id_file, new_uuid)   # produces "\"uuid-here\""
```

### File Filtering (Conflict Files)
A file MUST be ignored if its name matches any of the following patterns. This list covers known behaviors of common sync providers:

| Pattern | Provider |
|---|---|
| Contains `.sync-conflict` | Syncthing |
| Contains ` (conflicted copy)` | Dropbox |
| Ends with ` (1).json`, ` (2).json`, etc. | Google Drive |
| Ends with `.tmp` | Any (atomic write temp) |
| Ends with `.partial` | Any (interrupted write) |
| Starts with `.` | Hidden/system files |

```text
FUNCTION IsConflictFile(filename):
  RETURN (
    ".sync-conflict" IN filename OR
    " (conflicted copy)" IN filename OR
    MATCHES(filename, r" \(\d+\)\.(json|jsonl)$") OR
    filename ENDS_WITH ".tmp" OR
    filename ENDS_WITH ".partial" OR
    filename STARTS_WITH "."
  )
```

### Clock Skew Detection
```
local_time = GetUtcMs()
max_remote_time = MAX(remote_file.updated_at FOR ALL remote_files)
IF ABS(local_time - max_remote_time) > 300000:
  LOG_WARNING("Clock skew > 5 minutes detected")
```

### Queue Debounce
Clients MUST debounce rapid queue modifications:
- Wait **≥ 2 seconds** after the last queue action before writing to `queue_ops/<device-id>.jsonl`.
- Exception: app shutdown must flush immediately.

When flushing, drain pending ops atomically to avoid double-writes:

```python
# CORRECT — atomic drain
ops, self._pending_queue_ops = self._pending_queue_ops, []
if not ops:
    return
# write ops to disk...

# WRONG — check-then-clear has a race window
if not self._pending_queue_ops:
    return
# ... write ops ...
self._pending_queue_ops.clear()   # another thread may have added ops here
```

### Queue Consolidation
When the total number of ops across all `queue_ops/*.jsonl` files exceeds `queue_ops_consolidate_at` (default 50), a client SHOULD consolidate:

```text
FUNCTION ConsolidateQueue():
  items = RebuildQueue(current_snapshot, ops_dir)
  max_ts = MAX(op.ts FOR ALL ops read during rebuild)

  # Write new snapshot with consolidated_through_ts
  AtomicWrite(queue.json, WrapMeta(items, consolidated_through_ts=max_ts))

  # ONLY truncate the device's own op file
  own_ops_file = ops_dir / (this.device_id + ".jsonl")
  own_ops_file.write_text("")

  # DO NOT touch other devices' .jsonl files
```

**Rationale:** Truncating another device's op file is a sovereignty violation (Constraint 11). That device may be mid-write at the moment of truncation, causing data loss. The `consolidated_through_ts` field in the snapshot makes it safe to leave other devices' op files intact; ops at or before that timestamp will be skipped on the next rebuild.

### Log Rotation
Rotate logs by **filename date**, not filesystem `mtime`. Sync providers may update `mtime` when verifying files, making `mtime`-based age calculations unreliable.

```python
# CORRECT — parse date from filename
cutoff = (utc_today - timedelta(days=LOG_MAX_DAYS)).strftime("%Y%m%d")
for f in log_dir.glob("sync-????????.jsonl"):
    file_date = f.stem[5:]          # "sync-20240101" → "20240101"
    if file_date < cutoff:          # lexicographic comparison is correct for YYYYMMDD
        f.unlink()

# WRONG — mtime is set by the sync provider, not by this client
if f.stat().st_mtime * 1000 < cutoff_ms:
    f.unlink()
```

### Bootstrap (New Device)
When a device with existing local data joins a sync folder:
1. Generate new UUID v4. Persist to `.fps_device_id` as plain UTF-8 (no JSON encoding).
2. Convert local DB to FPS format.
3. For each feed, check if a record already exists in the remote `feeds.json`:
   - If the remote record has `status: "deleted"`, do **not** overwrite it with a fresh `active` record unless the user explicitly resubscribes. Bootstrapping a previously-deleted feed surprises other devices.
   - Otherwise, write the local record with `updated_at = now`.
4. Write all other records (episodes, queue) with `updated_at = now` as **pending ops**.
5. Run `SyncCycle()` once.

## 🛡️ Conflict & Edge Case Handling

| Scenario | Resolution |
|---|---|
| Provider creates `.sync-conflict-*.json` | Ignore entirely. Do not parse. Rely on canonical file. |
| Provider creates conflict file in `queue_ops/` (any naming pattern) | Ignore via `IsConflictFile()`. Each device's canonical op file is `<device-uuid>.jsonl` only. |
| Two devices add to queue offline | Both `add` ops replay in `(ts, device_id)` order. Both episodes present in final queue. |
| Two devices reorder queue offline | The op with the higher `(ts, device_id)` wins (LWW). The other device's reorder is silently superseded. This is expected and documented behavior. |
| Device A deletes feed while B adds episode from it | Feed deletion wins (LWW). Episode record may remain orphaned in `episodes.json`; client UI may gray it out. |
| Device A deletes feed; Device B bootstraps with that feed | Bootstrap does NOT resurrect the deleted feed. The remote `deleted` record wins if its `updated_at` is more recent than the bootstrap timestamp. |
| Rewinding episode progress | Allowed. `progress_seconds` is exact value from most recent `updated_at`. No `max()` clamping. |
| Episode on archived feed | Archiving a feed does not freeze its episodes. Episode state updates continue to sync normally. Client UI may choose to hide or dim episodes from archived feeds. |
| Corrupted `feeds.json` | Attempt restore from latest `snapshots/*.json.gz`, preserving original per-record timestamps. If all snapshots fail, start empty for that file. Do NOT stamp restored records with the current time. |
| Clock skew (device > 5 min ahead) | Warning logged. During skew window, that device may incorrectly win LWW conflicts. No automatic correction. |
| Schema v2.x encountered | Reject with clear error. Do not silently ignore unknown fields that may break logic. |
| Empty normalized URL | Reject. A feed MUST have a non-empty normalized URL. |
| Duplicate `guid` across different feeds | Treated as the same episode (same `ep_id`). `feed_url` in the stored record reflects whichever device last wrote it (LWW). This is an RSS publisher error; FPS does not deduplicate across feeds. |
| Device file not seen for > 90 days | Client MAY mark device as `retired` in `devices.json`. Client MAY prune the corresponding `queue_ops/<uuid>.jsonl` if the device is `retired` and the file contributes no ops newer than `consolidated_through_ts`. |
| iCloud/Google Drive renames the canonical file on conflict | The original `<uuid>.jsonl` or `feeds.json` may disappear. The client should treat a missing canonical file as empty/new (not an error), then proceed with the next sync cycle which will recreate it. |

## ✅ Implementation Checklist

- [ ] Generate UUID v4 per install. Persist to `.fps_device_id` as plain UTF-8 (no JSON encoding). Never reuse across reinstalls.
- [ ] Use UTC milliseconds for all `*_at` fields.
- [ ] Implement atomic file writes (`write .tmp → os.replace`).
- [ ] Handle JSON parsing errors gracefully (fallback to snapshot or empty state).
- [ ] Validate `schema_version` by parsing major version as an integer. Reject major-version mismatch. Accept any minor/patch within the same major.
- [ ] Normalize URLs before using as dictionary keys.
- [ ] Generate episode IDs using `guid:` prefix when RSS GUID is available.
- [ ] Implement three-state sync (`synced` → `merged` → `local`).
- [ ] Write queue ops to `queue_ops/<device-id>.jsonl` only. Never write to another device's file.
- [ ] Include `device_id` field in every queue op line.
- [ ] Sort ops by `(ts, device_id)` during queue reconstruction.
- [ ] Debounce queue writes ≥ 2s. Drain pending ops atomically on flush.
- [ ] Consolidate queue ops when total count > threshold. Only truncate own op file.
- [ ] Write `consolidated_through_ts` to `queue.json` snapshot after consolidation.
- [ ] Skip ops with `ts <= consolidated_through_ts` during queue reconstruction.
- [ ] Implement log rotation by filename date (not filesystem `mtime`).
- [ ] Prune snapshots to configured retention.
- [ ] Detect and warn on clock skew > 5 min.
- [ ] Provide `config.json` with client capabilities.
- [ ] Debounce rapid sync triggers (min 5s between full sync cycles).
- [ ] Filter provider conflict files before parsing, using `IsConflictFile()` with all known patterns.
- [ ] Implement bootstrap flow: check remote for `deleted` feeds before overwriting.
- [ ] On snapshot restore, preserve original per-record `updated_at` values.
- [ ] Export OPML 2.0 from `feeds.json` on demand (feeds where `status != "deleted"`).
- [ ] Add `status` and `updated_at`/`updated_by` fields to device records.
- [ ] Support marking devices as `retired` after 90 days of inactivity.

## 🧪 Testing Guidelines

1. **Empty Folder Bootstrap**: Verify initial files created with correct schema. Device registers itself with `status: "active"`.
2. **LWW Merge**: Simulate concurrent edits with differing timestamps. Verify winner. Simulate identical timestamps; verify `updated_by` UUID tie-break is deterministic.
3. **Queue Concurrent Add**: Device A adds ep1 offline. Device B adds ep2 offline. Sync both. Verify queue contains both. Verify order is `(ts, device_id)` deterministic.
4. **Queue Reorder + Add**: Reorder on A, add on B (concurrent). Verify final state: B's add is preserved, A's reorder wins (higher ts assumed). Swap timestamps and verify the opposite outcome.
5. **Conflict File Handling**: Introduce `*.sync-conflict-*`, `* (conflicted copy)*`, and `* (1).json` files. Verify client ignores all patterns.
6. **Rewind Progress**: Set progress to 3600, then set to 10 with a later timestamp. Verify final value is 10.
7. **Dead Feed Sync**: Force `error_count` increment on one device. Verify `health_status` and `error_count` propagate.
8. **Log Rotation by Date**: Write log files with names spanning 35 days. Verify files with dates older than `log_max_days` are deleted, regardless of filesystem `mtime`. Verify files newer than cutoff are retained.
9. **Partial Clients**: Run client without queue support (`queue_sync: false`). Verify feeds/episodes sync correctly; `queue_ops/` and `queue.json` are untouched.
10. **Corruption Recovery**: Corrupt `episodes.json`. Verify snapshot restore succeeds with original per-record timestamps preserved. Corrupt all snapshots too; verify graceful empty-state fallback.
11. **Bootstrap — New Feeds**: Create device with 50 local feeds. Point to existing sync folder with 30 different feeds. Verify final union of 80 feeds (assuming no overlaps).
12. **Bootstrap — Deleted Feed**: Remote folder has feed X with `status: "deleted"`. New device has feed X locally. Verify bootstrap does NOT resurrect feed X.
13. **Consolidation Sovereignty**: Device A triggers consolidation while Device B is mid-write to its own op file. Verify Device A does not truncate Device B's file. Verify `consolidated_through_ts` is correctly set. Verify Device B's op survives and is applied on next rebuild.
14. **`consolidated_through_ts` Skip**: Write a snapshot with `consolidated_through_ts = T`. Add ops with `ts <= T` to op files. Verify those ops are skipped during rebuild. Verify ops with `ts > T` are applied.
15. **Device ID Persistence**: Read `.fps_device_id` written by client. Verify value is a plain UUID string with no surrounding quotes. Verify the same UUID is used across restarts.
16. **Device Retirement**: Simulate a device not seen for 91 days. Verify another client marks it `retired`. Verify its op file is pruned only after its ops are all at or before `consolidated_through_ts`.
17. **HTTP/HTTPS Feed Deduplication**: Subscribe to `http://feeds.example.com/rss` and `https://feeds.example.com/rss`. Verify they are stored as two distinct keys. If the client detects a 301 redirect, verify it writes the HTTP entry as `deleted` and keeps the HTTPS entry as `active`.

## 📦 Extensibility Notes

- Custom fields must be under the `custom` object to avoid namespace collisions.
- Tags use reverse-domain notation: `com.clientname:key`.
- Snapshots are optional but strongly recommended for disaster recovery. Compress with `gzip`.
- OPML generation is deterministic from `feeds.json` where `status != "deleted"`.
- Unknown op types in `queue_ops/*.jsonl` MUST be skipped with a warning, not rejected. This allows future op types to be added without breaking older clients.
- Unknown fields in any JSON record MUST be preserved on read and re-written on merge. Silently dropping unknown fields breaks forward-compatibility.

**gPodder API Mapping:**
- `subscriptions` → `feeds.json`
- `episode_actions` → `episodes.json`
- `timestamp` → `updated_at` (UTC ms)
- `device` → `updated_by` (UUID)
- `position/total` → `progress_seconds/duration_seconds`
- `queue` → NEW in FilePodSync (op-based, no gPodder equivalent)

## 🐍 Python Reference Pattern

```python
import json, os, uuid, hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse, urlunparse, unquote

FPS_SCHEMA = "1.3.0"
FPS_COMPATIBLE_MAJOR = 1   # integer comparison, not string prefix


class FilePodSyncCore:
    def __init__(self, folder: Path):
        self.folder = folder
        self.device_id = self._load_device_id()
        self._synced = {}
        self._pending = []

    def _load_device_id(self) -> str:
        id_file = self.folder / ".fps_device_id"
        if id_file.exists():
            return id_file.read_text(encoding="utf-8").strip()
        new_id = str(uuid.uuid4())
        # Plain text write — do NOT use json.dumps or _atomic_write here,
        # as that would wrap the string in JSON quotes.
        id_file.write_text(new_id, encoding="utf-8")
        return new_id

    def _validate_schema(self, version: str, path: str) -> bool:
        try:
            major = int(version.split(".")[0])
        except (ValueError, IndexError, AttributeError):
            return False
        if major != FPS_COMPATIBLE_MAJOR:
            raise ValueError(
                f"Schema major version mismatch in {path}: "
                f"found {version}, compatible major is {FPS_COMPATIBLE_MAJOR}"
            )
        return True

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        p = urlparse(url.strip())
        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        if ":" in netloc:
            host, port = netloc.rsplit(":", 1)
            if (scheme == "http" and port == "80") or \
               (scheme == "https" and port == "443"):
                netloc = host
        path = unquote(p.path)
        if path.endswith("/") and len(path) > 1:
            path = path[:-1]
        return urlunparse((scheme, netloc, path, "", p.query, p.fragment))

    def _ep_id(self, guid: Optional[str], url: str) -> str:
        if guid and guid.strip().lower() not in ("", "none"):
            return f"guid:{guid.strip()}"
        return f"url:{hashlib.sha256(self._normalize_url(url).encode()).hexdigest()[:16]}"

    def _atomic_write(self, path: Path, data: dict) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(data, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8"
        )
        os.replace(str(tmp), str(path))

    def _is_conflict_file(self, name: str) -> bool:
        import re
        low = name.lower()
        return (
            ".sync-conflict" in low or
            " (conflicted copy)" in low or
            bool(re.search(r" \(\d+\)\.(json|jsonl)$", low)) or
            low.endswith(".tmp") or
            low.endswith(".partial") or
            name.startswith(".")
        )

    def _rotate_logs(self, log_dir: Path, max_days: int) -> None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_days)) \
                 .strftime("%Y%m%d")
        for f in log_dir.glob("sync-????????.jsonl"):
            file_date = f.stem[5:]      # "sync-20240101" → "20240101"
            if file_date < cutoff:
                f.unlink(missing_ok=True)

    def _flush_queue_ops(self, ops_dir: Path, pending: list,
                         device_id: str) -> None:
        # Atomic drain — swap out the list before any I/O
        ops, pending[:] = list(pending), []
        if not ops:
            return
        ops_file = ops_dir / f"{device_id}.jsonl"
        lines = [json.dumps(op, ensure_ascii=False) for op in ops]
        with ops_file.open("a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def _consolidate_queue(self, snapshot: dict, ops_dir: Path,
                           device_id: str) -> dict:
        """Rebuild queue into a new snapshot; only truncate own op file."""
        items, max_ts = self._rebuild_queue_with_max_ts(snapshot, ops_dir)
        new_snapshot = {
            "schema_version": FPS_SCHEMA,
            "updated_at": self._utc_ms(),
            "updated_by": device_id,
            "consolidated_through_ts": max_ts,
            "items": items,
        }
        # Truncate only the device's own op file
        own_file = ops_dir / f"{device_id}.jsonl"
        if own_file.exists():
            own_file.write_text("", encoding="utf-8")
        return new_snapshot

    def _rebuild_queue_with_max_ts(self, snapshot: dict,
                                   ops_dir: Path) -> tuple:
        items = list(snapshot.get("items", []))
        cutoff = snapshot.get("consolidated_through_ts", 0)
        ops = []
        max_ts = cutoff

        if ops_dir.exists():
            for f in ops_dir.iterdir():
                if not f.is_file() or f.suffix != ".jsonl":
                    continue
                if self._is_conflict_file(f.name):
                    continue
                try:
                    for line in f.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        op = json.loads(line)
                        if op.get("ts", 0) > cutoff:
                            ops.append(op)
                            max_ts = max(max_ts, op.get("ts", 0))
                except (json.JSONDecodeError, OSError):
                    pass

        ops.sort(key=lambda o: (o.get("ts", 0), o.get("device_id", "")))
        for op in ops:
            items = self._apply_queue_op(items, op)
        return items, max_ts

    @staticmethod
    def _utc_ms() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def _apply_queue_op(items: list, op: dict) -> list:
        t = op.get("op")
        if t == "add":
            new_items = list(op.get("items", []))
            after_id = op.get("after_id")
            if after_id is None:
                return items + new_items
            try:
                idx = next(i for i, it in enumerate(items)
                           if it.get("ep_id") == after_id)
                return items[:idx + 1] + new_items + items[idx + 1:]
            except StopIteration:
                return items + new_items
        elif t == "remove":
            rm = set(op.get("ids", []))
            return [it for it in items if it.get("ep_id") not in rm]
        elif t == "reorder":
            order = {eid: i for i, eid in enumerate(op.get("ids", []))}
            mentioned = sorted(
                [it for it in items if it.get("ep_id") in order],
                key=lambda it: order[it["ep_id"]]
            )
            rest = [it for it in items if it.get("ep_id") not in order]
            return mentioned + rest
        elif t == "clear":
            return []
        # Unknown op type — skip with no error (forward-compatibility)
        return items
```

---

## 📋 What Changed in v1.3

| Area | Change |
|---|---|
| Constraint 4 | Clarified that the wrapper `updated_at` MUST NOT be used in record-level merge decisions. |
| Constraint 7 | Specified that minor/patch bumps within the same major MUST be backwards-compatible. |
| Constraint 11 | New. Device File Sovereignty: a device may never write to another device's files. |
| `devices.json` | Added `status` (`active`/`retired`), `updated_at`, `updated_by` per device record. Defined 90-day retirement policy. |
| `feeds.json` | Added HTTP→HTTPS redirect handling guidance. Clarified that HTTP and HTTPS variants are distinct keys. |
| `episodes.json` | Clarified `feed_url` is informative, not normative. |
| `queue.json` | Added `consolidated_through_ts` field to snapshot. |
| `queue_ops/*.jsonl` | Added `device_id` field to every op line. |
| Queue reconstruction | Sort key is now `(ts, device_id)` for deterministic tie-breaking. Ops at or before `consolidated_through_ts` are skipped. |
| Queue consolidation | Redefined: only the device's own op file is truncated. Other devices' files are never touched. |
| Conflict file patterns | Expanded `IsConflictFile()` to cover Dropbox, Google Drive, iCloud naming in addition to Syncthing. |
| Log rotation | Changed from `mtime`-based to filename-date-based to avoid sync provider interference. |
| Bootstrap | Added guard: deleted feeds in remote state MUST NOT be resurrected by bootstrap. |
| Snapshot restore | Clarified: per-record `updated_at` values MUST be preserved; do not stamp with current time. |
| Device ID persistence | Clarified: write as plain UTF-8 string, never via `json.dumps`. |
| Extensibility | Unknown op types and unknown JSON fields must be preserved/skipped, not rejected. |
| Testing | Added 6 new test cases covering: consolidation sovereignty, `consolidated_through_ts` skip, device ID format, device retirement, bootstrap with deleted feeds, HTTP/HTTPS deduplication. |

---

*This specification is strict by design. Implementations must follow LWW-EL, schema validation, deterministic merge rules, and atomic writes to guarantee cross-client compatibility without centralized coordination.*

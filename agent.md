# FilePodSync Agent Specification v1.2

Strict technical rules, JSON schemas, merge algorithms, validation constraints, and implementation patterns for FilePodSync clients.

## 🔒 Core Constraints

1. **Single Folder Dependency**: Client must only require read/write access to one root folder.
2. **No Central Server**: All sync is provider-mediated. Client never contacts external sync APIs directly.
3. **JSON-Only**: State files must be valid JSON. No binary formats except compressed snapshots.
4. **LWW-EL**: Last-Write-Wins at Element Level for feeds/episodes/devices. Deterministic, stateless merge.
5. **Op-Based Queue**: Queue uses append-only per-device operation logs to prevent data loss during concurrent offline edits.
6. **Idempotent Operations**: Reapplying sync must not corrupt state.
7. **Schema Versioning**: All JSON state files include `schema_version` (semver). Clients must reject or gracefully handle incompatible future versions (major version mismatch).
8. **Atomic Writes**: Always write to `.tmp` then `os.replace()`. Never overwrite in-place.
9. **URL Normalization**: All feed URLs used as dictionary keys MUST be normalized before comparison or hashing.
10. **GUID Priority**: Episode identity uses RSS `<guid>` first, SHA256(URL) second. Cross-client matching depends on this.

## 📐 JSON Schemas

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
    "<uuid-v4>": {
      "name": "Litepop Terminal",
      "platform": "linux-desktop",
      "client": "litepop",
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
2. Remove default ports (`:80` HTTP, `:443` HTTPS).
3. Decode percent-encoding in path.
4. Remove trailing slash from path (if path length > 1).
5. Preserve query string and fragment.

### `episodes.json`
```json
{
  "schema_version": "1.2.0",
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
IF guid IS present AND non-empty AND != "none":
  id = "guid:" + stripped_guid
ELSE:
  id = "url:" + hex(sha256(normalized_url))[0:16]
```

### `queue.json` (Snapshot)
```json
{
  "schema_version": "1.2.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "items": [
    {"ep_id": "<ep-id-1>", "added_at": 1700000000000},
    {"ep_id": "<ep-id-2>", "added_at": 1700000000000}
  ]
}
```

### `queue_ops/<device-uuid>.jsonl`
Each line is a self-contained JSON object. Append-only.

```jsonl
{"ts":1700000100000,"op":"add","items":[{"ep_id":"guid:abc","added_at":1700000100000}],"after_id":null}
{"ts":1700000200000,"op":"remove","ids":["guid:abc"]}
{"ts":1700000300000,"op":"reorder","ids":["guid:xyz","guid:abc"]}
{"ts":1700000400000,"op":"clear"}
```

**Operations:**
- `add`: Insert `items` after `after_id`. If `after_id` is null or missing, append.
- `remove`: Delete all items whose `ep_id` is in `ids`.
- `reorder`: Reorder queue to match `ids`. Items not in `ids` retain relative order and move to end.
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

### Queue Reconstruction

```text
FUNCTION RebuildQueue(snapshot_path, ops_dir):
  items = snapshot.items IF snapshot EXISTS ELSE []
  ops = []

  FOR EACH file IN ops_dir MATCHING "*.jsonl":
    IF file.name CONTAINS "sync-conflict": CONTINUE
    FOR EACH line IN file:
      ops.append(PARSE_JSON(line))

  SORT ops BY ts ASCENDING

  FOR EACH op IN ops:
    items = ApplyQueueOp(items, op)

  RETURN items

FUNCTION ApplyQueueOp(items, op):
  IF op.op == "add":
    IF op.after_id IS NULL:
      items = items + op.items
    ELSE:
      idx = FIND_INDEX(items, ep_id == op.after_id)
      IF idx IS NULL: idx = LENGTH(items) - 1
      INSERT op.items AFTER idx

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
  2. ValidateSchemaVersions(remote)
  3. DetectClockSkew(remote)

  4. # Merge independent records
     base_feeds = MergeRecords(synced.feeds, remote.feeds)
     base_eps   = MergeRecords(synced.episodes, remote.episodes)
     base_devs  = MergeRecords(synced.devices, remote.devices)

  5. # Rebuild queue from ops
     base_queue = RebuildQueue(remote.queue, queue_ops_dir)

  6. # Replay local pending ops on top of merged base
     FOR EACH op IN pending_ops:
       IF op.type IN (feed_add, feed_remove):
         IF op.ts >= base_feeds[op.id].updated_at:
           ApplyFeedOp(base_feeds, op)
       ELSE IF op.type == ep_update:
         IF op.ts >= base_eps[op.ep_id].updated_at:
           ApplyEpisodeOp(base_eps, op)
     # Queue ops are already in queue_ops files; they were read in step 5.

  7. # Atomic write
     AtomicWrite(feeds.json, WrapMeta(base_feeds))
     AtomicWrite(episodes.json, WrapMeta(base_eps))
     AtomicWrite(devices.json, WrapMeta(base_devs))
     AtomicWrite(queue.json, WrapMeta(base_queue))

  8. # Update internal synced state
     synced = {feeds: base_feeds, episodes: base_eps, devices: base_devs, queue: base_queue}
     pending_ops.clear()

  9. Housekeeping()
  10. LogSyncAction()
```

## ⚡ Implementation Requirements

### Atomic Writes
```python
def atomic_write(path, data):
    tmp = path + ".tmp"
    write_json(tmp, data)
    os.replace(tmp, path)
```

### File Filtering (Conflict Files)
A file MUST be ignored if its name contains any of:
- `.sync-conflict`
- `.tmp`
- `.partial`
- starts with `.`

### Clock Skew Detection
```
local_time = GetUtcMs()
max_remote_time = MAX(remote_file.updated_at FOR ALL remote_files)
IF ABS(local_time - max_remote_time) > 300000:
  LOG_WARNING("Clock skew > 5 minutes detected")
```

### Queue Debounce
Clients MUST debounce rapid queue modifications:
- Wait **≥ 2 seconds** after the last queue action before writing to `queue_ops/*.jsonl`.
- Exception: app shutdown must flush immediately.

### Bootstrap (New Device)
When a device with existing local data joins a sync folder:
1. Generate new UUID.
2. Convert local DB to FPS format.
3. Write all records with current timestamps as **pending ops**.
4. Run `SyncCycle()` once.
5. This uploads local state and merges with any remote state atomically.

## 🛡️ Conflict & Edge Case Handling

| Scenario | Resolution |
|----------|------------|
| Provider creates `.sync-conflict-*.json` | Ignore entirely. Do not parse. Rely on canonical file. |
| Provider creates `.sync-conflict-*.jsonl` in `queue_ops/` | Ignore. Each device only reads its own op file plus non-conflict files from others. |
| Two devices add to queue offline | Both `add` ops replay. Both episodes present in final queue. |
| Device A deletes feed while B adds episode from it | Feed deletion wins (LWW). Episode record may remain orphaned in `episodes.json`; client UI may gray it out. |
| Rewinding episode progress | Allowed. `progress_seconds` is exact value from most recent `updated_at`. No `max()` clamping. |
| Corrupted `feeds.json` | Attempt restore from latest `snapshots/*.json.gz`. If fail, start empty. |
| Clock skew (device 5 min ahead) | Warning logged. During skew window, that device incorrectly wins LWW conflicts. |
| Schema v2.0 encountered | Reject with clear error. Do not silently ignore unknown fields that may break logic. |
| Empty normalized URL | Reject. A feed MUST have a non-empty URL. |
| Duplicate `guid` across different feeds | Treat as same episode. This is an RSS publisher error; FPS does not deduplicate across feeds. |

## ✅ Implementation Checklist

- [ ] Generate UUID v4 per install. Persist to `.fps_device_id`. Never reuse across reinstalls.
- [ ] Use UTC milliseconds for all `*_at` fields.
- [ ] Implement atomic file writes (`write temp -> rename`).
- [ ] Handle JSON parsing errors gracefully (fallback to snapshot or empty state).
- [ ] Validate `schema_version`. Reject incompatible major versions.
- [ ] Normalize URLs before using as dictionary keys.
- [ ] Generate episode IDs using `guid:` prefix when RSS guid is available.
- [ ] Implement three-state sync (`synced` → `merged` → `local`).
- [ ] Write queue ops to `queue_ops/<device-id>.jsonl` only.
- [ ] Debounce queue writes ≥ 2s.
- [ ] Consolidate queue ops when total count > 50.
- [ ] Implement log rotation by age and size.
- [ ] Prune snapshots to configured retention.
- [ ] Detect and warn on clock skew > 5 min.
- [ ] Provide `config.json` with client capabilities.
- [ ] Debounce rapid sync triggers (min 5s between full sync cycles).
- [ ] Filter provider conflict files before parsing.
- [ ] Implement bootstrap flow for devices with existing local data.
- [ ] Export OPML 2.0 from `feeds.json` on demand.

## 🧪 Testing Guidelines

1. **Empty Folder Bootstrap**: Verify initial files created with correct schema. Device registers itself.
2. **LWW Merge**: Simulate concurrent edits with differing timestamps. Verify winner.
3. **Queue Concurrent Add**: Device A adds ep1 offline. Device B adds ep2 offline. Sync both. Verify queue contains both in chronological order.
4. **Queue Reorder + Add**: Reorder on A, add on B. Verify final state is deterministic.
5. **Conflict File Handling**: Introduce `*.sync-conflict-*`. Verify client ignores.
6. **Rewind Progress**: Set progress to 3600, then set to 10. Verify final value is 10.
7. **Dead Feed Sync**: Force `error_count` increment on one device. Verify status propagates.
8. **Rotation**: Write 35 days of logs. Verify cleanup.
9. **Partial Clients**: Run client without queue support. Verify feeds/episodes sync correctly; queue files untouched.
10. **Corruption Recovery**: Corrupt `episodes.json`. Verify snapshot restore or graceful degradation.
11. **Bootstrap**: Create device with 50 local feeds. Point to existing sync folder. Verify union of feeds.

## 📦 Extensibility Notes

- Custom fields must be under `custom` object to avoid namespace collisions.
- Tags use reverse-domain notation: `com.clientname:key`.
- Snapshots are optional but recommended for disaster recovery. Compress with `gzip`.
- OPML generation is deterministic from `feeds.json` where `status != "deleted"`.
- **gPodder API Mapping**:
  - `subscriptions` → `feeds.json`
  - `episode_actions` → `episodes.json`
  - `timestamp` → `updated_at` (UTC ms)
  - `device` → `updated_by` (UUID)
  - `position/total` → `progress_seconds/duration_seconds`
  - `queue` → NEW in FilePodSync (op-based)

## 🐍 Python Reference Pattern

```python
import json, os, time, uuid, hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, unquote
from typing import Optional

FPS_SCHEMA = "1.2.0"
FPS_COMPATIBLE = "1."

class FilePodSyncCore:
    def __init__(self, folder: Path):
        self.folder = folder
        self.device_id = self._load_device_id()
        self._synced = {}
        self._pending = []

    def _normalize_url(self, url: str) -> str:
        p = urlparse(url.strip())
        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        if ':' in netloc:
            host, port = netloc.rsplit(':', 1)
            if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
                netloc = host
        path = unquote(p.path)
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]
        return urlunparse((scheme, netloc, path, '', p.query, p.fragment))

    def _ep_id(self, guid: Optional[str], url: str) -> str:
        if guid and guid.strip().lower() not in ("", "none"):
            return f"guid:{guid.strip()}"
        return f"url:{hashlib.sha256(self._normalize_url(url).encode()).hexdigest()[:16]}"

    def _atomic_write(self, path: Path, data: dict):
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        os.replace(str(tmp), str(path))
```

---
*This specification is strict by design. Implementations must follow LWW-EL, schema validation, deterministic merge rules, and atomic writes to guarantee cross-client compatibility without centralized coordination.*

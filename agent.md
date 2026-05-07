# FilePodSync Agent Specification v1.1

This document contains strict technical rules, JSON schemas, merge algorithms, validation constraints, and implementation patterns for FilePodSync clients.

## 🔒 Core Constraints
1. **Single Folder Dependency**: Client must only require read/write access to one root folder.
2. **No Central Server**: All sync is provider-mediated. Client never contacts external sync APIs directly.
3. **JSON-Only**: State files must be valid JSON. No binary formats except compressed snapshots.
4. **LWW-EL**: Last-Write-Wins at Element Level. Deterministic, stateless merge.
5. **Idempotent Operations**: Reapplying sync must not corrupt state.
6. **Schema Versioning**: All files include `schema_version`. Clients must reject or gracefully handle future versions.
7. **Atomic Writes**: Always write to `.tmp` then `os.rename()`. Never overwrite in-place.

## 📐 JSON Schemas

### `config.json`
```json
{
  "schema_version": "1.1.0",
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
    "snapshot_retention": 5
  }
}
```

### `devices.json`
```json
{
  "schema_version": "1.1.0",
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
  "schema_version": "1.1.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "feeds": {
    "<feed-url-or-id>": {
      "url": "https://...",
      "title": "...",
      "status": "active|archived|deleted|dead",
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

### `episodes.json`
```json
{
  "schema_version": "1.1.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "episodes": {
    "<ep-id>": {
      "feed_url": "<feed-url-or-id>",
      "guid": "rss-guid-here",
      "url": "https://...",
      "title": "...",
      "state": "unplayed|in_progress|completed",
      "progress_seconds": 0,
      "duration_seconds": 0,
      "updated_by": "<device-uuid>",
      "updated_at": 1700000000000,
      "custom": {}
    }
  }
}
```

### `queue.json`
```json
{
  "schema_version": "1.1.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "items": [
    {"ep_id": "<ep-id-1>", "added_at": 1700000000000},
    {"ep_id": "<ep-id-2>", "added_at": 1700000000000}
  ]
}
```

## 🧠 Merge Algorithm (Deterministic LWW-EL)

```text
FUNCTION SyncMerge(local, remote, file_type):
  IF remote IS NULL: RETURN local
  IF local IS NULL: RETURN remote
  
  IF file_type == "queue":
    # Queue uses file-level LWW to prevent interleaving chaos.
    # Clients MUST debounce writes (≥2s) before flushing queue.json
    IF remote.updated_at > local.updated_at: RETURN remote
    IF remote.updated_at == local.updated_at:
      RETURN IF remote.updated_by > local.updated_by THEN remote ELSE local
    RETURN local

  MERGED = COPY OF local
  FOR EACH key, record IN remote:
    IF key NOT IN local:
      MERGED[key] = record
    ELSE:
      IF record.updated_at > local[key].updated_at:
        MERGED[key] = record
      ELSE IF record.updated_at == local[key].updated_at:
        IF record.updated_by > local[key].updated_by:
          MERGED[key] = record
  RETURN MERGED
```

## ⚡ Sync Cycle Pseudocode

```text
1. CHECK folder exists & writable.
2. DOWNLOAD remote files (ignore *.sync-conflict*, *.tmp, *.partial).
3. PARSE local & remote JSON. Validate schema_version.
4. FOR EACH file_type IN [feeds, episodes, queue, devices]:
     state[file_type] = SyncMerge(local[file_type], remote[file_type], file_type)
5. APPLY pending local ops (add, remove, progress, queue-reorder):
     Update state with new UTC-ms timestamps & device UUID.
6. WRITE merged state to folder atomically (write .tmp -> rename).
7. LOG sync action to logs/sync-YYYYMMDD.jsonl
8. ROTATE logs & snapshots per config.rotation
9. GENERATE OPML on demand from feeds.json (optional)
```

## 🛡️ Conflict & Edge Case Handling

| Scenario | Resolution |
|----------|------------|
| Provider creates `.sync-conflict-file.json` | Explicitly ignore or auto-delete. Rely on main file LWW. |
| Two devices pause same episode simultaneously | LWW applies. `updated_at` tie-breaker ensures determinism. |
| New device with existing local state | Union feeds. LWW for episodes/queue. Upload merged state. |
| Queue reordering on one device | Entire `queue.json` overwrites older version. Debounce ≥2s. |
| Feed marked dead on one device | `status: "dead"` + `health_status: "error"` syncs. Others stop fetching. |
| Clock skew / NTP unavailable | Use UTC ms. Fallback to device monotonic counter + `device_drift_ms`. |
| `archive` vs `delete` conflict | `deleted` overrides `archived` via LWW. UI should warn on restore. |

## ✅ Implementation Checklist
- [ ] Generate UUID v4 per install. Never reuse.
- [ ] Use UTC milliseconds for all `*_at` fields.
- [ ] Implement atomic file writes (`write temp -> rename`).
- [ ] Handle JSON parsing errors gracefully (fallback to last-known good state).
- [ ] Respect `schema_version`. Reject future versions with clear warning.
- [ ] Implement log rotation & snapshot cleanup.
- [ ] Provide `config.json` with client capabilities.
- [ ] Debounce rapid sync triggers (min 5s between writes).
- [ ] Map RSS `guid` → stable ID. Fallback to `sha256(url)`.
- [ ] Queue writes MUST debounce ≥2s to avoid provider thrashing.
- [ ] Filter provider conflict files before parsing.

## 🧪 Testing Guidelines
1. **Empty Folder Bootstrap**: Verify initial files created with correct schema.
2. **LWW Merge**: Simulate concurrent edits with differing timestamps. Verify winner.
3. **Conflict File Handling**: Introduce `*.sync-conflict-*`. Verify client ignores.
4. **Queue Sync**: Test reorder, add, remove across 2 devices. Verify determinism.
5. **Dead Feed Sync**: Force `error_count` increment + `last_check` update on one device. Verify status propagates.
6. **Rotation**: Write 35 days of logs. Verify cleanup.
7. **Partial Clients**: Run client without queue support. Verify feeds/episodes sync correctly.

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

## 🐍 Python Implementation Pattern
```python
import json, os, time, uuid, hashlib
from pathlib import Path
from datetime import datetime, timezone

FPS_DIR = Path.home() / ".config" / "filepodsync"
FPS_DIR.mkdir(parents=True, exist_ok=True)

DEVICE_ID = uuid.uuid4().hex  # Persist this per install

def atomic_write(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    os.replace(str(tmp), str(path))

def get_utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def sync_trigger(local_path: Path, remote_path: Path):
    # Download, merge LWW, apply local pending, atomic write back
    pass
```

---
*This specification is strict by design. Implementations must follow LWW-EL, schema validation, deterministic merge rules, and atomic writes to guarantee cross-client compatibility without centralized coordination.*
```

### 🔑 Key Improvements Based
1. **Explicit gPodder API Mapping**: Direct field translations so you can replace `GPodderSync` with a 50-line file I/O wrapper.
2. **Queue Debounce Rule**: Prevents Syncthing/Dropbox from generating conflict storms when reordering. Enforces ≥2s write coalescing.
3. **Dead Feed Health Tracking**: Adds `health_status`, `last_check`, `error_count` to `feeds.json` so devices share fetch failures without re-parsing XML locally.
4. **Archive vs Delete Semantics**: Clarifies LWW precedence. Matches your `delete_and_mark_done` vs queue removal logic.
5. **Atomic Write Pattern**: Python-specific guidance to avoid partial writes during sync, which causes JSON decode crashes.
6. **Capability Negotiation**: `config.json` now explicitly declares what a client supports, allowing `litepop.py` to gracefully ignore tags/snapshots if not implemented.

This spec is production-ready, directly addresses the architectural pain points in your current implementation, and maintains strict provider-agnosticism. You can drop it into a GitHub repo and start building the `FilePodSync` module immediately.

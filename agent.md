# FilePodSync Agent Specification v1.0

This document contains strict technical rules, JSON schemas, merge algorithms, and validation constraints for implementing FilePodSync clients.

## 🔒 Core Constraints
1. **Single Folder Dependency**: Client must only require read/write access to one root folder.
2. **No Central Server**: All sync is provider-mediated. Client never contacts external sync APIs directly.
3. **JSON-Only**: State files must be valid JSON. No binary formats except compressed snapshots.
4. **LWW-EL**: Last-Write-Wins at Element Level. Deterministic, stateless merge.
5. **Idempotent Operations**: Reapplying sync must not corrupt state.
6. **Schema Versioning**: All files include `schema_version`. Clients must reject or gracefully handle future versions.

## 📐 JSON Schemas

### `config.json`
```json
{
  "schema_version": "1.0.0",
  "sync_interval_ms": 1800000,
  "capabilities": {
    "queue_sync": true,
    "tag_sync": true,
    "snapshot_sync": false
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
  "schema_version": "1.0.0",
  "devices": {
    "<uuid-v4>": {
      "name": "Device Name",
      "platform": "android|desktop|cli",
      "first_seen": 1700000000000,
      "last_seen": 1700000000000
    }
  }
}
```

### `feeds.json`
```json
{
  "schema_version": "1.0.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "feeds": {
    "<feed-id>": {
      "url": "https://...",
      "title": "...",
      "status": "active|archived|deleted|dead",
      "added_by": "<device-uuid>",
      "added_at": 1700000000000,
      "updated_by": "<device-uuid>",
      "updated_at": 1700000000000,
      "last_check": 1700000000000,
      "error_count": 0,
      "custom": {}
    }
  }
}
```

### `episodes.json`
```json
{
  "schema_version": "1.0.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "episodes": {
    "<ep-id>": {
      "feed_id": "<feed-id>",
      "guid": "rss-guid-here",
      "url": "https://...",
      "title": "...",
      "state": "unplayed|in_progress|completed",
      "progress_seconds": 0,
      "duration_seconds": 0,
      "archived": false,
      "deleted": false,
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
  "schema_version": "1.0.0",
  "updated_at": 1700000000000,
  "updated_by": "<device-uuid>",
  "items": ["<ep-id-1>", "<ep-id-2>", "..."]
}
```

## 🧠 Merge Algorithm (Deterministic LWW-EL)

```text
FUNCTION SyncMerge(local, remote, file_type):
  IF remote IS NULL: RETURN local
  IF local IS NULL: RETURN remote
  
  IF file_type == "queue":
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
2. DOWNLOAD remote files (ignore *.sync-conflict*, *.tmp).
3. PARSE local & remote JSON.
4. FOR EACH file_type IN [feeds, episodes, queue, devices]:
     state[file_type] = SyncMerge(local[file_type], remote[file_type], file_type)
5. APPLY pending local ops (add, remove, progress, queue-reorder):
     Update state with new timestamps & device UUID.
6. WRITE merged state to folder (atomic rename recommended).
7. LOG sync action to logs/sync-YYYYMMDD.jsonl
8. ROTATE logs & snapshots per config.rotation
9. GENERATE OPML on demand from feeds.json (optional)
```

## 🛡️ Conflict & Edge Case Handling

| Scenario | Resolution |
|----------|------------|
| Provider creates `.sync-conflict-file.json` | Ignore or delete. Rely on main file LWW merge. |
| Two devices pause same episode simultaneously | LWW applies. `updated_at` tie-breaker ensures determinism. |
| New device with existing local state | Union feeds. LWW for episodes/queue. Upload merged state. |
| Queue reordering on one device | Entire `queue.json` overwrites older version. Clients should debounce rapid reorders. |
| Feed marked dead on one device | `status: "dead"` syncs. Other devices stop fetching after next sync. |
| Clock skew / NTP unavailable | Use monotonic local counters. Add `device_time_drift_ms` to logs for debugging. |

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
- [ ] Handle `archived` vs `deleted` per LWW rules.

## 🧪 Testing Guidelines
1. **Empty Folder Bootstrap**: Verify initial files created with correct schema.
2. **LWW Merge**: Simulate concurrent edits with differing timestamps. Verify winner.
3. **Conflict File Handling**: Introduce `*.sync-conflict-*`. Verify client ignores.
4. **Queue Sync**: Test reorder, add, remove across 2 devices. Verify determinism.
5. **Dead Feed Sync**: Force `error_count` increment on one device. Verify status propagates.
6. **Rotation**: Write 35 days of logs. Verify cleanup.
7. **Partial Clients**: Run client without queue support. Verify feeds/episodes sync correctly.

## 📦 Extensibility Notes
- Custom fields must be under `custom` object to avoid namespace collisions.
- Tags use reverse-domain notation: `com.clientname:key`.
- Snapshots are optional but recommended for disaster recovery. Compress with `gzip`.
- OPML generation is deterministic from `feeds.json` where `status != "deleted"`.

---
*This specification is strict by design. Implementations must follow LWW-EL, schema validation, and deterministic merge rules to guarantee cross-client compatibility.*
```

### 💡 Implementation Notes for Developers
- **Android/Kotlin**: Use `OkHttp` or `WorkManager` for background sync. Atomic writes via `FileOutputStream` + `renameTo()`.
- **Desktop (Python/TS/Rust/C++)**: Watch folder changes with OS-native watchers (inotify/FSEvents/ReadDirectoryChangesW). Debounce writes.
- **Queue Sync**: If your client supports reordering, debounce rapid changes (e.g., 2s) before writing `queue.json` to avoid thrashing.
- **Dead Feed Detection**: Compute locally using `last_check` + `error_count`. Sync `status: "dead"` so all clients show consistent UI.
- **OPML**: Export on-demand. Format standard RSS OPML 2.0. Skip deleted feeds.
- **Conflict Files**: Syncthing/Dropbox may create `.sync-conflict` files. Your app should explicitly filter them out during sync.

This specification is production-ready, provider-agnostic, and designed to avoid the xkcd 927 problem by standardizing the *data contract* rather than the *sync transport*.

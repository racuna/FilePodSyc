# FilePodSync (FPS) Specification v1.1

**FilePodSync** is a lightweight, cloud-provider-agnostic podcast synchronization standard. It replaces centralized servers and custom HTTP APIs with a deterministic, file-based approach that works seamlessly with Dropbox, Google Drive, Syncthing, Filen, Nextcloud, or any folder-sync service.

## 🎯 Goals
- **Zero-API Architecture**: No servers, no auth tokens, no endpoint guessing. Just read/write JSON files.
- **Drop-in Replacement for gPodder Clients**: Direct field mapping from `gpodder.net`/`opodsync`/`nextcloud-gpodder` APIs.
- **Deterministic LWW Merge**: Last-Write-Wins at the record level. Fully conflict-resistant across devices.
- **Queue & State Sync**: First-class support for playback queues, episode progress, archiving, and dead-feed detection.
- **Extensible & Partial-Implementation Friendly**: Clients can sync only subscriptions, or add queue/tags/snapshots later.

## 📁 Directory Structure
```
/fps-sync/
├── config.json          # Schema version, sync settings, client capabilities
├── devices.json         # Registered device IDs & metadata
├── feeds.json           # Subscriptions, health status, archive/delete flags
├── episodes.json        # Episode states (progress, completion, timestamps)
├── queue.json           # Ordered playback queue (LWW-synced)
├── tags.json            # Optional: user-defined tags (namespaced)
├── logs/                # Append-only sync logs (rotated)
│   └── sync-YYYYMMDD.jsonl
└── snapshots/           # Optional: compressed full-state backups
    └── snapshot-YYYYMMDDTHHmmssZ.json.gz
```

## 🔄 Sync Flow
1. **Trigger**: App opens, closes, pauses playback, or hits periodic interval.
2. **Fetch & Lock**: Download remote JSON files. Ignore `*.sync-conflict*` files.
3. **Merge**: Apply LWW-EL (Last-Write-Wins at Element Level). Tie-breaker: lexicographical device UUID.
4. **Apply Local**: Replay pending local operations (add/remove/play/pause/queue-reorder).
5. **Upload**: Write merged state atomically (`temp` → `rename`).
6. **Housekeeping**: Rotate logs, prune snapshots, debounce rapid writes (≥2s).

## ⚖️ Conflict Resolution
- **LWW-EL**: Each record carries `updated_at` (UTC ms) and `updated_by` (UUID v4).
- **Queue**: Entire `queue.json` follows LWW. Clients MUST debounce rapid reorders to avoid thrashing.
- **New Device**: Merges via LWW. Unions feeds, keeps latest episode states.
- **Archived vs Deleted**: `status` can be `active`, `archived`, or `deleted`. Deletion overrides archiving via LWW.
- **Dead Feed Detection**: Syncs `last_check`, `error_count`, and `health_status`. Clients share health for consistent UI.

## 🧩 Extensibility
- **Custom Fields**: `custom` object in feeds/episodes for client-specific metadata (e.g., `playback_speed`, `auto_queue`).
- **Tags**: Optional `tags.json` with reverse-domain notation (`com.clientname:fav`).
- **Robust IDs**: `id = guid || sha256(url)`. Ensures cross-client episode matching.
- **OPML Export**: Auto-generated on-demand from `feeds.json`.
- **Capability Negotiation**: `config.json` declares supported features. Clients gracefully degrade.

## 🚀 Getting Started
1. Create an empty folder in your sync provider.
2. Configure your podcast client to point to it.
3. On first launch, the client generates initial `config.json`, `devices.json`, and empty state files.
4. Add more devices pointing to the same folder. They auto-merge.

## 📜 License
MIT. Use freely, fork, extend, and contribute.

---
*FilePodSync is designed to be simple, robust, and provider-agnostic. No servers, no APIs, just files.*
```


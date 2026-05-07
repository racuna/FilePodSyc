"""
FilePodSync (FPS) Client Library v1.2
Provider-agnostic, file-based podcast synchronization standard.
Zero-API, LWW-EL merge, atomic writes, op-based queue, conflict-resistant.
"""

import json
import os
import time
import uuid
import hashlib
import threading
import gzip
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable, Set
from urllib.parse import urlparse, urlunparse, unquote
from dataclasses import dataclass, field, asdict

# ─────────────────────────────────────────────────────────────
# CONSTANTS & UTILS
# ─────────────────────────────────────────────────────────────
SCHEMA_VERSION = "1.2.0"
SCHEMA_COMPATIBLE_MAJOR = "1."
LOG_MAX_MB = 10
LOG_MAX_DAYS = 30
SNAPSHOT_RETENTION = 5
QUEUE_DEBOUNCE_S = 2.0
QUEUE_OPS_CONSOLIDATE_AT = 50
SKEW_WARNING_MS = 300_000  # 5 minutes

logger = logging.getLogger("filepodsync")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)


def get_utc_ms() -> int:
    """Return current UTC time in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _atomic_write(path: Path, data: Any) -> None:
    """Write JSON atomically: temp file + rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
    os.replace(str(tmp), str(path))


def _load_json_safe(path: Path, fallback: Any = None) -> Any:
    """Load JSON file safely. Return fallback on missing or corrupt."""
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError, OSError) as e:
        logger.warning(f"Failed to load {path}: {e}")
        return fallback


def _is_conflict_file(name: str) -> bool:
    """Check if a filename indicates a provider conflict or temp file."""
    lower = name.lower()
    return (
        ".sync-conflict" in lower
        or lower.endswith(".tmp")
        or lower.endswith(".partial")
        or name.startswith(".")
    )


def _normalize_url(url: str) -> str:
    """
    Normalize a URL for use as a dictionary key.
    1. Lowercase scheme and host.
    2. Remove default ports (:80 for HTTP, :443 for HTTPS).
    3. Decode percent-encoding in path.
    4. Remove trailing slash from path (if path length > 1).
    5. Preserve query string and fragment.
    """
    if not url:
        return ""
    p = urlparse(url.strip())
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    # Remove default ports
    if ":" in netloc:
        host, port = netloc.rsplit(":", 1)
        if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
            netloc = host
    path = unquote(p.path)
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    return urlunparse((scheme, netloc, path, "", p.query, p.fragment))


def _generate_episode_id(guid: Optional[str], url: str) -> str:
    """
    Generate a stable episode ID.
    Priority: RSS guid > normalized URL hash.
    """
    if guid and guid.strip().lower() not in ("", "none"):
        return f"guid:{guid.strip()}"
    norm = _normalize_url(url)
    return f"url:{hashlib.sha256(norm.encode()).hexdigest()[:16]}"


# ─────────────────────────────────────────────────────────────
# QUEUE OPERATION HELPERS
# ─────────────────────────────────────────────────────────────

def _apply_queue_op(items: List[Dict], op: Dict) -> List[Dict]:
    """Apply a single queue operation to an item list."""
    op_type = op.get("op")

    if op_type == "add":
        new_items = list(op.get("items", []))
        after_id = op.get("after_id")
        if after_id is None:
            return items + new_items
        try:
            idx = next(i for i, it in enumerate(items) if it.get("ep_id") == after_id)
            return items[:idx + 1] + new_items + items[idx + 1:]
        except StopIteration:
            return items + new_items

    elif op_type == "remove":
        remove_ids = set(op.get("ids", []))
        return [it for it in items if it.get("ep_id") not in remove_ids]

    elif op_type == "reorder":
        order = op.get("ids", [])
        order_map = {ep_id: idx for idx, ep_id in enumerate(order)}
        mentioned = [it for it in items if it.get("ep_id") in order_map]
        not_mentioned = [it for it in items if it.get("ep_id") not in order_map]
        mentioned.sort(key=lambda it: order_map.get(it.get("ep_id"), float("inf")))
        return mentioned + not_mentioned

    elif op_type == "clear":
        return []

    else:
        logger.warning(f"Unknown queue op: {op_type}")
        return items


def _rebuild_queue(snapshot: Optional[Dict], ops_dir: Path) -> List[Dict]:
    """Rebuild queue from snapshot + all operation logs."""
    items = list(snapshot.get("items", [])) if snapshot else []
    ops = []

    if ops_dir.exists():
        for f in ops_dir.iterdir():
            if not f.is_file() or f.suffix != ".jsonl" or _is_conflict_file(f.name):
                continue
            try:
                for line in f.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        ops.append(json.loads(line))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to parse queue ops {f}: {e}")

    ops.sort(key=lambda o: o.get("ts", 0))
    for op in ops:
        items = _apply_queue_op(items, op)

    return items


# ─────────────────────────────────────────────────────────────
# MAIN CLIENT
# ─────────────────────────────────────────────────────────────

class FilePodSync:
    """
    Standalone FilePodSync client v1.2.
    Handles local state, LWW-EL merging, atomic writes, op-based queue,
    three-state sync, and provider-agnostic file I/O.
    """

    def __init__(self, sync_dir: str, device_name: str = "FilePodSync Client"):
        self.sync_dir = Path(sync_dir)
        self.sync_dir.mkdir(parents=True, exist_ok=True)

        self.device_id = self._load_device_id()
        self.device_name = device_name

        self._lock = threading.RLock()
        self._queue_debounce_timer: Optional[threading.Timer] = None
        self._pending_queue_ops: List[Dict] = []

        # Three-state architecture
        self._synced_state: Dict[str, Any] = {}
        self._pending_local_ops: List[Dict] = []

        self._init_structure()
        self._synced_state = self._load_state()

        logger.info(f"FilePodSync v{SCHEMA_VERSION} initialized. Device: {self.device_id} | Dir: {self.sync_dir}")

    def _load_device_id(self) -> str:
        """Load or generate persistent device UUID."""
        id_file = self.sync_dir / ".fps_device_id"
        if id_file.exists():
            return id_file.read_text(encoding="utf-8").strip()
        new_id = str(uuid.uuid4())
        _atomic_write(id_file, new_id)
        return new_id

    def _init_structure(self) -> None:
        """Create initial folder structure and empty state files."""
        for sub in ["logs", "snapshots", "queue_ops"]:
            (self.sync_dir / sub).mkdir(exist_ok=True)

        for fname in ["config", "devices", "feeds", "episodes", "queue"]:
            path = self.sync_dir / f"{fname}.json"
            if not path.exists():
                _atomic_write(path, {
                    "schema_version": SCHEMA_VERSION,
                    "updated_at": 0,
                    "updated_by": "",
                    "feeds" if fname == "feeds" else 
                    "episodes" if fname == "episodes" else 
                    "devices" if fname == "devices" else 
                    "items": {} if fname in ("feeds", "episodes", "devices") else []
                })

    def _load_state(self) -> Dict[str, Any]:
        """Load all state files from disk into memory."""
        state = {}
        for fname in ["config", "devices", "feeds", "episodes", "queue"]:
            path = self.sync_dir / f"{fname}.json"
            data = _load_json_safe(path, {})
            state[fname] = data
        return state

    def _validate_schema(self, data: Dict, path: Path) -> bool:
        """Validate schema version. Reject incompatible major versions."""
        version = data.get("schema_version", "")
        if not version:
            logger.warning(f"Missing schema_version in {path}")
            return False
        if not version.startswith(SCHEMA_COMPATIBLE_MAJOR):
            logger.error(
                f"Schema version mismatch in {path}: "
                f"found {version}, compatible with {SCHEMA_COMPATIBLE_MAJOR}x"
            )
            return False
        return True

    def _wrap_meta(self, data: Dict, key: str) -> Dict:
        """Wrap record data with metadata for atomic writes."""
        return {
            "schema_version": SCHEMA_VERSION,
            "updated_at": get_utc_ms(),
            "updated_by": self.device_id,
            key: data
        }

    def _merge_records(self, local: Dict, remote: Dict) -> Dict:
        """LWW-EL merge for feeds/episodes/devices maps."""
        merged = dict(local)
        for key, rrec in remote.items():
            lrec = local.get(key)
            if not lrec:
                merged[key] = rrec
                continue
            r_ts = rrec.get("updated_at", 0)
            l_ts = lrec.get("updated_at", 0)
            if r_ts > l_ts:
                merged[key] = rrec
            elif r_ts == l_ts:
                if rrec.get("updated_by", "") > lrec.get("updated_by", ""):
                    merged[key] = rrec
        return merged

    def _detect_clock_skew(self, remote_state: Dict) -> None:
        """Warn if remote timestamps are suspiciously far from local time."""
        local_time = get_utc_ms()
        max_remote = 0
        for fname in ["feeds", "episodes", "devices", "queue"]:
            ts = remote_state.get(fname, {}).get("updated_at", 0)
            if ts > max_remote:
                max_remote = ts
        if abs(local_time - max_remote) > SKEW_WARNING_MS:
            logger.warning(
                f"Clock skew detected: local={local_time}, remote_max={max_remote}, "
                f"diff={abs(local_time - max_remote)}ms"
            )

    # ─────────────────────────────────────────────────────────
    # PUBLIC API — FEEDS
    # ─────────────────────────────────────────────────────────

    def add_feed(self, url: str, title: str = "", custom: Dict = None) -> None:
        """Add a subscription. URL is normalized before storage."""
        with self._lock:
            norm_url = _normalize_url(url)
            if not norm_url:
                logger.warning("Attempted to add feed with empty URL")
                return

            feeds = self._synced_state.get("feeds", {}).get("feeds", {})
            now = get_utc_ms()

            feeds[norm_url] = {
                "url": norm_url,
                "title": title,
                "status": "active",
                "health_status": "healthy",
                "last_check": 0,
                "error_count": 0,
                "added_by": self.device_id,
                "added_at": now,
                "updated_by": self.device_id,
                "updated_at": now,
                "custom": custom or {}
            }

            self._save_state_file("feeds", feeds)
            self._log_sync("feed_add", {"url": norm_url})

    def remove_feed(self, url: str, mark_archived: bool = True) -> None:
        """Remove or archive a subscription."""
        with self._lock:
            norm_url = _normalize_url(url)
            feeds = self._synced_state.get("feeds", {}).get("feeds", {})

            if norm_url in feeds:
                feeds[norm_url]["status"] = "archived" if mark_archived else "deleted"
                feeds[norm_url]["updated_by"] = self.device_id
                feeds[norm_url]["updated_at"] = get_utc_ms()
                self._save_state_file("feeds", feeds)
                self._log_sync("feed_remove", {"url": norm_url, "archived": mark_archived})

    def update_feed_health(self, url: str, health_status: str, error_count: int = 0) -> None:
        """Update feed health status (for dead feed tracking)."""
        with self._lock:
            norm_url = _normalize_url(url)
            feeds = self._synced_state.get("feeds", {}).get("feeds", {})

            if norm_url in feeds:
                feeds[norm_url]["health_status"] = health_status
                feeds[norm_url]["error_count"] = error_count
                feeds[norm_url]["last_check"] = get_utc_ms()
                feeds[norm_url]["updated_by"] = self.device_id
                feeds[norm_url]["updated_at"] = get_utc_ms()
                self._save_state_file("feeds", feeds)

    # ─────────────────────────────────────────────────────────
    # PUBLIC API — EPISODES
    # ─────────────────────────────────────────────────────────

    def update_episode(
        self,
        episode_url: str,
        feed_url: str,
        guid: Optional[str] = None,
        title: str = "",
        position: int = 0,
        total: int = 0,
        state: str = "unplayed",
        custom: Dict = None
    ) -> None:
        """
        Update episode state. Position is exact (no clamping).
        guid is used for stable ID generation when available.
        """
        with self._lock:
            ep_id = _generate_episode_id(guid, episode_url)
            eps = self._synced_state.get("episodes", {}).get("episodes", {})
            now = get_utc_ms()

            if ep_id not in eps:
                eps[ep_id] = {
                    "feed_url": _normalize_url(feed_url),
                    "guid": guid.strip() if guid else None,
                    "url": episode_url,
                    "title": title,
                    "state": state,
                    "progress_seconds": position,
                    "duration_seconds": total,
                    "updated_by": self.device_id,
                    "updated_at": now,
                    "custom": custom or {}
                }
            else:
                rec = eps[ep_id]
                rec["progress_seconds"] = position  # Exact value, no max() clamping
                rec["duration_seconds"] = total if total > 0 else rec.get("duration_seconds", 0)
                rec["state"] = state if state in ("unplayed", "in_progress", "completed", "skipped") else rec.get("state", "unplayed")
                rec["updated_by"] = self.device_id
                rec["updated_at"] = now
                if title:
                    rec["title"] = title

            self._save_state_file("episodes", eps)
            self._log_sync("ep_update", {"ep_id": ep_id, "state": state, "position": position})

    # ─────────────────────────────────────────────────────────
    # PUBLIC API — QUEUE (Op-Based)
    # ─────────────────────────────────────────────────────────

    def queue_add(self, ep_id: str, after_id: Optional[str] = None) -> None:
        """Add an episode to the queue (debounced)."""
        self._pending_queue_ops.append({
            "ts": get_utc_ms(),
            "op": "add",
            "items": [{"ep_id": ep_id, "added_at": get_utc_ms()}],
            "after_id": after_id
        })
        self._debounce_queue_flush()

    def queue_remove(self, ep_ids: List[str]) -> None:
        """Remove episodes from the queue (debounced)."""
        self._pending_queue_ops.append({
            "ts": get_utc_ms(),
            "op": "remove",
            "ids": ep_ids
        })
        self._debounce_queue_flush()

    def queue_reorder(self, ep_ids: List[str]) -> None:
        """Reorder the queue (debounced)."""
        self._pending_queue_ops.append({
            "ts": get_utc_ms(),
            "op": "reorder",
            "ids": ep_ids
        })
        self._debounce_queue_flush()

    def queue_clear(self) -> None:
        """Clear the entire queue (debounced)."""
        self._pending_queue_ops.append({
            "ts": get_utc_ms(),
            "op": "clear"
        })
        self._debounce_queue_flush()

    def _debounce_queue_flush(self) -> None:
        """Debounce queue writes to avoid rapid successive disk hits."""
        if self._queue_debounce_timer:
            self._queue_debounce_timer.cancel()
        self._queue_debounce_timer = threading.Timer(QUEUE_DEBOUNCE_S, self._flush_queue_ops)
        self._queue_debounce_timer.daemon = True
        self._queue_debounce_timer.start()

    def _flush_queue_ops(self) -> None:
        """Write pending queue ops to the device's op log."""
        with self._lock:
            if not self._pending_queue_ops:
                return

            ops_file = self.sync_dir / "queue_ops" / f"{self.device_id}.jsonl"
            lines = [json.dumps(op, ensure_ascii=False) for op in self._pending_queue_ops]
            with ops_file.open("a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")

            count = len(self._pending_queue_ops)
            self._pending_queue_ops.clear()
            self._log_sync("queue_flush", {"count": count})

            # Check if we should consolidate
            self._maybe_consolidate_queue()

    def _maybe_consolidate_queue(self) -> None:
        """Consolidate queue ops into queue.json when threshold exceeded."""
        total_ops = 0
        ops_dir = self.sync_dir / "queue_ops"
        if ops_dir.exists():
            for f in ops_dir.iterdir():
                if f.is_file() and f.suffix == ".jsonl" and not _is_conflict_file(f.name):
                    try:
                        total_ops += sum(1 for _ in f.open("r", encoding="utf-8"))
                    except OSError:
                        pass

        if total_ops >= QUEUE_OPS_CONSOLIDATE_AT:
            logger.info(f"Consolidating queue ops ({total_ops} total)")
            self._consolidate_queue()

    def _consolidate_queue(self) -> None:
        """Rebuild queue from ops, write snapshot, truncate op logs."""
        with self._lock:
            snapshot = self._synced_state.get("queue", {})
            items = _rebuild_queue(snapshot, self.sync_dir / "queue_ops")
            self._save_state_file("queue", items)

            # Truncate all op logs
            ops_dir = self.sync_dir / "queue_ops"
            if ops_dir.exists():
                for f in ops_dir.iterdir():
                    if f.is_file() and f.suffix == ".jsonl" and not _is_conflict_file(f.name):
                        f.write_text("")

    # ─────────────────────────────────────────────────────────
    # SYNC CYCLE
    # ─────────────────────────────────────────────────────────

    def sync(self) -> Dict[str, Any]:
        """
        Full three-state sync cycle.
        1. Read remote state from disk.
        2. Merge LWW-EL for feeds/episodes/devices.
        3. Rebuild queue from ops.
        4. Apply pending local ops on top.
        5. Atomic write merged state.
        6. Update internal synced state.
        """
        with self._lock:
            # 1. Read remote state
            remote = {}
            for fname in ["config", "devices", "feeds", "episodes", "queue"]:
                path = self.sync_dir / f"{fname}.json"
                if _is_conflict_file(path.name):
                    continue
                data = _load_json_safe(path, {})
                if data and not self._validate_schema(data, path):
                    # Try to restore from snapshot
                    data = self._restore_from_snapshot(fname)
                remote[fname] = data

            # 2. Detect clock skew
            self._detect_clock_skew(remote)

            # 3. Three-state merge
            # synced_state = what we last successfully wrote
            # remote = what's currently on disk (may include changes from other devices)
            merged_feeds = self._merge_records(
                self._synced_state.get("feeds", {}).get("feeds", {}),
                remote.get("feeds", {}).get("feeds", {})
            )
            merged_eps = self._merge_records(
                self._synced_state.get("episodes", {}).get("episodes", {}),
                remote.get("episodes", {}).get("episodes", {})
            )
            merged_devs = self._merge_records(
                self._synced_state.get("devices", {}).get("devices", {}),
                remote.get("devices", {}).get("devices", {})
            )

            # 4. Rebuild queue from ops
            merged_queue = _rebuild_queue(
                remote.get("queue", {}),
                self.sync_dir / "queue_ops"
            )

            # 5. Apply any pending local ops that haven't been flushed yet
            # (These would be in-memory pending ops for feeds/episodes)
            for op in self._pending_local_ops:
                if op["type"] == "feed" and op["url"] in merged_feeds:
                    if op["ts"] >= merged_feeds[op["url"]].get("updated_at", 0):
                        merged_feeds[op["url"]].update(op["data"])
                        merged_feeds[op["url"]]["updated_by"] = self.device_id
                        merged_feeds[op["url"]]["updated_at"] = op["ts"]
                elif op["type"] == "episode" and op["ep_id"] in merged_eps:
                    if op["ts"] >= merged_eps[op["ep_id"]].get("updated_at", 0):
                        merged_eps[op["ep_id"]].update(op["data"])
                        merged_eps[op["ep_id"]]["updated_by"] = self.device_id
                        merged_eps[op["ep_id"]]["updated_at"] = op["ts"]
            self._pending_local_ops.clear()

            # 6. Register/update this device
            merged_devs[self.device_id] = {
                "name": self.device_name,
                "platform": "python",
                "client": "filepodsync",
                "first_seen": merged_devs.get(self.device_id, {}).get("first_seen", get_utc_ms()),
                "last_seen": get_utc_ms()
            }

            # 7. Atomic write all merged state
            self._save_state_file("feeds", merged_feeds)
            self._save_state_file("episodes", merged_eps)
            self._save_state_file("devices", merged_devs)
            self._save_state_file("queue", merged_queue)

            # 8. Update internal synced state
            self._synced_state["feeds"] = self._wrap_meta(merged_feeds, "feeds")
            self._synced_state["episodes"] = self._wrap_meta(merged_eps, "episodes")
            self._synced_state["devices"] = self._wrap_meta(merged_devs, "devices")
            self._synced_state["queue"] = self._wrap_meta(merged_queue, "items")

            # 9. Housekeeping
            self._housekeeping()
            self._log_sync("sync_complete", {
                "feeds": len(merged_feeds),
                "episodes": len(merged_eps),
                "queue": len(merged_queue)
            })

            return {
                "feeds": len(merged_feeds),
                "episodes": len(merged_eps),
                "queue": len(merged_queue),
                "timestamp": get_utc_ms()
            }

    def _save_state_file(self, fname: str, data: Any) -> None:
        """Save a single state file with proper metadata wrapping."""
        key = "items" if fname == "queue" else fname
        wrapped = self._wrap_meta(data, key)
        _atomic_write(self.sync_dir / f"{fname}.json", wrapped)

    def _restore_from_snapshot(self, fname: str) -> Dict:
        """Attempt to restore a corrupted file from the latest snapshot."""
        snap_dir = self.sync_dir / "snapshots"
        if not snap_dir.exists():
            return {}

        snaps = sorted(
            snap_dir.glob("snapshot-*.json.gz"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        for snap in snaps:
            try:
                with gzip.open(snap, "rt", encoding="utf-8") as f:
                    data = json.load(f)
                recovered = data.get(fname, {})
                logger.info(f"Restored {fname} from snapshot {snap}")
                return recovered
            except (gzip.BadGzipFile, json.JSONDecodeError, OSError):
                continue

        logger.warning(f"Could not restore {fname} from any snapshot")
        return {}

    # ─────────────────────────────────────────────────────────
    # BOOTSTRAP
    # ─────────────────────────────────────────────────────────

    def bootstrap_from_local(
        self,
        feeds: List[Dict],
        episodes: List[Dict],
        queue: List[str]
    ) -> None:
        """
        Upload existing local state to the sync folder.
        Use when a new device already has podcast data.
        All records get fresh timestamps so they win LWW merge.
        """
        with self._lock:
            now = get_utc_ms()

            feed_map = {}
            for f in feeds:
                norm = _normalize_url(f.get("url", ""))
                if norm:
                    feed_map[norm] = {
                        "url": norm,
                        "title": f.get("title", ""),
                        "status": "active",
                        "health_status": "healthy",
                        "last_check": 0,
                        "error_count": 0,
                        "added_by": self.device_id,
                        "added_at": now,
                        "updated_by": self.device_id,
                        "updated_at": now,
                        "custom": f.get("custom", {})
                    }

            ep_map = {}
            for e in episodes:
                ep_id = _generate_episode_id(e.get("guid"), e.get("url", ""))
                ep_map[ep_id] = {
                    "feed_url": _normalize_url(e.get("feed_url", "")),
                    "guid": e.get("guid"),
                    "url": e.get("url", ""),
                    "title": e.get("title", ""),
                    "state": e.get("state", "unplayed"),
                    "progress_seconds": e.get("progress_seconds", 0),
                    "duration_seconds": e.get("duration_seconds", 0),
                    "updated_by": self.device_id,
                    "updated_at": now,
                    "custom": e.get("custom", {})
                }

            queue_items = [{"ep_id": ep_id, "added_at": now} for ep_id in queue]

            # Write as pending ops so they get merged properly in next sync
            self._pending_local_ops = []
            for url, rec in feed_map.items():
                self._pending_local_ops.append({
                    "type": "feed", "url": url, "ts": now, "data": rec
                })
            for ep_id, rec in ep_map.items():
                self._pending_local_ops.append({
                    "type": "episode", "ep_id": ep_id, "ts": now, "data": rec
                })

            # Queue ops go directly to op log
            if queue_items:
                ops_file = self.sync_dir / "queue_ops" / f"{self.device_id}.jsonl"
                op = {
                    "ts": now,
                    "op": "add",
                    "items": queue_items,
                    "after_id": None
                }
                with ops_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(op, ensure_ascii=False) + "\n")

            logger.info(f"Bootstrap prepared: {len(feed_map)} feeds, {len(ep_map)} episodes, {len(queue)} queue items")

    # ─────────────────────────────────────────────────────────
    # EXPORT & UTILITIES
    # ─────────────────────────────────────────────────────────

    def export_opml(self) -> str:
        """Generate standard OPML 2.0 from synced feeds."""
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<opml version="2.0">',
            '  <head><title>FilePodSync Subscriptions</title></head>',
            '  <body>'
        ]
        feeds = self._synced_state.get("feeds", {}).get("feeds", {})
        for fid, feed in feeds.items():
            if feed.get("status") == "deleted":
                continue
            title = feed.get("title", fid).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            url = feed.get("url", fid)
            lines.append(f'    <outline type="rss" text="{title}" xmlUrl="{url}" />')
        lines.extend(['  </body>', '</opml>'])
        return "\n".join(lines)

    def get_state(self) -> Dict[str, Any]:
        """Return current in-memory state (read-only reference)."""
        with self._lock:
            return dict(self._synced_state)

    def get_feeds(self) -> Dict[str, Dict]:
        """Return active feeds map."""
        with self._lock:
            return {
                k: v for k, v in self._synced_state.get("feeds", {}).get("feeds", {}).items()
                if v.get("status") != "deleted"
            }

    def get_episodes(self) -> Dict[str, Dict]:
        """Return all episode records."""
        with self._lock:
            return dict(self._synced_state.get("episodes", {}).get("episodes", {}))

    def get_queue(self) -> List[Dict]:
        """Return current queue items (rebuilt from ops)."""
        with self._lock:
            snapshot = self._synced_state.get("queue", {})
            return _rebuild_queue(snapshot, self.sync_dir / "queue_ops")

    # ─────────────────────────────────────────────────────────
    # HOUSEKEEPING
    # ─────────────────────────────────────────────────────────

    def _housekeeping(self) -> None:
        """Rotate logs, prune snapshots, clean old files."""
        self._rotate_logs()
        self._prune_snapshots()

    def _rotate_logs(self) -> None:
        """Rotate logs by size and age."""
        log_dir = self.sync_dir / "logs"
        if not log_dir.exists():
            return

        cutoff = get_utc_ms() - (LOG_MAX_DAYS * 86400 * 1000)

        for f in log_dir.glob("*.jsonl"):
            try:
                stat = f.stat()
                # Remove by age
                if stat.st_mtime * 1000 < cutoff:
                    f.unlink()
                    continue
                # Rotate by size
                if stat.st_size > LOG_MAX_MB * 1024 * 1024:
                    # Compress old log
                    gz_path = f.with_suffix(".jsonl.gz")
                    with f.open("rb") as src, gzip.open(gz_path, "wb") as dst:
                        dst.write(src.read())
                    f.unlink()
            except OSError:
                pass

    def _prune_snapshots(self) -> None:
        """Keep only the configured number of snapshots."""
        snap_dir = self.sync_dir / "snapshots"
        if not snap_dir.exists():
            return

        snaps = sorted(
            snap_dir.glob("snapshot-*.json.gz"),
            key=lambda p: p.stat().st_mtime
        )
        while len(snaps) > SNAPSHOT_RETENTION:
            try:
                snaps.pop(0).unlink()
            except OSError:
                break

    def create_snapshot(self) -> Path:
        """Create a compressed full-state snapshot."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.sync_dir / "snapshots" / f"snapshot-{ts}.json.gz"
        with self._lock:
            with gzip.open(path, "wt", encoding="utf-8") as f:
                json.dump(self._synced_state, f, default=str)
        self._prune_snapshots()
        return path

    def _log_sync(self, event: str, data: Dict = None) -> None:
        """Append to daily sync log."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        log_file = self.sync_dir / "logs" / f"sync-{today}.jsonl"
        entry = {
            "ts": get_utc_ms(),
            "device": self.device_id,
            "event": event,
            **(data or {})
        }
        with log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, separators=(",", ":"), ensure_ascii=False) + "\n")

    # ─────────────────────────────────────────────────────────
    # SHUTDOWN
    # ─────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Flush pending ops and create snapshot before exit."""
        with self._lock:
            # Flush any pending queue ops immediately
            if self._queue_debounce_timer:
                self._queue_debounce_timer.cancel()
            self._flush_queue_ops()

            # Create snapshot
            self.create_snapshot()

            self._log_sync("shutdown", {})
            logger.info("FilePodSync shutdown complete")

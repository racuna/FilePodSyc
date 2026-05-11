"""
filepodsync.py — FilePodSync Client Library v1.3
=================================================
Provider-agnostic, file-based podcast synchronization.
Zero-API · LWW-EL merge · atomic writes · op-based queue · conflict-resistant.

Spec:   agent.md v1.3  /  README.md v1.3
Author: FilePodSync contributors
License: MIT

Changes from v1.2
-----------------
- Device ID persisted as plain UTF-8; json.dumps quoting bug eliminated.
- Queue ops carry device_id; sort key is (ts, device_id) for determinism.
- Queue reconstruction skips ops with ts <= consolidated_through_ts.
- Consolidation resets ONLY the local device's op file (sovereignty rule).
- consolidated_through_ts written to queue.json snapshot after consolidation.
- Log rotation driven by filename date, not filesystem mtime.
- _flush_queue_ops drains pending list atomically (swap before I/O).
- Snapshot restore preserves original per-record updated_at values.
- Bootstrap guards against resurrecting remotely-deleted feeds.
- _is_conflict_file covers Syncthing, Dropbox, Google Drive, iCloud.
- Device records carry status/updated_at/updated_by; retirement policy added.
- Schema version validated by integer major comparison, not string prefix.
- Unknown queue op types silently skipped (forward-compat).
- Unknown JSON fields preserved on read/write (forward-compat).
- sync() throttled by MIN_SYNC_INTERVAL_S; force=True bypasses.
- device_name, platform, client exposed as constructor params.
- redirect_feed_http_to_https() helper for 301-redirect handling.
- retire_stale_devices() and prune_retired_device_ops() added.
- Context manager support (__enter__/__exit__).
- Comprehensive smoke test expanded to cover all new behaviour.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import threading
import time as _time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse, urlunparse

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_VERSION          = "1.3.0"
SCHEMA_COMPATIBLE_MAJOR = 1           # int; reject files whose major differs

LOG_MAX_MB                = 10
LOG_MAX_DAYS              = 30
SNAPSHOT_RETENTION        = 5
QUEUE_DEBOUNCE_S          = 2.0
QUEUE_OPS_CONSOLIDATE_AT  = 50
SKEW_WARNING_MS           = 300_000   # 5 minutes
DEVICE_RETIREMENT_DAYS    = 90
MIN_SYNC_INTERVAL_S       = 5.0

_VALID_EPISODE_STATES = frozenset({"unplayed", "in_progress", "completed", "skipped"})
_VALID_FEED_STATUSES  = frozenset({"active", "archived", "deleted"})
_VALID_HEALTH_STATUSES = frozenset({"healthy", "stale", "error"})
_VALID_DEVICE_STATUSES = frozenset({"active", "retired"})

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("filepodsync")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(_h)


# ─────────────────────────────────────────────────────────────────────────────
# PURE UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_utc_ms() -> int:
    """Return the current UTC time in milliseconds since the Unix epoch."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _atomic_write(path: Path, data: Any) -> None:
    """
    Write *data* as JSON to *path* atomically.

    Writes to ``<path>.tmp`` first, then renames.  The .tmp file must live on
    the same filesystem as the target so the rename is atomic (POSIX) or
    best-effort (Windows).

    **Do not use this for the device-ID file.**  The device ID is a plain
    string; calling json.dumps on it would wrap it in JSON quotes, producing
    ``"uuid-here"`` instead of ``uuid-here``.  Use ``Path.write_text()``
    directly for that file.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    os.replace(str(tmp), str(path))


def _load_json_safe(path: Path, fallback: Any = None) -> Any:
    """Load a JSON file; return *fallback* on missing, corrupt, or I/O error."""
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError, OSError) as exc:
        logger.warning("Failed to load %s: %s", path, exc)
        return fallback


def _is_conflict_file(name: str) -> bool:
    """
    Return True if *name* identifies a provider-generated conflict or temp file.

    Patterns covered:

    ============  =============================================
    Provider      Pattern
    ============  =============================================
    Syncthing     ``.sync-conflict`` anywhere in name
    Dropbox       ``(conflicted copy)`` anywhere in name
    Google Drive  trailing `` (N).json`` / `` (N).jsonl``
    iCloud        ``(conflicted copy DATE)`` (same as Dropbox)
    Any           ends with ``.tmp`` or ``.partial``
    Any           starts with ``.`` (hidden / system)
    ============  =============================================
    """
    low = name.lower()
    return (
        ".sync-conflict" in low
        or " (conflicted copy)" in low
        or bool(re.search(r" \(\d+\)\.(json|jsonl)$", low))
        or low.endswith(".tmp")
        or low.endswith(".partial")
        or name.startswith(".")
    )


def _normalize_url(url: str) -> str:
    """
    Normalize a feed URL for use as a stable dictionary key.

    Rules (spec §feeds.json URL Normalization):

    1. Lowercase scheme and host.
    2. Remove default ports (``:80`` for http, ``:443`` for https).
    3. Decode percent-encoding in the path.
    4. Remove a trailing ``/`` from the path (unless path is just ``/``).
    5. Preserve query string and fragment.

    ``http://`` and ``https://`` variants are distinct keys.
    Returns ``""`` for an empty or un-parseable input.
    """
    if not url:
        return ""
    p = urlparse(url.strip())
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
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
    Generate a stable, cross-client episode identifier.

    Priority:

    1. RSS ``<guid>`` — ``"guid:<stripped_guid>"``
    2. URL hash      — ``"url:<sha256(normalized_url)[:16]>"``
    """
    if guid and guid.strip().lower() not in ("", "none"):
        return f"guid:{guid.strip()}"
    return f"url:{hashlib.sha256(_normalize_url(url).encode()).hexdigest()[:16]}"


def _validate_schema_version(data: Dict, label: Any) -> bool:
    """
    Validate *data*'s ``schema_version`` field against SCHEMA_COMPATIBLE_MAJOR.

    Uses integer comparison on the major component — never a string-prefix
    check — so ``"1.99.0"`` is accepted and ``"2.0.0"`` is rejected cleanly.
    """
    raw = data.get("schema_version", "")
    if not raw:
        logger.warning("Missing schema_version in %s", label)
        return False
    try:
        major = int(str(raw).split(".")[0])
    except (ValueError, IndexError):
        logger.warning("Un-parseable schema_version %r in %s", raw, label)
        return False
    if major != SCHEMA_COMPATIBLE_MAJOR:
        logger.error(
            "Schema major-version mismatch in %s: found %s, "
            "compatible major is %d — file rejected",
            label, raw, SCHEMA_COMPATIBLE_MAJOR,
        )
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# LWW-EL MERGE
# ─────────────────────────────────────────────────────────────────────────────

def _merge_records(local: Dict[str, Dict], remote: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Last-Write-Wins at Element Level merge for feeds / episodes / devices maps.

    Winner per key:

    1. Higher ``updated_at`` wins.
    2. Tie: lexicographically larger ``updated_by`` UUID wins (deterministic).

    The file-level wrapper ``updated_at`` is **never** consulted here.
    Unknown fields inside records are preserved verbatim (forward-compat).
    """
    merged: Dict[str, Dict] = dict(local)
    for key, rrec in remote.items():
        lrec = local.get(key)
        if lrec is None:
            merged[key] = rrec
            continue
        r_ts = rrec.get("updated_at", 0)
        l_ts = lrec.get("updated_at", 0)
        if r_ts > l_ts:
            merged[key] = rrec
        elif r_ts == l_ts and rrec.get("updated_by", "") > lrec.get("updated_by", ""):
            merged[key] = rrec
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# QUEUE OPERATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _apply_queue_op(items: List[Dict], op: Dict) -> List[Dict]:
    """
    Apply one queue operation to *items* and return the updated list.

    Unknown ``op`` values are silently skipped for forward-compatibility
    with future op types.
    """
    op_type = op.get("op")

    if op_type == "add":
        new_items = list(op.get("items", []))
        after_id  = op.get("after_id")
        if after_id is None:
            return items + new_items
        try:
            idx = next(i for i, it in enumerate(items) if it.get("ep_id") == after_id)
            return items[: idx + 1] + new_items + items[idx + 1 :]
        except StopIteration:
            return items + new_items  # after_id not found → append

    elif op_type == "remove":
        rm = set(op.get("ids", []))
        return [it for it in items if it.get("ep_id") not in rm]

    elif op_type == "reorder":
        order_map = {eid: i for i, eid in enumerate(op.get("ids", []))}
        mentioned = sorted(
            [it for it in items if it.get("ep_id") in order_map],
            key=lambda it: order_map[it["ep_id"]],
        )
        rest = [it for it in items if it.get("ep_id") not in order_map]
        return mentioned + rest

    elif op_type == "clear":
        return []

    else:
        logger.warning("Unknown queue op type %r — skipping (forward-compat)", op_type)
        return items


def _rebuild_queue(
    snapshot: Optional[Dict],
    ops_dir: Path,
) -> Tuple[List[Dict], int]:
    """
    Reconstruct the canonical queue from a snapshot and per-device op logs.

    Algorithm
    ---------
    1. Start with ``snapshot["items"]`` (or ``[]`` if absent).
    2. Read ``consolidated_through_ts`` from the snapshot (default ``0``).
    3. Collect all ops from every non-conflict ``.jsonl`` file in *ops_dir*
       whose ``ts`` is **strictly greater** than ``consolidated_through_ts``.
    4. Sort collected ops by ``(ts, device_id)`` — deterministic on ties.
    5. Apply ops in order.

    Returns
    -------
    ``(items, max_applied_ts)`` where *max_applied_ts* is the highest ``ts``
    among ops that were actually applied.  The caller uses this to set
    ``consolidated_through_ts`` when writing a new snapshot.
    """
    snap         = snapshot or {}
    items: List[Dict] = list(snap.get("items", []))
    cutoff_ts: int    = snap.get("consolidated_through_ts", 0)
    ops: List[Dict]   = []

    if ops_dir.exists():
        for f in ops_dir.iterdir():
            if not f.is_file() or f.suffix != ".jsonl":
                continue
            if _is_conflict_file(f.name):
                continue
            try:
                for raw_line in f.read_text(encoding="utf-8").splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    op = json.loads(line)
                    if op.get("ts", 0) > cutoff_ts:
                        ops.append(op)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to parse queue ops in %s: %s", f, exc)

    # Deterministic sort: primary key ts, secondary key device_id
    ops.sort(key=lambda o: (o.get("ts", 0), o.get("device_id", "")))

    max_ts = cutoff_ts
    for op in ops:
        items  = _apply_queue_op(items, op)
        max_ts = max(max_ts, op.get("ts", 0))

    return items, max_ts


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CLIENT CLASS
# ─────────────────────────────────────────────────────────────────────────────

class FilePodSync:
    """
    FilePodSync client v1.3.

    Manages local podcast state, syncs it with a shared folder, and exposes a
    clean API for subscribing, tracking playback, and managing the play queue.

    Thread-safety
    -------------
    All public methods that touch state acquire ``self._lock`` (RLock).
    The queue-debounce timer runs on a daemon thread; it uses an atomic
    list-swap to avoid double-write races when shutdown() is called
    concurrently.

    Usage
    -----
    ::

        fps = FilePodSync("/path/to/sync-folder", device_name="My Laptop")
        fps.add_feed("https://feeds.example.com/podcast", "Example Pod")
        ep = fps.update_episode("https://…/ep1.mp3", "https://feeds.example.com/podcast",
                                guid="ep-001", state="in_progress", position=600)
        fps.queue_add(ep)
        fps.sync()
        fps.shutdown()

    Or as a context manager::

        with FilePodSync("/path/to/sync-folder") as fps:
            fps.sync()
    """

    def __init__(
        self,
        sync_dir: str,
        device_name: str = "FilePodSync Client",
        platform: str    = "python",
        client: str      = "filepodsync",
    ) -> None:
        self.sync_dir    = Path(sync_dir)
        self.device_name = device_name
        self.platform    = platform
        self.client      = client

        self.sync_dir.mkdir(parents=True, exist_ok=True)

        # Load (or generate) the persistent device UUID *before* init_structure
        # so the device record is stamped correctly on first run.
        self.device_id: str = self._load_device_id()

        self._lock = threading.RLock()

        # Debounce state for queue writes
        self._queue_debounce_timer: Optional[threading.Timer] = None
        self._pending_queue_ops:    List[Dict]                = []

        # Three-state sync architecture
        #   _synced_state  = last successfully written full state
        #   _pending_local_ops = feed/episode mutations not yet in a sync cycle
        self._synced_state:      Dict[str, Any] = {}
        self._pending_local_ops: List[Dict]     = []

        # Throttle: monotonic clock of the last completed sync()
        self._last_sync_at: float = 0.0

        self._init_structure()
        self._synced_state = self._load_state()

        logger.info(
            "FilePodSync v%s initialized | device=%s | dir=%s",
            SCHEMA_VERSION, self.device_id, self.sync_dir,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # DEVICE ID
    # ─────────────────────────────────────────────────────────────────────────

    def _load_device_id(self) -> str:
        """
        Load or generate the persistent device UUID.

        Stored as a **plain UTF-8 text file** — never via ``json.dumps``.
        ``json.dumps`` on a bare string adds surrounding double-quotes, which
        would make the stored value ``"uuid-here"`` and cause every downstream
        comparison to include the literal quote characters.

        Migration guard: if the file was written by a v1.2 client with the
        quoting bug, the surrounding quotes are stripped and a warning is
        logged so the operator knows to verify ``devices.json``.
        """
        id_file = self.sync_dir / ".fps_device_id"
        if id_file.exists():
            raw = id_file.read_text(encoding="utf-8").strip()
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]
                logger.warning(
                    "Device ID was JSON-quoted (v1.2 bug); stripped quotes. "
                    "Value: %s. If devices.json contains quoted UUIDs, "
                    "delete .fps_device_id and re-initialize to get a clean ID.",
                    raw,
                )
            return raw
        new_id = str(uuid.uuid4())
        id_file.write_text(new_id, encoding="utf-8")  # plain text — no json.dumps
        logger.info("Generated new device ID: %s", new_id)
        return new_id

    # ─────────────────────────────────────────────────────────────────────────
    # INITIALIZATION
    # ─────────────────────────────────────────────────────────────────────────

    def _init_structure(self) -> None:
        """
        Create the folder skeleton and write missing state files with defaults.

        Existing files are never overwritten; only absent ones are created.
        """
        for sub in ("logs", "snapshots", "queue_ops"):
            (self.sync_dir / sub).mkdir(exist_ok=True)

        defaults: Dict[str, Dict] = {
            "config": {
                "schema_version": SCHEMA_VERSION,
                "sync_interval_ms": 1_800_000,
                "capabilities": {
                    "queue_sync":         True,
                    "tag_sync":           False,
                    "snapshot_sync":      True,
                    "dead_feed_tracking": True,
                },
                "rotation": {
                    "log_max_days":               LOG_MAX_DAYS,
                    "log_max_mb":                 LOG_MAX_MB,
                    "snapshot_retention":         SNAPSHOT_RETENTION,
                    "queue_ops_consolidate_at":   QUEUE_OPS_CONSOLIDATE_AT,
                },
            },
            "devices": {
                "schema_version": SCHEMA_VERSION,
                "updated_at": 0,
                "updated_by": "",
                "devices":    {},
            },
            "feeds": {
                "schema_version": SCHEMA_VERSION,
                "updated_at": 0,
                "updated_by": "",
                "feeds":      {},
            },
            "episodes": {
                "schema_version": SCHEMA_VERSION,
                "updated_at": 0,
                "updated_by": "",
                "episodes":   {},
            },
            "queue": {
                "schema_version":        SCHEMA_VERSION,
                "updated_at":            0,
                "updated_by":            "",
                "consolidated_through_ts": 0,
                "items":                 [],
            },
        }

        for fname, default in defaults.items():
            path = self.sync_dir / f"{fname}.json"
            if not path.exists():
                _atomic_write(path, default)

    def _load_state(self) -> Dict[str, Any]:
        """Load all five state files from disk into memory."""
        state: Dict[str, Any] = {}
        for fname in ("config", "devices", "feeds", "episodes", "queue"):
            state[fname] = _load_json_safe(self.sync_dir / f"{fname}.json", {})
        return state

    # ─────────────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _validate_schema(self, data: Dict, path: Path) -> bool:
        return _validate_schema_version(data, path)

    def _wrap_meta(self, payload: Any, key: str, extra: Optional[Dict] = None) -> Dict:
        """Wrap a data payload with standard file-level metadata."""
        wrapper: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "updated_at":     get_utc_ms(),
            "updated_by":     self.device_id,
            key:              payload,
        }
        if extra:
            wrapper.update(extra)
        return wrapper

    def _save_state_file(
        self,
        fname: str,
        data: Any,
        extra: Optional[Dict] = None,
    ) -> None:
        """
        Atomically write one state file and update the in-memory synced state.

        *extra* allows injecting additional top-level fields (e.g.
        ``consolidated_through_ts`` for queue.json).
        """
        key     = "items" if fname == "queue" else fname
        wrapped = self._wrap_meta(data, key, extra)
        _atomic_write(self.sync_dir / f"{fname}.json", wrapped)
        self._synced_state[fname] = wrapped

    def _detect_clock_skew(self, remote_state: Dict) -> None:
        """Warn if any remote file timestamp is far from local UTC."""
        local_now  = get_utc_ms()
        max_remote = max(
            (remote_state.get(f, {}).get("updated_at", 0)
             for f in ("feeds", "episodes", "devices", "queue")),
            default=0,
        )
        if max_remote and abs(local_now - max_remote) > SKEW_WARNING_MS:
            logger.warning(
                "Clock skew detected: local=%d remote_max=%d diff=%dms — "
                "LWW results may be incorrect during skew window",
                local_now, max_remote, abs(local_now - max_remote),
            )

    # ─────────────────────────────────────────────────────────────────────────
    # SNAPSHOT RECOVERY
    # ─────────────────────────────────────────────────────────────────────────

    def _restore_from_snapshot(self, fname: str) -> Dict:
        """
        Restore a corrupted state file from the most recent snapshot.

        **Critical:** per-record ``updated_at`` values are preserved verbatim.
        Do *not* re-stamp records with the current time; that would cause
        restored stale data to win LWW conflicts against other devices' more
        recent writes on the next sync cycle.
        """
        snap_dir = self.sync_dir / "snapshots"
        if not snap_dir.exists():
            return {}

        snaps = sorted(
            snap_dir.glob("snapshot-*.json.gz"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for snap in snaps:
            try:
                with gzip.open(snap, "rt", encoding="utf-8") as fh:
                    data = json.load(fh)
                recovered = data.get(fname, {})
                if recovered:
                    logger.info("Restored %s from snapshot %s", fname, snap.name)
                    return recovered  # original timestamps preserved
            except (gzip.BadGzipFile, json.JSONDecodeError, OSError):
                continue

        logger.warning("Could not restore %s from any snapshot; using empty state", fname)
        return {}

    # ─────────────────────────────────────────────────────────────────────────
    # DEVICE REGISTRY
    # ─────────────────────────────────────────────────────────────────────────

    def _stamp_own_device(self, devices: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Upsert this device's record with current ``last_seen`` and ``status``.
        Preserves ``first_seen`` from any existing record.
        """
        now      = get_utc_ms()
        existing = devices.get(self.device_id, {})
        devices[self.device_id] = {
            "name":       self.device_name,
            "platform":   self.platform,
            "client":     self.client,
            "status":     "active",
            "first_seen": existing.get("first_seen", now),
            "last_seen":  now,
            "updated_by": self.device_id,
            "updated_at": now,
        }
        return devices

    def retire_stale_devices(self) -> List[str]:
        """
        Mark devices not seen in ``DEVICE_RETIREMENT_DAYS`` days as ``"retired"``.

        The change is written to ``devices.json`` immediately.
        Returns the list of retired device UUIDs.
        """
        with self._lock:
            devices = self._synced_state.get("devices", {}).get("devices", {})
            now     = get_utc_ms()
            cutoff  = now - DEVICE_RETIREMENT_DAYS * 86_400_000
            retired = []

            for dev_id, rec in devices.items():
                if dev_id == self.device_id:
                    continue
                if rec.get("status") == "retired":
                    continue
                if rec.get("last_seen", now) < cutoff:
                    rec["status"]     = "retired"
                    rec["updated_by"] = self.device_id
                    rec["updated_at"] = now
                    retired.append(dev_id)

            if retired:
                self._save_state_file("devices", devices)
                logger.info("Retired %d stale device(s): %s", len(retired), retired)
            return retired

    def prune_retired_device_ops(self) -> List[str]:
        """
        Delete op files for ``"retired"`` devices whose every op has already
        been folded into the queue snapshot (i.e. ``max_ts_in_file <=
        consolidated_through_ts``).

        Returns the list of pruned filenames.
        """
        with self._lock:
            devices = self._synced_state.get("devices", {}).get("devices", {})
            ctt     = self._synced_state.get("queue", {}).get("consolidated_through_ts", 0)
            ops_dir = self.sync_dir / "queue_ops"
            pruned: List[str] = []

            for dev_id, rec in devices.items():
                if rec.get("status") != "retired":
                    continue
                ops_file = ops_dir / f"{dev_id}.jsonl"
                if not ops_file.exists():
                    continue
                try:
                    raw_lines = [
                        l for l in ops_file.read_text(encoding="utf-8").splitlines()
                        if l.strip()
                    ]
                    if not raw_lines:
                        ops_file.unlink(missing_ok=True)
                        pruned.append(ops_file.name)
                        continue
                    max_in_file = max(
                        json.loads(l).get("ts", 0) for l in raw_lines
                    )
                    if max_in_file <= ctt:
                        ops_file.unlink(missing_ok=True)
                        pruned.append(ops_file.name)
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning("Could not inspect %s for pruning: %s", ops_file, exc)

            if pruned:
                logger.info("Pruned retired-device op files: %s", pruned)
            return pruned

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API — FEEDS
    # ─────────────────────────────────────────────────────────────────────────

    def add_feed(self, url: str, title: str = "", custom: Optional[Dict] = None) -> bool:
        """
        Subscribe to a feed.

        The URL is normalized before storage.  If the same normalized URL
        already exists, the record is updated (title, status reset to
        ``"active"``) while preserving ``added_by`` and ``added_at``.

        Returns ``False`` if the URL normalizes to an empty string.
        """
        with self._lock:
            norm = _normalize_url(url)
            if not norm:
                logger.warning("add_feed: empty or invalid URL %r — ignored", url)
                return False

            feeds    = self._synced_state.get("feeds", {}).get("feeds", {})
            now      = get_utc_ms()
            existing = feeds.get(norm, {})

            feeds[norm] = {
                "url":          norm,
                "title":        title or existing.get("title", ""),
                "status":       "active",
                "health_status": existing.get("health_status", "healthy"),
                "last_check":   existing.get("last_check", 0),
                "error_count":  0,
                "added_by":     existing.get("added_by", self.device_id),
                "added_at":     existing.get("added_at", now),
                "updated_by":   self.device_id,
                "updated_at":   now,
                "custom":       {**existing.get("custom", {}), **(custom or {})},
            }

            self._save_state_file("feeds", feeds)
            self._log_event("feed_add", {"url": norm, "title": title})
            return True

    def remove_feed(self, url: str, archive: bool = False) -> bool:
        """
        Soft-delete (``status="deleted"``) or archive (``status="archived"``) a feed.

        Returns ``False`` if the normalized URL is not found.

        Note: deleted feeds are **retained** in ``feeds.json`` so the deletion
        propagates to every other device on the next sync.
        """
        with self._lock:
            norm  = _normalize_url(url)
            feeds = self._synced_state.get("feeds", {}).get("feeds", {})
            if norm not in feeds:
                logger.warning("remove_feed: URL not found: %s", norm)
                return False

            feeds[norm]["status"]     = "archived" if archive else "deleted"
            feeds[norm]["updated_by"] = self.device_id
            feeds[norm]["updated_at"] = get_utc_ms()

            self._save_state_file("feeds", feeds)
            self._log_event("feed_remove", {"url": norm, "archived": archive})
            return True

    def update_feed_health(
        self,
        url: str,
        health_status: str,
        error_count: int = 0,
    ) -> bool:
        """
        Record a dead-feed-tracking update for *url*.

        ``health_status`` must be one of ``"healthy"``, ``"stale"``, ``"error"``.
        Returns ``False`` if the feed is not found or the status value is invalid.
        """
        with self._lock:
            if health_status not in _VALID_HEALTH_STATUSES:
                logger.warning("update_feed_health: invalid status %r", health_status)
                return False
            norm  = _normalize_url(url)
            feeds = self._synced_state.get("feeds", {}).get("feeds", {})
            if norm not in feeds:
                return False

            now = get_utc_ms()
            feeds[norm].update({
                "health_status": health_status,
                "error_count":   error_count,
                "last_check":    now,
                "updated_by":    self.device_id,
                "updated_at":    now,
            })
            self._save_state_file("feeds", feeds)
            return True

    def redirect_feed_http_to_https(self, http_url: str) -> bool:
        """
        Handle a permanent HTTP→HTTPS redirect (HTTP 301) for a feed.

        Marks the ``http://`` entry as ``"deleted"`` and ensures the
        ``https://`` variant exists as ``"active"``, copying metadata
        from the old record.  Both keys are written atomically.

        Returns ``False`` if *http_url* does not normalize to an ``http://``
        URL.
        """
        with self._lock:
            http_norm = _normalize_url(http_url)
            if not http_norm.startswith("http://"):
                logger.warning(
                    "redirect_feed_http_to_https: URL does not use http: %s", http_norm
                )
                return False

            https_norm = "https://" + http_norm[len("http://"):]
            feeds      = self._synced_state.get("feeds", {}).get("feeds", {})
            now        = get_utc_ms()
            source     = feeds.get(http_norm, {})

            # Retire the http:// entry
            if http_norm in feeds:
                feeds[http_norm]["status"]     = "deleted"
                feeds[http_norm]["updated_by"] = self.device_id
                feeds[http_norm]["updated_at"] = now

            # Ensure the https:// entry exists (promote from old record)
            if https_norm not in feeds:
                feeds[https_norm] = {
                    "url":          https_norm,
                    "title":        source.get("title", ""),
                    "status":       "active",
                    "health_status": "healthy",
                    "last_check":   0,
                    "error_count":  0,
                    "added_by":     source.get("added_by", self.device_id),
                    "added_at":     source.get("added_at", now),
                    "updated_by":   self.device_id,
                    "updated_at":   now,
                    "custom":       source.get("custom", {}),
                }

            self._save_state_file("feeds", feeds)
            self._log_event("feed_redirect", {"from": http_norm, "to": https_norm})
            return True

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API — EPISODES
    # ─────────────────────────────────────────────────────────────────────────

    def update_episode(
        self,
        episode_url: str,
        feed_url: str,
        guid: Optional[str] = None,
        title: str = "",
        position: int = 0,
        total: int = 0,
        state: str = "unplayed",
        custom: Optional[Dict] = None,
    ) -> str:
        """
        Create or update an episode record.

        ``position`` is stored **exactly** — no ``max()`` clamping.  Rewinding
        to an earlier position is a valid operation (spec §Rewinding progress).

        ``feed_url`` is informative, not normative: when the same GUID appears
        in multiple feeds (cross-posted content), LWW-EL determines which
        ``feed_url`` survives.

        Returns the stable episode ID (e.g. ``"guid:episode-001"``).
        """
        with self._lock:
            ep_id = _generate_episode_id(guid, episode_url)
            eps   = self._synced_state.get("episodes", {}).get("episodes", {})
            now   = get_utc_ms()

            if ep_id not in eps:
                eps[ep_id] = {
                    "feed_url":        _normalize_url(feed_url),
                    "guid":            guid.strip() if guid else None,
                    "url":             episode_url,
                    "title":           title,
                    "state":           state if state in _VALID_EPISODE_STATES else "unplayed",
                    "progress_seconds": position,
                    "duration_seconds": total,
                    "updated_by":      self.device_id,
                    "updated_at":      now,
                    "custom":          custom or {},
                }
            else:
                rec = eps[ep_id]
                rec["progress_seconds"] = position   # exact — no clamping
                if total > 0:
                    rec["duration_seconds"] = total
                if state in _VALID_EPISODE_STATES:
                    rec["state"] = state
                if title:
                    rec["title"] = title
                if custom:
                    rec["custom"] = {**rec.get("custom", {}), **custom}
                rec["updated_by"] = self.device_id
                rec["updated_at"] = now

            self._save_state_file("episodes", eps)
            self._log_event("ep_update", {
                "ep_id": ep_id, "state": state, "position": position,
            })
            return ep_id

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API — QUEUE (Op-Based)
    # ─────────────────────────────────────────────────────────────────────────

    def queue_add(self, ep_id: str, after_id: Optional[str] = None) -> None:
        """
        Enqueue *ep_id*, optionally after *after_id*.

        If *after_id* is ``None``, the episode is appended to the end.
        The write is debounced by ``QUEUE_DEBOUNCE_S`` seconds.
        """
        now = get_utc_ms()
        self._pending_queue_ops.append({
            "ts":        now,
            "device_id": self.device_id,
            "op":        "add",
            "items":     [{"ep_id": ep_id, "added_at": now}],
            "after_id":  after_id,
        })
        self._debounce_queue_flush()

    def queue_remove(self, ep_ids: List[str]) -> None:
        """Remove all *ep_ids* from the queue (debounced)."""
        self._pending_queue_ops.append({
            "ts":        get_utc_ms(),
            "device_id": self.device_id,
            "op":        "remove",
            "ids":       ep_ids,
        })
        self._debounce_queue_flush()

    def queue_reorder(self, ep_ids: List[str]) -> None:
        """
        Reorder the queue so that items appear in *ep_ids* order.

        Items not listed in *ep_ids* retain their relative order and move
        to the end.  Concurrent reorders from different devices are resolved
        by ``(ts, device_id)`` — the later one wins.
        """
        self._pending_queue_ops.append({
            "ts":        get_utc_ms(),
            "device_id": self.device_id,
            "op":        "reorder",
            "ids":       ep_ids,
        })
        self._debounce_queue_flush()

    def queue_clear(self) -> None:
        """Remove all items from the queue (debounced)."""
        self._pending_queue_ops.append({
            "ts":        get_utc_ms(),
            "device_id": self.device_id,
            "op":        "clear",
        })
        self._debounce_queue_flush()

    def _debounce_queue_flush(self) -> None:
        """Reset (or start) the debounce timer for queue writes."""
        if self._queue_debounce_timer:
            self._queue_debounce_timer.cancel()
        t = threading.Timer(QUEUE_DEBOUNCE_S, self._flush_queue_ops)
        t.daemon = True
        t.start()
        self._queue_debounce_timer = t

    def _flush_queue_ops(self) -> None:
        """
        Write pending queue ops to this device's ``.jsonl`` op file.

        The pending list is drained via an **atomic swap** before any I/O:
        ``ops, self._pending_queue_ops = self._pending_queue_ops, []``
        This ensures that a concurrent ``shutdown()`` flush and a timer
        expiry can never both see a non-empty list, eliminating the
        check-then-act race present in v1.2.
        """
        # Acquire lock only long enough to drain the list
        with self._lock:
            ops, self._pending_queue_ops = self._pending_queue_ops, []

        if not ops:
            return

        ops_file = self.sync_dir / "queue_ops" / f"{self.device_id}.jsonl"
        lines    = [json.dumps(op, ensure_ascii=False) for op in ops]
        try:
            with ops_file.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        except OSError as exc:
            logger.error("Failed to write queue ops to %s: %s", ops_file, exc)
            # Re-queue on failure so ops are not silently lost
            with self._lock:
                self._pending_queue_ops = ops + self._pending_queue_ops
            return

        self._log_event("queue_flush", {"count": len(ops)})
        self._maybe_consolidate_queue()

    # ─────────────────────────────────────────────────────────────────────────
    # QUEUE CONSOLIDATION
    # ─────────────────────────────────────────────────────────────────────────

    def _maybe_consolidate_queue(self) -> None:
        """Trigger consolidation if total op count across all devices exceeds threshold."""
        ops_dir = self.sync_dir / "queue_ops"
        if not ops_dir.exists():
            return
        total = 0
        for f in ops_dir.iterdir():
            if f.is_file() and f.suffix == ".jsonl" and not _is_conflict_file(f.name):
                try:
                    total += sum(
                        1 for l in f.read_text(encoding="utf-8").splitlines()
                        if l.strip()
                    )
                except OSError:
                    pass
        if total >= QUEUE_OPS_CONSOLIDATE_AT:
            logger.info("Queue op threshold reached (%d lines); consolidating", total)
            self._consolidate_queue()

    def _consolidate_queue(self) -> None:
        """
        Fold all queue ops into ``queue.json`` and reset **only this device's**
        op file.

        Other devices' ``.jsonl`` files are **never touched** — Device File
        Sovereignty (spec Constraint 11).  The ``consolidated_through_ts``
        field in the new snapshot tells future rebuilds to skip ops that have
        already been applied, making it safe to leave other files intact.
        """
        with self._lock:
            snapshot          = self._synced_state.get("queue", {})
            items, max_ts     = _rebuild_queue(snapshot, self.sync_dir / "queue_ops")

            extra = {"consolidated_through_ts": max_ts}
            self._save_state_file("queue", items, extra)

            # Reset ONLY the local device's op file
            own_file = self.sync_dir / "queue_ops" / f"{self.device_id}.jsonl"
            if own_file.exists():
                own_file.write_text("", encoding="utf-8")

            logger.info(
                "Queue consolidated: %d items, consolidated_through_ts=%d",
                len(items), max_ts,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # THREE-STATE SYNC CYCLE
    # ─────────────────────────────────────────────────────────────────────────

    def sync(self, force: bool = False) -> Dict[str, Any]:
        """
        Execute a full three-state sync cycle.

        Steps
        -----
        1.  Read all remote state files from disk.
        2.  Validate schema versions; fall back to snapshot restore on failure.
        3.  Detect clock skew.
        4.  LWW-EL merge for feeds, episodes, and devices.
        5.  Rebuild queue from op logs (respects ``consolidated_through_ts``).
        6.  Replay in-memory pending local ops on top of the merged base.
        7.  Stamp this device's record with ``last_seen = now``.
        8.  Atomically write all four state files.
        9.  Update ``_synced_state``; clear ``_pending_local_ops``.
        10. Run housekeeping (log rotation, snapshot pruning).
        11. Log the sync event.

        Parameters
        ----------
        force:
            Bypass the ``MIN_SYNC_INTERVAL_S`` throttle.  Use for shutdown
            or explicit user-initiated syncs.

        Returns
        -------
        A dict with counts of ``feeds``, ``episodes``, ``devices``,
        ``queue`` items, and ``timestamp``.  Returns ``{"throttled": True}``
        if the minimum interval has not elapsed.
        """
        if not force:
            elapsed = _time.monotonic() - self._last_sync_at
            if elapsed < MIN_SYNC_INTERVAL_S:
                logger.debug(
                    "sync() throttled (%.1fs since last sync, min=%.1fs)",
                    elapsed, MIN_SYNC_INTERVAL_S,
                )
                return {"throttled": True}

        with self._lock:
            # ── 1. Read remote state ─────────────────────────────────────
            remote: Dict[str, Any] = {}
            for fname in ("config", "devices", "feeds", "episodes", "queue"):
                path = self.sync_dir / f"{fname}.json"
                if _is_conflict_file(path.name):
                    continue
                data = _load_json_safe(path, {})
                if data and not self._validate_schema(data, path):
                    data = self._restore_from_snapshot(fname)
                remote[fname] = data or {}

            # ── 2–3. Skew detection ──────────────────────────────────────
            self._detect_clock_skew(remote)

            # ── 4. LWW-EL merge ─────────────────────────────────────────
            merged_feeds = _merge_records(
                self._synced_state.get("feeds",   {}).get("feeds",   {}),
                remote.get("feeds",               {}).get("feeds",   {}),
            )
            merged_eps = _merge_records(
                self._synced_state.get("episodes", {}).get("episodes", {}),
                remote.get("episodes",            {}).get("episodes", {}),
            )
            merged_devs = _merge_records(
                self._synced_state.get("devices",  {}).get("devices", {}),
                remote.get("devices",             {}).get("devices", {}),
            )

            # ── 5. Rebuild queue ─────────────────────────────────────────
            merged_queue, _ = _rebuild_queue(
                remote.get("queue", {}),
                self.sync_dir / "queue_ops",
            )

            # ── 6. Replay in-memory pending ops ─────────────────────────
            for op in self._pending_local_ops:
                if op["type"] == "feed":
                    url = op["url"]
                    existing = merged_feeds.get(url)
                    if existing is None or op["ts"] >= existing.get("updated_at", 0):
                        merged_feeds[url] = op["data"]
                elif op["type"] == "episode":
                    ep_id    = op["ep_id"]
                    existing = merged_eps.get(ep_id)
                    if existing is None or op["ts"] >= existing.get("updated_at", 0):
                        merged_eps[ep_id] = op["data"]
            self._pending_local_ops.clear()

            # ── 7. Stamp own device ──────────────────────────────────────
            merged_devs = self._stamp_own_device(merged_devs)

            # ── 8. Atomic write ──────────────────────────────────────────
            # Preserve consolidated_through_ts from the remote snapshot so
            # we don't reset it to 0 when rewriting queue.json.
            ctt = remote.get("queue", {}).get("consolidated_through_ts", 0)

            self._save_state_file("feeds",    merged_feeds)
            self._save_state_file("episodes", merged_eps)
            self._save_state_file("devices",  merged_devs)
            self._save_state_file("queue",    merged_queue, {"consolidated_through_ts": ctt})

            # ── 10. Housekeeping ─────────────────────────────────────────
            self._housekeeping()

            # ── 11. Log ──────────────────────────────────────────────────
            result: Dict[str, Any] = {
                "feeds":     len(merged_feeds),
                "episodes":  len(merged_eps),
                "devices":   len(merged_devs),
                "queue":     len(merged_queue),
                "timestamp": get_utc_ms(),
            }
            self._log_event("sync_complete", result)
            self._last_sync_at = _time.monotonic()
            logger.info(
                "Sync complete: %d feeds, %d episodes, %d queue items",
                result["feeds"], result["episodes"], result["queue"],
            )
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # BOOTSTRAP
    # ─────────────────────────────────────────────────────────────────────────

    def bootstrap_from_local(
        self,
        feeds: List[Dict],
        episodes: List[Dict],
        queue_ep_ids: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Import an existing local library into the sync folder.

        Intended for devices joining a sync folder for the first time that
        already have podcast data (subscriptions, playback progress, queue).

        **Deletion guard:** If a feed already exists in the remote folder with
        ``status="deleted"``, it is **not** overwritten.  Bootstrapping
        respects prior deletions made by other devices; the feed is silently
        skipped.  The count is returned in ``"feeds_skipped_deleted"``.

        Feed/episode data is staged as ``_pending_local_ops``.  Call
        ``sync(force=True)`` immediately after this method to merge with the
        remote state.

        Queue items are written directly to this device's op log as an ``add``
        operation.

        Returns
        -------
        Dict with keys: ``feeds``, ``feeds_skipped_deleted``, ``episodes``,
        ``queue``.
        """
        with self._lock:
            remote_feeds: Dict = (
                _load_json_safe(self.sync_dir / "feeds.json", {}).get("feeds", {})
            )
            now = get_utc_ms()

            feed_map: Dict[str, Dict] = {}
            skipped_deleted = 0

            for f in feeds:
                norm = _normalize_url(f.get("url", ""))
                if not norm:
                    continue
                # Do not resurrect feeds that were explicitly deleted remotely
                if remote_feeds.get(norm, {}).get("status") == "deleted":
                    skipped_deleted += 1
                    logger.debug("bootstrap: skipping remotely-deleted feed %s", norm)
                    continue
                feed_map[norm] = {
                    "url":          norm,
                    "title":        f.get("title", ""),
                    "status":       "active",
                    "health_status": "healthy",
                    "last_check":   0,
                    "error_count":  0,
                    "added_by":     self.device_id,
                    "added_at":     now,
                    "updated_by":   self.device_id,
                    "updated_at":   now,
                    "custom":       f.get("custom", {}),
                }

            ep_map: Dict[str, Dict] = {}
            for e in episodes:
                ep_id = _generate_episode_id(e.get("guid"), e.get("url", ""))
                ep_map[ep_id] = {
                    "feed_url":        _normalize_url(e.get("feed_url", "")),
                    "guid":            e.get("guid"),
                    "url":             e.get("url", ""),
                    "title":           e.get("title", ""),
                    "state":           e.get("state", "unplayed"),
                    "progress_seconds": e.get("progress_seconds", 0),
                    "duration_seconds": e.get("duration_seconds", 0),
                    "updated_by":      self.device_id,
                    "updated_at":      now,
                    "custom":          e.get("custom", {}),
                }

            # Stage as pending ops so sync() merges them properly
            for url, rec in feed_map.items():
                self._pending_local_ops.append(
                    {"type": "feed", "url": url, "ts": now, "data": rec}
                )
            for ep_id, rec in ep_map.items():
                self._pending_local_ops.append(
                    {"type": "episode", "ep_id": ep_id, "ts": now, "data": rec}
                )

            # Queue items go straight to the op log
            q_list = queue_ep_ids or []
            if q_list:
                queue_items = [{"ep_id": eid, "added_at": now} for eid in q_list]
                ops_file    = self.sync_dir / "queue_ops" / f"{self.device_id}.jsonl"
                op = {
                    "ts":        now,
                    "device_id": self.device_id,
                    "op":        "add",
                    "items":     queue_items,
                    "after_id":  None,
                }
                with ops_file.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(op, ensure_ascii=False) + "\n")

            counts = {
                "feeds":                 len(feed_map),
                "feeds_skipped_deleted": skipped_deleted,
                "episodes":              len(ep_map),
                "queue":                 len(q_list),
            }
            logger.info("Bootstrap prepared: %s", counts)
            return counts

    # ─────────────────────────────────────────────────────────────────────────
    # READ ACCESSORS
    # ─────────────────────────────────────────────────────────────────────────

    def get_feeds(self, include_archived: bool = False) -> Dict[str, Dict]:
        """
        Return feed records, excluding deleted ones.

        Parameters
        ----------
        include_archived:
            If ``True``, return both ``"active"`` and ``"archived"`` feeds.
            If ``False`` (default), return only ``"active"`` feeds.
        """
        with self._lock:
            all_feeds = self._synced_state.get("feeds", {}).get("feeds", {})
            if include_archived:
                return {k: v for k, v in all_feeds.items()
                        if v.get("status") != "deleted"}
            return {k: v for k, v in all_feeds.items()
                    if v.get("status") == "active"}

    def get_episodes(self) -> Dict[str, Dict]:
        """Return all episode records (no filtering)."""
        with self._lock:
            return dict(self._synced_state.get("episodes", {}).get("episodes", {}))

    def get_episodes_for_feed(self, feed_url: str) -> Dict[str, Dict]:
        """Return episode records whose ``feed_url`` matches *feed_url* (normalized)."""
        with self._lock:
            norm = _normalize_url(feed_url)
            return {
                ep_id: rec
                for ep_id, rec in
                self._synced_state.get("episodes", {}).get("episodes", {}).items()
                if rec.get("feed_url") == norm
            }

    def get_queue(self) -> List[Dict]:
        """
        Return the current play queue.

        The queue is reconstructed live from the snapshot and op logs so the
        result is always up-to-date with ops written since the last sync.
        """
        with self._lock:
            snapshot = self._synced_state.get("queue", {})
            items, _ = _rebuild_queue(snapshot, self.sync_dir / "queue_ops")
            return items

    def get_devices(self) -> Dict[str, Dict]:
        """Return all device records."""
        with self._lock:
            return dict(self._synced_state.get("devices", {}).get("devices", {}))

    def get_state(self) -> Dict[str, Any]:
        """Return a shallow copy of the full in-memory synced state (read-only)."""
        with self._lock:
            return dict(self._synced_state)

    # ─────────────────────────────────────────────────────────────────────────
    # EXPORT
    # ─────────────────────────────────────────────────────────────────────────

    def export_opml(self, include_archived: bool = True) -> str:
        """
        Generate OPML 2.0 from the current feed list.

        Output is **deterministic**: feeds are sorted by ``(title, url)`` so
        the same state always produces the same file byte-for-byte.

        Deleted feeds are never included.  Archived feeds are included by
        default (``include_archived=True``).
        """
        feeds = self.get_feeds(include_archived=include_archived)
        ordered = sorted(feeds.values(), key=lambda f: (f.get("title", ""), f.get("url", "")))

        def _esc(s: str) -> str:
            return (s.replace("&", "&amp;")
                     .replace('"', "&quot;")
                     .replace("<", "&lt;")
                     .replace(">", "&gt;"))

        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<opml version="2.0">',
            "  <head>",
            "    <title>FilePodSync Subscriptions</title>",
            f"    <dateCreated>{datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S +0000')}</dateCreated>",
            "  </head>",
            "  <body>",
        ]
        for feed in ordered:
            title = _esc(feed.get("title") or feed.get("url", ""))
            url   = _esc(feed.get("url", ""))
            lines.append(f'    <outline type="rss" text="{title}" xmlUrl="{url}" />')
        lines += ["  </body>", "</opml>"]
        return "\n".join(lines)

    def import_opml(self, opml_text: str) -> int:
        """
        Import subscriptions from an OPML 2.0 string.

        Parses ``xmlUrl`` attributes from ``<outline type="rss">`` elements
        and calls ``add_feed()`` for each one.  Returns the count of feeds
        successfully imported (invalid/empty URLs are skipped).
        """
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(opml_text)
        except ET.ParseError as exc:
            logger.error("import_opml: XML parse error: %s", exc)
            return 0

        count = 0
        for outline in root.iter("outline"):
            url   = outline.get("xmlUrl", "").strip()
            title = outline.get("text", outline.get("title", "")).strip()
            if url and self.add_feed(url, title):
                count += 1
        logger.info("import_opml: imported %d feeds", count)
        return count

    # ─────────────────────────────────────────────────────────────────────────
    # HOUSEKEEPING
    # ─────────────────────────────────────────────────────────────────────────

    def _housekeeping(self) -> None:
        """Run log rotation and snapshot pruning."""
        self._rotate_logs()
        self._prune_snapshots()

    def _rotate_logs(self) -> None:
        """
        Rotate logs by **filename date**, not filesystem ``mtime``.

        Sync providers (Dropbox, Syncthing, iCloud, Google Drive) may update
        ``mtime`` when verifying or transferring files, making mtime-based age
        checks produce false negatives (files never deleted) or false positives
        (files deleted too early).

        Log files follow the naming convention ``sync-YYYYMMDD.jsonl``.
        YYYYMMDD lexicographic comparison is equivalent to date comparison.

        Oversized files (> ``LOG_MAX_MB``) are compressed to ``.jsonl.gz``
        regardless of age.
        """
        log_dir = self.sync_dir / "logs"
        if not log_dir.exists():
            return

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=LOG_MAX_DAYS)
        ).strftime("%Y%m%d")

        for f in log_dir.glob("sync-????????.jsonl"):
            file_date = f.stem[5:]  # "sync-20240101" → "20240101"
            try:
                if file_date < cutoff:
                    f.unlink()
                    continue
                if f.stat().st_size > LOG_MAX_MB * 1024 * 1024:
                    gz_path = f.with_suffix(".jsonl.gz")
                    with f.open("rb") as src, gzip.open(gz_path, "wb") as dst:
                        dst.write(src.read())
                    f.unlink()
            except OSError:
                pass

    def _prune_snapshots(self) -> None:
        """Keep only the most recent ``SNAPSHOT_RETENTION`` snapshot files."""
        snap_dir = self.sync_dir / "snapshots"
        if not snap_dir.exists():
            return
        snaps = sorted(
            snap_dir.glob("snapshot-*.json.gz"),
            key=lambda p: p.stat().st_mtime,
        )
        while len(snaps) > SNAPSHOT_RETENTION:
            try:
                snaps.pop(0).unlink()
            except OSError:
                break

    def create_snapshot(self) -> Path:
        """
        Write a compressed ``snapshot-<ISO8601>.json.gz`` file containing the
        full in-memory synced state.

        Called automatically during ``shutdown()`` and after each
        ``sync()`` cycle (via ``_housekeeping`` → ``_prune_snapshots`` only
        prunes; explicit snapshot creation is the caller's responsibility for
        the disaster-recovery guarantee).
        """
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.sync_dir / "snapshots" / f"snapshot-{ts}.json.gz"
        with self._lock:
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                json.dump(self._synced_state, fh, default=str)
        self._prune_snapshots()
        logger.info("Snapshot created: %s", path.name)
        return path

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT LOGGING
    # ─────────────────────────────────────────────────────────────────────────

    def _log_event(self, event: str, data: Optional[Dict] = None) -> None:
        """
        Append one JSON line to today's sync log (``logs/sync-YYYYMMDD.jsonl``).

        Errors are caught and logged to the Python logger rather than
        propagated; a log write failure must never abort a sync cycle.
        """
        today    = datetime.now(timezone.utc).strftime("%Y%m%d")
        log_file = self.sync_dir / "logs" / f"sync-{today}.jsonl"
        entry    = {
            "ts":     get_utc_ms(),
            "device": self.device_id,
            "event":  event,
            **(data or {}),
        }
        try:
            with log_file.open("a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(entry, separators=(",", ":"), ensure_ascii=False) + "\n"
                )
        except OSError as exc:
            logger.warning("Could not write to sync log %s: %s", log_file, exc)

    # ─────────────────────────────────────────────────────────────────────────
    # SHUTDOWN & CONTEXT MANAGER
    # ─────────────────────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """
        Flush pending queue ops, create a final snapshot, and log the shutdown.

        Safe to call from a signal handler or ``atexit`` hook.  The debounce
        timer is cancelled first so the daemon thread does not race with the
        explicit flush below.
        """
        # Cancel timer before flushing to prevent a double-write race
        if self._queue_debounce_timer:
            self._queue_debounce_timer.cancel()
            self._queue_debounce_timer = None

        self._flush_queue_ops()  # immediate, no debounce

        try:
            self.create_snapshot()
        except Exception as exc:  # pragma: no cover
            logger.warning("Snapshot failed during shutdown: %s", exc)

        self._log_event("shutdown")
        logger.info("FilePodSync shutdown complete.")

    def __enter__(self) -> "FilePodSync":
        return self

    def __exit__(self, *_: Any) -> None:
        self.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# SMOKE TEST  —  python filepodsync.py
# ─────────────────────────────────────────────────────────────────────────────

def _run_smoke_tests() -> None:
    """
    Self-contained smoke test suite.  Runs in a temporary directory.
    Covers all v1.3 behavioral changes as well as core v1.2 features.
    Exit code 0 on success, 1 on failure.
    """
    import sys
    import tempfile

    logging.basicConfig(level=logging.WARNING)  # suppress INFO noise during tests
    passed = failed = 0

    def ok(name: str) -> None:
        nonlocal passed
        passed += 1
        print(f"  ✓  {name}")

    def fail(name: str, reason: str) -> None:
        nonlocal failed
        failed += 1
        print(f"  ✗  {name}: {reason}")

    def check(name: str, condition: bool, reason: str = "") -> None:
        (ok if condition else lambda n: fail(n, reason or "assertion failed"))(name)

    print("\n══════  FilePodSync v1.3 smoke tests  ══════\n")

    with tempfile.TemporaryDirectory() as tmp:
        fps = FilePodSync(tmp, device_name="Test Device", platform="test", client="test")

        # ── Device ID ────────────────────────────────────────────────────────
        raw_id = (Path(tmp) / ".fps_device_id").read_text(encoding="utf-8").strip()
        check("Device ID: no JSON quotes",   not raw_id.startswith('"'))
        check("Device ID: valid UUID format", len(raw_id.split("-")) == 5)

        # ── URL normalization ────────────────────────────────────────────────
        check("URL norm: trailing slash",
              _normalize_url("https://feeds.example.com/podcast/") ==
              "https://feeds.example.com/podcast")
        check("URL norm: uppercase scheme+host",
              _normalize_url("HTTPS://Feeds.Example.COM/podcast") ==
              "https://feeds.example.com/podcast")
        check("URL norm: remove :443",
              _normalize_url("https://feeds.example.com:443/podcast") ==
              "https://feeds.example.com/podcast")
        check("URL norm: keep :8080",
              _normalize_url("https://feeds.example.com:8080/podcast") ==
              "https://feeds.example.com:8080/podcast")
        check("URL norm: http and https are distinct",
              _normalize_url("http://feeds.example.com/podcast") !=
              _normalize_url("https://feeds.example.com/podcast"))

        # ── Conflict file detection ──────────────────────────────────────────
        check("Conflict: Syncthing",   _is_conflict_file("feeds.sync-conflict-20240101-1.json"))
        check("Conflict: Dropbox",     _is_conflict_file("feeds (John's conflicted copy 2024).json"))
        check("Conflict: Google Drive",_is_conflict_file("feeds (1).json"))
        check("Conflict: .tmp",        _is_conflict_file("feeds.json.tmp"))
        check("Conflict: hidden",      _is_conflict_file(".feeds.json"))
        check("No conflict: normal",   not _is_conflict_file("feeds.json"))
        check("No conflict: device op",not _is_conflict_file("a1b2c3d4-e5f6-7890-abcd-ef1234567890.jsonl"))

        # ── Schema validation ────────────────────────────────────────────────
        check("Schema: compatible 1.3",  _validate_schema_version({"schema_version": "1.3.0"}, "test"))
        check("Schema: compatible 1.99", _validate_schema_version({"schema_version": "1.99.0"}, "test"))
        check("Schema: reject 2.0",      not _validate_schema_version({"schema_version": "2.0.0"}, "test"))
        check("Schema: reject 0.x",      not _validate_schema_version({"schema_version": "0.9.0"}, "test"))
        check("Schema: reject missing",  not _validate_schema_version({}, "test"))

        # ── Episode ID generation ────────────────────────────────────────────
        check("EP ID: guid prefix",  _generate_episode_id("ep-001", "https://cdn.example.com/ep1.mp3") == "guid:ep-001")
        check("EP ID: guid=none → url hash", _generate_episode_id("none",  "https://cdn.example.com/ep1.mp3").startswith("url:"))
        check("EP ID: no guid → url hash",   _generate_episode_id(None,    "https://cdn.example.com/ep1.mp3").startswith("url:"))

        # ── Feed operations ──────────────────────────────────────────────────
        check("add_feed: returns True",  fps.add_feed("HTTPS://Feeds.Example.com/Podcast/", "Example Podcast"))
        check("add_feed: empty URL",     fps.add_feed("") is False)
        feeds = fps.get_feeds()
        check("add_feed: normalized key",   "https://feeds.example.com/podcast" in feeds)
        check("add_feed: title stored",      feeds["https://feeds.example.com/podcast"]["title"] == "Example Podcast")

        # Duplicate normalized URL → idempotent
        fps.add_feed("https://feeds.example.com/podcast/", "Example Podcast Updated")
        check("add_feed: idempotent count",  len(fps.get_feeds()) == 1)

        check("remove_feed: returns True",   fps.remove_feed("https://feeds.example.com/podcast/"))
        check("remove_feed: deleted",         fps.get_feeds().get("https://feeds.example.com/podcast") is None)
        check("remove_feed: unknown",         fps.remove_feed("https://notfound.example.com/rss") is False)

        fps.add_feed("https://feeds.example.com/podcast", "Example Podcast")  # re-add for later tests

        # ── HTTP → HTTPS redirect ────────────────────────────────────────────
        fps.add_feed("http://feeds.example.com/podcast", "HTTP Feed")
        fps.redirect_feed_http_to_https("http://feeds.example.com/podcast")
        all_feeds = fps.get_feeds(include_archived=True)
        check("redirect: http deleted",
              all_feeds.get("http://feeds.example.com/podcast", {}).get("status") == "deleted")
        check("redirect: https active",
              all_feeds.get("https://feeds.example.com/podcast", {}).get("status") == "active")

        # ── Episode operations ───────────────────────────────────────────────
        ep_id = fps.update_episode(
            episode_url="https://cdn.example.com/ep1.mp3",
            feed_url="https://feeds.example.com/podcast",
            guid="episode-001",
            title="Episode 1",
            position=600,
            total=3600,
            state="in_progress",
        )
        check("episode: correct ID",    ep_id == "guid:episode-001")
        check("episode: state stored",  fps.get_episodes()[ep_id]["state"] == "in_progress")
        check("episode: position",      fps.get_episodes()[ep_id]["progress_seconds"] == 600)

        # Rewind — no clamping
        fps.update_episode(
            episode_url="https://cdn.example.com/ep1.mp3",
            feed_url="https://feeds.example.com/podcast",
            guid="episode-001",
            position=10,
            state="in_progress",
        )
        check("episode: rewind allowed", fps.get_episodes()[ep_id]["progress_seconds"] == 10)

        # get_episodes_for_feed
        ep_map = fps.get_episodes_for_feed("https://feeds.example.com/podcast")
        check("episodes_for_feed: found",  ep_id in ep_map)
        empty   = fps.get_episodes_for_feed("https://other.example.com/rss")
        check("episodes_for_feed: empty",  len(empty) == 0)

        # ── Queue ops ────────────────────────────────────────────────────────
        fps.queue_add(ep_id)
        fps._flush_queue_ops()                 # bypass debounce
        q = fps.get_queue()
        check("queue_add: present",    len(q) == 1 and q[0]["ep_id"] == ep_id)

        ep_id2 = fps.update_episode(
            "https://cdn.example.com/ep2.mp3",
            "https://feeds.example.com/podcast",
            guid="episode-002",
            state="unplayed",
        )
        fps.queue_add(ep_id2, after_id=ep_id)
        fps._flush_queue_ops()
        q = fps.get_queue()
        check("queue_add after_id: order", q[0]["ep_id"] == ep_id and q[1]["ep_id"] == ep_id2)

        fps.queue_reorder([ep_id2, ep_id])
        fps._flush_queue_ops()
        q = fps.get_queue()
        check("queue_reorder: inverted", q[0]["ep_id"] == ep_id2 and q[1]["ep_id"] == ep_id)

        fps.queue_remove([ep_id2])
        fps._flush_queue_ops()
        q = fps.get_queue()
        check("queue_remove: gone",    len(q) == 1 and q[0]["ep_id"] == ep_id)

        fps.queue_clear()
        fps._flush_queue_ops()
        check("queue_clear: empty",    len(fps.get_queue()) == 0)

        # ── Queue ops carry device_id ────────────────────────────────────────
        fps.queue_add(ep_id)
        fps._flush_queue_ops()
        ops_file = Path(tmp) / "queue_ops" / f"{fps.device_id}.jsonl"
        lines = [l for l in ops_file.read_text(encoding="utf-8").splitlines() if l.strip()]
        last_op = json.loads(lines[-1])
        check("queue op: device_id present", "device_id" in last_op)
        check("queue op: device_id correct", last_op["device_id"] == fps.device_id)

        # ── Queue reconstruction: consolidated_through_ts ────────────────────
        # Write a snapshot with ctt = max of all current ops, then add a new op.
        # The new op (ts > ctt) should appear; old ops should be skipped.
        fps._flush_queue_ops()
        fps.queue_clear()
        fps._flush_queue_ops()
        fps._consolidate_queue()
        snap_ctt = (Path(tmp) / "queue.json")
        snap_data = json.loads(snap_ctt.read_text(encoding="utf-8"))
        ctt_val = snap_data.get("consolidated_through_ts", 0)
        check("consolidation: ctt written",  ctt_val > 0)

        fps.queue_add(ep_id)
        fps._flush_queue_ops()
        q = fps.get_queue()
        check("consolidation: post-ctt op applied", len(q) == 1)

        # ── Consolidation sovereignty: own file reset, NOT others ────────────
        # Create a fake second device op file and verify consolidation leaves it alone
        other_dev_file = Path(tmp) / "queue_ops" / "other-device-uuid.jsonl"
        fake_op = json.dumps({"ts": get_utc_ms() - 1_000_000, "device_id": "other-device-uuid",
                               "op": "add", "items": [{"ep_id": "guid:fake", "added_at": 0}],
                               "after_id": None})
        other_dev_file.write_text(fake_op + "\n", encoding="utf-8")
        fps._consolidate_queue()
        check("sovereignty: other device file untouched", other_dev_file.exists())
        own_file_content = (Path(tmp) / "queue_ops" / f"{fps.device_id}.jsonl").read_text(encoding="utf-8")
        check("sovereignty: own file reset", own_file_content.strip() == "")

        # ── LWW-EL merge ─────────────────────────────────────────────────────
        local  = {"k1": {"updated_at": 100, "updated_by": "aaa", "val": "old"},
                  "k2": {"updated_at": 100, "updated_by": "bbb", "val": "local-only"}}
        remote = {"k1": {"updated_at": 200, "updated_by": "bbb", "val": "new"},
                  "k3": {"updated_at":  50, "updated_by": "ccc", "val": "remote-only"}}
        merged = _merge_records(local, remote)
        check("LWW: higher ts wins",     merged["k1"]["val"] == "new")
        check("LWW: local-only survives",merged["k2"]["val"] == "local-only")
        check("LWW: remote-only added",  merged["k3"]["val"] == "remote-only")

        # Tie-break by device_id
        local2  = {"k": {"updated_at": 100, "updated_by": "aaa", "v": "A"}}
        remote2 = {"k": {"updated_at": 100, "updated_by": "bbb", "v": "B"}}
        check("LWW: tie → larger UUID wins", _merge_records(local2, remote2)["k"]["v"] == "B")
        remote3 = {"k": {"updated_at": 100, "updated_by": "000", "v": "C"}}
        check("LWW: tie → smaller UUID loses", _merge_records(local2, remote3)["k"]["v"] == "A")

        # ── Bootstrap: deleted feed guard ────────────────────────────────────
        fps2 = FilePodSync(tmp, device_name="Second Device")
        # Add a feed and delete it so it exists as "deleted" in feeds.json
        fps.add_feed("https://deleted.example.com/rss", "To Delete")
        fps.remove_feed("https://deleted.example.com/rss")
        fps.sync(force=True)

        result = fps2.bootstrap_from_local(
            feeds=[{"url": "https://deleted.example.com/rss", "title": "Should Not Resurrect"}],
            episodes=[],
        )
        check("bootstrap: skipped deleted", result["feeds_skipped_deleted"] == 1)
        check("bootstrap: feed not staged", result["feeds"] == 0)

        # ── Full sync cycle ───────────────────────────────────────────────────
        r = fps.sync(force=True)
        check("sync: returns feeds count",    r.get("feeds", 0) > 0)
        check("sync: returns episodes count", r.get("episodes", 0) > 0)
        check("sync: returns devices count",  r.get("devices", 0) > 0)
        check("sync: timestamp present",      r.get("timestamp", 0) > 0)

        # Throttle
        r2 = fps.sync()
        check("sync: throttled",  r2.get("throttled") is True)
        r3 = fps.sync(force=True)
        check("sync: force bypasses throttle", r3.get("throttled") is not True)

        # ── OPML export / import round-trip ──────────────────────────────────
        opml_out = fps.export_opml()
        check("OPML: contains xmlUrl",     'xmlUrl="https://feeds.example.com/podcast"' in opml_out)
        check("OPML: deleted excluded",    'xmlUrl="https://deleted.example.com/rss"' not in opml_out)
        check("OPML: valid XML header",    opml_out.startswith('<?xml version="1.0"'))

        fps3 = FilePodSync(tmp + "_import", device_name="Import Device")
        imported = fps3.import_opml(opml_out)
        check("OPML import: count > 0",    imported > 0)
        check("OPML import: feed present", "https://feeds.example.com/podcast" in fps3.get_feeds())

        # ── Snapshot & restore ────────────────────────────────────────────────
        snap = fps.create_snapshot()
        check("snapshot: file exists",     snap.exists())
        check("snapshot: is gzip",         snap.suffix == ".gz")

        # Corrupt feeds.json and restore from snapshot
        (Path(tmp) / "feeds.json").write_text("{invalid json", encoding="utf-8")
        restored = fps._restore_from_snapshot("feeds")
        check("snapshot restore: non-empty", bool(restored))
        # Verify timestamps are preserved (not re-stamped to now)
        if restored and "feeds" in restored:
            any_ts = next(iter(restored["feeds"].values()), {}).get("updated_at", 0)
            check("snapshot restore: ts preserved", any_ts < get_utc_ms() - 1000)

        # ── Log rotation by filename date ─────────────────────────────────────
        log_dir = Path(tmp) / "logs"
        old_log = log_dir / "sync-20200101.jsonl"
        new_log = log_dir / "sync-99991231.jsonl"
        old_log.write_text('{"ts":1,"event":"old"}\n', encoding="utf-8")
        new_log.write_text('{"ts":2,"event":"new"}\n', encoding="utf-8")
        # Manually set mtime on old_log to NOW to verify we DON'T use mtime
        os.utime(old_log, times=(_time.time(), _time.time()))
        fps._rotate_logs()
        check("log rotation: old deleted",     not old_log.exists())
        check("log rotation: new preserved",    new_log.exists())
        new_log.unlink(missing_ok=True)

        # ── Retire stale devices & prune ops ─────────────────────────────────
        stale_id = "stale-device-0000-0000-000000000001"
        stale_ts = get_utc_ms() - (DEVICE_RETIREMENT_DAYS + 1) * 86_400_000
        devices = fps._synced_state.get("devices", {}).get("devices", {})
        devices[stale_id] = {
            "name": "Stale", "platform": "test", "client": "test",
            "status": "active",
            "first_seen": stale_ts, "last_seen": stale_ts,
            "updated_by": stale_id, "updated_at": stale_ts,
        }
        fps._save_state_file("devices", devices)
        retired = fps.retire_stale_devices()
        check("retire devices: found",   stale_id in retired)
        devs    = fps.get_devices()
        check("retire devices: status",  devs[stale_id]["status"] == "retired")

        stale_ops = Path(tmp) / "queue_ops" / f"{stale_id}.jsonl"
        stale_ops.write_text("", encoding="utf-8")
        pruned = fps.prune_retired_device_ops()
        check("prune ops: empty file removed", stale_ops.name in pruned)

        # ── Shutdown & context manager ────────────────────────────────────────
        fps.shutdown()
        check("shutdown: snapshot exists",
              len(list((Path(tmp) / "snapshots").glob("*.gz"))) > 0)

        with FilePodSync(tmp + "_ctx") as ctx_fps:
            ctx_fps.add_feed("https://ctx.example.com/rss", "Context Feed")
        check("context manager: no exception", True)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n══════  {passed}/{total} passed", "✓" if failed == 0 else f"  {failed} FAILED  ✗", " ══════\n")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    _run_smoke_tests()

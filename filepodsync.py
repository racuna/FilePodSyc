"""
FilePodSync (FPS) Client Library v1.1
Provider-agnostic, file-based podcast synchronization standard.
Zero-API, LWW-EL merge, atomic writes, conflict-resistant.
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
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict

# ─────────────────────────────────────────────────────────────
# CONSTANTS & UTILS
# ─────────────────────────────────────────────────────────────
SCHEMA_VERSION = "1.1.0"
LOG_MAX_MB = 10
SNAPSHOT_RETENTION = 5
QUEUE_DEBOUNCE_S = 2.0
UTC_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger("filepodsync")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

def get_utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _atomic_write(path: Path, data: Any) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    os.replace(tmp, path)

def _load_json_safe(path: Path, fallback: Any = None) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, ValueError):
        return fallback

def _filter_sync_conflicts(folder: Path) -> List[Path]:
    return [
        f for f in folder.iterdir() 
        if f.is_file() and 
           f.suffix == ".json" and 
           "sync-conflict" not in f.name.lower() and 
           f.suffix not in (".tmp", ".partial")
    ]

def _generate_episode_id(guid: Optional[str], url: str) -> str:
    if guid and guid.strip().lower() != "none":
        return f"guid:{guid.strip()}"
    return f"url:{hashlib.sha256(url.encode()).hexdigest()[:16]}"

# ─────────────────────────────────────────────────────────────
# MAIN CLIENT
# ─────────────────────────────────────────────────────────────
class FilePodSync:
    """
    Standalone FilePodSync client.
    Handles local state, LWW-EL merging, atomic writes, and provider sync triggers.
    """
    def __init__(self, sync_dir: str, device_name: str = "FilePodSync Client"):
        self.sync_dir = Path(sync_dir)
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        
        self.device_id = self._load_device_id()
        self.device_name = device_name
        
        self._lock = threading.RLock()
        self._queue_debounce_timer: Optional[threading.Timer] = None
        self._pending_actions: List[Dict] = []
        
        self._init_structure()
        self.state = self._load_state()
        
        logger.info(f"FilePodSync initialized. Device: {self.device_id} | Dir: {self.sync_dir}")

    def _load_device_id(self) -> str:
        id_file = self.sync_dir / ".fps_device_id"
        if id_file.exists():
            return id_file.read_text().strip()
        new_id = str(uuid.uuid4())
        _atomic_write(id_file, new_id)
        return new_id

    def _init_structure(self) -> None:
        for sub in ["logs", "snapshots"]:
            (self.sync_dir / sub).mkdir(exist_ok=True)
        for f in ["config", "devices", "feeds", "episodes", "queue"]:
            path = self.sync_dir / f"{f}.json"
            if not path.exists():
                _atomic_write(path, {"schema_version": SCHEMA_VERSION, "updated_at": 0, "updated_by": "", "data": {}})

    def _load_state(self) -> Dict[str, Any]:
        state = {}
        for f in ["config", "devices", "feeds", "episodes", "queue"]:
            state[f] = _load_json_safe(self.sync_dir / f"{f}.json", {})
            if "data" not in state[f]:
                state[f]["data"] = {}
        return state

    def _save_state(self, subset: Optional[List[str]] = None) -> None:
        files = subset or ["config", "devices", "feeds", "episodes", "queue"]
        now = get_utc_ms()
        for f in files:
            data = self.state[f]
            data["updated_at"] = now
            data["updated_by"] = self.device_id
            _atomic_write(self.sync_dir / f"{f}.json", data)
        self._log_sync("state_flushed", {"files": files})

    # ─────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────
    def add_feed(self, url: str, title: str = "", custom: Dict = {}) -> None:
        with self._lock:
            feeds = self.state["feeds"]["data"]
            now = get_utc_ms()
            feed_id = url
            if feed_id not in feeds:
                feeds[feed_id] = {
                    "url": url, "title": title, "status": "active",
                    "health_status": "healthy", "last_check": 0, "error_count": 0,
                    "added_by": self.device_id, "added_at": now,
                    "updated_by": self.device_id, "updated_at": now, "custom": custom
                }
            self._save_state(["feeds"])

    def remove_feed(self, url: str, mark_archived: bool = True) -> None:
        with self._lock:
            feed_id = url
            if feed_id in self.state["feeds"]["data"]:
                self.state["feeds"]["data"][feed_id]["status"] = "archived" if mark_archived else "deleted"
                self.state["feeds"]["data"][feed_id].update({
                    "updated_by": self.device_id, "updated_at": get_utc_ms()
                })
            self._save_state(["feeds"])

    def update_episode(self, episode_url: str, feed_url: str, 
                       position: int = 0, total: int = 0, 
                       state: str = "unplayed", custom: Dict = {}) -> None:
        with self._lock:
            ep_id = _generate_episode_id("", episode_url)
            eps = self.state["episodes"]["data"]
            now = get_utc_ms()
            
            if ep_id not in eps:
                eps[ep_id] = {
                    "feed_url": feed_url, "url": episode_url, 
                    "state": state, "progress_seconds": 0, "duration_seconds": total,
                    "updated_by": self.device_id, "updated_at": now, "custom": custom
                }
            
            rec = eps[ep_id]
            rec.update({
                "progress_seconds": max(rec.get("progress_seconds", 0), position),
                "duration_seconds": total if total > 0 else rec.get("duration_seconds", total),
                "state": state if state in ("unplayed", "in_progress", "completed") else rec.get("state", "unplayed"),
                "updated_by": self.device_id, "updated_at": now
            })
            if state == "completed":
                rec["progress_seconds"] = total
            
            self._save_state(["episodes"])

    def set_queue(self, episode_urls: List[str]) -> None:
        """Debounced queue update. Prevents sync thrashing."""
        if self._queue_debounce_timer:
            self._queue_debounce_timer.cancel()
        
        self._pending_queue = episode_urls
        self._queue_debounce_timer = threading.Timer(QUEUE_DEBOUNCE_S, self._flush_queue)
        self._queue_debounce_timer.daemon = True
        self._queue_debounce_timer.start()

    def _flush_queue(self) -> None:
        with self._lock:
            self.state["queue"] = {
                "schema_version": SCHEMA_VERSION,
                "updated_at": get_utc_ms(),
                "updated_by": self.device_id,
                "items": self._pending_queue
            }
            self._save_state(["queue"])
            self._log_sync("queue_flushed", {"count": len(self._pending_queue)})

    def sync(self) -> Dict[str, Any]:
        """Full LWW-EL sync with provider-managed folder."""
        with self._lock:
            remote_state = {}
            for f in ["config", "devices", "feeds", "episodes", "queue"]:
                path = self.sync_dir / f"{f}.json"
                if "sync-conflict" in path.name.lower():
                    continue
                remote_state[f] = _load_json_safe(path, {})
            
            merged = self._merge_state(self.state, remote_state)
            self.state = merged
            
            self._register_device()
            self._save_state()
            self._housekeeping()
            
            return {
                "feeds": len(merged["feeds"]["data"]),
                "episodes": len(merged["episodes"]["data"]),
                "queue": len(merged.get("queue", {}).get("items", [])),
                "timestamp": merged.get("feeds", {}).get("updated_at", 0)
            }

    def _merge_state(self, local: Dict, remote: Dict) -> Dict:
        merged = {k: dict(v) for k, v in local.items()}
        
        for key in ["feeds", "episodes"]:
            local_data = local[key].get("data", {})
            remote_data = remote.get(key, {}).get("data", {})
            merged_data = dict(local_data)
            
            for rid, rrec in remote_data.items():
                lrec = local_data.get(rid)
                if not lrec:
                    merged_data[rid] = rrec
                else:
                    if rrec.get("updated_at", 0) > lrec.get("updated_at", 0):
                        merged_data[rid] = rrec
                    elif rrec.get("updated_at", 0) == lrec.get("updated_at", 0):
                        if rrec.get("updated_by", "") > lrec.get("updated_by", ""):
                            merged_data[rid] = rrec
            
            merged[key]["data"] = merged_data
            
        if "queue" in remote and remote["queue"].get("items"):
            lq = local.get("queue", {})
            rq = remote["queue"]
            if rq.get("updated_at", 0) > lq.get("updated_at", 0):
                merged["queue"] = rq
            elif rq.get("updated_at", 0) == lq.get("updated_at", 0):
                if rq.get("updated_by", "") > lq.get("updated_by", ""):
                    merged["queue"] = rq
                    
        return merged

    def _register_device(self) -> None:
        devs = self.state["devices"].get("data", {})
        if self.device_id not in devs:
            devs[self.device_id] = {
                "name": self.device_name, "platform": "python",
                "first_seen": get_utc_ms(), "last_seen": get_utc_ms()
            }
        else:
            devs[self.device_id]["last_seen"] = get_utc_ms()
        self.state["devices"]["data"] = devs

    def export_opml(self) -> str:
        """Generates standard OPML 2.0 from synced feeds."""
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<opml version="2.0">',
            '  <head><title>FilePodSync Subscriptions</title></head>',
            '  <body>'
        ]
        for fid, feed in self.state["feeds"]["data"].items():
            if feed.get("status") == "deleted":
                continue
            title = feed.get("title", fid).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            url = feed.get("url", fid)
            lines.append(f'    <outline type="rss" text="{title}" xmlUrl="{url}" />')
        lines.extend(['  </body>', '</opml>'])
        return "\n".join(lines)

    def _housekeeping(self) -> None:
        # Log rotation
        log_dir = self.sync_dir / "logs"
        for f in log_dir.glob("*.jsonl"):
            if f.stat().st_size > LOG_MAX_MB * 1024 * 1024:
                backup = f.with_suffix(".jsonl.bak")
                f.rename(backup)
        
        # Snapshot pruning
        snap_dir = self.sync_dir / "snapshots"
        snaps = sorted(snap_dir.glob("snapshot-*.json.gz"), key=lambda p: p.stat().st_mtime)
        while len(snaps) > SNAPSHOT_RETENTION:
            snaps.pop(0).unlink()

    def _log_sync(self, event: str, data: Dict = {}) -> None:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        log_file = self.sync_dir / "logs" / f"sync-{today}.jsonl"
        entry = {"ts": get_utc_ms(), "device": self.device_id, "event": event, **data}
        with log_file.open("a") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")

    def create_snapshot(self) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.sync_dir / "snapshots" / f"snapshot-{ts}.json.gz"
        with gzip.open(path, "wt") as f:
            json.dump(self.state, f, default=str)
        self._housekeeping()
        return path

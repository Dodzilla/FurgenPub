#!/usr/bin/env python3
"""
Furgen Content Server - Dependency Manager Agent (Vast.ai instances)

This is a single-file agent intended to run on each ComfyUI instance.
It polls the backend for dependency queue items and downloads missing artifacts
into the ComfyUI workspace, reporting status + inventory back to the backend.

Backend endpoints used (relative to FCS_API_BASE_URL, which should end with /api):
  - POST   /dependencies/register
  - GET    /dependencies/queue?instanceId=...&limit=...
  - POST   /dependencies/status
  - POST   /dependencies/heartbeat

Required environment variables on the instance:
  - FCS_API_BASE_URL   e.g. https://us-central1-<projectId>.cloudfunctions.net/api
  - SERVER_TYPE        e.g. furry-standard-v8

Recommended (for auth):
  - DEPENDENCY_MANAGER_SHARED_SECRET  (same value configured in Firebase Functions secret)

Download tokens (optional; required only if dependencies specify them):
  - HF_TOKEN
  - CIVITAI_TOKEN

Optional knobs:
  - DM_INSTANCE_ID          (skip IP detection; recommended if you can inject it)
  - DM_INSTANCE_IP          (use this IP for register() lookup)
  - DM_COMFYUI_DIR          (default: $WORKSPACE/ComfyUI)
  - WORKSPACE               (default: /workspace)
  - DM_POLL_SECONDS         (default: 5)
  - DM_HEARTBEAT_SECONDS    (default: 30)
  - MAX_PARALLEL_DOWNLOADS  (default: 1)
  - DM_STATE_PATH           (default: $WORKSPACE/dependency_agent_state.json)
  - DM_ALLOWED_DOMAINS      (comma-separated allowlist; default: huggingface.co,civitai.com)
  - DM_DYNAMIC_EVICTION_ENABLED   (override profile.dynamicPolicy.enabled; default: false)
  - DM_DYNAMIC_MIN_FREE_BYTES     (override profile.dynamicPolicy.minFreeBytes; supports 10GB/500MiB)
  - DM_DYNAMIC_MAX_BYTES          (override profile.dynamicPolicy.maxDynamicBytes; supports 50GB/2TiB)
  - DM_EVICTION_BATCH_MAX         (override profile.dynamicPolicy.evictionBatchMax; default: 20)
  - DM_PIN_TTL_SECONDS            (do not evict deps touched within this window; default: 1800)

Queue item expectations:
  - The backend should include a `resolved` object for download items with:
      { url, auth, destRelativePath, sha256?, expectedSizeBytes?, kind? }
  - For touch items, `resolved` should include at least:
      { destRelativePath, kind }
    The backend in this repo enriches /dependencies/queue responses accordingly.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import signal
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


AGENT_VERSION = "dm-agent-py/0.2.0"


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_int(name: str, default: int) -> int:
    v = _env_str(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = _env_str(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


def _parse_bytes(v: Optional[str]) -> Optional[int]:
    if not v:
        return None
    s = v.strip()
    if not s:
        return None

    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\\s*([a-zA-Z]{0,4})$", s)
    if not m:
        return None
    num = float(m.group(1))
    unit = (m.group(2) or "").lower()

    if unit in ("", "b"):
        return int(num)

    # Use IEC-style base (1024) for all K/M/G/T variants to avoid surprises with disk_usage().
    base = 1024
    mult = {
        "k": base,
        "kb": base,
        "kib": base,
        "m": base**2,
        "mb": base**2,
        "mib": base**2,
        "g": base**3,
        "gb": base**3,
        "gib": base**3,
        "t": base**4,
        "tb": base**4,
        "tib": base**4,
    }.get(unit)
    if mult is None:
        return None
    return int(num * mult)


def _split_csv(v: Optional[str]) -> List[str]:
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _sleep_with_jitter(seconds: float, jitter_ratio: float = 0.2) -> None:
    if seconds <= 0:
        return
    jitter = seconds * jitter_ratio
    time.sleep(max(0.0, seconds + random.uniform(-jitter, jitter)))


def _looks_like_ipv4(s: str) -> bool:
    if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", s):
        return False
    parts = s.split(".")
    return all(0 <= int(p) <= 255 for p in parts)


def detect_public_ip(timeout_seconds: float = 5.0) -> Optional[str]:
    urls = [
        "https://api.ipify.org",
        "https://checkip.amazonaws.com",
        "https://ipv4.icanhazip.com",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "dm-agent-ip/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                text = resp.read().decode("utf-8", errors="ignore").strip()
                if _looks_like_ipv4(text):
                    return text
        except Exception:
            continue
    return None


def disk_stats(path: Path) -> Dict[str, int]:
    usage = shutil.disk_usage(str(path))
    return {"totalBytes": int(usage.total), "freeBytes": int(usage.free), "usedBytes": int(usage.used)}


def safe_join(base_dir: Path, rel: str) -> Path:
    rel_path = Path(rel)
    if rel_path.is_absolute():
        raise ValueError(f"destRelativePath must be relative, got: {rel}")
    # Prevent "../" traversal by ensuring the resolved path stays within base_dir.
    base_resolved = base_dir.resolve()
    target = (base_resolved / rel_path).resolve()
    if target == base_resolved:
        return target
    if not str(target).startswith(str(base_resolved) + os.sep):
        raise ValueError(f"destRelativePath escapes base dir: {rel}")
    return target


class ApiError(RuntimeError):
    def __init__(self, status: int, body: Optional[str] = None):
        super().__init__(f"API error {status}: {body or ''}".strip())
        self.status = status
        self.body = body


def _json_loads_or_none(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


def api_json(
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
) -> Tuple[int, Optional[Any]]:
    payload = None
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=payload, method=method.upper(), headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw:
                return resp.status, None
            parsed = _json_loads_or_none(raw)
            return resp.status, parsed if parsed is not None else raw
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        raise ApiError(int(getattr(e, "code", 500) or 500), raw) from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error calling {url}: {e}") from None


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def http_download(
    url: str,
    dest_partial: Path,
    auth_header: Optional[str],
    timeout_seconds: float = 30.0,
    chunk_size: int = 8 * 1024 * 1024,
    user_agent: str = "dm-agent-download/1.0",
    allowed_domains: Optional[Set[str]] = None,
    verbose: bool = False,
) -> None:
    if allowed_domains:
        try:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.hostname or "").lower()
            ok = any(host == d or host.endswith("." + d) for d in allowed_domains)
            if not ok:
                raise ValueError(f"Download domain not allowed: {host}")
        except Exception:
            raise ValueError("Invalid download URL") from None

    dest_partial.parent.mkdir(parents=True, exist_ok=True)

    headers: Dict[str, str] = {"User-Agent": user_agent}
    if auth_header:
        headers["Authorization"] = auth_header

    req = urllib.request.Request(url, headers=headers, method="GET")
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    # Always write to a temp file, then atomically rename to final path.
    start = time.time()
    downloaded = 0
    last_log = 0.0

    with opener.open(req, timeout=timeout_seconds) as resp:
        # urllib doesn't expose status reliably on all handlers; best-effort.
        with dest_partial.open("wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if verbose:
                    now = time.time()
                    if now - last_log >= 10:
                        elapsed = max(0.001, now - start)
                        mb = downloaded / (1024 * 1024)
                        rate = mb / elapsed
                        logging.info("downloaded %.1f MiB (%.2f MiB/s) -> %s", mb, rate, str(dest_partial))
                        last_log = now


@dataclass
class LocalState:
    installed_static: Set[str]
    installed_dynamic: Set[str]
    failed: Set[str]
    # For dynamic deps only: depId -> {destRelativePath, sizeBytes, lastTouchedAtMs}
    lru: Dict[str, Dict[str, Any]]

    @staticmethod
    def empty() -> "LocalState":
        return LocalState(installed_static=set(), installed_dynamic=set(), failed=set(), lru={})


class DependencyAgent:
    def __init__(self) -> None:
        self.api_base_url = (_env_str("FCS_API_BASE_URL") or "").rstrip("/")
        self.server_type = (_env_str("SERVER_TYPE") or "").strip()
        self.shared_secret = _env_str("DEPENDENCY_MANAGER_SHARED_SECRET")
        self.hf_token = _env_str("HF_TOKEN")
        self.civitai_token = _env_str("CIVITAI_TOKEN")
        self.instance_id = _env_str("DM_INSTANCE_ID")
        self.instance_ip = _env_str("DM_INSTANCE_IP")
        self.workspace = Path(_env_str("WORKSPACE", "/workspace") or "/workspace")
        self.comfyui_dir = Path(_env_str("DM_COMFYUI_DIR") or str(self.workspace / "ComfyUI"))
        self.state_path = Path(_env_str("DM_STATE_PATH") or str(self.workspace / "dependency_agent_state.json"))
        self.poll_seconds = _env_float("DM_POLL_SECONDS", 5.0)
        self.heartbeat_seconds = _env_float("DM_HEARTBEAT_SECONDS", 30.0)
        self.max_parallel = max(1, min(4, _env_int("MAX_PARALLEL_DOWNLOADS", 1)))
        self.verbose_progress = (_env_str("DM_VERBOSE_PROGRESS") or "").lower() in ("1", "true", "yes", "on")

        allowed = _split_csv(_env_str("DM_ALLOWED_DOMAINS")) or ["huggingface.co", "hf.co", "civitai.com"]
        self.allowed_domains = {d.lower() for d in allowed if d}

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._resolved_instance_id: Optional[str] = None
        self._profile: Dict[str, Any] = {}
        self._downloading: Set[str] = set()
        self._state: LocalState = self._load_state()
        self._dynamic_bytes_used = 0
        self._last_heartbeat_ms = 0

        # Best-effort local reconciliation (no API calls).
        with self._lock:
            self._reconcile_lru_locked()

    def validate_env(self) -> None:
        if not self.api_base_url:
            raise SystemExit("Missing required env var: FCS_API_BASE_URL")
        if not self.server_type:
            raise SystemExit("Missing required env var: SERVER_TYPE")

    def stop(self) -> None:
        self._stop.set()

    def _headers(self, use_token: bool = True, include_secret: bool = False) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if use_token and self._token:
            h["Authorization"] = f"Bearer {self._token}"
        if include_secret and self.shared_secret:
            h["X-DM-Secret"] = self.shared_secret
        return h

    def _dynamic_policy(self) -> Dict[str, Any]:
        # Profile (from /dependencies/register) may include dynamicPolicy.*; env vars override.
        prof = self._profile.get("dynamicPolicy") if isinstance(self._profile.get("dynamicPolicy"), dict) else {}

        enabled_env = _env_str("DM_DYNAMIC_EVICTION_ENABLED")
        if enabled_env is not None:
            enabled = enabled_env.lower() in ("1", "true", "yes", "y", "on")
        else:
            enabled = bool(prof.get("enabled") is True)

        min_free_env = _parse_bytes(_env_str("DM_DYNAMIC_MIN_FREE_BYTES"))
        min_free_prof = int(prof.get("minFreeBytes")) if isinstance(prof.get("minFreeBytes"), (int, float)) else 0
        min_free_bytes = min_free_env if min_free_env is not None else min_free_prof
        if enabled and min_free_bytes <= 0:
            # Conservative default floor when eviction is enabled but profile isn't set yet.
            min_free_bytes = 5 * 1024 * 1024 * 1024

        max_dyn_env = _parse_bytes(_env_str("DM_DYNAMIC_MAX_BYTES"))
        max_dyn_prof = int(prof.get("maxDynamicBytes")) if isinstance(prof.get("maxDynamicBytes"), (int, float)) else 0
        max_dynamic_bytes = max_dyn_env if max_dyn_env is not None else max_dyn_prof
        if max_dynamic_bytes < 0:
            max_dynamic_bytes = 0

        batch_env = _env_str("DM_EVICTION_BATCH_MAX")
        if batch_env is not None:
            eviction_batch_max = _env_int("DM_EVICTION_BATCH_MAX", 20)
        else:
            eviction_batch_max = int(prof.get("evictionBatchMax")) if isinstance(prof.get("evictionBatchMax"), (int, float)) else 20

        pin_env = _env_str("DM_PIN_TTL_SECONDS")
        if pin_env is not None:
            pin_ttl_seconds = _env_int("DM_PIN_TTL_SECONDS", 1800)
        else:
            pin_ttl_seconds = int(prof.get("pinTtlSeconds")) if isinstance(prof.get("pinTtlSeconds"), (int, float)) else 1800

        return {
            "enabled": enabled,
            "minFreeBytes": int(min_free_bytes),
            "maxDynamicBytes": int(max_dynamic_bytes),
            "evictionBatchMax": max(1, int(eviction_batch_max)),
            "pinTtlMs": max(0, int(pin_ttl_seconds) * 1000),
        }

    def _reconcile_lru_locked(self) -> None:
        # Recompute dynamic bytes used and drop entries whose files no longer exist.
        total = 0
        now = _now_ms()
        changed = False

        for dep_id in list(self._state.installed_dynamic):
            if dep_id not in self._state.lru:
                # Keep the installed set (we may not know the path), but we cannot evict/measure it.
                continue

        for dep_id, entry in list(self._state.lru.items()):
            if not isinstance(dep_id, str) or not dep_id:
                self._state.lru.pop(dep_id, None)
                changed = True
                continue
            if not isinstance(entry, dict):
                self._state.lru.pop(dep_id, None)
                self._state.installed_dynamic.discard(dep_id)
                changed = True
                continue
            dest_rel = entry.get("destRelativePath")
            if not isinstance(dest_rel, str) or not dest_rel:
                self._state.lru.pop(dep_id, None)
                self._state.installed_dynamic.discard(dep_id)
                changed = True
                continue
            try:
                path = safe_join(self.comfyui_dir, dest_rel)
            except Exception:
                self._state.lru.pop(dep_id, None)
                self._state.installed_dynamic.discard(dep_id)
                changed = True
                continue
            if not path.exists():
                self._state.lru.pop(dep_id, None)
                self._state.installed_dynamic.discard(dep_id)
                changed = True
                continue

            try:
                size = int(path.stat().st_size)
            except Exception:
                size = 0
            prev_size = entry.get("sizeBytes")
            if not isinstance(prev_size, int) or prev_size != size:
                entry["sizeBytes"] = size
                changed = True

            if not isinstance(entry.get("lastTouchedAtMs"), int):
                entry["lastTouchedAtMs"] = now
                changed = True

            self._state.installed_dynamic.add(dep_id)
            total += size

        self._dynamic_bytes_used = int(total)
        if changed:
            self._save_state()

    def _touch_dynamic_locked(self, dep_id: str, dest_rel: Optional[str]) -> None:
        now = _now_ms()
        entry = self._state.lru.get(dep_id)
        if not isinstance(entry, dict):
            entry = {}

        if dest_rel:
            entry["destRelativePath"] = dest_rel

        current_dest = entry.get("destRelativePath")
        size = 0
        exists = False
        if isinstance(current_dest, str) and current_dest:
            try:
                path = safe_join(self.comfyui_dir, current_dest)
                if path.exists():
                    exists = True
                    size = int(path.stat().st_size)
            except Exception:
                exists = False
                size = 0

        if not exists:
            # Don't create new LRU entries for missing files; touches should not mark deps installed.
            if dep_id in self._state.lru:
                entry["lastTouchedAtMs"] = now
            return

        prev_size = entry.get("sizeBytes")
        if not isinstance(prev_size, int):
            prev_size = 0

        entry["sizeBytes"] = size
        entry["lastTouchedAtMs"] = now
        self._state.lru[dep_id] = entry

        self._state.installed_dynamic.add(dep_id)
        self._state.installed_static.discard(dep_id)
        self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) + int(size) - int(prev_size))

    def _evict_dynamic_locked(self, required_free_bytes: int, protect: Set[str]) -> int:
        policy = self._dynamic_policy()
        if not policy.get("enabled"):
            return 0

        now = _now_ms()
        pinned: Set[str] = set(protect) | set(self._downloading)

        pin_ttl_ms = int(policy.get("pinTtlMs") or 0)
        if pin_ttl_ms > 0:
            for dep_id, entry in self._state.lru.items():
                if not isinstance(entry, dict):
                    continue
                touched = entry.get("lastTouchedAtMs")
                if isinstance(touched, int) and (now - touched) <= pin_ttl_ms:
                    pinned.add(dep_id)

        candidates: List[Tuple[int, str, str]] = []
        for dep_id, entry in self._state.lru.items():
            if dep_id in pinned:
                continue
            if not isinstance(entry, dict):
                continue
            dest_rel = entry.get("destRelativePath")
            if not isinstance(dest_rel, str) or not dest_rel:
                continue
            touched = entry.get("lastTouchedAtMs")
            touched_i = int(touched) if isinstance(touched, int) else 0
            candidates.append((touched_i, dep_id, dest_rel))

        candidates.sort(key=lambda t: (t[0], t[1]))

        freed = 0
        evicted = 0
        eviction_batch_max = int(policy.get("evictionBatchMax") or 20)

        for _, dep_id, dest_rel in candidates:
            if evicted >= eviction_batch_max:
                break

            stats = disk_stats(self.comfyui_dir)
            free_now = int(stats.get("freeBytes", 0))
            max_dynamic = int(policy.get("maxDynamicBytes") or 0)

            if free_now >= required_free_bytes and (max_dynamic <= 0 or self._dynamic_bytes_used <= max_dynamic):
                break

            try:
                path = safe_join(self.comfyui_dir, dest_rel)
            except Exception as e:
                logging.warning("Cannot evict %s (bad path %s): %s", dep_id, dest_rel, e)
                continue

            size = 0
            try:
                if path.exists():
                    size = int(path.stat().st_size)
                    path.unlink()
            except Exception as e:
                logging.warning("Failed to evict %s (%s): %s", dep_id, dest_rel, e)
                continue

            entry = self._state.lru.pop(dep_id, None) or {}
            prev_size = entry.get("sizeBytes") if isinstance(entry.get("sizeBytes"), int) else size
            self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) - int(prev_size))

            self._state.installed_dynamic.discard(dep_id)
            self._state.failed.discard(dep_id)
            freed += int(size)
            evicted += 1
            logging.info("Evicted dynamic dependency %s (%d bytes): %s", dep_id, size, dest_rel)

        if evicted > 0:
            self._save_state()

        return freed

    def _ensure_space_for_download(self, expected_size_bytes: int, dep_id: str) -> bool:
        policy = self._dynamic_policy()
        if not policy.get("enabled"):
            return False

        # Ensure we keep a minimum free-space floor after writing the new file.
        required_free = int(policy.get("minFreeBytes") or 0) + max(0, int(expected_size_bytes))
        required_free = max(0, required_free)

        did_evict = False
        with self._lock:
            self._reconcile_lru_locked()
            freed = self._evict_dynamic_locked(required_free_bytes=required_free, protect={dep_id})
            did_evict = freed > 0

        stats = disk_stats(self.comfyui_dir)
        free_now = int(stats.get("freeBytes", 0))
        if free_now < required_free:
            raise RuntimeError(f"Insufficient disk space: freeBytes={free_now} requiredFreeBytes={required_free}")

        return did_evict

    def _load_state(self) -> LocalState:
        try:
            raw = self.state_path.read_text("utf-8")
            data = json.loads(raw)
            installed_static = set(x for x in data.get("installed_static", []) if isinstance(x, str))
            installed_dynamic = set(x for x in data.get("installed_dynamic", []) if isinstance(x, str))
            failed = set(x for x in data.get("failed", []) if isinstance(x, str))
            lru_raw = data.get("lru") if isinstance(data, dict) else None
            lru: Dict[str, Dict[str, Any]] = {}
            if isinstance(lru_raw, dict):
                now = _now_ms()
                for dep_id, entry in lru_raw.items():
                    if not isinstance(dep_id, str) or not dep_id:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    dest_rel = entry.get("destRelativePath") or entry.get("path")
                    size = entry.get("sizeBytes") if isinstance(entry.get("sizeBytes"), int) else 0
                    touched = entry.get("lastTouchedAtMs") if isinstance(entry.get("lastTouchedAtMs"), int) else now
                    if isinstance(dest_rel, str) and dest_rel:
                        lru[dep_id] = {
                            "destRelativePath": dest_rel,
                            "sizeBytes": int(size) if size > 0 else 0,
                            "lastTouchedAtMs": int(touched),
                        }
            return LocalState(installed_static=installed_static, installed_dynamic=installed_dynamic, failed=failed, lru=lru)
        except Exception:
            return LocalState.empty()

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        data = {
            "installed_static": sorted(self._state.installed_static),
            "installed_dynamic": sorted(self._state.installed_dynamic),
            "failed": sorted(self._state.failed),
            "lru": self._state.lru,
            "updatedAtMs": _now_ms(),
        }
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), "utf-8")
        os.replace(str(tmp), str(self.state_path))

    def _register(self) -> None:
        instance_ip = self.instance_ip
        if not self.instance_id and not instance_ip:
            instance_ip = detect_public_ip()
            if instance_ip:
                logging.info("Detected public IP: %s", instance_ip)
            else:
                logging.warning("Could not detect public IP; set DM_INSTANCE_ID or DM_INSTANCE_IP for reliable registration.")

        url = f"{self.api_base_url}/dependencies/register"
        headers = {}
        if self.shared_secret:
            headers["X-DM-Secret"] = self.shared_secret
        logging.info(
            "Registering dependency agent: api=%s serverType=%s instanceId=%s instanceIp=%s secretSet=%s",
            self.api_base_url,
            self.server_type,
            self.instance_id or "-",
            instance_ip or "-",
            "yes" if self.shared_secret else "no",
        )

        body: Dict[str, Any] = {
            "serverType": self.server_type,
            "agentVersion": AGENT_VERSION,
        }
        if self.instance_id:
            body["instanceId"] = self.instance_id
        if instance_ip:
            body["instanceIp"] = instance_ip

        status, resp = api_json("POST", url, body=body, headers=headers, timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected register response: {status} {resp}")

        instance_id = resp.get("instanceId")
        agent_token = resp.get("agentToken")
        if not isinstance(instance_id, str) or not instance_id:
            raise RuntimeError(f"Register did not return instanceId: {resp}")
        if not isinstance(agent_token, str) or not agent_token:
            raise RuntimeError(f"Register did not return agentToken: {resp}")

        profile = resp.get("profile")
        if isinstance(profile, dict):
            self._profile = profile
        else:
            self._profile = {}

        self._resolved_instance_id = instance_id
        self._token = agent_token
        logging.info("Registered dependency agent: instanceId=%s", instance_id)

    def _post_status(self, item: Dict[str, Any], state: str, error: Optional[str] = None) -> None:
        if not self._resolved_instance_id:
            return
        url = f"{self.api_base_url}/dependencies/status"
        with self._lock:
            dynamic_bytes_used = int(self._dynamic_bytes_used)
        body: Dict[str, Any] = {
            "instanceId": self._resolved_instance_id,
            "itemId": item.get("itemId") or item.get("depId"),
            "depId": item.get("depId"),
            "op": item.get("op"),
            "state": state,
            "diskStats": disk_stats(self.comfyui_dir),
            "dynamicBytesUsed": dynamic_bytes_used,
        }
        if error:
            body["error"] = error[:500]
        api_json("POST", url, body=body, headers=self._headers(use_token=True, include_secret=False), timeout_seconds=30.0)

    def _heartbeat(self, queue_depth: Optional[int] = None) -> None:
        if not self._resolved_instance_id:
            return
        url = f"{self.api_base_url}/dependencies/heartbeat"
        with self._lock:
            self._reconcile_lru_locked()
            installed_static = sorted(self._state.installed_static)
            installed_dynamic = sorted(self._state.installed_dynamic)
            failed = sorted(self._state.failed)
            downloading = sorted(self._downloading)
            dynamic_bytes_used = int(self._dynamic_bytes_used)

        body: Dict[str, Any] = {
            "instanceId": self._resolved_instance_id,
            "installedStaticDepIds": installed_static,
            "installedDynamicDepIds": installed_dynamic,
            "downloadingDepIds": downloading,
            "failedDepIds": failed,
            "diskStats": disk_stats(self.comfyui_dir),
            "dynamicBytesUsed": dynamic_bytes_used,
        }
        if queue_depth is not None:
            body["queueDepth"] = int(queue_depth)

        api_json("POST", url, body=body, headers=self._headers(use_token=True, include_secret=False), timeout_seconds=30.0)
        self._last_heartbeat_ms = _now_ms()

    def _fetch_queue(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self._resolved_instance_id:
            return []
        instance_id = self._resolved_instance_id
        url = f"{self.api_base_url}/dependencies/queue?instanceId={urllib.parse.quote(instance_id)}&limit={int(limit)}"
        status, resp = api_json("GET", url, headers=self._headers(use_token=True, include_secret=False), timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected queue response: {status} {resp}")
        items = resp.get("items", [])
        if not isinstance(items, list):
            return []
        out: List[Dict[str, Any]] = []
        for it in items:
            if isinstance(it, dict):
                out.append(it)
        return out

    def _resolve_auth_header(self, auth: Optional[str]) -> Optional[str]:
        a = (auth or "none").lower()
        if a == "none":
            return None
        if a == "hf_token":
            if not self.hf_token:
                raise RuntimeError("Missing HF_TOKEN for dependency download requiring hf_token")
            return f"Bearer {self.hf_token}"
        if a == "civitai_token":
            if not self.civitai_token:
                raise RuntimeError("Missing CIVITAI_TOKEN for dependency download requiring civitai_token")
            return f"Bearer {self.civitai_token}"
        raise RuntimeError(f"Unsupported auth type: {auth}")

    def _download_item(self, item: Dict[str, Any]) -> None:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            raise RuntimeError("Queue item missing depId")

        resolved = item.get("resolved")
        if not isinstance(resolved, dict):
            raise RuntimeError("Queue item missing resolved download info (resolved=null)")

        url = resolved.get("url")
        dest_rel = resolved.get("destRelativePath")
        kind = resolved.get("kind")
        sha256_expected = resolved.get("sha256")
        expected_size = resolved.get("expectedSizeBytes")
        expected_size_bytes = int(expected_size) if isinstance(expected_size, (int, float)) and expected_size > 0 else 0

        if not isinstance(url, str) or not url:
            raise RuntimeError("Resolved dependency missing url")
        if not isinstance(dest_rel, str) or not dest_rel:
            raise RuntimeError("Resolved dependency missing destRelativePath")

        dest_abs = safe_join(self.comfyui_dir, dest_rel)
        dest_abs.parent.mkdir(parents=True, exist_ok=True)
        partial = dest_abs.with_suffix(dest_abs.suffix + ".partial")

        auth = resolved.get("auth")
        auth_header = self._resolve_auth_header(auth if isinstance(auth, str) else None)

        # Fast path: if the file already exists (e.g., legacy provisioning), treat as installed.
        if dest_abs.exists():
            if isinstance(sha256_expected, str) and sha256_expected:
                actual_existing = sha256_file(dest_abs)
                if actual_existing.lower() != sha256_expected.lower():
                    logging.warning(
                        "Existing file sha256 mismatch for %s; re-downloading. expected=%s got=%s path=%s",
                        dep_id,
                        sha256_expected,
                        actual_existing,
                        str(dest_abs),
                    )
                else:
                    with self._lock:
                        if isinstance(kind, str) and kind.lower() == "dynamic":
                            self._touch_dynamic_locked(dep_id, dest_rel)
                        else:
                            # If this dep was previously dynamic, drop it from the LRU index.
                            prev = self._state.lru.pop(dep_id, None) or {}
                            prev_size = prev.get("sizeBytes") if isinstance(prev.get("sizeBytes"), int) else 0
                            self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) - int(prev_size))
                            self._state.installed_dynamic.discard(dep_id)
                            self._state.installed_static.add(dep_id)
                        self._state.failed.discard(dep_id)
                        self._downloading.discard(dep_id)
                        self._save_state()
                    return
            else:
                with self._lock:
                    if isinstance(kind, str) and kind.lower() == "dynamic":
                        self._touch_dynamic_locked(dep_id, dest_rel)
                    else:
                        prev = self._state.lru.pop(dep_id, None) or {}
                        prev_size = prev.get("sizeBytes") if isinstance(prev.get("sizeBytes"), int) else 0
                        self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) - int(prev_size))
                        self._state.installed_dynamic.discard(dep_id)
                        self._state.installed_static.add(dep_id)
                    self._state.failed.discard(dep_id)
                    self._downloading.discard(dep_id)
                    self._save_state()
                return

        did_evict = self._ensure_space_for_download(expected_size_bytes, dep_id)
        if did_evict and _now_ms() - int(self._last_heartbeat_ms) >= 2000:
            # Eviction changes inventory; push an early heartbeat to reduce scheduling race windows.
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass

        try:
            http_download(
                url=url,
                dest_partial=partial,
                auth_header=auth_header,
                timeout_seconds=60.0,
                allowed_domains=self.allowed_domains,
                verbose=self.verbose_progress,
            )

            if isinstance(sha256_expected, str) and sha256_expected:
                actual = sha256_file(partial)
                if actual.lower() != sha256_expected.lower():
                    raise RuntimeError(f"sha256 mismatch for {dep_id}: expected {sha256_expected}, got {actual}")
        except Exception:
            try:
                if partial.exists():
                    partial.unlink()
            except Exception:
                pass
            raise

        os.replace(str(partial), str(dest_abs))

        should_heartbeat = False
        with self._lock:
            if isinstance(kind, str) and kind.lower() == "dynamic":
                self._touch_dynamic_locked(dep_id, dest_rel)
            else:
                prev = self._state.lru.pop(dep_id, None) or {}
                prev_size = prev.get("sizeBytes") if isinstance(prev.get("sizeBytes"), int) else 0
                self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) - int(prev_size))
                self._state.installed_dynamic.discard(dep_id)
                self._state.installed_static.add(dep_id)
            self._state.failed.discard(dep_id)
            self._downloading.discard(dep_id)

            policy = self._dynamic_policy()
            if policy.get("enabled"):
                freed = self._evict_dynamic_locked(required_free_bytes=int(policy.get("minFreeBytes") or 0), protect={dep_id})
                should_heartbeat = freed > 0

            self._save_state()

        if should_heartbeat and _now_ms() - int(self._last_heartbeat_ms) >= 2000:
            # This will update installedDynamicDepIds after eviction.
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass

    def _touch_item(self, item: Dict[str, Any]) -> None:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            return

        resolved = item.get("resolved")
        kind = None
        dest_rel = None
        if isinstance(resolved, dict):
            kind = resolved.get("kind")
            dest_rel = resolved.get("destRelativePath")

        with self._lock:
            self._state.failed.discard(dep_id)
            is_dynamic = (isinstance(kind, str) and kind.lower() == "dynamic") or (dep_id in self._state.installed_dynamic)
            if is_dynamic:
                self._touch_dynamic_locked(dep_id, dest_rel if isinstance(dest_rel, str) else None)
            self._save_state()

    def _process_item(self, item: Dict[str, Any]) -> None:
        op = item.get("op")
        dep_id = item.get("depId")
        item_id = item.get("itemId")
        if not isinstance(op, str):
            op = ""
        if not isinstance(item_id, str):
            item_id = ""
        if not isinstance(dep_id, str):
            dep_id = ""

        if op not in ("download", "touch"):
            self._post_status(item, "failed", error=f"Unknown op: {op}")
            return

        if op == "download":
            with self._lock:
                if dep_id:
                    self._downloading.add(dep_id)
            self._post_status(item, "running")

            # Small retry loop to handle transient failures.
            last_err: Optional[str] = None
            for attempt in range(1, 4):
                try:
                    self._download_item(item)
                    self._post_status(item, "succeeded")
                    # Ensure backend inventory is updated quickly so reserved jobs can proceed.
                    if _now_ms() - int(self._last_heartbeat_ms) >= 2000:
                        try:
                            self._heartbeat(queue_depth=None)
                        except Exception:
                            pass
                    return
                except Exception as e:
                    last_err = str(e)
                    logging.warning("Download failed (attempt %d/3) itemId=%s depId=%s: %s", attempt, item_id, dep_id, last_err)
                    _sleep_with_jitter(5.0 * attempt)

            with self._lock:
                if dep_id:
                    self._state.failed.add(dep_id)
                    self._downloading.discard(dep_id)
                    self._save_state()
            self._post_status(item, "failed", error=last_err or "download failed")
            return

        # touch
        self._post_status(item, "running")
        try:
            self._touch_item(item)
            self._post_status(item, "succeeded")
        except Exception as e:
            self._post_status(item, "failed", error=str(e))

    def run_forever(self) -> None:
        self.validate_env()

        backoff = 2.0
        while not self._stop.is_set():
            try:
                self._register()
                break
            except ApiError as e:
                if e.status == 401:
                    logging.error(
                        "Register failed: unauthorized (check DEPENDENCY_MANAGER_SHARED_SECRET and FCS_API_BASE_URL)."
                    )
                    if e.body:
                        logging.error("Register response body: %s", e.body)
                else:
                    logging.error("Register failed: %s", e)
                _sleep_with_jitter(backoff)
                backoff = min(60.0, backoff * 1.5)
            except Exception as e:
                logging.error("Register failed: %s", e)
                _sleep_with_jitter(backoff)
                backoff = min(60.0, backoff * 1.5)

        if not self._resolved_instance_id or not self._token:
            raise SystemExit("Failed to register; exiting.")

        logging.info("ComfyUI dir: %s", str(self.comfyui_dir))
        logging.info("State file: %s", str(self.state_path))
        logging.info("Allowed download domains: %s", ",".join(sorted(self.allowed_domains)))
        logging.info("Polling every %.1fs, heartbeat every %.1fs, max_parallel=%d", self.poll_seconds, self.heartbeat_seconds, self.max_parallel)
        policy = self._dynamic_policy()
        if policy.get("enabled"):
            logging.info(
                "Dynamic eviction enabled: minFreeBytes=%d maxDynamicBytes=%d evictionBatchMax=%d pinTtlMs=%d",
                int(policy.get("minFreeBytes") or 0),
                int(policy.get("maxDynamicBytes") or 0),
                int(policy.get("evictionBatchMax") or 0),
                int(policy.get("pinTtlMs") or 0),
            )
        else:
            logging.info("Dynamic eviction disabled (set profile.dynamicPolicy.enabled or DM_DYNAMIC_EVICTION_ENABLED=1)")

        executor = ThreadPoolExecutor(max_workers=self.max_parallel)
        inflight: Set[Future[None]] = set()

        while not self._stop.is_set():
            try:
                # Heartbeat (coarse).
                now = _now_ms()
                if now - self._last_heartbeat_ms >= int(self.heartbeat_seconds * 1000):
                    self._heartbeat(queue_depth=None)

                # Fetch queue and dispatch work.
                items = self._fetch_queue(limit=25)
                queue_depth = len(items)
                if queue_depth > 0 and now - self._last_heartbeat_ms >= int(5 * 1000):
                    # Opportunistically include queueDepth without waiting full heartbeat interval.
                    self._heartbeat(queue_depth=queue_depth + len(inflight))

                # Clean up completed futures.
                done = {f for f in inflight if f.done()}
                inflight -= done
                for f in done:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error("Unhandled worker error: %s", e)

                # Dispatch new work up to capacity.
                for item in items:
                    if self._stop.is_set():
                        break
                    if len(inflight) >= self.max_parallel:
                        break
                    inflight.add(executor.submit(self._process_item, item))

                _sleep_with_jitter(self.poll_seconds)
            except ApiError as e:
                # Unauthorized usually means token rotated or instance doc missing; re-register.
                if e.status in (401, 403):
                    logging.warning("Unauthorized (status=%d); re-registering.", e.status)
                    try:
                        self._register()
                    except Exception as re:
                        logging.error("Re-register failed: %s", re)
                else:
                    logging.error("API error: %s", e)
                _sleep_with_jitter(5.0)
            except Exception as e:
                logging.error("Main loop error: %s", e)
                _sleep_with_jitter(5.0)

        executor.shutdown(wait=False, cancel_futures=True)
        logging.info("Dependency agent stopped.")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    agent = DependencyAgent()

    def _handle_signal(_signum: int, _frame: Any) -> None:
        logging.info("Signal received; stopping...")
        agent.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        agent.run_forever()
        return 0
    except SystemExit as e:
        logging.error(str(e))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

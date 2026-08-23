#!/usr/bin/env python3
"""
Furgen Content Server - Dependency Manager Agent (Vast.ai instances)

This is a single-file agent intended to run on each ComfyUI instance.
It polls the backend for dependency queue items and downloads missing artifacts
into the ComfyUI workspace, reporting status + inventory back to the backend.

Backend endpoints used (relative to FCS_API_BASE_URL, usually the skinny coordinationApi endpoint):
  Legacy dependency channel (still supported for backwards compatibility):
  - POST   /dependencies/register
  - GET    /dependencies/queue?instanceId=...&limit=...
  - POST   /dependencies/status
  - POST   /dependencies/heartbeat

  Agent control channel (v1 pull execution):
  - POST   /agent/register
  - GET    /agent/queue?instanceId=...&limit=...&waitSec=...
  - POST   /agent/ack
  - POST   /agent/event
  - POST   /agent/heartbeat
  - POST   /agent/url-refresh

Required environment variables on the instance:
  - FCS_API_BASE_URL   e.g. https://us-central1-<projectId>.cloudfunctions.net/coordinationApi
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
  - MAX_PARALLEL_DOWNLOADS  (default: 3)
  - DM_STATE_PATH           (default: $WORKSPACE/dependency_agent_state.json)
  - DM_ALLOWED_DOMAINS      (comma-separated allowlist for dependency/model downloads; default: huggingface.co,hf.co,civitai.red,civitai.com)
  - DM_INPUT_ALLOWED_DOMAINS (optional comma-separated allowlist for job input/prefetch downloads; default: allow all)
  - DM_DOWNLOAD_TOOL             (default: auto; options: auto, wget, python, aria2; auto prefers aria2c when installed)
  - DM_DOWNLOAD_TIMEOUT_SECONDS (socket timeout for downloads; default: 300)
  - DM_DOWNLOAD_CHUNK_MIB        (download read chunk size in MiB; default: 1)
  - DM_DOWNLOAD_DEBUG            (enable extra download diagnostics; default: false)
  - DM_DYNAMIC_EVICTION_ENABLED   (override profile.dynamicPolicy.enabled; default: false)
  - DM_DYNAMIC_MIN_FREE_BYTES     (override profile.dynamicPolicy.minFreeBytes; supports 10GB/500MiB)
  - DM_DYNAMIC_MAX_BYTES          (override profile.dynamicPolicy.maxDynamicBytes; supports 50GB/2TiB)
  - DM_EVICTION_BATCH_MAX         (override profile.dynamicPolicy.evictionBatchMax; default: 20)
  - DM_PIN_TTL_SECONDS            (do not evict deps touched within this window; default: 1800)
  - DM_AGENT_CONTROL_ENABLED      (enable /agent/* control channel; default: true)
  - DM_INSTANCE_BOOTSTRAP_TOKEN   (instance bootstrap token for /agent/register when required)
  - DM_AGENT_POLL_SECONDS         (poll cadence for /agent/queue; default: 2)
  - DM_AGENT_HEARTBEAT_SECONDS    (active heartbeat cadence for /agent/heartbeat; default: 8)
  - DM_AGENT_IDLE_HEARTBEAT_SECONDS (idle heartbeat cadence when RTDB signal wait is healthy; default: active cadence)
  - DM_AGENT_QUEUE_WAIT_SEC       (long-poll waitSec for /agent/queue; default: 2)
  - DM_AGENT_RTDB_SIGNAL_WAIT_ENABLED (when RTDB coordination is healthy, wait locally for queue signals; default: true)
  - DM_AGENT_RTDB_SIGNAL_SAFETY_MIN_SECONDS (minimum fallback /agent/queue probe when signal stream is healthy; default: 900)
  - DM_AGENT_RTDB_QUEUE_CLAIM_ENABLED (allow server-gated direct RTDB queue claims; default: true)
  - DM_AGENT_RTDB_LEASE_HEARTBEAT_ENABLED (allow server-gated active lease heartbeats through RTDB; default: true)
  - DM_COORDINATION_RUNTIME_FULL_SYNC_SECONDS (full RTDB runtime mirror inventory cadence; default: 900)
  - DM_AGENT_WAITING_DEPS_EVENT_SECONDS (waiting_dependencies event cadence; default: 60)
  - DM_AGENT_DEPENDENCY_WAIT_POLL_SECONDS (dependency readiness poll while waiting; default: 0.5)
  - DM_AGENT_PROGRESS_EVENT_SECONDS (execution_progress event cadence; default: 60)
  - DM_AGENT_API_RETRY_ATTEMPTS    (agent API retry attempts for transient network/5xx/401; default: 5)
  - DM_AGENT_API_RETRY_BASE_SECONDS (initial agent API retry backoff; default: 1)
  - DM_AGENT_API_RETRY_MAX_SECONDS (max agent API retry backoff; default: 20)
  - DM_AGENT_TERMINAL_EVENT_RETRY_ATTEMPTS (extra retries for terminal job events; default: 8)
  - DM_AGENT_MAX_UPLOAD_WORKERS    (local output upload worker cap; default: max(4, exec*2))
  - DM_LOCAL_COMFY_BASE_URL       (local ComfyUI URL; default: http://127.0.0.1:8188)
  - DM_COMFY_NODE_TIMING_ENABLED  (capture native Comfy node-boundary timings; default: true on video_gen_v3/video_gen_v4)
  - DM_COMFY_NODE_TIMING_MAX_ROWS (maximum persisted slow-node rows per job; default/max: 64)
  - DM_LOCAL_READINESS_FILE       (readiness marker file in Comfy input dir; default: provisioning_complete.txt)
  - DM_VIDEO_GEN_V2_BOOTSTRAP_GATE_WAIT_SECONDS (max wait for the managed video bootstrap gate; default: 1800)
  - DM_VIDEO_GEN_V2_BOOTSTRAP_COMFY_WAIT_SECONDS (max post-gate wait for managed Comfy startup; default: 300)
  - DM_AGENT_MAX_EXEC_WORKERS     (local execute_job worker cap; default: 2)
  - DM_GPU_COORDINATOR_URL        (loopback GPU coordinator base URL; unset disables coordinator integration)
  - DM_GPU_COORDINATOR_REQUIRED   (fail closed when coordinator discovery is unavailable; default: false)
  - DM_GPU_COORDINATOR_TOKEN      (optional bearer token for loopback coordinator requests)
  - DM_GPU_COORDINATOR_LEASE_TTL_SECONDS (renewable coordinator lease TTL; default: 60)
  - GPU_ADMISSION_MODE           (off, shadow, or enforcing; default: off)
  - DM_GPU_ADMISSION_MAX_DEPTH   (shared foreground queue bound; default: 64)
  - DM_COMFY_FULL_TRIM_MEM_AVAILABLE_GIB (full Comfy cache trim threshold; default: 24)
  - DM_COMFY_FULL_TRIM_MEMORY_PSI_AVG10 (full Comfy cache trim PSI avg10 threshold; default: 5)
  - DM_MINING_ONLY                (set to 1 for PRL mining-only instances; skips Comfy probes and job execution)
  - DM_INPUT_CACHE_DIR            (persistent remote-input cache dir; default: $WORKSPACE/.dm_input_cache)
  - DM_INPUT_CACHE_MAX_BYTES      (max remote-input cache size; default: 20GiB)
  - DM_AGENT_SELF_UPDATE_ENABLED  (allow backend-directed in-place script updates; default: true)
  - DM_AGENT_SELF_UPDATE_ALLOW_DOWNGRADE (allow backend-directed downgrades/rollbacks; default: false)
  - DM_AGENT_SELF_UPDATE_RETRY_SECONDS (retry delay after failed update attempts; default: 300)
  - DM_EXISTING_FILE_STABLE_SECONDS (minimum age before existing files without size/hash metadata are trusted; default: 120)

Queue item expectations:
  - The backend should include a `resolved` object for download items with:
      { url, auth, destRelativePath, sha256?, expectedSizeBytes?, kind? }
  - For touch/delete items, `resolved` should include at least:
      { destRelativePath, kind }
    The backend in this repo enriches /dependencies/queue responses accordingly.
"""

from __future__ import annotations

import ast
import base64
from collections import deque
import hashlib
import http.client
import ipaddress
import json
import logging
import math
import mimetypes
import os
import random
import re
import shlex
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


AGENT_VERSION = "dm-agent-py/0.10.154"
RUNTIME_ENV_DELIVERY_KEYS = frozenset(("HF_TOKEN", "CIVITAI_TOKEN", "FURGEN_H3_ATTENTION_BACKEND"))
VIDEO_GEN_V2_FURGENPUB_COMMIT = "f46d81937e578aaf6f2674cd5deb7982ea09b4bb"
VIDEO_GEN_V2_FURGENPUB_RAW_BASE_URL = (
    f"https://raw.githubusercontent.com/Dodzilla/FurgenPub/{VIDEO_GEN_V2_FURGENPUB_COMMIT}/docker/support"
)
MAX_AGENT_ERROR_MESSAGE_CHARS = 4000
# Flags that control ComfyUI memory behaviour rather than transport/attention.
# A restart that rebuilds COMFYUI_ARGS must carry these over from the
# provisioning env; dropping them changes how the workload allocates VRAM.
PRESERVED_COMFY_MEMORY_FLAGS = (
    "--disable-async-offload",
    "--disable-pinned-memory",
    "--disable-cuda-malloc",
    "--cache-none",
    "--high-ram",
    "--fast-disk",
    "--disable-smart-memory",
)
PRESERVED_COMFY_MEMORY_VALUE_FLAGS = (
    "--vram-headroom",
    "--reserve-vram",
    "--cache-lru",
)
PRESERVED_COMFY_MEMORY_TWO_VALUE_FLAGS = (
    "--cache-ram",
)


def _preserved_comfy_memory_flags(source_args: str) -> str:
    """Return the memory-related flags from source_args, as a prefixed string."""
    tokens = str(source_args or "").split()
    preserved: List[str] = []
    for index, token in enumerate(tokens):
        if token in PRESERVED_COMFY_MEMORY_FLAGS:
            preserved.append(token)
            continue
        if token in PRESERVED_COMFY_MEMORY_VALUE_FLAGS and index + 1 < len(tokens):
            value = tokens[index + 1]
            if not value.startswith("--"):
                preserved.extend([token, value])
            continue
        if token in PRESERVED_COMFY_MEMORY_TWO_VALUE_FLAGS and index + 2 < len(tokens):
            values = tokens[index + 1:index + 3]
            if all(not value.startswith("--") for value in values):
                preserved.extend([token, *values])
    return f" {' '.join(preserved)}" if preserved else ""
MAX_COMFY_NODE_TIMING_ROWS = 64
MAX_COMFY_WS_FRAME_BYTES = 32 * 1024 * 1024
RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
NON_RETRYABLE_QUEUE_STATES = {"cancelled", "canceled", "succeeded", "completed", "deleted"}
RTDB_AGENT_NON_TERMINAL_QUEUE_STATES = {
    "queued",
    "leased",
    "ready",
    "waiting_dependencies",
    "running",
    "uploading",
    "cancel_requested",
    "retrying",
}


def _available_dependency_slots(max_parallel: int, inflight_count: int) -> int:
    return max(0, max(1, int(max_parallel)) - max(0, int(inflight_count)))
RIFE_VFI_ZIP_URLS = [
    "https://huggingface.co/hzwer/RIFE/resolve/main/RIFEv4.26_0921.zip",
    "https://hf-mirror.com/hzwer/RIFE/resolve/main/RIFEv4.26_0921.zip",
]
RIFE_VFI_ZIP_SIZE_BYTES = 22869906
RIFE_VFI_ZIP_SHA256 = "1fa9b9cda3d9b8c3e301359e2595960902f97bf926c08598b0e9957a3f3f760e"
RIFE_VFI_FLOWNET_SIZE_BYTES = 24636301
PRL_MINER_TRANSIENT_STOP_REASONS = {"execute_job"}
PRL_MINER_PAUSE_MODES = {"stop_start", "suspend_resume", "keep_running"}
DEFAULT_PRL_MINER_PAUSE_MODE = "stop_start"
PRL_MINER_KINDS = {"alpha_miner", "srbminer_multi"}
DEFAULT_PRL_MINER_KIND = "alpha_miner"
PRL_MINER_PACKAGE_TYPES = {"binary", "tar_gz"}
DEFAULT_PRL_MINER_PACKAGE_TYPE = "binary"
AGENT_GPU_BLOCKING_STAGES = {"ready", "preparing_prompt", "executing"}
PRL_MINER_SHARE_SIGNAL_RE = re.compile(
    r"\b(accepted|rejected|share submission returned error|stratum error response|dropped reason=|action=drop_share|action=reconnect_drop_ambiguous_share)\b",
    re.IGNORECASE,
)
PRL_MINER_POOL_ACTIVITY_RE = re.compile(
    r"\b(?:component=share\s+submitted|share\s+submitted|component=pool\s+(?:job_update|difficulty_set)|pool\s+(?:job_update|difficulty_set))\b",
    re.IGNORECASE,
)
PRL_MINER_POOL_ERROR_RE = re.compile(
    r"(stratum (?:connection closed|recv timeout|recv\(\) failed|send\(\) failed|send\(\) timed out)|pool did not accept|share submission returned error)",
    re.IGNORECASE,
)
PRL_MINER_SHARE_COUNTER_RE = re.compile(
    r"\b(?P<name>accepted|accepts?|submitted|submits?|rejected|rejects?|invalid|stale|errors?|share_errors?)"
    r"(?:[_\s-]*shares?)?\s*[=:]\s*(?P<value>\d+)\b",
    re.IGNORECASE,
)
PRL_SINKHOLE_IPS = {"146.112.61.110", "::ffff:146.112.61.110"}
PRL_CLEAN_RESOLVERS = ("1.1.1.1", "8.8.8.8")
HASHRATE_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[KMGT]?H)\s*/?\s*s(?:ec)?", re.IGNORECASE)
ALPHA_HASHRATE_RE = re.compile(r"\bhashrate_th_s=(?P<value>\d+(?:\.\d+)?)\b", re.IGNORECASE)
SRB_CURRENT_HASHRATE_RE = re.compile(
    r"(?:^|[\r\n])[^\r\n]*?(?:\bHashrate|\bTotal:)\s+"
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[KMGT]?H)\s*/?\s*s(?:ec)?",
    re.IGNORECASE,
)


def _hashrate_to_hps(value: float, unit: str) -> float:
    unit = str(unit or "").strip().upper()
    multiplier = {
        "H": 1.0,
        "KH": 1_000.0,
        "MH": 1_000_000.0,
        "GH": 1_000_000_000.0,
        "TH": 1_000_000_000_000.0,
    }.get(unit, 1.0)
    return float(value) * multiplier


def _format_hps(hps: float) -> str:
    value = float(hps)
    for suffix, divisor in (("TH/s", 1_000_000_000_000.0), ("GH/s", 1_000_000_000.0), ("MH/s", 1_000_000.0), ("KH/s", 1_000.0)):
        if abs(value) >= divisor:
            return f"{value / divisor:.2f} {suffix}"
    return f"{value:.2f} H/s"


def _parse_latest_hashrate_from_text(text: str) -> Tuple[Optional[float], str]:
    preferred: List[Tuple[int, float]] = []
    for match in ALPHA_HASHRATE_RE.finditer(text or ""):
        try:
            preferred.append((match.start(), float(match.group("value")) * 1_000_000_000_000.0))
        except Exception:
            continue
    # SRBMiner 3.5.x prints current/aggregate hashrate first, followed by
    # 1/6/12-hour averages. Those longer averages are legitimately 0 H/s
    # after a fresh start, so do not let the final one overwrite the current
    # rate. Older SRBMiner releases label the aggregate row as "Total:".
    for match in SRB_CURRENT_HASHRATE_RE.finditer(text or ""):
        try:
            preferred.append((match.start(), _hashrate_to_hps(float(match.group("value")), str(match.group("unit")))))
        except Exception:
            continue
    if preferred:
        latest_hps = max(preferred, key=lambda item: item[0])[1]
        return latest_hps, _format_hps(latest_hps)

    latest_hps: Optional[float] = None
    for match in HASHRATE_RE.finditer(text or ""):
        try:
            latest_hps = _hashrate_to_hps(float(match.group("value")), str(match.group("unit")))
        except Exception:
            continue
    if latest_hps is None:
        return None, ""
    return latest_hps, _format_hps(latest_hps)


def _parse_prl_miner_log_signals(text: str) -> Dict[str, Any]:
    lower = (text or "").lower()
    accepted_events = len(re.findall(r"\baccepted\b", lower))
    submitted_events = len(re.findall(r"\bshare\s+submitted\b", lower))
    rejected_events = len(re.findall(r"\brejected\b", lower))
    share_errors = len(PRL_MINER_POOL_ERROR_RE.findall(text or ""))
    share_signals = len(PRL_MINER_SHARE_SIGNAL_RE.findall(text or ""))
    pool_activity = len(PRL_MINER_POOL_ACTIVITY_RE.findall(text or ""))
    counter_values = {
        "accepted": None,
        "submitted": None,
        "rejected": None,
        "shareErrors": None,
    }
    for match in PRL_MINER_SHARE_COUNTER_RE.finditer(text or ""):
        name = match.group("name").lower().replace("-", "_")
        try:
            value = int(match.group("value"))
        except Exception:
            continue
        if name.startswith("accept"):
            counter_values["accepted"] = value
        elif name.startswith("submit"):
            counter_values["submitted"] = value
        elif name.startswith("reject") or name in ("invalid", "stale"):
            counter_values["rejected"] = value
        else:
            counter_values["shareErrors"] = value
    submitted = int(counter_values["submitted"]) if counter_values["submitted"] is not None else submitted_events
    rejected = int(counter_values["rejected"]) if counter_values["rejected"] is not None else rejected_events + share_errors
    if counter_values["shareErrors"] is not None:
        share_errors = int(counter_values["shareErrors"])
        rejected = max(rejected, share_errors)
    if counter_values["accepted"] is not None:
        accepted = int(counter_values["accepted"])
    else:
        accepted = max(accepted_events, max(0, submitted - rejected))
    return {
        "accepted": accepted,
        "submitted": submitted,
        "rejected": rejected,
        "shareErrors": share_errors,
        "hasShareSignal": share_signals > 0 or submitted > 0 or pool_activity > 0,
        "poolHealthy": accepted > 0 or submitted > 0 or pool_activity > 0,
    }


def _normalize_prl_pause_mode(value: Any) -> str:
    cleaned = str(value or "").strip().lower().replace("-", "_")
    return cleaned if cleaned in PRL_MINER_PAUSE_MODES else DEFAULT_PRL_MINER_PAUSE_MODE


def _normalize_prl_miner_kind(value: Any) -> str:
    cleaned = str(value or "").strip().lower().replace("-", "_")
    return cleaned if cleaned in PRL_MINER_KINDS else DEFAULT_PRL_MINER_KIND


def _normalize_prl_miner_package_type(value: Any) -> str:
    cleaned = str(value or "").strip().lower().replace("-", "_")
    return cleaned if cleaned in PRL_MINER_PACKAGE_TYPES else DEFAULT_PRL_MINER_PACKAGE_TYPE


def _normalize_archive_member_path(value: Any) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    raw = re.sub(r"/+", "/", raw).lstrip("./")
    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        return ""
    return "/".join(parts)[:500]


def _strip_stratum_scheme(pool_url: str) -> str:
    cleaned = str(pool_url or "").strip()
    return re.sub(r"^stratum\+(?:tcp|ssl)://", "", cleaned, flags=re.IGNORECASE)


def _pool_safe_worker(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", str(value or ""))
    return cleaned[:120]


class PrlNetworkPreflightError(RuntimeError):
    def __init__(self, message: str, diagnostics: Dict[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


def _normalize_download_urls(primary_url: str, alternate_urls: Any = None) -> List[str]:
    urls: List[str] = []

    def add(value: Any) -> None:
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in urls:
            urls.append(cleaned)

    if isinstance(alternate_urls, list):
        for row in alternate_urls:
            add(row)
    elif isinstance(alternate_urls, str):
        add(alternate_urls)
    add(primary_url)
    return urls


def _normalize_prl_static_difficulty(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, (str, int, float)):
        cleaned = str(value).strip()
        return cleaned[:64] if cleaned else ""
    return ""


def _clean_prl_payload_string(value: Any, max_length: int) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()[:max_length]


def _normalize_prl_payload_float(value: Any, minimum: float, maximum: float) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed) or parsed < minimum or parsed > maximum:
        return None
    return round(parsed, 3)


def _normalize_prl_payload_int(value: Any, fallback: int, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        return fallback
    try:
        parsed = int(value)
    except Exception:
        return fallback
    return max(minimum, min(maximum, parsed))


def _host_from_url(raw_url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(str(raw_url or "").strip())
        return str(parsed.hostname or "").strip()
    except Exception:
        return ""


def _resolve_host_ips(host: str) -> List[str]:
    if not host:
        return []
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except Exception:
        return []
    out: List[str] = []
    for info in infos:
        try:
            ip = str(info[4][0])
        except Exception:
            continue
        if ip and ip not in out:
            out.append(ip)
    return out


def _tls_issuer_for_host(host: str, timeout_seconds: float = 5.0) -> str:
    if not host:
        return ""
    try:
        context = ssl._create_unverified_context()
        with socket.create_connection((host, 443), timeout=timeout_seconds) as sock:
            with context.wrap_socket(sock, server_hostname=host) as tls:
                cert = tls.getpeercert()
                if not cert:
                    der_cert = tls.getpeercert(binary_form=True)
                    if der_cert:
                        tmp_name = ""
                        try:
                            with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as tmp:
                                tmp.write(ssl.DER_cert_to_PEM_cert(der_cert))
                                tmp_name = tmp.name
                            cert = ssl._ssl._test_decode_cert(tmp_name)
                        finally:
                            if tmp_name:
                                try:
                                    os.unlink(tmp_name)
                                except Exception:
                                    pass
        issuer_parts = cert.get("issuer") if isinstance(cert, dict) else None
        if not issuer_parts:
            return ""
        rows: List[str] = []
        for part in issuer_parts:
            if not isinstance(part, tuple):
                continue
            for key, value in part:
                rows.append(f"{key}={value}")
        return ", ".join(rows)[:500]
    except Exception as exc:
        return f"error:{str(exc)[:220]}"


def _is_prl_sinkhole_ips(ips: List[str]) -> bool:
    """Treat any non-public answer for a public PRL endpoint as blocked.

    Some providers redirect blocked download hosts to loopback or RFC1918
    addresses instead of the known Cisco Umbrella sinkhole. Those answers must
    trigger the same repair path; otherwise the miner endlessly retries a local
    connection and remains invisible to the pool.
    """
    if not ips:
        return True
    for raw_ip in ips:
        cleaned = str(raw_ip or "").strip().split("%", 1)[0]
        if cleaned in PRL_SINKHOLE_IPS:
            return True
        try:
            if not ipaddress.ip_address(cleaned).is_global:
                return True
        except ValueError:
            return True
    return False


def _is_cisco_umbrella_issuer(issuer: str) -> bool:
    return "cisco umbrella" in str(issuer or "").lower()


def _write_clean_resolv_conf() -> Tuple[bool, str]:
    path = Path("/etc/resolv.conf")
    try:
        existing = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        desired = "\n".join([*(f"nameserver {resolver}" for resolver in PRL_CLEAN_RESOLVERS), "options timeout:2 attempts:2", ""])
        if existing == desired:
            return True, "already_clean"
        backup = Path(f"/etc/resolv.conf.fcs-prl-preflight.{int(time.time())}.bak")
        try:
            if path.exists():
                shutil.copy2(str(path), str(backup))
        except Exception:
            pass
        path.write_text(desired, encoding="utf-8")
        return True, "updated"
    except Exception as exc:
        return False, str(exc)[:300]


def _remove_blocked_prl_host_overrides(
    hosts: Iterable[str],
    hosts_path: Path = Path("/etc/hosts"),
) -> Tuple[bool, str]:
    """Remove only target host aliases mapped to non-public addresses.

    Preserve localhost and unrelated aliases even when they share the same
    hosts-file line. This is deliberately narrower than rewriting /etc/hosts.
    """
    targets = {str(host or "").strip().lower().rstrip(".") for host in hosts if str(host or "").strip()}
    if not targets:
        return True, "no_targets"
    try:
        if not hosts_path.exists():
            return True, "missing"
        existing = hosts_path.read_text(encoding="utf-8", errors="replace")
        next_lines: List[str] = []
        removed: List[str] = []
        for original_line in existing.splitlines():
            body, separator, comment = original_line.partition("#")
            tokens = body.split()
            if len(tokens) < 2 or not _is_prl_sinkhole_ips([tokens[0]]):
                next_lines.append(original_line)
                continue
            kept_aliases: List[str] = []
            for alias in tokens[1:]:
                normalized = alias.strip().lower().rstrip(".")
                if normalized in targets:
                    removed.append(alias)
                else:
                    kept_aliases.append(alias)
            if len(kept_aliases) == len(tokens) - 1:
                next_lines.append(original_line)
                continue
            if kept_aliases:
                rebuilt = f"{tokens[0]}\t{' '.join(kept_aliases)}"
                if separator:
                    rebuilt += f"  #{comment}"
                next_lines.append(rebuilt)
            elif separator and comment.strip():
                next_lines.append(f"#{comment}")
        if not removed:
            return True, "unchanged"
        backup = Path(f"{hosts_path}.fcs-prl-preflight.{int(time.time())}.bak")
        try:
            shutil.copy2(str(hosts_path), str(backup))
        except Exception:
            pass
        hosts_path.write_text("\n".join(next_lines) + "\n", encoding="utf-8")
        return True, f"removed:{','.join(sorted(set(removed)))[:220]}"
    except Exception as exc:
        return False, str(exc)[:300]


def _prl_network_preflight(pool_url: str, miner_urls: List[str]) -> Dict[str, Any]:
    diagnostics: Dict[str, Any] = {
        "checkedAtMs": int(_now_ms()),
        "hosts": {},
        "resolverRepairAttempted": False,
        "resolverRepairSucceeded": False,
    }
    host_sources: List[Tuple[str, str, bool]] = []
    pool_host = _host_from_url(pool_url)
    if pool_host:
        host_sources.append(("pool", pool_host, False))
    for url in miner_urls:
        host = _host_from_url(url)
        if host:
            host_sources.append(("miner", host, str(url).lower().startswith("https://")))

    unique_hosts: List[Tuple[str, str, bool]] = []
    seen: Set[str] = set()
    for source, host, check_tls in host_sources:
        key = f"{source}:{host}"
        if key in seen:
            continue
        seen.add(key)
        unique_hosts.append((source, host, check_tls))

    def collect() -> bool:
        bad = False
        hosts_out: Dict[str, Any] = {}
        for index, (source, host, check_tls) in enumerate(unique_hosts):
            ips = _resolve_host_ips(host)
            issuer = _tls_issuer_for_host(host) if check_tls else ""
            sinkhole = _is_prl_sinkhole_ips(ips)
            cisco = _is_cisco_umbrella_issuer(issuer)
            if sinkhole or cisco:
                bad = True
            # RTDB rejects map keys containing ".", "#", "$", "[", or "]".
            # Keep the actual host in the value and use a stable safe key for mirrors.
            hosts_out[f"{source}_{index}"] = {
                "source": source,
                "host": host,
                "ips": ips[:8],
                "sinkholeDns": sinkhole,
                **({"tlsIssuer": issuer} if issuer else {}),
                **({"ciscoUmbrellaTls": cisco} if issuer else {}),
            }
        diagnostics["hosts"] = hosts_out
        return bad

    bad = collect()
    if bad:
        diagnostics["resolverRepairAttempted"] = True
        hosts_repaired, hosts_detail = _remove_blocked_prl_host_overrides(
            host for _, host, _ in unique_hosts
        )
        diagnostics["hostsRepairSucceeded"] = hosts_repaired
        diagnostics["hostsRepairDetail"] = hosts_detail
        repaired, detail = _write_clean_resolv_conf()
        diagnostics["resolverRepairSucceeded"] = repaired
        diagnostics["resolverRepairDetail"] = detail
        bad = collect()
    diagnostics["ok"] = not bad
    if bad:
        raise PrlNetworkPreflightError(
            "PRL network preflight failed: blocked/non-public DNS resolution or TLS interception detected",
            diagnostics,
        )
    return diagnostics


def _read_tail_text(path: Path, max_bytes: int = 32768) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        size = path.stat().st_size
        with path.open("rb") as f:
            if size > max_bytes:
                f.seek(max(0, size - max_bytes))
            return f.read(max_bytes).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _query_gpu_telemetry() -> Dict[str, Any]:
    fields = [
        "name",
        "utilization.gpu",
        "memory.used",
        "memory.total",
        "power.draw",
        "power.limit",
        "temperature.gpu",
        "clocks.current.graphics",
        "clocks.current.sm",
        "clocks.current.memory",
        "clocks.current.video",
        "pstate",
        "clocks_throttle_reasons.active",
    ]
    try:
        proc = subprocess.run(
            [
                "nvidia-smi",
                f"--query-gpu={','.join(fields)}",
                "--format=csv,noheader,nounits",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.5,
            check=False,
        )
        line = (proc.stdout or "").strip().splitlines()[0] if proc.stdout else ""
        if proc.returncode != 0 or not line:
            return _query_basic_gpu_telemetry()
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < len(fields):
            return _query_basic_gpu_telemetry()
        out: Dict[str, Any] = {
            "gpuName": parts[0][:160],
            "gpuTelemetryAtMs": _now_ms(),
        }

        def assign_float(index: int, key: str, min_value: float = 0.0, max_value: Optional[float] = None) -> None:
            try:
                raw = parts[index]
                if not raw or raw.upper() == "N/A":
                    return
                value = float(raw)
                if value < min_value:
                    return
                if max_value is not None and value > max_value:
                    return
                out[key] = value
            except Exception:
                pass

        def assign_string(index: int, key: str, max_len: int = 160) -> None:
            raw = parts[index] if index < len(parts) else ""
            if raw and raw.upper() != "N/A":
                out[key] = raw[:max_len]

        assign_float(1, "gpuUtilizationPct", 0.0, 100.0)
        assign_float(2, "gpuMemoryUsedMb")
        assign_float(3, "gpuMemoryTotalMb")
        assign_float(4, "gpuPowerDrawW")
        assign_float(5, "gpuPowerLimitW")
        assign_float(6, "gpuTemperatureC")
        assign_float(7, "gpuGraphicsClockMhz")
        assign_float(8, "gpuSmClockMhz")
        assign_float(9, "gpuMemoryClockMhz")
        assign_float(10, "gpuVideoClockMhz")
        assign_string(11, "gpuPstate", 32)
        assign_string(12, "gpuClocksThrottleReasonsActive", 200)
        return out
    except Exception:
        return _query_basic_gpu_telemetry()


def _query_basic_gpu_telemetry() -> Dict[str, Any]:
    try:
        proc = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,memory.used,power.draw",
                "--format=csv,noheader,nounits",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.5,
            check=False,
        )
        line = (proc.stdout or "").strip().splitlines()[0] if proc.stdout else ""
        if proc.returncode != 0 or not line:
            return {}
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 4:
            return {}
        out: Dict[str, Any] = {
            "gpuName": parts[0][:160],
            "gpuTelemetryAtMs": _now_ms(),
        }
        try:
            out["gpuUtilizationPct"] = max(0.0, min(100.0, float(parts[1])))
        except Exception:
            pass
        try:
            out["gpuMemoryUsedMb"] = max(0.0, float(parts[2]))
        except Exception:
            pass
        try:
            out["gpuPowerDrawW"] = max(0.0, float(parts[3]))
        except Exception:
            pass
        return out
    except Exception:
        return {}


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _first_env(*names: str) -> Optional[str]:
    for name in names:
        value = _env_str(name)
        if value:
            return value
    return None


def _decode_jwt_payload_unverified(token: str) -> Dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        data = json.loads(decoded.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _fetch_salad_imds_claims() -> Dict[str, Any]:
    token_url = _env_str("SALAD_IMDS_TOKEN_URL", "http://169.254.169.254/v1/token")
    if not token_url:
        return {}
    try:
        req = urllib.request.Request(token_url, headers={"Accept": "application/json, text/plain, */*", "Metadata": "true"})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=1.0) as resp:
            raw = resp.read(16 * 1024).decode("utf-8", errors="replace").strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and isinstance(parsed.get("jwt"), str):
                raw = parsed["jwt"]
        except Exception:
            pass
        return _decode_jwt_payload_unverified(raw)
    except Exception as e:
        logging.debug("Salad IMDS metadata fetch skipped/failed: %s", e)
        return {}


def detect_provider_metadata() -> Dict[str, Any]:
    provider = (_first_env("DM_PROVIDER", "FCS_COMPUTE_PROVIDER", "COMPUTE_PROVIDER") or "").lower()
    has_salad_env = any(
        _env_str(name)
        for name in (
            "SALAD_CONTAINER_GROUP_INSTANCE_ID",
            "SALAD_CONTAINER_GROUP_NAME",
            "SALAD_MACHINE_ID",
            "SALAD_PROJECT_NAME",
            "SALAD_ORGANIZATION_NAME",
        )
    )
    if provider != "salad" and not has_salad_env:
        return {}

    claims = _fetch_salad_imds_claims()
    metadata: Dict[str, Any] = {
        "provider": "salad",
        "organizationName": _first_env("SALAD_ORGANIZATION_NAME", "DM_SALAD_ORGANIZATION_NAME") or claims.get("salad_organization_name") or claims.get("organization_name") or claims.get("organization"),
        "projectName": _first_env("SALAD_PROJECT_NAME", "DM_SALAD_PROJECT_NAME") or claims.get("salad_project_name") or claims.get("project_name") or claims.get("project"),
        "containerGroupName": _first_env("SALAD_CONTAINER_GROUP_NAME", "DM_SALAD_CONTAINER_GROUP_NAME") or claims.get("salad_container_group_name") or claims.get("container_group_name") or claims.get("container_group"),
        "containerGroupInstanceId": (
            _first_env("SALAD_CONTAINER_GROUP_INSTANCE_ID", "SALAD_INSTANCE_ID", "DM_SALAD_INSTANCE_ID")
            or claims.get("salad_container_group_instance_id")
            or claims.get("container_group_instance_id")
            or claims.get("instance_id")
            or claims.get("sub")
        ),
        "machineId": _first_env("SALAD_MACHINE_ID", "DM_SALAD_MACHINE_ID") or claims.get("salad_machine_id") or claims.get("machine_id"),
        "replicaId": _first_env("SALAD_REPLICA_ID", "DM_SALAD_REPLICA_ID") or claims.get("salad_replica_id") or claims.get("replica_id"),
        "allocationId": _first_env("SALAD_ALLOCATION_ID", "DM_SALAD_ALLOCATION_ID") or claims.get("allocation_id"),
        "hostname": socket.gethostname(),
        "identitySource": "env+imds" if claims else "env",
    }
    metadata = {k: v for k, v in metadata.items() if v is not None and v != ""}
    if claims:
        metadata["imdsClaims"] = claims
    return metadata


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


EXISTING_FILE_STABLE_SECONDS = max(0, _env_int("DM_EXISTING_FILE_STABLE_SECONDS", 120))


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


def _linux_process_start_time(pid: int) -> Optional[int]:
    """Return Linux /proc starttime ticks for PID-reuse-safe process identity."""
    try:
        raw = (Path("/proc") / str(int(pid)) / "stat").read_text("utf-8", errors="replace")
        close_idx = raw.rfind(")")
        if close_idx < 0:
            return None
        fields = raw[close_idx + 1:].strip().split()
        # fields starts at proc(5) field 3 (state); starttime is field 22.
        return int(fields[19]) if len(fields) > 19 else None
    except Exception:
        return None


_CGROUP_MEMORY_PEAK_FALLBACK_BYTES = 0


def collect_cgroup_memory_telemetry(cgroup_root: Path = Path("/sys/fs/cgroup")) -> Dict[str, Any]:
    """Collect cheap container-wide cgroup v2 memory counters for runtime sizing."""
    global _CGROUP_MEMORY_PEAK_FALLBACK_BYTES
    fields = {
        "currentBytes": "memory.current",
        "peakBytes": "memory.peak",
        "swapCurrentBytes": "memory.swap.current",
    }
    telemetry: Dict[str, Any] = {
        "source": "cgroup_v2",
        "measuredAtMs": _now_ms(),
    }
    for output_key, file_name in fields.items():
        try:
            raw = (cgroup_root / file_name).read_text(encoding="utf-8").strip()
            value = int(raw)
            if value >= 0:
                telemetry[output_key] = value
        except (OSError, TypeError, ValueError):
            continue
    current_bytes = telemetry.get("currentBytes")
    if isinstance(current_bytes, int):
        _CGROUP_MEMORY_PEAK_FALLBACK_BYTES = max(_CGROUP_MEMORY_PEAK_FALLBACK_BYTES, current_bytes)
        if "peakBytes" not in telemetry:
            telemetry["peakBytes"] = _CGROUP_MEMORY_PEAK_FALLBACK_BYTES
            telemetry["peakSource"] = "agent_max_current"
        else:
            telemetry["peakSource"] = "memory.peak"
    return telemetry if any(key in telemetry for key in fields) else {}


def collect_ssh_host_key_sha256(ssh_dir: Path = Path("/etc/ssh")) -> List[str]:
    """Return OpenSSH SHA-256 fingerprints for locally served host public keys."""
    fingerprints: List[str] = []
    for path in sorted(ssh_dir.glob("ssh_host_*_key.pub")):
        try:
            parts = path.read_text(encoding="utf-8").strip().split()
            if len(parts) < 2:
                continue
            key_blob = base64.b64decode(parts[1], validate=True)
            digest = base64.b64encode(hashlib.sha256(key_blob).digest()).decode("ascii").rstrip("=")
            fingerprint = f"SHA256:{digest}"
            if fingerprint not in fingerprints:
                fingerprints.append(fingerprint)
        except (OSError, TypeError, ValueError):
            continue
    return fingerprints


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_to_ms(value: Optional[str]) -> Optional[int]:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sha256_hex_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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


def _nearest_existing_path(path: Path) -> Path:
    current = path
    for _ in range(32):
        if current.exists():
            return current
        parent = current.parent
        if parent == current:
            return current
        current = parent
    return path


def _read_mountinfo() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        text = Path("/proc/self/mountinfo").read_text("utf-8", errors="ignore")
    except Exception:
        return rows

    for line in text.splitlines():
        try:
            left, right = line.split(" - ", 1)
            left_parts = left.split()
            right_parts = right.split()
            if len(left_parts) < 5 or len(right_parts) < 3:
                continue
            rows.append({
                "mountPoint": left_parts[4].replace("\\040", " "),
                "majorMinor": left_parts[2],
                "root": left_parts[3].replace("\\040", " "),
                "filesystemType": right_parts[0],
                "source": right_parts[1],
            })
        except Exception:
            continue
    return rows


def _mount_for_path(path: Path) -> Dict[str, str]:
    try:
        resolved = str(_nearest_existing_path(path).resolve())
    except Exception:
        resolved = str(path)

    best: Dict[str, str] = {}
    for row in _read_mountinfo():
        mount_point = row.get("mountPoint") or ""
        if not mount_point:
            continue
        if resolved == mount_point or resolved.startswith(mount_point.rstrip("/") + "/"):
            if len(mount_point) > len(best.get("mountPoint", "")):
                best = row
    return best


def disk_stats(path: Path) -> Dict[str, Any]:
    usage_path = _nearest_existing_path(path)
    usage = shutil.disk_usage(str(usage_path))
    out: Dict[str, Any] = {
        "path": str(path),
        "statPath": str(usage_path),
        "totalBytes": int(usage.total),
        "freeBytes": int(usage.free),
        "usedBytes": int(usage.used),
    }
    try:
        out["resolvedPath"] = str(usage_path.resolve())
    except Exception:
        pass
    mount = _mount_for_path(usage_path)
    if mount:
        out["mount"] = mount
    return out


def _scan_deleted_open_files(base_paths: Iterable[Path], max_examples: int = 20) -> Dict[str, Any]:
    bases: List[str] = []
    for base in base_paths:
        try:
            bases.append(str(_nearest_existing_path(base).resolve()))
        except Exception:
            continue
    bases = sorted(set(bases), key=len, reverse=True)
    if not bases:
        return {"count": 0, "bytes": 0, "examples": [], "truncated": False}

    count = 0
    total_bytes = 0
    examples: List[Dict[str, Any]] = []
    truncated = False
    proc = Path("/proc")
    try:
        pid_dirs = [p for p in proc.iterdir() if p.name.isdigit()]
    except Exception:
        return {"count": 0, "bytes": 0, "examples": [], "truncated": False}

    for pid_dir in pid_dirs:
        fd_dir = pid_dir / "fd"
        try:
            fds = list(fd_dir.iterdir())
        except Exception:
            continue
        for fd in fds:
            try:
                target = os.readlink(str(fd))
            except Exception:
                continue
            if " (deleted)" not in target:
                continue
            clean_target = target.replace(" (deleted)", "")
            try:
                resolved_target = str(Path(clean_target).resolve())
            except Exception:
                resolved_target = clean_target
            if not any(resolved_target == base or resolved_target.startswith(base.rstrip("/") + "/") for base in bases):
                continue
            size = 0
            try:
                size = int(os.stat(str(fd)).st_size)
            except Exception:
                size = 0
            count += 1
            total_bytes += max(0, size)
            if len(examples) < max_examples:
                examples.append({
                    "pid": pid_dir.name,
                    "fd": fd.name,
                    "path": clean_target[:500],
                    "sizeBytes": int(size),
                })
            else:
                truncated = True
    return {"count": count, "bytes": int(total_bytes), "examples": examples, "truncated": truncated}


def _is_path_open_by_process(path: Path) -> bool:
    try:
        target_path = str(path.resolve())
    except Exception:
        target_path = str(path)
    proc = Path("/proc")
    try:
        pid_dirs = [p for p in proc.iterdir() if p.name.isdigit()]
    except Exception:
        return False
    for pid_dir in pid_dirs:
        fd_dir = pid_dir / "fd"
        try:
            fds = list(fd_dir.iterdir())
        except Exception:
            continue
        for fd in fds:
            try:
                fd_target = os.readlink(str(fd)).replace(" (deleted)", "")
            except Exception:
                continue
            try:
                fd_target_resolved = str(Path(fd_target).resolve())
            except Exception:
                fd_target_resolved = fd_target
            if fd_target_resolved == target_path:
                return True
    return False


def _discard_oversized_partial(dest_partial: Path, expected_size_bytes: int, context: str) -> bool:
    if expected_size_bytes <= 0:
        return False
    try:
        if not dest_partial.exists():
            return False
        partial_bytes = int(dest_partial.stat().st_size)
    except Exception:
        return False
    if partial_bytes <= int(expected_size_bytes):
        return False
    try:
        dest_partial.unlink()
        logging.warning(
            "Discarded oversized partial for %s: got %d bytes, expected %d bytes, path=%s",
            context,
            int(partial_bytes),
            int(expected_size_bytes),
            str(dest_partial),
        )
        return True
    except Exception as e:
        logging.warning(
            "Failed to discard oversized partial for %s: got %d bytes, expected %d bytes, path=%s err=%s",
            context,
            int(partial_bytes),
            int(expected_size_bytes),
            str(dest_partial),
            e,
        )
        return False


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


class NetworkError(RuntimeError):
    def __init__(self, url: str, reason: Any):
        self.url = _safe_url_for_logs(url)
        self.reason = reason
        super().__init__(f"Network error calling {self.url}: {reason}")


def _json_loads_or_none(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


def _strip_none(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, nested in value.items():
            stripped = _strip_none(nested)
            if stripped is not None:
                out[key] = stripped
        return out
    if isinstance(value, list):
        return [_strip_none(item) for item in value if item is not None]
    return value


def _http_status_codes_from_error(err: Exception) -> List[int]:
    msg = str(err)
    codes: List[int] = []
    patterns = (
        r"\bHTTP(?:/\d(?:\.\d)?)?\s+(\d{3})\b",
        r"\bERROR\s+(\d{3})\b",
        r"\bstatus=(\d{3})\b",
        r"\bHTTP Error\s+(\d{3})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, msg, flags=re.IGNORECASE):
            try:
                code = int(match.group(1))
            except Exception:
                continue
            if code not in codes:
                codes.append(code)
    return codes


def _is_permanent_http_error(err: Exception) -> bool:
    codes = _http_status_codes_from_error(err)
    if not codes:
        return False
    for code in codes:
        if code in RETRYABLE_HTTP_STATUS_CODES:
            return False
        if code >= 500:
            return False
    return any(400 <= code < 500 for code in codes)


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
    except (urllib.error.URLError, socket.timeout, TimeoutError, http.client.HTTPException) as e:
        raise NetworkError(url, e) from None


def api_form_json(
    method: str,
    url: str,
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
) -> Tuple[int, Optional[Any]]:
    req_headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if headers:
        req_headers.update(headers)
    payload = urllib.parse.urlencode({k: str(v) for k, v in body.items()}).encode("utf-8")
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
    except (urllib.error.URLError, socket.timeout, TimeoutError, http.client.HTTPException) as e:
        raise NetworkError(url, e) from None


def sha256_file(
    path: Path,
    chunk_size: int = 8 * 1024 * 1024,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> str:
    import hashlib

    h = hashlib.sha256()
    total_size = 0
    try:
        total_size = int(path.stat().st_size)
    except Exception:
        total_size = 0
    processed = 0
    last_progress_at = 0.0
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            processed += len(chunk)
            if progress_cb:
                now = time.time()
                if processed == total_size or now - last_progress_at >= 2.0:
                    try:
                        progress_cb(int(processed), int(total_size))
                    except Exception:
                        pass
                    last_progress_at = now
    return h.hexdigest()


def huggingface_hub_download(
    url: str,
    dest_partial: Path,
    auth_header: Optional[str],
    expected_size_bytes: int = 0,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    """Download a pinned Hugging Face /resolve/ URL through hf_xet when available."""
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    path_parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    try:
        resolve_index = path_parts.index("resolve")
    except ValueError as exc:
        raise RuntimeError("Hugging Face URL is not a pinned /resolve/ URL") from exc
    if host not in {"huggingface.co", "www.huggingface.co", "hf.co", "www.hf.co"}:
        raise RuntimeError(f"Unsupported Hugging Face host: {host or 'missing'}")
    if resolve_index < 2 or resolve_index + 2 >= len(path_parts):
        raise RuntimeError("Hugging Face /resolve/ URL is missing repo, revision, or filename")

    repo_id = "/".join(path_parts[:resolve_index])
    revision = path_parts[resolve_index + 1]
    filename = "/".join(path_parts[resolve_index + 2:])
    token = auth_header[7:] if isinstance(auth_header, str) and auth_header.startswith("Bearer ") else None
    cache_dir = Path(os.environ.get("DM_HF_CACHE_DIR") or (Path(os.environ.get("WORKSPACE", "/workspace")) / ".dm_hf_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest_partial.parent.mkdir(parents=True, exist_ok=True)

    try:
        from huggingface_hub import hf_hub_download as hf_download
    except Exception as exc:
        raise RuntimeError(f"huggingface_hub unavailable: {exc}") from exc

    previous_high_performance = os.environ.get("HF_XET_HIGH_PERFORMANCE")
    os.environ["HF_XET_HIGH_PERFORMANCE"] = "1"
    try:
        cached_path = Path(hf_download(
            repo_id=repo_id,
            filename=filename,
            revision=revision,
            cache_dir=str(cache_dir),
            token=token,
        )).resolve()
    finally:
        if previous_high_performance is None:
            os.environ.pop("HF_XET_HIGH_PERFORMANCE", None)
        else:
            os.environ["HF_XET_HIGH_PERFORMANCE"] = previous_high_performance

    actual_size = int(cached_path.stat().st_size)
    if expected_size_bytes > 0 and actual_size != int(expected_size_bytes):
        raise RuntimeError(
            f"Hugging Face cached file size mismatch: expected {expected_size_bytes}, got {actual_size}"
        )

    staged_link = dest_partial.with_name(f"{dest_partial.name}.hf-{uuid.uuid4().hex}")
    try:
        try:
            os.link(cached_path, staged_link)
        except OSError:
            shutil.copyfile(cached_path, staged_link)
        os.replace(staged_link, dest_partial)
    finally:
        try:
            staged_link.unlink(missing_ok=True)
        except Exception:
            pass
    if progress_cb:
        progress_cb(actual_size, int(expected_size_bytes or actual_size))


AGENT_VERSION_RE = re.compile(r'^\s*AGENT_VERSION\s*=\s*["\']([^"\']+)["\']', re.MULTILINE)
AGENT_VERSION_TAG_RE = re.compile(r"^dm-agent-py/(\d+)\.(\d+)\.(\d+)$", re.IGNORECASE)


def extract_agent_version_from_script(path: Path) -> Optional[str]:
    try:
        text = path.read_text("utf-8")
    except Exception:
        return None
    match = AGENT_VERSION_RE.search(text)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def parse_agent_version_tuple(agent_version: str) -> Optional[Tuple[int, int, int]]:
    value = str(agent_version or "").strip()
    if not value:
        return None
    match = AGENT_VERSION_TAG_RE.fullmatch(value)
    if not match:
        return None
    try:
        return tuple(int(part) for part in match.groups())
    except Exception:
        return None


def compare_agent_versions(left: str, right: str) -> Optional[int]:
    left_tuple = parse_agent_version_tuple(left)
    right_tuple = parse_agent_version_tuple(right)
    if left_tuple is None or right_tuple is None:
        return None
    if left_tuple < right_tuple:
        return -1
    if left_tuple > right_tuple:
        return 1
    return 0


def _safe_url_for_logs(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        host = (p.hostname or "").lower()
        scheme = p.scheme or "https"
        path = p.path or "/"
        if not host:
            return "<invalid-url>"
        return f"{scheme}://{host}{path}"
    except Exception:
        return "<invalid-url>"


def _command_exists(cmd: str) -> bool:
    try:
        return shutil.which(cmd) is not None
    except Exception:
        return False


def http_download(
    url: str,
    dest_partial: Path,
    auth_header: Optional[str],
    expected_size_bytes: int = 0,
    timeout_seconds: float = 30.0,
    chunk_size: int = 8 * 1024 * 1024,
    user_agent: str = "dm-agent-download/1.0",
    allowed_domains: Optional[Set[str]] = None,
    verbose: bool = False,
    debug: bool = False,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host:
        raise ValueError("Invalid download URL")

    if allowed_domains:
        ok = any(host == d or host.endswith("." + d) for d in allowed_domains)
        if not ok:
            raise ValueError(f"Download domain not allowed: {host}")

    safe_url = _safe_url_for_logs(url)

    dest_partial.parent.mkdir(parents=True, exist_ok=True)

    headers: Dict[str, str] = {"User-Agent": user_agent}
    if auth_header:
        headers["Authorization"] = auth_header

    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    # Resume from existing partial downloads when possible to improve reliability with large files.
    existing_bytes = 0
    try:
        if dest_partial.exists():
            existing_bytes = int(dest_partial.stat().st_size)
    except Exception:
        existing_bytes = 0

    if debug:
        try:
            infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            ips: List[str] = []
            for info in infos:
                sockaddr = info[4]
                ip = sockaddr[0] if isinstance(sockaddr, tuple) and len(sockaddr) > 0 else None
                if isinstance(ip, str) and ip and ip not in ips:
                    ips.append(ip)
            if ips:
                logging.info("download dns: host=%s ips=%s", host, ",".join(ips[:8]))
        except Exception as e:
            logging.info("download dns failed: host=%s err=%s", host, e)

    if expected_size_bytes > 0 and existing_bytes > expected_size_bytes:
        # Corrupt partial (or wrong file); restart.
        _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
        existing_bytes = 0

    if expected_size_bytes > 0 and existing_bytes == expected_size_bytes:
        # Previous attempt fully downloaded but crashed before rename.
        return

    req_headers = dict(headers)
    if existing_bytes > 0:
        req_headers["Range"] = f"bytes={existing_bytes}-"

    req = urllib.request.Request(url, headers=req_headers, method="GET")

    start = time.time()
    downloaded = max(0, int(existing_bytes))
    last_log = 0.0

    expected_total: Optional[int] = int(expected_size_bytes) if expected_size_bytes > 0 else None
    status: Optional[int] = None
    safe_final_url: Optional[str] = None
    content_length: Optional[str] = None
    content_range: Optional[str] = None
    accept_ranges: Optional[str] = None
    etag: Optional[str] = None

    try:
        with opener.open(req, timeout=timeout_seconds) as resp:
            status = getattr(resp, "status", None)
            if not isinstance(status, int):
                try:
                    status = int(resp.getcode())
                except Exception:
                    status = 200
            try:
                safe_final_url = _safe_url_for_logs(resp.geturl())
            except Exception:
                safe_final_url = None

            final_host = ""
            try:
                final_host = (urllib.parse.urlparse(resp.geturl()).hostname or "").lower()
            except Exception:
                final_host = ""
            if allowed_domains and final_host:
                ok = any(final_host == d or final_host.endswith("." + d) for d in allowed_domains)
                if not ok:
                    raise RuntimeError(f"Redirected to disallowed host: {final_host}")

            try:
                content_length = resp.headers.get("Content-Length")
                content_range = resp.headers.get("Content-Range")
                accept_ranges = resp.headers.get("Accept-Ranges")
                etag = resp.headers.get("ETag")
            except Exception:
                content_length = None
                content_range = None
                accept_ranges = None
                etag = None

            if debug:
                logging.info(
                    "download start: url=%s final=%s existingBytes=%d expectedBytes=%d timeout=%.1fs status=%s cl=%s cr=%s ar=%s etag=%s",
                    safe_url,
                    safe_final_url or "-",
                    int(existing_bytes),
                    int(expected_size_bytes or 0),
                    float(timeout_seconds),
                    str(status),
                    content_length or "-",
                    content_range or "-",
                    accept_ranges or "-",
                    (etag[:60] + "...") if isinstance(etag, str) and len(etag) > 60 else (etag or "-"),
                )

            # Determine whether we're appending (resume) or restarting.
            mode = "wb"
            if existing_bytes > 0 and status == 206:
                mode = "ab"

                # Best-effort validation + total size detection.
                if isinstance(content_range, str) and content_range:
                    m = re.match(r"^bytes\\s+(\\d+)-(\\d+)/(\\d+|\\*)$", content_range.strip())
                    if m:
                        start_b = int(m.group(1))
                        total_s = m.group(3)
                        if start_b != existing_bytes:
                            raise RuntimeError(
                                f"Resume mismatch for {safe_url}: expected start {existing_bytes}, got {start_b}"
                            )
                        if expected_total is None and total_s.isdigit():
                            expected_total = int(total_s)
                if expected_total is None:
                    try:
                        if isinstance(content_length, str) and content_length.isdigit():
                            expected_total = existing_bytes + int(content_length)
                    except Exception:
                        pass
            else:
                # If the server ignored Range (200) we restart from scratch.
                if existing_bytes > 0 and status == 200:
                    logging.info("Server did not honor Range; restarting download: %s", safe_url)
                    existing_bytes = 0
                    downloaded = 0
                mode = "wb"

                if expected_total is None:
                    try:
                        if isinstance(content_length, str) and content_length.isdigit():
                            expected_total = int(content_length)
                    except Exception:
                        pass

            with dest_partial.open(mode) as f:
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if expected_total is not None and downloaded > int(expected_total):
                        raise RuntimeError(
                            f"Download exceeded expected size for {safe_url}: got {downloaded} bytes, expected {expected_total} bytes"
                        )
                    if progress_cb:
                        try:
                            progress_cb(int(downloaded), int(expected_total or expected_size_bytes or 0))
                        except Exception:
                            pass
                    if verbose:
                        now = time.time()
                        if now - last_log >= 10:
                            elapsed = max(0.001, now - start)
                            mb = downloaded / (1024 * 1024)
                            rate = mb / elapsed
                            logging.info("downloaded %.1f MiB (%.2f MiB/s) -> %s", mb, rate, str(dest_partial))
                            last_log = now

        if expected_total is not None:
            actual_size = 0
            try:
                actual_size = int(dest_partial.stat().st_size)
            except Exception:
                actual_size = 0
            if actual_size != int(expected_total):
                if actual_size > int(expected_total):
                    _discard_oversized_partial(dest_partial, int(expected_total), safe_url)
                raise RuntimeError(f"Incomplete download for {safe_url}: got {actual_size} bytes, expected {expected_total} bytes")
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", 0) or 0)
        if code == 416 and existing_bytes > 0:
            # Range not satisfiable; treat as complete if local size matches total.
            total: Optional[int] = None
            try:
                cr = e.headers.get("Content-Range") if getattr(e, "headers", None) is not None else None
                if isinstance(cr, str) and cr:
                    m = re.match(r"^bytes\\s+\\*/(\\d+)$", cr.strip())
                    if m:
                        total = int(m.group(1))
            except Exception:
                total = None

            if total is not None:
                try:
                    if int(dest_partial.stat().st_size) >= int(total):
                        return
                except Exception:
                    pass

        retry_after = None
        try:
            retry_after = e.headers.get("Retry-After") if getattr(e, "headers", None) is not None else None
        except Exception:
            retry_after = None

        err_url = None
        try:
            err_url = _safe_url_for_logs(e.geturl())
        except Exception:
            err_url = safe_url

        extra = f" retry_after={retry_after}" if retry_after else ""
        raise RuntimeError(f"HTTP {code} downloading {err_url}:{extra} {e}") from None
    except Exception as e:
        partial_bytes = 0
        try:
            partial_bytes = int(dest_partial.stat().st_size) if dest_partial.exists() else 0
        except Exception:
            partial_bytes = 0
        discarded_oversized = False
        expected_for_discard = int(expected_total or expected_size_bytes or 0)
        if expected_for_discard > 0 and partial_bytes > expected_for_discard:
            discarded_oversized = _discard_oversized_partial(dest_partial, expected_for_discard, safe_url)
        parts: List[str] = [
            f"type={type(e).__name__}",
            f"url={safe_url}",
        ]
        if safe_final_url:
            parts.append(f"final={safe_final_url}")
        if isinstance(status, int):
            parts.append(f"status={status}")
        if existing_bytes > 0:
            parts.append("resume=1")

        size_part = f"bytes={partial_bytes}"
        if expected_total is not None:
            size_part += f"/{expected_total}"
        elif expected_size_bytes > 0:
            size_part += f"/{int(expected_size_bytes)}"
        parts.append(size_part)
        if discarded_oversized:
            parts.append("discardedOversizedPartial=1")

        if isinstance(content_length, str) and content_length:
            parts.append(f"cl={content_length}")
        if isinstance(content_range, str) and content_range:
            parts.append("cr=1")
        if isinstance(accept_ranges, str) and accept_ranges:
            parts.append(f"ar={accept_ranges}")

        ctx = " ".join(parts)
        raise RuntimeError(f"Download error ({ctx}): {e}") from None


def wget_download(
    url: str,
    dest_partial: Path,
    auth_header: Optional[str],
    expected_size_bytes: int = 0,
    timeout_seconds: float = 300.0,
    allowed_domains: Optional[Set[str]] = None,
    debug: bool = False,
    user_agent: str = "dm-agent-wget/1.0",
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    if not _command_exists("wget"):
        raise RuntimeError("wget not found on PATH (install wget or set DM_DOWNLOAD_TOOL=python).")

    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host:
        raise ValueError("Invalid download URL")

    if allowed_domains:
        ok = any(host == d or host.endswith("." + d) for d in allowed_domains)
        if not ok:
            raise ValueError(f"Download domain not allowed: {host}")

    safe_url = _safe_url_for_logs(url)
    dest_partial.parent.mkdir(parents=True, exist_ok=True)

    existing_bytes = 0
    try:
        if dest_partial.exists():
            existing_bytes = int(dest_partial.stat().st_size)
    except Exception:
        existing_bytes = 0

    if expected_size_bytes > 0 and existing_bytes > expected_size_bytes:
        _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
        existing_bytes = 0

    if expected_size_bytes > 0 and existing_bytes == expected_size_bytes:
        return

    if debug:
        try:
            infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            ips: List[str] = []
            for info in infos:
                sockaddr = info[4]
                ip = sockaddr[0] if isinstance(sockaddr, tuple) and len(sockaddr) > 0 else None
                if isinstance(ip, str) and ip and ip not in ips:
                    ips.append(ip)
            if ips:
                logging.info("download dns: host=%s ips=%s", host, ",".join(ips[:8]))
        except Exception as e:
            logging.info("download dns failed: host=%s err=%s", host, e)

    cmd: List[str] = [
        "wget",
        "--server-response",
        "--max-redirect=20",
        "--timeout",
        str(int(max(1.0, float(timeout_seconds)))),
        "--tries=3",
        "--waitretry=5",
        "--retry-connrefused",
        "--continue",
        "--user-agent",
        user_agent,
        "--output-document",
        str(dest_partial),
    ]

    if auth_header:
        cmd += ["--header", f"Authorization: {auth_header}"]

    if debug:
        logging.info(
            "wget start: url=%s existingBytes=%d expectedBytes=%d timeout=%.1fs",
            safe_url,
            int(existing_bytes),
            int(expected_size_bytes or 0),
            float(timeout_seconds),
        )

    proc = subprocess.Popen(
        cmd + [url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stderr_lines: List[str] = []

    def _drain_stderr() -> None:
        stream = proc.stderr
        if stream is None:
            return
        try:
            for line in stream:
                stderr_lines.append(line)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    last_progress_at = 0.0
    oversize_error: Optional[str] = None
    while True:
        ret = proc.poll()
        now = time.time()
        if progress_cb and (ret is not None or now - last_progress_at >= 2.0):
            current_bytes = 0
            try:
                if dest_partial.exists():
                    current_bytes = int(dest_partial.stat().st_size)
            except Exception:
                current_bytes = 0
            try:
                progress_cb(int(current_bytes), int(expected_size_bytes or 0))
            except Exception:
                pass
            if expected_size_bytes > 0 and current_bytes > int(expected_size_bytes):
                oversize_error = (
                    f"Download exceeded expected size for {safe_url}: "
                    f"got {current_bytes} bytes, expected {int(expected_size_bytes)} bytes"
                )
                try:
                    proc.terminate()
                except Exception:
                    pass
            last_progress_at = now
        if ret is not None:
            break
        time.sleep(1.0)

    stderr_thread.join(timeout=5.0)
    if oversize_error:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
        raise RuntimeError(oversize_error)
    stderr = "".join(stderr_lines)
    locations: List[str] = []
    statuses: List[int] = []
    retry_after: Optional[str] = None
    for line in stderr.splitlines():
        m = re.search(r"^\\s*Location:\\s*(\\S+)\\s*$", line, flags=re.IGNORECASE)
        if m:
            locations.append(m.group(1))
            continue
        m = re.search(r"\\bHTTP/\\S+\\s+(\\d{3})\\b", line)
        if m:
            try:
                statuses.append(int(m.group(1)))
            except Exception:
                pass
            continue
        m = re.search(r"^\\s*Retry-After:\\s*(\\S+)\\s*$", line, flags=re.IGNORECASE)
        if m:
            retry_after = m.group(1)

    if allowed_domains and locations:
        for loc in locations[-10:]:
            lh = ""
            try:
                lh = (urllib.parse.urlparse(loc).hostname or "").lower()
            except Exception:
                lh = ""
            if lh:
                ok = any(lh == d or lh.endswith("." + d) for d in allowed_domains)
                if not ok:
                    raise ValueError(f"Download domain not allowed: {lh}")

    if proc.returncode != 0:
        tail = "\n".join(stderr.splitlines()[-30:])
        status_part = f" http={statuses[-1]}" if statuses else ""
        retry_part = f" retry_after={retry_after}" if retry_after else ""
        raise RuntimeError(f"wget failed (exit={proc.returncode}{status_part}{retry_part}) for {safe_url}: {tail}")

    if expected_size_bytes > 0:
        actual_size = 0
        try:
            actual_size = int(dest_partial.stat().st_size)
        except Exception:
            actual_size = 0
        if actual_size != int(expected_size_bytes):
            if actual_size > int(expected_size_bytes):
                _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
            raise RuntimeError(
                f"Incomplete download for {safe_url}: got {actual_size} bytes, expected {int(expected_size_bytes)} bytes"
            )


def aria2_download(
    url: str,
    dest_partial: Path,
    auth_header: Optional[str],
    expected_size_bytes: int = 0,
    timeout_seconds: float = 300.0,
    allowed_domains: Optional[Set[str]] = None,
    debug: bool = False,
    user_agent: str = "dm-agent-aria2/1.0",
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    """Multi-connection download via aria2c (-x8 -s8). Single-stream HTTP from the
    model CDNs is frequently the bottleneck on Vast hosts; splitting the transfer
    rescues hosts whose per-connection throughput collapses.

    Domain allowlisting is enforced on the initial URL host (redirect-chain
    enforcement is wget-only); final size is verified the same as other tools.
    """
    if not _command_exists("aria2c"):
        raise RuntimeError("aria2c not found on PATH (install aria2 or set DM_DOWNLOAD_TOOL=wget).")

    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host:
        raise ValueError("Invalid download URL")

    if allowed_domains:
        ok = any(host == d or host.endswith("." + d) for d in allowed_domains)
        if not ok:
            raise ValueError(f"Download domain not allowed: {host}")

    safe_url = _safe_url_for_logs(url)
    dest_partial.parent.mkdir(parents=True, exist_ok=True)

    existing_bytes = 0
    try:
        if dest_partial.exists():
            existing_bytes = int(dest_partial.stat().st_size)
    except Exception:
        existing_bytes = 0

    if expected_size_bytes > 0 and existing_bytes > expected_size_bytes:
        _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
        existing_bytes = 0

    if expected_size_bytes > 0 and existing_bytes == expected_size_bytes:
        return

    cmd: List[str] = [
        "aria2c",
        "--continue=true",
        "--max-connection-per-server=8",
        "--split=8",
        "--min-split-size=4M",
        "--file-allocation=none",
        "--max-tries=3",
        "--retry-wait=5",
        f"--timeout={int(max(1.0, float(timeout_seconds)))}",
        "--connect-timeout=30",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "--console-log-level=warn",
        "--summary-interval=0",
        "--user-agent",
        user_agent,
        "--dir",
        str(dest_partial.parent),
        "--out",
        dest_partial.name,
    ]
    if auth_header:
        cmd += ["--header", f"Authorization: {auth_header}"]

    if debug:
        logging.info(
            "aria2 start: url=%s existingBytes=%d expectedBytes=%d timeout=%.1fs",
            safe_url,
            int(existing_bytes),
            int(expected_size_bytes or 0),
            float(timeout_seconds),
        )

    proc = subprocess.Popen(
        cmd + [url],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_lines: List[str] = []

    def _drain_output() -> None:
        stream = proc.stdout
        if stream is None:
            return
        try:
            for line in stream:
                output_lines.append(line)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    output_thread = threading.Thread(target=_drain_output, daemon=True)
    output_thread.start()

    def _current_downloaded_bytes() -> int:
        # Segmented writes make the file sparse; st_blocks reflects bytes actually
        # written, while st_size jumps to the furthest segment offset.
        try:
            if not dest_partial.exists():
                return 0
            st = dest_partial.stat()
            block_bytes = int(getattr(st, "st_blocks", 0)) * 512
            if block_bytes > 0:
                return min(int(st.st_size), block_bytes)
            return int(st.st_size)
        except Exception:
            return 0

    last_progress_at = 0.0
    last_progress_bytes = int(existing_bytes)
    last_byte_progress_at = time.time()
    oversize_error: Optional[str] = None
    stalled_error: Optional[str] = None
    while True:
        ret = proc.poll()
        now = time.time()
        if progress_cb and (ret is not None or now - last_progress_at >= 2.0):
            current_bytes = _current_downloaded_bytes()
            if current_bytes > last_progress_bytes:
                last_progress_bytes = int(current_bytes)
                last_byte_progress_at = now
            try:
                progress_cb(int(current_bytes), int(expected_size_bytes or 0))
            except Exception:
                pass
            if expected_size_bytes > 0 and current_bytes > int(expected_size_bytes):
                oversize_error = (
                    f"Download exceeded expected size for {safe_url}: "
                    f"got {current_bytes} bytes, expected {int(expected_size_bytes)} bytes"
                )
                try:
                    proc.terminate()
                except Exception:
                    pass
            elif now - last_byte_progress_at >= float(timeout_seconds):
                stalled_error = (
                    f"aria2c stalled for {safe_url}: no byte progress for "
                    f"{int(now - last_byte_progress_at)}s at {int(current_bytes)} bytes"
                )
                try:
                    proc.terminate()
                except Exception:
                    pass
            last_progress_at = now
        if ret is not None:
            break
        time.sleep(1.0)

    output_thread.join(timeout=5.0)
    if oversize_error:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
        raise RuntimeError(oversize_error)
    if stalled_error:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        raise RuntimeError(stalled_error)

    if proc.returncode != 0:
        tail = "\n".join("".join(output_lines).splitlines()[-30:])
        raise RuntimeError(f"aria2c failed (exit={proc.returncode}) for {safe_url}: {tail}")

    if expected_size_bytes > 0:
        actual_size = 0
        try:
            actual_size = int(dest_partial.stat().st_size)
        except Exception:
            actual_size = 0
        if actual_size != int(expected_size_bytes):
            if actual_size > int(expected_size_bytes):
                _discard_oversized_partial(dest_partial, int(expected_size_bytes), safe_url)
            raise RuntimeError(
                f"Incomplete download for {safe_url}: got {actual_size} bytes, expected {int(expected_size_bytes)} bytes"
            )


def http_download_to_file(
    url: str,
    dest_path: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 60.0,
    chunk_size: int = 8 * 1024 * 1024,
    user_agent: Optional[str] = None,
) -> None:
    req_headers = dict(headers or {})
    if user_agent and "User-Agent" not in req_headers and "user-agent" not in req_headers:
        req_headers["User-Agent"] = user_agent
    req = urllib.request.Request(url, headers=req_headers, method="GET")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        with dest_path.open("wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)


def curl_download_to_file(
    url: str,
    dest_path: Path,
    timeout_seconds: float = 300.0,
    user_agent: str = "dm-agent-curl/1.0",
) -> None:
    if not _command_exists("curl"):
        raise RuntimeError("curl not found on PATH.")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "curl",
        "--fail",
        "--location",
        "--retry",
        "3",
        "--retry-delay",
        "5",
        "--connect-timeout",
        "30",
        "--max-time",
        str(int(max(1.0, float(timeout_seconds)))),
        "--user-agent",
        user_agent,
        "--output",
        str(dest_path),
        url,
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=max(5.0, float(timeout_seconds) + 30.0),
    )
    if proc.returncode != 0:
        tail = "\n".join(str(proc.stderr or "").splitlines()[-30:])
        raise RuntimeError(f"curl failed (exit={proc.returncode}) for {_safe_url_for_logs(url)}: {tail}")


def download_file_with_tool_fallback(
    url: str,
    dest_path: Path,
    timeout_seconds: float = 300.0,
    chunk_size: int = 8 * 1024 * 1024,
    user_agent: str = "dm-agent-download/1.0",
    tools: Optional[List[str]] = None,
) -> str:
    ordered_tools = tools or ["python", "aria2", "wget", "curl"]
    errors: List[str] = []
    for tool in ordered_tools:
        normalized = str(tool or "").strip().lower()
        if not normalized:
            continue
        try:
            try:
                if dest_path.exists():
                    dest_path.unlink()
            except Exception:
                pass
            if normalized == "python":
                http_download_to_file(
                    url,
                    dest_path,
                    timeout_seconds=timeout_seconds,
                    chunk_size=chunk_size,
                    user_agent=user_agent,
                )
            elif normalized == "aria2":
                aria2_download(
                    url=url,
                    dest_partial=dest_path,
                    auth_header=None,
                    expected_size_bytes=0,
                    timeout_seconds=timeout_seconds,
                    allowed_domains=None,
                    debug=False,
                    user_agent=user_agent,
                )
            elif normalized == "wget":
                wget_download(
                    url=url,
                    dest_partial=dest_path,
                    auth_header=None,
                    expected_size_bytes=0,
                    timeout_seconds=timeout_seconds,
                    allowed_domains=None,
                    debug=False,
                    user_agent=user_agent,
                )
            elif normalized == "curl":
                curl_download_to_file(
                    url,
                    dest_path,
                    timeout_seconds=timeout_seconds,
                    user_agent=user_agent,
                )
            else:
                continue
            size = int(dest_path.stat().st_size) if dest_path.exists() else 0
            if size <= 0:
                raise RuntimeError("download produced an empty file")
            if errors:
                logging.info("Download succeeded with %s after fallback(s): %s", normalized, "; ".join(errors[-3:]))
            return normalized
        except Exception as exc:
            errors.append(f"{normalized}: {exc}")
            logging.warning("Download tool %s failed for %s: %s", normalized, _safe_url_for_logs(url), exc)
    raise RuntimeError(f"All download tools failed for {_safe_url_for_logs(url)}: {'; '.join(errors)}")


def http_head(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
) -> Tuple[int, Dict[str, str]]:
    req = urllib.request.Request(url, headers=headers or {}, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return int(resp.status), {k.lower(): v for k, v in dict(resp.headers).items()}
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500) or 500), {k.lower(): v for k, v in dict(getattr(e, "headers", {})).items()}


def _best_effort_expected_size_from_headers(headers: Optional[Dict[str, str]]) -> int:
    if not isinstance(headers, dict):
        return 0

    candidates = [
        headers.get("x-linked-size"),
        headers.get("x-file-size"),
        headers.get("content-length"),
    ]
    for value in candidates:
        if isinstance(value, str):
            raw = value.strip()
            if raw.isdigit():
                try:
                    parsed = int(raw)
                except Exception:
                    parsed = 0
                if parsed > 0:
                    return parsed

    content_range = headers.get("content-range")
    if isinstance(content_range, str):
        match = re.search(r"/(\d+)\s*$", content_range.strip())
        if match:
            try:
                parsed = int(match.group(1))
            except Exception:
                parsed = 0
            if parsed > 0:
                return parsed

    return 0


def http_put_bytes(
    url: str,
    body: bytes = b"",
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 300.0,
) -> Tuple[int, str, Dict[str, str]]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported upload URL scheme: {parsed.scheme}")
    host = parsed.hostname or ""
    if not host:
        raise ValueError("Upload URL missing hostname")
    port = parsed.port
    path_with_query = parsed.path or "/"
    if parsed.query:
        path_with_query = f"{path_with_query}?{parsed.query}"

    if parsed.scheme == "https":
        conn: Any = http.client.HTTPSConnection(host, port or 443, timeout=timeout_seconds)
    else:
        conn = http.client.HTTPConnection(host, port or 80, timeout=timeout_seconds)

    req_headers = {"Content-Length": str(len(body))}
    if headers:
        req_headers.update(headers)

    try:
        conn.putrequest("PUT", path_with_query)
        for k, v in req_headers.items():
            conn.putheader(k, v)
        conn.endheaders()
        if body:
            conn.send(body)

        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        out_headers = {k.lower(): v for k, v in dict(resp.headers).items()}
        return int(resp.status), raw, out_headers
    finally:
        try:
            conn.close()
        except Exception:
            pass


def http_put_file_stream(
    url: str,
    file_path: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 300.0,
    chunk_size: int = 8 * 1024 * 1024,
    start_offset: int = 0,
    length: Optional[int] = None,
) -> Tuple[int, str, Dict[str, str]]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported upload URL scheme: {parsed.scheme}")
    host = parsed.hostname or ""
    if not host:
        raise ValueError("Upload URL missing hostname")
    port = parsed.port
    path_with_query = parsed.path or "/"
    if parsed.query:
        path_with_query = f"{path_with_query}?{parsed.query}"

    if parsed.scheme == "https":
        conn: Any = http.client.HTTPSConnection(host, port or 443, timeout=timeout_seconds)
    else:
        conn = http.client.HTTPConnection(host, port or 80, timeout=timeout_seconds)

    total_size = int(file_path.stat().st_size)
    if start_offset < 0:
        raise ValueError("start_offset must be >= 0")
    if start_offset > total_size:
        raise ValueError("start_offset exceeds file size")
    if length is None:
        length = total_size - start_offset
    if length < 0:
        raise ValueError("length must be >= 0")

    req_headers = {"Content-Length": str(int(length))}
    if headers:
        req_headers.update(headers)

    try:
        conn.putrequest("PUT", path_with_query)
        for k, v in req_headers.items():
            conn.putheader(k, v)
        conn.endheaders()

        with file_path.open("rb") as f:
            if start_offset:
                f.seek(start_offset)
            remaining = int(length)
            while remaining > 0:
                chunk = f.read(min(int(chunk_size), remaining))
                if not chunk:
                    break
                conn.send(chunk)
                remaining -= len(chunk)

        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        out_headers = {k.lower(): v for k, v in dict(resp.headers).items()}
        return int(resp.status), raw, out_headers
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _parse_gcs_resume_offset(headers: Dict[str, str]) -> int:
    range_header = headers.get("range") or headers.get("Range")
    if isinstance(range_header, str):
        match = re.match(r"^bytes=0-(\d+)$", range_header.strip())
        if match:
            return int(match.group(1)) + 1
    return 0


def gcs_resumable_query_offset(
    session_url: str,
    total_size: int,
    content_type: str,
    timeout_seconds: float = 60.0,
) -> int:
    headers = {
        "Content-Type": content_type,
        "Content-Range": f"bytes */{int(total_size)}",
    }
    status, body, resp_headers = http_put_bytes(
        session_url,
        body=b"",
        headers=headers,
        timeout_seconds=timeout_seconds,
    )
    if status in (200, 201):
        return int(total_size)
    if status == 308:
        return _parse_gcs_resume_offset(resp_headers)
    raise RuntimeError(f"GCS resumable status query failed (status={status}): {body[:200]}")


def gcs_resumable_upload_file(
    session_url: str,
    file_path: Path,
    content_type: str,
    timeout_seconds: float = 300.0,
    chunk_size: int = 8 * 1024 * 1024,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    total_size = int(file_path.stat().st_size)
    if total_size <= 0:
        raise RuntimeError("Cannot resumable-upload an empty file.")

    chunk_size = max(256 * 1024, int(chunk_size))
    if chunk_size % (256 * 1024) != 0:
        chunk_size = ((chunk_size // (256 * 1024)) + 1) * (256 * 1024)

    offset = 0
    consecutive_failures = 0

    while offset < total_size:
        chunk_end = min(offset + chunk_size, total_size) - 1
        chunk_length = (chunk_end - offset) + 1
        headers = {
            "Content-Type": content_type,
            "Content-Range": f"bytes {int(offset)}-{int(chunk_end)}/{int(total_size)}",
        }

        try:
            status, body, resp_headers = http_put_file_stream(
                session_url,
                file_path,
                headers=headers,
                timeout_seconds=timeout_seconds,
                chunk_size=chunk_size,
                start_offset=offset,
                length=chunk_length,
            )
            consecutive_failures = 0
        except Exception:
            consecutive_failures += 1
            offset = gcs_resumable_query_offset(
                session_url,
                total_size,
                content_type,
                timeout_seconds=min(60.0, timeout_seconds),
            )
            if progress_cb:
                try:
                    progress_cb(int(offset), int(total_size))
                except Exception:
                    pass
            if consecutive_failures >= 5:
                raise
            _sleep_with_jitter(min(5.0, float(consecutive_failures)))
            continue

        if status in (200, 201):
            offset = total_size
            if progress_cb:
                try:
                    progress_cb(int(total_size), int(total_size))
                except Exception:
                    pass
            return

        if status == 308:
            next_offset = _parse_gcs_resume_offset(resp_headers)
            if next_offset <= offset:
                next_offset = gcs_resumable_query_offset(
                    session_url,
                    total_size,
                    content_type,
                    timeout_seconds=min(60.0, timeout_seconds),
                )
            offset = max(offset, next_offset)
            if progress_cb:
                try:
                    progress_cb(int(offset), int(total_size))
                except Exception:
                    pass
            continue

        if status in (408, 429) or 500 <= status <= 599:
            consecutive_failures += 1
            offset = gcs_resumable_query_offset(
                session_url,
                total_size,
                content_type,
                timeout_seconds=min(60.0, timeout_seconds),
            )
            if progress_cb:
                try:
                    progress_cb(int(offset), int(total_size))
                except Exception:
                    pass
            if consecutive_failures >= 5:
                raise RuntimeError(f"GCS resumable upload failed after retries (status={status}): {body[:200]}")
            _sleep_with_jitter(min(5.0, float(consecutive_failures)))
            continue

        raise RuntimeError(f"GCS resumable upload failed (status={status}): {body[:200]}")


@dataclass
class LocalState:
    installed_static: Set[str]
    installed_dynamic: Set[str]
    failed: Set[str]
    # For dynamic deps only: depId -> {destRelativePath, sizeBytes, lastTouchedAtMs}
    lru: Dict[str, Dict[str, Any]]
    # Download retry schedule: depId -> {itemId, resolved, attempts, nextAttemptAtMs, lastError, lastAttemptAtMs}
    retry: Dict[str, Dict[str, Any]]
    # depId -> durable proof for the exact file bytes that were accepted.
    # The stat fingerprint makes normal job admission cheap; the first use in
    # every agent process re-hashes the file so silent on-disk corruption cannot
    # survive an agent restart.
    verified: Dict[str, Dict[str, Any]]
    # Custom-node classes that a successful bundle install proved were
    # available. Every later ComfyUI restart must re-prove this contract before
    # the worker can restore its readiness marker.
    node_bundle_verify_class_types: Set[str]
    # Bundle-scoped proof enables targeted repair of the smallest affected set.
    node_bundle_verify_class_types_by_bundle: Dict[str, Set[str]]

    @staticmethod
    def empty() -> "LocalState":
        return LocalState(
            installed_static=set(),
            installed_dynamic=set(),
            failed=set(),
            lru={},
            retry={},
            verified={},
            node_bundle_verify_class_types=set(),
            node_bundle_verify_class_types_by_bundle={},
        )


@dataclass
class DownloadActivity:
    dep_id: str
    dest_relative_path: str = ""
    expected_bytes: int = 0
    downloaded_bytes: int = 0
    started_at_ms: int = field(default_factory=_now_ms)
    updated_at_ms: int = field(default_factory=_now_ms)
    stage: str = "starting"
    tool: str = ""
    throughput_bytes_per_sec: int = 0
    last_sample_at_ms: int = 0
    last_sample_bytes: int = 0


class GPUCoordinatorError(RuntimeError):
    """Base error for the optional loopback GPU residency coordinator."""


class GPUCoordinatorBusy(GPUCoordinatorError):
    def __init__(self, message: str, retry_after_seconds: float = 2.0) -> None:
        super().__init__(message)
        self.retry_after_seconds = max(0.25, min(60.0, float(retry_after_seconds)))


class GPUCoordinatorUnavailable(GPUCoordinatorError):
    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = int(status) if isinstance(status, int) else None


@dataclass
class GPUCoordinatorLease:
    holder: str
    work_id: str
    epoch: int
    fencing_token: str
    deadline_ms: int
    ttl_ms: int
    pid: Optional[int] = None
    process_start_time: Optional[int] = None
    process_group_id: Optional[int] = None
    admission_ticket_id: Optional[str] = None
    admission_claim_token: Optional[str] = None
    admission_heartbeat_at_ms: int = 0
    metadata_provider: Optional[Callable[[], Dict[str, Any]]] = field(default=None, repr=False)
    renew_stop: threading.Event = field(default_factory=threading.Event, repr=False)
    lost: threading.Event = field(default_factory=threading.Event, repr=False)
    renew_thread: Optional[threading.Thread] = field(default=None, repr=False)
    released: bool = False
    release_confirmed: bool = False
    released_reason: str = ""
    released_keep_warm: bool = False
    release_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    identity_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class GPUCoordinatorClient:
    """Dependency-free client for the loopback fenced-lease API.

    An unset URL is the backward-compatible feature flag. When a URL is set,
    transport failures and busy responses remain fail-closed. Only an explicit
    unsupported response (404/501) may disable an optional coordinator.
    """

    BUSY_STATUSES = {409, 423, 425, 429, 503}

    def __init__(self, base_url: str, required: bool = False, token: str = "") -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        if self.base_url:
            parsed = urllib.parse.urlparse(self.base_url)
            hostname = str(parsed.hostname or "").strip().lower()
            is_loopback = hostname == "localhost"
            if not is_loopback:
                try:
                    is_loopback = ipaddress.ip_address(hostname).is_loopback
                except ValueError:
                    is_loopback = False
            if parsed.scheme not in ("http", "https") or not is_loopback:
                raise ValueError("DM_GPU_COORDINATOR_URL must be an HTTP(S) loopback URL")
        self.required = bool(required)
        self.token = str(token or "").strip()
        self._supported: Optional[bool] = None
        self._status_cache: Dict[str, Any] = {}
        self._status_cache_at_ms = 0
        self._lock = threading.Lock()

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @staticmethod
    def _retry_after_seconds(headers: Any, default: float = 2.0) -> float:
        try:
            raw = headers.get("Retry-After") if headers is not None else None
            if raw is not None:
                return max(0.25, min(60.0, float(raw)))
        except (TypeError, ValueError):
            pass
        return float(default)

    def _request(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 5.0,
    ) -> Tuple[int, Dict[str, Any], Any]:
        url = f"{self.base_url}{endpoint if endpoint.startswith('/') else '/' + endpoint}"
        encoded = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=encoded, headers=self._headers(), method=method.upper())
        try:
            with urllib.request.urlopen(request, timeout=max(0.25, timeout_seconds)) as response:
                raw = response.read()
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
                return int(response.status), parsed if isinstance(parsed, dict) else {}, response.headers
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                parsed = {}
            return int(exc.code), parsed if isinstance(parsed, dict) else {}, exc.headers
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise GPUCoordinatorUnavailable(f"GPU coordinator request failed: {method} {endpoint}: {exc}") from exc

    def _ensure_supported(self) -> bool:
        if not self.configured:
            return False
        with self._lock:
            supported = self._supported
        if supported is not None:
            return bool(supported)
        status, payload, _headers = self._request("GET", "/v1/gpu/status", timeout_seconds=2.0)
        if 200 <= status < 300:
            with self._lock:
                self._supported = True
                self._status_cache = dict(payload)
                self._status_cache_at_ms = _now_ms()
            return True
        if status in (404, 501) and not self.required:
            logging.warning(
                "GPU coordinator endpoint is unsupported (status=%d); retaining legacy GPU behavior.",
                status,
            )
            with self._lock:
                self._supported = False
            return False
        raise GPUCoordinatorUnavailable(f"GPU coordinator status probe failed (status={status})")

    def status(self, max_age_ms: int = 5000) -> Dict[str, Any]:
        if not self.configured:
            return {"configured": False, "supported": False}
        if not self._ensure_supported():
            return {"configured": True, "supported": False}
        now_ms = _now_ms()
        with self._lock:
            cached = dict(self._status_cache)
            cached_at_ms = int(self._status_cache_at_ms or 0)
        if cached and now_ms - cached_at_ms <= max(0, int(max_age_ms)):
            return {"configured": True, "supported": True, **cached}
        status, payload, _headers = self._request("GET", "/v1/gpu/status", timeout_seconds=2.0)
        if status < 200 or status >= 300:
            raise GPUCoordinatorUnavailable(f"GPU coordinator status failed (status={status})")
        with self._lock:
            self._status_cache = dict(payload)
            self._status_cache_at_ms = now_ms
        return {"configured": True, "supported": True, **payload}

    def admission_recovery_status(self) -> Dict[str, Any]:
        if not self._ensure_supported():
            return {"configured": True, "supported": False, "safeToClearAdmission": False}
        status, payload, _headers = self._request(
            "GET",
            "/v1/gpu/admission-recovery",
            timeout_seconds=2.0,
        )
        if status < 200 or status >= 300:
            raise GPUCoordinatorUnavailable(
                f"GPU coordinator admission recovery probe failed (status={status})"
            )
        return {"configured": True, "supported": True, **payload}

    def acquire(
        self,
        holder: str,
        work_id: str,
        ttl_ms: int,
        admission_claim_id: Optional[str] = None,
        admission_ticket_id: Optional[str] = None,
    ) -> Optional[GPUCoordinatorLease]:
        if not self._ensure_supported():
            return None
        status, payload, headers = self._request(
            "POST",
            "/v1/gpu/leases/acquire",
            body={
                "holder": holder,
                "workId": work_id,
                "ttlMs": int(ttl_ms),
                **({"admissionClaimId": admission_claim_id} if admission_claim_id else {}),
                **({"admissionTicketId": admission_ticket_id} if admission_ticket_id else {}),
            },
            timeout_seconds=10.0,
        )
        if status in self.BUSY_STATUSES:
            reason = str(payload.get("reason") or payload.get("error") or "gpu_coordinator_busy")
            raise GPUCoordinatorBusy(reason, self._retry_after_seconds(headers))
        if status < 200 or status >= 300:
            raise GPUCoordinatorUnavailable(f"GPU coordinator acquire failed (status={status})")
        return self._parse_lease(payload, holder, work_id, int(ttl_ms))

    @staticmethod
    def _parse_lease(payload: Dict[str, Any], holder: str, work_id: str, ttl_ms: int) -> GPUCoordinatorLease:
        lease_data = payload.get("lease") if isinstance(payload.get("lease"), dict) else payload
        fencing_token = str(lease_data.get("fencingToken") or "")
        epoch = lease_data.get("epoch")
        if not fencing_token or not isinstance(epoch, (int, float)):
            raise GPUCoordinatorUnavailable("GPU coordinator returned an invalid lease")
        deadline_ms = lease_data.get("deadlineMs")
        return GPUCoordinatorLease(
            holder=holder,
            work_id=work_id,
            epoch=int(epoch),
            fencing_token=fencing_token,
            deadline_ms=int(deadline_ms) if isinstance(deadline_ms, (int, float)) else _now_ms() + int(ttl_ms),
            ttl_ms=int(ttl_ms),
        )

    def reconcile(self, holder: str, work_id: str, metadata: Optional[Dict[str, Any]] = None) -> GPUCoordinatorLease:
        if not self._ensure_supported():
            raise GPUCoordinatorUnavailable("GPU coordinator is not enabled")
        status, payload, _headers = self._request(
            "POST",
            "/v1/gpu/reconcile",
            body={"holder": holder, "workId": work_id, "metadata": dict(metadata or {})},
            timeout_seconds=2.0,
        )
        if status < 200 or status >= 300:
            raise GPUCoordinatorUnavailable(f"GPU coordinator reconcile failed (status={status})")
        return self._parse_lease(payload, holder, work_id, 60_000)

    def renew(self, lease: GPUCoordinatorLease) -> None:
        with lease.identity_lock:
            token = urllib.parse.quote(lease.fencing_token, safe="")
            body: Dict[str, Any] = {"epoch": int(lease.epoch), "ttlMs": int(lease.ttl_ms)}
            if isinstance(lease.pid, int) and lease.pid > 0 and isinstance(lease.process_start_time, int):
                body["metadata"] = {
                    "pid": int(lease.pid),
                    "processStartTime": int(lease.process_start_time),
                }
                if isinstance(lease.process_group_id, int) and lease.process_group_id > 0:
                    body["metadata"]["processGroupId"] = int(lease.process_group_id)
        status, payload, _headers = self._request(
            "POST",
            f"/v1/gpu/leases/{token}/renew",
            body=body,
            timeout_seconds=5.0,
        )
        if status < 200 or status >= 300:
            raise GPUCoordinatorUnavailable(f"GPU coordinator renew failed (status={status})", status=status)
        lease_data = payload.get("lease") if isinstance(payload.get("lease"), dict) else payload
        deadline_ms = lease_data.get("deadlineMs")
        with lease.identity_lock:
            lease.deadline_ms = int(deadline_ms) if isinstance(deadline_ms, (int, float)) else _now_ms() + int(lease.ttl_ms)

    def release(self, lease: GPUCoordinatorLease, reason: str, keep_warm: bool = False) -> None:
        with lease.identity_lock:
            token = urllib.parse.quote(lease.fencing_token, safe="")
            epoch = int(lease.epoch)
        status, _payload, _headers = self._request(
            "POST",
            f"/v1/gpu/leases/{token}/release",
            body={
                "epoch": epoch,
                "reason": str(reason or "work_complete")[:160],
                "keepWarm": bool(keep_warm),
            },
            timeout_seconds=5.0,
        )
        if status not in (200, 204, 404, 409, 410):
            raise GPUCoordinatorUnavailable(f"GPU coordinator release failed (status={status})")


@dataclass
class AgentExecuteLease:
    item_id: str
    lease_id: str
    job_id: str
    execution_attempt: int
    attempt_epoch: int
    started_at_ms: int
    stage: str = "leased"
    lease_order: int = 0
    event_version: int = 0
    command_id: str = ""
    cancel_requested: bool = False
    cancel_reason: str = ""
    prompt_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    command_state: Dict[str, Any] = field(default_factory=dict)
    prefetched_inputs: List[Dict[str, Any]] = field(default_factory=list)
    history_entry: Dict[str, Any] = field(default_factory=dict)
    tmp_root: Optional[str] = None
    ready_at_ms: int = 0
    execute_started_at_ms: int = 0
    prompt_submitted_at_ms: int = 0
    upload_enqueued_at_ms: int = 0
    upload_started_at_ms: int = 0
    gpu_retry_after_ms: int = 0
    gpu_coordinator_lease: Optional[GPUCoordinatorLease] = None
    gpu_admission_ticket_id: Optional[str] = None
    gpu_admission_claim_token: Optional[str] = None


@dataclass(frozen=True)
class AgentSelfUpdateRelease:
    target_version: str
    download_url: str
    sha256: Optional[str] = None


WATCHDOG_RELEASE_ENV_KEYS = {
    "DM_AGENT_URL",
    "DEPENDENCY_AGENT_TARGET_VERSION",
    "DEPENDENCY_AGENT_UPDATE_URL",
    "DEPENDENCY_AGENT_UPDATE_SHA256",
    "DEPENDENCY_AGENT_RELEASE_VERSION",
    "DEPENDENCY_AGENT_RELEASE_SHA256",
    "DM_AGENT_ENV_PATH",
}


class PrlMinerController:
    def __init__(self, workspace: Path, download_timeout_seconds: float, download_chunk_size: int) -> None:
        self.root = Path(workspace) / ".fcs" / "prl"
        self.binary_path = self.root / "prl_gpu_miner"
        self.binary_metadata_path = self.root / "prl_gpu_miner.meta.json"
        self.log_path = self.root / "prl_miner.log"
        self.agent_update_resume_path = self.root / "resume_after_agent_update.json"
        self.gate_dir = self.root / "launch_gates"
        self.download_timeout_seconds = max(30.0, float(download_timeout_seconds))
        self.download_chunk_size = max(1024 * 1024, int(download_chunk_size))
        self._lock = threading.Lock()
        self._process_op_lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._state = "stopped"
        self._desired_state = "stopped"
        self._worker = ""
        self._pool_url = ""
        self._miner_version = ""
        self._miner_kind = DEFAULT_PRL_MINER_KIND
        self._miner_package_type = DEFAULT_PRL_MINER_PACKAGE_TYPE
        self._miner_executable_path = ""
        self._cpu_mining_enabled = False
        self._cpu_threads_reduce = 0
        self._cpu_threads_priority = 2
        self._started_at_ms = 0
        self._stopped_at_ms = 0
        self._last_exit_code: Optional[int] = None
        self._last_error = ""
        self._last_failure_category = ""
        self._last_network_diagnostics: Dict[str, Any] = {}
        self._static_difficulty = ""
        self._static_difficulty_source = ""
        self._static_difficulty_matched_gpu_name = ""
        self._static_difficulty_experiment_id = ""
        self._static_difficulty_experiment_variant = ""
        self._static_difficulty_experiment_bucket: Optional[float] = None
        self._static_difficulty_experiment_allocation_pct: Optional[float] = None
        self._pause_mode = DEFAULT_PRL_MINER_PAUSE_MODE
        self._last_start_payload: Dict[str, Any] = {}
        self._paused_start_payload: Optional[Dict[str, Any]] = None
        self._paused_reason = ""
        self._suspended_for_work = False
        self._suspended_at_ms = 0
        self._pause_stop_count = 0
        self._resume_start_count = 0
        self._suspend_count = 0
        self._resume_signal_count = 0
        self._keep_running_bypass_count = 0
        self._consecutive_failures = 0
        self._last_auto_restart_attempt_ms = 0
        self._last_auto_restart_reason = ""
        self._auto_restart_count = 0
        self._last_agent_update_resume_attempt_ms = 0
        self._pending_gate_path: Optional[Path] = None

    def _reap_locked(self) -> None:
        if self._proc is None:
            return
        return_code = self._proc.poll()
        if return_code is None:
            return
        self._last_exit_code = int(return_code)
        self._proc = None
        gate_path = self._pending_gate_path
        self._pending_gate_path = None
        if gate_path is not None:
            try:
                gate_path.unlink(missing_ok=True)
            except Exception:
                pass
        self._stopped_at_ms = _now_ms()
        if self._desired_state in ("running", "starting"):
            self._state = "failed" if return_code != 0 else "stopped"
            if return_code != 0 and not self._last_error:
                self._last_error = f"miner exited with code {return_code}"
            if return_code != 0:
                runtime_ms = max(0, int(self._stopped_at_ms) - int(self._started_at_ms or 0))
                if runtime_ms >= 60_000:
                    self._consecutive_failures = 1
                else:
                    self._consecutive_failures += 1
        else:
            self._state = "stopped"
            self._desired_state = "stopped"
            self._consecutive_failures = 0

    def _is_running_locked(self) -> bool:
        self._reap_locked()
        return self._proc is not None and self._proc.poll() is None

    def _set_error(self, message: str) -> None:
        with self._lock:
            self._state = "failed"
            self._desired_state = "stopped"
            self._last_error = str(message)[:MAX_AGENT_ERROR_MESSAGE_CHARS]

    def _set_failure_category(self, category: str) -> None:
        self._last_failure_category = str(category or "").strip()[:80]

    def save_agent_update_resume_marker(self, reason: str) -> bool:
        with self._lock:
            self._reap_locked()
            payload = dict(self._last_start_payload) if self._last_start_payload else {}
            should_resume = bool(payload) and (
                self._proc is not None or
                self._state in ("running", "starting", "paused") or
                self._desired_state in ("running", "starting")
            )
        if not should_resume:
            try:
                self.agent_update_resume_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        data = {
            "schemaVersion": 1,
            "createdAtMs": _now_ms(),
            "reason": str(reason or "agent_update")[:80],
            "payload": payload,
        }
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            tmp = self.agent_update_resume_path.with_name(
                f".{self.agent_update_resume_path.name}.{uuid.uuid4().hex}.tmp"
            )
            tmp.write_text(json.dumps(data, sort_keys=True), "utf-8")
            os.replace(str(tmp), str(self.agent_update_resume_path))
            logging.info("Saved idle PRL miner resume marker before dependency agent update.")
            return True
        except Exception as exc:
            logging.warning("Failed saving idle PRL miner resume marker before dependency agent update: %s", exc)
            return False

    def resume_after_agent_update_if_requested(self, reason: str) -> bool:
        now_ms = _now_ms()
        if now_ms - int(self._last_agent_update_resume_attempt_ms or 0) < 30_000:
            return False
        if not self.agent_update_resume_path.exists():
            return False

        with self._lock:
            if self._is_running_locked():
                try:
                    self.agent_update_resume_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False

        try:
            data = json.loads(self.agent_update_resume_path.read_text("utf-8"))
        except Exception as exc:
            logging.warning("Dropping unreadable idle PRL miner agent-update resume marker: %s", exc)
            try:
                self.agent_update_resume_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        created_at_ms = int(data.get("createdAtMs") or 0) if isinstance(data, dict) else 0
        if created_at_ms <= 0 or now_ms - created_at_ms > 24 * 60 * 60 * 1000:
            logging.info("Dropping stale idle PRL miner agent-update resume marker.")
            try:
                self.agent_update_resume_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        payload = data.get("payload") if isinstance(data, dict) else None
        if not isinstance(payload, dict) or not payload:
            try:
                self.agent_update_resume_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        self._last_agent_update_resume_attempt_ms = now_ms
        try:
            logging.info("Resuming idle PRL miner from agent-update marker after %s.", reason or "agent_start")
            self.start(dict(payload))
            try:
                self.agent_update_resume_path.unlink(missing_ok=True)
            except Exception:
                pass
            return True
        except Exception as exc:
            logging.warning("Failed resuming idle PRL miner from agent-update marker: %s", exc)
            return False

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            self._reap_locked()
            pid = self._proc.pid if self._proc is not None and self._proc.poll() is None else None
            running = pid is not None
            out: Dict[str, Any] = {
                "state": self._state,
                "desiredState": self._desired_state,
                "pid": int(pid) if pid else None,
                "lastExitCode": self._last_exit_code,
                "lastError": self._last_error or None,
            }
            if self._worker:
                out["worker"] = self._worker
            if self._pool_url:
                out["poolUrl"] = self._pool_url
            if self._miner_version:
                out["minerVersion"] = self._miner_version
            if self._miner_kind:
                out["minerKind"] = self._miner_kind
            if self._miner_package_type:
                out["minerPackageType"] = self._miner_package_type
            out["minerExecutablePath"] = self._miner_executable_path or None
            if self._miner_kind == "srbminer_multi":
                out["cpuMiningEnabled"] = bool(self._cpu_mining_enabled)
                out["cpuThreadsReduce"] = int(self._cpu_threads_reduce)
                out["cpuThreadsPriority"] = int(self._cpu_threads_priority)
            out["staticDifficulty"] = self._static_difficulty or None
            out["staticDifficultySource"] = self._static_difficulty_source or None
            out["staticDifficultyMatchedGpuName"] = self._static_difficulty_matched_gpu_name or None
            out["staticDifficultyExperimentId"] = self._static_difficulty_experiment_id or None
            out["staticDifficultyExperimentVariant"] = self._static_difficulty_experiment_variant or None
            out["staticDifficultyExperimentBucket"] = (
                float(self._static_difficulty_experiment_bucket)
                if self._static_difficulty_experiment_bucket is not None
                else None
            )
            out["staticDifficultyExperimentAllocationPct"] = (
                float(self._static_difficulty_experiment_allocation_pct)
                if self._static_difficulty_experiment_allocation_pct is not None
                else None
            )
            if self._started_at_ms > 0:
                out["startedAtMs"] = int(self._started_at_ms)
            if self._stopped_at_ms > 0:
                out["stoppedAtMs"] = int(self._stopped_at_ms)
            if self._auto_restart_count > 0:
                out["autoRestartCount"] = int(self._auto_restart_count)
            if self._last_auto_restart_attempt_ms > 0:
                out["lastAutoRestartAttemptAtMs"] = int(self._last_auto_restart_attempt_ms)
            if self._last_auto_restart_reason:
                out["lastAutoRestartReason"] = self._last_auto_restart_reason
            # Keep the key present so RTDB PATCH mirrors delete a stale failure
            # category after a later start succeeds.
            out["lastFailureCategory"] = self._last_failure_category or None
            if self._last_network_diagnostics:
                out["networkDiagnostics"] = self._last_network_diagnostics
            out["pauseMode"] = self._pause_mode
            out["pauseStopCount"] = int(self._pause_stop_count)
            out["resumeStartCount"] = int(self._resume_start_count)
            out["suspendCount"] = int(self._suspend_count)
            out["resumeSignalCount"] = int(self._resume_signal_count)
            out["keepRunningBypassCount"] = int(self._keep_running_bypass_count)
            paused_for_work = bool(self._paused_start_payload or self._suspended_for_work)
            out["pausedForWork"] = paused_for_work
            out["suspendedForWork"] = bool(self._suspended_for_work)
            if paused_for_work and self._paused_reason:
                out["pausedReason"] = self._paused_reason
            else:
                out["pausedReason"] = None
            if self._suspended_for_work and self._suspended_at_ms > 0:
                out["suspendedAtMs"] = int(self._suspended_at_ms)
            else:
                out["suspendedAtMs"] = None
        try:
            existing_pids = self._find_existing_miner_pids()
            out["minerProcessCount"] = int(len(existing_pids))
            if existing_pids:
                out["minerProcessPids"] = [int(existing_pid) for existing_pid in existing_pids[:20]]
            out.update(_query_gpu_telemetry())
            if self.log_path.exists():
                stat = self.log_path.stat()
                out["logSizeBytes"] = int(stat.st_size)
                out["logUpdatedAtMs"] = int(stat.st_mtime * 1000)
                tail = _read_tail_text(self.log_path)
                hps, text = _parse_latest_hashrate_from_text(tail)
                if hps is not None:
                    out["localHashrateHps"] = float(hps)
                    out["localHashrateText"] = text
                if tail:
                    signals = _parse_prl_miner_log_signals(tail)
                    out["recentAcceptedShares"] = int(signals["accepted"])
                    out["recentSubmittedShares"] = int(signals["submitted"])
                    out["recentRejectedShares"] = int(signals["rejected"])
                    out["recentShareErrors"] = int(signals["shareErrors"])
                    if signals["hasShareSignal"]:
                        out["lastShareLikeLogAtMs"] = int(stat.st_mtime * 1000)
                    if signals["poolHealthy"]:
                        out["poolHealthy"] = True
        except Exception as exc:
            out["telemetryError"] = str(exc)[:300]
        out["watch"] = {
            "minerAlive": bool(running or int(out.get("minerProcessCount") or 0) > 0),
            "minerProcessCount": int(out.get("minerProcessCount") or 0),
            "gpuUtilizationPct": out.get("gpuUtilizationPct"),
            "gpuPowerDrawW": out.get("gpuPowerDrawW"),
            "localHashrateHps": out.get("localHashrateHps"),
            "localHashrateText": out.get("localHashrateText"),
            "poolConnected": bool(self._pool_url and out.get("state") in ("running", "starting") and bool(out.get("poolHealthy"))),
            "lastShareLikeLogAtMs": out.get("lastShareLikeLogAtMs"),
            "lastTelemetryAtMs": int(_now_ms()),
            "telemetryError": out.get("telemetryError"),
        }
        return out

    def _cached_package_binary_matches(
        self,
        expected_sha256: str,
        package_type: str,
        executable_path: str,
    ) -> bool:
        if not self.binary_path.exists() or not self.binary_metadata_path.exists():
            return False
        try:
            metadata = json.loads(self.binary_metadata_path.read_text("utf-8"))
        except Exception:
            return False
        return (
            isinstance(metadata, dict) and
            str(metadata.get("packageType") or "") == package_type and
            str(metadata.get("archiveSha256") or "").lower() == expected_sha256 and
            str(metadata.get("executablePath") or "") == executable_path
        )

    def _write_binary_metadata(self, metadata: Dict[str, Any]) -> None:
        tmp = self.binary_metadata_path.with_name(f".{self.binary_metadata_path.name}.{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(metadata, sort_keys=True), "utf-8")
        os.replace(str(tmp), str(self.binary_metadata_path))

    def _install_tar_gz_binary(self, archive_path: Path, executable_path: str) -> None:
        normalized_executable = _normalize_archive_member_path(executable_path)
        if not normalized_executable:
            raise RuntimeError("Missing or invalid PRL miner executable path for tar_gz package.")
        tmp_binary = self.root / f".prl_gpu_miner.{uuid.uuid4().hex}.tmp"
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                selected = None
                for member in archive.getmembers():
                    member_name = _normalize_archive_member_path(member.name)
                    if member_name == normalized_executable:
                        selected = member
                        break
                if selected is None or selected.isdir():
                    raise RuntimeError(f"PRL miner executable '{normalized_executable}' not found in tar_gz package.")
                extracted = archive.extractfile(selected)
                if extracted is None:
                    raise RuntimeError(f"Unable to extract PRL miner executable '{normalized_executable}'.")
                with tmp_binary.open("wb") as out:
                    shutil.copyfileobj(extracted, out)
            os.chmod(tmp_binary, 0o755)
            os.replace(str(tmp_binary), str(self.binary_path))
        finally:
            try:
                if tmp_binary.exists():
                    tmp_binary.unlink()
            except Exception:
                pass

    def _ensure_binary(
        self,
        download_urls: List[str],
        expected_sha256: str,
        package_type: str = DEFAULT_PRL_MINER_PACKAGE_TYPE,
        executable_path: str = "",
    ) -> Path:
        expected = str(expected_sha256 or "").strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", expected):
            raise RuntimeError("Invalid or missing PRL miner SHA-256.")
        urls = _normalize_download_urls("", download_urls)
        if not urls:
            raise RuntimeError("Missing PRL miner download URL.")
        package_type = _normalize_prl_miner_package_type(package_type)
        executable_path = _normalize_archive_member_path(executable_path)
        if package_type == "tar_gz" and not executable_path:
            raise RuntimeError("Missing PRL miner executable path for tar_gz package.")

        self.root.mkdir(parents=True, exist_ok=True)
        if package_type == "tar_gz" and self._cached_package_binary_matches(expected, package_type, executable_path):
            os.chmod(self.binary_path, 0o755)
            with self._lock:
                self._last_failure_category = ""
            return self.binary_path
        if package_type == "binary" and self.binary_path.exists():
            try:
                if sha256_file(self.binary_path).lower() == expected:
                    os.chmod(self.binary_path, 0o755)
                    try:
                        self.binary_metadata_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    with self._lock:
                        self._last_failure_category = ""
                    return self.binary_path
            except Exception:
                pass

        errors: List[str] = []
        for download_url in urls:
            tmp_path = self.root / f".prl_gpu_miner.{uuid.uuid4().hex}.tmp"
            try:
                download_tool = download_file_with_tool_fallback(
                    download_url,
                    tmp_path,
                    timeout_seconds=max(60.0, float(self.download_timeout_seconds)),
                    chunk_size=int(self.download_chunk_size),
                    user_agent=f"dm-agent-prl-miner/{AGENT_VERSION}",
                )
                actual = sha256_file(tmp_path).lower()
                if actual != expected:
                    raise RuntimeError(f"PRL miner checksum mismatch: expected {expected} got {actual}")
                if package_type == "tar_gz":
                    self._install_tar_gz_binary(tmp_path, executable_path)
                    self._write_binary_metadata({
                        "packageType": package_type,
                        "archiveSha256": expected,
                        "executablePath": executable_path,
                        "downloadUrl": download_url,
                    })
                else:
                    os.chmod(tmp_path, 0o755)
                    os.replace(str(tmp_path), str(self.binary_path))
                    try:
                        self.binary_metadata_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                logging.info(
                    "Installed PRL miner binary via %s url=%s sha256=%s packageType=%s executablePath=%s",
                    download_tool,
                    _safe_url_for_logs(download_url),
                    expected,
                    package_type,
                    executable_path or "-",
                )
                with self._lock:
                    self._last_failure_category = ""
                return self.binary_path
            except Exception as exc:
                errors.append(f"{_safe_url_for_logs(download_url)}: {str(exc)[:700]}")
            finally:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass
        with self._lock:
            self._last_failure_category = "miner_binary_download"
        raise RuntimeError("All PRL miner download URLs failed: " + "; ".join(errors)[:MAX_AGENT_ERROR_MESSAGE_CHARS])

    def _signal_process(self, proc: subprocess.Popen, sig: int) -> None:
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), sig)
                return
        except Exception:
            pass
        try:
            os.kill(proc.pid, sig)
        except ProcessLookupError:
            pass

    def _terminate_process(self, proc: subprocess.Popen, sig: int) -> None:
        if sig not in (signal.SIGTERM, signal.SIGKILL):
            self._signal_process(proc, sig)
            return
        if sig == signal.SIGTERM:
            proc.terminate()
        else:
            proc.kill()

    def _find_existing_miner_pids(self) -> List[int]:
        target = str(self.binary_path)
        pids: List[int] = []
        proc_root = Path("/proc")
        if not proc_root.exists():
            return pids
        for entry in proc_root.iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid <= 0 or pid == os.getpid():
                continue
            try:
                raw = (entry / "cmdline").read_bytes()
            except Exception:
                continue
            if not raw:
                continue
            try:
                status_text = (entry / "status").read_text(errors="ignore")
                if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
                    continue
            except Exception:
                pass
            parts = [part.decode("utf-8", errors="ignore") for part in raw.split(b"\0") if part]
            if not parts:
                continue
            if parts[0] == target or target in parts[0] or any(part == target for part in parts[1:]):
                pids.append(pid)
        return sorted(set(pids))

    def _pid_running(self, pid: int) -> bool:
        try:
            status_text = (Path("/proc") / str(pid) / "status").read_text(errors="ignore")
            if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
                return False
        except FileNotFoundError:
            return False
        except Exception:
            pass
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False

    def _terminate_pid(self, pid: int, sig: int) -> None:
        try:
            if hasattr(os, "killpg"):
                process_group_id = os.getpgid(pid)
                if process_group_id > 0 and process_group_id != os.getpgrp():
                    os.killpg(process_group_id, sig)
                    return
        except Exception:
            pass
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass

    def _cleanup_miner_processes(self, reason: str, keep_pid: Optional[int] = None, timeout_seconds: float = 2.0) -> int:
        pids = [pid for pid in self._find_existing_miner_pids() if keep_pid is None or pid != keep_pid]
        if not pids:
            return 0
        logging.warning(
            "Cleaning up %d stray idle PRL miner process(es) reason=%s pids=%s keepPid=%s",
            len(pids),
            reason or "unspecified",
            ",".join(str(pid) for pid in pids[:20]),
            keep_pid or "-",
        )
        for pid in pids:
            self._terminate_pid(pid, signal.SIGTERM)
        deadline = time.time() + max(0.2, min(10.0, float(timeout_seconds or 2.0)))
        while time.time() < deadline:
            if not any(self._pid_running(pid) for pid in pids):
                break
            time.sleep(0.1)
        remaining = [pid for pid in pids if self._pid_running(pid)]
        for pid in remaining:
            self._terminate_pid(pid, signal.SIGKILL)
        return len(pids)

    def start(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._process_op_lock:
            return self._start_serialized(payload)

    def prepare_gated(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Complete all non-GPU mining preparation before lease admission."""
        with self._process_op_lock:
            return self._start_serialized(payload, prepare_only=True)

    def _start_serialized(self, payload: Dict[str, Any], prepare_only: bool = False) -> Dict[str, Any]:
        pool_url = payload.get("poolUrl") if isinstance(payload.get("poolUrl"), str) else ""
        payout_address = payload.get("payoutAddress") if isinstance(payload.get("payoutAddress"), str) else ""
        worker = payload.get("worker") if isinstance(payload.get("worker"), str) else ""
        download_url = payload.get("minerDownloadUrl") if isinstance(payload.get("minerDownloadUrl"), str) else ""
        download_urls = _normalize_download_urls(download_url, payload.get("minerDownloadUrls"))
        miner_sha256 = payload.get("minerSha256") if isinstance(payload.get("minerSha256"), str) else ""
        miner_version = payload.get("minerVersion") if isinstance(payload.get("minerVersion"), str) else ""
        miner_kind = _normalize_prl_miner_kind(payload.get("minerKind"))
        miner_package_type = _normalize_prl_miner_package_type(payload.get("minerPackageType"))
        miner_executable_path = _normalize_archive_member_path(payload.get("minerExecutablePath"))
        cpu_mining_enabled = payload.get("cpuMiningEnabled") is True
        cpu_threads_reduce = _normalize_prl_payload_int(payload.get("cpuThreadsReduce"), 0, 0, 1024)
        cpu_threads_priority = _normalize_prl_payload_int(payload.get("cpuThreadsPriority"), 2, 1, 5)
        static_difficulty = _normalize_prl_static_difficulty(payload.get("staticDifficulty"))
        static_difficulty_source = _clean_prl_payload_string(payload.get("staticDifficultySource"), 80)
        static_difficulty_matched_gpu_name = _clean_prl_payload_string(payload.get("staticDifficultyMatchedGpuName"), 160)
        static_difficulty_experiment_id = _clean_prl_payload_string(payload.get("staticDifficultyExperimentId"), 120)
        static_difficulty_experiment_variant = _clean_prl_payload_string(payload.get("staticDifficultyExperimentVariant"), 80)
        static_difficulty_experiment_bucket = _normalize_prl_payload_float(payload.get("staticDifficultyExperimentBucket"), 0.0, 100.0)
        static_difficulty_experiment_allocation_pct = _normalize_prl_payload_float(
            payload.get("staticDifficultyExperimentAllocationPct"),
            0.0,
            100.0,
        )
        if miner_kind != "alpha_miner":
            static_difficulty = ""
            static_difficulty_source = ""
            static_difficulty_matched_gpu_name = ""
            static_difficulty_experiment_id = ""
            static_difficulty_experiment_variant = ""
            static_difficulty_experiment_bucket = None
            static_difficulty_experiment_allocation_pct = None
        if miner_kind == "srbminer_multi" and cpu_mining_enabled:
            logging.warning(
                "Ignoring SRBMiner CPU mining request for PearlHash: current PRL mining config is GPU-only; keeping --disable-cpu."
            )
            cpu_mining_enabled = False
        if not cpu_mining_enabled:
            cpu_threads_reduce = 0
            cpu_threads_priority = 2
        if miner_kind != "srbminer_multi":
            cpu_mining_enabled = False
            cpu_threads_reduce = 0
            cpu_threads_priority = 2
        pause_mode = _normalize_prl_pause_mode(payload.get("pauseMode"))
        stop_timeout = float(payload.get("stopTimeoutSec")) if isinstance(payload.get("stopTimeoutSec"), (int, float)) else 10.0
        force_restart = payload.get("forceRestart") is True

        if not pool_url:
            raise RuntimeError("Missing PRL pool URL.")
        if not payout_address:
            raise RuntimeError("Missing PRL payout address.")
        if not worker:
            raise RuntimeError("Missing PRL worker name.")
        if not download_urls:
            raise RuntimeError("Missing PRL miner download URL.")
        if not all(str(url).startswith("file://") for url in download_urls):
            try:
                diagnostics = _prl_network_preflight(pool_url, download_urls)
                with self._lock:
                    self._last_network_diagnostics = diagnostics
                    if self._last_failure_category in ("dns_sinkhole", "pool_connectivity"):
                        self._last_failure_category = ""
            except PrlNetworkPreflightError as exc:
                with self._lock:
                    self._last_failure_category = "dns_sinkhole"
                    self._last_network_diagnostics = exc.diagnostics
                raise

        with self._lock:
            already_running = self._is_running_locked()
            same_target = (
                already_running and
                self._pool_url == pool_url and
                self._worker == worker and
                self._miner_kind == miner_kind and
                self._miner_package_type == miner_package_type and
                self._miner_executable_path == miner_executable_path and
                self._cpu_mining_enabled == cpu_mining_enabled and
                self._cpu_threads_reduce == cpu_threads_reduce and
                self._cpu_threads_priority == cpu_threads_priority and
                self._static_difficulty == static_difficulty and
                self._pause_mode == pause_mode and
                (not miner_version or self._miner_version == miner_version)
            )
            if same_target:
                self._state = "running"
                self._desired_state = "running"
                self._static_difficulty = static_difficulty
                self._static_difficulty_source = static_difficulty_source
                self._static_difficulty_matched_gpu_name = static_difficulty_matched_gpu_name
                self._static_difficulty_experiment_id = static_difficulty_experiment_id
                self._static_difficulty_experiment_variant = static_difficulty_experiment_variant
                self._static_difficulty_experiment_bucket = static_difficulty_experiment_bucket
                self._static_difficulty_experiment_allocation_pct = static_difficulty_experiment_allocation_pct
                self._cpu_mining_enabled = cpu_mining_enabled
                self._cpu_threads_reduce = cpu_threads_reduce
                self._cpu_threads_priority = cpu_threads_priority
                self._pause_mode = pause_mode
                self._last_start_payload = dict(payload)
                current_pid = self._proc.pid if self._proc is not None else None
                if not force_restart:
                    self._cleanup_miner_processes("dedupe_prl_miner_same_target", keep_pid=current_pid, timeout_seconds=stop_timeout)
                    if self._suspended_for_work:
                        self._state = "paused"
                    else:
                        self._paused_start_payload = None
                        self._paused_reason = ""
                        self._suspended_for_work = False
                        self._suspended_at_ms = 0
        if same_target and not force_restart:
            if prepare_only:
                return {"alreadyRunning": True, "payload": dict(payload)}
            return self.snapshot()

        if already_running:
            self._stop_serialized(
                "force_restart_prl_miner" if force_restart else "replace_prl_miner",
                timeout_seconds=stop_timeout,
            )

        binary = self._ensure_binary(
            download_urls,
            miner_sha256,
            package_type=miner_package_type,
            executable_path=miner_executable_path,
        )
        self._cleanup_miner_processes("before_prl_miner_start", timeout_seconds=stop_timeout)
        if miner_kind == "srbminer_multi":
            pool_arg = _strip_stratum_scheme(pool_url)
            worker_arg = _pool_safe_worker(worker) or "worker"
            args = [
                str(binary),
                "--disable-cpu",
                "--algorithm",
                "pearlhash",
                "--pool",
                pool_arg,
                "--wallet",
                f"{payout_address}.{worker_arg}",
            ]
        else:
            args = [
                str(binary),
                "--pool",
                pool_url,
                "--address",
                payout_address,
                "--worker",
                worker,
            ]
            if static_difficulty:
                args.extend(["--password", f"x;d={static_difficulty}"])

        if prepare_only:
            return {
                "alreadyRunning": False,
                "args": list(args),
                "payload": dict(payload),
                "worker": worker,
                "poolUrl": pool_url,
                "minerVersion": miner_version,
                "minerKind": miner_kind,
                "minerPackageType": miner_package_type,
                "minerExecutablePath": miner_executable_path,
                "cpuMiningEnabled": cpu_mining_enabled,
                "cpuThreadsReduce": cpu_threads_reduce,
                "cpuThreadsPriority": cpu_threads_priority,
                "staticDifficulty": static_difficulty,
                "staticDifficultySource": static_difficulty_source,
                "staticDifficultyMatchedGpuName": static_difficulty_matched_gpu_name,
                "staticDifficultyExperimentId": static_difficulty_experiment_id,
                "staticDifficultyExperimentVariant": static_difficulty_experiment_variant,
                "staticDifficultyExperimentBucket": static_difficulty_experiment_bucket,
                "staticDifficultyExperimentAllocationPct": static_difficulty_experiment_allocation_pct,
                "pauseMode": pause_mode,
            }

        self.root.mkdir(parents=True, exist_ok=True)
        log_file = self.log_path.open("ab")
        try:
            proc = subprocess.Popen(
                args,
                cwd=str(self.root),
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=os.environ.copy(),
                preexec_fn=os.setsid if hasattr(os, "setsid") else None,
            )
        except Exception:
            log_file.close()
            raise
        finally:
            try:
                log_file.close()
            except Exception:
                pass

        with self._lock:
            self._proc = proc
            self._state = "running"
            self._desired_state = "running"
            self._worker = worker
            self._pool_url = pool_url
            self._miner_version = miner_version
            self._miner_kind = miner_kind
            self._miner_package_type = miner_package_type
            self._miner_executable_path = miner_executable_path
            self._cpu_mining_enabled = cpu_mining_enabled
            self._cpu_threads_reduce = cpu_threads_reduce
            self._cpu_threads_priority = cpu_threads_priority
            self._static_difficulty = static_difficulty
            self._static_difficulty_source = static_difficulty_source
            self._static_difficulty_matched_gpu_name = static_difficulty_matched_gpu_name
            self._static_difficulty_experiment_id = static_difficulty_experiment_id
            self._static_difficulty_experiment_variant = static_difficulty_experiment_variant
            self._static_difficulty_experiment_bucket = static_difficulty_experiment_bucket
            self._static_difficulty_experiment_allocation_pct = static_difficulty_experiment_allocation_pct
            self._pause_mode = pause_mode
            self._started_at_ms = _now_ms()
            self._stopped_at_ms = 0
            self._last_exit_code = None
            self._last_error = ""
            self._last_failure_category = ""
            self._last_start_payload = dict(payload)
            self._paused_start_payload = None
            self._paused_reason = ""
            self._suspended_for_work = False
            self._suspended_at_ms = 0
        logging.info(
            (
                "Started idle PRL miner pid=%s worker=%s pool=%s minerKind=%s packageType=%s "
                "staticDifficulty=%s pauseMode=%s cpuMiningEnabled=%s cpuThreadsReduce=%s cpuThreadsPriority=%s"
            ),
            proc.pid,
            worker,
            pool_url,
            miner_kind,
            miner_package_type,
            static_difficulty or "vardiff",
            pause_mode,
            cpu_mining_enabled,
            cpu_threads_reduce,
            cpu_threads_priority,
        )
        return self.snapshot()

    def start_gated(self, prepared: Dict[str, Any]) -> Dict[str, Any]:
        """Launch a non-CUDA wrapper that cannot exec the miner until released."""
        with self._process_op_lock:
            if prepared.get("alreadyRunning") is True:
                return {"alreadyRunning": True, "snapshot": self.snapshot(), "gatePath": None}
            args = prepared.get("args")
            if not isinstance(args, list) or not args or not all(isinstance(value, str) and value for value in args):
                raise RuntimeError("Invalid prepared PRL miner launch")
            self.root.mkdir(parents=True, exist_ok=True)
            self.gate_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(self.gate_dir, 0o700)
            gate_path = self.gate_dir / f"{uuid.uuid4().hex}.gate"
            gate_path.unlink(missing_ok=True)
            wrapper = (
                "import os,sys,time\n"
                "gate=sys.argv[1]\n"
                "argv=sys.argv[2:]\n"
                "while True:\n"
                " try:\n"
                "  flags=os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)\n"
                "  fd=os.open(gate, flags)\n"
                "  try: data=os.read(fd, 32)\n"
                "  finally: os.close(fd)\n"
                "  if data == b'release\\n':\n"
                "   try: os.unlink(gate)\n"
                "   except FileNotFoundError: pass\n"
                "   os.execvpe(argv[0], argv, os.environ)\n"
                " except FileNotFoundError: pass\n"
                " time.sleep(0.02)\n"
            )
            log_file = self.log_path.open("ab")
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-c", wrapper, str(gate_path), *args],
                    cwd=str(self.root),
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    env=os.environ.copy(),
                    preexec_fn=os.setsid if hasattr(os, "setsid") else None,
                )
            finally:
                log_file.close()
            with self._lock:
                self._proc = proc
                self._pending_gate_path = gate_path
                self._state = "starting"
                self._desired_state = "running"
                self._worker = str(prepared.get("worker") or "")
                self._pool_url = str(prepared.get("poolUrl") or "")
                self._miner_version = str(prepared.get("minerVersion") or "")
                self._miner_kind = str(prepared.get("minerKind") or DEFAULT_PRL_MINER_KIND)
                self._miner_package_type = str(prepared.get("minerPackageType") or DEFAULT_PRL_MINER_PACKAGE_TYPE)
                self._miner_executable_path = str(prepared.get("minerExecutablePath") or "")
                self._cpu_mining_enabled = prepared.get("cpuMiningEnabled") is True
                self._cpu_threads_reduce = int(prepared.get("cpuThreadsReduce") or 0)
                self._cpu_threads_priority = int(prepared.get("cpuThreadsPriority") or 2)
                self._static_difficulty = str(prepared.get("staticDifficulty") or "")
                self._static_difficulty_source = str(prepared.get("staticDifficultySource") or "")
                self._static_difficulty_matched_gpu_name = str(prepared.get("staticDifficultyMatchedGpuName") or "")
                self._static_difficulty_experiment_id = str(prepared.get("staticDifficultyExperimentId") or "")
                self._static_difficulty_experiment_variant = str(prepared.get("staticDifficultyExperimentVariant") or "")
                self._static_difficulty_experiment_bucket = prepared.get("staticDifficultyExperimentBucket")
                self._static_difficulty_experiment_allocation_pct = prepared.get("staticDifficultyExperimentAllocationPct")
                self._pause_mode = str(prepared.get("pauseMode") or DEFAULT_PRL_MINER_PAUSE_MODE)
                self._started_at_ms = _now_ms()
                self._stopped_at_ms = 0
                self._last_exit_code = None
                self._last_error = ""
                self._last_failure_category = ""
                self._last_start_payload = dict(prepared.get("payload") or {})
                self._paused_start_payload = None
                self._paused_reason = ""
                self._suspended_for_work = False
                self._suspended_at_ms = 0
            return {"alreadyRunning": False, "snapshot": self.snapshot(), "gatePath": str(gate_path)}

    def release_start_gate(self, gate_path_raw: str) -> Dict[str, Any]:
        with self._process_op_lock:
            gate_path = Path(str(gate_path_raw or ""))
            with self._lock:
                self._reap_locked()
                proc = self._proc
                expected_gate = self._pending_gate_path
            if proc is None or proc.poll() is not None or expected_gate is None or gate_path != expected_gate:
                raise RuntimeError("PRL miner gate wrapper is no longer active")
            temporary = gate_path.with_name(f".{gate_path.name}.{uuid.uuid4().hex}.tmp")
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(b"release\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, gate_path)
                directory_fd = os.open(self.gate_dir, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                temporary.unlink(missing_ok=True)
            consume_deadline = time.monotonic() + 1.0
            while gate_path.exists() and proc.poll() is None and time.monotonic() < consume_deadline:
                time.sleep(0.01)
            with self._lock:
                if self._proc is not proc or proc.poll() is not None or gate_path.exists():
                    gate_path.unlink(missing_ok=True)
                    raise RuntimeError("PRL miner wrapper exited before gate release")
                self._pending_gate_path = None
                self._state = "running"
                self._desired_state = "running"
            return self.snapshot()

    def cancel_gated_start(self, reason: str) -> None:
        with self._process_op_lock:
            gate_path = self._pending_gate_path
            if gate_path is not None:
                gate_path.unlink(missing_ok=True)
            self._stop_serialized(reason, timeout_seconds=5.0)

    def stop(self, reason: str = "", timeout_seconds: float = 10.0) -> Dict[str, Any]:
        with self._process_op_lock:
            return self._stop_serialized(reason, timeout_seconds=timeout_seconds)

    def _stop_serialized(self, reason: str = "", timeout_seconds: float = 10.0) -> Dict[str, Any]:
        normalized_reason = str(reason or "").strip()
        clear_pause_after_stop = normalized_reason not in PRL_MINER_TRANSIENT_STOP_REASONS
        timeout_seconds = max(1.0, min(120.0, float(timeout_seconds or 10.0)))
        with self._lock:
            self._reap_locked()
            proc = self._proc
            gate_path = self._pending_gate_path
            self._pending_gate_path = None
            if gate_path is not None:
                try:
                    gate_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if proc is None:
                self._state = "stopped"
                self._desired_state = "stopped"
                if self._stopped_at_ms <= 0:
                    self._stopped_at_ms = _now_ms()
                return_noop = True
            else:
                return_noop = False
            if return_noop:
                proc = None
            else:
                self._state = "stopping"
                self._desired_state = "stopped"
                was_suspended = bool(self._suspended_for_work)
        if proc is None:
            if clear_pause_after_stop:
                with self._lock:
                    self._paused_start_payload = None
                    self._paused_reason = ""
                    self._suspended_for_work = False
                    self._suspended_at_ms = 0
            self._cleanup_miner_processes(reason or "stop_without_tracked_proc", timeout_seconds=timeout_seconds)
            return self.snapshot()

        logging.info("Stopping idle PRL miner pid=%s reason=%s", proc.pid, reason or "unspecified")
        try:
            if was_suspended:
                self._signal_process(proc, signal.SIGCONT)
            self._terminate_process(proc, signal.SIGTERM)
            deadline = time.time() + timeout_seconds
            while time.time() < deadline:
                if proc.poll() is not None:
                    break
                time.sleep(0.1)
            if proc.poll() is None:
                self._terminate_process(proc, signal.SIGKILL)
                try:
                    proc.wait(timeout=5.0)
                except Exception:
                    pass
            else:
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    pass
        finally:
            with self._lock:
                self._last_exit_code = int(proc.returncode) if proc.returncode is not None else None
                self._proc = None
                self._state = "stopped"
                self._desired_state = "stopped"
                self._stopped_at_ms = _now_ms()
                self._suspended_for_work = False
                self._suspended_at_ms = 0
                if clear_pause_after_stop:
                    self._paused_start_payload = None
                    self._paused_reason = ""
            self._cleanup_miner_processes(reason or "post_stop_cleanup", timeout_seconds=1.0)
        return self.snapshot()

    def stop_if_running(self, reason: str) -> None:
        with self._process_op_lock:
            with self._lock:
                running = self._is_running_locked()
            if running:
                self._stop_serialized(reason)
            else:
                self._cleanup_miner_processes(reason or "stop_if_running_without_tracked_proc")

    def pause_for_work(
        self,
        reason: str,
        timeout_seconds: Optional[float] = None,
        force_stop: bool = False,
    ) -> None:
        with self._process_op_lock:
            with self._lock:
                running = self._is_running_locked()
                payload = dict(self._last_start_payload) if self._last_start_payload else {}
                proc = self._proc
                pause_mode = "stop_start" if force_stop else self._pause_mode
            if not running:
                self._cleanup_miner_processes(reason or "pause_without_tracked_proc", timeout_seconds=timeout_seconds or 10.0)
                return
            if pause_mode == "keep_running":
                with self._lock:
                    self._keep_running_bypass_count += 1
                    self._paused_reason = str(reason or "")
                logging.info("Keeping idle PRL miner running during %s pauseMode=keep_running", reason or "work")
                return
            if not payload:
                self._stop_serialized(reason, timeout_seconds=timeout_seconds or 10.0)
                return
            if pause_mode == "suspend_resume" and proc is not None:
                logging.info("Suspending idle PRL miner pid=%s reason=%s", proc.pid, reason or "work")
                self._signal_process(proc, signal.SIGSTOP)
                with self._lock:
                    self._state = "paused"
                    self._desired_state = "running"
                    self._paused_start_payload = payload
                    self._paused_reason = str(reason or "")
                    self._suspended_for_work = True
                    self._suspended_at_ms = _now_ms()
                    self._suspend_count += 1
                return
            if timeout_seconds is None:
                raw_timeout = payload.get("stopTimeoutSec")
                timeout_seconds = float(raw_timeout) if isinstance(raw_timeout, (int, float)) else 10.0
            self._stop_serialized(reason, timeout_seconds=timeout_seconds)
            with self._lock:
                self._paused_start_payload = payload
                self._paused_reason = str(reason or "")
                self._pause_stop_count += 1

    def resume_if_paused(self, reason: str) -> bool:
        with self._lock:
            self._reap_locked()
            if self._suspended_for_work and self._proc is not None and self._proc.poll() is None:
                proc = self._proc
                paused_reason = self._paused_reason
            else:
                proc = None
                paused_reason = self._paused_reason
        if proc is not None:
            logging.info("Continuing suspended idle PRL miner after %s", reason or paused_reason or "work")
            self._signal_process(proc, signal.SIGCONT)
            with self._lock:
                self._state = "running"
                self._desired_state = "running"
                self._paused_start_payload = None
                self._paused_reason = ""
                self._suspended_for_work = False
                self._suspended_at_ms = 0
                self._resume_signal_count += 1
            return True
        with self._lock:
            self._reap_locked()
            if self._is_running_locked():
                return False
            payload = dict(self._paused_start_payload) if self._paused_start_payload else None
        if not payload:
            return False
        logging.info("Resuming idle PRL miner after %s", reason or self._paused_reason or "work")
        self.start(payload)
        with self._lock:
            self._resume_start_count += 1
        return True

    def restart_if_desired(self, reason: str) -> bool:
        now_ms = _now_ms()
        with self._lock:
            self._reap_locked()
            if self._is_running_locked():
                return False
            desired = self._desired_state in ("running", "starting")
            payload = dict(self._last_start_payload) if desired and self._last_start_payload else None
            failures = max(0, int(self._consecutive_failures))
            last_attempt_ms = int(self._last_auto_restart_attempt_ms or 0)
        if not payload:
            return False

        if failures <= 1:
            backoff_ms = 2_000
        elif failures == 2:
            backoff_ms = 5_000
        elif failures == 3:
            backoff_ms = 15_000
        elif failures <= 6:
            backoff_ms = 60_000
        elif failures <= 10:
            backoff_ms = 5 * 60_000
        else:
            backoff_ms = 15 * 60_000
        if last_attempt_ms > 0 and now_ms - last_attempt_ms < backoff_ms:
            return False

        with self._lock:
            self._last_auto_restart_attempt_ms = now_ms
            self._last_auto_restart_reason = str(reason or "auto_restart")[:120]
            self._auto_restart_count += 1
        logging.warning(
            "Restarting failed idle PRL miner after %s (worker=%s failures=%d backoffMs=%d)",
            reason or "auto_restart",
            payload.get("worker"),
            failures,
            backoff_ms,
        )
        self.start(payload)
        return True

    def defer_start(self, payload: Dict[str, Any], reason: str) -> None:
        """Remember a backend mining request without starting GPU work yet."""
        with self._lock:
            self._last_start_payload = dict(payload)
            self._desired_state = "starting"
            if self._state not in ("running", "paused"):
                self._state = "stopped"
            self._last_error = str(reason or "gpu_coordinator_busy")[:500]

    def desired_start_payload(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            payload = self._paused_start_payload or self._last_start_payload
            desired = self._desired_state in ("running", "starting") or bool(self._paused_start_payload)
            return dict(payload) if desired and payload else None

    def handle_command(self, item: Dict[str, Any], ack_func: Callable[..., Dict[str, Any]]) -> None:
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        if not item_id or not lease_id:
            return
        action = str(payload.get("action") or "").strip().lower()
        try:
            if action == "start":
                self.start(payload)
            elif action == "stop":
                timeout = float(payload.get("stopTimeoutSec")) if isinstance(payload.get("stopTimeoutSec"), (int, float)) else 10.0
                item_reason = item.get("reason") if isinstance(item.get("reason"), str) else ""
                reason = str(payload.get("reason") or item_reason or "backend_stop_command")
                if reason.strip() in PRL_MINER_TRANSIENT_STOP_REASONS:
                    self.pause_for_work(
                        reason,
                        timeout_seconds=timeout,
                        force_stop=payload.get("forceStop") is True,
                    )
                else:
                    self.stop(reason, timeout_seconds=timeout)
            else:
                ack_func(item_id, lease_id, "command_ignored_stale")
                return
            ack_func(item_id, lease_id, "command_succeeded")
        except Exception as exc:
            self._set_error(str(exc))
            error_code = "prl_miner_failed"
            if isinstance(exc, PrlNetworkPreflightError):
                error_code = "prl_network_preflight_failed"
            ack_func(
                item_id,
                lease_id,
                "command_failed",
                error_code=error_code,
                error_message=str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS],
            )


class ComfyNodeTimingCollector:
    """Best-effort, dependency-free reader for ComfyUI's local WebSocket events.

    This intentionally measures wall time between native ``executing`` boundaries.
    It never synchronizes CUDA, polls GPU state, or participates in job completion.
    Any telemetry failure is recorded as partial coverage and must remain fail-open.
    """

    _WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(
        self,
        base_url: str,
        client_id: str,
        prompt_id: str,
        workflow: Dict[str, Any],
        max_rows: int = MAX_COMFY_NODE_TIMING_ROWS,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.client_id = str(client_id)
        self.prompt_id = str(prompt_id)
        self.max_rows = max(1, min(MAX_COMFY_NODE_TIMING_ROWS, int(max_rows)))
        self._node_meta: Dict[str, Dict[str, str]] = {}
        for raw_id, raw_node in workflow.items():
            if not isinstance(raw_node, dict):
                continue
            node_id = str(raw_id)[:64]
            class_type = str(raw_node.get("class_type") or "")[:96]
            meta = raw_node.get("_meta") if isinstance(raw_node.get("_meta"), dict) else {}
            title = str(meta.get("title") or "")[:96]
            self._node_meta[node_id] = {
                "classType": class_type,
                "title": title,
            }

        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._terminal = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None
        self._recv_buffer = bytearray()
        self._fragments = bytearray()
        self._fragment_opcode: Optional[int] = None
        self._capture_started_ns = time.monotonic_ns()
        self._execution_started_ns: Optional[int] = None
        self._execution_finished_ns: Optional[int] = None
        self._active: Optional[Dict[str, Any]] = None
        self._spans: List[Dict[str, Any]] = []
        self._cached_node_ids: Set[str] = set()
        self._terminal_event = ""
        self._issues: Set[str] = set()
        self._connected = False

    def start(self, timeout_seconds: float = 3.0) -> bool:
        try:
            self._connect(timeout_seconds=max(0.5, min(10.0, timeout_seconds)))
            self._thread = threading.Thread(
                target=self._run,
                name=f"comfy-node-timing-{self.prompt_id[:8]}",
                daemon=True,
            )
            self._thread.start()
            if not self._ready.wait(timeout=max(0.5, min(10.0, timeout_seconds))):
                self._record_issue("status_handshake_timeout")
                self.stop()
                return False
            return True
        except Exception as exc:
            self._record_issue("connect_failed")
            logging.debug("Comfy node timing unavailable for prompt %s: %s", self.prompt_id, exc)
            self.stop()
            return False

    def wait_terminal(self, timeout_seconds: float = 1.0) -> bool:
        return self._terminal.wait(timeout=max(0.0, min(2.0, timeout_seconds)))

    def finish_from_history(self, terminal_status: str, observed_ns: Optional[int] = None) -> None:
        """Close an otherwise-open span after authoritative history is terminal."""
        now_ns = int(observed_ns) if isinstance(observed_ns, int) and observed_ns > 0 else time.monotonic_ns()
        with self._lock:
            if self._execution_finished_ns is None:
                self._close_active_locked(now_ns, "history_terminal", complete=False)
                self._execution_finished_ns = now_ns
                self._terminal_event = f"history_{str(terminal_status or 'terminal')[:32]}"
                self._issues.add("missing_ws_terminal")
        self._terminal.set()

    def stop(self) -> None:
        self._stop.set()
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            spans = [dict(row) for row in self._spans]
            execution_started_ns = self._execution_started_ns
            execution_finished_ns = self._execution_finished_ns
            cached_count = len(self._cached_node_ids)
            issues = sorted(self._issues)
            terminal_event = self._terminal_event

        aggregate: Dict[str, Dict[str, Any]] = {}
        for span in spans:
            node_id = str(span.get("nodeId") or "")[:64]
            if not node_id:
                continue
            duration_ms = max(0, int(span.get("durationMs") or 0))
            row = aggregate.get(node_id)
            if row is None:
                row = {
                    "nodeId": node_id,
                    "classType": str(span.get("classType") or "")[:96],
                    "title": str(span.get("title") or "")[:96],
                    "durationMs": 0,
                    "runs": 0,
                }
                aggregate[node_id] = row
            row["durationMs"] = int(row["durationMs"]) + duration_ms
            row["runs"] = int(row["runs"]) + 1

        rows = sorted(aggregate.values(), key=lambda row: (-int(row["durationMs"]), str(row["nodeId"])))
        total_node_ms = sum(int(row["durationMs"]) for row in rows)
        kept = rows[:self.max_rows]
        omitted = rows[self.max_rows:]
        for row in kept:
            if not row.get("classType"):
                row.pop("classType", None)
            if not row.get("title"):
                row.pop("title", None)
            if row.get("runs") == 1:
                row.pop("runs", None)

        wall_ms: Optional[int] = None
        if execution_started_ns is not None and execution_finished_ns is not None:
            wall_ms = max(0, int(round((execution_finished_ns - execution_started_ns) / 1_000_000)))
        complete = bool(terminal_event) and execution_started_ns is not None and execution_finished_ns is not None and not issues
        payload: Dict[str, Any] = {
            "version": 1,
            "source": "comfy_websocket_boundaries_v1",
            "complete": complete,
            "nodeCount": len(rows),
            "cachedNodeCount": cached_count,
            "totalNodeMs": total_node_ms,
            "truncated": len(omitted) > 0,
            "nodes": kept,
        }
        if wall_ms is not None:
            payload["wallMs"] = wall_ms
            payload["unattributedMs"] = max(0, wall_ms - total_node_ms)
        if terminal_event:
            payload["terminalEvent"] = terminal_event[:48]
        if omitted:
            payload["omittedNodeCount"] = len(omitted)
            payload["omittedNodeMs"] = sum(int(row["durationMs"]) for row in omitted)
        if issues:
            payload["issues"] = issues[:8]
        return payload

    def _record_issue(self, issue: str) -> None:
        with self._lock:
            self._issues.add(str(issue)[:64])

    def _connect(self, timeout_seconds: float) -> None:
        parsed = urllib.parse.urlparse(self.base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise RuntimeError(f"Unsupported local Comfy URL for WebSocket: {self.base_url}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        sock = socket.create_connection((parsed.hostname, port), timeout=timeout_seconds)
        self._sock = sock
        if parsed.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=parsed.hostname)
            self._sock = sock
        sock.settimeout(1.0)

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        base_path = (parsed.path or "").rstrip("/")
        target = f"{base_path}/ws?{urllib.parse.urlencode({'clientId': self.client_id})}"
        host = parsed.hostname if port in (80, 443) else f"{parsed.hostname}:{port}"
        request = (
            f"GET {target} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        sock.sendall(request)
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("Comfy WebSocket closed during handshake")
            response.extend(chunk)
            if len(response) > 65536:
                raise RuntimeError("Comfy WebSocket handshake response was too large")
        header_bytes, remainder = bytes(response).split(b"\r\n\r\n", 1)
        header_text = header_bytes.decode("iso-8859-1")
        if not header_text.startswith("HTTP/1.1 101"):
            raise RuntimeError(f"Comfy WebSocket handshake failed: {header_text.splitlines()[0]}")
        headers: Dict[str, str] = {}
        for line in header_text.split("\r\n")[1:]:
            if ":" in line:
                name, value = line.split(":", 1)
                headers[name.strip().lower()] = value.strip()
        expected_accept = base64.b64encode(hashlib.sha1((key + self._WS_GUID).encode("ascii")).digest()).decode("ascii")
        if headers.get("sec-websocket-accept") != expected_accept:
            raise RuntimeError("Comfy WebSocket handshake accept key mismatch")
        self._recv_buffer.extend(remainder)
        with self._lock:
            self._connected = True
        self._send_control(
            0x1,
            json.dumps({"type": "feature_flags", "data": {"supports_preview_metadata": True}}).encode("utf-8"),
        )

    def _recv_exact(self, size: int) -> bytes:
        while len(self._recv_buffer) < size:
            sock = self._sock
            if sock is None:
                raise EOFError("WebSocket stopped")
            chunk = sock.recv(min(65536, size - len(self._recv_buffer)))
            if not chunk:
                raise EOFError("WebSocket closed")
            self._recv_buffer.extend(chunk)
        out = bytes(self._recv_buffer[:size])
        del self._recv_buffer[:size]
        return out

    def _recv_frame(self) -> Tuple[int, bool, bytes]:
        head = self._recv_exact(2)
        first, second = head[0], head[1]
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]
        if length > MAX_COMFY_WS_FRAME_BYTES:
            raise RuntimeError(f"Comfy WebSocket frame exceeds {MAX_COMFY_WS_FRAME_BYTES} bytes")
        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(int(length))
        if mask:
            payload = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))
        return opcode, fin, payload

    def _send_control(self, opcode: int, payload: bytes = b"") -> None:
        sock = self._sock
        if sock is None:
            return
        payload = payload[:125]
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))
        frame = bytes([0x80 | (opcode & 0x0F), 0x80 | len(payload)]) + mask + masked
        with self._send_lock:
            sock.sendall(frame)

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    opcode, fin, payload = self._recv_frame()
                except socket.timeout:
                    continue
                if opcode == 0x8:
                    if not self._terminal.is_set():
                        now_ns = time.monotonic_ns()
                        with self._lock:
                            self._close_active_locked(now_ns, "websocket_closed", complete=False)
                            self._issues.add("websocket_disconnected")
                    break
                if opcode == 0x9:
                    self._send_control(0xA, payload)
                    continue
                if opcode == 0xA:
                    continue
                if opcode in (0x1, 0x2):
                    if self._fragment_opcode is not None:
                        raise RuntimeError("Comfy WebSocket started a data frame before finishing the prior message")
                    self._fragment_opcode = opcode
                    self._fragments = bytearray(payload)
                elif opcode == 0x0 and self._fragment_opcode is not None:
                    self._fragments.extend(payload)
                else:
                    continue
                if len(self._fragments) > MAX_COMFY_WS_FRAME_BYTES:
                    raise RuntimeError(f"Comfy WebSocket fragmented message exceeds {MAX_COMFY_WS_FRAME_BYTES} bytes")
                if not fin:
                    continue
                complete_opcode = self._fragment_opcode
                complete_payload = bytes(self._fragments)
                self._fragment_opcode = None
                self._fragments.clear()
                if complete_opcode != 0x1:
                    continue
                try:
                    message = json.loads(complete_payload.decode("utf-8"))
                except Exception:
                    continue
                self._handle_message(message, time.monotonic_ns())
        except (EOFError, OSError):
            if not self._stop.is_set() and not self._terminal.is_set():
                now_ns = time.monotonic_ns()
                with self._lock:
                    self._close_active_locked(now_ns, "websocket_disconnected", complete=False)
                    self._issues.add("websocket_disconnected")
        except Exception as exc:
            if not self._stop.is_set() and not self._terminal.is_set():
                now_ns = time.monotonic_ns()
                with self._lock:
                    self._close_active_locked(now_ns, "websocket_reader_failed", complete=False)
                    self._issues.add("websocket_reader_failed")
                logging.debug("Comfy node timing reader failed for prompt %s: %s", self.prompt_id, exc)
        finally:
            with self._lock:
                self._connected = False

    def _handle_message(self, message: Any, now_ns: int) -> None:
        if not isinstance(message, dict):
            return
        event_type = message.get("type")
        data = message.get("data") if isinstance(message.get("data"), dict) else {}
        if event_type == "status":
            if not self._ready.is_set():
                if data.get("sid") == self.client_id:
                    self._ready.set()
                else:
                    self._record_issue("status_sid_mismatch")
            return
        if data.get("prompt_id") != self.prompt_id:
            return
        with self._lock:
            if event_type == "execution_start":
                self._execution_started_ns = now_ns
                return
            if event_type == "execution_cached":
                nodes = data.get("nodes")
                if isinstance(nodes, list):
                    for node_id in nodes[:512]:
                        if isinstance(node_id, (str, int)):
                            self._cached_node_ids.add(str(node_id)[:64])
                return
            if event_type == "executing":
                node = data.get("node")
                if node is None:
                    if self._execution_finished_ns is None:
                        self._close_active_locked(now_ns, "execution_complete", complete=True)
                        self._execution_finished_ns = now_ns
                        self._terminal_event = "executing_complete"
                        self._terminal.set()
                    return
                self._close_active_locked(now_ns, "next_node", complete=True)
                node_id = str(node)[:64]
                display_node = str(data.get("display_node") or node_id)[:64]
                metadata = self._node_meta.get(node_id) or self._node_meta.get(display_node) or {}
                if self._execution_started_ns is None:
                    self._execution_started_ns = now_ns
                    self._issues.add("missing_execution_start")
                self._active = {
                    "nodeId": node_id,
                    "classType": metadata.get("classType", ""),
                    "title": metadata.get("title", ""),
                    "startNs": now_ns,
                }
                return
            terminal_event = event_type in ("execution_success", "execution_interrupted") or (
                event_type == "execution_error" and isinstance(data.get("timestamp"), (int, float))
            )
            if terminal_event and self._execution_finished_ns is None:
                self._close_active_locked(now_ns, str(event_type), complete=True)
                self._execution_finished_ns = now_ns
                self._terminal_event = str(event_type)
                self._terminal.set()

    def _close_active_locked(self, end_ns: int, end_reason: str, complete: bool) -> None:
        active = self._active
        self._active = None
        if not active:
            return
        start_ns = int(active.get("startNs") or end_ns)
        self._spans.append({
            "nodeId": str(active.get("nodeId") or "")[:64],
            "classType": str(active.get("classType") or "")[:96],
            "title": str(active.get("title") or "")[:96],
            "durationMs": max(0, int(round((end_ns - start_ns) / 1_000_000))),
            "complete": bool(complete),
            "endReason": str(end_reason)[:48],
        })


class DependencyAgent:
    def __init__(self) -> None:
        self.api_base_url = (_env_str("FCS_API_BASE_URL") or "").rstrip("/")
        self.server_type = (_env_str("SERVER_TYPE") or "").strip()
        self.mining_only = _env_bool("DM_MINING_ONLY", False)
        self.shared_secret = _env_str("DEPENDENCY_MANAGER_SHARED_SECRET")
        self.instance_bootstrap_token = _env_str("DM_INSTANCE_BOOTSTRAP_TOKEN") or _env_str("AGENT_INSTANCE_BOOTSTRAP_TOKEN")
        self.hf_token = _env_str("HF_TOKEN")
        self.civitai_token = _env_str("CIVITAI_TOKEN")
        self.instance_id = _env_str("DM_INSTANCE_ID")
        self.instance_ip = _env_str("DM_INSTANCE_IP")
        self.provider_metadata = detect_provider_metadata()
        self.workspace = Path(_env_str("WORKSPACE", "/workspace") or "/workspace")
        self.comfyui_dir = Path(_env_str("DM_COMFYUI_DIR") or str(self.workspace / "ComfyUI"))
        self.state_path = Path(_env_str("DM_STATE_PATH") or str(self.workspace / "dependency_agent_state.json"))
        self.poll_seconds = _env_float("DM_POLL_SECONDS", 5.0)
        self.heartbeat_seconds = _env_float("DM_HEARTBEAT_SECONDS", 30.0)
        self.max_parallel = max(1, min(4, _env_int("MAX_PARALLEL_DOWNLOADS", 3)))
        self.verbose_progress = (_env_str("DM_VERBOSE_PROGRESS") or "").lower() in ("1", "true", "yes", "on")
        self.download_debug = _env_bool("DM_DOWNLOAD_DEBUG", False)
        self.download_tool = (_env_str("DM_DOWNLOAD_TOOL") or "auto").strip().lower()

        self.download_timeout_seconds = max(30.0, min(3600.0, _env_float("DM_DOWNLOAD_TIMEOUT_SECONDS", 300.0)))
        chunk_mib = _env_int("DM_DOWNLOAD_CHUNK_MIB", 1)
        chunk_mib = max(1, min(32, chunk_mib))
        self.download_chunk_size = int(chunk_mib) * 1024 * 1024
        self._idle_prl_miner = PrlMinerController(
            self.workspace,
            self.download_timeout_seconds,
            self.download_chunk_size,
        )
        self.idle_prl_free_comfy_before_start = _env_bool("DM_IDLE_PRL_FREE_COMFY_BEFORE_START", True)
        self.idle_prl_free_comfy_min_interval_ms = int(
            max(5.0, min(600.0, _env_float("DM_IDLE_PRL_FREE_COMFY_MIN_INTERVAL_SECONDS", 30.0))) * 1000
        )
        self.idle_prl_free_comfy_timeout_seconds = max(
            2.0,
            min(30.0, _env_float("DM_IDLE_PRL_FREE_COMFY_TIMEOUT_SECONDS", 10.0)),
        )
        self._last_idle_prl_comfy_free_ms = 0
        coordinator_url = (_env_str("DM_GPU_COORDINATOR_URL") or "").strip().rstrip("/")
        coordinator_required = _env_bool("DM_GPU_COORDINATOR_REQUIRED", False)
        if coordinator_required and not coordinator_url:
            coordinator_url = "http://127.0.0.1:8189"
        self.gpu_coordinator_lease_ttl_ms = int(
            max(15.0, min(3600.0, _env_float("DM_GPU_COORDINATOR_LEASE_TTL_SECONDS", 60.0))) * 1000
        )
        self.gpu_admission_mode = (_env_str("GPU_ADMISSION_MODE") or "off").strip().lower()
        if self.gpu_admission_mode not in ("off", "shadow", "enforcing"):
            raise RuntimeError("GPU_ADMISSION_MODE must be off, shadow, or enforcing")
        self.gpu_admission_max_depth = max(1, min(256, _env_int("DM_GPU_ADMISSION_MAX_DEPTH", 64)))
        self.gpu_admission_ticket_stale_ms = int(
            max(5.0, min(120.0, _env_float("DM_GPU_ADMISSION_TICKET_STALE_SECONDS", 20.0))) * 1000
        )
        self.gpu_admission_recovery_ms = int(
            max(10.0, min(300.0, _env_float("DM_GPU_ADMISSION_RECOVERY_SECONDS", 45.0))) * 1000
        )
        self.gpu_admission_heartbeat_seconds = max(
            1.0, min(20.0, _env_float("DM_GPU_ADMISSION_HEARTBEAT_SECONDS", 5.0))
        )
        self._gpu_coordinator = GPUCoordinatorClient(
            coordinator_url,
            required=coordinator_required,
            token=_env_str("DM_GPU_COORDINATOR_TOKEN") or "",
        )
        self._mining_gpu_lease: Optional[GPUCoordinatorLease] = None
        self._mining_gpu_lease_lock = threading.RLock()
        self._mining_gpu_retry_after_ms = 0
        self.comfy_full_trim_mem_available_bytes = int(
            max(1.0, min(256.0, _env_float("DM_COMFY_FULL_TRIM_MEM_AVAILABLE_GIB", 24.0))) * 1024 ** 3
        )
        self.comfy_full_trim_memory_psi_avg10 = max(
            0.0,
            min(100.0, _env_float("DM_COMFY_FULL_TRIM_MEMORY_PSI_AVG10", 5.0)),
        )

        # Agent control channel knobs (execute pull mode).
        self.agent_control_enabled = _env_bool("DM_AGENT_CONTROL_ENABLED", True)
        self.agent_runtime_config_overrides_env = _env_bool("DM_AGENT_RUNTIME_CONFIG_OVERRIDES_ENV", True)
        self._agent_poll_seconds_env = _env_str("DM_AGENT_POLL_SECONDS") is not None
        self._agent_heartbeat_seconds_env = _env_str("DM_AGENT_HEARTBEAT_SECONDS") is not None
        self._agent_idle_heartbeat_seconds_env = _env_str("DM_AGENT_IDLE_HEARTBEAT_SECONDS") is not None
        self._agent_waiting_deps_event_seconds_env = _env_str("DM_AGENT_WAITING_DEPS_EVENT_SECONDS") is not None
        self._agent_progress_event_seconds_env = _env_str("DM_AGENT_PROGRESS_EVENT_SECONDS") is not None
        self.agent_poll_seconds = max(0.5, _env_float("DM_AGENT_POLL_SECONDS", 2.0))
        self.agent_heartbeat_seconds = max(2.0, _env_float("DM_AGENT_HEARTBEAT_SECONDS", 8.0))
        self.agent_idle_heartbeat_seconds = max(
            self.agent_heartbeat_seconds,
            min(300.0, _env_float("DM_AGENT_IDLE_HEARTBEAT_SECONDS", self.agent_heartbeat_seconds)),
        )
        self.agent_queue_wait_sec = max(0, min(20, _env_int("DM_AGENT_QUEUE_WAIT_SEC", 2)))
        self.agent_rtdb_signal_wait_enabled = _env_bool("DM_AGENT_RTDB_SIGNAL_WAIT_ENABLED", True)
        self.agent_rtdb_queue_claim_enabled = _env_bool("DM_AGENT_RTDB_QUEUE_CLAIM_ENABLED", True)
        self.agent_rtdb_lease_heartbeat_enabled = _env_bool("DM_AGENT_RTDB_LEASE_HEARTBEAT_ENABLED", True)
        self.agent_rtdb_signal_safety_min_seconds = max(
            15.0,
            min(1800.0, _env_float("DM_AGENT_RTDB_SIGNAL_SAFETY_MIN_SECONDS", 900.0)),
        )
        # A queue signal always triggers an immediate (cheap, direct-RTDB) queue claim,
        # but it must NOT force a full /agent/heartbeat on every bump: under active job
        # churn the server bumps the agentQueue signal many times per second, and forcing
        # a heartbeat per bump turned normal traffic into a ~24x coordinationApi request +
        # RTDB egress storm (incident 2026-07-05). This floor caps how often a signal may
        # force a heartbeat; default = the normal heartbeat cadence, so signals never
        # accelerate heartbeats past the timer. Set to 0 to restore forcing on every signal.
        self._agent_signal_heartbeat_min_seconds_env = _env_str("DM_AGENT_SIGNAL_HEARTBEAT_MIN_SECONDS") is not None
        self.agent_signal_heartbeat_min_seconds = max(
            0.0,
            _env_float("DM_AGENT_SIGNAL_HEARTBEAT_MIN_SECONDS", self.agent_heartbeat_seconds),
        )
        self.coordination_runtime_full_sync_seconds = max(
            60.0,
            min(3600.0, _env_float("DM_COORDINATION_RUNTIME_FULL_SYNC_SECONDS", 900.0)),
        )
        self.agent_full_capacity_poll_seconds = max(5.0, min(300.0, _env_float("DM_AGENT_FULL_CAPACITY_POLL_SECONDS", 30.0)))
        self.agent_waiting_deps_event_ms = int(max(15.0, _env_float("DM_AGENT_WAITING_DEPS_EVENT_SECONDS", 60.0)) * 1000)
        self.agent_dependency_wait_poll_seconds = max(0.1, min(5.0, _env_float("DM_AGENT_DEPENDENCY_WAIT_POLL_SECONDS", 0.5)))
        self.agent_progress_event_ms = int(max(15.0, _env_float("DM_AGENT_PROGRESS_EVENT_SECONDS", 60.0)) * 1000)
        self.agent_api_retry_attempts = max(1, min(10, _env_int("DM_AGENT_API_RETRY_ATTEMPTS", 5)))
        self.agent_api_retry_base_seconds = max(0.1, min(10.0, _env_float("DM_AGENT_API_RETRY_BASE_SECONDS", 1.0)))
        self.agent_api_retry_max_seconds = max(
            self.agent_api_retry_base_seconds,
            min(120.0, _env_float("DM_AGENT_API_RETRY_MAX_SECONDS", 20.0)),
        )
        self.agent_terminal_event_retry_attempts = max(1, min(20, _env_int("DM_AGENT_TERMINAL_EVENT_RETRY_ATTEMPTS", 8)))
        self.agent_upload_retry_attempts = max(1, min(8, _env_int("DM_AGENT_UPLOAD_RETRY_ATTEMPTS", 4)))
        self.agent_local_comfy_base_url = (_env_str("DM_LOCAL_COMFY_BASE_URL", "http://127.0.0.1:8188") or "http://127.0.0.1:8188").rstrip("/")
        # Post-job Comfy recycle is opt-in only. video_gen_v4 previously
        # defaulted on, and its Vast template baked
        # DM_RESTART_COMFY_AFTER_SUCCESSFUL_JOB=true, as an OOM workaround
        # for an earlier pin. Ignore those pins so already-rented workers
        # drop the recycle on self-update. Re-enable with
        # DM_FORCE_RESTART_COMFY_AFTER_SUCCESSFUL_JOB /
        # DM_FORCE_RESTART_COMFY_AFTER_FAILED_JOB if a current-pin leak
        # shows it is still needed.
        self.restart_comfy_after_successful_job = _env_bool(
            "DM_FORCE_RESTART_COMFY_AFTER_SUCCESSFUL_JOB",
            False,
        )
        self.restart_comfy_after_failed_job = _env_bool(
            "DM_FORCE_RESTART_COMFY_AFTER_FAILED_JOB",
            False,
        )
        self.comfy_node_timing_enabled = _env_bool(
            "DM_COMFY_NODE_TIMING_ENABLED",
            self.server_type in ("video_gen_v3", "video_gen_v4"),
        )
        self.comfy_node_timing_max_rows = max(
            1,
            min(MAX_COMFY_NODE_TIMING_ROWS, _env_int("DM_COMFY_NODE_TIMING_MAX_ROWS", MAX_COMFY_NODE_TIMING_ROWS)),
        )
        self.local_comfy_allow_discovery = _env_bool(
            "DM_LOCAL_COMFY_ALLOW_DISCOVERY",
            self.server_type not in ("video_gen_v2", "video_gen_v3", "video_gen_v4"),
        )
        self._agent_local_readiness_file_env = _env_str("DM_LOCAL_READINESS_FILE")
        default_readiness_file = "provisioned_furry_all.txt" if (self.server_type or "").strip() == "video_gen_v2" else "provisioning_complete.txt"
        self.agent_local_readiness_file = self._agent_local_readiness_file_env or default_readiness_file
        self.agent_max_execute_workers = 0 if self.mining_only else max(1, min(8, _env_int("DM_AGENT_MAX_EXEC_WORKERS", 2)))
        default_upload_workers = max(4, int(self.agent_max_execute_workers) * 2)
        self.agent_max_upload_workers = 0 if self.mining_only else max(1, min(16, _env_int("DM_AGENT_MAX_UPLOAD_WORKERS", default_upload_workers)))
        self.asset_gen_v5_script = _env_str("DM_ASSET_GEN_V5_SCRIPT")
        self.furgenpub_raw_base_url = self._resolve_furgenpub_raw_base_url()
        self._resolved_local_comfy_base_url = self.agent_local_comfy_base_url
        self._last_local_comfy_discovery_ms = 0
        self._comfy_queue_summary_ttl_ms = max(1000, min(10000, int(_env_float("DM_COMFY_QUEUE_SUMMARY_TTL_SECONDS", 2.0) * 1000)))
        self._last_comfy_queue_summary: Dict[str, Any] = {}
        self._last_comfy_queue_summary_at_ms = 0
        self._comfy_runtime_snapshot_ttl_ms = max(
            5000,
            min(300000, int(_env_float("DM_COMFY_RUNTIME_SNAPSHOT_TTL_SECONDS", 30.0) * 1000)),
        )
        self._last_comfy_runtime_snapshot: Dict[str, Any] = {}
        self._last_comfy_runtime_snapshot_at_ms = 0
        self.node_contract_probe_ttl_ms = int(
            max(10.0, min(600.0, _env_float("DM_NODE_CONTRACT_PROBE_TTL_SECONDS", 60.0))) * 1000
        )
        # A class missing from /object_info is only treated as genuinely absent
        # once it has stayed missing, with ComfyUI continuously reachable, for
        # this long. ComfyUI serves /object_info while it is still importing
        # custom nodes, so a single probe taken during a restart reports classes
        # that are merely not imported YET. Acting on that snapshot quarantines a
        # healthy worker and reinstalls healthy bundles (2026-08-19, instance
        # 48062383). Must comfortably exceed custom-node import time.
        self.node_contract_missing_confirm_ms = int(
            max(0.0, min(900.0, _env_float("DM_NODE_CONTRACT_MISSING_CONFIRM_SECONDS", 180.0))) * 1000
        )
        self._node_contract_probe_cache: Dict[str, Any] = {}
        # contractHash -> (frozenset(missing), first time that set was observed).
        # Keyed per contract because the probe is called with DIFFERENT required
        # sets (the heartbeat's full set, and a restart's verify subset); a single
        # shared slot would let those two callers reset each other's window so it
        # never elapsed. Any change to a set (progressive import) or a failed
        # probe (ComfyUI went away) resets that contract's entry.
        self._node_contract_missing_windows: Dict[str, Tuple[frozenset, int]] = {}
        self.input_cache_dir = Path(_env_str("DM_INPUT_CACHE_DIR") or str(self.workspace / ".dm_input_cache"))
        self.input_cache_max_bytes = max(0, int(_parse_bytes(_env_str("DM_INPUT_CACHE_MAX_BYTES")) or 20 * 1024 * 1024 * 1024))
        self.input_cache_heartbeat_max_keys = max(0, min(1000, _env_int("DM_INPUT_CACHE_HEARTBEAT_MAX_KEYS", 50)))
        self.self_update_enabled = _env_bool("DM_AGENT_SELF_UPDATE_ENABLED", True)
        self.self_update_allow_downgrade = _env_bool("DM_AGENT_SELF_UPDATE_ALLOW_DOWNGRADE", False)
        self.self_update_retry_seconds = max(30.0, _env_float("DM_AGENT_SELF_UPDATE_RETRY_SECONDS", 300.0))
        self.agent_update_check_seconds = max(30.0, _env_float("DM_AGENT_UPDATE_CHECK_SECONDS", 60.0))
        self.self_script_path = Path(os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__))
        self.self_env_path = Path(_env_str("DM_AGENT_ENV_PATH") or str(self.workspace / "dependency_agent.env"))
        self._runtime_env_delivery_id = ""
        self._runtime_env_applied_keys: List[str] = []
        self._runtime_env_next_fetch_ms = 0
        self.self_marker_path = Path(
            _env_str("DM_AGENT_MARKER_PATH") or f"{_env_str('DM_AGENT_PID_PATH') or str(self.workspace / 'dependency_agent.pid')}.launch"
        )

        allowed = _split_csv(_env_str("DM_ALLOWED_DOMAINS")) or ["huggingface.co", "hf.co", "civitai.red", "civitai.com"]
        self.allowed_domains = {d.lower() for d in allowed if d}
        input_allowed = (
            _split_csv(_env_str("DM_INPUT_ALLOWED_DOMAINS"))
            or _split_csv(_env_str("PREFETCH_ALLOWED_DOMAINS"))
            or _split_csv(_env_str("INPUT_PREFETCH_ALLOWED_DOMAINS"))
        )
        self.input_allowed_domains = {d.lower() for d in input_allowed if d}

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._dependency_verification_lock = threading.Lock()
        # A ComfyUI restart is a process-level operation shared by every agent
        # worker. Keep the restart and readiness wait in one critical section so
        # concurrent execute workers cannot issue overlapping supervisor restarts.
        self._comfy_restart_lock = threading.RLock()
        self._node_contract_probe_lock = threading.Lock()
        self._comfy_restart_command_active = threading.Event()
        self._post_job_comfy_recycle_active = threading.Event()
        self._interrupted_comfy_restart_path = self.workspace / ".fcs" / "comfy_restart_in_progress.json"
        self._next_interrupted_comfy_restart_recovery_ms = 0
        self._token: Optional[str] = None
        self._resolved_instance_id: Optional[str] = None
        self._profile: Dict[str, Any] = {}
        self._downloading: Set[str] = set()
        self._session_hash_verified_dep_ids: Set[str] = set()
        self._download_activity: Dict[str, DownloadActivity] = {}
        self._state: LocalState = self._load_state()
        self._dynamic_bytes_used = 0
        self._last_heartbeat_ms = 0
        self._last_dependency_queue_depth = 0

        self._agent_access_token: Optional[str] = None
        self._agent_access_token_expires_at_ms = 0
        self._agent_bootstrap_profile: Dict[str, Any] = {}
        self._agent_server_time_offset_ms = 0
        self._agent_channel_supported = self.agent_control_enabled
        self._next_agent_register_attempt_ms = 0
        self._last_agent_heartbeat_ms = 0
        self._last_agent_update_check_ms = _now_ms()
        self._agent_max_concurrent_execute_jobs = 1
        self._agent_max_prefetch_jobs = 0
        self._active_exec_by_item: Dict[str, AgentExecuteLease] = {}
        self._active_maintenance_by_item: Dict[str, Dict[str, Any]] = {}
        self._ready_agent_item_ids: deque[str] = deque()
        self._agent_lease_order = 0
        self._input_cache_downloading: Set[str] = set()
        self._loop_wakeup = threading.Event()
        self._dependency_poll_wakeup = threading.Event()
        self._agent_poll_wakeup = threading.Event()
        self._agent_prefetch_executor: Optional[ThreadPoolExecutor] = None
        self._agent_execute_executor: Optional[ThreadPoolExecutor] = None
        self._agent_upload_executor: Optional[ThreadPoolExecutor] = None
        self._agent_maintenance_executor: Optional[ThreadPoolExecutor] = None
        self._agent_prl_miner_executor: Optional[ThreadPoolExecutor] = None
        self._agent_prefetch_inflight: Set[Future[None]] = set()
        self._agent_execute_inflight: Set[Future[None]] = set()
        self._agent_upload_inflight: Set[Future[None]] = set()
        self._agent_maintenance_inflight: Set[Future[None]] = set()
        self._agent_prl_miner_inflight: Set[Future[None]] = set()
        self._pending_self_update: Optional[AgentSelfUpdateRelease] = None
        self._pending_self_update_source = ""
        self._self_update_retry_at_ms = 0
        self._coordination: Optional[Dict[str, Any]] = None
        self._coordination_id_token: Optional[str] = None
        self._coordination_refresh_token: Optional[str] = None
        self._coordination_id_token_expires_at_ms = 0
        self._coordination_stream_thread: Optional[threading.Thread] = None
        self._coordination_stream_stop = threading.Event()
        self._coordination_stream_healthy = False
        self._coordination_dependency_http_checkpoint_due_ms = 0
        self._coordination_agent_http_checkpoint_due_ms = 0
        self._coordination_agent_direct_empty_probe_ms = 0
        self._coordination_dependency_direct_empty_probe_ms = 0
        self._last_dependency_runtime_full_sync_ms = 0
        self._last_dependency_runtime_signature = ""
        self._last_agent_runtime_full_sync_ms = 0
        self._last_agent_runtime_signature = ""
        self._last_agent_http_transition_signature = ""

        # Best-effort local reconciliation (no API calls).
        with self._lock:
            self._reconcile_lru_locked()

        self.input_cache_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_furgenpub_raw_base_url(self) -> str:
        raw = (
            _env_str("FURGENPUB_RAW_BASE_URL")
            or "https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support"
        ).rstrip("/")
        if self.server_type != "video_gen_v2":
            return raw

        pinned = VIDEO_GEN_V2_FURGENPUB_RAW_BASE_URL.rstrip("/")
        normalized = raw.rstrip("/")
        if not normalized:
            return pinned

        furgenpub_marker = "raw.githubusercontent.com/Dodzilla/FurgenPub/"
        pinned_marker = f"/{VIDEO_GEN_V2_FURGENPUB_COMMIT}/"
        allow_unpinned = _env_bool("DM_VIDEO_GEN_V2_ALLOW_UNPINNED_FURGENPUB_RAW_BASE", False)
        if furgenpub_marker in normalized and pinned_marker not in normalized and not allow_unpinned:
            logging.warning(
                "Ignoring stale or unpinned FURGENPUB_RAW_BASE_URL for video_gen_v2: %s; using %s",
                normalized,
                pinned,
            )
            return pinned
        return normalized

    @staticmethod
    def _pinned_furgenpub_support_base_url(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        normalized = value.strip().rstrip("/")
        if not re.fullmatch(
            r"https://raw\.githubusercontent\.com/Dodzilla/FurgenPub/[0-9a-f]{40}/docker/support",
            normalized,
        ):
            return None
        return normalized

    def validate_env(self) -> None:
        if not self.api_base_url:
            raise SystemExit("Missing required env var: FCS_API_BASE_URL")
        if not self.server_type:
            raise SystemExit("Missing required env var: SERVER_TYPE")

    def _repair_video_gen_v2_agent_env_launch_args(self) -> bool:
        env_path = Path(_env_str("DM_AGENT_ENV_PATH") or str(self.workspace / "dependency_agent.env"))
        try:
            if not env_path.exists():
                return False
            original = env_path.read_text(encoding="utf-8")
            lines = original.splitlines()
            repaired = [
                line for line in lines
                if not re.match(r"^\s*(?:export\s+)?COMFYUI_ARGS=", line)
            ]
            if not any(re.match(r"^\s*(?:export\s+)?DM_LOCAL_COMFY_ALLOW_DISCOVERY=", line) for line in repaired):
                repaired.append("export DM_LOCAL_COMFY_ALLOW_DISCOVERY=false")
            next_text = "\n".join(repaired) + ("\n" if repaired else "")
            if next_text == original:
                return False
            env_path.write_text(next_text, encoding="utf-8")
            try:
                env_path.chmod(0o600)
            except Exception:
                pass
            return True
        except Exception as exc:
            logging.warning("Failed to repair video_gen_v2 dependency agent env launch args: %s", exc)
            return False

    def _repair_video_gen_v2_supervisor_launch_script(self) -> bool:
        launch_script = Path(
            _env_str("DM_COMFYUI_SUPERVISOR_LAUNCH_SCRIPT")
            or "/opt/supervisor-scripts/comfyui.sh"
        )
        try:
            if not launch_script.exists() or not launch_script.is_file():
                return False
            source = launch_script.read_text(encoding="utf-8")
            block = (
                "# FURGEN dependency agent watchdog bootstrap\n"
                "dm_agent_env_path=\"${DM_AGENT_ENV_PATH:-${WORKSPACE:-/workspace}/dependency_agent.env}\"\n"
                "furgen_saved_comfyui_args=\"${COMFYUI_ARGS-}\"\n"
                "furgen_had_comfyui_args=0\n"
                "if [[ \"${COMFYUI_ARGS+x}\" == \"x\" ]]; then furgen_had_comfyui_args=1; fi\n"
                "if [[ -r \"${dm_agent_env_path}\" ]]; then\n"
                "    set -a\n"
                "    source \"${dm_agent_env_path}\"\n"
                "    set +a\n"
                "fi\n"
                "if [[ \"${furgen_had_comfyui_args}\" == \"1\" ]]; then\n"
                "    COMFYUI_ARGS=\"${furgen_saved_comfyui_args}\"\n"
                "else\n"
                "    unset COMFYUI_ARGS\n"
                "fi\n"
                "unset furgen_saved_comfyui_args furgen_had_comfyui_args\n"
                "furgen_attention_backend=\"$(printf '%s' \"${FURGEN_H3_ATTENTION_BACKEND:-sageattention2}\" | tr '[:upper:]' '[:lower:]')\"\n"
                "COMFYUI_ARGS=\"${COMFYUI_ARGS// --use-sage-attention/}\"\n"
                "COMFYUI_ARGS=\"${COMFYUI_ARGS// --use-pytorch-cross-attention/}\"\n"
                "COMFYUI_ARGS=\"${COMFYUI_ARGS// --use-ck-attention/}\"\n"
                "if [[ \"${furgen_attention_backend}\" == \"pytorch\" || \"${furgen_attention_backend}\" == \"pytorch_cross_attention\" || \"${furgen_attention_backend}\" == \"pytorch-cross-attention\" ]]; then\n"
                "    COMFYUI_ARGS=\"${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header} --use-pytorch-cross-attention\"\n"
                "elif [[ \"${furgen_attention_backend}\" == \"comfy_kitchen\" || \"${furgen_attention_backend}\" == \"comfy-kitchen\" || \"${furgen_attention_backend}\" == \"ck\" || \"${furgen_attention_backend}\" == \"ck_attention\" || \"${furgen_attention_backend}\" == \"comfy_kitchen_int8\" ]]; then\n"
                "    COMFYUI_ARGS=\"${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header} --use-ck-attention\"\n"
                "else\n"
                "    COMFYUI_ARGS=\"${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header} --use-sage-attention\"\n"
                "fi\n"
                "unset furgen_attention_backend\n"
                "dm_agent_disable=\"$(printf '%s' \"${DM_AGENT_DISABLE:-}\" | tr '[:upper:]' '[:lower:]')\"\n"
                "if [[ \"${dm_agent_disable}\" != \"1\" && \"${dm_agent_disable}\" != \"true\" ]]; then\n"
                "    watchdog_path=\"${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.sh}\"\n"
                "    watchdog_log_path=\"${DM_AGENT_WATCHDOG_LOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.log}\"\n"
                "    if [[ -x \"${watchdog_path}\" ]]; then\n"
                "        if ! command -v pgrep >/dev/null 2>&1 || ! pgrep -f \"${watchdog_path}\" >/dev/null 2>&1; then\n"
                "            if setsid --help 2>&1 | grep -q -- '--fork'; then\n"
                "                nohup setsid -f \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
                "            else\n"
                "                nohup setsid \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
                "            fi\n"
                "        fi\n"
                "    fi\n"
                "fi\n"
                "unset dm_agent_disable\n"
                "# /FURGEN dependency agent watchdog bootstrap\n"
            )
            pattern = re.compile(
                r"# FURGEN dependency agent watchdog bootstrap\n(?:.*?\n)# /FURGEN dependency agent watchdog bootstrap\n",
                re.DOTALL,
            )
            if pattern.search(source):
                next_source = pattern.sub(block, source)
            else:
                marker = "python main.py ${COMFYUI_ARGS}"
                idx = source.find(marker)
                if idx < 0:
                    logging.warning("Unable to locate ComfyUI launch command while repairing %s", launch_script)
                    return False
                line_start = source.rfind("\n", 0, idx) + 1
                next_source = source[:line_start] + block + source[line_start:]
            if next_source == source:
                return False
            launch_script.write_text(next_source, encoding="utf-8")
            try:
                launch_script.chmod(0o755)
            except Exception:
                pass
            return True
        except Exception as exc:
            logging.warning("Failed to repair video_gen_v2 ComfyUI launch script: %s", exc)
            return False

    def _repair_comfy_watchdog_process_isolation(self) -> bool:
        """Ensure future supervisor starts detach the watchdog from Comfy's process tree."""
        launch_script = Path(
            _env_str("DM_COMFYUI_SUPERVISOR_LAUNCH_SCRIPT")
            or "/opt/supervisor-scripts/comfyui.sh"
        )
        try:
            if not launch_script.exists() or not launch_script.is_file():
                return False
            source = launch_script.read_text(encoding="utf-8")
            detached_launch = (
                "if setsid --help 2>&1 | grep -q -- '--fork'; then\n"
                "                nohup setsid -f \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
                "            else\n"
                "                nohup setsid \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
                "            fi"
            )
            launch_pattern = re.compile(
                r'(?:if setsid --help 2>&1 \| grep -q -- \'--fork\'; then\n\s*'
                r'nohup setsid -f "\$\{watchdog_path\}" >> "\$\{watchdog_log_path\}" 2>&1 &\n\s*'
                r'else\n\s*nohup setsid "\$\{watchdog_path\}" >> "\$\{watchdog_log_path\}" 2>&1 &\n\s*fi|'
                r'nohup(?: setsid(?: -f)?)? "\$\{watchdog_path\}" >> "\$\{watchdog_log_path\}" 2>&1 &)'
            )
            next_source = launch_pattern.sub(detached_launch, source)
            if next_source == source:
                return False
            launch_script.write_text(next_source, encoding="utf-8")
            try:
                launch_script.chmod(0o755)
            except Exception:
                pass
            logging.warning(
                "Repaired ComfyUI watchdog launch detachment; it will take effect on the next supervisor start."
            )
            return True
        except Exception as exc:
            logging.warning("Failed repairing ComfyUI watchdog launch isolation: %s", exc)
            return False

    def _video_gen_v2_bootstrap_gate_active(self) -> bool:
        if (self.server_type or "").strip() != "video_gen_v2":
            return False
        if not _env_bool("FURGEN_COMFYUI_BOOTSTRAP_GATE_ENABLED", True):
            return False
        allowed_path = Path(
            _env_str("FURGEN_COMFYUI_START_ALLOWED_FILE")
            or str(self.workspace / ".furgen_comfyui_start_allowed")
        )
        return not allowed_path.exists()

    def _wait_for_video_gen_v2_bootstrap_ready(self, item_id: str, lease_id: str) -> None:
        """Avoid racing the foreground support bootstrap for the same Comfy checkout.

        The Vast onstart script deliberately holds a gate while it installs the
        baseline video node stack and rewrites the managed Comfy launch script.
        A create-time install_node_bundles command can otherwise touch the same
        repos and launch that script while it is still being rewritten. Keep the
        maintenance lease alive while waiting, then let the foreground bootstrap
        finish starting Comfy before running the incremental bundle verification.
        """
        if (self.server_type or "").strip() != "video_gen_v2":
            return
        if not _env_bool("FURGEN_COMFYUI_BOOTSTRAP_GATE_ENABLED", True):
            return
        if not _env_str("FURGEN_COMFYUI_START_ALLOWED_FILE"):
            # Only defer instances that explicitly opted into the managed Vast
            # bootstrap gate. Other video_gen_v2 environments may use the agent
            # bundle installer as their primary bootstrap path.
            return
        if not self._video_gen_v2_bootstrap_gate_active():
            return

        held_lease = [{
            "itemId": item_id,
            "leaseId": lease_id,
            "stage": "maintenance:install_node_bundles:bootstrap_gate",
        }]

        def refresh_lease() -> None:
            if not self._coordination:
                return
            self._coordination_check_active_lease_cancels(held_lease)

        gate_timeout = max(
            60.0,
            _env_float("DM_VIDEO_GEN_V2_BOOTSTRAP_GATE_WAIT_SECONDS", 1800.0),
        )
        gate_deadline = time.monotonic() + gate_timeout
        logging.info(
            "Deferring video_gen_v2 node bundle install until managed bootstrap gate is released: itemId=%s",
            item_id,
        )
        while self._video_gen_v2_bootstrap_gate_active():
            refresh_lease()
            if time.monotonic() >= gate_deadline:
                raise RuntimeError(
                    f"Timed out waiting for video_gen_v2 bootstrap gate before node bundle install ({int(gate_timeout)}s)."
                )
            if self._stop.wait(2.0):
                raise RuntimeError("Dependency agent stopped while waiting for video_gen_v2 bootstrap gate.")

        comfy_timeout = max(
            30.0,
            _env_float("DM_VIDEO_GEN_V2_BOOTSTRAP_COMFY_WAIT_SECONDS", 300.0),
        )
        comfy_deadline = time.monotonic() + comfy_timeout
        while not (
            self._local_comfy_reachable(timeout_seconds=2.0)
            and self._local_readiness_file_present()
        ):
            refresh_lease()
            if time.monotonic() >= comfy_deadline:
                raise RuntimeError(
                    f"Managed video_gen_v2 bootstrap released its gate but ComfyUI did not become ready within {int(comfy_timeout)}s."
                )
            if self._stop.wait(2.0):
                raise RuntimeError("Dependency agent stopped while waiting for managed video_gen_v2 ComfyUI startup.")

        refresh_lease()
        logging.info(
            "Managed video_gen_v2 bootstrap is ready; continuing node bundle install: itemId=%s",
            item_id,
        )

    def _repair_video_gen_v2_comfy_launch_contract(self, restart_if_unreachable: bool = False) -> None:
        if (self.server_type or "").strip() != "video_gen_v2":
            return
        env_changed = self._repair_video_gen_v2_agent_env_launch_args()
        launch_changed = self._repair_video_gen_v2_supervisor_launch_script()
        if env_changed or launch_changed:
            logging.warning(
                "Repaired video_gen_v2 ComfyUI launch contract: envChanged=%s launchScriptChanged=%s",
                env_changed,
                launch_changed,
            )
        if not restart_if_unreachable or self.mining_only:
            return
        if self._video_gen_v2_bootstrap_gate_active():
            logging.info(
                "Skipping video_gen_v2 ComfyUI restart while bootstrap gate is active; "
                "support provisioning has not allowed ComfyUI startup yet."
            )
            return
        configured = self._normalize_local_comfy_base_url(self.agent_local_comfy_base_url) or "http://127.0.0.1:8188"
        if self._probe_local_comfy_base_url(configured, timeout_seconds=2.0):
            self._resolved_local_comfy_base_url = configured
            return
        logging.warning("Configured video_gen_v2 ComfyUI endpoint %s is unreachable; restarting via launch script.", configured)
        if self._restart_local_comfy_with_launch_script():
            self._wait_for_local_comfy_restart([], timeout_seconds=180.0)

    def _resolve_download_tool(self) -> str:
        tool = (self.download_tool or "auto").strip().lower()
        if tool == "auto":
            return "aria2" if _command_exists("aria2c") else "wget"
        return tool

    def _download_allowed_domains_for_item(
        self,
        item: Dict[str, Any],
        resolved: Dict[str, Any],
    ) -> Optional[Set[str]]:
        dep_id = item.get("depId")
        if isinstance(dep_id, str) and dep_id.startswith("jobfile_"):
            return self.input_allowed_domains or None

        dest_rel = resolved.get("destRelativePath")
        if isinstance(dest_rel, str):
            normalized_dest = dest_rel.strip().lstrip("/").replace("\\", "/")
            if normalized_dest.startswith("input/"):
                return self.input_allowed_domains or None

        return self.allowed_domains

    def _resolve_remote_expected_size_bytes(
        self,
        url: str,
        auth_header: Optional[str],
    ) -> int:
        headers: Dict[str, str] = {
            "User-Agent": "dm-agent-head/1.0",
        }
        if auth_header:
            headers["Authorization"] = auth_header

        try:
            status, resp_headers = http_head(
                url,
                headers=headers,
                timeout_seconds=min(60.0, float(self.download_timeout_seconds)),
            )
        except Exception as e:
            logging.info("remote size HEAD failed for %s: %s", _safe_url_for_logs(url), e)
            return 0

        if status >= 400:
            logging.info("remote size HEAD returned %s for %s", status, _safe_url_for_logs(url))
            return 0

        size_bytes = _best_effort_expected_size_from_headers(resp_headers)
        if size_bytes <= 0 and self.download_debug:
            logging.info(
                "remote size HEAD returned no usable size for %s (headers=%s)",
                _safe_url_for_logs(url),
                json.dumps(resp_headers, sort_keys=True)[:800],
            )
        return max(0, int(size_bytes))

    def _update_download_activity(
        self,
        dep_id: str,
        dest_relative_path: Optional[str] = None,
        expected_bytes: Optional[int] = None,
        downloaded_bytes: Optional[int] = None,
        stage: Optional[str] = None,
        tool: Optional[str] = None,
    ) -> None:
        if not dep_id:
            return
        now_ms = _now_ms()
        with self._lock:
            row = self._download_activity.get(dep_id)
            if row is None:
                row = DownloadActivity(dep_id=dep_id, started_at_ms=now_ms, updated_at_ms=now_ms)
                self._download_activity[dep_id] = row

            if isinstance(dest_relative_path, str) and dest_relative_path:
                row.dest_relative_path = dest_relative_path
            if isinstance(expected_bytes, int) and expected_bytes >= 0:
                row.expected_bytes = int(expected_bytes)

            stage_changed = isinstance(stage, str) and stage and stage != row.stage
            if stage_changed:
                row.stage = stage
                row.throughput_bytes_per_sec = 0
                row.last_sample_at_ms = 0
                row.last_sample_bytes = int(downloaded_bytes) if isinstance(downloaded_bytes, int) and downloaded_bytes >= 0 else row.downloaded_bytes

            if isinstance(tool, str) and tool:
                row.tool = tool

            if isinstance(downloaded_bytes, int) and downloaded_bytes >= 0:
                current_bytes = int(downloaded_bytes)
                if row.last_sample_at_ms > 0 and now_ms > row.last_sample_at_ms and current_bytes >= row.last_sample_bytes:
                    delta_ms = now_ms - row.last_sample_at_ms
                    delta_bytes = current_bytes - row.last_sample_bytes
                    if delta_ms >= 500:
                        row.throughput_bytes_per_sec = int(delta_bytes / max(0.001, delta_ms / 1000.0))
                row.downloaded_bytes = current_bytes
                row.last_sample_at_ms = now_ms
                row.last_sample_bytes = current_bytes

            row.updated_at_ms = now_ms

    def _clear_download_activity(self, dep_id: str) -> None:
        if not dep_id:
            return
        with self._lock:
            self._download_activity.pop(dep_id, None)

    def _serialize_download_activity_locked(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for dep_id in sorted(self._download_activity.keys()):
            row = self._download_activity.get(dep_id)
            if row is None:
                continue
            item: Dict[str, Any] = {
                "depId": dep_id,
                "stage": row.stage,
                "downloadedBytes": int(max(0, row.downloaded_bytes)),
                "startedAtMs": int(max(0, row.started_at_ms)),
                "updatedAtMs": int(max(0, row.updated_at_ms)),
            }
            if row.dest_relative_path:
                item["destRelativePath"] = row.dest_relative_path
            if row.expected_bytes > 0:
                item["expectedBytes"] = int(row.expected_bytes)
            if row.throughput_bytes_per_sec > 0:
                item["throughputBytesPerSec"] = int(row.throughput_bytes_per_sec)
            if row.tool:
                item["tool"] = row.tool
            out.append(item)
        return out

    def _dependency_runtime_snapshot_locked(self) -> Tuple[List[str], List[str], List[str], List[str], List[Dict[str, Any]]]:
        self._reconcile_lru_locked()
        installed_static = sorted(self._state.installed_static)
        installed_dynamic = sorted(self._state.installed_dynamic)
        installed = set(installed_static) | set(installed_dynamic)

        stale_failed = self._state.failed & installed
        stale_downloading = self._downloading & installed
        stale_activity = set(self._download_activity.keys()) & installed
        if stale_failed or stale_downloading or stale_activity:
            stale = stale_failed | stale_downloading | stale_activity
            for dep_id in stale:
                self._state.failed.discard(dep_id)
                self._downloading.discard(dep_id)
                self._download_activity.pop(dep_id, None)
                self._state.retry.pop(dep_id, None)
            self._save_state()
            logging.info(
                "Pruned installed deps from transient dependency state: %s",
                ", ".join(sorted(stale)[:20]),
            )

        failed = sorted(dep_id for dep_id in self._state.failed if dep_id not in installed)
        downloading = sorted(dep_id for dep_id in self._downloading if dep_id not in installed)
        active_downloads = [
            row
            for row in self._serialize_download_activity_locked()
            if isinstance(row.get("depId"), str) and row.get("depId") not in installed
        ]
        return installed_static, installed_dynamic, failed, downloading, active_downloads

    def stop(self) -> None:
        self._stop.set()
        self._coordination_stream_stop.set()
        self._dependency_poll_wakeup.set()
        self._agent_poll_wakeup.set()
        self._loop_wakeup.set()
        try:
            self._idle_prl_miner.stop_if_running("agent_stop")
        except Exception as e:
            logging.warning("Failed stopping idle PRL miner during agent stop: %s", e)
        finally:
            self._release_mining_gpu_lease("agent_stop")

    def _start_gpu_lease_renewal(self, lease: Optional[GPUCoordinatorLease]) -> None:
        if lease is None:
            return

        def _renew_loop() -> None:
            # Foreground recovery has a ten-second coordinator reattach window.
            # Renew Comfy frequently enough to detect an epoch reset in time.
            normal_interval = (
                2.0
                if lease.holder == "comfy"
                else max(2.0, min(20.0, float(lease.ttl_ms) / 3000.0))
            )
            retry_interval = min(1.0, normal_interval)
            while not lease.renew_stop.wait(normal_interval):
                while not lease.renew_stop.is_set():
                    try:
                        self._refresh_gpu_lease_metadata(lease)
                        self._renew_gpu_lease_with_reconcile(lease)
                        if (
                            lease.admission_ticket_id
                            and lease.admission_claim_token
                            and _now_ms() - int(lease.admission_heartbeat_at_ms or 0)
                            >= int(self.gpu_admission_heartbeat_seconds * 1000)
                        ):
                            if not self._heartbeat_comfy_gpu_admission(
                                lease.admission_ticket_id,
                                lease.admission_claim_token,
                            ):
                                lease.lost.set()
                                raise GPUCoordinatorUnavailable("FIFO GPU admission claim was fenced")
                            lease.admission_heartbeat_at_ms = _now_ms()
                        break
                    except Exception as exc:
                        if lease.lost.is_set():
                            logging.error(
                                "Lost GPU coordinator lease holder=%s workId=%s: %s",
                                lease.holder,
                                lease.work_id,
                                exc,
                            )
                            self._handle_lost_gpu_lease(lease, "coordinator_lease_rejected")
                            return
                        with lease.identity_lock:
                            remaining_ms = int(lease.deadline_ms) - _now_ms()
                            epoch = int(lease.epoch)
                        if remaining_ms <= 1000:
                            lease.lost.set()
                            logging.error(
                                "Lost GPU coordinator lease holder=%s workId=%s epoch=%d: %s",
                                lease.holder,
                                lease.work_id,
                                epoch,
                                exc,
                            )
                            self._handle_lost_gpu_lease(lease, "coordinator_lease_expired")
                            return
                        logging.warning(
                            "GPU coordinator lease renew failed; retrying holder=%s workId=%s remainingMs=%d: %s",
                            lease.holder,
                            lease.work_id,
                            remaining_ms,
                            exc,
                        )
                        if lease.renew_stop.wait(min(retry_interval, max(0.25, remaining_ms / 2000.0))):
                            return

        thread = threading.Thread(
            target=_renew_loop,
            name=f"gpu-lease-renew-{lease.holder}-{lease.work_id[:24]}",
            daemon=True,
        )
        lease.renew_thread = thread
        thread.start()

    def _refresh_gpu_lease_metadata(self, lease: GPUCoordinatorLease) -> Dict[str, Any]:
        provider = lease.metadata_provider
        metadata = provider() if callable(provider) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        if metadata:
            with lease.identity_lock:
                pid = metadata.get("pid")
                start_time = metadata.get("processStartTime")
                process_group_id = metadata.get("processGroupId")
                lease.pid = int(pid) if isinstance(pid, (int, float)) and int(pid) > 0 else None
                lease.process_start_time = (
                    int(start_time) if isinstance(start_time, (int, float, str)) and str(start_time).isdigit() else None
                )
                lease.process_group_id = (
                    int(process_group_id)
                    if isinstance(process_group_id, (int, float)) and int(process_group_id) > 0
                    else None
                )
        with lease.identity_lock:
            out: Dict[str, Any] = {}
            if isinstance(lease.pid, int) and lease.pid > 0:
                out["pid"] = int(lease.pid)
            if isinstance(lease.process_start_time, int):
                out["processStartTime"] = int(lease.process_start_time)
            if isinstance(lease.process_group_id, int) and lease.process_group_id > 0:
                out["processGroupId"] = int(lease.process_group_id)
            return out

    def _renew_gpu_lease_with_reconcile(self, lease: GPUCoordinatorLease) -> None:
        first_error: Optional[Exception] = None
        try:
            self._gpu_coordinator.renew(lease)
            return
        except Exception as exc:
            first_error = exc
            if lease.holder != "comfy":
                # Mining is intentionally never adopted across coordinator
                # epochs; recovery stops it and the agent reacquires later.
                if isinstance(exc, GPUCoordinatorUnavailable) and exc.status in (404, 409, 410):
                    lease.lost.set()
                raise

        recovery_deadline = time.monotonic() + 9.0
        last_error: Exception = first_error or GPUCoordinatorUnavailable("GPU lease renewal failed")
        while not lease.renew_stop.is_set() and time.monotonic() < recovery_deadline:
            metadata = self._refresh_gpu_lease_metadata(lease)
            try:
                replacement = self._gpu_coordinator.reconcile(lease.holder, lease.work_id, metadata)
                with lease.release_lock:
                    released_during_reconcile = lease.released
                    released_reason = lease.released_reason or "released_during_reconcile"
                    released_keep_warm = lease.released_keep_warm
                    with lease.identity_lock:
                        lease.epoch = replacement.epoch
                        lease.fencing_token = replacement.fencing_token
                        lease.deadline_ms = replacement.deadline_ms
                        lease.ttl_ms = replacement.ttl_ms
                if released_during_reconcile:
                    # The normal release may already have raced using the old
                    # fence. Close the newly adopted lease explicitly.
                    self._gpu_coordinator.release(
                        lease,
                        released_reason,
                        keep_warm=released_keep_warm,
                    )
                    return
                logging.warning(
                    "Reconciled live Comfy GPU lease after coordinator epoch reset workId=%s epoch=%d",
                    lease.work_id,
                    replacement.epoch,
                )
                return
            except Exception as reconcile_error:
                last_error = reconcile_error
            # A transient request failure need not imply restart. If the old
            # epoch is still valid, a normal renewal wins without adoption.
            try:
                self._gpu_coordinator.renew(lease)
                return
            except Exception as renew_error:
                last_error = renew_error
            if lease.renew_stop.wait(0.5):
                return
        lease.lost.set()
        with lease.identity_lock:
            lease.deadline_ms = _now_ms()
        raise GPUCoordinatorUnavailable(f"Comfy GPU lease reconciliation window expired: {last_error}")

    def _comfy_gpu_process_metadata(self) -> Dict[str, Any]:
        main_pids = self._find_local_comfy_main_processes()
        if not main_pids:
            return {}
        pid = int(main_pids[0])
        start_time = _linux_process_start_time(pid)
        metadata: Dict[str, Any] = {"pid": pid}
        if start_time is not None:
            metadata["processStartTime"] = int(start_time)
        try:
            process_group_id = os.getpgid(pid)
            if process_group_id > 0:
                metadata["processGroupId"] = int(process_group_id)
        except (OSError, ProcessLookupError):
            pass
        return metadata

    def _acquire_gpu_lease(
        self,
        holder: str,
        work_id: str,
        metadata_provider: Optional[Callable[[], Dict[str, Any]]] = None,
        admission_ticket_id: Optional[str] = None,
        admission_claim_token: Optional[str] = None,
    ) -> Optional[GPUCoordinatorLease]:
        handoff_deadline = time.monotonic() + (10.0 if admission_claim_token else 0.0)
        while True:
            try:
                lease = self._gpu_coordinator.acquire(
                    holder,
                    work_id,
                    self.gpu_coordinator_lease_ttl_ms,
                    admission_claim_id=admission_claim_token,
                    admission_ticket_id=admission_ticket_id,
                )
                break
            except GPUCoordinatorBusy:
                if not admission_claim_token or time.monotonic() >= handoff_deadline:
                    raise
                time.sleep(min(0.25, max(0.01, handoff_deadline - time.monotonic())))
        if lease is not None:
            lease.metadata_provider = metadata_provider
            lease.admission_ticket_id = admission_ticket_id
            lease.admission_claim_token = admission_claim_token
            lease.admission_heartbeat_at_ms = _now_ms()
            self._refresh_gpu_lease_metadata(lease)
        self._start_gpu_lease_renewal(lease)
        if lease is not None:
            logging.info(
                "Acquired GPU coordinator lease holder=%s workId=%s epoch=%d deadlineMs=%d",
                holder,
                work_id,
                lease.epoch,
                lease.deadline_ms,
            )
        return lease

    def _release_gpu_lease(
        self,
        lease: Optional[GPUCoordinatorLease],
        reason: str,
        keep_warm: bool = False,
    ) -> bool:
        if lease is None:
            return True
        with lease.release_lock:
            if lease.released:
                return lease.release_confirmed
            lease.released = True
            lease.released_reason = str(reason or "work_complete")[:160]
            lease.released_keep_warm = bool(keep_warm)
        lease.renew_stop.set()
        thread = lease.renew_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        try:
            self._gpu_coordinator.release(lease, reason, keep_warm=keep_warm)
            logging.info(
                "Released GPU coordinator lease holder=%s workId=%s epoch=%d reason=%s keepWarm=%s",
                lease.holder,
                lease.work_id,
                lease.epoch,
                reason,
                keep_warm,
            )
            lease.release_confirmed = True
            return True
        except Exception as exc:
            logging.warning(
                "GPU coordinator lease release failed holder=%s workId=%s epoch=%d: %s",
                lease.holder,
                lease.work_id,
                lease.epoch,
                exc,
            )
            return False

    def _release_comfy_gpu_lease(self, lease: AgentExecuteLease, reason: str, keep_warm: bool = True) -> None:
        coordinator_lease = lease.gpu_coordinator_lease
        lease.gpu_coordinator_lease = None
        local_release_confirmed = self._release_gpu_lease(coordinator_lease, reason, keep_warm=keep_warm)
        if local_release_confirmed:
            self._release_comfy_gpu_admission(lease, reason)
        elif lease.gpu_admission_claim_token:
            logging.error(
                "Leaving FIFO admission claim for fenced recovery because the local coordinator release was not confirmed: "
                "ticket=%s jobId=%s",
                lease.gpu_admission_ticket_id,
                lease.job_id,
            )

    def _acquire_mining_gpu_lease(self, reason: str) -> bool:
        if not self._gpu_coordinator.configured:
            return True
        if self._gpu_admission_has_foreground_work():
            self._mining_gpu_retry_after_ms = _now_ms() + 1_000
            return False
        if _now_ms() < self._mining_gpu_retry_after_ms:
            return False
        with self._mining_gpu_lease_lock:
            existing = self._mining_gpu_lease
            if existing is not None and not existing.lost.is_set() and not existing.released:
                return True
            if existing is not None:
                self._mining_gpu_lease = None
                self._release_gpu_lease(existing, "mining_lease_replaced", keep_warm=False)
            try:
                lease = self._acquire_gpu_lease("mining", f"idle-prl:{reason or 'idle'}")
            except GPUCoordinatorBusy as exc:
                self._mining_gpu_retry_after_ms = _now_ms() + int(exc.retry_after_seconds * 1000)
                logging.debug("Idle PRL mining admission deferred reason=%s retryAfter=%.2fs: %s", reason, exc.retry_after_seconds, exc)
                return False
            except GPUCoordinatorUnavailable as exc:
                self._mining_gpu_retry_after_ms = _now_ms() + 2_000
                logging.debug("Idle PRL mining coordinator unavailable reason=%s: %s", reason, exc)
                return False
            # Close the read/acquire race: foreground work can enqueue after
            # the first RTDB check but before the local mining lease commits.
            if self._gpu_admission_has_foreground_work():
                self._mining_gpu_retry_after_ms = _now_ms() + 1_000
                self._release_gpu_lease(lease, "foreground_enqueued_during_mining_handoff", keep_warm=False)
                return False
            if lease is None:
                return True
            self._mining_gpu_lease = lease
            self._mining_gpu_retry_after_ms = 0
            return True

    def _release_mining_gpu_lease(self, reason: str) -> None:
        with self._mining_gpu_lease_lock:
            lease = self._mining_gpu_lease
            self._mining_gpu_lease = None
        self._release_gpu_lease(lease, reason, keep_warm=False)

    def _handle_lost_gpu_lease(self, lease: GPUCoordinatorLease, reason: str) -> None:
        """Fail closed when a running miner loses its exact coordinator fence."""
        if lease.holder != "mining":
            return
        lease.renew_stop.set()
        with self._mining_gpu_lease_lock:
            if self._mining_gpu_lease is lease:
                self._mining_gpu_lease = None
            self._mining_gpu_retry_after_ms = max(self._mining_gpu_retry_after_ms, _now_ms() + 2_000)
        try:
            self._idle_prl_miner.stop_if_running(reason)
            logging.error(
                "Stopped idle PRL miner after GPU coordinator lease loss workId=%s reason=%s",
                lease.work_id,
                reason,
            )
        except Exception as exc:
            logging.critical(
                "Failed to stop idle PRL miner after coordinator lease loss workId=%s: %s",
                lease.work_id,
                exc,
            )

    def _register_mining_process_with_gpu_coordinator(self) -> None:
        if not self._gpu_coordinator.configured:
            return
        snapshot = self._idle_prl_miner.snapshot()
        pid_value = snapshot.get("pid")
        if not isinstance(pid_value, (int, float)) or int(pid_value) <= 0:
            raise GPUCoordinatorUnavailable("Started PRL miner did not report a PID")
        pid = int(pid_value)
        process_start_time = _linux_process_start_time(pid)
        if process_start_time is None:
            raise GPUCoordinatorUnavailable(f"Unable to read PRL miner process identity for pid={pid}")
        try:
            process_group_id = int(os.getpgid(pid))
        except (OSError, ProcessLookupError) as exc:
            raise GPUCoordinatorUnavailable(f"Unable to read PRL miner process group for pid={pid}: {exc}") from exc
        with self._mining_gpu_lease_lock:
            lease = self._mining_gpu_lease
            if lease is None:
                # Optional unsupported coordinators retain legacy behavior.
                return
            with lease.identity_lock:
                lease.pid = pid
                lease.process_start_time = process_start_time
                lease.process_group_id = process_group_id
            self._gpu_coordinator.renew(lease)
        logging.info(
            "Registered PRL miner process with GPU coordinator pid=%d processStartTime=%d processGroupId=%d epoch=%d",
            pid,
            process_start_time,
            process_group_id,
            lease.epoch,
        )

    def _stop_idle_prl_mining_for_work(self, reason: str) -> None:
        try:
            if reason == "execute_job":
                # Coordinator-managed mining must release VRAM, so suspend/resume
                # and keep-running modes are deliberately overridden here.
                if self._gpu_coordinator.configured:
                    self._idle_prl_miner.pause_for_work(reason, force_stop=True)
                else:
                    self._idle_prl_miner.pause_for_work(reason)
            elif reason == "self_update":
                self._idle_prl_miner.stop_if_running(reason)
            else:
                logging.debug("Keeping idle PRL miner running during %s", reason)
        except Exception as e:
            logging.warning("Failed stopping idle PRL miner before %s: %s", reason, e)
        finally:
            if reason in ("execute_job", "self_update"):
                self._release_mining_gpu_lease(reason)

    def _host_memory_pressure_requires_full_comfy_trim(self) -> bool:
        try:
            meminfo = Path("/proc/meminfo").read_text("utf-8", errors="replace")
            match = re.search(r"^MemAvailable:\s+(\d+)\s+kB\s*$", meminfo, re.MULTILINE)
            if match and int(match.group(1)) * 1024 < int(self.comfy_full_trim_mem_available_bytes):
                return True
        except Exception:
            pass
        try:
            pressure = Path("/proc/pressure/memory").read_text("utf-8", errors="replace")
            match = re.search(r"^some\s+avg10=([0-9.]+)", pressure, re.MULTILINE)
            if match and float(match.group(1)) >= float(self.comfy_full_trim_memory_psi_avg10):
                return True
        except Exception:
            pass
        return False

    def _free_local_comfy_for_idle_prl_mining(self, reason: str) -> None:
        if self.mining_only or not self.idle_prl_free_comfy_before_start:
            return
        now_ms = _now_ms()
        if (
            self._last_idle_prl_comfy_free_ms > 0 and
            now_ms - self._last_idle_prl_comfy_free_ms < self.idle_prl_free_comfy_min_interval_ms
        ):
            return
        self._last_idle_prl_comfy_free_ms = now_ms
        try:
            timeout = self.idle_prl_free_comfy_timeout_seconds
            base_url = self._resolve_local_comfy_base_url(force_refresh=True, timeout_seconds=min(5.0, timeout))
            # Preserve the existing fleet-wide full trim. CPU cache retention
            # is intentionally enabled only on coordinator-managed workers.
            full_trim = (
                self._host_memory_pressure_requires_full_comfy_trim()
                if self._gpu_coordinator.configured else True
            )
            status, _resp = api_json(
                "POST",
                f"{base_url}/free",
                body={"unload_models": True, "free_memory": full_trim},
                timeout_seconds=timeout,
            )
            logging.info(
                "Requested local Comfy model unload before idle PRL miner start reason=%s status=%s baseUrl=%s fullTrim=%s",
                reason or "unspecified",
                status,
                base_url,
                full_trim,
            )
        except Exception as e:
            logging.warning(
                "Failed freeing local Comfy memory before idle PRL miner start reason=%s: %s",
                reason or "unspecified",
                e,
            )

    def _resume_idle_prl_mining_if_idle(self, reason: str) -> None:
        try:
            with self._lock:
                gpu_blocking_work_count = sum(
                    1
                    for lease in self._active_exec_by_item.values()
                    if str(getattr(lease, "stage", "") or "") in AGENT_GPU_BLOCKING_STAGES
                )
                maintenance_count = len(self._agent_maintenance_inflight)
            if gpu_blocking_work_count > 0 or maintenance_count > 0:
                return
            if self._pending_self_update is not None:
                return
            miner_snapshot = self._idle_prl_miner.snapshot()
            miner_running = str(miner_snapshot.get("state") or "") in ("running", "starting", "paused")
            should_attempt_resume = bool(miner_snapshot.get("pausedForWork"))
            should_attempt_start = (
                should_attempt_resume or
                (
                    str(miner_snapshot.get("desiredState") or "") in ("running", "starting") and
                    str(miner_snapshot.get("state") or "") != "running"
                )
            )
            if self._gpu_coordinator.configured and should_attempt_start:
                desired_payload = self._idle_prl_miner.desired_start_payload()
                if not desired_payload:
                    return
                try:
                    prepared = self._idle_prl_miner.prepare_gated({**desired_payload, "forceRestart": True})
                except Exception as exc:
                    logging.warning("Idle PRL preparation failed before GPU admission after %s: %s", reason, exc)
                    return
                if not self._acquire_mining_gpu_lease(reason):
                    return
                try:
                    gated = self._idle_prl_miner.start_gated(prepared)
                    gate_path = str(gated.get("gatePath") or "")
                    self._register_mining_process_with_gpu_coordinator()
                    self._idle_prl_miner.release_start_gate(gate_path)
                    logging.info("Started gated idle PRL miner after %s", reason)
                    self._force_idle_prl_runtime_refresh(reason)
                except Exception as exc:
                    self._idle_prl_miner.cancel_gated_start("mining_process_registration_failed")
                    self._idle_prl_miner.defer_start(desired_payload, "gpu_coordinator_fenced_during_start")
                    self._release_mining_gpu_lease("mining_start_failed")
                    logging.info("Deferred gated idle PRL miner after %s: %s", reason, exc)
                return
            if (miner_running or should_attempt_start) and not self._acquire_mining_gpu_lease(reason):
                if miner_running:
                    should_stop_unmanaged = self._gpu_coordinator.required
                    try:
                        should_stop_unmanaged = self._gpu_coordinator.status().get("enforcing") is True
                    except Exception:
                        pass
                    if should_stop_unmanaged:
                        self._idle_prl_miner.stop_if_running("gpu_coordinator_admission_denied")
                return
            if should_attempt_start:
                self._free_local_comfy_for_idle_prl_mining(reason)
            try:
                resumed = self._idle_prl_miner.resume_if_paused(reason)
                restarted = False if resumed else self._idle_prl_miner.restart_if_desired(reason)
                resumed_after_update = False if (resumed or restarted) else self._idle_prl_miner.resume_after_agent_update_if_requested(reason)
                if resumed or restarted or resumed_after_update:
                    self._register_mining_process_with_gpu_coordinator()
            except Exception:
                self._idle_prl_miner.stop_if_running("mining_process_registration_failed")
                self._release_mining_gpu_lease("mining_start_failed")
                raise
            if resumed or restarted or resumed_after_update:
                logging.info(
                    "%s idle PRL miner after %s",
                    "Resumed" if resumed else ("Restarted failed" if restarted else "Restored"),
                    reason,
                )
                self._force_idle_prl_runtime_refresh(reason)
            final_snapshot = self._idle_prl_miner.snapshot()
            if str(final_snapshot.get("state") or "") not in ("running", "starting", "paused"):
                self._release_mining_gpu_lease("mining_not_running")
        except Exception as e:
            logging.warning("Failed resuming idle PRL miner after %s: %s", reason, e)

    def _mark_agent_gpu_work_finished(
        self,
        lease: AgentExecuteLease,
        reason: str,
        final_stage: str = "finalizing",
    ) -> None:
        with self._lock:
            active = self._active_exec_by_item.get(lease.item_id)
            if active:
                active.stage = final_stage
        self._release_comfy_gpu_lease(lease, reason, keep_warm=True)
        self._resume_idle_prl_mining_if_idle(reason)
        self._request_agent_queue_poll()

    # Include shared secret by default to avoid token races causing 401 loops
    # (token is still used when present; backend accepts either).
    def _headers(self, use_token: bool = True, include_secret: bool = True) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if use_token and self._token:
            h["Authorization"] = f"Bearer {self._token}"
        if include_secret and self.shared_secret:
            h["X-DM-Secret"] = self.shared_secret
        return h

    def _agent_headers(self, use_token: bool = True, include_secret: bool = True) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if use_token and self._agent_access_token:
            h["Authorization"] = f"Bearer {self._agent_access_token}"
        if include_secret and self.shared_secret:
            h["X-DM-Secret"] = self.shared_secret
        return h

    def _agent_root_url(self) -> str:
        if self.api_base_url.endswith("/api"):
            return self.api_base_url[:-4]
        return self.api_base_url

    def _resolve_agent_endpoint_url(self, endpoint: str) -> str:
        value = (endpoint or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if value.startswith("/api/"):
            return f"{self._agent_root_url()}{value}"
        if not value.startswith("/"):
            value = "/" + value
        return f"{self.api_base_url}{value}"

    def _server_now_ms(self) -> int:
        return _now_ms() + int(self._agent_server_time_offset_ms)

    def _note_server_time(self, value: Optional[str]) -> None:
        ms = _iso_to_ms(value)
        if ms is None:
            return
        self._agent_server_time_offset_ms = int(ms - _now_ms())

    def _sleep_agent_api_retry(self, label: str, attempt_idx: int, error: Exception) -> None:
        base = float(getattr(self, "agent_api_retry_base_seconds", 1.0) or 1.0)
        cap = float(getattr(self, "agent_api_retry_max_seconds", 20.0) or 20.0)
        delay = min(cap, base * (2 ** max(0, int(attempt_idx))))
        delay = delay * (0.75 + random.random() * 0.5)
        logging.warning(
            "Transient agent %s failure; retrying in %.1fs (attempt=%d): %s",
            label,
            delay,
            int(attempt_idx) + 2,
            error,
        )
        time.sleep(max(0.1, delay))

    def _is_retryable_agent_control_error(self, error: Exception) -> bool:
        if isinstance(error, NetworkError):
            return True
        if isinstance(error, ApiError):
            return error.status in RETRYABLE_HTTP_STATUS_CODES or error.status in (401, 403) or error.status >= 500
        if isinstance(error, (OSError, socket.timeout, TimeoutError, http.client.HTTPException)):
            return True
        msg = str(error).lower()
        return any(
            token in msg
            for token in (
                "temporary failure in name resolution",
                "name or service not known",
                "connection reset",
                "connection refused",
                "timed out",
                "agent_token_invalid",
                "agent_token_expired",
            )
        )

    def _agent_api(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 30.0,
        use_token: bool = True,
        include_secret: bool = True,
    ) -> Dict[str, Any]:
        url = self._resolve_agent_endpoint_url(endpoint)
        if query:
            query_items: List[Tuple[str, str]] = []
            for k, v in query.items():
                if v is None:
                    continue
                query_items.append((str(k), str(v)))
            if query_items:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urllib.parse.urlencode(query_items)}"

        attempts = max(1, int(getattr(self, "agent_api_retry_attempts", 1) or 1))
        last_error: Optional[Exception] = None
        for attempt_idx in range(attempts):
            try:
                status, resp = api_json(
                    method,
                    url,
                    body=body,
                    headers=self._agent_headers(use_token=use_token, include_secret=include_secret),
                    timeout_seconds=timeout_seconds,
                )
                break
            except ApiError as e:
                last_error = e
                should_retry = e.status in RETRYABLE_HTTP_STATUS_CODES or e.status >= 500
                if use_token and e.status in (401, 403) and endpoint != "/agent/register":
                    should_retry = True
                    self._agent_access_token = None
                    self._agent_access_token_expires_at_ms = 0
                    try:
                        self._agent_register()
                    except Exception as refresh_err:
                        logging.warning("Agent token refresh before retry failed: %s", refresh_err)
                if not should_retry or attempt_idx >= attempts - 1:
                    raise
                self._sleep_agent_api_retry("api", attempt_idx, e)
            except NetworkError as e:
                last_error = e
                if attempt_idx >= attempts - 1:
                    raise
                self._sleep_agent_api_retry("api", attempt_idx, e)
        else:
            if last_error:
                raise last_error
            raise RuntimeError("Agent API failed without an error")
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected agent API response ({status}): {resp}")
        self._note_server_time(resp.get("serverTime") if isinstance(resp.get("serverTime"), str) else None)
        if resp.get("ok") is not True:
            err = resp.get("error") if isinstance(resp.get("error"), dict) else {}
            code = err.get("code") if isinstance(err.get("code"), str) else "unknown_error"
            message = err.get("message") if isinstance(err.get("message"), str) else str(resp)
            raise RuntimeError(f"Agent API error [{code}]: {message}")
        return resp

    def _agent_token_needs_refresh(self) -> bool:
        if not self._agent_access_token:
            return True
        if self._agent_access_token_expires_at_ms <= 0:
            return True
        # Refresh at least two minutes before expiry.
        return self._server_now_ms() >= (int(self._agent_access_token_expires_at_ms) - 120_000)

    def _normalize_coordination_path(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        out = value.strip()
        if not out:
            return None
        if not out.startswith("/"):
            out = "/" + out
        if len(out) > 1:
            out = out.rstrip("/")
        return out

    def _normalize_coordination_payload(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict) or raw.get("enabled") is not True:
            return None
        mode = raw.get("mode")
        database_url = raw.get("databaseUrl")
        api_key = raw.get("apiKey")
        auth = raw.get("auth") if isinstance(raw.get("auth"), dict) else {}
        custom_token = auth.get("customToken")
        paths = raw.get("paths") if isinstance(raw.get("paths"), dict) else {}
        signals = paths.get("signals") if isinstance(paths.get("signals"), dict) else {}
        runtime = paths.get("runtime") if isinstance(paths.get("runtime"), dict) else {}
        queues = paths.get("queues") if isinstance(paths.get("queues"), dict) else {}
        instance_root = self._normalize_coordination_path(paths.get("instanceRoot"))
        signals_root = self._normalize_coordination_path(signals.get("root"))
        agent_signal = self._normalize_coordination_path(signals.get("agentQueue"))
        dependency_signal = self._normalize_coordination_path(signals.get("dependencyQueue"))
        runtime_root = self._normalize_coordination_path(runtime.get("root"))
        agent_runtime = self._normalize_coordination_path(runtime.get("agentControl"))
        dependency_runtime = self._normalize_coordination_path(runtime.get("dependencyManager"))
        agent_queue_items = self._normalize_coordination_path(queues.get("agentQueueItems"))
        dependency_queue_items = self._normalize_coordination_path(queues.get("dependencyQueueItems"))
        if mode != "rtdb_v1":
            return None
        if not isinstance(database_url, str) or not database_url.strip():
            return None
        if not isinstance(api_key, str) or not api_key.strip():
            return None
        if not isinstance(custom_token, str) or not custom_token.strip():
            return None
        if not all((instance_root, signals_root, agent_signal, dependency_signal, runtime_root, agent_runtime, dependency_runtime)):
            return None
        if not agent_queue_items:
            agent_queue_items = self._normalize_coordination_path(f"{instance_root}/agentQueue/items")
        if not dependency_queue_items:
            dependency_queue_items = self._normalize_coordination_path(f"{instance_root}/dependencyQueue/items")

        safety = raw.get("safetyPollSeconds") if isinstance(raw.get("safetyPollSeconds"), dict) else {}
        agent_safety = safety.get("agent")
        dep_safety = safety.get("dependencies")
        checkpoint = raw.get("firestoreCheckpointSeconds")
        lease_duration = raw.get("leaseDurationSeconds")
        features = raw.get("features") if isinstance(raw.get("features"), dict) else {}
        try:
            agent_safety_sec = max(1.0, float(agent_safety if isinstance(agent_safety, (int, float)) else 15.0))
        except Exception:
            agent_safety_sec = 15.0
        try:
            dep_safety_sec = max(1.0, float(dep_safety if isinstance(dep_safety, (int, float)) else 30.0))
        except Exception:
            dep_safety_sec = 30.0
        try:
            checkpoint_sec = max(5.0, float(checkpoint if isinstance(checkpoint, (int, float)) else 60.0))
        except Exception:
            checkpoint_sec = 60.0
        try:
            lease_duration_sec = max(10.0, min(600.0, float(lease_duration if isinstance(lease_duration, (int, float)) else 90.0)))
        except Exception:
            lease_duration_sec = 90.0

        normalized = {
            "enabled": True,
            "mode": "rtdb_v1",
            "databaseUrl": database_url.strip(),
            "apiKey": api_key.strip(),
            "customToken": custom_token.strip(),
            "paths": {
                "instanceRoot": instance_root,
                "signalsRoot": signals_root,
                "agentQueueSignal": agent_signal,
                "dependencyQueueSignal": dependency_signal,
                "runtimeRoot": runtime_root,
                "agentControlRuntime": agent_runtime,
                "dependencyManagerRuntime": dependency_runtime,
                "agentQueueItems": agent_queue_items,
                "dependencyQueueItems": dependency_queue_items,
            },
            "safetyPollSeconds": {
                "agent": agent_safety_sec,
                "dependencies": dep_safety_sec,
            },
            "firestoreCheckpointSeconds": checkpoint_sec,
            "leaseDurationSeconds": lease_duration_sec,
            "features": {
                "agentQueueClaimV1": bool(features.get("agentQueueClaimV1")) and bool(agent_queue_items),
                "dependencyQueueClaimV1": bool(features.get("dependencyQueueClaimV1")) and bool(dependency_queue_items),
                "agentLeaseHeartbeatV1": bool(features.get("agentLeaseHeartbeatV1")),
            },
            "legacyHttpFallback": raw.get("legacyHttpFallback") is not False,
        }
        normalized["configKey"] = json.dumps(
            {
                "databaseUrl": normalized["databaseUrl"],
                "paths": normalized["paths"],
                "safetyPollSeconds": normalized["safetyPollSeconds"],
                "firestoreCheckpointSeconds": normalized["firestoreCheckpointSeconds"],
                "leaseDurationSeconds": normalized["leaseDurationSeconds"],
                "features": normalized["features"],
            },
            sort_keys=True,
        )
        return normalized

    def _coordination_identity_toolkit_url(self, api_key: str) -> str:
        auth_emulator = _env_str("FIREBASE_AUTH_EMULATOR_HOST")
        if auth_emulator:
            return f"http://{auth_emulator}/identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={urllib.parse.quote(api_key)}"
        return f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={urllib.parse.quote(api_key)}"

    def _coordination_secure_token_url(self, api_key: str) -> str:
        auth_emulator = _env_str("FIREBASE_AUTH_EMULATOR_HOST")
        if auth_emulator:
            return f"http://{auth_emulator}/securetoken.googleapis.com/v1/token?key={urllib.parse.quote(api_key)}"
        return f"https://securetoken.googleapis.com/v1/token?key={urllib.parse.quote(api_key)}"

    def _coordination_token_needs_refresh(self) -> bool:
        if not self._coordination_id_token:
            return True
        if self._coordination_id_token_expires_at_ms <= 0:
            return True
        return _now_ms() >= (int(self._coordination_id_token_expires_at_ms) - 60_000)

    def _refresh_coordination_id_token(self) -> str:
        coord = self._coordination
        if not coord:
            raise RuntimeError("RTDB coordination is not configured")

        refresh_token = self._coordination_refresh_token
        api_key = coord.get("apiKey")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError("RTDB coordination refresh token is unavailable")
        if not isinstance(api_key, str) or not api_key:
            raise RuntimeError("RTDB coordination API key is unavailable")

        status, resp = api_form_json(
            "POST",
            self._coordination_secure_token_url(api_key),
            body={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout_seconds=30.0,
        )
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected RTDB token refresh response: {status} {resp}")

        id_token = resp.get("id_token")
        next_refresh_token = resp.get("refresh_token")
        expires_in = resp.get("expires_in")
        if not isinstance(id_token, str) or not id_token:
            raise RuntimeError(f"RTDB token refresh missing id_token: {resp}")
        try:
            expires_in_sec = max(60, int(expires_in if isinstance(expires_in, (int, float, str)) else 3600))
        except Exception:
            expires_in_sec = 3600

        self._coordination_id_token = id_token
        if isinstance(next_refresh_token, str) and next_refresh_token:
            self._coordination_refresh_token = next_refresh_token
        self._coordination_id_token_expires_at_ms = _now_ms() + (expires_in_sec * 1000)
        return id_token

    def _exchange_coordination_custom_token(self) -> str:
        coord = self._coordination
        if not coord:
            raise RuntimeError("RTDB coordination is not configured")

        api_key = coord.get("apiKey")
        custom_token = coord.get("customToken")
        if not isinstance(api_key, str) or not api_key:
            raise RuntimeError("RTDB coordination API key is unavailable")
        if not isinstance(custom_token, str) or not custom_token:
            raise RuntimeError("RTDB coordination custom token is unavailable")

        status, resp = api_json(
            "POST",
            self._coordination_identity_toolkit_url(api_key),
            body={
                "token": custom_token,
                "returnSecureToken": True,
            },
            timeout_seconds=30.0,
        )
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected RTDB custom-token exchange response: {status} {resp}")

        id_token = resp.get("idToken")
        refresh_token = resp.get("refreshToken")
        expires_in = resp.get("expiresIn")
        if not isinstance(id_token, str) or not id_token:
            raise RuntimeError(f"RTDB custom-token exchange missing idToken: {resp}")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError(f"RTDB custom-token exchange missing refreshToken: {resp}")
        try:
            expires_in_sec = max(60, int(expires_in if isinstance(expires_in, (int, float, str)) else 3600))
        except Exception:
            expires_in_sec = 3600

        self._coordination_id_token = id_token
        self._coordination_refresh_token = refresh_token
        self._coordination_id_token_expires_at_ms = _now_ms() + (expires_in_sec * 1000)
        return id_token

    def _ensure_coordination_id_token(self, force_refresh: bool = False) -> str:
        if force_refresh:
            self._coordination_id_token = None
            self._coordination_id_token_expires_at_ms = 0

        if not self._coordination_token_needs_refresh():
            if not self._coordination_id_token:
                raise RuntimeError("RTDB coordination ID token is unexpectedly empty")
            return self._coordination_id_token

        try:
            return self._refresh_coordination_id_token()
        except Exception:
            return self._exchange_coordination_custom_token()

    def _coordination_rtdb_url(
        self,
        node_path: str,
        id_token: Optional[str] = None,
        query: Optional[Dict[str, str]] = None,
    ) -> str:
        coord = self._coordination
        if not coord:
            raise RuntimeError("RTDB coordination is not configured")
        database_url = coord.get("databaseUrl")
        if not isinstance(database_url, str) or not database_url:
            raise RuntimeError("RTDB coordination database URL is unavailable")

        parsed = urllib.parse.urlparse(database_url)
        base_path = parsed.path.rstrip("/")
        clean_node = self._normalize_coordination_path(node_path) or "/"
        target_path = f"{base_path}{clean_node}.json"
        query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if query:
            query_items.extend((str(key), str(value)) for key, value in query.items())
        if id_token:
            query_items.append(("auth", id_token))
        return urllib.parse.urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                target_path,
                "",
                urllib.parse.urlencode(query_items),
                "",
            )
        )

    def _coordination_set_stream_health(self, healthy: bool) -> None:
        was_healthy = bool(self._coordination_stream_healthy)
        self._coordination_stream_healthy = bool(healthy)
        if was_healthy and not healthy:
            self._request_agent_queue_poll()
            self._request_dependency_queue_poll()

    def _coordination_restart_stream(self) -> None:
        old_thread = self._coordination_stream_thread
        old_stop = self._coordination_stream_stop
        old_stop.set()
        self._coordination_set_stream_health(False)
        if old_thread and old_thread.is_alive():
            old_thread.join(timeout=2.0)

        if not self._coordination or self._stop.is_set():
            self._coordination_stream_thread = None
            return

        self._coordination_stream_stop = threading.Event()
        thread = threading.Thread(
            target=self._coordination_stream_loop,
            name="dm-rtdb-coordination",
            daemon=True,
        )
        self._coordination_stream_thread = thread
        thread.start()

    def _clear_coordination(self, source: str) -> None:
        had_coordination = bool(self._coordination)
        had_stream_healthy = bool(self._coordination_stream_healthy)
        if had_coordination:
            logging.info("RTDB coordination disabled from %s; reverting to legacy polling.", source)
        self._coordination = None
        self._coordination_id_token = None
        self._coordination_refresh_token = None
        self._coordination_id_token_expires_at_ms = 0
        self._coordination_dependency_http_checkpoint_due_ms = 0
        self._coordination_agent_http_checkpoint_due_ms = 0
        self._coordination_agent_direct_empty_probe_ms = 0
        self._coordination_dependency_direct_empty_probe_ms = 0
        self._coordination_restart_stream()
        if had_coordination and not had_stream_healthy:
            self._request_agent_queue_poll()
            self._request_dependency_queue_poll()

    def _set_coordination_from_response(self, response: Any, source: str) -> None:
        raw = response.get("coordination") if isinstance(response, dict) else None
        normalized = self._normalize_coordination_payload(raw)
        if normalized is None:
            self._clear_coordination(source)
            return

        current_key = self._coordination.get("configKey") if isinstance(self._coordination, dict) else None
        if current_key == normalized.get("configKey"):
            self._coordination = normalized
            return

        self._coordination = normalized
        self._coordination_id_token = None
        self._coordination_refresh_token = None
        self._coordination_id_token_expires_at_ms = 0
        self._coordination_dependency_http_checkpoint_due_ms = 0
        self._coordination_agent_http_checkpoint_due_ms = 0
        self._coordination_agent_direct_empty_probe_ms = 0
        self._coordination_dependency_direct_empty_probe_ms = 0
        logging.info(
            "RTDB coordination enabled from %s: db=%s dependencySafetyPoll=%.1fs agentSafetyPoll=%.1fs agentSignalSafetyMin=%.1fs checkpoint=%.0fs features=%s",
            source,
            _safe_url_for_logs(str(normalized.get("databaseUrl") or "")),
            float(normalized["safetyPollSeconds"]["dependencies"]),
            float(normalized["safetyPollSeconds"]["agent"]),
            float(self.agent_rtdb_signal_safety_min_seconds),
            float(normalized["firestoreCheckpointSeconds"]),
            json.dumps(normalized.get("features", {}), sort_keys=True),
        )
        self._coordination_restart_stream()

    def _runtime_config_seconds(self, value: Any, minimum: float, maximum: float) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if parsed != parsed or parsed <= 0:
            return None
        return max(float(minimum), min(float(maximum), parsed))

    def _apply_agent_runtime_config(self, raw: Any, source: str) -> None:
        if not isinstance(raw, dict):
            return

        changes: List[str] = []
        heartbeat_seconds = self._runtime_config_seconds(raw.get("activeAgentHeartbeatSec", raw.get("agentHeartbeatSec")), 2.0, 30.0)
        if heartbeat_seconds is not None and (
            self.agent_runtime_config_overrides_env or not self._agent_heartbeat_seconds_env
        ):
            if abs(float(self.agent_heartbeat_seconds) - heartbeat_seconds) >= 0.1:
                self.agent_heartbeat_seconds = heartbeat_seconds
                if self.agent_idle_heartbeat_seconds < self.agent_heartbeat_seconds:
                    self.agent_idle_heartbeat_seconds = self.agent_heartbeat_seconds
                if not self._agent_signal_heartbeat_min_seconds_env:
                    self.agent_signal_heartbeat_min_seconds = self.agent_heartbeat_seconds
                changes.append(f"activeHeartbeat={heartbeat_seconds:.1f}s")

        idle_heartbeat_seconds = self._runtime_config_seconds(raw.get("idleAgentHeartbeatSec"), 2.0, 300.0)
        if idle_heartbeat_seconds is not None and (
            self.agent_runtime_config_overrides_env or not self._agent_idle_heartbeat_seconds_env
        ):
            idle_heartbeat_seconds = max(float(self.agent_heartbeat_seconds), idle_heartbeat_seconds)
            if abs(float(self.agent_idle_heartbeat_seconds) - idle_heartbeat_seconds) >= 0.1:
                self.agent_idle_heartbeat_seconds = idle_heartbeat_seconds
                changes.append(f"idleHeartbeat={idle_heartbeat_seconds:.1f}s")

        poll_seconds = self._runtime_config_seconds(raw.get("agentPollSec"), 0.5, 30.0)
        if poll_seconds is not None and (
            self.agent_runtime_config_overrides_env or not self._agent_poll_seconds_env
        ):
            if abs(float(self.agent_poll_seconds) - poll_seconds) >= 0.1:
                self.agent_poll_seconds = poll_seconds
                changes.append(f"poll={poll_seconds:.1f}s")

        progress_event_seconds = self._runtime_config_seconds(raw.get("progressEventSec"), 60.0, 600.0)
        if progress_event_seconds is not None and not self._agent_progress_event_seconds_env:
            progress_event_ms = int(progress_event_seconds * 1000)
            if abs(float(self.agent_progress_event_ms) - progress_event_ms) >= 100:
                self.agent_progress_event_ms = progress_event_ms
                changes.append(f"progressEvent={progress_event_seconds:.1f}s")

        waiting_deps_event_seconds = self._runtime_config_seconds(raw.get("waitingDepsEventSec"), 60.0, 600.0)
        if waiting_deps_event_seconds is not None and not self._agent_waiting_deps_event_seconds_env:
            waiting_deps_event_ms = int(waiting_deps_event_seconds * 1000)
            if abs(float(self.agent_waiting_deps_event_ms) - waiting_deps_event_ms) >= 100:
                self.agent_waiting_deps_event_ms = waiting_deps_event_ms
                changes.append(f"waitingDepsEvent={waiting_deps_event_seconds:.1f}s")

        if changes:
            logging.info("Applied agent runtime config from %s: %s", source, ", ".join(changes))

    def _coordination_should_use_safety_polls(self) -> bool:
        return bool(self._coordination and self._coordination_stream_healthy and self.agent_rtdb_signal_wait_enabled)

    def _has_active_agent_or_dependency_work_for_heartbeat(self) -> bool:
        with self._lock:
            return bool(
                self._active_exec_by_item
                or self._active_maintenance_by_item
                or self._downloading
                or self._input_cache_downloading
                or self._agent_prefetch_inflight
                or self._agent_execute_inflight
                or self._agent_upload_inflight
                or self._agent_maintenance_inflight
            )

    def _agent_heartbeat_interval_seconds(self) -> float:
        if (
            self._coordination_should_use_safety_polls()
            and not self._has_active_agent_or_dependency_work_for_heartbeat()
        ):
            return max(float(self.agent_heartbeat_seconds), float(self.agent_idle_heartbeat_seconds))
        return float(self.agent_heartbeat_seconds)

    def _coordination_dependency_poll_seconds(self) -> float:
        if self._coordination_should_use_safety_polls():
            return float(self._coordination["safetyPollSeconds"]["dependencies"])
        return float(self.poll_seconds)

    def _coordination_agent_poll_seconds(self) -> float:
        if self._coordination_should_use_safety_polls():
            return max(
                float(self._coordination["safetyPollSeconds"]["agent"]),
                float(self.agent_rtdb_signal_safety_min_seconds),
            )
        return float(self.agent_poll_seconds)

    def _coordination_http_checkpoint_due(self, now_ms: Optional[int] = None, channel: str = "dependencyManager") -> bool:
        if not self._coordination:
            return True
        if self._coordination.get("legacyHttpFallback") is not True:
            return False
        at_ms = int(now_ms if isinstance(now_ms, int) else _now_ms())
        due_ms = (
            self._coordination_agent_http_checkpoint_due_ms
            if channel == "agentControl"
            else self._coordination_dependency_http_checkpoint_due_ms
        )
        return at_ms >= int(due_ms)

    def _coordination_note_http_checkpoint(self, now_ms: Optional[int] = None, channel: str = "dependencyManager") -> None:
        at_ms = int(now_ms if isinstance(now_ms, int) else _now_ms())
        if not self._coordination:
            if channel == "agentControl":
                self._coordination_agent_http_checkpoint_due_ms = at_ms
            else:
                self._coordination_dependency_http_checkpoint_due_ms = at_ms
            return
        interval_sec = float(self._coordination.get("firestoreCheckpointSeconds") or 60.0)
        next_due_ms = at_ms + int(max(300.0, interval_sec) * 1000)
        if channel == "agentControl":
            self._coordination_agent_http_checkpoint_due_ms = next_due_ms
        else:
            self._coordination_dependency_http_checkpoint_due_ms = next_due_ms

    def _coordination_runtime_retry_delay_seconds(self, attempt: int) -> float:
        return min(2.0, 0.5 * (2 ** max(0, int(attempt))))

    def _is_coordination_runtime_retryable_api_status(self, status: int) -> bool:
        return int(status) in (408, 409, 429, 500, 502, 503, 504)

    def _flatten_rtdb_patch(self, value: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, nested in (value or {}).items():
            path = f"{prefix}/{key}" if prefix else str(key)
            if nested is None:
                out[path] = None
                continue
            if isinstance(nested, dict):
                out.update(self._flatten_rtdb_patch(nested, path))
            else:
                out[path] = nested
        return out

    def _coordination_patch_runtime(self, patch: Dict[str, Any], timeout_seconds: float = 15.0) -> bool:
        if not self._coordination:
            return False
        flattened_patch = self._flatten_rtdb_patch(patch)
        if not flattened_patch:
            return False

        def _attempt(id_token: str) -> bool:
            url = self._coordination_rtdb_url(
                self._coordination["paths"]["runtimeRoot"],
                id_token=id_token,
                query={"print": "silent"},
            )
            status, resp = api_json("PATCH", url, body=flattened_patch, timeout_seconds=timeout_seconds)
            if status not in (200, 204):
                raise RuntimeError(f"Unexpected RTDB runtime patch response: {status} {resp}")
            return True

        def _attempt_with_transient_retry(id_token: str) -> bool:
            for attempt in range(2):
                try:
                    return _attempt(id_token)
                except ApiError as e:
                    if e.status in (401, 403):
                        raise
                    if attempt == 0 and self._is_coordination_runtime_retryable_api_status(e.status):
                        _sleep_with_jitter(
                            self._coordination_runtime_retry_delay_seconds(attempt),
                            jitter_ratio=0.1,
                        )
                        continue
                    raise
                except NetworkError:
                    if attempt == 0:
                        _sleep_with_jitter(
                            self._coordination_runtime_retry_delay_seconds(attempt),
                            jitter_ratio=0.1,
                        )
                        continue
                    raise
            return False

        try:
            return _attempt_with_transient_retry(self._ensure_coordination_id_token())
        except ApiError as e:
            if e.status not in (401, 403):
                logging.warning("RTDB runtime patch API error: %s", e)
                return False
        except Exception as e:
            logging.warning("RTDB runtime patch failed: %s", e)
            return False

        try:
            return _attempt_with_transient_retry(self._ensure_coordination_id_token(force_refresh=True))
        except Exception as e:
            logging.warning("RTDB runtime patch retry failed: %s", e)
            return False

    def _coordination_feature_enabled(self, name: str) -> bool:
        coord = self._coordination
        if not coord:
            return False
        features = coord.get("features") if isinstance(coord.get("features"), dict) else {}
        enabled = bool(features.get(name))
        if name in ("agentQueueClaimV1", "dependencyQueueClaimV1"):
            return enabled and bool(self.agent_rtdb_queue_claim_enabled)
        if name == "agentLeaseHeartbeatV1":
            return enabled and bool(self.agent_rtdb_lease_heartbeat_enabled)
        return enabled

    def _coordination_get_json(
        self,
        node_path: str,
        timeout_seconds: float = 10.0,
        query: Optional[Dict[str, str]] = None,
    ) -> Optional[Any]:
        id_token = self._ensure_coordination_id_token()
        status, resp = api_json(
            "GET",
            self._coordination_rtdb_url(node_path, id_token=id_token, query=query),
            timeout_seconds=timeout_seconds,
        )
        if status == 200:
            if resp == "null":
                return None
            return resp
        if status == 204:
            return None
        raise RuntimeError(f"Unexpected RTDB GET response: {status} {resp}")

    def _coordination_get_json_with_etag(
        self,
        node_path: str,
        timeout_seconds: float = 10.0,
    ) -> Tuple[Optional[Any], str]:
        id_token = self._ensure_coordination_id_token()
        url = self._coordination_rtdb_url(node_path, id_token=id_token)
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "X-Firebase-ETag": "true",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = _json_loads_or_none(raw) if raw else None
                return parsed, str(resp.headers.get("ETag") or "")
        except urllib.error.HTTPError as e:
            raw = ""
            try:
                raw = e.read().decode("utf-8", errors="replace")
            except Exception:
                raw = ""
            raise ApiError(int(getattr(e, "code", 500) or 500), raw) from None
        except (urllib.error.URLError, socket.timeout, TimeoutError, http.client.HTTPException) as e:
            raise NetworkError(url, e) from None

    def _coordination_put_json_if_match(
        self,
        node_path: str,
        value: Dict[str, Any],
        etag: str,
        timeout_seconds: float = 10.0,
    ) -> bool:
        if not etag:
            return False
        id_token = self._ensure_coordination_id_token()
        url = self._coordination_rtdb_url(node_path, id_token=id_token)
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            method="PUT",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "If-Match": etag,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                return int(resp.status) in (200, 204)
        except urllib.error.HTTPError as e:
            if int(getattr(e, "code", 500) or 500) == 412:
                return False
            raw = ""
            try:
                raw = e.read().decode("utf-8", errors="replace")
            except Exception:
                raw = ""
            raise ApiError(int(getattr(e, "code", 500) or 500), raw) from None
        except (urllib.error.URLError, socket.timeout, TimeoutError, http.client.HTTPException) as e:
            raise NetworkError(url, e) from None

    def _gpu_admission_root_path(self) -> str:
        server_type = (self.server_type or "").strip()
        if not server_type or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", server_type):
            raise RuntimeError("A valid SERVER_TYPE is required for GPU admission")
        return f"/gpuAdmission/{server_type}"

    def _gpu_admission_prune(self, raw: Any, now_ms: int) -> Dict[str, Any]:
        root = dict(raw) if isinstance(raw, dict) else {}
        tickets_raw = root.get("tickets") if isinstance(root.get("tickets"), dict) else {}
        tickets: Dict[str, Any] = {}
        for ticket_id, ticket_value in tickets_raw.items():
            if not isinstance(ticket_value, dict):
                continue
            ticket = dict(ticket_value)
            expires_at_ms = int(ticket.get("expiresAtMs") or 0)
            heartbeat_at_ms = int(ticket.get("heartbeatAtMs") or 0)
            is_active = str(ticket.get("state") or "") == "active"
            is_parked_async = ticket.get("holder") == "inference_async" and ticket.get("attached") is not True
            if expires_at_ms > now_ms and (
                is_active or is_parked_async or now_ms - heartbeat_at_ms <= self.gpu_admission_ticket_stale_ms
            ):
                tickets[str(ticket_id)] = ticket
        root["tickets"] = tickets
        return root

    def _gpu_admission_transaction(
        self,
        mutate: Callable[[Dict[str, Any]], Tuple[Optional[Dict[str, Any]], Any]],
        attempts: int = 24,
    ) -> Any:
        if not self._coordination:
            raise RuntimeError("RTDB coordination is unavailable for GPU admission")
        root_path = self._gpu_admission_root_path()
        for attempt in range(max(1, attempts)):
            raw, etag = self._coordination_get_json_with_etag(root_path, timeout_seconds=10.0)
            replacement, result = mutate(dict(raw) if isinstance(raw, dict) else {})
            if replacement is None:
                return result
            if self._coordination_put_json_if_match(root_path, replacement, etag, timeout_seconds=10.0):
                return result
            _sleep_with_jitter(min(0.5, 0.02 * (2 ** min(5, attempt))), jitter_ratio=0.25)
        raise RuntimeError("GPU admission transaction remained contended")

    def _gpu_admission_ticket_key(self, lease: AgentExecuteLease) -> str:
        identity = f"{self.instance_id or 'instance'}:{lease.item_id}"
        return "comfy_" + base64.urlsafe_b64encode(identity.encode("utf-8")).decode("ascii").rstrip("=")

    def _enqueue_comfy_gpu_admission(self, lease: AgentExecuteLease, estimated_duration_ms: int) -> Optional[str]:
        if self.gpu_admission_mode != "enforcing":
            return None
        ticket_id = self._gpu_admission_ticket_key(lease)
        now_ms = _now_ms()

        def mutate(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
            root = self._gpu_admission_prune(raw, now_ms)
            tickets = root["tickets"]
            existing = tickets.get(ticket_id)
            if isinstance(existing, dict):
                existing["heartbeatAtMs"] = now_ms
                existing["expiresAtMs"] = max(int(existing.get("expiresAtMs") or 0), now_ms + 3_600_000)
                existing["attached"] = True
                root["updatedAtMs"] = now_ms
                root["revision"] = int(root.get("revision") or 0) + 1
                return root, ticket_id
            if len(tickets) >= self.gpu_admission_max_depth:
                raise GPUCoordinatorBusy("GPU admission queue is full", 5.0)
            sequence = int(root.get("nextSequence") or 0) + 1
            root["nextSequence"] = sequence
            tickets[ticket_id] = {
                "ticketId": ticket_id,
                "requestId": str(lease.job_id)[:128],
                "holder": "comfy",
                "sequence": sequence,
                "enqueuedAtMs": now_ms,
                "heartbeatAtMs": now_ms,
                "expiresAtMs": now_ms + 3_600_000,
                "estimatedDurationMs": max(1_000, min(3_600_000, int(estimated_duration_ms))),
                "stream": False,
                "attached": True,
                "state": "waiting",
            }
            root["updatedAtMs"] = now_ms
            root["revision"] = int(root.get("revision") or 0) + 1
            return root, ticket_id

        lease.gpu_admission_ticket_id = self._gpu_admission_transaction(mutate)
        return lease.gpu_admission_ticket_id

    def _recover_gpu_admission_if_locally_idle(self) -> bool:
        try:
            coordinator_status = self._gpu_coordinator.admission_recovery_status()
        except Exception as exc:
            logging.warning("GPU admission recovery could not verify the local coordinator: %s", exc)
            return False
        if coordinator_status.get("safeToClearAdmission") is not True:
            return False
        now_ms = _now_ms()

        def mutate(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
            root = self._gpu_admission_prune(raw, now_ms)
            active = root.get("active")
            if not isinstance(active, dict) or active.get("state") != "recovering":
                return None, False
            if now_ms - int(active.get("heartbeatAtMs") or 0) < self.gpu_admission_recovery_ms:
                return None, False
            ticket_id = str(active.get("ticketId") or "")
            root["active"] = None
            root["tickets"].pop(ticket_id, None)
            root["updatedAtMs"] = now_ms
            root["revision"] = int(root.get("revision") or 0) + 1
            return root, True

        recovered = bool(self._gpu_admission_transaction(mutate))
        if recovered:
            logging.warning("Recovered a stale GPU admission claim after verifying the local coordinator was idle")
        return recovered

    def _wait_for_comfy_gpu_admission(self, lease: AgentExecuteLease, estimated_duration_ms: int) -> Optional[str]:
        ticket_id = self._enqueue_comfy_gpu_admission(lease, estimated_duration_ms)
        if ticket_id is None:
            return None
        last_heartbeat_ms = 0
        while not self._stop.is_set():
            if self._is_cancel_requested(lease):
                self._release_comfy_gpu_admission(lease, "cancelled_while_waiting")
                raise RuntimeError("Cancellation requested while waiting for GPU admission")
            now_ms = _now_ms()
            should_heartbeat = now_ms - last_heartbeat_ms >= int(self.gpu_admission_heartbeat_seconds * 1000)
            saw_recovering = False

            def mutate(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
                nonlocal saw_recovering
                root = self._gpu_admission_prune(raw, now_ms)
                tickets = root["tickets"]
                ticket = tickets.get(ticket_id)
                if not isinstance(ticket, dict):
                    return None, None
                active = root.get("active")
                if isinstance(active, dict):
                    if active.get("ticketId") == ticket_id:
                        return None, str(active.get("claimToken") or "") or None
                    if now_ms - int(active.get("heartbeatAtMs") or 0) >= self.gpu_admission_recovery_ms:
                        if active.get("state") != "recovering":
                            active["state"] = "recovering"
                            root["active"] = active
                            root["updatedAtMs"] = now_ms
                            root["revision"] = int(root.get("revision") or 0) + 1
                            saw_recovering = True
                            return root, None
                        saw_recovering = True
                    if should_heartbeat:
                        ticket["heartbeatAtMs"] = now_ms
                        root["updatedAtMs"] = now_ms
                        root["revision"] = int(root.get("revision") or 0) + 1
                        return root, None
                    return None, None
                eligible = sorted(
                    (
                        row for row in tickets.values()
                        if isinstance(row, dict) and row.get("attached") is True and row.get("state") == "waiting"
                    ),
                    key=lambda row: (int(row.get("sequence") or 0), str(row.get("ticketId") or "")),
                )
                if not eligible or eligible[0].get("ticketId") != ticket_id:
                    if should_heartbeat:
                        ticket["heartbeatAtMs"] = now_ms
                        root["updatedAtMs"] = now_ms
                        root["revision"] = int(root.get("revision") or 0) + 1
                        return root, None
                    return None, None
                claim_token = uuid.uuid4().hex
                ticket["state"] = "active"
                ticket["heartbeatAtMs"] = now_ms
                root["active"] = {
                    "ticketId": ticket_id,
                    "requestId": str(ticket.get("requestId") or ""),
                    "holder": "comfy",
                    "sequence": int(ticket.get("sequence") or 0),
                    "claimToken": claim_token,
                    "claimedAtMs": now_ms,
                    "heartbeatAtMs": now_ms,
                    "state": "active",
                }
                root["updatedAtMs"] = now_ms
                root["revision"] = int(root.get("revision") or 0) + 1
                return root, claim_token

            claim_token = self._gpu_admission_transaction(mutate)
            if claim_token:
                lease.gpu_admission_claim_token = str(claim_token)
                logging.info("Claimed FIFO GPU admission ticket=%s jobId=%s", ticket_id, lease.job_id)
                return lease.gpu_admission_claim_token
            if saw_recovering:
                self._recover_gpu_admission_if_locally_idle()
            if should_heartbeat:
                last_heartbeat_ms = now_ms
            self._stop.wait(0.5)
        self._release_comfy_gpu_admission(lease, "agent_stopping")
        raise RuntimeError("Agent stopped while waiting for GPU admission")

    def _heartbeat_comfy_gpu_admission(self, ticket_id: str, claim_token: str) -> bool:
        now_ms = _now_ms()

        def mutate(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
            root = self._gpu_admission_prune(raw, now_ms)
            active = root.get("active")
            ticket = root["tickets"].get(ticket_id)
            if not isinstance(active, dict) or not isinstance(ticket, dict):
                return None, False
            if active.get("ticketId") != ticket_id or active.get("claimToken") != claim_token:
                return None, False
            active["heartbeatAtMs"] = now_ms
            active["state"] = "active"
            ticket["heartbeatAtMs"] = now_ms
            root["updatedAtMs"] = now_ms
            root["revision"] = int(root.get("revision") or 0) + 1
            return root, True

        return bool(self._gpu_admission_transaction(mutate))

    def _release_comfy_gpu_admission(self, lease: AgentExecuteLease, reason: str) -> None:
        ticket_id = lease.gpu_admission_ticket_id
        claim_token = lease.gpu_admission_claim_token
        lease.gpu_admission_ticket_id = None
        lease.gpu_admission_claim_token = None
        if not ticket_id or self.gpu_admission_mode != "enforcing":
            return
        now_ms = _now_ms()

        def mutate(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
            root = self._gpu_admission_prune(raw, now_ms)
            active = root.get("active")
            if claim_token and isinstance(active, dict) and (
                active.get("ticketId") != ticket_id or active.get("claimToken") != claim_token
            ):
                return None, False
            root["tickets"].pop(ticket_id, None)
            if isinstance(active, dict) and active.get("ticketId") == ticket_id:
                elapsed_ms = max(1, now_ms - int(active.get("claimedAtMs") or now_ms))
                previous_ms = int(root.get("serviceEwmaMs") or elapsed_ms)
                root["serviceEwmaMs"] = round(previous_ms * 0.8 + elapsed_ms * 0.2)
                root["active"] = None
            root["updatedAtMs"] = now_ms
            root["revision"] = int(root.get("revision") or 0) + 1
            return root, True

        try:
            self._gpu_admission_transaction(mutate)
            logging.info("Released FIFO GPU admission ticket=%s jobId=%s reason=%s", ticket_id, lease.job_id, reason)
        except Exception as exc:
            logging.error("Failed releasing FIFO GPU admission ticket=%s jobId=%s: %s", ticket_id, lease.job_id, exc)

    def _gpu_admission_has_foreground_work(self) -> bool:
        if self.gpu_admission_mode != "enforcing":
            return False
        if not self._coordination:
            return True
        try:
            raw = self._coordination_get_json(self._gpu_admission_root_path(), timeout_seconds=5.0)
            root = self._gpu_admission_prune(raw, _now_ms())
            if root.get("durableDemandActive") is True:
                return True
            if isinstance(root.get("active"), dict):
                return True
            return any(
                isinstance(ticket, dict) and ticket.get("attached") is True
                for ticket in root.get("tickets", {}).values()
            )
        except Exception as exc:
            logging.warning("Failing closed on mining because GPU admission state is unavailable: %s", exc)
            return True

    def _coordination_queue_item_path(self, root_path: str, encoded_key: str) -> str:
        root = self._normalize_coordination_path(root_path) or ""
        key = str(encoded_key or "").strip().strip("/")
        if not root or not key:
            raise RuntimeError("RTDB queue item path is unavailable")
        return f"{root}/{key}"

    def _coordination_queue_item_key(self, item_id: str) -> str:
        normalized = str(item_id or "").strip()
        if not normalized:
            return "default"
        return base64.urlsafe_b64encode(normalized.encode("utf-8")).decode("utf-8").rstrip("=") or "default"

    def _coordination_candidate_sort_key(self, item: Dict[str, Any]) -> Tuple[float, int, str]:
        priority = item.get("priority")
        created = item.get("createdAtMs")
        return (
            -float(priority if isinstance(priority, (int, float)) else 0),
            int(created if isinstance(created, (int, float)) else 0),
            str(item.get("itemId") or ""),
        )

    def _coordination_collect_queue_candidates(
        self,
        queue_path_key: str,
        raw: Optional[Any],
        skip_execute_jobs: bool,
    ) -> Optional[List[Tuple[str, Dict[str, Any]]]]:
        if raw is None or raw == "null":
            return []
        if not isinstance(raw, dict):
            logging.warning("RTDB queue root for %s was not an object; falling back to HTTP.", queue_path_key)
            return None

        candidates: List[Tuple[str, Dict[str, Any]]] = []
        dependency_http_fallback_required = False
        for encoded_key, value in raw.items():
            if not isinstance(encoded_key, str) or not isinstance(value, dict):
                continue
            item = dict(value)
            item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else encoded_key
            if not isinstance(item_id, str) or not item_id:
                continue
            item["itemId"] = item_id
            if str(item.get("state") or "") != "queued":
                continue
            if skip_execute_jobs and str(item.get("type") or "") == "execute_job":
                continue
            if queue_path_key == "dependencyQueueItems":
                op = item.get("op")
                resolved = item.get("resolved")
                payload_source = str(item.get("payloadSource") or "")
                if op in ("download", "touch", "delete") and not isinstance(resolved, dict) and payload_source != "firestore":
                    # A stale or partially written mirror must not force the entire
                    # dependency queue through the HTTP fallback. That lets one
                    # malformed high-priority item starve every valid metadata-only
                    # RTDB item behind it. Preserve fallback when it is the only
                    # candidate, but skip it when direct-claimable work exists.
                    dependency_http_fallback_required = True
                    logging.info(
                        "RTDB dependency queue item %s lacks resolved payload; skipping it while collecting direct candidates.",
                        item_id,
                    )
                    continue
            candidates.append((encoded_key, item))
        if candidates:
            return candidates
        if dependency_http_fallback_required:
            return None
        return candidates

    def _coordination_claim_queue_items(
        self,
        queue_path_key: str,
        feature_name: str,
        target_state: str,
        limit: int,
        skip_execute_jobs: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        if not self._coordination or not self._coordination_stream_healthy:
            return None
        if not self._coordination_feature_enabled(feature_name):
            return None
        paths = self._coordination.get("paths") if isinstance(self._coordination.get("paths"), dict) else {}
        root_path = paths.get(queue_path_key)
        if not isinstance(root_path, str) or not root_path:
            return None

        query_attempts: List[Tuple[str, Dict[str, str]]] = []
        if queue_path_key == "agentQueueItems":
            claim_window = max(4, min(40, max(1, int(limit)) * 4))
            query_attempts.append(
                (
                    "claimOrderKey",
                    {
                        "orderBy": json.dumps("claimOrderKey"),
                        "startAt": json.dumps("queued|"),
                        "endAt": json.dumps("queued|\uf8ff"),
                        "limitToFirst": str(claim_window),
                    },
                )
            )
        query_attempts.append(
            (
                "state",
                {
                    "orderBy": json.dumps("state"),
                    "equalTo": json.dumps("queued"),
                },
            )
        )

        candidates: Optional[List[Tuple[str, Dict[str, Any]]]] = None
        for query_name, query in query_attempts:
            try:
                raw = self._coordination_get_json(
                    root_path,
                    timeout_seconds=10.0,
                    query=query,
                )
            except Exception as e:
                logging.warning("RTDB queue read failed for %s via %s: %s", queue_path_key, query_name, e)
                return None
            parsed = self._coordination_collect_queue_candidates(queue_path_key, raw, skip_execute_jobs)
            if parsed is None:
                return None
            if parsed or query_name == "state":
                candidates = parsed
                break

        if candidates is None:
            return None

        if not candidates:
            return []

        claimed: List[Dict[str, Any]] = []
        instance_id = str(self._resolved_instance_id or "")
        lease_duration_sec = float(self._coordination.get("leaseDurationSeconds") or 90.0)
        for encoded_key, item in sorted(candidates, key=lambda row: self._coordination_candidate_sort_key(row[1])):
            if len(claimed) >= max(1, int(limit)):
                break
            item_path = self._coordination_queue_item_path(root_path, encoded_key)
            try:
                current, etag = self._coordination_get_json_with_etag(item_path, timeout_seconds=10.0)
                if not isinstance(current, dict) or str(current.get("state") or "") != "queued":
                    continue
                lease_id = f"lease_{uuid.uuid4().hex}"
                lease_expires_at_ms = self._server_now_ms() + int(max(10.0, lease_duration_sec) * 1000)
                attempts = int(current.get("attempts") or 0) if isinstance(current.get("attempts"), (int, float)) else 0
                next_value = dict(current)
                next_value.update(
                    {
                        "state": target_state,
                        "leaseOwner": instance_id,
                        "leaseId": lease_id,
                        "leaseExpiresAtMs": lease_expires_at_ms,
                        "leaseExpiresAt": _ms_to_iso(lease_expires_at_ms),
                        "attempts": attempts + 1,
                        "updatedAtMs": self._server_now_ms(),
                    }
                )
                claimed_item = dict(next_value)
                claimed_item["itemId"] = str(current.get("itemId") or item.get("itemId") or encoded_key)
                write_value = dict(next_value)
                if queue_path_key == "agentQueueItems":
                    payload = current.get("payload")
                    if isinstance(payload, dict):
                        if isinstance(payload.get("jobId"), str):
                            write_value.setdefault("requestedByJobId", payload.get("jobId"))
                        if isinstance(payload.get("executionAttempt"), (int, float)):
                            write_value.setdefault("executionAttempt", int(payload.get("executionAttempt")))
                        if isinstance(payload.get("attemptEpoch"), (int, float)):
                            write_value.setdefault("attemptEpoch", int(payload.get("attemptEpoch")))
                    # The agent has the payload in claimed_item; keeping it in the
                    # leased RTDB mirror makes every heartbeat/event reconciliation
                    # reread the full command body.
                    write_value.pop("payload", None)
                    write_value.pop("claimOrderKey", None)
                elif queue_path_key == "dependencyQueueItems":
                    write_value.pop("payload", None)
                    write_value.pop("resolved", None)
                if not self._coordination_put_json_if_match(item_path, write_value, etag, timeout_seconds=10.0):
                    continue
                if queue_path_key == "agentQueueItems" and not isinstance(claimed_item.get("payload"), dict):
                    fetched_item = self._agent_fetch_queue_item(claimed_item["itemId"], lease_id)
                    if isinstance(fetched_item, dict) and isinstance(fetched_item.get("payload"), dict):
                        claimed_item.update(fetched_item)
                        claimed_item["leaseId"] = lease_id
                        claimed_item["leaseExpiresAt"] = _ms_to_iso(lease_expires_at_ms)
                    else:
                        logging.warning(
                            "RTDB agent queue claim %s/%s had no payload and queue-item fetch returned no payload; lease will expire.",
                            queue_path_key,
                            claimed_item.get("itemId"),
                        )
                        continue
                if queue_path_key == "dependencyQueueItems":
                    op = claimed_item.get("op")
                    if op in ("download", "touch", "delete") and not isinstance(claimed_item.get("resolved"), dict):
                        fetched_item = self._fetch_queue_item(claimed_item["itemId"])
                        if isinstance(fetched_item, dict) and isinstance(fetched_item.get("resolved"), dict):
                            claimed_item.update(fetched_item)
                            claimed_item["leaseId"] = lease_id
                            claimed_item["leaseExpiresAt"] = _ms_to_iso(lease_expires_at_ms)
                        else:
                            logging.warning(
                                "RTDB dependency queue claim %s/%s had no resolved payload and queue-item fetch returned no resolved payload; lease will expire.",
                                queue_path_key,
                                claimed_item.get("itemId"),
                            )
                            continue
                claimed.append(claimed_item)
            except Exception as e:
                logging.warning("RTDB queue claim failed for %s/%s: %s", queue_path_key, item.get("itemId"), e)
                return None
        return claimed

    def _coordination_fetch_agent_queue(self, limit: int) -> Optional[List[Dict[str, Any]]]:
        skip_execute_jobs = bool(self.mining_only)
        if not skip_execute_jobs:
            try:
                skip_execute_jobs = not (
                    self._local_comfy_reachable(timeout_seconds=2.0)
                    and self._local_readiness_file_present()
                )
            except Exception as e:
                logging.debug("Skipping direct RTDB execute_job claims while local readiness probe fails: %s", e)
                skip_execute_jobs = True
        items = self._coordination_claim_queue_items(
            "agentQueueItems",
            "agentQueueClaimV1",
            "leased",
            limit,
            skip_execute_jobs=skip_execute_jobs,
        )
        if items == [] and skip_execute_jobs:
            return []
        if items == [] and self._coordination_direct_empty_probe_due("agent"):
            return None
        return items

    def _coordination_fetch_dependency_queue(self, limit: int) -> Optional[List[Dict[str, Any]]]:
        items = self._coordination_claim_queue_items(
            "dependencyQueueItems",
            "dependencyQueueClaimV1",
            "running",
            limit,
        )
        if items == [] and self._coordination_direct_empty_probe_due("dependency"):
            return None
        return items

    def _coordination_check_active_lease_cancels(self, held_leases: List[Dict[str, Any]]) -> None:
        if not held_leases or not self._coordination_feature_enabled("agentLeaseHeartbeatV1"):
            return
        paths = self._coordination.get("paths") if isinstance(self._coordination.get("paths"), dict) else {}
        root_path = paths.get("agentQueueItems")
        if not isinstance(root_path, str) or not root_path:
            return
        now_ms = self._server_now_ms()
        lease_duration_sec = float(self._coordination.get("leaseDurationSeconds") or 90.0)
        lease_duration_ms = int(max(10.0, lease_duration_sec) * 1000)
        refresh_window_ms = max(5_000, min(max(1_000, lease_duration_ms - 1_000), int(lease_duration_ms * 0.6)))
        for lease in held_leases:
            item_id = lease.get("itemId")
            lease_id = lease.get("leaseId")
            if not isinstance(item_id, str) or not isinstance(lease_id, str):
                continue
            item_path = self._coordination_queue_item_path(root_path, self._coordination_queue_item_key(item_id))
            try:
                row = self._coordination_get_json(item_path, timeout_seconds=10.0)
            except Exception as e:
                logging.warning("RTDB active lease cancel check failed for %s: %s", item_id, e)
                continue
            if not isinstance(row, dict):
                continue
            if row.get("leaseId") != lease_id:
                continue
            if row.get("itemId") not in (None, item_id):
                continue
            state = str(row.get("state") or "")
            if state == "cancel_requested":
                self._mark_cancel_signal(
                    {
                        "jobId": lease.get("jobId"),
                        "executionAttempt": lease.get("executionAttempt"),
                        "attemptEpoch": lease.get("attemptEpoch"),
                        "leaseId": lease_id,
                        "reason": row.get("cancelReason") if isinstance(row.get("cancelReason"), str) else "cancel_requested",
                    }
                )
                continue
            if state not in RTDB_AGENT_NON_TERMINAL_QUEUE_STATES:
                continue
            lease_expires_at_ms = int(row.get("leaseExpiresAtMs") or 0) if isinstance(row.get("leaseExpiresAtMs"), (int, float)) else 0
            if lease_expires_at_ms > now_ms + refresh_window_ms:
                continue
            try:
                current, etag = self._coordination_get_json_with_etag(item_path, timeout_seconds=10.0)
                if (
                    not isinstance(current, dict)
                    or current.get("itemId") != item_id
                    or current.get("leaseId") != lease_id
                    or str(current.get("state") or "") not in RTDB_AGENT_NON_TERMINAL_QUEUE_STATES
                ):
                    continue
                next_expires_at_ms = self._server_now_ms() + lease_duration_ms
                next_value = dict(current)
                next_value.update(
                    {
                        "leaseExpiresAtMs": next_expires_at_ms,
                        "leaseExpiresAt": _ms_to_iso(next_expires_at_ms),
                        "updatedAtMs": self._server_now_ms(),
                    }
                )
                self._coordination_put_json_if_match(item_path, next_value, etag, timeout_seconds=10.0)
            except Exception as e:
                logging.debug("RTDB active lease heartbeat failed for %s/%s: %s", item_id, lease_id, e)

    def _coordination_direct_empty_probe_due(self, channel: str) -> bool:
        if not self._coordination:
            return True
        now_ms = _now_ms()
        if channel == "agent":
            interval_ms = int(max(15.0, float(self._coordination["safetyPollSeconds"]["agent"])) * 1000)
            if now_ms - int(self._coordination_agent_direct_empty_probe_ms) >= interval_ms:
                self._coordination_agent_direct_empty_probe_ms = now_ms
                return True
            return False
        interval_ms = int(max(15.0, float(self._coordination["safetyPollSeconds"]["dependencies"])) * 1000)
        if now_ms - int(self._coordination_dependency_direct_empty_probe_ms) >= interval_ms:
            self._coordination_dependency_direct_empty_probe_ms = now_ms
            return True
        return False

    def _coordination_runtime_full_sync_due(self, last_full_sync_ms: int, now_ms: Optional[int] = None) -> bool:
        if int(last_full_sync_ms or 0) <= 0:
            return True
        at_ms = int(now_ms if isinstance(now_ms, int) else _now_ms())
        interval_ms = int(max(60.0, float(self.coordination_runtime_full_sync_seconds or 300.0)) * 1000)
        return at_ms - int(last_full_sync_ms) >= interval_ms

    def _runtime_payload_signature(self, payload: Dict[str, Any]) -> str:
        return _sha256_hex_bytes(_canonical_json_bytes(payload))

    def _dependency_runtime_transition_signature(self, queue_depth: Optional[int] = None) -> str:
        with self._lock:
            if isinstance(queue_depth, int):
                self._last_dependency_queue_depth = max(0, int(queue_depth))
            installed_static, installed_dynamic, failed, downloading, active_downloads = self._dependency_runtime_snapshot_locked()
            active_download_signature = [
                {
                    "depId": row.get("depId"),
                    "stage": row.get("stage"),
                    "destRelativePath": row.get("destRelativePath"),
                    "expectedBytes": row.get("expectedBytes"),
                }
                for row in active_downloads
                if isinstance(row, dict)
            ]
            payload = {
                "installedStatic": installed_static,
                "installedDynamic": installed_dynamic,
                "failed": failed,
                "downloading": downloading,
                "activeDownloads": active_download_signature,
                "dynamicBytesUsed": int(self._dynamic_bytes_used),
            }
        return self._runtime_payload_signature(payload)

    def _collect_dependency_runtime_payload(self, queue_depth: Optional[int] = None, full: bool = True) -> Dict[str, Any]:
        with self._lock:
            installed_static, installed_dynamic, failed, downloading, active_downloads = self._dependency_runtime_snapshot_locked()
            dynamic_bytes_used = int(self._dynamic_bytes_used)
            if isinstance(queue_depth, int):
                self._last_dependency_queue_depth = max(0, int(queue_depth))
            queue_depth_value = int(self._last_dependency_queue_depth)

        now_ms = _now_ms()
        manager: Dict[str, Any] = {
            "queueDepth": queue_depth_value,
            "lastHeartbeatAtMs": now_ms,
            "installedDepIdsStaticHash": _sha256_hex_bytes(_canonical_json_bytes(installed_static)),
            "installedDepIdsDynamicHash": _sha256_hex_bytes(_canonical_json_bytes(installed_dynamic)),
            "downloadingDepIdsHash": _sha256_hex_bytes(_canonical_json_bytes(downloading)),
            "failedDepIdsHash": _sha256_hex_bytes(_canonical_json_bytes(failed)),
        }
        if active_downloads:
            manager["activeDownloads"] = active_downloads
            manager["downloadingDepIds"] = downloading
        if full:
            stats = disk_stats(self.comfyui_dir)
            manager.update({
                "installedDepIdsStatic": installed_static,
                "installedDepIdsDynamic": installed_dynamic,
                "downloadingDepIds": downloading,
                "activeDownloads": active_downloads,
                "failedDepIds": failed,
                "inventoryTruncated": False,
                "dynamicBytesUsed": dynamic_bytes_used,
                "disk": {
                    "totalBytes": int(stats.get("totalBytes", 0)),
                    "freeBytes": int(stats.get("freeBytes", 0)),
                    "usedBytes": int(stats.get("usedBytes", 0)),
                    "measuredAtMs": now_ms,
                    "path": stats.get("path"),
                    "statPath": stats.get("statPath"),
                    "resolvedPath": stats.get("resolvedPath"),
                    "mount": stats.get("mount"),
                },
            })
        hot_manager: Dict[str, Any] = {
            "queueDepth": queue_depth_value,
            "lastHeartbeatAtMs": now_ms,
            "activeDownloadCount": len(active_downloads),
            "downloadingDepIdsCount": len(downloading),
            "failedDepIdsCount": len(failed),
            "installedDepIdsStaticHash": manager["installedDepIdsStaticHash"],
            "installedDepIdsDynamicHash": manager["installedDepIdsDynamicHash"],
            "downloadingDepIdsHash": manager["downloadingDepIdsHash"],
            "failedDepIdsHash": manager["failedDepIdsHash"],
            "dynamicBytesUsed": dynamic_bytes_used,
        }
        if full and "disk" in manager:
            hot_manager["disk"] = manager["disk"]
        return {
            "dependencyManager": manager,
            "hot": {
                "dependencyManager": hot_manager,
                "updatedAtMs": now_ms,
            },
            "updatedAtMs": now_ms,
        }

    def _write_dependency_runtime_mirror(self, queue_depth: Optional[int] = None) -> bool:
        now_ms = _now_ms()
        signature = self._dependency_runtime_transition_signature(queue_depth=queue_depth)
        full = (
            signature != self._last_dependency_runtime_signature
            or self._coordination_runtime_full_sync_due(self._last_dependency_runtime_full_sync_ms, now_ms)
        )
        ok = self._coordination_patch_runtime(
            self._collect_dependency_runtime_payload(queue_depth=queue_depth, full=full)
        )
        if ok:
            self._last_dependency_runtime_signature = signature
            if full:
                self._last_dependency_runtime_full_sync_ms = now_ms
        return ok

    def _disk_diagnostics_payload(self) -> Dict[str, Any]:
        paths: List[Tuple[str, Path]] = [
            ("workspace", self.workspace),
            ("comfyui", self.comfyui_dir),
            ("models", self.comfyui_dir / "models"),
            ("state", self.state_path.parent),
            ("inputCache", self.input_cache_dir),
            ("root", Path("/")),
        ]
        seen: Set[str] = set()
        entries: List[Dict[str, Any]] = []
        for label, path in paths:
            try:
                key = str(_nearest_existing_path(path).resolve())
            except Exception:
                key = str(path)
            if key in seen:
                continue
            seen.add(key)
            try:
                stats = disk_stats(path)
                entries.append({
                    "label": label,
                    "path": stats.get("path"),
                    "statPath": stats.get("statPath"),
                    "resolvedPath": stats.get("resolvedPath"),
                    "totalBytes": stats.get("totalBytes"),
                    "freeBytes": stats.get("freeBytes"),
                    "usedBytes": stats.get("usedBytes"),
                    "mount": stats.get("mount"),
                })
            except Exception as e:
                entries.append({"label": label, "path": str(path), "error": str(e)[:500]})

        deleted_open = _scan_deleted_open_files([self.workspace, self.comfyui_dir], max_examples=20)
        return {
            "paths": entries,
            "deletedOpenFiles": deleted_open,
        }

    def _gpu_coordinator_runtime_snapshot(self) -> Dict[str, Any]:
        if not self._gpu_coordinator.configured:
            return {"configured": False, "supported": False}
        try:
            status = self._gpu_coordinator.status()
            payload = {
                key: status.get(key)
                for key in (
                    "configured", "supported", "enforcing", "state", "phase", "epoch", "miningGraceSeconds",
                    "miningNotBeforeMs", "diagnosticsBusy", "draining",
                )
                if key in status
            }
            # Explicitly overwrite a prior transient coordinator error in the
            # RTDB/Firestore merge patches once the status probe succeeds.
            payload["error"] = ""
            config = status.get("config") if isinstance(status.get("config"), dict) else {}
            if isinstance(config.get("mode"), str):
                payload["mode"] = str(config["mode"])[:24]
            payload["attestedAtMs"] = _now_ms()
            try:
                gateway_status, gateway_payload = api_json(
                    "GET",
                    _env_str("DM_INFERENCE_GATEWAY_HEALTH_URL", "http://127.0.0.1:8080/health") or
                    "http://127.0.0.1:8080/health",
                    timeout_seconds=3.0,
                )
                payload["gatewayReady"] = gateway_status == 200 and gateway_payload.get("ready") is True
                payload["gatewayError"] = ""
                payload["gatewayBootId"] = str(gateway_payload.get("gatewayBootId") or "")[:64]
                coordinator_health = gateway_payload.get("coordinator") if isinstance(gateway_payload.get("coordinator"), dict) else {}
                inference_readiness = coordinator_health.get("inferenceReadiness") if isinstance(coordinator_health.get("inferenceReadiness"), dict) else {}
                payload["inferenceReady"] = payload["gatewayReady"] and inference_readiness.get("ready") is True
                if isinstance(inference_readiness.get("reason"), str):
                    payload["inferenceReadinessReason"] = str(inference_readiness["reason"])[:80]
            except Exception as exc:
                payload["gatewayReady"] = False
                payload["inferenceReady"] = False
                payload["gatewayError"] = str(exc)[:500]
            proc_root = Path("/proc")
            if proc_root.is_dir():
                agent_process_count = 0
                watchdog_process_count = 0
                for entry in proc_root.iterdir():
                    if not entry.name.isdigit():
                        continue
                    try:
                        cmdline = (entry / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
                        comm = (entry / "comm").read_text(encoding="utf-8", errors="replace").strip().lower()
                    except OSError:
                        continue
                    # The watchdog launches the agent through `flock`, whose
                    # command line also contains the Python script path. Count
                    # only the Python interpreter so the supervisor is not
                    # mistaken for a duplicate dependency-agent process.
                    if "dependency_agent_v1.py" in cmdline and comm.startswith("python"):
                        agent_process_count += 1
                    if "dependency_agent_watchdog.sh" in cmdline:
                        watchdog_process_count += 1
                payload["agentProcessCount"] = agent_process_count
                payload["watchdogProcessCount"] = watchdog_process_count
            lease = status.get("lease") if isinstance(status.get("lease"), dict) else {}
            miner_snapshot = self._idle_prl_miner.snapshot()
            miner_running = isinstance(miner_snapshot.get("pid"), int) and int(miner_snapshot["pid"]) > 0
            matching_mining_lease = (
                isinstance(lease, dict) and
                str(lease.get("holder") or "").lower() == "mining" and
                str(lease.get("state") or "").upper() in ("ACTIVE", "REGISTERING")
            )
            payload["miningLeaseInvariantOk"] = not miner_running or matching_mining_lease
            if not payload["miningLeaseInvariantOk"]:
                payload["inferenceReady"] = False
                payload["inferenceReadinessReason"] = "unmanaged_mining_process"
            payload["lease"] = {
                    key: lease.get(key)
                    for key in ("holder", "workId", "state", "deadlineMs", "warmDeadlineMs", "registrationDeadlineMs")
                    if key in lease
                } if lease else None
            metrics = status.get("metrics") if isinstance(status.get("metrics"), dict) else {}
            if metrics:
                payload["metrics"] = {
                    key: metrics.get(key)
                    for key in (
                        "leaseConflicts", "staleTokenRejections", "snapshotSaves", "snapshotSaveErrors",
                        "snapshotRestores", "snapshotRestoreErrors", "snapshotRestoreIneffective",
                        "snapshotSkippedDirtyEviction",
                        "snapshotSkippedKeySwitch", "llamaStarts", "llamaStops", "comfyEvictions",
                        "miningRevocations", "recoveryCount",
                    )
                    if key in metrics and isinstance(metrics.get(key), (int, float))
                }
            cache = status.get("cache") if isinstance(status.get("cache"), dict) else {}
            if cache:
                payload["cache"] = {
                    key: cache.get(key)
                    for key in (
                        "entries", "bytes", "quotaBytes", "minFreeBytes", "freeBytes", "ttlSeconds",
                        "bytesPerOccupiedToken",
                    )
                    if key in cache and isinstance(cache.get(key), (int, float))
                }
            llama = status.get("llama") if isinstance(status.get("llama"), dict) else {}
            if llama:
                payload["llama"] = {
                    key: llama.get(key) for key in ("pid", "running")
                    if key in llama and isinstance(llama.get(key), (bool, int))
                }
            memory = status.get("memory") if isinstance(status.get("memory"), dict) else {}
            if memory:
                payload["memory"] = {
                    key: memory.get(key) for key in ("totalBytes", "availableBytes")
                    if key in memory and isinstance(memory.get(key), (int, float))
                }
            gpu_processes = status.get("gpuProcesses") if isinstance(status.get("gpuProcesses"), list) else []
            payload["gpuProcesses"] = [
                {"pid": int(item["pid"]), "usedBytes": int(item["usedBytes"])}
                for item in gpu_processes[:32]
                if isinstance(item, dict) and isinstance(item.get("pid"), (int, float)) and
                isinstance(item.get("usedBytes"), (int, float))
            ]
            return payload
        except Exception as exc:
            return {
                "configured": True,
                "supported": None,
                "error": str(exc)[:500],
            }

    def _collect_agent_runtime_payload(self, full: bool = True, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if body is None:
            held_leases = self._collect_active_leases()
            local_comfy = True if self.mining_only else self._local_comfy_reachable()
            readiness_present = True if self.mining_only else self._local_readiness_file_present()
            node_contract = {
                "ready": True,
                "requiredClassCount": 0,
                "missingClassTypes": [],
                "affectedBundleIds": [],
                "checkedAtMs": _now_ms(),
            } if self.mining_only else self._local_node_contract_runtime(force=False)
            queue_summary = {} if self.mining_only else self._local_comfy_queue_summary(timeout_seconds=5.0)
            input_cache_inventory = self._collect_input_cache_inventory()
            stage_counts = self._agent_stage_counts_payload()
            memory_telemetry = collect_cgroup_memory_telemetry()
            comfy_runtime = {} if self.mining_only else self._comfy_runtime_snapshot()
            body = {
                "localComfyReachable": bool(local_comfy),
                "localReadinessFilePresent": bool(readiness_present),
                "localReadinessFile": self.agent_local_readiness_file,
                "nodeContract": node_contract,
                "queueDepth": int(len(held_leases)),
                **({"queueSummary": queue_summary} if queue_summary else {}),
                "stageCounts": stage_counts,
                "heldLeases": held_leases,
                "runningItemIds": [row["itemId"] for row in held_leases if isinstance(row.get("itemId"), str)],
                "maxConcurrentExecuteJobs": int(self._agent_effective_execute_capacity()),
                "maxPrefetchJobs": int(self._agent_effective_prefetch_capacity()),
                "maxUploadJobs": int(self.agent_max_upload_workers),
                "inputCacheKeys": input_cache_inventory.get("keys", []),
                "inputCacheKeyCount": int(input_cache_inventory.get("keyCount", 0)),
                "inputCacheBytesUsed": int(input_cache_inventory.get("bytesUsed", 0)),
                "inputCacheMaxBytes": int(input_cache_inventory.get("maxBytes", 0)),
                "inputCacheInventoryTruncated": bool(input_cache_inventory.get("inventoryTruncated")),
                **({"memoryTelemetry": memory_telemetry} if memory_telemetry else {}),
                **({"comfyRuntime": comfy_runtime} if comfy_runtime else {}),
                "idleMining": self._idle_prl_miner.snapshot(),
                "gpuCoordinator": self._gpu_coordinator_runtime_snapshot(),
                "agentVersion": AGENT_VERSION,
                "capabilities": {
                    "dependencyChannel": True,
                    "agentPullExecution": not self.mining_only,
                    "hybridOutputUploadsV1": True,
                    "dependencyDeleteFiles": True,
                    "idlePrlMining": True,
                    "gpuCoordinatorLeases": True,
                    "miningOnly": bool(self.mining_only),
                    "comfyRestartProcessIsolated": self._comfy_restart_process_isolated(),
                },
            }
        now_ms = _now_ms()
        input_cache_keys = sorted({
            str(value)
            for value in body.get("inputCacheKeys", [])
            if isinstance(value, str) and value
        }) if isinstance(body.get("inputCacheKeys"), list) else []
        input_cache_keys_hash = _sha256_hex_bytes(_canonical_json_bytes(input_cache_keys))
        agent_control: Dict[str, Any] = {
            "lastHeartbeatAtMs": now_ms,
            "localComfyReachable": body.get("localComfyReachable") is True,
            "localReadinessFilePresent": body.get("localReadinessFilePresent") is True,
            "nodeContract": body.get("nodeContract") if isinstance(body.get("nodeContract"), dict) else {},
            "queueDepth": int(body.get("queueDepth") or 0),
            "stageCounts": body.get("stageCounts") if isinstance(body.get("stageCounts"), dict) else {},
            "heldLeases": body.get("heldLeases") if isinstance(body.get("heldLeases"), list) else [],
            "runningItemIds": body.get("runningItemIds") if isinstance(body.get("runningItemIds"), list) else [],
            "heldLeaseCount": len(body.get("heldLeases") if isinstance(body.get("heldLeases"), list) else []),
            "maxConcurrentExecuteJobs": int(body.get("maxConcurrentExecuteJobs") or 0),
            "maxPrefetchJobs": int(body.get("maxPrefetchJobs") or 0),
            "maxUploadJobs": int(body.get("maxUploadJobs") or 0),
            "inputCacheKeysHash": input_cache_keys_hash,
        }
        memory_telemetry = body.get("memoryTelemetry")
        if isinstance(memory_telemetry, dict) and memory_telemetry:
            agent_control["memoryTelemetry"] = memory_telemetry
        comfy_runtime = body.get("comfyRuntime")
        if isinstance(comfy_runtime, dict) and comfy_runtime:
            agent_control["comfyRuntime"] = comfy_runtime
        queue_summary = body.get("queueSummary")
        if isinstance(queue_summary, dict) and queue_summary:
            agent_control["queueSummary"] = queue_summary
        gpu_coordinator = body.get("gpuCoordinator")
        if isinstance(gpu_coordinator, dict) and gpu_coordinator:
            agent_control["gpuCoordinator"] = gpu_coordinator
        if full:
            capabilities = {
                "dependencyChannel": True,
                "agentPullExecution": not self.mining_only,
                "hybridOutputUploadsV1": True,
                "dependencyDeleteFiles": True,
                "idlePrlMining": True,
                "miningOnly": bool(self.mining_only),
                "comfyRestartProcessIsolated": self._comfy_restart_process_isolated(),
            }
            body_capabilities = body.get("capabilities")
            if isinstance(body_capabilities, dict):
                capabilities.update(body_capabilities)
            agent_control.update({
                "localReadinessFile": body.get("localReadinessFile") or self.agent_local_readiness_file,
                "inputCacheKeys": body.get("inputCacheKeys", []),
                "inputCacheKeysHash": input_cache_keys_hash,
                "inputCacheKeyCount": int(body.get("inputCacheKeyCount") or 0),
                "inputCacheBytesUsed": int(body.get("inputCacheBytesUsed") or 0),
                "inputCacheMaxBytes": int(body.get("inputCacheMaxBytes") or 0),
                "inputCacheInventoryTruncated": body.get("inputCacheInventoryTruncated") is True,
                "idleMining": body.get("idleMining") if isinstance(body.get("idleMining"), dict) else {},
                "agentVersion": body.get("agentVersion") or AGENT_VERSION,
                "capabilities": capabilities,
            })
        hot_agent_control: Dict[str, Any] = {
            "lastHeartbeatAtMs": now_ms,
            "localComfyReachable": agent_control.get("localComfyReachable"),
            "localReadinessFilePresent": agent_control.get("localReadinessFilePresent"),
            "nodeContract": agent_control.get("nodeContract"),
            "queueDepth": agent_control.get("queueDepth"),
            "stageCounts": agent_control.get("stageCounts"),
            "heldLeaseCount": agent_control.get("heldLeaseCount"),
            "runningItemIds": agent_control.get("runningItemIds"),
            "maxConcurrentExecuteJobs": agent_control.get("maxConcurrentExecuteJobs"),
            "maxPrefetchJobs": agent_control.get("maxPrefetchJobs"),
            "maxUploadJobs": agent_control.get("maxUploadJobs"),
            "inputCacheKeysHash": agent_control.get("inputCacheKeysHash"),
        }
        if "memoryTelemetry" in agent_control:
            hot_agent_control["memoryTelemetry"] = agent_control.get("memoryTelemetry")
        if "comfyRuntime" in agent_control:
            hot_agent_control["comfyRuntime"] = agent_control.get("comfyRuntime")
        if "queueSummary" in agent_control:
            hot_agent_control["queueSummary"] = agent_control.get("queueSummary")
        if "gpuCoordinator" in agent_control:
            hot_agent_control["gpuCoordinator"] = agent_control.get("gpuCoordinator")
        if full:
            hot_agent_control.update({
                "localReadinessFile": agent_control.get("localReadinessFile"),
                "inputCacheKeyCount": agent_control.get("inputCacheKeyCount"),
                "inputCacheBytesUsed": agent_control.get("inputCacheBytesUsed"),
                "inputCacheMaxBytes": agent_control.get("inputCacheMaxBytes"),
                "inputCacheInventoryTruncated": agent_control.get("inputCacheInventoryTruncated"),
                "idleMining": self._compact_idle_mining_runtime_hot(agent_control.get("idleMining")),
                "agentVersion": agent_control.get("agentVersion"),
                "capabilities": agent_control.get("capabilities"),
            })
        return {
            "agentControl": agent_control,
            "hot": {
                "agentControl": hot_agent_control,
                "updatedAtMs": now_ms,
            },
            "updatedAtMs": now_ms,
        }

    def _compact_idle_mining_runtime_hot(self, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            return {}
        watch = raw.get("watch") if isinstance(raw.get("watch"), dict) else {}
        out = {
            "state": raw.get("state"),
            "desiredState": raw.get("desiredState"),
            "pid": raw.get("pid"),
            "worker": raw.get("worker"),
            "minerProcessCount": raw.get("minerProcessCount"),
            "recentAcceptedShares": raw.get("recentAcceptedShares"),
            "recentSubmittedShares": raw.get("recentSubmittedShares"),
            "recentRejectedShares": raw.get("recentRejectedShares"),
            "recentShareErrors": raw.get("recentShareErrors"),
            "autoRestartCount": raw.get("autoRestartCount"),
            "lastAutoRestartAttemptAtMs": raw.get("lastAutoRestartAttemptAtMs"),
            "lastAutoRestartReason": raw.get("lastAutoRestartReason"),
            "watch": {
                "minerAlive": watch.get("minerAlive"),
                "minerProcessCount": watch.get("minerProcessCount"),
                "gpuUtilizationPct": watch.get("gpuUtilizationPct"),
                "gpuPowerDrawW": watch.get("gpuPowerDrawW"),
                "localHashrateHps": watch.get("localHashrateHps"),
                "localHashrateText": watch.get("localHashrateText"),
                "poolConnected": watch.get("poolConnected"),
                "lastShareLikeLogAtMs": watch.get("lastShareLikeLogAtMs"),
                "lastTelemetryAtMs": watch.get("lastTelemetryAtMs"),
                "telemetryError": watch.get("telemetryError"),
            },
        }
        return _strip_none(out)

    def _write_agent_runtime_mirror(
        self,
        body: Optional[Dict[str, Any]] = None,
        transition_signature: Optional[str] = None,
        force_full: bool = False,
    ) -> bool:
        now_ms = _now_ms()
        signature = transition_signature if isinstance(transition_signature, str) else (
            self._agent_runtime_transition_signature(body) if isinstance(body, dict) else ""
        )
        full = bool(force_full or not isinstance(body, dict))
        if isinstance(body, dict):
            full = full or (
                signature != self._last_agent_runtime_signature
                or self._coordination_runtime_full_sync_due(self._last_agent_runtime_full_sync_ms, now_ms)
            )
        ok = self._coordination_patch_runtime(
            self._collect_agent_runtime_payload(full=full, body=body),
            timeout_seconds=10.0,
        )
        if ok:
            if signature:
                self._last_agent_runtime_signature = signature
            if full:
                self._last_agent_runtime_full_sync_ms = now_ms
        return ok

    def _force_idle_prl_runtime_refresh(self, reason: str) -> None:
        try:
            self._write_agent_runtime_mirror()
        except Exception as exc:
            logging.debug("Idle PRL runtime mirror refresh failed after %s: %s", reason or "unknown", exc)
        self._last_agent_http_transition_signature = ""
        self._last_agent_runtime_signature = ""
        self._last_agent_heartbeat_ms = 0
        self._request_agent_queue_poll()

    def _agent_runtime_transition_signature(self, body: Dict[str, Any]) -> str:
        stage_counts_raw = body.get("stageCounts") if isinstance(body.get("stageCounts"), dict) else {}
        stage_counts = {
            key: int(stage_counts_raw.get(key) or 0)
            for key in (
                "leased",
                "prefetching",
                "ready",
                "waiting_dependencies",
                "preparing_prompt",
                "executing",
                "uploading",
                "uploadWorkerCapacity",
                "uploadBacklog",
            )
        }
        idle_raw = body.get("idleMining") if isinstance(body.get("idleMining"), dict) else {}
        idle_signature = {
            key: idle_raw.get(key)
            for key in (
                "desired",
                "running",
                "state",
                "desiredState",
                "enabled",
                "active",
                "pid",
                "minerProcessCount",
                "poolHealthy",
                "localHashrateHps",
                "logUpdatedAtMs",
                "lastShareLikeLogAtMs",
                "lastError",
                "lastFailureCategory",
                "minerKind",
                "minerPackageType",
                "minerExecutablePath",
                "cpuMiningEnabled",
                "cpuThreadsReduce",
                "cpuThreadsPriority",
                "staticDifficulty",
                "staticDifficultySource",
                "staticDifficultyMatchedGpuName",
            )
            if key in idle_raw
        }
        input_cache_keys_raw = body.get("inputCacheKeys")
        input_cache_keys = sorted({
            str(value)
            for value in input_cache_keys_raw
            if isinstance(value, str) and value
        }) if isinstance(input_cache_keys_raw, list) else []
        payload = {
            "localComfyReachable": body.get("localComfyReachable") is True,
            "localReadinessFilePresent": body.get("localReadinessFilePresent") is True,
            "localReadinessFile": str(body.get("localReadinessFile") or ""),
            "queueDepth": int(body.get("queueDepth") or 0),
            "runningItemIds": sorted([str(v) for v in body.get("runningItemIds", []) if isinstance(v, str)]),
            "stageCounts": stage_counts,
            "inputCacheKeysHash": _sha256_hex_bytes(_canonical_json_bytes(input_cache_keys)),
            "inputCacheKeyCount": int(body.get("inputCacheKeyCount") or 0),
            "inputCacheBytesUsed": int(body.get("inputCacheBytesUsed") or 0),
            "inputCacheMaxBytes": int(body.get("inputCacheMaxBytes") or 0),
            "inputCacheInventoryTruncated": body.get("inputCacheInventoryTruncated") is True,
            "sshHostKeySha256": sorted([
                str(value) for value in body.get("sshHostKeySha256", []) if isinstance(value, str)
            ]),
            "idleMining": idle_signature,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _coordination_handle_stream_event(self, event_name: str, raw_data: str) -> None:
        event = (event_name or "").strip().lower()
        if not event or event in ("keep-alive", "keepalive"):
            return
        if event in ("cancel", "auth_revoked"):
            raise RuntimeError(f"RTDB signal stream closed by server event={event}")
        if event not in ("put", "patch"):
            return
        payload = _json_loads_or_none(raw_data)
        if not isinstance(payload, dict):
            return
        path = payload.get("path")
        if not isinstance(path, str):
            path = "/"
        if path == "/" or path.startswith("/agentQueue"):
            # Claim immediately (cheap direct-RTDB read) so pickup latency is unchanged,
            # but only nudge a heartbeat if one is already ~due. Forcing a heartbeat on
            # every signal is what amplified normal churn into the 2026-07-05 storm; the
            # heartbeat timer already POSTs promptly when held-lease/stage state changes.
            self._request_agent_queue_poll()
            min_ms = self.agent_signal_heartbeat_min_seconds * 1000.0
            if (self._server_now_ms() - int(self._last_agent_heartbeat_ms)) >= min_ms:
                self._last_agent_heartbeat_ms = 0
        if path == "/" or path.startswith("/dependencyQueue"):
            self._request_dependency_queue_poll()

    def _coordination_stream_loop(self) -> None:
        backoff_seconds = 1.0
        while not self._stop.is_set() and not self._coordination_stream_stop.is_set():
            conn: Optional[Any] = None
            try:
                if not self._coordination:
                    return
                id_token = self._ensure_coordination_id_token()
                url = self._coordination_rtdb_url(self._coordination["paths"]["signalsRoot"], id_token=id_token)
                parsed = urllib.parse.urlparse(url)
                path_with_query = parsed.path or "/"
                if parsed.query:
                    path_with_query = f"{path_with_query}?{parsed.query}"
                if parsed.scheme == "https":
                    conn = http.client.HTTPSConnection(parsed.hostname or "", parsed.port or 443, timeout=90.0)
                else:
                    conn = http.client.HTTPConnection(parsed.hostname or "", parsed.port or 80, timeout=90.0)
                conn.putrequest("GET", path_with_query)
                conn.putheader("Accept", "text/event-stream")
                conn.putheader("Cache-Control", "no-cache")
                conn.endheaders()
                resp = conn.getresponse()
                if int(resp.status) not in (200, 204):
                    raw = resp.read().decode("utf-8", errors="replace")
                    if int(resp.status) in (401, 403):
                        raise ApiError(int(resp.status), raw)
                    raise RuntimeError(f"Unexpected RTDB stream response: {resp.status} {raw}")

                logging.info("RTDB coordination signal stream connected: %s", _safe_url_for_logs(url))
                self._coordination_set_stream_health(True)
                self._request_agent_queue_poll()
                self._request_dependency_queue_poll()
                backoff_seconds = 1.0

                current_event = ""
                data_lines: List[str] = []
                while not self._stop.is_set() and not self._coordination_stream_stop.is_set():
                    raw_line = resp.fp.readline()
                    if not raw_line:
                        raise RuntimeError("RTDB signal stream ended")
                    line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                    if not line:
                        if data_lines:
                            self._coordination_handle_stream_event(current_event, "\n".join(data_lines))
                        current_event = ""
                        data_lines = []
                        continue
                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        current_event = line[6:].strip()
                        continue
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip())
                return
            except ApiError as e:
                if e.status in (401, 403):
                    self._coordination_id_token = None
                    self._coordination_id_token_expires_at_ms = 0
                    logging.warning("RTDB coordination signal stream unauthorized; refreshing token.")
                else:
                    logging.warning("RTDB coordination signal stream API error: %s", e)
            except Exception as e:
                if not self._stop.is_set() and not self._coordination_stream_stop.is_set():
                    logging.warning("RTDB coordination signal stream failed: %s", e)
            finally:
                self._coordination_set_stream_health(False)
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

            if self._stop.is_set() or self._coordination_stream_stop.is_set():
                return
            _sleep_with_jitter(backoff_seconds, jitter_ratio=0.1)
            backoff_seconds = min(30.0, backoff_seconds * 1.5)

    def _request_dependency_queue_poll(self) -> None:
        self._dependency_poll_wakeup.set()
        self._loop_wakeup.set()

    def _local_readiness_file_path(self) -> Path:
        candidate = Path(self.agent_local_readiness_file)
        if candidate.is_absolute():
            return candidate
        return self.comfyui_dir / "input" / self.agent_local_readiness_file

    def _video_gen_v2_sageattention_runtime_path(self) -> Path:
        configured = _env_str("VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH")
        return Path(configured) if configured else self.workspace / "sageattention2_runtime.json"

    def _h3_attention_backend(self) -> str:
        configured = (_env_str("FURGEN_H3_ATTENTION_BACKEND", "sageattention2") or "").strip().lower()
        if configured in {"pytorch", "pytorch_cross_attention", "pytorch-cross-attention"}:
            return "pytorch"
        if configured in {"comfy_kitchen", "comfy-kitchen", "ck", "ck_attention", "comfy_kitchen_int8"}:
            return "comfy_kitchen"
        return "sageattention2"

    def _h3_attention_cli_flag(self) -> str:
        backend = self._h3_attention_backend()
        if backend == "pytorch":
            return "--use-pytorch-cross-attention"
        if backend == "comfy_kitchen":
            return "--use-ck-attention"
        return "--use-sage-attention"

    def _h3_listener_uses_configured_attention(self, argv: Any, expected_torch: str, system: Any) -> bool:
        if not expected_torch or not isinstance(argv, list):
            return False
        if str(system.get("pytorch_version") or "").strip() != expected_torch:
            return False
        required = self._h3_attention_cli_flag()
        forbidden = {
            "--use-sage-attention",
            "--use-pytorch-cross-attention",
            "--use-ck-attention",
        } - {required}
        return required in argv and not any(flag in argv for flag in forbidden)

    def _video_gen_v2_sageattention_runtime_ready(self) -> bool:
        server_type = (self.server_type or "").strip()
        if server_type not in ("video_gen_v2", "video_gen_v3", "video_gen_v4"):
            return True
        if self._h3_attention_backend() in {"pytorch", "comfy_kitchen"}:
            try:
                expected_torch = _env_str("FURGEN_H3_PYTORCH_RUNTIME_VERSION", "2.10.0+cu129") or ""
                status, stats = self._comfy_api_json("GET", "/system_stats", timeout_seconds=5.0)
                system = stats.get("system") if status == 200 and isinstance(stats, dict) else None
                argv = system.get("argv") if isinstance(system, dict) else None
                return self._h3_listener_uses_configured_attention(argv, expected_torch, system or {})
            except Exception:
                return False
        try:
            payload = json.loads(
                self._video_gen_v2_sageattention_runtime_path().read_text(encoding="utf-8")
            )
            if not isinstance(payload, dict):
                return False
            if str(payload.get("attentionBackend") or "").strip().lower() != "sageattention2":
                return False
            if payload.get("kernelSmokePassed") is not True:
                return False

            if server_type in ("video_gen_v3", "video_gen_v4"):
                expected_torch = str(payload.get("torchVersion") or "").strip()
                configured_torch = _env_str("FURGEN_H3_PYTORCH_RUNTIME_VERSION", "2.10.0+cu129") or ""
                configured_sage = _env_str("FURGEN_H3_SAGEATTENTION_VERSION", "2.2.0") or ""
                configured_wheel_sha = _env_str("FURGEN_H3_SAGEATTENTION_WHEEL_SHA256") or ""
                if expected_torch != configured_torch:
                    return False
                if str(payload.get("version") or "").split("+", 1)[0] != configured_sage:
                    return False
                if str(payload.get("torchCudaRuntime") or "").strip() != "12.9":
                    return False
                if configured_wheel_sha and str(payload.get("wheelSha256") or "").strip() != configured_wheel_sha:
                    return False
                status, stats = self._comfy_api_json("GET", "/system_stats", timeout_seconds=5.0)
                system = stats.get("system") if status == 200 and isinstance(stats, dict) else None
                argv = system.get("argv") if isinstance(system, dict) else None
                return (
                    bool(expected_torch)
                    and isinstance(argv, list)
                    and "--use-sage-attention" in argv
                    and str(system.get("pytorch_version") or "").strip() == expected_torch
                )

            capability_text = str(payload.get("cudaCapability") or "").strip()
            capability_match = re.fullmatch(r"(\d+)\.(\d+)", capability_text)
            if capability_match is None:
                return False
            capability_major = int(capability_match.group(1))
            kernel_policy = str(payload.get("kernelPolicy") or "").strip()
            if capability_major == 12:
                expected_policy = "sm120_qk_int8_pv_fp16_triton"
                return (
                    kernel_policy == expected_policy
                    and payload.get("comfyPolicyActive") is True
                    and str(payload.get("comfyPolicy") or "").strip() == expected_policy
                )
            return kernel_policy == "upstream_auto_dispatch"
        except Exception:
            return False

    def _remove_local_readiness_file(self) -> None:
        try:
            path = self._local_readiness_file_path()
            if path.exists():
                path.unlink()
        except Exception:
            pass

    def _gpu_coordinator_ready_for_readiness(self) -> bool:
        """Require the configured fail-closed coordinator before advertising capacity."""
        if not self._gpu_coordinator.configured or not self._gpu_coordinator.required:
            return True
        try:
            status = self._gpu_coordinator.status(max_age_ms=0)
            return status.get("supported") is True and status.get("draining") is not True
        except GPUCoordinatorError:
            return False

    def _write_local_readiness_file(self) -> None:
        if not self._gpu_coordinator_ready_for_readiness():
            raise RuntimeError("Refusing to write readiness marker while the required GPU coordinator is unavailable")
        if hasattr(self, "_state"):
            contract = self._local_node_contract_runtime(force=True)
            if contract.get("ready") is not True:
                detail = ",".join(contract.get("missingClassTypes", [])[:10])
                if not detail:
                    # Inconclusive probe: name the reason instead of an empty list.
                    detail = str(contract.get("probeError") or "node contract probe inconclusive")
                raise RuntimeError(
                    "Refusing to write readiness marker with missing custom-node classes: " + detail
                )
        path = self._local_readiness_file_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"Provisioning completed at {_now_iso()}\n", encoding="utf-8")

    def _write_interrupted_comfy_restart_intent(self, item_id: str, lease_id: str = "") -> None:
        path = self._interrupted_comfy_restart_path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, Any] = {
            "schemaVersion": 2,
            "itemId": str(item_id or ""),
            "leaseId": str(lease_id or ""),
            "startedAt": _now_iso(),
        }
        temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
        fd: Optional[int] = None
        try:
            fd = os.open(str(temp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            offset = 0
            while offset < len(encoded):
                written = os.write(fd, encoded[offset:])
                if written <= 0:
                    raise OSError("short write while persisting interrupted ComfyUI restart intent")
                offset += written
            os.fsync(fd)
            os.close(fd)
            fd = None
            os.replace(str(temp_path), str(path))
            try:
                dir_fd = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _read_interrupted_comfy_restart_intent(self) -> Optional[Dict[str, Any]]:
        try:
            raw = json.loads(self._interrupted_comfy_restart_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception as exc:
            logging.warning("Failed reading interrupted ComfyUI restart intent: %s", exc)
            return None
        return raw if isinstance(raw, dict) else None

    def _clear_interrupted_comfy_restart_intent(self) -> None:
        try:
            self._interrupted_comfy_restart_path.unlink(missing_ok=True)
        except Exception as exc:
            logging.warning("Failed clearing interrupted ComfyUI restart intent: %s", exc)

    def _recover_interrupted_comfy_restart_readiness(self) -> bool:
        if self._comfy_restart_command_active.is_set():
            return False
        intent = self._read_interrupted_comfy_restart_intent()
        if intent is None:
            return False
        if not self._local_comfy_reachable(timeout_seconds=5.0):
            return False
        if not self._video_gen_v2_sageattention_runtime_ready():
            return False
        node_contract = self._local_node_contract_runtime(force=True)
        if node_contract.get("ready") is not True:
            # Same rule as _local_readiness_file_present: only a PROVEN-broken
            # contract may destroy the readiness marker. An inconclusive probe
            # means ComfyUI is unreachable or still importing nodes.
            if node_contract.get("conclusive") is True:
                self._remove_local_readiness_file()
                logging.error(
                    "Refusing orphaned readiness repair because the live node contract is incomplete: missing=%s bundles=%s",
                    ",".join(node_contract.get("missingClassTypes", [])[:10]),
                    ",".join(node_contract.get("affectedBundleIds", [])),
                )
            else:
                logging.info(
                    "Deferring orphaned readiness repair until the node contract is conclusive: %s",
                    node_contract.get("probeError") or "probe inconclusive",
                )
            return False
        self._write_local_readiness_file()
        item_id = str(intent.get("itemId") or "").strip()
        lease_id = str(intent.get("leaseId") or "").strip()
        schema_version = int(intent.get("schemaVersion") or 0) if isinstance(intent.get("schemaVersion"), (int, float)) else 0

        if schema_version >= 2 and item_id and lease_id:
            if not self._agent_access_token:
                logging.info(
                    "Recovered readiness after an interrupted ComfyUI restart; waiting for agent registration before ACK itemId=%s",
                    item_id,
                )
                return False
            try:
                ack = self._agent_ack(item_id, lease_id, "command_succeeded")
            except Exception as exc:
                logging.warning(
                    "Interrupted ComfyUI restart recovery ACK failed; retaining intent for retry itemId=%s: %s",
                    item_id,
                    exc,
                )
                return False
            accepted = ack.get("accepted") is True
            stale_terminal = ack.get("accepted") is False and str(ack.get("reason") or "") in {
                "stale_lease",
                "terminal",
                "already_terminal",
            }
            if not accepted and not stale_terminal:
                logging.warning(
                    "Interrupted ComfyUI restart recovery ACK was not accepted; retaining intent itemId=%s response=%s",
                    item_id,
                    ack,
                )
                return False
            self._clear_interrupted_comfy_restart_intent()
            self._request_agent_queue_poll()
            logging.warning(
                "Recovered interrupted ComfyUI restart; readiness restored and %s itemId=%s",
                "success ACK applied" if accepted else "terminal command reconciled",
                item_id,
            )
            try:
                self._agent_heartbeat()
            except Exception:
                pass
            return True

        # Legacy intents did not persist the lease ID, so they cannot safely ACK.
        # Restore readiness and force an HTTP queue reconciliation immediately
        # instead of waiting for the normal 15-minute RTDB safety poll.
        self._request_agent_queue_poll()
        self._clear_interrupted_comfy_restart_intent()
        logging.warning(
            "Recovered readiness marker from a legacy interrupted ComfyUI restart intent; requested immediate queue reconciliation."
        )
        return True

    def _repair_orphaned_readiness_marker(self) -> bool:
        """Restore a lost readiness marker after Comfy is safely usable.

        The managed launch and bundle-install paths remove the readiness marker
        before restarting ComfyUI. A process interruption or an out-of-band
        supervisor restart can leave that marker absent even after ComfyUI has
        recovered. Agent-pull assignment then remains blocked indefinitely
        because there is no interrupted-restart intent to trigger the narrower
        recovery path above.

        Never repair while bootstrap, restart, or maintenance work is active;
        in those states a missing marker is intentional.
        """
        if self.mining_only:
            return False
        if self._local_readiness_file_present():
            return False
        if self._comfy_restart_command_active.is_set():
            return False
        if self._interrupted_comfy_restart_path.exists():
            return False
        if self._video_gen_v2_bootstrap_gate_active():
            return False
        if not self._gpu_coordinator_ready_for_readiness():
            return False
        with self._lock:
            if self._agent_maintenance_inflight or self._active_maintenance_by_item:
                return False
        if not self._local_comfy_reachable(timeout_seconds=5.0):
            return False
        if not self._video_gen_v2_sageattention_runtime_ready():
            return False
        node_contract = self._local_node_contract_runtime(force=True)
        if node_contract.get("ready") is not True:
            # Same rule as _local_readiness_file_present: only a PROVEN-broken
            # contract may destroy the readiness marker. An inconclusive probe
            # means ComfyUI is unreachable or still importing nodes.
            if node_contract.get("conclusive") is True:
                self._remove_local_readiness_file()
                logging.error(
                    "Refusing orphaned readiness repair because the live node contract is incomplete: missing=%s bundles=%s",
                    ",".join(node_contract.get("missingClassTypes", [])[:10]),
                    ",".join(node_contract.get("affectedBundleIds", [])),
                )
            else:
                logging.info(
                    "Deferring orphaned readiness repair until the node contract is conclusive: %s",
                    node_contract.get("probeError") or "probe inconclusive",
                )
            return False

        recovered_post_job_recycle = self._post_job_comfy_recycle_active.is_set()
        self._write_local_readiness_file()
        if recovered_post_job_recycle:
            self._post_job_comfy_recycle_active.clear()
            self._last_agent_http_transition_signature = ""
            self._last_agent_runtime_signature = ""
            self._last_agent_heartbeat_ms = 0
        self._request_agent_queue_poll()
        logging.warning(
            "Repaired orphaned %s readiness marker after confirming local ComfyUI health%s.",
            (self.server_type or "unknown").strip() or "unknown",
            " and released the stale post-job recycle capacity gate" if recovered_post_job_recycle else "",
        )
        return True

    def _restore_local_readiness_file_after_pre_restart_failure(self, reason: str) -> bool:
        try:
            if not self._local_comfy_reachable(timeout_seconds=10.0):
                return False
            if not self._video_gen_v2_sageattention_runtime_ready():
                return False
            if self._local_node_contract_runtime(force=True).get("ready") is not True:
                return False
            self._write_local_readiness_file()
            logging.warning("Restored readiness marker after pre-restart failure: %s", reason)
            return True
        except Exception as exc:
            logging.warning("Failed restoring readiness marker after pre-restart failure: %s", exc)
            return False

    def _local_readiness_file_present(self) -> bool:
        try:
            if not self._local_readiness_file_path().exists():
                return False
            if not self._gpu_coordinator_ready_for_readiness():
                self._remove_local_readiness_file()
                return False
            if not self._video_gen_v2_sageattention_runtime_ready():
                return False
            node_contract = self._local_node_contract_runtime(force=False)
            if node_contract.get("ready") is not True:
                # Only destroy the readiness marker when the contract is PROVEN
                # broken. An inconclusive probe (ComfyUI unreachable or still
                # importing custom nodes) previously deleted the marker, which
                # the server reads as "agent readiness file missing" — that is
                # what turned every routine ComfyUI restart into a quarantine +
                # bundle reinstall + reboot loop (2026-08-19, instance 48062383).
                # Report not-ready either way so no job is assigned meanwhile.
                if node_contract.get("conclusive") is True:
                    self._remove_local_readiness_file()
                return False
            return True
        except Exception:
            return False

    def _process_parent_pid(self, pid: int) -> Optional[int]:
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8", errors="replace")
            close_idx = raw.rfind(")")
            if close_idx < 0:
                return None
            fields = raw[close_idx + 1:].strip().split()
            return int(fields[1]) if len(fields) > 1 else None
        except Exception:
            return None

    def _process_command_line(self, pid: int) -> str:
        try:
            raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
            return raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()
        except Exception:
            return ""

    def _comfy_restart_isolation_mode(self) -> str:
        """Describe whether a Comfy restart can kill or relaunch this agent."""
        try:
            agent_pgid = os.getpgrp()
            comfy_pids = self._find_local_comfy_processes()
            if not comfy_pids:
                return "unknown"
            for pid in comfy_pids:
                try:
                    if os.getpgid(pid) == agent_pgid:
                        return "unsafe_shared_process_group"
                except ProcessLookupError:
                    continue
            ancestors: Set[int] = set()
            ancestor_commands: List[str] = []
            cursor = os.getpid()
            for _ in range(64):
                parent = self._process_parent_pid(cursor)
                if not parent or parent <= 1 or parent == cursor:
                    break
                ancestors.add(parent)
                ancestor_commands.append(self._process_command_line(parent))
                cursor = parent
            configured_supervisor_script = str(
                Path(
                    _env_str("DM_COMFYUI_SUPERVISOR_LAUNCH_SCRIPT")
                    or "/opt/supervisor-scripts/comfyui.sh"
                )
            )
            supervisor_basename = Path(configured_supervisor_script).name
            supervisor_ancestor = any(
                command and (
                    configured_supervisor_script in command
                    or "/supervisor-scripts/comfyui.sh" in command
                    or (supervisor_basename == "comfyui.sh" and re.search(r"(?:^|\s)comfyui\.sh(?:\s|$)", command))
                )
                for command in ancestor_commands
            )
            if supervisor_ancestor or any(pid in ancestors for pid in comfy_pids):
                return "supervisor_descendant_recoverable"
            return "detached"
        except Exception as exc:
            logging.warning("Unable to verify ComfyUI restart process isolation: %s", exc)
            return "unknown"

    def _comfy_restart_process_isolated(self) -> bool:
        """Return true when restart is detached or crash-recoverable via the supervisor."""
        return self._comfy_restart_isolation_mode() in {
            "detached",
            "supervisor_descendant_recoverable",
        }

    def _normalize_local_comfy_base_url(self, raw: Optional[str]) -> Optional[str]:
        if not isinstance(raw, str):
            return None
        value = raw.strip()
        if not value:
            return None
        if "://" not in value:
            value = f"http://{value}"
        try:
            parsed = urllib.parse.urlparse(value)
        except Exception:
            return None
        scheme = (parsed.scheme or "http").lower()
        if scheme not in ("http", "https"):
            return None
        host = (parsed.hostname or "").strip()
        if not host:
            return None
        if host == "0.0.0.0":
            host = "127.0.0.1"
        port = parsed.port
        if not isinstance(port, int) or port <= 0:
            port = 8188
        return f"{scheme}://{host}:{port}"

    def _extract_local_comfy_base_url_from_logs(self) -> Optional[str]:
        # Probe a small set of likely logs first to avoid expensive scans.
        candidates = [
            self.workspace / "comfyui.log",
            self.workspace / "comfy.log",
            self.workspace / "logs" / "comfyui.log",
            self.workspace / "logs" / "comfy.log",
            self.comfyui_dir / "comfyui.log",
            self.comfyui_dir / "logs" / "comfyui.log",
            self.workspace / "dependency_agent.log",
        ]
        gui_re = re.compile(r"To see the GUI go to:\s*(https?://[^\s]+)", re.IGNORECASE)
        url_re = re.compile(r"(https?://(?:127\.0\.0\.1|0\.0\.0\.0):\d+)", re.IGNORECASE)

        for path in candidates:
            try:
                if not path.exists() or not path.is_file():
                    continue
                # Tail only last ~128KB to keep it cheap.
                with path.open("rb") as f:
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                    start = max(0, size - 131072)
                    f.seek(start, os.SEEK_SET)
                    text = f.read().decode("utf-8", errors="ignore")

                # Prefer explicit startup message.
                for line in reversed(text.splitlines()):
                    m = gui_re.search(line)
                    if m:
                        normalized = self._normalize_local_comfy_base_url(m.group(1))
                        if normalized:
                            return normalized

                # Fallback: any loopback URL mention.
                for line in reversed(text.splitlines()):
                    m = url_re.search(line)
                    if m:
                        normalized = self._normalize_local_comfy_base_url(m.group(1))
                        if normalized:
                            return normalized
            except Exception:
                continue
        return None

    def _local_comfy_base_url_candidates(self) -> List[str]:
        out: List[str] = []

        def _add(url: Optional[str]) -> None:
            normalized = self._normalize_local_comfy_base_url(url)
            if normalized and normalized not in out:
                out.append(normalized)

        # Honor explicit env var first.
        _add(self.agent_local_comfy_base_url)

        if self.local_comfy_allow_discovery:
            # Optionally pick up runtime-discovered GUI URL from local logs.
            now = _now_ms()
            if (now - int(self._last_local_comfy_discovery_ms)) >= 30_000:
                discovered = self._extract_local_comfy_base_url_from_logs()
                if discovered:
                    _add(discovered)
                self._last_local_comfy_discovery_ms = now

            # Known common local ports across templates.
            _add("http://127.0.0.1:8188")
            _add("http://127.0.0.1:18188")
        return out

    def _probe_local_comfy_base_url(self, base_url: str, timeout_seconds: float = 5.0) -> bool:
        for endpoint in ("/queue", "/system_stats"):
            try:
                status, _resp = api_json("GET", f"{base_url}{endpoint}", timeout_seconds=timeout_seconds)
                if status == 200 or status in (401, 403):
                    return True
            except Exception:
                continue
        return False

    def _resolve_local_comfy_base_url(self, force_refresh: bool = False, timeout_seconds: float = 5.0) -> str:
        if not force_refresh and self._resolved_local_comfy_base_url:
            return self._resolved_local_comfy_base_url

        for base_url in self._local_comfy_base_url_candidates():
            if self._probe_local_comfy_base_url(base_url, timeout_seconds=timeout_seconds):
                if base_url != self._resolved_local_comfy_base_url:
                    logging.info("Resolved local Comfy base URL: %s", base_url)
                self._resolved_local_comfy_base_url = base_url
                return base_url

        # If all probes fail, keep last known URL to avoid thrashing API paths.
        fallback = self._normalize_local_comfy_base_url(self._resolved_local_comfy_base_url) or self.agent_local_comfy_base_url
        self._resolved_local_comfy_base_url = fallback
        return fallback

    def _local_comfy_reachable(self, timeout_seconds: float = 5.0) -> bool:
        # Force refresh periodically so we can recover from port changes.
        resolved = self._resolve_local_comfy_base_url(force_refresh=True, timeout_seconds=timeout_seconds)
        return self._probe_local_comfy_base_url(resolved, timeout_seconds=timeout_seconds)

    def _wait_for_local_comfy_down(self, timeout_seconds: float = 30.0) -> bool:
        deadline = time.time() + max(1.0, timeout_seconds)
        while time.time() < deadline:
            if not self._local_comfy_reachable(timeout_seconds=1.0):
                return True
            time.sleep(0.5)
        return False

    def _local_comfy_queue_summary(
        self,
        timeout_seconds: float = 5.0,
        max_age_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ttl_ms = int(max_age_ms if isinstance(max_age_ms, int) and max_age_ms > 0 else self._comfy_queue_summary_ttl_ms)
        now_ms = _now_ms()
        cached = self._last_comfy_queue_summary if isinstance(self._last_comfy_queue_summary, dict) else {}
        cached_at_ms = int(self._last_comfy_queue_summary_at_ms or 0)
        if cached and cached_at_ms > 0 and (now_ms - cached_at_ms) <= ttl_ms:
            return dict(cached)

        try:
            status, resp = self._comfy_api_json("GET", "/queue", timeout_seconds=timeout_seconds)
            if status != 200 or not isinstance(resp, dict):
                raise RuntimeError(f"Unexpected /queue response: {status} {resp}")
            running = resp.get("queue_running") if isinstance(resp.get("queue_running"), list) else []
            pending = resp.get("queue_pending") if isinstance(resp.get("queue_pending"), list) else []
            summary = {
                "runningCount": int(len(running)),
                "pendingCount": int(len(pending)),
                "totalCount": int(len(running) + len(pending)),
                "checkedAtMs": int(now_ms),
                "source": "agent_heartbeat",
            }
            self._last_comfy_queue_summary = dict(summary)
            self._last_comfy_queue_summary_at_ms = now_ms
            return summary
        except Exception as exc:
            logging.debug("Local Comfy queue summary failed: %s", exc)
            summary = {
                "runningCount": 0,
                "pendingCount": 0,
                "totalCount": 0,
                "checkedAtMs": int(now_ms),
                "source": "agent_queue_unreachable",
            }
            self._last_comfy_queue_summary = dict(summary)
            self._last_comfy_queue_summary_at_ms = now_ms
            return summary

    def _comfy_runtime_snapshot(
        self,
        timeout_seconds: float = 3.0,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Snapshot the ComfyUI listener's effective argv and VRAM split.

        Answers two questions that were previously only reachable by SSHing to
        the box or reading a Vast template by hand: which launch flags the
        RUNNING process actually has (a restart rebuilds COMFYUI_ARGS, so the
        template is not authoritative), and how device VRAM divides between the
        PyTorch caching allocator and everything else. The non-PyTorch share is
        the number that matters for H3 OOMs -- it is where DynamicVRAM/aimdo
        holds model weights, and it does not appear in any torch-level metric.
        """
        ttl_ms = int(self._comfy_runtime_snapshot_ttl_ms)
        now_ms = _now_ms()
        cached = self._last_comfy_runtime_snapshot if isinstance(self._last_comfy_runtime_snapshot, dict) else {}
        cached_at_ms = int(self._last_comfy_runtime_snapshot_at_ms or 0)
        if not force_refresh and cached and cached_at_ms > 0 and (now_ms - cached_at_ms) <= ttl_ms:
            return dict(cached)

        snapshot: Dict[str, Any] = {
            "source": "comfy_system_stats_v1",
            "measuredAtMs": int(now_ms),
            # Read from the agent's own env: ComfyUI inherits it, so a mismatch
            # against the template means the restart path dropped it.
            "allocatorConf": _env_str("PYTORCH_ALLOC_CONF", "") or "",
        }
        try:
            status, stats = self._comfy_api_json("GET", "/system_stats", timeout_seconds=timeout_seconds)
            if status != 200 or not isinstance(stats, dict):
                raise RuntimeError(f"Unexpected /system_stats response: {status}")
            system = stats.get("system") if isinstance(stats.get("system"), dict) else {}
            argv = system.get("argv")
            if isinstance(argv, list):
                flags = [str(token) for token in argv[1:] if isinstance(token, (str, int, float))]
                snapshot["comfyArgs"] = " ".join(flags)[:1000]
            pytorch_version = str(system.get("pytorch_version") or "").strip()
            if pytorch_version:
                snapshot["pytorchVersion"] = pytorch_version
            for output_key, stats_key in (("ramTotalBytes", "ram_total"), ("ramFreeBytes", "ram_free")):
                value = system.get(stats_key)
                if isinstance(value, (int, float)) and value >= 0:
                    snapshot[output_key] = int(value)

            devices = stats.get("devices") if isinstance(stats.get("devices"), list) else []
            device = devices[0] if devices and isinstance(devices[0], dict) else {}
            if device:
                name = str(device.get("name") or "").strip()
                if name:
                    snapshot["deviceName"] = name[:120]
                for output_key, stats_key in (
                    ("vramTotalBytes", "vram_total"),
                    ("vramFreeBytes", "vram_free"),
                    ("torchVramTotalBytes", "torch_vram_total"),
                    ("torchVramFreeBytes", "torch_vram_free"),
                ):
                    value = device.get(stats_key)
                    if isinstance(value, (int, float)) and value >= 0:
                        snapshot[output_key] = int(value)
                # ComfyUI reports vram_free as (cuda free + torch cached-free)
                # and torch_vram_total as torch's reserved pool, so everything
                # the caching allocator does not own falls out as:
                #   total - free + torchFree - torchReserved
                required = ("vramTotalBytes", "vramFreeBytes", "torchVramTotalBytes", "torchVramFreeBytes")
                if all(key in snapshot for key in required):
                    non_torch = (
                        snapshot["vramTotalBytes"]
                        - snapshot["vramFreeBytes"]
                        + snapshot["torchVramFreeBytes"]
                        - snapshot["torchVramTotalBytes"]
                    )
                    snapshot["nonTorchVramBytes"] = int(max(0, non_torch))
            self._last_comfy_runtime_snapshot = dict(snapshot)
            self._last_comfy_runtime_snapshot_at_ms = now_ms
            return snapshot
        except Exception as exc:
            logging.debug("Comfy runtime snapshot failed: %s", exc)
            snapshot["source"] = "comfy_system_stats_unreachable"
            snapshot["error"] = str(exc)[:200]
            self._last_comfy_runtime_snapshot = dict(snapshot)
            self._last_comfy_runtime_snapshot_at_ms = now_ms
            return snapshot

    @staticmethod
    def _format_gib(value: Any) -> str:
        if not isinstance(value, (int, float)):
            return "n/a"
        return f"{float(value) / (1024 ** 3):.2f}GiB"

    def _log_comfy_runtime_snapshot(self, context: str, snapshot: Dict[str, Any]) -> None:
        if not isinstance(snapshot, dict) or not snapshot:
            return
        logging.info(
            "ComfyUI runtime (%s): args=%r allocatorConf=%r device=%s vramTotal=%s vramFree=%s "
            "torchReserved=%s torchCachedFree=%s nonTorch=%s ramFree=%s source=%s",
            context,
            snapshot.get("comfyArgs", ""),
            snapshot.get("allocatorConf", ""),
            snapshot.get("deviceName", "n/a"),
            self._format_gib(snapshot.get("vramTotalBytes")),
            self._format_gib(snapshot.get("vramFreeBytes")),
            self._format_gib(snapshot.get("torchVramTotalBytes")),
            self._format_gib(snapshot.get("torchVramFreeBytes")),
            self._format_gib(snapshot.get("nonTorchVramBytes")),
            self._format_gib(snapshot.get("ramFreeBytes")),
            snapshot.get("source", ""),
        )

    def _resolve_asset_gen_v5_script(self) -> Optional[Path]:
        candidates: List[Path] = []
        if self.asset_gen_v5_script:
            candidates.append(Path(self.asset_gen_v5_script))
        if (self.server_type or "").strip() in ("asset_gen_v5_lite", "asset_gen_v6_lite", "asset_gen_v7_lite", "foxy_all"):
            candidates.extend([
                self.workspace / f"{(self.server_type or '').strip()}.sh",
                Path(f"/workspace/{(self.server_type or '').strip()}.sh"),
                Path(f"/opt/FurgenPub/docker/support/{(self.server_type or '').strip()}.sh"),
                Path(f"/workspace/FurgenPub/docker/support/{(self.server_type or '').strip()}.sh"),
                self.workspace / "asset_gen_v5_lite.sh",
                Path("/workspace/asset_gen_v5_lite.sh"),
                Path("/opt/FurgenPub/docker/support/asset_gen_v5_lite.sh"),
                Path("/workspace/FurgenPub/docker/support/asset_gen_v5_lite.sh"),
            ])
        candidates.extend([
            self.workspace / "asset_gen_v5.sh",
            Path("/workspace/asset_gen_v5.sh"),
            Path("/opt/FurgenPub/docker/support/asset_gen_v5.sh"),
            Path("/workspace/FurgenPub/docker/support/asset_gen_v5.sh"),
        ])
        for candidate in candidates:
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                continue
        return None

    def _comfy_python_executable(self) -> str:
        """Return the Python interpreter used by the local ComfyUI runtime."""
        configured = _env_str("DM_COMFY_PYTHON_EXECUTABLE")
        candidates = [
            Path(configured) if configured else None,
            Path("/venv/main/bin/python"),
            self.comfyui_dir / ".venv" / "bin" / "python",
            self.comfyui_dir / "venv" / "bin" / "python",
        ]
        for candidate in candidates:
            if candidate is None:
                continue
            try:
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    return str(candidate)
            except OSError:
                continue
        return sys.executable

    def _install_git_custom_node(
        self,
        repo_url: str,
        verify_dir_name: Optional[str] = None,
        git_ref: Optional[str] = None,
        install_requirements: bool = True,
        pip_args: Optional[List[str]] = None,
    ) -> None:
        if not repo_url.startswith("https://github.com/"):
            raise RuntimeError(f"Unsupported custom node repository: {repo_url}")
        if git_ref and not re.match(r"^[A-Za-z0-9._/@+-]{1,128}$", git_ref):
            raise RuntimeError(f"Unsupported custom node git ref: {git_ref}")
        git = shutil.which("git")
        if not git:
            raise RuntimeError("git not found; cannot install custom node repository")
        pip_cmd = [self._comfy_python_executable(), "-m", "pip"]
        node_dir = (verify_dir_name or os.path.basename(repo_url.rstrip("/"))).removesuffix(".git")
        if not re.match(r"^[A-Za-z0-9_.-]{1,128}$", node_dir):
            raise RuntimeError(f"Unsupported custom node directory name: {node_dir}")
        target = self.comfyui_dir / "custom_nodes" / node_dir
        requirements = target / "requirements.txt"

        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            subprocess.run(
                [git, "-C", str(target), "config", "--global", "--add", "safe.directory", str(target)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
            )
            try:
                subprocess.run(
                    [git, "-C", str(target), "pull", "--ff-only"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=180,
                )
            except subprocess.CalledProcessError as exc:
                logging.warning(
                    "git pull --ff-only failed for custom node %s; resetting checkout before retry: %s",
                    node_dir,
                    str(exc)[:500],
                )
                subprocess.run(
                    [git, "-C", str(target), "fetch", "--all", "--prune"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=300,
                )
                branch_proc = subprocess.run(
                    [git, "-C", str(target), "rev-parse", "--abbrev-ref", "HEAD"],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=30,
                )
                branch = branch_proc.stdout.strip() if branch_proc.returncode == 0 else ""
                if not branch or branch == "HEAD":
                    origin_head_proc = subprocess.run(
                        [git, "-C", str(target), "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD"],
                        check=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=30,
                    )
                    origin_head = origin_head_proc.stdout.strip()
                    branch = origin_head.removeprefix("origin/") if origin_head.startswith("origin/") else ""
                reset_target = git_ref or (f"origin/{branch}" if branch else "origin/main")
                subprocess.run(
                    [git, "-C", str(target), "reset", "--hard", reset_target],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=300,
                )
                subprocess.run(
                    [git, "-C", str(target), "clean", "-fd"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=180,
                )
        else:
            subprocess.run(
                [git, "clone", repo_url, str(target), "--recursive"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
            )

        if git_ref:
            subprocess.run(
                [git, "-C", str(target), "checkout", git_ref],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
            )

        if install_requirements and requirements.exists():
            subprocess.run(
                [*pip_cmd, "install", "--no-cache-dir", *(pip_args or []), "-r", str(requirements)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600,
            )

    def _python_package_version(self, package_name: str) -> Optional[str]:
        try:
            proc = subprocess.run(
                [
                    self._comfy_python_executable(),
                    "-c",
                    "import importlib.metadata as m,sys; print(m.version(sys.argv[1]))",
                    package_name,
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
            )
            return proc.stdout.strip() or None
        except Exception:
            return None

    def _ensure_python_package_constraint(self, requirement: str, package_name: str) -> bool:
        before = self._python_package_version(package_name)
        subprocess.run(
            [self._comfy_python_executable(), "-m", "pip", "install", "--no-cache-dir", requirement],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600,
        )
        after = self._python_package_version(package_name)
        changed = before != after
        if changed:
            logging.info(
                "Adjusted Python package for node compatibility: %s %s -> %s (%s)",
                package_name,
                before or "missing",
                after or "missing",
                requirement,
            )
        return changed

    def _ensure_video_gen_v2_image_filters_opencv(self) -> bool:
        before = {
            package_name: self._python_package_version(package_name)
            for package_name in (
                "opencv-contrib-python",
                "opencv-contrib-python-headless",
                "opencv-python",
                "opencv-python-headless",
            )
        }
        opencv_packages = [
            "opencv-python",
            "opencv-python-headless",
            "opencv-contrib-python",
            "opencv-contrib-python-headless",
        ]
        for _ in range(2):
            subprocess.run(
                [self._comfy_python_executable(), "-m", "pip", "uninstall", "-y", *opencv_packages],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600,
            )
        subprocess.run(
            [
                self._comfy_python_executable(),
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "--force-reinstall",
                "--no-deps",
                "opencv-contrib-python-headless==4.10.0.84",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600,
        )
        subprocess.run(
            [
                self._comfy_python_executable(),
                "-c",
                "import cv2; from cv2.ximgproc import guidedFilter; print(cv2.__version__)",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
        after = {
            package_name: self._python_package_version(package_name)
            for package_name in (
                "opencv-contrib-python",
                "opencv-contrib-python-headless",
                "opencv-python",
                "opencv-python-headless",
            )
        }
        changed = before != after
        if changed:
            logging.info("Adjusted OpenCV packages for video_gen_v2 Image-Filters bundle: %s -> %s", before, after)
        return changed

    def _ensure_comfyui_vfi_rife_model(self) -> bool:
        vfi_dir = self.comfyui_dir / "custom_nodes" / "ComfyUI-VFI"
        rife_dir = vfi_dir / "rife"
        target_dir = vfi_dir / "rife" / "train_log"
        target = target_dir / "flownet.pkl"

        try:
            if target.exists() and int(target.stat().st_size) == RIFE_VFI_FLOWNET_SIZE_BYTES:
                return False
        except Exception:
            pass

        if not rife_dir.exists():
            raise RuntimeError(f"ComfyUI-VFI RIFE directory not found: {rife_dir}")

        target_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = rife_dir / "_furgen_rife_download"
        zip_path = temp_dir / "RIFEv4.26_0921.zip"
        temp_target = target.with_name(f".{target.name}.tmp-{uuid.uuid4().hex}")
        temp_dir.mkdir(parents=True, exist_ok=True)

        download_errors: List[str] = []
        try:
            for url in RIFE_VFI_ZIP_URLS:
                try:
                    logging.info("Downloading ComfyUI-VFI RIFE model archive: %s", _safe_url_for_logs(url))
                    tool = download_file_with_tool_fallback(
                        url,
                        zip_path,
                        timeout_seconds=max(60.0, min(float(self.download_timeout_seconds), 180.0)),
                        chunk_size=int(self.download_chunk_size),
                        user_agent=f"{AGENT_VERSION} rife-vfi-model",
                        tools=["curl", "aria2", "wget", "python"],
                    )
                    actual_zip_size = int(zip_path.stat().st_size) if zip_path.exists() else 0
                    if actual_zip_size != RIFE_VFI_ZIP_SIZE_BYTES:
                        raise RuntimeError(
                            f"RIFE model archive size mismatch: expected {RIFE_VFI_ZIP_SIZE_BYTES}, got {actual_zip_size}"
                        )
                    actual_zip_sha = sha256_file(zip_path).lower()
                    if actual_zip_sha != RIFE_VFI_ZIP_SHA256:
                        raise RuntimeError(
                            f"RIFE model archive checksum mismatch: expected {RIFE_VFI_ZIP_SHA256}, got {actual_zip_sha}"
                        )

                    with zipfile.ZipFile(zip_path, "r") as archive:
                        candidates = [
                            name for name in archive.namelist()
                            if name.rstrip("/").split("/")[-1] == "flownet.pkl"
                        ]
                        if not candidates:
                            raise RuntimeError("RIFE model archive does not contain flownet.pkl")
                        member = candidates[0]
                        info = archive.getinfo(member)
                        if int(info.file_size) != RIFE_VFI_FLOWNET_SIZE_BYTES:
                            raise RuntimeError(
                                f"flownet.pkl size mismatch in archive: expected {RIFE_VFI_FLOWNET_SIZE_BYTES}, got {int(info.file_size)}"
                            )
                        with archive.open(member, "r") as src, temp_target.open("wb") as out:
                            shutil.copyfileobj(src, out, length=1024 * 1024)

                    actual_model_size = int(temp_target.stat().st_size) if temp_target.exists() else 0
                    if actual_model_size != RIFE_VFI_FLOWNET_SIZE_BYTES:
                        raise RuntimeError(
                            f"extracted flownet.pkl size mismatch: expected {RIFE_VFI_FLOWNET_SIZE_BYTES}, got {actual_model_size}"
                        )
                    os.replace(str(temp_target), str(target))
                    logging.info(
                        "Installed ComfyUI-VFI RIFE model via %s: %s (%d bytes)",
                        tool,
                        target,
                        actual_model_size,
                    )
                    return True
                except Exception as exc:
                    download_errors.append(f"{_safe_url_for_logs(url)}: {exc}")
                    logging.warning("ComfyUI-VFI RIFE model download failed from %s: %s", _safe_url_for_logs(url), exc)
                    try:
                        if zip_path.exists():
                            zip_path.unlink()
                    except Exception:
                        pass
                    try:
                        if temp_target.exists():
                            temp_target.unlink()
                    except Exception:
                        pass
            raise RuntimeError(f"ComfyUI-VFI RIFE model download failed from all sources: {'; '.join(download_errors)}")
        finally:
            try:
                if temp_target.exists():
                    temp_target.unlink()
            except Exception:
                pass
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _safe_node_bundle_id(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not re.match(r"^[A-Za-z0-9_.-]{1,128}$", candidate):
            return None
        return candidate

    def _safe_support_script_name(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not re.match(r"^[A-Za-z0-9_.-]{1,128}\.sh$", candidate):
            return None
        return candidate

    def _safe_python_package_specs(self, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        out: List[str] = []
        for entry in value:
            if not isinstance(entry, str):
                continue
            package = entry.strip()
            if (
                package
                and len(package) <= 256
                and not package.startswith("-")
                and re.match(r"^[A-Za-z0-9._~:/?#[\]@!$&'()*+,;=%<>=-]+$", package)
            ):
                out.append(package)
        return out

    def _safe_python_module_name(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,8}$", candidate):
            return None
        return candidate

    def _safe_python_identifiers(self, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        out: List[str] = []
        for entry in value:
            if isinstance(entry, str) and re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$", entry.strip()):
                name = entry.strip()
                if name not in out:
                    out.append(name)
        return out

    def _resolve_support_script(self, script_name: str) -> Optional[Path]:
        safe_name = self._safe_support_script_name(script_name)
        if not safe_name:
            return None
        if safe_name in ("asset_gen_v5.sh", "asset_gen_v5_lite.sh"):
            family_script = self._resolve_asset_gen_v5_script()
            if family_script is not None:
                return family_script
        candidates = [
            self.workspace / safe_name,
            Path(f"/workspace/{safe_name}"),
            self.workspace / "FurgenPub" / "docker" / "support" / safe_name,
            Path("/opt/FurgenPub/docker/support") / safe_name,
            Path("/workspace/FurgenPub/docker/support") / safe_name,
        ]
        for candidate in candidates:
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                continue
        return None

    def _schema_v2_steps(self, spec: Dict[str, Any], key: str = "steps") -> List[Dict[str, Any]]:
        if not isinstance(spec, dict) or spec.get("schemaVersion") != 2:
            return []
        raw = spec.get(key)
        if isinstance(raw, dict) and key == "compatibility":
            raw = raw.get("steps")
        if not isinstance(raw, list):
            return []
        return [step for step in raw if isinstance(step, dict)]

    def _execute_python_import_check_step(self, step: Dict[str, Any]) -> None:
        modules = [
            module for module in (
                self._safe_python_module_name(value)
                for value in (step.get("modules") if isinstance(step.get("modules"), list) else [])
            )
            if module
        ]
        from_imports: List[Dict[str, Any]] = []
        raw_from_imports = step.get("fromImports")
        if isinstance(raw_from_imports, list):
            for entry in raw_from_imports:
                if not isinstance(entry, dict):
                    continue
                module = self._safe_python_module_name(entry.get("module"))
                names = self._safe_python_identifiers(entry.get("names"))
                if module and names:
                    from_imports.append({"module": module, "names": names})
        if not modules and not from_imports:
            raise RuntimeError("python_import_check step has no valid imports")
        script = (
            "import importlib\n"
            f"modules = {json.dumps(modules)}\n"
            f"from_imports = {json.dumps(from_imports)}\n"
            "for module in modules:\n"
            "    importlib.import_module(module)\n"
            "for item in from_imports:\n"
            "    mod = importlib.import_module(item['module'])\n"
            "    for name in item['names']:\n"
            "        getattr(mod, name)\n"
        )
        subprocess.run(
            [self._comfy_python_executable(), "-c", script],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )

    def _execute_node_bundle_install_step(
        self,
        bundle_id: str,
        step: Dict[str, Any],
        required_class_types: Optional[Iterable[str]] = None,
    ) -> bool:
        step_type = step.get("type")
        if step_type in ("git_custom_node", "git_custom_nodes", "comfy_core_update"):
            if not self._install_node_bundle_from_spec(bundle_id, step, required_class_types=required_class_types):
                raise RuntimeError(f"Unsupported nested install step for {bundle_id}: {step_type}")
            return True
        if step_type == "support_script_bundle":
            script_name = self._safe_support_script_name(step.get("scriptName"))
            if not script_name:
                raise RuntimeError(f"Bundle {bundle_id} support_script_bundle step is missing a safe scriptName")
            raw_bundle_ids = step.get("bundleIds")
            step_bundle_ids = [
                safe for safe in (
                    self._safe_node_bundle_id(value)
                    for value in (raw_bundle_ids if isinstance(raw_bundle_ids, list) else [bundle_id])
                )
                if safe
            ]
            if not step_bundle_ids:
                raise RuntimeError(f"Bundle {bundle_id} support_script_bundle step has no valid bundleIds")
            script_path = self._resolve_support_script(script_name)
            if script_path is None:
                raise RuntimeError(f"Unable to locate support script {script_name} for bundle {bundle_id}")
            timeout = step.get("timeoutSeconds")
            timeout_seconds = int(timeout) if isinstance(timeout, (int, float)) and timeout > 0 else max(1800, 300 * len(step_bundle_ids))
            subprocess.run(
                ["bash", str(script_path), "install-bundles", *step_bundle_ids],
                cwd=str(self.workspace),
                env=os.environ.copy(),
                check=True,
                timeout=min(max(timeout_seconds, 60), 7200),
            )
            return True
        if step_type == "furgen_support_custom_node":
            package_name = step.get("packageName")
            raw_base_value = step.get("furgenPubRawBaseUrl")
            pinned_raw_base = self._pinned_furgenpub_support_base_url(raw_base_value)
            if raw_base_value is not None and pinned_raw_base is None:
                raise RuntimeError(
                    f"Bundle {bundle_id} furgen_support_custom_node step has an invalid pinned FurgenPub support URL"
                )
            required = self._normalize_required_class_types(step.get("requiredClassTypes"))
            if not required:
                required = self._normalize_required_class_types(required_class_types)
            if package_name == "FurgenVideoTools":
                self._install_furgen_video_tools_node(
                    required_class_types=required,
                    furgenpub_raw_base_url=pinned_raw_base,
                )
                return True
            if package_name in ("furgen_video_compat_nodes.py", "FurgenVideoCompatNodes"):
                self._install_furgen_video_compat_nodes(furgenpub_raw_base_url=pinned_raw_base)
                return True
            raise RuntimeError(f"Unsupported Furgen support custom node package for {bundle_id}: {package_name}")
        if step_type == "pip_install":
            packages = self._safe_python_package_specs(step.get("packages"))
            if not packages:
                raise RuntimeError(f"Bundle {bundle_id} pip_install step has no valid packages")
            command = [self._comfy_python_executable(), "-m", "pip", "install", "--no-cache-dir"]
            extra_index_urls = [
                value.strip()
                for value in (step.get("extraIndexUrls") if isinstance(step.get("extraIndexUrls"), list) else [])
                if isinstance(value, str) and re.fullmatch(r"https://[A-Za-z0-9.-]+(?::[0-9]+)?(?:/[A-Za-z0-9._~:/?#[\]@!$&'()*+,;=%-]*)?", value.strip())
            ][:4]
            for extra_index_url in extra_index_urls:
                command.extend(["--extra-index-url", extra_index_url])
            if step.get("forceReinstall") is True:
                command.append("--force-reinstall")
            if step.get("noDeps") is True:
                command.append("--no-deps")
            command.extend(packages)
            subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=900)
            return True
        if step_type == "pip_uninstall":
            packages = self._safe_python_package_specs(step.get("packages"))
            if not packages:
                raise RuntimeError(f"Bundle {bundle_id} pip_uninstall step has no valid packages")
            result = subprocess.run(
                [self._comfy_python_executable(), "-m", "pip", "uninstall", "-y", *packages],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600,
            )
            if result.returncode != 0:
                stderr = result.stderr.decode("utf-8", errors="replace") if isinstance(result.stderr, bytes) else str(result.stderr or "")
                logging.warning(
                    "Best-effort pip uninstall failed for node bundle %s (exit=%d); continuing to the pinned install and import verification: %s",
                    bundle_id,
                    int(result.returncode),
                    stderr.strip()[-1000:] or "no stderr",
                )
            return True
        if step_type == "python_import_check":
            self._execute_python_import_check_step(step)
            return False
        raise RuntimeError(f"Unsupported install step for {bundle_id}: {step_type}")

    def _run_node_bundle_compatibility_steps(
        self,
        bundle_ids: List[str],
        bundle_specs: Optional[Dict[str, Any]] = None,
        required_class_types: Optional[Iterable[str]] = None,
    ) -> bool:
        changed = False
        specs = bundle_specs if isinstance(bundle_specs, dict) else {}
        for bundle_id in bundle_ids:
            spec = specs.get(bundle_id)
            if not isinstance(spec, dict):
                continue
            for step in self._schema_v2_steps(spec, "compatibility"):
                logging.info("Running node bundle compatibility step installSource=spec bundleId=%s type=%s", bundle_id, step.get("type"))
                changed = self._execute_node_bundle_install_step(
                    bundle_id,
                    step,
                    required_class_types=required_class_types,
                ) or changed
        return changed

    def _ensure_node_bundle_runtime_compatibility(
        self,
        bundle_ids: List[str],
        bundle_specs: Optional[Dict[str, Any]] = None,
        required_class_types: Optional[Iterable[str]] = None,
    ) -> bool:
        changed = False
        specs = bundle_specs if isinstance(bundle_specs, dict) else {}
        spec_compat_bundle_ids = {
            bundle_id
            for bundle_id in bundle_ids
            if isinstance(specs.get(bundle_id), dict) and self._schema_v2_steps(specs[bundle_id], "compatibility")
        }
        changed = self._run_node_bundle_compatibility_steps(
            bundle_ids,
            bundle_specs=specs,
            required_class_types=required_class_types,
        ) or changed
        bundle_id_set = set(bundle_ids)
        if "video_gen_v2_10s_ltx_nodes" in bundle_id_set and "video_gen_v2_10s_ltx_nodes" not in spec_compat_bundle_ids:
            self._install_furgen_video_compat_nodes()
            changed = True
            # ComfyUI-LTXVideo currently imports `pad` from kornia's pyramid module.
            # kornia 0.8 removed that symbol, so a live Comfy process can appear ready
            # while the next restart will fail to import the node pack. Enforce the pin
            # immediately before any bundle readiness marker is written.
            changed = self._ensure_python_package_constraint("kornia<0.8", "kornia") or changed
        if "video_gen_v2_image_filters_nodes" in bundle_id_set and "video_gen_v2_image_filters_nodes" not in spec_compat_bundle_ids:
            changed = self._ensure_video_gen_v2_image_filters_opencv() or changed
        return changed

    def _install_node_bundle_from_spec(
        self,
        bundle_id: str,
        spec: Dict[str, Any],
        required_class_types: Optional[Iterable[str]] = None,
    ) -> bool:
        if not isinstance(spec, dict):
            return False
        if spec.get("schemaVersion") == 2:
            steps = self._schema_v2_steps(spec)
            if not steps:
                raise RuntimeError(f"Bundle {bundle_id} schemaVersion=2 install spec has no steps")
            for step in steps:
                logging.info("Installing node bundle step installSource=spec bundleId=%s type=%s", bundle_id, step.get("type"))
                self._execute_node_bundle_install_step(
                    bundle_id,
                    step,
                    required_class_types=required_class_types,
                )
            return True
        spec_type = spec.get("type")
        if spec_type == "comfy_core_update":
            git_ref = spec.get("ref")
            install_requirements = spec.get("installRequirements")
            force_reset = spec.get("forceReset")
            self._update_comfyui_core(
                git_ref=git_ref if isinstance(git_ref, str) and git_ref else None,
                install_requirements=install_requirements if isinstance(install_requirements, bool) else True,
                force_reset=force_reset if isinstance(force_reset, bool) else False,
            )
            return True
        if spec_type == "git_custom_node":
            repository = spec.get("repository")
            if not isinstance(repository, str) or not repository:
                raise RuntimeError(f"Bundle {bundle_id} git_custom_node install spec is missing repository")
            directory_name = spec.get("directoryName")
            git_ref = spec.get("ref")
            install_requirements = spec.get("installRequirements")
            pip_args = spec.get("pipArgs")
            self._install_git_custom_node(
                repository,
                verify_dir_name=directory_name if isinstance(directory_name, str) and directory_name else None,
                git_ref=git_ref if isinstance(git_ref, str) and git_ref else None,
                install_requirements=install_requirements if isinstance(install_requirements, bool) else True,
                pip_args=[arg for arg in pip_args if isinstance(arg, str)] if isinstance(pip_args, list) else None,
            )
            return True
        if spec_type == "furgen_asset_gen_runtime_helpers":
            self._install_furgen_asset_gen_runtime_helpers()
            return True
        if spec_type in ("furgen_video_tools", "furgen_video_tools_v2"):
            install_required_class_types = required_class_types
            if install_required_class_types is None:
                if bundle_id in ("video_gen_v2_furgen_color_nodes", "video_gen_v2_furgen_color_nodes_v2"):
                    install_required_class_types = self._video_gen_v2_bundle_verify_class_types(bundle_id)
                elif spec_type == "furgen_video_tools_v2":
                    install_required_class_types = self._video_gen_v2_bundle_verify_class_types(
                        "video_gen_v2_furgen_color_nodes_v2"
                    )
                elif spec_type == "furgen_video_tools":
                    install_required_class_types = self._video_gen_v2_bundle_verify_class_types(
                        "video_gen_v2_furgen_color_nodes"
                    )
            self._install_furgen_video_tools_node(required_class_types=install_required_class_types)
            return True
        if spec_type == "git_custom_nodes":
            repositories = spec.get("repositories")
            if not isinstance(repositories, list) or not repositories:
                raise RuntimeError(f"Bundle {bundle_id} git_custom_nodes install spec is missing repositories")
            for index, entry in enumerate(repositories):
                if not isinstance(entry, dict):
                    raise RuntimeError(f"Bundle {bundle_id} repository spec #{index + 1} is invalid")
                if not self._install_node_bundle_from_spec(f"{bundle_id}[{index}]", entry):
                    raise RuntimeError(f"Bundle {bundle_id} repository spec #{index + 1} has unsupported type")
            return True
        return False

    def _update_comfyui_core(
        self,
        git_ref: Optional[str] = None,
        install_requirements: bool = True,
        force_reset: bool = False,
    ) -> None:
        if git_ref and not re.match(r"^[A-Za-z0-9._/@+-]{1,128}$", git_ref):
            raise RuntimeError(f"Unsupported ComfyUI git ref: {git_ref}")
        git = shutil.which("git")
        if not git:
            raise RuntimeError("git not found; cannot update ComfyUI core")
        if not (self.comfyui_dir / ".git").exists():
            raise RuntimeError(f"ComfyUI directory is not a git checkout: {self.comfyui_dir}")

        subprocess.run(
            [git, "-C", str(self.comfyui_dir), "config", "--global", "--add", "safe.directory", str(self.comfyui_dir)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        if force_reset:
            subprocess.run(
                [git, "-C", str(self.comfyui_dir), "fetch", "--all", "--prune"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
            )
            current_branch = subprocess.run(
                [git, "-C", str(self.comfyui_dir), "rev-parse", "--abbrev-ref", "HEAD"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
            ).stdout.strip()
            reset_target = git_ref or (f"origin/{current_branch}" if current_branch and current_branch != "HEAD" else "origin/master")
            subprocess.run(
                [git, "-C", str(self.comfyui_dir), "reset", "--hard", reset_target],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
            )
        else:
            subprocess.run(
                [git, "-C", str(self.comfyui_dir), "pull", "--ff-only"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
            )
        if git_ref:
            subprocess.run(
                [git, "-C", str(self.comfyui_dir), "checkout", git_ref],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
            )

        if install_requirements:
            for requirements_name in ("requirements.txt", "manager_requirements.txt"):
                requirements = self.comfyui_dir / requirements_name
                if requirements.exists():
                    subprocess.run(
                        [self._comfy_python_executable(), "-m", "pip", "install", "--no-cache-dir", "-r", str(requirements)],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=900,
                    )

    def _install_furgen_video_compat_nodes(self, furgenpub_raw_base_url: Optional[str] = None) -> None:
        custom_nodes_dir = self.comfyui_dir / "custom_nodes"
        custom_nodes_dir.mkdir(parents=True, exist_ok=True)
        compat_path = custom_nodes_dir / "furgen_video_compat_nodes.py"

        candidates = [
            self.self_script_path.parent.parent / "support" / "custom_nodes" / "furgen_video_compat_nodes.py",
            self.self_script_path.parent / "docker" / "support" / "custom_nodes" / "furgen_video_compat_nodes.py",
            self.workspace / "FurgenPub" / "docker" / "support" / "custom_nodes" / "furgen_video_compat_nodes.py",
            Path("/opt/FurgenPub/docker/support/custom_nodes/furgen_video_compat_nodes.py"),
            Path("/workspace/FurgenPub/docker/support/custom_nodes/furgen_video_compat_nodes.py"),
        ]
        for src_path in candidates:
            try:
                if src_path.exists():
                    shutil.copy2(src_path, compat_path)
                    subprocess.run([self._comfy_python_executable(), "-m", "py_compile", str(compat_path)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
                    logging.info("Installed Furgen video compat nodes from local source: %s", src_path)
                    return
            except Exception as exc:
                logging.warning("Failed local Furgen video compat node install from %s: %s", src_path, str(exc)[:500])

        remote_url = (
            f"{furgenpub_raw_base_url or self.furgenpub_raw_base_url}"
            "/custom_nodes/furgen_video_compat_nodes.py"
        )
        request = urllib.request.Request(remote_url, headers={"User-Agent": "furgen-dependency-agent/1.0"})
        with urllib.request.urlopen(request, timeout=60.0) as resp:
            compat_path.write_bytes(resp.read())
        subprocess.run([self._comfy_python_executable(), "-m", "py_compile", str(compat_path)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        logging.info("Installed Furgen video compat nodes from raw source: %s", remote_url)
    def _furgen_video_tools_source_candidates(self) -> List[Path]:
        candidates: List[Path] = []
        script_path = self.self_script_path
        for base in (
            script_path.parent.parent / "support",
            script_path.parent / "docker" / "support",
            self.workspace / "FurgenPub" / "docker" / "support",
            Path("/opt/FurgenPub/docker/support"),
            Path("/workspace/FurgenPub/docker/support"),
        ):
            candidates.append(base / "custom_nodes" / "FurgenVideoTools")
        deduped: List[Path] = []
        seen: Set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if key not in seen:
                seen.add(key)
                deduped.append(candidate)
        return deduped

    def _normalize_required_class_types(self, required_class_types: Optional[Iterable[str]]) -> List[str]:
        out: List[str] = []
        for class_type in required_class_types or []:
            if isinstance(class_type, str) and class_type and class_type not in out:
                out.append(class_type)
        return out

    def _furgen_video_tools_missing_class_types(
        self,
        src_dir: Path,
        required_class_types: Optional[Iterable[str]],
    ) -> List[str]:
        required = self._normalize_required_class_types(required_class_types)
        if not required:
            return []
        impl_path = src_dir / "furgen_video_tools.py"
        try:
            impl = impl_path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(impl, filename=str(impl_path))
        except Exception as exc:
            logging.warning("Unable to inspect FurgenVideoTools source %s: %s", impl_path, exc)
            return required

        class_names = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
        mapping_names: Set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(target, ast.Name) and target.id == "NODE_CLASS_MAPPINGS" for target in node.targets):
                continue
            if not isinstance(node.value, ast.Dict):
                continue
            for key in node.value.keys:
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    mapping_names.add(key.value)
        return [class_type for class_type in required if class_type not in class_names or class_type not in mapping_names]

    def _furgen_video_tools_source_is_usable(
        self,
        src_dir: Path,
        required_class_types: Optional[Iterable[str]] = None,
        log_skip: bool = False,
    ) -> bool:
        init_path = src_dir / "__init__.py"
        impl_path = src_dir / "furgen_video_tools.py"
        if not init_path.exists() or not impl_path.exists():
            return False
        try:
            init_source = init_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return False
        if "furgen_sageattention_policy" in init_source and not (src_dir / "furgen_sageattention_policy.py").exists():
            if log_skip:
                logging.warning("Skipping incomplete FurgenVideoTools source missing SageAttention policy module: %s", src_dir)
            return False
        required = self._normalize_required_class_types(required_class_types)
        if not required:
            return True
        missing = self._furgen_video_tools_missing_class_types(src_dir, required)
        if missing:
            if log_skip:
                preview = ", ".join(missing[:12])
                suffix = "" if len(missing) <= 12 else f", +{len(missing) - 12} more"
                logging.warning(
                    "Skipping FurgenVideoTools source missing required class types (%s%s): %s",
                    preview,
                    suffix,
                    src_dir,
                )
            return False
        return True

    def _furgen_video_tools_required_class_types_for_install(
        self,
        bundle_ids: Iterable[str],
        verify_class_types: Iterable[str],
    ) -> List[str]:
        known: List[str] = []
        for bundle_id in bundle_ids:
            if bundle_id not in ("video_gen_v2_furgen_color_nodes", "video_gen_v2_furgen_color_nodes_v2", "video_gen_v2_fcs_concat_videos"):
                continue
            for class_type in self._video_gen_v2_bundle_verify_class_types(bundle_id):
                if class_type not in known:
                    known.append(class_type)
        if not known:
            return []

        verify_set = set(self._normalize_required_class_types(verify_class_types))
        if verify_set:
            filtered = [class_type for class_type in known if class_type in verify_set]
            if filtered:
                return filtered
        return known

    def _install_furgen_video_tools_node(
        self,
        required_class_types: Optional[Iterable[str]] = None,
        furgenpub_raw_base_url: Optional[str] = None,
    ) -> None:
        required = self._normalize_required_class_types(required_class_types)
        custom_nodes_dir = self.comfyui_dir / "custom_nodes"
        dest_dir = custom_nodes_dir / "FurgenVideoTools"
        custom_nodes_dir.mkdir(parents=True, exist_ok=True)

        # A signed bundle's commit-pinned URL is authoritative. Falling back to
        # a usable but stale image-local copy would make the recorded install
        # signature lie about the bytes actually installed.
        source_candidates = [] if furgenpub_raw_base_url else self._furgen_video_tools_source_candidates()
        for src_dir in source_candidates:
            try:
                if self._furgen_video_tools_source_is_usable(src_dir, required_class_types=required, log_skip=True):
                    temp_dir = dest_dir.with_name(f".{dest_dir.name}.tmp-{uuid.uuid4().hex}")
                    if temp_dir.exists():
                        shutil.rmtree(temp_dir)
                    shutil.copytree(src_dir, temp_dir, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
                    if dest_dir.exists():
                        shutil.rmtree(dest_dir)
                    os.replace(str(temp_dir), str(dest_dir))
                    logging.info("Installed managed custom node FurgenVideoTools from local source: %s", src_dir)
                    return
            except Exception as exc:
                logging.warning("Failed local FurgenVideoTools install from %s: %s", src_dir, str(exc)[:500])

        remote_base = (
            f"{furgenpub_raw_base_url or self.furgenpub_raw_base_url}"
            "/custom_nodes/FurgenVideoTools"
        )
        temp_dir = dest_dir.with_name(f".{dest_dir.name}.tmp-{uuid.uuid4().hex}")
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            for filename in ("__init__.py", "furgen_video_tools.py"):
                url = f"{remote_base}/{filename}"
                request = urllib.request.Request(url, headers={"User-Agent": "furgen-dependency-agent/1.0"})
                with urllib.request.urlopen(request, timeout=60.0) as resp:
                    data = resp.read()
                (temp_dir / filename).write_bytes(data)
            init_source = (temp_dir / "__init__.py").read_text(encoding="utf-8", errors="replace")
            if "furgen_sageattention_policy" in init_source:
                policy_filename = "furgen_sageattention_policy.py"
                policy_url = f"{remote_base}/{policy_filename}"
                policy_request = urllib.request.Request(
                    policy_url,
                    headers={"User-Agent": "furgen-dependency-agent/1.0"},
                )
                with urllib.request.urlopen(policy_request, timeout=60.0) as resp:
                    (temp_dir / policy_filename).write_bytes(resp.read())
            if required and not self._furgen_video_tools_source_is_usable(temp_dir, required_class_types=required):
                raise RuntimeError(
                    "Downloaded FurgenVideoTools source is missing required class types "
                    f"for install: {', '.join(required)} ({remote_base})"
                )
            subprocess.run(
                [self._comfy_python_executable(), "-m", "py_compile", str(temp_dir / "__init__.py"), str(temp_dir / "furgen_video_tools.py")],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60,
            )
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            os.replace(str(temp_dir), str(dest_dir))
            logging.info("Installed managed custom node FurgenVideoTools from raw source: %s", remote_base)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    def _install_furgen_asset_gen_runtime_helpers(self) -> None:
        self._install_git_custom_node(
            "https://github.com/Dodzilla/easy-comfy-nodes-async",
            verify_dir_name="easy-comfy-nodes-async",
            git_ref="a7d58d21de8a47fc42537c204650f9c03066f22a",
            install_requirements=True,
        )
        self._install_furgen_video_compat_nodes()

    def _install_video_gen_v2_node_bundle(
        self,
        bundle_id: str,
        required_class_types: Optional[Iterable[str]] = None,
    ) -> None:
        if bundle_id == "video_gen_v2_10s_ltx_nodes":
            self._install_git_custom_node(
                "https://github.com/kijai/ComfyUI-KJNodes",
                verify_dir_name="ComfyUI-KJNodes",
                git_ref="bc8e4ce4254bcd0050383386ee2f9d753dbf1fa5",
                install_requirements=True,
            )
            self._install_git_custom_node(
                "https://github.com/Lightricks/ComfyUI-LTXVideo",
                verify_dir_name="ComfyUI-LTXVideo",
                install_requirements=True,
                pip_args=["kornia<0.8"],
            )
            self._install_git_custom_node(
                "https://github.com/TenStrip/10S-Comfy-nodes",
                verify_dir_name="10S_Nodes",
            )
            self._install_git_custom_node(
                "https://github.com/evanspearman/ComfyMath",
                verify_dir_name="ComfyMath",
            )
            self._install_git_custom_node(
                "https://github.com/GACLove/ComfyUI-VFI",
                verify_dir_name="ComfyUI-VFI",
                install_requirements=False,
            )
            self._install_git_custom_node(
                "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite",
                verify_dir_name="ComfyUI-VideoHelperSuite",
                git_ref="08e8df15db24da292d4b7f943c460dc2ab442b24",
                install_requirements=True,
            )
            self._install_furgen_video_compat_nodes()
            return
        if bundle_id == "video_gen_v2_ltx_context_windows":
            self._update_comfyui_core(
                git_ref="cd77c551d6c7efa46a8ba514fd6f4e04aac76b4d",
                install_requirements=True,
                force_reset=True,
            )
            return
        if bundle_id == "video_gen_v2_furgen_color_nodes":
            self._install_furgen_video_tools_node(
                required_class_types=required_class_types or self._video_gen_v2_bundle_verify_class_types(bundle_id)
            )
            return
        if bundle_id in ("video_gen_v2_furgen_color_nodes_v2", "video_gen_v2_fcs_concat_videos"):
            self._install_furgen_video_tools_node(
                required_class_types=required_class_types or self._video_gen_v2_bundle_verify_class_types(bundle_id)
            )
            return
        if bundle_id == "video_gen_v2_image_filters_nodes":
            self._install_git_custom_node(
                "https://github.com/spacepxl/ComfyUI-Image-Filters",
                verify_dir_name="ComfyUI-Image-Filters",
                git_ref="bbb3fb0045461adf3602faeedaf40af57090d4e2",
                install_requirements=False,
            )
            self._ensure_video_gen_v2_image_filters_opencv()
            return
        if bundle_id == "asset_gen_v5_ltx23_fp8":
            self._install_git_custom_node(
                "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite",
                verify_dir_name="ComfyUI-VideoHelperSuite",
                git_ref="08e8df15db24da292d4b7f943c460dc2ab442b24",
            )
            self._install_git_custom_node(
                "https://github.com/kijai/ComfyUI-MelBandRoFormer",
                verify_dir_name="ComfyUI-MelBandRoFormer",
            )
            return
        raise RuntimeError(f"Unsupported video_gen_v2 node bundle: {bundle_id}")

    def _video_gen_v2_bundle_verify_class_types(self, bundle_id: str) -> List[str]:
        if bundle_id == "video_gen_v2_10s_ltx_nodes":
            return [
                "CM_FloatToInt",
                "GuiderParameters",
                "ImageResizeKJv2",
                "VHS_VideoCombine",
                "LatentMotionSharpener",
                "LatentTemporalInpainter",
                "LTXAVTextEncoderLoader",
                "LTXVAudioVAELoader",
                "LTXVAudioVAEEncode",
                "LTXVAudioVideoMask",
                "LTXVAddGuideMulti",
                "LTXVAddLatentGuide",
                "LTXVConcatAVLatent",
                "LTXVCropGuides",
                "LTXVImgToVideoConditionOnly",
                "LTXVImgToVideoInplace",
                "LTXVImgToVideoInplaceKJ",
                "LTXVLatentUpsampler",
                "LTXVSeparateAVLatent",
                "LTXAddVideoICLoRAGuide",
                "MultimodalGuider",
                "ImageBatchExtendWithOverlap",
                "VHS_LoadVideo",
            ]
        if bundle_id == "video_gen_v2_ltx_context_windows":
            return [
                "LTXVContextWindows",
            ]
        if bundle_id == "video_gen_v2_furgen_color_nodes":
            return [
                "FurgenExposureAdjust",
                "FurgenGetImageRangeFromBatch",
                "FurgenPrependImageToBatch",
                "FurgenTrimAudioDuration",
                "FurgenReferenceColorMatch",
            ]
        if bundle_id == "video_gen_v2_furgen_color_nodes_v2":
            return [
                "FurgenGetImageRangeFromBatch",
                "FurgenPrependImageToBatch",
                "FurgenSeamScaleStabilize",
                "FurgenTrimAudioDuration",
                "FurgenAdaptiveExposureMatch",
                "FurgenColorTransferMatch",
                "FurgenTemporalToneSmooth",
                "FurgenTemporalUnsharpMask",
                "FurgenLatentGuideTemporalMask",
                "FurgenLTXVAddLatentGuideTemporal",
                "FurgenLTXGuideAttentionAdjust",
                "FurgenAssertFiniteImages",
                "FurgenAssertFiniteLatent",
            ]
        if bundle_id == "video_gen_v2_fcs_concat_videos":
            return [
                "FCSConcatVideos",
                "FCSConcatVideosV2",
            ]
        if bundle_id == "video_gen_v2_image_filters_nodes":
            return [
                "AdainImage",
                "BatchNormalizeImage",
                "ColorMatchImage",
                "ExposureAdjust",
                "RemapRange",
            ]
        if bundle_id == "asset_gen_v5_ltx23_fp8":
            return [
                "MelBandRoFormerModelLoader",
                "MelBandRoFormerSampler",
                "VHS_LoadAudioUpload",
            ]
        return []

    def _local_comfy_has_all_class_types(self, class_types: List[str]) -> bool:
        wanted = [class_type for class_type in class_types if isinstance(class_type, str) and class_type.strip()]
        if not wanted:
            return False
        return bool(self._probe_local_node_contract(required_class_types=wanted, force=True).get("ready"))

    def _restart_local_comfy_with_supervisor(self) -> bool:
        supervisorctl = shutil.which("supervisorctl")
        if not supervisorctl:
            return False
        service_names = ["comfyui", "comfy", "comfyui-server"]
        errors: List[str] = []
        for service_name in service_names:
            try:
                proc = subprocess.run(
                    [supervisorctl, "restart", service_name],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=90,
                )
            except Exception as exc:
                errors.append(f"{service_name}: {exc}")
                continue
            combined = f"{proc.stdout}\n{proc.stderr}".lower()
            if proc.returncode == 0 or "started" in combined or "startsecs" in combined:
                logging.info("Restarted ComfyUI via supervisor service '%s'.", service_name)
                return True
            errors.append(f"{service_name}: rc={proc.returncode} {proc.stdout} {proc.stderr}".strip())
        logging.warning("Supervisor ComfyUI restart failed: %s", " | ".join(errors)[-2000:])
        return False

    def _comfy_launch_script_candidates(self) -> List[Path]:
        candidates: List[Path] = []
        explicit = _env_str("DM_COMFYUI_LAUNCH_SCRIPT")
        if explicit:
            candidates.append(Path(explicit))
        candidates.extend([
            Path("/opt/supervisor-scripts/comfyui.sh"),
            self.workspace / "comfyui.sh",
            self.workspace / "start_comfyui.sh",
        ])
        deduped: List[Path] = []
        seen: Set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if key not in seen:
                seen.add(key)
                deduped.append(candidate)
        return deduped

    def _find_local_comfy_processes(self) -> List[int]:
        proc_dir = Path("/proc")
        if not proc_dir.exists():
            return []
        pids: List[int] = []
        comfy_dir = self.comfyui_dir.resolve()
        launch_scripts = {str(candidate) for candidate in self._comfy_launch_script_candidates()}
        for entry in proc_dir.iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid == os.getpid():
                continue
            try:
                raw = (entry / "cmdline").read_bytes()
                args = [
                    part.decode("utf-8", errors="replace")
                    for part in raw.split(b"\x00")
                    if part
                ]
            except Exception:
                continue
            if not args:
                continue
            has_launch_script = any(
                arg in launch_scripts
                or arg.endswith("/comfyui.sh")
                or arg.endswith("/start_comfyui.sh")
                for arg in args
            )
            has_main_py = any(arg == "main.py" or arg.endswith("/main.py") for arg in args)
            if not has_launch_script and not has_main_py:
                continue
            cwd_matches = False
            try:
                cwd = Path(os.readlink(entry / "cwd")).resolve()
                cwd_matches = cwd == comfy_dir or comfy_dir in cwd.parents
            except Exception:
                cwd_matches = False
            main_path_matches = any(
                arg == str(comfy_dir / "main.py")
                or arg.endswith("/ComfyUI/main.py")
                for arg in args
            )
            if has_launch_script or (has_main_py and (cwd_matches or main_path_matches)):
                pids.append(pid)
        return sorted(set(pids))

    def _find_local_comfy_main_processes(self) -> List[int]:
        """Return only Comfy's Python main.py PIDs, excluding stable wrappers."""
        main_pids: List[int] = []
        for pid in self._find_local_comfy_processes():
            try:
                raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
                args = [
                    part.decode("utf-8", errors="replace")
                    for part in raw.split(b"\x00")
                    if part
                ]
            except Exception:
                continue
            if any(arg == "main.py" or arg.endswith("/main.py") for arg in args):
                main_pids.append(pid)
        return sorted(set(main_pids))

    def _wait_for_pids_to_exit(self, pids: List[int], timeout_seconds: float) -> bool:
        deadline = time.time() + max(0.0, timeout_seconds)
        remaining = set(int(pid) for pid in pids if isinstance(pid, int) and pid > 0)
        while remaining and time.time() < deadline:
            for pid in list(remaining):
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    remaining.discard(pid)
                except PermissionError:
                    pass
            if remaining:
                time.sleep(0.5)
        return not remaining

    def _restart_local_comfy_with_launch_script(self) -> bool:
        launch_script: Optional[Path] = None
        for candidate in self._comfy_launch_script_candidates():
            if candidate.exists() and candidate.is_file():
                launch_script = candidate
                break
        if launch_script is None:
            return False

        pids = self._find_local_comfy_processes()
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
            except Exception as exc:
                logging.warning("Failed to terminate ComfyUI pid %s before launch-script restart: %s", pid, exc)
        turnover_proven = bool(pids) and self._wait_for_pids_to_exit(pids, timeout_seconds=20.0)
        if pids and not turnover_proven:
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    continue
                except Exception as exc:
                    logging.warning("Failed to kill ComfyUI pid %s before launch-script restart: %s", pid, exc)
            turnover_proven = self._wait_for_pids_to_exit(pids, timeout_seconds=5.0)

        env = os.environ.copy()
        env.setdefault("WORKSPACE", str(self.workspace))
        env.setdefault("DM_COMFYUI_DIR", str(self.comfyui_dir))
        env["DM_LOCAL_COMFY_BASE_URL"] = self.agent_local_comfy_base_url
        if self.server_type in ("video_gen_v2", "video_gen_v3", "video_gen_v4"):
            env["DM_LOCAL_COMFY_ALLOW_DISCOVERY"] = "false"
        # The Vast Comfy image portal wrapper can block forever waiting for
        # /etc/portal.yaml when launched outside its original supervisor path.
        env["SERVERLESS"] = "true"
        # Vast's launch wrapper uses `unbuffer -p` when available. In a
        # detached dependency-agent process stdin is already closed, so `-p`
        # propagates EOF and terminates ComfyUI during startup.
        env["DISABLE_PTY"] = "true"
        configured = self._normalize_local_comfy_base_url(self.agent_local_comfy_base_url) or "http://127.0.0.1:8188"
        parsed = urllib.parse.urlparse(configured)
        launch_port = parsed.port or 8188
        launch_args = f"--disable-auto-launch --listen 0.0.0.0 --port {launch_port} --enable-cors-header"
        if self.server_type in ("video_gen_v2", "video_gen_v3", "video_gen_v4"):
            launch_args += f" {self._h3_attention_cli_flag()}"
        # The provisioning env carries memory/VRAM flags that the workload
        # depends on (allocator behaviour, DynamicVRAM headroom, node cache).
        # This fallback rebuilds COMFYUI_ARGS from scratch, so re-apply them or
        # a launch-script restart silently downgrades the process.
        launch_args += _preserved_comfy_memory_flags(os.environ.get("COMFYUI_ARGS", ""))
        env["COMFYUI_ARGS"] = launch_args
        log_path = Path(_env_str("DM_COMFYUI_RESTART_LOG_PATH") or str(self.workspace / "comfyui_restart.log"))
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            subprocess.Popen(
                [
                    "bash",
                    "-lc",
                    (
                        "set -o pipefail; "
                        "dashboard_fd=\"${DM_COMFYUI_DASHBOARD_LOG_FD:-/proc/1/fd/1}\"; "
                        "[[ -w \"$dashboard_fd\" ]] || dashboard_fd=/dev/stdout; "
                        "bash \"$1\" 2>&1 | tee -a \"$2\" > \"$dashboard_fd\""
                    ),
                    "_",
                    str(launch_script),
                    str(log_path),
                ],
                cwd=str(self.workspace),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            logging.info("Restarted ComfyUI via launch script: %s", launch_script)
            if turnover_proven:
                self._last_comfy_restart_process_turnover_proven = True
            return True
        except Exception as exc:
            logging.warning("ComfyUI launch-script restart failed (%s): %s", launch_script, exc)
            return False

    def _restart_local_comfy(
        self,
        prefer_process_restart: bool = False,
        allow_supervisor_restart: bool = True,
    ) -> None:
        with self._comfy_restart_lock:
            # asset_gen_v7_lite deliberately uses Vast's SSH runtime so port
            # 8080 belongs to the authenticated inference gateway. That
            # runtime has no supervisord process to relaunch ComfyUI after a
            # Comfy-Manager reboot endpoint stops it. Always use the detached
            # image launch script on v7; otherwise a dynamic node-bundle
            # install can leave an otherwise healthy worker permanently down.
            if (self.server_type or "").strip() == "asset_gen_v7_lite":
                if self._restart_local_comfy_with_launch_script():
                    return
                raise RuntimeError("asset_gen_v7_lite ComfyUI launch script is unavailable.")

            if allow_supervisor_restart and prefer_process_restart and self._restart_local_comfy_with_supervisor():
                return

            comfy_was_reachable = self._local_comfy_reachable(timeout_seconds=2.0)
            restart_endpoints = [
                "/manager/reboot",
                "/api/manager/reboot",
                "/restart",
                "/system/restart",
                "/reboot",
            ]
            for endpoint in restart_endpoints:
                try:
                    self._comfy_api_json("GET", endpoint, timeout_seconds=10.0)
                    if self._wait_for_local_comfy_down(timeout_seconds=30.0):
                        self._last_comfy_restart_process_turnover_proven = True
                    return
                except Exception as exc:
                    msg = str(exc).lower()
                    if (
                        comfy_was_reachable
                        and ("connection reset" in msg or "ecconnreset" in msg or "timeout" in msg)
                    ):
                        if self._wait_for_local_comfy_down(timeout_seconds=30.0):
                            self._last_comfy_restart_process_turnover_proven = True
                        return

            if allow_supervisor_restart and not prefer_process_restart and self._restart_local_comfy_with_supervisor():
                return
            if self._restart_local_comfy_with_launch_script():
                return

            raise RuntimeError("All local ComfyUI restart endpoints failed or are unavailable.")

    def _restart_local_comfy_and_wait(
        self,
        prefer_process_restart: bool = False,
        allow_supervisor_restart: bool = True,
        verify_class_types: Optional[List[str]] = None,
        timeout_seconds: float = 300.0,
        skip_if_reachable: bool = False,
        require_readiness_marker: bool = True,
    ) -> bool:
        """Restart ComfyUI once and wait for readiness while blocking peer restarts.

        When failure recovery workers arrive concurrently, the first worker owns
        the restart. Later workers recheck reachability after acquiring the lock
        and join the completed recovery instead of restarting ComfyUI again.
        Returns True when this caller initiated the restart.
        """
        with self._comfy_restart_lock:
            if skip_if_reachable and self._local_comfy_reachable(timeout_seconds=5.0):
                logging.info("ComfyUI recovered while waiting for restart ownership; skipping duplicate restart.")
                return False

            previous_process_ids = self._find_local_comfy_main_processes()
            self._last_comfy_restart_process_turnover_proven = False
            if allow_supervisor_restart:
                self._restart_local_comfy(prefer_process_restart=prefer_process_restart)
            else:
                self._restart_local_comfy(
                    prefer_process_restart=prefer_process_restart,
                    allow_supervisor_restart=False,
                )
            if verify_class_types is None:
                if require_readiness_marker:
                    self._wait_for_local_comfy_ready(timeout_seconds=timeout_seconds)
                else:
                    self._wait_for_local_comfy_ready(
                        timeout_seconds=timeout_seconds,
                        require_readiness_marker=False,
                    )
            else:
                self._restart_verify_previous_process_ids = previous_process_ids
                try:
                    self._wait_for_local_comfy_restart(
                        verify_class_types,
                        timeout_seconds=timeout_seconds,
                    )
                finally:
                    self._restart_verify_previous_process_ids = []
            # The restart path rebuilds COMFYUI_ARGS from scratch, so the flags
            # the template baked are not proof of what the new process got.
            # Record the replacement's actual argv while the restart is still
            # the obvious cause of any change.
            self._log_comfy_runtime_snapshot(
                "after restart",
                self._comfy_runtime_snapshot(force_refresh=True),
            )
            return True

    def _arm_post_job_comfy_recycle(self, lease: AgentExecuteLease, outcome: str = "success") -> None:
        enabled = (
            self.restart_comfy_after_failed_job
            if outcome == "failure"
            else self.restart_comfy_after_successful_job
        )
        if not enabled:
            return
        if self._post_job_comfy_recycle_active.is_set():
            return
        self._post_job_comfy_recycle_active.set()
        logging.info(
            "Armed post-job ComfyUI recycle before releasing capacity: jobId=%s itemId=%s outcome=%s",
            lease.job_id,
            lease.item_id,
            outcome,
        )
        try:
            self._write_agent_runtime_mirror(force_full=True)
        except Exception as exc:
            logging.debug("Failed to publish post-job ComfyUI recycle gate: %s", exc)

    def _complete_post_job_comfy_recycle(self, lease: AgentExecuteLease) -> bool:
        if not self._post_job_comfy_recycle_active.is_set():
            return True

        logging.info(
            "Recycling ComfyUI after completed workflow before releasing capacity: jobId=%s itemId=%s",
            lease.job_id,
            lease.item_id,
        )
        self._remove_local_readiness_file()
        try:
            self._write_agent_runtime_mirror(force_full=True)
        except Exception as exc:
            logging.debug("Failed to publish ComfyUI recycle readiness drain: %s", exc)

        try:
            verify_class_types = self._node_bundle_restart_verify_class_types()
            self._restart_local_comfy_and_wait(
                prefer_process_restart=True,
                allow_supervisor_restart=False,
                verify_class_types=verify_class_types or None,
                timeout_seconds=300.0,
                require_readiness_marker=False,
            )
            self._write_local_readiness_file()
            self._post_job_comfy_recycle_active.clear()
            self._last_agent_http_transition_signature = ""
            self._last_agent_runtime_signature = ""
            self._last_agent_heartbeat_ms = 0
            try:
                self._write_agent_runtime_mirror(force_full=True)
            except Exception as exc:
                logging.debug("Failed to publish completed ComfyUI recycle runtime: %s", exc)
            self._request_agent_queue_poll()
            logging.info(
                "Post-job ComfyUI recycle completed; capacity may resume: jobId=%s itemId=%s verifiedClassTypes=%d",
                lease.job_id,
                lease.item_id,
                len(verify_class_types),
            )
            return True
        except Exception as exc:
            # Keep both the local capacity gate and readiness drain active. A
            # queued command may exist already, but it must not execute against
            # a failed or partially restarted Comfy process.
            self._last_agent_http_transition_signature = ""
            self._last_agent_runtime_signature = ""
            self._last_agent_heartbeat_ms = 0
            try:
                self._write_agent_runtime_mirror(force_full=True)
            except Exception as mirror_exc:
                logging.debug("Failed to publish failed ComfyUI recycle runtime: %s", mirror_exc)
            logging.error(
                "Post-job ComfyUI recycle failed; capacity remains drained: jobId=%s itemId=%s err=%s",
                lease.job_id,
                lease.item_id,
                exc,
            )
            return False

    def _invalidate_local_node_contract_probe(self) -> None:
        lock = getattr(self, "_node_contract_probe_lock", None)
        if lock is None:
            self._node_contract_probe_cache = {}
            self._reset_node_contract_missing_tracker()
            return
        with lock:
            self._node_contract_probe_cache = {}
            self._reset_node_contract_missing_tracker()

    def _reset_node_contract_missing_tracker(self, signature: Optional[str] = None) -> None:
        """Forget in-flight missing-class confirmation windows.

        Clears every contract when `signature` is None (ComfyUI is restarting, so
        nothing observed before is still valid), otherwise just that contract's
        window (its probe failed). Keeps each window measuring a CONTINUOUSLY
        reachable ComfyUI reporting the SAME classes missing — never a total
        accumulated across restarts.
        Callers already hold `_node_contract_probe_lock` where one exists.
        """
        windows = getattr(self, "_node_contract_missing_windows", None)
        if not isinstance(windows, dict):
            self._node_contract_missing_windows = {}
            return
        if signature is None:
            windows.clear()
        else:
            windows.pop(signature, None)

    def _track_node_contract_missing(
        self,
        signature: str,
        missing: List[str],
        checked_at_ms: int,
    ) -> int:
        """Return how long this exact missing set has been observed, in ms.

        Any change to the set restarts the clock: while ComfyUI imports custom
        nodes the set shrinks on every probe, so a still-loading ComfyUI never
        accumulates time and never gets reported as broken.
        """
        windows = getattr(self, "_node_contract_missing_windows", None)
        if not isinstance(windows, dict):
            windows = {}
            self._node_contract_missing_windows = windows
        current = frozenset(missing)
        previous = windows.get(signature)
        if previous is None or previous[0] != current:
            windows[signature] = (current, checked_at_ms)
            return 0
        return max(0, checked_at_ms - int(previous[1]))

    def _probe_local_node_contract(
        self,
        required_class_types: Optional[Iterable[str]] = None,
        force: bool = False,
        timeout_seconds: float = 10.0,
    ) -> Dict[str, Any]:
        """Check every required custom-node class with one cached Comfy request.

        `/object_info` is intentionally fetched at most once per TTL during
        steady-state heartbeats. Restart/install paths force a fresh snapshot,
        so readiness never rests on pre-restart class data.
        """
        wanted = sorted({
            class_type.strip()
            for class_type in (
                required_class_types
                if required_class_types is not None
                else self._node_bundle_restart_verify_class_types()
            )
            if isinstance(class_type, str) and class_type.strip()
        })
        # Use datetime rather than time.time() so restart-loop tests and callers
        # that control the polling clock do not consume an extra deadline tick.
        checked_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        signature = _sha256_hex_bytes(_canonical_json_bytes(wanted))
        if not wanted:
            result = {
                "ready": True,
                "requiredClassCount": 0,
                "missingClassTypes": [],
                "checkedAtMs": checked_at_ms,
                "contractHash": signature,
                "conclusive": True,
            }
            self._node_contract_probe_cache = result
            return result

        lock = getattr(self, "_node_contract_probe_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._node_contract_probe_lock = lock
        with lock:
            cached = getattr(self, "_node_contract_probe_cache", {})
            ttl_ms = int(getattr(self, "node_contract_probe_ttl_ms", 60_000) or 60_000)
            if (
                not force
                and isinstance(cached, dict)
                and cached.get("contractHash") == signature
                and checked_at_ms - int(cached.get("checkedAtMs") or 0) < ttl_ms
            ):
                return dict(cached)

            try:
                status, response = self._comfy_api_json("GET", "/object_info", timeout_seconds=timeout_seconds)
                if status != 200 or not isinstance(response, dict):
                    raise RuntimeError(f"object_info returned status={status}")
                missing = [class_type for class_type in wanted if class_type not in response]
                if not missing:
                    self._reset_node_contract_missing_tracker(signature)
                    result = {
                        "ready": True,
                        "requiredClassCount": len(wanted),
                        "missingClassTypes": [],
                        "checkedAtMs": checked_at_ms,
                        "contractHash": signature,
                        "conclusive": True,
                    }
                else:
                    missing_for_ms = self._track_node_contract_missing(signature, missing, checked_at_ms)
                    if missing_for_ms >= self.node_contract_missing_confirm_ms:
                        result = {
                            "ready": False,
                            "requiredClassCount": len(wanted),
                            "missingClassTypes": missing[:100],
                            "checkedAtMs": checked_at_ms,
                            "contractHash": signature,
                            "conclusive": True,
                            "missingForMs": missing_for_ms,
                        }
                    else:
                        # Not yet proven absent. Report it as an inconclusive
                        # probe (empty missing list + probeError) so neither the
                        # local readiness marker nor the server-side quarantine
                        # acts on a ComfyUI that is still importing nodes.
                        result = {
                            "ready": False,
                            "requiredClassCount": len(wanted),
                            "missingClassTypes": [],
                            "checkedAtMs": checked_at_ms,
                            "contractHash": signature,
                            "conclusive": False,
                            "missingForMs": missing_for_ms,
                            "unconfirmedMissingClassTypes": missing[:100],
                            "probeError": (
                                f"node contract unconfirmed: {len(missing)} class(es) missing for "
                                f"{missing_for_ms}ms (<{self.node_contract_missing_confirm_ms}ms); "
                                "ComfyUI may still be importing custom nodes"
                            )[:500],
                        }
            except Exception as exc:
                # Could not look. Never claim specific classes are absent on the
                # strength of a failed probe — that is indistinguishable from a
                # restarting ComfyUI and previously reported the ENTIRE required
                # contract as missing.
                self._reset_node_contract_missing_tracker(signature)
                result = {
                    "ready": False,
                    "requiredClassCount": len(wanted),
                    "missingClassTypes": [],
                    "checkedAtMs": checked_at_ms,
                    "contractHash": signature,
                    "conclusive": False,
                    "probeError": str(exc)[:500],
                }
            self._node_contract_probe_cache = result
            return dict(result)

    def _local_node_contract_runtime(self, force: bool = False) -> Dict[str, Any]:
        result = self._probe_local_node_contract(force=force)
        missing = result.get("missingClassTypes") if isinstance(result.get("missingClassTypes"), list) else []
        return {
            **result,
            "affectedBundleIds": self._node_contract_affected_bundle_ids(missing),
        }

    def _local_comfy_has_class_type(self, class_type: str, timeout_seconds: float = 10.0) -> bool:
        if not isinstance(class_type, str) or not class_type.strip():
            return False
        normalized = class_type.strip()
        quoted = urllib.parse.quote(normalized, safe="")
        try:
            status, resp = self._comfy_api_json("GET", f"/object_info/{quoted}", timeout_seconds=timeout_seconds)
            if status == 200 and isinstance(resp, dict):
                if normalized in resp and isinstance(resp.get(normalized), dict):
                    return True
                if resp.get("name") == normalized:
                    return True
        except Exception:
            pass

        try:
            status, resp = self._comfy_api_json("GET", "/object_info", timeout_seconds=timeout_seconds)
            return status == 200 and isinstance(resp, dict) and normalized in resp
        except Exception:
            return False

    def _wait_for_local_comfy_ready(
        self,
        timeout_seconds: float = 300.0,
        require_readiness_marker: bool = True,
    ) -> None:
        deadline = time.time() + max(30.0, timeout_seconds)
        while time.time() < deadline:
            reachable = self._local_comfy_reachable(timeout_seconds=5.0)
            marker_ready = not require_readiness_marker or self._local_readiness_file_present()
            runtime_ready = self._video_gen_v2_sageattention_runtime_ready()
            if reachable and marker_ready and runtime_ready:
                return
            time.sleep(2.0)
        raise RuntimeError("Local ComfyUI did not become ready after restart.")

    def _wait_for_local_comfy_restart(
        self,
        verify_class_types: List[str],
        timeout_seconds: float = 300.0,
        previous_process_ids: Optional[Iterable[int]] = None,
    ) -> None:
        deadline = time.time() + max(30.0, timeout_seconds)
        saw_down = not self._local_comfy_reachable(timeout_seconds=5.0)
        restart_process_ids = (
            previous_process_ids
            if previous_process_ids is not None
            else getattr(self, "_restart_verify_previous_process_ids", [])
        )
        previous_pids = {
            int(pid) for pid in restart_process_ids or []
            if isinstance(pid, int) and pid > 0
        }
        saw_process_turnover = bool(
            getattr(self, "_last_comfy_restart_process_turnover_proven", False)
        )
        normalized_verify = list(dict.fromkeys(
            class_type.strip()
            for class_type in verify_class_types
            if isinstance(class_type, str) and class_type.strip()
        ))
        last_missing = normalized_verify
        self._invalidate_local_node_contract_probe()

        while time.time() < deadline:
            reachable = self._local_comfy_reachable(timeout_seconds=5.0)
            if previous_pids:
                current_pids = set(self._find_local_comfy_main_processes())
                if current_pids and current_pids.isdisjoint(previous_pids):
                    saw_process_turnover = True
            if not reachable:
                saw_down = True
                time.sleep(2.0)
                continue

            contract = self._probe_local_node_contract(
                required_class_types=normalized_verify,
                force=True,
                timeout_seconds=10.0,
            )
            missing = contract.get("missingClassTypes") if isinstance(contract.get("missingClassTypes"), list) else normalized_verify
            # An inconclusive probe now reports an EMPTY missing list, which must
            # never be mistaken for "restart verified". Only a conclusive probe
            # can end this wait.
            conclusive = contract.get("conclusive") is True
            if not conclusive:
                missing = (
                    contract.get("unconfirmedMissingClassTypes")
                    if isinstance(contract.get("unconfirmedMissingClassTypes"), list)
                    else normalized_verify
                )
            runtime_ready = self._video_gen_v2_sageattention_runtime_ready()
            # Never validate against the old listener while an asynchronous
            # restart is still pending. A port-down transition or an entirely
            # new Comfy PID set proves process turnover; the latter also covers
            # synchronous launch-script restarts completed before polling.
            if runtime_ready and conclusive and not missing and (saw_down or saw_process_turnover):
                return
            if runtime_ready and not normalized_verify and saw_down and reachable:
                return

            last_missing = missing
            time.sleep(2.0)

        detail = ""
        if last_missing:
            detail = f" Missing classes: {', '.join(last_missing[:10])}"
        raise RuntimeError(f"Local ComfyUI did not finish restarting after bundle installation.{detail}")

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
                    if _is_path_open_by_process(path):
                        logging.warning("Skipping eviction of open dynamic dependency %s: %s", dep_id, dest_rel)
                        continue
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
            self._state.verified.pop(dep_id, None)
            self._session_hash_verified_dep_ids.discard(dep_id)
            freed += int(size)
            evicted += 1
            logging.info("Evicted dynamic dependency %s (%d bytes): %s", dep_id, size, dest_rel)

        if evicted > 0:
            self._save_state()

        return freed

    def _ensure_space_for_download(self, expected_size_bytes: int, dep_id: str, dest_abs: Optional[Path] = None) -> bool:
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

        stats_path = dest_abs.parent if isinstance(dest_abs, Path) else self.comfyui_dir
        stats = disk_stats(stats_path)
        free_now = int(stats.get("freeBytes", 0))
        if free_now < required_free:
            diag = self._disk_diagnostics_payload()
            deleted_open = diag.get("deletedOpenFiles") if isinstance(diag, dict) else {}
            deleted_count = deleted_open.get("count") if isinstance(deleted_open, dict) else 0
            deleted_bytes = deleted_open.get("bytes") if isinstance(deleted_open, dict) else 0
            mount = stats.get("mount") if isinstance(stats.get("mount"), dict) else {}
            mount_point = mount.get("mountPoint") if isinstance(mount, dict) else None
            fs_type = mount.get("filesystemType") if isinstance(mount, dict) else None
            raise RuntimeError(
                f"Insufficient disk space: freeBytes={free_now} requiredFreeBytes={required_free} "
                f"path={stats.get('statPath') or stats_path} mount={mount_point or '-'} fs={fs_type or '-'} "
                f"deletedOpenBytes={int(deleted_bytes or 0)} deletedOpenCount={int(deleted_count or 0)}"
            )

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
            retry_raw = data.get("retry") if isinstance(data, dict) else None
            retry: Dict[str, Dict[str, Any]] = {}
            if isinstance(retry_raw, dict):
                now = _now_ms()
                for dep_id, entry in retry_raw.items():
                    if not isinstance(dep_id, str) or not dep_id:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    item_id = entry.get("itemId") if isinstance(entry.get("itemId"), str) else dep_id
                    resolved = entry.get("resolved") if isinstance(entry.get("resolved"), dict) else None
                    attempts = int(entry.get("attempts")) if isinstance(entry.get("attempts"), (int, float)) else 0
                    next_at = int(entry.get("nextAttemptAtMs")) if isinstance(entry.get("nextAttemptAtMs"), (int, float)) else now
                    last_err = entry.get("lastError") if isinstance(entry.get("lastError"), str) else None
                    last_attempt = int(entry.get("lastAttemptAtMs")) if isinstance(entry.get("lastAttemptAtMs"), (int, float)) else 0
                    if resolved:
                        retry[dep_id] = {
                            "itemId": item_id,
                            "resolved": resolved,
                            "attempts": max(0, attempts),
                            "nextAttemptAtMs": max(0, next_at),
                            "lastError": last_err or "",
                            "lastAttemptAtMs": max(0, last_attempt),
                        }
            verified_raw = data.get("verified") if isinstance(data, dict) else None
            verified: Dict[str, Dict[str, Any]] = {}
            if isinstance(verified_raw, dict):
                for dep_id, entry in verified_raw.items():
                    if not isinstance(dep_id, str) or not dep_id or not isinstance(entry, dict):
                        continue
                    dest_rel = entry.get("destRelativePath")
                    sha256_value = entry.get("sha256")
                    size_bytes = entry.get("sizeBytes")
                    mtime_ns = entry.get("mtimeNs")
                    verified_at_ms = entry.get("verifiedAtMs")
                    if not isinstance(dest_rel, str) or not dest_rel:
                        continue
                    if not isinstance(size_bytes, int) or size_bytes < 0:
                        continue
                    if not isinstance(mtime_ns, int) or mtime_ns < 0:
                        continue
                    verified[dep_id] = {
                        "destRelativePath": dest_rel,
                        "sha256": sha256_value.lower() if isinstance(sha256_value, str) else "",
                        "sizeBytes": size_bytes,
                        "mtimeNs": mtime_ns,
                        "verifiedAtMs": verified_at_ms if isinstance(verified_at_ms, int) else 0,
                    }
            node_bundle_verify_class_types = set(
                class_type
                for class_type in data.get("node_bundle_verify_class_types", [])
                if isinstance(class_type, str) and class_type
            )
            node_bundle_verify_class_types_by_bundle: Dict[str, Set[str]] = {}
            by_bundle_raw = data.get("node_bundle_verify_class_types_by_bundle", {})
            if isinstance(by_bundle_raw, dict):
                for bundle_id, class_types in by_bundle_raw.items():
                    if not isinstance(bundle_id, str) or not bundle_id or not isinstance(class_types, list):
                        continue
                    normalized_classes = {
                        class_type.strip()
                        for class_type in class_types
                        if isinstance(class_type, str) and class_type.strip()
                    }
                    if normalized_classes:
                        node_bundle_verify_class_types_by_bundle[bundle_id] = normalized_classes
                        node_bundle_verify_class_types.update(normalized_classes)
            return LocalState(
                installed_static=installed_static,
                installed_dynamic=installed_dynamic,
                failed=failed,
                lru=lru,
                retry=retry,
                verified=verified,
                node_bundle_verify_class_types=node_bundle_verify_class_types,
                node_bundle_verify_class_types_by_bundle=node_bundle_verify_class_types_by_bundle,
            )
        except Exception:
            return LocalState.empty()

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        data = {
            "installed_static": sorted(self._state.installed_static),
            "installed_dynamic": sorted(self._state.installed_dynamic),
            "failed": sorted(self._state.failed),
            "lru": self._state.lru,
            "retry": self._state.retry,
            "verified": self._state.verified,
            "node_bundle_verify_class_types": sorted(self._state.node_bundle_verify_class_types),
            "node_bundle_verify_class_types_by_bundle": {
                bundle_id: sorted(class_types)
                for bundle_id, class_types in sorted(self._state.node_bundle_verify_class_types_by_bundle.items())
            },
            "updatedAtMs": _now_ms(),
        }
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), "utf-8")
        os.replace(str(tmp), str(self.state_path))

    def _remember_node_bundle_verify_class_types(
        self,
        class_types: List[str],
        bundle_ids: Optional[List[str]] = None,
        bundle_specs: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized = {
            class_type
            for class_type in class_types
            if isinstance(class_type, str) and class_type
        }
        if not normalized:
            return
        changed = False
        with self._lock:
            missing = normalized - self._state.node_bundle_verify_class_types
            if missing:
                self._state.node_bundle_verify_class_types.update(missing)
                changed = True
            normalized_bundle_ids = [
                bundle_id for bundle_id in (bundle_ids or [])
                if isinstance(bundle_id, str) and bundle_id
            ]
            for bundle_id in normalized_bundle_ids:
                scoped = {
                    class_type.strip()
                    for class_type in self._video_gen_v2_bundle_verify_class_types(bundle_id)
                    if isinstance(class_type, str) and class_type.strip()
                }
                spec = bundle_specs.get(bundle_id) if isinstance(bundle_specs, dict) else None
                if isinstance(spec, dict) and isinstance(spec.get("verifyClassTypes"), list):
                    scoped.update(
                        class_type.strip()
                        for class_type in spec["verifyClassTypes"]
                        if isinstance(class_type, str) and class_type.strip()
                    )
                if len(normalized_bundle_ids) == 1 and not scoped:
                    scoped = set(normalized)
                scoped.intersection_update(normalized)
                if not scoped:
                    continue
                existing = self._state.node_bundle_verify_class_types_by_bundle.setdefault(bundle_id, set())
                bundle_missing = scoped - existing
                if bundle_missing:
                    existing.update(bundle_missing)
                    changed = True
        if changed:
            self._save_state()
            logging.info(
                "Persisted custom-node restart verification contract: added=%d total=%d",
                len(missing),
                len(self._state.node_bundle_verify_class_types),
            )

    def _node_bundle_restart_verify_class_types(self) -> List[str]:
        with self._lock:
            return sorted(self._state.node_bundle_verify_class_types)

    def _node_contract_affected_bundle_ids(self, missing_class_types: Iterable[str]) -> List[str]:
        missing = {
            class_type.strip()
            for class_type in missing_class_types
            if isinstance(class_type, str) and class_type.strip()
        }
        if not missing:
            return []
        with self._lock:
            return sorted(
                bundle_id
                for bundle_id, class_types in self._state.node_bundle_verify_class_types_by_bundle.items()
                if missing.intersection(class_types)
            )

    def _normalize_agent_update_release(self, raw: Any) -> Optional[AgentSelfUpdateRelease]:
        if not isinstance(raw, dict):
            return None

        target = raw.get("targetVersion")
        download_url = raw.get("downloadUrl")
        if not isinstance(target, str) or not target.strip():
            return None
        if not isinstance(download_url, str) or not download_url.strip():
            return None

        sha256 = raw.get("sha256")
        sha256_norm = sha256.strip().lower() if isinstance(sha256, str) and re.match(r"^[0-9a-fA-F]{64}$", sha256.strip()) else None
        return AgentSelfUpdateRelease(
            target_version=target.strip(),
            download_url=download_url.strip(),
            sha256=sha256_norm,
        )

    def _current_script_sha256(self) -> Optional[str]:
        try:
            return sha256_file(self.self_script_path).lower()
        except Exception:
            return None

    def _clear_pending_self_update(self) -> None:
        self._pending_self_update = None
        self._pending_self_update_source = ""
        self._self_update_retry_at_ms = 0

    def _is_blocked_self_update_downgrade(self, release: AgentSelfUpdateRelease) -> bool:
        if self.self_update_allow_downgrade:
            return False
        comparison = compare_agent_versions(release.target_version, AGENT_VERSION)
        return comparison is not None and comparison < 0

    def _self_update_required(self, release: AgentSelfUpdateRelease) -> bool:
        if release.target_version != AGENT_VERSION:
            return True
        if release.sha256:
            current_sha = self._current_script_sha256()
            return current_sha is None or current_sha != release.sha256
        return False

    def _persist_self_update_release_for_watchdog(self, release: AgentSelfUpdateRelease, sha256: str) -> None:
        sha256_norm = (release.sha256 or sha256 or "").strip().lower()
        updates = {
            "DM_AGENT_URL": release.download_url,
            "DEPENDENCY_AGENT_TARGET_VERSION": release.target_version,
            "DEPENDENCY_AGENT_UPDATE_URL": release.download_url,
            "DEPENDENCY_AGENT_UPDATE_SHA256": sha256_norm,
            "DEPENDENCY_AGENT_RELEASE_VERSION": release.target_version,
            "DEPENDENCY_AGENT_RELEASE_SHA256": sha256_norm,
            "DM_AGENT_ENV_PATH": str(self.self_env_path),
        }
        for key, value in updates.items():
            if value:
                os.environ[key] = value

        if str(self.self_env_path):
            existing: List[str] = []
            try:
                if self.self_env_path.exists():
                    existing = self.self_env_path.read_text("utf-8", errors="replace").splitlines()
            except Exception as exc:
                logging.warning("Failed reading dependency agent env for self-update persistence: %s", exc)
                existing = []

            kept = []
            export_re = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=")
            for line in existing:
                match = export_re.match(line)
                if match and match.group(1) in WATCHDOG_RELEASE_ENV_KEYS:
                    continue
                kept.append(line)

            for key in sorted(updates):
                value = updates[key]
                if value:
                    kept.append(f"export {key}={shlex.quote(str(value))}")

            try:
                self.self_env_path.parent.mkdir(parents=True, exist_ok=True)
                next_text = "\n".join(kept).rstrip() + "\n"
                current_text = None
                try:
                    if self.self_env_path.exists():
                        current_text = self.self_env_path.read_text("utf-8", errors="replace")
                except Exception:
                    current_text = None
                if current_text != next_text:
                    tmp_env = self.self_env_path.parent / f".{self.self_env_path.name}.{uuid.uuid4().hex}.tmp"
                    tmp_env.write_text(next_text, "utf-8")
                    try:
                        os.chmod(tmp_env, 0o600)
                    except Exception:
                        pass
                    os.replace(str(tmp_env), str(self.self_env_path))
            except Exception as exc:
                logging.warning("Failed persisting dependency agent release env for watchdog: %s", exc)

        try:
            self.self_marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_text = f"{release.target_version}\n{sha256_norm}\n{release.download_url}\n"
            current_marker = None
            try:
                if self.self_marker_path.exists():
                    current_marker = self.self_marker_path.read_text("utf-8", errors="replace")
            except Exception:
                current_marker = None
            if current_marker != marker_text:
                self.self_marker_path.write_text(marker_text, "utf-8")
                try:
                    os.chmod(self.self_marker_path, 0o600)
                except Exception:
                    pass
        except Exception as exc:
            logging.warning("Failed persisting dependency agent launch marker for watchdog: %s", exc)

    def _apply_runtime_env_delivery(self, raw: Any, source: str) -> bool:
        if not isinstance(raw, dict):
            return False
        delivery_id = raw.get("deliveryId")
        env = raw.get("env")
        if not isinstance(delivery_id, str) or not re.match(r"^[A-Za-z0-9_-]{16,128}$", delivery_id.strip()):
            return False
        if not isinstance(env, dict):
            return False
        updates: Dict[str, str] = {}
        for key in RUNTIME_ENV_DELIVERY_KEYS:
            value = env.get(key)
            if isinstance(value, str) and value.strip():
                updates[key] = value
        if "HF_TOKEN" not in updates:
            return False

        existing: List[str] = []
        try:
            if self.self_env_path.exists():
                existing = self.self_env_path.read_text("utf-8", errors="replace").splitlines()
        except Exception as exc:
            logging.warning("Failed reading dependency agent env for runtime delivery: %s", exc)
            return False
        export_re = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=")
        kept = []
        for line in existing:
            match = export_re.match(line)
            if match and match.group(1) in updates:
                continue
            kept.append(line)
        for key in sorted(updates):
            kept.append(f"export {key}={shlex.quote(updates[key])}")
        next_text = "\n".join(kept).rstrip() + "\n"
        try:
            self.self_env_path.parent.mkdir(parents=True, exist_ok=True)
            current_text = self.self_env_path.read_text("utf-8", errors="replace") if self.self_env_path.exists() else None
            if current_text != next_text:
                tmp_env = self.self_env_path.parent / f".{self.self_env_path.name}.{uuid.uuid4().hex}.tmp"
                tmp_env.write_text(next_text, "utf-8")
                os.chmod(tmp_env, 0o600)
                os.replace(str(tmp_env), str(self.self_env_path))
        except Exception as exc:
            logging.warning("Failed persisting authenticated runtime env delivery: %s", exc)
            return False

        for key, value in updates.items():
            os.environ[key] = value
        self.hf_token = updates.get("HF_TOKEN") or self.hf_token
        self.civitai_token = updates.get("CIVITAI_TOKEN") or self.civitai_token
        self._runtime_env_delivery_id = delivery_id.strip()
        self._runtime_env_applied_keys = sorted(updates)
        logging.info(
            "Applied authenticated runtime env delivery from %s: deliveryId=%s keys=%s",
            source,
            self._runtime_env_delivery_id,
            ",".join(self._runtime_env_applied_keys),
        )
        return True

    def _maybe_fetch_runtime_env_delivery(self, source: str) -> None:
        if self._runtime_env_delivery_id:
            return
        if not self._resolved_instance_id or not self._agent_access_token:
            return
        now_ms = _now_ms()
        if now_ms < int(self._runtime_env_next_fetch_ms):
            return
        self._runtime_env_next_fetch_ms = now_ms + 30_000
        try:
            resp = self._agent_api(
                "GET",
                "/agent/runtime-env",
                query={"instanceId": self._resolved_instance_id},
                timeout_seconds=30.0,
                use_token=True,
                include_secret=False,
            )
            data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
            self._apply_runtime_env_delivery(data.get("runtimeEnvDelivery"), source)
        except Exception as exc:
            logging.warning("Authenticated runtime env delivery fetch failed: %s", exc)

    def _maybe_queue_self_update(self, raw: Any, source: str) -> None:
        if not self.self_update_enabled:
            return

        release = self._normalize_agent_update_release(raw)
        if release is None:
            return

        if self._is_blocked_self_update_downgrade(release):
            if self._pending_self_update == release:
                self._clear_pending_self_update()
            logging.warning(
                "Ignoring dependency agent self-update downgrade: current=%s target=%s source=%s",
                AGENT_VERSION,
                release.target_version,
                source,
            )
            return

        if not self._self_update_required(release):
            current_sha = self._current_script_sha256()
            if current_sha:
                self._persist_self_update_release_for_watchdog(release, current_sha)
            if self._pending_self_update == release:
                self._clear_pending_self_update()
            return

        if self._pending_self_update == release:
            return

        self._pending_self_update = release
        self._pending_self_update_source = source
        self._self_update_retry_at_ms = 0
        logging.info(
            "Queued dependency agent self-update: current=%s target=%s source=%s url=%s",
            AGENT_VERSION,
            release.target_version,
            source,
            release.download_url,
        )

    def _perform_pending_self_update(self) -> None:
        release = self._pending_self_update
        if release is None or self._stop.is_set():
            return

        now_ms = _now_ms()
        if now_ms < int(self._self_update_retry_at_ms):
            return

        if self._is_blocked_self_update_downgrade(release):
            logging.warning(
                "Skipping queued dependency agent self-update downgrade: current=%s target=%s source=%s",
                AGENT_VERSION,
                release.target_version,
                self._pending_self_update_source or "-",
            )
            self._clear_pending_self_update()
            return

        with self._lock:
            active_exec_count = len(self._active_exec_by_item)
            active_maintenance_count = len(self._active_maintenance_by_item)
        if active_exec_count > 0 or active_maintenance_count > 0:
            self._self_update_retry_at_ms = _now_ms() + int(self.self_update_retry_seconds * 1000)
            logging.info(
                "Deferring dependency agent self-update until active leases finish: "
                "current=%s target=%s activeExec=%d activeMaintenance=%d retryIn=%.0fs",
                AGENT_VERSION,
                release.target_version,
                active_exec_count,
                active_maintenance_count,
                float(self.self_update_retry_seconds),
            )
            return

        tmp_path = self.self_script_path.parent / f".{self.self_script_path.name}.{uuid.uuid4().hex}.tmp"
        current_sha = self._current_script_sha256()

        try:
            self.self_script_path.parent.mkdir(parents=True, exist_ok=True)
            http_download_to_file(
                release.download_url,
                tmp_path,
                timeout_seconds=max(60.0, float(self.download_timeout_seconds)),
                chunk_size=int(self.download_chunk_size),
                user_agent=f"dm-agent-self-update/{AGENT_VERSION}",
            )

            downloaded_sha = sha256_file(tmp_path).lower()
            if release.sha256 and downloaded_sha != release.sha256:
                raise RuntimeError(
                    f"self-update checksum mismatch: expected {release.sha256} got {downloaded_sha}"
                )

            downloaded_version = extract_agent_version_from_script(tmp_path)
            if downloaded_version != release.target_version:
                raise RuntimeError(
                    "self-update version mismatch: "
                    f"expected {release.target_version} got {downloaded_version or 'missing'}"
                )

            if current_sha and downloaded_sha == current_sha and release.target_version == AGENT_VERSION:
                logging.info(
                    "Dependency agent self-update target already installed: version=%s path=%s",
                    release.target_version,
                    self.self_script_path,
                )
                self._clear_pending_self_update()
                return

            if self._idle_prl_miner.save_agent_update_resume_marker("self_update"):
                logging.info("Dependency agent self-update will restore idle PRL miner after restart.")
            self._stop_idle_prl_mining_for_work("self_update")

            try:
                if self.self_script_path.exists():
                    mode = self.self_script_path.stat().st_mode & 0o777
                    os.chmod(tmp_path, mode or 0o755)
                else:
                    os.chmod(tmp_path, 0o755)
            except Exception:
                pass

            os.replace(str(tmp_path), str(self.self_script_path))
            self._persist_self_update_release_for_watchdog(release, downloaded_sha)
            logging.info(
                "Restarting dependency agent into updated script: old=%s new=%s path=%s",
                AGENT_VERSION,
                release.target_version,
                self.self_script_path,
            )
            os.execv(sys.executable, [sys.executable, str(self.self_script_path), *sys.argv[1:]])
        except Exception as e:
            self._self_update_retry_at_ms = _now_ms() + int(self.self_update_retry_seconds * 1000)
            try:
                self._idle_prl_miner.resume_after_agent_update_if_requested("self_update_failed")
            except Exception as resume_exc:
                logging.warning("Failed restoring idle PRL miner after self-update failure: %s", resume_exc)
            logging.warning(
                "Dependency agent self-update failed: current=%s target=%s source=%s retryIn=%.0fs err=%s",
                AGENT_VERSION,
                release.target_version,
                self._pending_self_update_source or "-",
                float(self.self_update_retry_seconds),
                e,
            )
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _register(self) -> None:
        instance_ip = self.instance_ip
        if not self.instance_id and not instance_ip and self.provider_metadata.get("provider") != "salad":
            instance_ip = detect_public_ip()
            if instance_ip:
                logging.info("Detected public IP: %s", instance_ip)
            else:
                logging.warning("Could not detect public IP; set DM_INSTANCE_ID or DM_INSTANCE_IP for reliable registration.")
        elif not self.instance_id and not instance_ip and self.provider_metadata.get("provider") == "salad":
            logging.info("Using Salad provider metadata for registration identity.")

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
        if self.provider_metadata:
            body["provider"] = self.provider_metadata.get("provider")
            body["providerMetadata"] = self.provider_metadata

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

        readiness_from_register = resp.get("readinessCheckFile")
        if (
            not self._agent_local_readiness_file_env
            and isinstance(readiness_from_register, str)
            and readiness_from_register.strip()
        ):
            self.agent_local_readiness_file = readiness_from_register.strip()
            logging.info(
                "Using readiness marker from server config: %s",
                self.agent_local_readiness_file,
            )

        self._set_coordination_from_response(resp, "dependencies/register")
        self._maybe_queue_self_update(resp.get("agentUpdate"), "dependencies/register")
        self._resolved_instance_id = instance_id
        self._token = agent_token
        logging.info("Registered dependency agent: instanceId=%s", instance_id)

    def _post_status(
        self,
        item: Dict[str, Any],
        state: str,
        error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self._resolved_instance_id:
            return None
        url = f"{self.api_base_url}/dependencies/status"
        with self._lock:
            dynamic_bytes_used = int(self._dynamic_bytes_used)
            _, _, _, _, active_downloads = self._dependency_runtime_snapshot_locked()
        body: Dict[str, Any] = {
            "instanceId": self._resolved_instance_id,
            "itemId": item.get("itemId") or item.get("depId"),
            "depId": item.get("depId"),
            "op": item.get("op"),
            "state": state,
            "activeDownloads": active_downloads,
            "diskStats": disk_stats(self.comfyui_dir),
            "diskDiagnostics": self._disk_diagnostics_payload(),
            "dynamicBytesUsed": dynamic_bytes_used,
        }
        if state == "succeeded":
            dep_id = item.get("depId")
            with self._lock:
                verification = self._state.verified.get(dep_id) if isinstance(dep_id, str) else None
            if isinstance(verification, dict):
                body["verification"] = {
                    "algorithm": "sha256" if verification.get("sha256") else "size",
                    "sha256": verification.get("sha256") or None,
                    "sizeBytes": verification.get("sizeBytes"),
                    "mtimeNs": str(verification.get("mtimeNs")) if verification.get("mtimeNs") is not None else None,
                    "verifiedAtMs": verification.get("verifiedAtMs"),
                    "destRelativePath": verification.get("destRelativePath"),
                    "finalPathVerified": True,
                    "agentVersion": AGENT_VERSION,
                }
        if error:
            body["error"] = error[:MAX_AGENT_ERROR_MESSAGE_CHARS]
        status, response = api_json(
            "POST",
            url,
            body=body,
            headers=self._headers(use_token=True),
            timeout_seconds=30.0,
        )
        if status == 200 and isinstance(response, dict):
            return response
        return None

    def _heartbeat(self, queue_depth: Optional[int] = None) -> None:
        if not self._resolved_instance_id:
            return
        now_ms = _now_ms()
        with self._lock:
            _, _, _, _, active_downloads = self._dependency_runtime_snapshot_locked()
        rtdb_ok = True
        if self._coordination:
            rtdb_ok = self._write_dependency_runtime_mirror(queue_depth=queue_depth)
        if self._coordination and rtdb_ok and not active_downloads and not self._coordination_http_checkpoint_due(now_ms):
            self._last_heartbeat_ms = now_ms
            return

        url = f"{self.api_base_url}/dependencies/heartbeat"
        with self._lock:
            installed_static, installed_dynamic, failed, downloading, active_downloads = self._dependency_runtime_snapshot_locked()
            dynamic_bytes_used = int(self._dynamic_bytes_used)

        body: Dict[str, Any] = {
            "instanceId": self._resolved_instance_id,
            "installedStaticDepIds": installed_static,
            "installedDynamicDepIds": installed_dynamic,
            "downloadingDepIds": downloading,
            "activeDownloads": active_downloads,
            "failedDepIds": failed,
            "diskStats": disk_stats(self.comfyui_dir),
            "diskDiagnostics": self._disk_diagnostics_payload(),
            "dynamicBytesUsed": dynamic_bytes_used,
        }
        if queue_depth is not None:
            body["queueDepth"] = int(queue_depth)

        status, resp = api_json("POST", url, body=body, headers=self._headers(use_token=True), timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected heartbeat response: {status} {resp}")
        self._maybe_queue_self_update(resp.get("agentUpdate"), "dependencies/heartbeat")
        self._coordination_note_http_checkpoint(now_ms, channel="dependencyManager")
        self._last_heartbeat_ms = now_ms

    def _fetch_queue(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self._resolved_instance_id:
            return []
        rtdb_items = self._coordination_fetch_dependency_queue(limit)
        if rtdb_items is not None:
            self._last_dependency_queue_depth = len(rtdb_items)
            return rtdb_items
        instance_id = self._resolved_instance_id
        url = f"{self.api_base_url}/dependencies/queue?instanceId={urllib.parse.quote(instance_id)}&limit={int(limit)}"
        status, resp = api_json("GET", url, headers=self._headers(use_token=True), timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected queue response: {status} {resp}")
        items = resp.get("items", [])
        if not isinstance(items, list):
            self._last_dependency_queue_depth = 0
            return []
        out: List[Dict[str, Any]] = []
        for it in items:
            if isinstance(it, dict):
                out.append(it)
        self._last_dependency_queue_depth = len(out)
        return out

    def _fetch_queue_item(self, item_id: str) -> Optional[Dict[str, Any]]:
        if not self._resolved_instance_id or not item_id:
            return None
        instance_id = self._resolved_instance_id
        url = (
            f"{self.api_base_url}/dependencies/queue-item"
            f"?instanceId={urllib.parse.quote(instance_id)}"
            f"&itemId={urllib.parse.quote(item_id)}"
        )
        status, resp = api_json("GET", url, headers=self._headers(use_token=True), timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected queue item response: {status} {resp}")
        item = resp.get("item")
        if not isinstance(item, dict):
            return None
        return item

    def _agent_effective_execute_capacity(self) -> int:
        if self.mining_only:
            return 0
        if self._post_job_comfy_recycle_active.is_set():
            return 0
        return max(1, min(int(self.agent_max_execute_workers), int(self._agent_max_concurrent_execute_jobs)))

    def _agent_effective_prefetch_capacity(self) -> int:
        return max(0, int(self._agent_max_prefetch_jobs))

    def _agent_register(self) -> None:
        if not self._resolved_instance_id:
            raise RuntimeError("Cannot register agent control channel before dependency registration resolves instanceId")
        body: Dict[str, Any] = {
            "instanceId": self._resolved_instance_id,
            "serverType": self.server_type,
            "agentVersion": AGENT_VERSION,
            "capabilities": {
                "dependencyChannel": True,
                "agentPullExecution": True,
                "dependencyDeleteFiles": True,
                "downloadTool": self._resolve_download_tool(),
                "comfyRestartProcessIsolated": self._comfy_restart_process_isolated(),
                "comfyRestartIsolationMode": self._comfy_restart_isolation_mode(),
            },
        }
        if self.instance_bootstrap_token:
            body["instanceBootstrapToken"] = self.instance_bootstrap_token
        elif self._token:
            # Reuse dependency-channel token as bootstrap proof when a dedicated
            # bootstrap token is not explicitly provided.
            body["instanceBootstrapToken"] = self._token
        if self.provider_metadata:
            body["provider"] = self.provider_metadata.get("provider")
            body["providerMetadata"] = self.provider_metadata

        resp = self._agent_api(
            "POST",
            "/agent/register",
            body=body,
            timeout_seconds=30.0,
            use_token=False,
            include_secret=True,
        )
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        token = data.get("agentAccessToken")
        token_exp = data.get("agentAccessTokenExpiresAt")
        if not isinstance(token, str) or not token:
            raise RuntimeError(f"Agent register missing agentAccessToken: {resp}")
        token_exp_ms = _iso_to_ms(token_exp if isinstance(token_exp, str) else None)
        if token_exp_ms is None:
            token_exp_ms = self._server_now_ms() + 45 * 60 * 1000

        profile = data.get("bootstrapProfile") if isinstance(data.get("bootstrapProfile"), dict) else {}
        max_concurrent = profile.get("maxConcurrentExecuteJobs")
        max_prefetch = profile.get("maxPrefetchJobs")
        if isinstance(max_concurrent, (int, float)) and max_concurrent > 0:
            self._agent_max_concurrent_execute_jobs = max(1, int(max_concurrent))
        else:
            self._agent_max_concurrent_execute_jobs = 1
        if isinstance(max_prefetch, (int, float)) and max_prefetch >= 0:
            self._agent_max_prefetch_jobs = max(0, int(max_prefetch))
        else:
            self._agent_max_prefetch_jobs = 0

        self._agent_access_token = token
        self._agent_access_token_expires_at_ms = int(token_exp_ms)
        self._agent_bootstrap_profile = profile
        self._agent_channel_supported = True
        self._set_coordination_from_response(data, "/agent/register")
        self._apply_agent_runtime_config(data.get("agentRuntimeConfig"), "/agent/register")
        self._maybe_fetch_runtime_env_delivery("/agent/register")
        self._maybe_queue_self_update(data.get("agentUpdate"), "/agent/register")
        self._last_agent_update_check_ms = _now_ms()
        logging.info(
            "Registered agent control channel: instanceId=%s maxConcurrentExecuteJobs=%d maxPrefetchJobs=%d tokenExpiresAt=%s",
            self._resolved_instance_id,
            int(self._agent_max_concurrent_execute_jobs),
            int(self._agent_max_prefetch_jobs),
            token_exp if isinstance(token_exp, str) else "-",
        )

    def _maybe_register_agent_control(self) -> None:
        if not self.agent_control_enabled or not self._agent_channel_supported:
            return
        if not self._resolved_instance_id:
            return
        now = self._server_now_ms()
        if now < int(self._next_agent_register_attempt_ms):
            return
        if not self._agent_token_needs_refresh():
            return

        try:
            self._agent_register()
        except ApiError as e:
            if e.status in (404, 405):
                self._agent_channel_supported = False
                logging.warning("Agent control channel unavailable (status=%d). Continuing in legacy dependency-only mode.", e.status)
                return
            self._next_agent_register_attempt_ms = now + 30_000
            if e.status == 401:
                logging.warning("Agent register unauthorized (status=401). Check shared secret/bootstrap token.")
            else:
                logging.warning("Agent register failed (status=%d): %s", e.status, e)
        except Exception as e:
            self._next_agent_register_attempt_ms = now + 30_000
            logging.warning("Agent register failed: %s", e)

    def _agent_fetch_queue(self, limit: int, wait_sec: Optional[int] = None) -> List[Dict[str, Any]]:
        if not self._resolved_instance_id:
            return []
        if not self._agent_access_token:
            return []
        rtdb_items = self._coordination_fetch_agent_queue(limit)
        if rtdb_items is not None:
            return rtdb_items
        wait_value = self.agent_queue_wait_sec if wait_sec is None else wait_sec
        resp = self._agent_api(
            "GET",
            "/agent/queue",
            query={
                "instanceId": self._resolved_instance_id,
                "limit": max(1, min(20, int(limit))),
                "waitSec": max(0, min(20, int(wait_value))),
            },
            timeout_seconds=max(30.0, float(wait_value) + 10.0),
            use_token=True,
            include_secret=True,
        )
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        items = data.get("items")
        if not isinstance(items, list):
            return []
        out: List[Dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                out.append(item)
        return out

    def _agent_fetch_queue_item(self, item_id: str, lease_id: str) -> Optional[Dict[str, Any]]:
        if not self._resolved_instance_id or not self._agent_access_token:
            return None
        if not item_id or not lease_id:
            return None
        resp = self._agent_api(
            "GET",
            "/agent/queue-item",
            query={
                "instanceId": self._resolved_instance_id,
                "itemId": item_id,
                "leaseId": lease_id,
            },
            timeout_seconds=30.0,
            use_token=True,
            include_secret=True,
        )
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        item = data.get("item")
        return item if isinstance(item, dict) else None

    def _agent_ack(
        self,
        item_id: str,
        lease_id: str,
        ack_type: str,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        tuple_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self._resolved_instance_id:
            raise RuntimeError("Cannot ack without resolved instanceId")
        body: Dict[str, Any] = {
            "schemaVersion": 1,
            "instanceId": self._resolved_instance_id,
            "itemId": item_id,
            "leaseId": lease_id,
            "ackType": ack_type,
        }
        if error_code:
            body["errorCode"] = str(error_code)[:120]
        if error_message:
            body["errorMessage"] = str(error_message)[:MAX_AGENT_ERROR_MESSAGE_CHARS]
        if tuple_fields:
            for key in ("jobId", "executionAttempt", "attemptEpoch"):
                if key in tuple_fields:
                    body[key] = tuple_fields[key]
        resp = self._agent_api("POST", "/agent/ack", body=body, timeout_seconds=30.0, use_token=True, include_secret=True)
        return resp.get("data") if isinstance(resp.get("data"), dict) else {}

    def _agent_event(
        self,
        lease: AgentExecuteLease,
        event_version: int,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self._resolved_instance_id:
            raise RuntimeError("Cannot emit event without resolved instanceId")
        event_payload = dict(payload) if isinstance(payload, dict) else {}
        if (
            self._coalesce_agent_lifecycle_events(lease)
            and int(lease.prompt_submitted_at_ms or 0) > 0
            and event_type in {
                "execution_progress",
                "output_commit_started",
                "job_completed",
                "job_failed",
                "job_cancelled",
            }
        ):
            event_payload.setdefault("coalescedLifecycle", self._coalesced_agent_lifecycle_payload(lease))

        body: Dict[str, Any] = {
            "schemaVersion": 1,
            "instanceId": self._resolved_instance_id,
            "jobId": lease.job_id,
            "executionAttempt": lease.execution_attempt,
            "attemptEpoch": lease.attempt_epoch,
            "leaseId": lease.lease_id,
            "eventVersion": int(event_version),
            "eventType": event_type,
        }
        if event_payload:
            for k, v in event_payload.items():
                body[k] = v

        digest_body = {k: v for k, v in body.items() if k != "eventDigest"}
        body["eventDigest"] = _sha256_hex_bytes(_canonical_json_bytes(digest_body))

        resp = self._agent_api("POST", "/agent/event", body=body, timeout_seconds=30.0, use_token=True, include_secret=True)
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        accepted = data.get("accepted")
        reason = data.get("reason")
        if accepted is not True and isinstance(reason, str):
            logging.info(
                "Agent event not accepted: jobId=%s type=%s version=%d reason=%s",
                lease.job_id,
                event_type,
                int(event_version),
                reason,
            )
        return data

    def _emit_agent_event(self, lease: AgentExecuteLease, event_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            active = self._active_exec_by_item.get(lease.item_id)
            if active:
                active.event_version += 1
                event_version = int(active.event_version)
            else:
                lease.event_version += 1
                event_version = int(lease.event_version)
        return self._agent_event(lease, event_version, event_type, payload=payload)

    def _coalesce_agent_lifecycle_events(self, lease: AgentExecuteLease) -> bool:
        policy = lease.payload.get("eventPolicy") if isinstance(lease.payload, dict) else None
        return bool(isinstance(policy, dict) and policy.get("coalesceLifecycle") is True)

    def _coalesced_agent_lifecycle_payload(self, lease: AgentExecuteLease) -> Dict[str, Any]:
        now_ms = _now_ms()
        inputs_ready_at_ms = int(lease.ready_at_ms or lease.execute_started_at_ms or now_ms)
        prompt_submitted_at_ms = max(
            inputs_ready_at_ms,
            int(lease.prompt_submitted_at_ms or lease.execute_started_at_ms or now_ms),
        )
        execution_started_at_ms = max(
            prompt_submitted_at_ms,
            int(lease.execute_started_at_ms or now_ms),
        )
        return {
            "version": 1,
            "capturedAtMs": now_ms,
            "inputsReadyAtMs": inputs_ready_at_ms,
            "promptSubmittedAtMs": prompt_submitted_at_ms,
            "executionStartedAtMs": execution_started_at_ms,
        }

    def _emit_agent_event_best_effort(self, lease: AgentExecuteLease, event_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
        try:
            self._emit_agent_event(lease, event_type, payload)
        except Exception as e:
            logging.debug("Best-effort agent event failed: jobId=%s type=%s err=%s", lease.job_id, event_type, e)

    def _emit_agent_event_durable(self, lease: AgentExecuteLease, event_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        emit_override = self.__dict__.get("_emit_agent_event")
        if callable(emit_override):
            result = emit_override(lease, event_type, payload)
            return result if isinstance(result, dict) else {}

        with self._lock:
            active = self._active_exec_by_item.get(lease.item_id)
            if active:
                active.event_version += 1
                event_version = int(active.event_version)
            else:
                lease.event_version += 1
                event_version = int(lease.event_version)

        attempts = max(1, int(getattr(self, "agent_terminal_event_retry_attempts", 1) or 1))
        last_error: Optional[Exception] = None
        for attempt_idx in range(attempts):
            try:
                result = self._agent_event(lease, event_version, event_type, payload=payload)
                if result.get("accepted") is not True:
                    raise RuntimeError(
                        f"Durable agent event {event_type} was not accepted: {result.get('reason') or result}"
                    )
                return result
            except Exception as e:
                last_error = e
                if attempt_idx >= attempts - 1 or not self._is_retryable_agent_control_error(e):
                    break
                self._sleep_agent_api_retry(f"terminal event {event_type}", attempt_idx, e)
        if last_error:
            raise last_error
        raise RuntimeError(f"Durable agent event {event_type} failed without an error")

    def _collect_active_leases(self) -> List[Dict[str, Any]]:
        with self._lock:
            active = list(self._active_exec_by_item.values())
            maintenance = list(self._active_maintenance_by_item.values())
        out: List[Dict[str, Any]] = []
        for lease in active:
            out.append(
                {
                    "itemId": lease.item_id,
                    "leaseId": lease.lease_id,
                    "jobId": lease.job_id,
                    "executionAttempt": lease.execution_attempt,
                    "attemptEpoch": lease.attempt_epoch,
                    "stage": lease.stage,
                    **({"readyAgeMs": max(0, _now_ms() - int(lease.ready_at_ms))} if int(lease.ready_at_ms or 0) > 0 and lease.stage == "ready" else {}),
                    **({"gpuRetryAfterMs": int(lease.gpu_retry_after_ms)} if int(lease.gpu_retry_after_ms or 0) > _now_ms() and lease.stage == "ready" else {}),
                    **({"uploadWorkerQueueMs": max(0, _now_ms() - int(lease.upload_enqueued_at_ms))} if int(lease.upload_enqueued_at_ms or 0) > 0 and lease.stage == "uploading" else {}),
                }
            )
        for lease in maintenance:
            item_id = lease.get("itemId")
            lease_id = lease.get("leaseId")
            if not isinstance(item_id, str) or not item_id or not isinstance(lease_id, str) or not lease_id:
                continue
            out.append(
                {
                    "itemId": item_id,
                    "leaseId": lease_id,
                    "jobId": "",
                    "executionAttempt": None,
                    "attemptEpoch": None,
                    "stage": lease.get("stage") if isinstance(lease.get("stage"), str) else "maintenance",
                }
            )
        return out

    def _mark_cancel_signal(self, signal: Dict[str, Any]) -> None:
        if not isinstance(signal, dict):
            return
        job_id = signal.get("jobId")
        execution_attempt = signal.get("executionAttempt")
        attempt_epoch = signal.get("attemptEpoch")
        lease_id = signal.get("leaseId")
        if not isinstance(job_id, str) or not job_id:
            return
        if not isinstance(execution_attempt, (int, float)) or not isinstance(attempt_epoch, (int, float)) or not isinstance(lease_id, str):
            return
        reason = signal.get("reason") if isinstance(signal.get("reason"), str) else "cancel_requested"

        with self._lock:
            for lease in self._active_exec_by_item.values():
                if (
                    lease.job_id == job_id
                    and int(lease.execution_attempt) == int(execution_attempt)
                    and int(lease.attempt_epoch) == int(attempt_epoch)
                    and lease.lease_id == lease_id
                ):
                    if not lease.cancel_requested:
                        logging.info(
                            "Received cancel signal for job=%s attempt=%d epoch=%d lease=%s",
                            lease.job_id,
                            int(lease.execution_attempt),
                            int(lease.attempt_epoch),
                            lease.lease_id,
                        )
                    lease.cancel_requested = True
                    lease.cancel_reason = reason

    def _agent_heartbeat(self) -> Dict[str, Any]:
        if not self._resolved_instance_id or not self._agent_access_token:
            return {}

        self._maybe_fetch_runtime_env_delivery("/agent/heartbeat")

        held_leases = self._collect_active_leases()
        local_comfy = True if self.mining_only else self._local_comfy_reachable()
        readiness_present = True if self.mining_only else self._local_readiness_file_present()
        node_contract = {
            "ready": True,
            "requiredClassCount": 0,
            "missingClassTypes": [],
            "affectedBundleIds": [],
            "checkedAtMs": _now_ms(),
        } if self.mining_only else self._local_node_contract_runtime(force=False)
        queue_depth = len(held_leases)
        queue_summary = {} if self.mining_only else self._local_comfy_queue_summary(timeout_seconds=5.0)
        input_cache_inventory = self._collect_input_cache_inventory()
        stage_counts = self._agent_stage_counts_payload()
        memory_telemetry = collect_cgroup_memory_telemetry()
        comfy_runtime = {} if self.mining_only else self._comfy_runtime_snapshot()
        ssh_host_key_sha256 = collect_ssh_host_key_sha256()

        body: Dict[str, Any] = {
            "schemaVersion": 1,
            "instanceId": self._resolved_instance_id,
            "localComfyReachable": bool(local_comfy),
            "localReadinessFilePresent": bool(readiness_present),
            "localReadinessFile": self.agent_local_readiness_file,
            "nodeContract": node_contract,
            "queueDepth": int(queue_depth),
            **({"queueSummary": queue_summary} if queue_summary else {}),
            "stageCounts": stage_counts,
            "heldLeases": held_leases,
            "runningItemIds": [row["itemId"] for row in held_leases if isinstance(row.get("itemId"), str)],
            "maxConcurrentExecuteJobs": int(self._agent_effective_execute_capacity()),
            "maxPrefetchJobs": int(self._agent_effective_prefetch_capacity()),
            "maxUploadJobs": int(self.agent_max_upload_workers),
            "inputCacheKeys": input_cache_inventory.get("keys", []),
            "inputCacheKeyCount": int(input_cache_inventory.get("keyCount", 0)),
            "inputCacheBytesUsed": int(input_cache_inventory.get("bytesUsed", 0)),
            "inputCacheMaxBytes": int(input_cache_inventory.get("maxBytes", 0)),
            "inputCacheInventoryTruncated": bool(input_cache_inventory.get("inventoryTruncated")),
            **({"memoryTelemetry": memory_telemetry} if memory_telemetry else {}),
            **({"comfyRuntime": comfy_runtime} if comfy_runtime else {}),
            **({"sshHostKeySha256": ssh_host_key_sha256} if ssh_host_key_sha256 else {}),
            "idleMining": self._idle_prl_miner.snapshot(),
            "gpuCoordinator": self._gpu_coordinator_runtime_snapshot(),
            "agentVersion": AGENT_VERSION,
            "capabilities": {
                "dependencyChannel": True,
                "agentPullExecution": not self.mining_only,
                "dependencyDeleteFiles": True,
                "idlePrlMining": True,
                "miningOnly": bool(self.mining_only),
                "comfyRestartProcessIsolated": self._comfy_restart_process_isolated(),
                "comfyRestartIsolationMode": self._comfy_restart_isolation_mode(),
                "runtimeEnvDeliveryV1": True,
                "runtimeEnvDeliveryId": self._runtime_env_delivery_id,
                "runtimeEnvAppliedKeys": list(self._runtime_env_applied_keys),
            },
        }
        now_ms = _now_ms()
        transition_signature = self._agent_runtime_transition_signature(body)
        has_active_agent_work = len(held_leases) > 0
        rtdb_ok = True
        if self._coordination:
            rtdb_ok = self._write_agent_runtime_mirror(body=body, transition_signature=transition_signature)
            if rtdb_ok and has_active_agent_work:
                self._coordination_check_active_lease_cancels(held_leases)
        rtdb_lease_heartbeat = self._coordination_feature_enabled("agentLeaseHeartbeatV1")
        agent_update_check_due = (
            now_ms - int(self._last_agent_update_check_ms) >= int(self.agent_update_check_seconds * 1000)
        )
        if (
            self._coordination
            and rtdb_ok
            and (not has_active_agent_work or rtdb_lease_heartbeat)
            and not agent_update_check_due
            and not self._coordination_http_checkpoint_due(now_ms, channel="agentControl")
            and transition_signature == self._last_agent_http_transition_signature
        ):
            self._last_agent_heartbeat_ms = now_ms
            return {}

        resp = self._agent_api("POST", "/agent/heartbeat", body=body, timeout_seconds=30.0, use_token=True, include_secret=True)
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        self._apply_agent_runtime_config(data.get("agentRuntimeConfig"), "/agent/heartbeat")
        self._maybe_queue_self_update(data.get("agentUpdate"), "/agent/heartbeat")
        self._coordination_note_http_checkpoint(now_ms, channel="agentControl")
        self._last_agent_http_transition_signature = transition_signature
        self._last_agent_heartbeat_ms = _now_ms()
        self._last_agent_update_check_ms = self._last_agent_heartbeat_ms

        lease_results = data.get("leases")
        stale_pre_execution: List[AgentExecuteLease] = []
        if isinstance(lease_results, list):
            for row in lease_results:
                if not isinstance(row, dict):
                    continue
                item_id = row.get("itemId")
                result = row.get("result")
                if isinstance(item_id, str) and isinstance(result, str) and result == "stale":
                    with self._lock:
                        lease = self._active_exec_by_item.get(item_id)
                        if lease:
                            lease.cancel_requested = True
                            lease.cancel_reason = "lease_stale"
                            if lease.stage in ("leased", "ready", "waiting_dependencies"):
                                stale_pre_execution.append(lease)

        # Do not feed a backend-rejected ready lease through the GPU executor
        # merely to discover its cancel flag. Prefetching leases are left to
        # their worker so temporary files cannot be removed beneath an active IO.
        for lease in stale_pre_execution:
            self._cleanup_agent_lease(lease)

        cancel_signals = data.get("cancelSignals")
        if isinstance(cancel_signals, list):
            for signal in cancel_signals:
                if isinstance(signal, dict):
                    self._mark_cancel_signal(signal)
        return data

    def _register_active_lease(self, lease: AgentExecuteLease) -> None:
        with self._lock:
            self._active_exec_by_item[lease.item_id] = lease

    def _prepare_logical_execute_lease(
        self,
        item_id: str,
        job_id: str,
        execution_attempt: int,
        attempt_epoch: int,
    ) -> str:
        """Enforce one progressing lease per logical job attempt.

        Equal/older duplicate deliveries are ignored locally without ACKing the
        current tuple (the backend's ignored-stale ACK intentionally requeues a
        current dispatched job). A newer epoch cancels older local work; normal
        capacity accounting keeps the replacement from executing concurrently
        while an older prompt is still winding down.
        """
        with self._lock:
            if item_id in self._active_exec_by_item:
                return "duplicate_item"
            matches = [
                lease
                for lease in self._active_exec_by_item.values()
                if lease.job_id == job_id and int(lease.execution_attempt) == int(execution_attempt)
            ]
            if not matches:
                return "accept"
            newest_epoch = max(int(lease.attempt_epoch) for lease in matches)
            if int(attempt_epoch) <= newest_epoch:
                return "stale_logical_lease" if int(attempt_epoch) < newest_epoch else "duplicate_logical_lease"
            for lease in matches:
                lease.cancel_requested = True
                lease.cancel_reason = "superseded_by_newer_attempt_epoch"
            return "accept_superseding"

    def _finish_active_lease(self, item_id: str) -> None:
        with self._lock:
            self._active_exec_by_item.pop(item_id, None)
            try:
                self._ready_agent_item_ids.remove(item_id)
            except ValueError:
                pass
        # A slot just freed. Poll locally for any backlog item instead of relying
        # on the server to bump our own agentQueue signal (a self-wake the server
        # suppresses when AGENT_SELF_WAKE_QUEUE_SIGNAL_ENABLED=false). Newly
        # server-assigned work still arrives via its own dispatch signal.
        self._request_agent_queue_poll()

    def _request_agent_queue_poll(self) -> None:
        self._agent_poll_wakeup.set()
        self._loop_wakeup.set()

    def _agent_stage_counts_locked(self) -> Tuple[int, int, int]:
        counts = self._agent_stage_counts_map_locked()
        execute_count = int(counts.get("executing", 0))
        prefetch_count = sum(int(counts.get(stage, 0)) for stage in ("leased", "prefetching", "ready", "waiting_dependencies", "preparing_prompt"))
        upload_count = int(counts.get("uploading", 0))
        return execute_count, prefetch_count, upload_count

    def _agent_stage_counts_map_locked(self) -> Dict[str, int]:
        counts: Dict[str, int] = {
            "leased": 0,
            "prefetching": 0,
            "ready": 0,
            "waiting_dependencies": 0,
            "preparing_prompt": 0,
            "executing": 0,
            "uploading": 0,
            "finalizing": 0,
        }
        for lease in self._active_exec_by_item.values():
            stage = lease.stage if lease.stage in counts else "leased"
            counts[stage] += 1
        return counts

    def _agent_stage_counts_payload(self) -> Dict[str, int]:
        with self._lock:
            counts = dict(self._agent_stage_counts_map_locked())
            ready_ages = [
                max(0, _now_ms() - int(lease.ready_at_ms))
                for lease in self._active_exec_by_item.values()
                if lease.stage == "ready" and int(lease.ready_at_ms or 0) > 0
            ]
        counts["checkedAtMs"] = _now_ms()
        upload_capacity = max(0, int(getattr(self, "agent_max_upload_workers", 0) or 0))
        counts["uploadWorkerCapacity"] = upload_capacity
        counts["uploadBacklog"] = max(0, int(counts.get("uploading", 0)) - upload_capacity)
        counts["readyMaxAgeMs"] = max(ready_ages) if ready_ages else 0
        return counts

    def _next_agent_lease_order(self) -> int:
        with self._lock:
            self._agent_lease_order += 1
            return int(self._agent_lease_order)

    def _enqueue_ready_locked(self, lease: AgentExecuteLease) -> None:
        try:
            self._ready_agent_item_ids.remove(lease.item_id)
        except ValueError:
            pass
        inserted = False
        for idx, item_id in enumerate(self._ready_agent_item_ids):
            other = self._active_exec_by_item.get(item_id)
            other_order = int(other.lease_order) if other else 0
            if other is None or other.stage != "ready" or int(lease.lease_order) < other_order:
                self._ready_agent_item_ids.insert(idx, lease.item_id)
                inserted = True
                break
        if not inserted:
            self._ready_agent_item_ids.append(lease.item_id)

    def _pop_next_ready_lease(self) -> Optional[AgentExecuteLease]:
        with self._lock:
            candidates = len(self._ready_agent_item_ids)
            now_ms = _now_ms()
            while self._ready_agent_item_ids and candidates > 0:
                candidates -= 1
                item_id = self._ready_agent_item_ids.popleft()
                lease = self._active_exec_by_item.get(item_id)
                if not lease or lease.stage != "ready":
                    continue
                if int(lease.gpu_retry_after_ms or 0) > now_ms:
                    self._ready_agent_item_ids.append(item_id)
                    continue
                lease.stage = "preparing_prompt"
                lease.gpu_retry_after_ms = 0
                return lease
        return None

    def _defer_ready_lease_for_gpu_coordinator(
        self,
        lease: AgentExecuteLease,
        retry_after_seconds: float,
        reason: str,
    ) -> None:
        retry_ms = int(max(0.25, min(60.0, float(retry_after_seconds))) * 1000)
        with self._lock:
            active = self._active_exec_by_item.get(lease.item_id)
            if active is None:
                return
            active.stage = "ready"
            active.gpu_retry_after_ms = _now_ms() + retry_ms
            self._enqueue_ready_locked(active)
        logging.info(
            "Deferred Comfy execution for GPU coordinator itemId=%s jobId=%s retryMs=%d reason=%s",
            lease.item_id,
            lease.job_id,
            retry_ms,
            reason,
        )
        self._request_agent_queue_poll()

    def _drain_ready_agent_leases(self) -> int:
        if self._agent_execute_executor is None:
            return 0
        execute_capacity = self._agent_effective_execute_capacity()
        with self._lock:
            active_execute_count, _prefetch_count_unused, _upload_count_unused = self._agent_stage_counts_locked()
        submitted = 0
        while active_execute_count < execute_capacity:
            ready_lease = self._pop_next_ready_lease()
            if ready_lease is None:
                break
            self._submit_agent_execute(ready_lease)
            active_execute_count += 1
            submitted += 1
        return submitted

    def _is_cancel_requested(self, lease: AgentExecuteLease) -> bool:
        with self._lock:
            active = self._active_exec_by_item.get(lease.item_id)
            return bool(active.cancel_requested) if active else False

    def _mark_cancel_by_item_id(self, item_id: str, reason: str = "cancel_requested") -> None:
        with self._lock:
            lease = self._active_exec_by_item.get(item_id)
            if lease:
                lease.cancel_requested = True
                lease.cancel_reason = reason

    def _cleanup_agent_lease(self, lease: AgentExecuteLease) -> None:
        tmp_root = Path(lease.tmp_root) if isinstance(lease.tmp_root, str) and lease.tmp_root else None
        self._release_comfy_gpu_lease(lease, "execute_job_cleanup", keep_warm=False)
        self._finish_active_lease(lease.item_id)
        self._resume_idle_prl_mining_if_idle("execute_job_complete")
        self._request_agent_queue_poll()
        if tmp_root is not None:
            try:
                shutil.rmtree(str(tmp_root), ignore_errors=True)
            except Exception:
                pass

    def _invalidate_dependency_verification_locked(self, dep_id: str, mark_failed: bool = False) -> None:
        self._state.verified.pop(dep_id, None)
        self._session_hash_verified_dep_ids.discard(dep_id)
        self._state.installed_static.discard(dep_id)
        self._state.installed_dynamic.discard(dep_id)
        previous_lru = self._state.lru.pop(dep_id, None)
        previous_size = previous_lru.get("sizeBytes") if isinstance(previous_lru, dict) else 0
        if isinstance(previous_size, int) and previous_size > 0:
            self._dynamic_bytes_used = max(0, int(getattr(self, "_dynamic_bytes_used", 0)) - previous_size)
        if mark_failed:
            self._state.failed.add(dep_id)

    def _record_dependency_verification_locked(
        self,
        dep_id: str,
        dest_rel: str,
        dest_abs: Path,
        sha256_value: str,
    ) -> Dict[str, Any]:
        stat_result = dest_abs.stat()
        record = {
            "destRelativePath": dest_rel,
            "sha256": str(sha256_value or "").strip().lower(),
            "sizeBytes": int(stat_result.st_size),
            "mtimeNs": int(stat_result.st_mtime_ns),
            "verifiedAtMs": _now_ms(),
        }
        self._state.verified[dep_id] = record
        self._session_hash_verified_dep_ids.add(dep_id)
        return dict(record)

    @staticmethod
    def _verification_record_matches_stat(record: Dict[str, Any], dest_rel: str, stat_result: os.stat_result) -> bool:
        return (
            record.get("destRelativePath") == dest_rel
            and record.get("sizeBytes") == int(stat_result.st_size)
            and record.get("mtimeNs") == int(stat_result.st_mtime_ns)
        )

    def _verify_resolved_dependency_file(
        self,
        item: Dict[str, Any],
        *,
        require_full_hash: bool,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> bool:
        dep_id = item.get("depId")
        resolved = item.get("resolved")
        if not isinstance(dep_id, str) or not dep_id or not isinstance(resolved, dict):
            return False
        dest_rel = resolved.get("destRelativePath")
        if not isinstance(dest_rel, str) or not dest_rel:
            return False
        dest_abs = safe_join(self.comfyui_dir, dest_rel)
        expected_size_raw = resolved.get("expectedSizeBytes")
        expected_size = int(expected_size_raw) if isinstance(expected_size_raw, (int, float)) and expected_size_raw > 0 else 0
        expected_sha = str(resolved.get("sha256") or "").strip().lower()

        verification_lock = getattr(self, "_dependency_verification_lock", None)
        if verification_lock is None:
            verification_lock = threading.Lock()
            self._dependency_verification_lock = verification_lock
        with verification_lock:
            try:
                stat_result = dest_abs.stat()
            except OSError:
                with self._lock:
                    self._invalidate_dependency_verification_locked(dep_id)
                    self._save_state()
                return False
            if not dest_abs.is_file() or (expected_size > 0 and int(stat_result.st_size) != expected_size):
                with self._lock:
                    self._invalidate_dependency_verification_locked(dep_id)
                    self._save_state()
                return False

            with self._lock:
                record = self._state.verified.get(dep_id)
                record_matches = isinstance(record, dict) and self._verification_record_matches_stat(record, dest_rel, stat_result)
                sha_matches_record = isinstance(record, dict) and str(record.get("sha256") or "").lower() == expected_sha
                session_verified = dep_id in self._session_hash_verified_dep_ids
            if record_matches and sha_matches_record and (not expected_sha or not require_full_hash or session_verified):
                return True

            actual_sha = sha256_file(dest_abs, progress_cb=progress_cb) if expected_sha else ""
            if expected_sha and actual_sha.lower() != expected_sha:
                logging.error(
                    "Dependency integrity check failed: depId=%s expected=%s got=%s path=%s",
                    dep_id,
                    expected_sha,
                    actual_sha,
                    str(dest_abs),
                )
                with self._lock:
                    self._invalidate_dependency_verification_locked(dep_id, mark_failed=True)
                    self._save_state()
                return False

            with self._lock:
                self._record_dependency_verification_locked(dep_id, dest_rel, dest_abs, actual_sha or expected_sha)
                self._save_state()
            return True

    def _verified_installed_dep_ids_for_execution(self, required_dep_ids: Iterable[str]) -> Set[str]:
        ready: Set[str] = set()
        for dep_id in dict.fromkeys(required_dep_ids):
            if not isinstance(dep_id, str) or not dep_id:
                continue
            with self._lock:
                installed = dep_id in self._state.installed_static or dep_id in self._state.installed_dynamic
                record = self._state.verified.get(dep_id)
                downloading = dep_id in self._downloading
            if not installed or downloading or not isinstance(record, dict):
                continue
            item = {
                "depId": dep_id,
                "resolved": {
                    "destRelativePath": record.get("destRelativePath"),
                    "expectedSizeBytes": record.get("sizeBytes"),
                    "sha256": record.get("sha256"),
                },
            }
            if self._verify_resolved_dependency_file(item, require_full_hash=True):
                ready.add(dep_id)
        return ready

    def _current_installed_dep_ids(self) -> Set[str]:
        with self._lock:
            self._reconcile_lru_locked()
            out = set(self._state.installed_static)
            out.update(self._state.installed_dynamic)
            out.difference_update(self._downloading)
        return out

    def _comfy_api_json(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 30.0,
    ) -> Tuple[int, Optional[Any]]:
        ep = endpoint if endpoint.startswith("/") else "/" + endpoint
        base_url = self._resolve_local_comfy_base_url(force_refresh=False, timeout_seconds=min(5.0, timeout_seconds))
        try:
            return api_json(method, f"{base_url}{ep}", body=body, timeout_seconds=timeout_seconds)
        except Exception as first_error:
            refreshed = self._resolve_local_comfy_base_url(force_refresh=True, timeout_seconds=min(5.0, timeout_seconds))
            if refreshed == base_url:
                raise
            logging.info(
                "Retrying ComfyUI %s %s after local base URL refresh: %s -> %s",
                method.upper(),
                ep,
                base_url,
                refreshed,
            )
            try:
                return api_json(method, f"{refreshed}{ep}", body=body, timeout_seconds=timeout_seconds)
            except Exception:
                raise first_error

    def _comfy_submit_prompt(
        self,
        workflow: Dict[str, Any],
        client_id: str,
        prompt_id: Optional[str] = None,
    ) -> str:
        body: Dict[str, Any] = {"prompt": workflow, "client_id": client_id}
        if isinstance(prompt_id, str) and prompt_id:
            body["prompt_id"] = prompt_id
        status, resp = self._comfy_api_json(
            "POST",
            "/prompt",
            body=body,
            timeout_seconds=60.0,
        )
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected /prompt response: {status} {resp}")
        prompt_id = resp.get("prompt_id")
        if not isinstance(prompt_id, str) or not prompt_id:
            raise RuntimeError(f"/prompt did not return prompt_id: {resp}")
        requested_prompt_id = body.get("prompt_id")
        if isinstance(requested_prompt_id, str) and prompt_id != requested_prompt_id:
            logging.warning(
                "ComfyUI ignored requested prompt_id; node timing will be unavailable for this job: expected=%s actual=%s",
                requested_prompt_id,
                prompt_id,
            )
        return prompt_id

    def _start_comfy_node_timing_collector(
        self,
        workflow: Dict[str, Any],
        client_id: str,
        prompt_id: str,
    ) -> Optional[ComfyNodeTimingCollector]:
        if not bool(getattr(self, "comfy_node_timing_enabled", False)):
            return None
        try:
            base_url = self._resolve_local_comfy_base_url(force_refresh=False, timeout_seconds=3.0)
            collector = ComfyNodeTimingCollector(
                base_url=base_url,
                client_id=client_id,
                prompt_id=prompt_id,
                workflow=workflow,
                max_rows=int(getattr(self, "comfy_node_timing_max_rows", MAX_COMFY_NODE_TIMING_ROWS)),
            )
            if collector.start(timeout_seconds=3.0):
                return collector
        except Exception as exc:
            logging.debug("Comfy node timing setup failed open for prompt %s: %s", prompt_id, exc)
        return None

    def _comfy_get_history(self, prompt_id: str) -> Dict[str, Any]:
        status, resp = self._comfy_api_json("GET", f"/history/{urllib.parse.quote(prompt_id)}", timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected /history response: {status} {resp}")
        entry = resp.get(prompt_id)
        if isinstance(entry, dict):
            return entry
        # Some ComfyUI builds return the history entry directly for /history/<prompt_id>.
        if isinstance(resp.get("outputs"), dict):
            return resp
        return entry if isinstance(entry, dict) else {}

    def _comfy_interrupt(self) -> None:
        try:
            self._comfy_api_json("POST", "/interrupt", body={}, timeout_seconds=10.0)
        except Exception as e:
            logging.debug("Comfy interrupt failed: %s", e)

    def _parse_workflow_from_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        workflow_ref = payload.get("workflowRef")
        if not isinstance(workflow_ref, dict):
            raise RuntimeError("execute_job payload missing workflowRef")
        mode = workflow_ref.get("mode")
        if mode == "inline":
            inline_json = workflow_ref.get("inlineJson")
            if isinstance(inline_json, str) and inline_json.strip():
                parsed = json.loads(inline_json)
                if isinstance(parsed, dict):
                    return parsed
                raise RuntimeError("inline workflow is not a JSON object")
            if isinstance(inline_json, dict):
                return inline_json
            raise RuntimeError("inline workflow missing inlineJson")
        if mode in ("download_url", "url"):
            download_url = workflow_ref.get("downloadUrl")
            if not isinstance(download_url, str) or not download_url.strip():
                raise RuntimeError("download_url workflow missing downloadUrl")
            expected_size_raw = workflow_ref.get("expectedSizeBytes")
            expected_size = int(expected_size_raw) if isinstance(expected_size_raw, (int, float)) and expected_size_raw > 0 else 0
            if expected_size > 32 * 1024 * 1024:
                raise RuntimeError(f"download_url workflow too large: {expected_size} bytes")
            expected_sha = workflow_ref.get("sha256")
            expected_sha_norm = expected_sha.strip().lower() if isinstance(expected_sha, str) and re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha.strip()) else ""
            cache_name = f"{expected_sha_norm or uuid.uuid4().hex}.json"
            cache_path = self.workspace / "workflow_payloads" / cache_name

            cache_valid = False
            if cache_path.exists():
                try:
                    if expected_size > 0 and int(cache_path.stat().st_size) != expected_size:
                        cache_valid = False
                    elif expected_sha_norm and sha256_file(cache_path).lower() != expected_sha_norm:
                        cache_valid = False
                    else:
                        cache_valid = True
                except Exception:
                    cache_valid = False

            if not cache_valid:
                tmp_path = cache_path.with_suffix(".tmp")
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                http_download_to_file(
                    download_url.strip(),
                    tmp_path,
                    timeout_seconds=max(30.0, min(float(self.download_timeout_seconds), 300.0)),
                    chunk_size=int(self.download_chunk_size),
                    user_agent=f"{AGENT_VERSION} workflow-fetch",
                )
                if expected_size > 0 and int(tmp_path.stat().st_size) != expected_size:
                    raise RuntimeError(
                        f"workflow download size mismatch: expected {expected_size} got {int(tmp_path.stat().st_size)}"
                    )
                if expected_sha_norm:
                    actual_sha = sha256_file(tmp_path).lower()
                    if actual_sha != expected_sha_norm:
                        raise RuntimeError(f"workflow download checksum mismatch: expected {expected_sha_norm} got {actual_sha}")
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(str(tmp_path), str(cache_path))

            raw = cache_path.read_text("utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            raise RuntimeError("downloaded workflow is not a JSON object")
        raise RuntimeError(f"Unsupported workflowRef.mode: {mode}")

    def _workflow_uses_class_type(self, workflow: Dict[str, Any], class_type: str) -> bool:
        wanted = str(class_type or "").strip()
        if not wanted:
            return False
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") == wanted:
                return True
        return False

    def _ensure_runtime_assets_for_workflow(self, workflow: Dict[str, Any]) -> None:
        if self._workflow_uses_class_type(workflow, "RIFEInterpolation"):
            self._ensure_comfyui_vfi_rife_model()

    def _input_cache_expected_size_bytes(self, row: Dict[str, Any]) -> int:
        for key in ("expectedSizeBytes", "sizeBytes", "bytes"):
            value = row.get(key)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
        return 0

    def _canonical_input_source(self, row: Dict[str, Any], name: str) -> str:
        bucket = row.get("sourceBucket")
        object_path = row.get("sourceObjectPath")
        if isinstance(bucket, str) and bucket and isinstance(object_path, str) and object_path:
            return f"gs://{bucket}/{object_path}"

        raw_url = row.get("downloadUrl")
        if isinstance(raw_url, str) and raw_url:
            try:
                parsed = urllib.parse.urlparse(raw_url)
                return urllib.parse.urlunparse(
                    (
                        parsed.scheme.lower(),
                        parsed.netloc.lower(),
                        parsed.path,
                        "",
                        "",
                        "",
                    )
                )
            except Exception:
                return raw_url
        return name

    def _input_cache_key(self, row: Dict[str, Any], name: str) -> str:
        provided_cache_key = row.get("cacheKey")
        if isinstance(provided_cache_key, str) and provided_cache_key.strip():
            normalized = provided_cache_key.strip().lower()
            if re.fullmatch(r"(?:url|sha256|key)_[0-9a-f]{64}", normalized):
                return normalized
            digest = _sha256_hex_bytes(_canonical_json_bytes({"cacheKey": provided_cache_key.strip(), "name": name}))
            return f"key_{digest}"
        expected_sha = row.get("sha256")
        if isinstance(expected_sha, str) and expected_sha:
            return f"sha256_{expected_sha.strip().lower()}"
        stable_source = self._canonical_input_source(row, name)
        digest = _sha256_hex_bytes(_canonical_json_bytes({"source": stable_source, "name": name}))
        return f"url_{digest}"

    def _extract_input_cache_key_from_path(self, path: Path) -> Optional[str]:
        match = re.match(r"^((?:url|sha256|key)_[0-9a-f]{64})(?:_|$)", path.name.lower())
        if not match:
            return None
        return match.group(1)

    def _collect_input_cache_inventory(self) -> Dict[str, Any]:
        cache_key_mtime: Dict[str, float] = {}
        total_bytes = 0
        for path in self._iter_input_cache_files():
            try:
                stat = path.stat()
            except Exception:
                continue
            total_bytes += int(stat.st_size)
            cache_key = self._extract_input_cache_key_from_path(path)
            if not cache_key:
                continue
            prev_mtime = cache_key_mtime.get(cache_key)
            if prev_mtime is None or float(stat.st_mtime) > prev_mtime:
                cache_key_mtime[cache_key] = float(stat.st_mtime)

        ordered_keys = [
            key for key, _mtime in sorted(
                cache_key_mtime.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        max_keys = max(0, int(self.input_cache_heartbeat_max_keys))
        if max_keys <= 0:
            selected_keys: List[str] = []
            truncated = len(ordered_keys) > 0
        else:
            selected_keys = ordered_keys[:max_keys]
            truncated = len(ordered_keys) > len(selected_keys)

        return {
            "keys": selected_keys,
            "keyCount": len(ordered_keys),
            "bytesUsed": int(total_bytes),
            "maxBytes": int(self.input_cache_max_bytes),
            "inventoryTruncated": truncated,
        }

    def _input_cache_path(self, cache_key: str, desired_name: str) -> Path:
        safe_name = os.path.basename(desired_name) or "input.bin"
        return self.input_cache_dir / cache_key[:2] / f"{cache_key}_{safe_name}"

    def _touch_input_cache_path(self, path: Path) -> None:
        now = time.time()
        try:
            os.utime(path, (now, now))
        except Exception:
            pass

    def _is_cached_input_valid(self, cache_path: Path, row: Dict[str, Any]) -> bool:
        if not cache_path.exists() or not cache_path.is_file():
            return False
        expected_size = self._input_cache_expected_size_bytes(row)
        if expected_size > 0:
            try:
                if int(cache_path.stat().st_size) != expected_size:
                    return False
            except Exception:
                return False
        expected_sha = row.get("sha256")
        if isinstance(expected_sha, str) and expected_sha:
            try:
                actual_sha = sha256_file(cache_path)
            except Exception:
                return False
            if actual_sha.lower() != expected_sha.lower():
                return False
        return True

    def _protected_input_cache_paths_locked(self) -> Set[str]:
        protected: Set[str] = set()
        for lease in self._active_exec_by_item.values():
            for entry in lease.prefetched_inputs:
                cache_path = entry.get("cache_path")
                if isinstance(cache_path, str) and cache_path:
                    protected.add(os.path.abspath(cache_path))
        return protected

    def _iter_input_cache_files(self) -> List[Path]:
        out: List[Path] = []
        tmp_dir = (self.input_cache_dir / ".tmp").resolve()
        if not self.input_cache_dir.exists():
            return out
        for path in self.input_cache_dir.rglob("*"):
            if not path.is_file():
                continue
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if tmp_dir in resolved.parents:
                continue
            out.append(path)
        return out

    def _prune_input_cache(self, incoming_bytes: int = 0) -> None:
        if int(self.input_cache_max_bytes) <= 0:
            return
        with self._lock:
            protected = self._protected_input_cache_paths_locked()

        candidates: List[Tuple[float, int, Path]] = []
        total_bytes = 0
        for path in self._iter_input_cache_files():
            try:
                stat = path.stat()
            except Exception:
                continue
            total_bytes += int(stat.st_size)
            candidates.append((float(stat.st_mtime), int(stat.st_size), path))

        candidates.sort(key=lambda row: (row[0], str(row[2])))
        while total_bytes + max(0, int(incoming_bytes)) > int(self.input_cache_max_bytes) and candidates:
            _mtime, size_bytes, path = candidates.pop(0)
            abs_path = os.path.abspath(str(path))
            if abs_path in protected:
                continue
            try:
                path.unlink()
                total_bytes = max(0, total_bytes - int(size_bytes))
            except Exception:
                continue

    def _ensure_cached_input(self, lease: AgentExecuteLease, row: Dict[str, Any], idx: int) -> Dict[str, Any]:
        name = row.get("name") if isinstance(row.get("name"), str) and row.get("name") else f"input_{idx}"
        cache_key = self._input_cache_key(row, name)
        cache_path = self._input_cache_path(cache_key, name)
        tmp_dir = self.input_cache_dir / ".tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        while True:
            if self._is_cached_input_valid(cache_path, row):
                self._touch_input_cache_path(cache_path)
                return {
                    "name": name,
                    "cache_key": cache_key,
                    "cache_path": str(cache_path),
                }

            try:
                if cache_path.exists():
                    cache_path.unlink()
            except Exception:
                pass

            should_download = False
            with self._lock:
                if cache_key not in self._input_cache_downloading:
                    self._input_cache_downloading.add(cache_key)
                    should_download = True
            if should_download:
                break
            time.sleep(0.2)

        try:
            self._agent_maybe_refresh_urls(lease, lease.command_state, force=False)
            download_url = row.get("downloadUrl")
            if not isinstance(download_url, str) or not download_url:
                raise RuntimeError(f"Input file #{idx} missing downloadUrl")

            self._prune_input_cache(self._input_cache_expected_size_bytes(row))
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            partial = tmp_dir / f"{cache_key}.{uuid.uuid4().hex}.partial"
            try:
                http_download_to_file(
                    download_url,
                    partial,
                    timeout_seconds=float(self.download_timeout_seconds),
                    chunk_size=int(self.download_chunk_size),
                )
                if not self._is_cached_input_valid(partial, row):
                    raise RuntimeError(f"input_cache_validation_failed for {name}")
                os.replace(str(partial), str(cache_path))
            finally:
                try:
                    if partial.exists():
                        partial.unlink()
                except Exception:
                    pass
            self._touch_input_cache_path(cache_path)
            return {
                "name": name,
                "cache_key": cache_key,
                "cache_path": str(cache_path),
            }
        finally:
            with self._lock:
                self._input_cache_downloading.discard(cache_key)

    def _copy_input_to_comfy(self, source_path: Path, desired_name: str) -> Path:
        safe_name = os.path.basename(desired_name) or f"input_{uuid.uuid4().hex}"
        dest = self.comfyui_dir / "input" / safe_name
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_dest = dest.parent / f".{safe_name}.{uuid.uuid4().hex}.tmp"
        try:
            if temp_dest.exists():
                temp_dest.unlink()
        except Exception:
            pass
        try:
            os.link(str(source_path), str(temp_dest))
        except Exception:
            shutil.copy2(str(source_path), str(temp_dest))
        os.replace(str(temp_dest), str(dest))
        return dest

    def _comfy_view_url(self, filename: str, subfolder: Optional[str], file_type: str = "output") -> str:
        params: List[Tuple[str, str]] = [("filename", filename), ("type", file_type)]
        if subfolder:
            params.append(("subfolder", subfolder))
        base_url = self._resolve_local_comfy_base_url(force_refresh=False, timeout_seconds=2.0)
        return f"{base_url}/view?{urllib.parse.urlencode(params)}"

    def _collect_history_output_refs(self, history_entry: Dict[str, Any]) -> List[Dict[str, str]]:
        outputs = history_entry.get("outputs")
        if not isinstance(outputs, dict):
            return []

        refs: List[Dict[str, str]] = []
        seen: Set[str] = set()
        preferred_keys = ("images", "gifs", "videos", "audios", "audio", "latents", "meshes", "files")
        path_like_keys = ("path", "file_path", "glb_path", "obj_path", "mesh_path")

        def _split_path(raw_path: str) -> Optional[Tuple[str, str]]:
            normalized = str(raw_path or "").strip().replace("\\", "/")
            if not normalized:
                return None

            # Trellis export nodes may return an absolute path under ComfyUI's output
            # directory. Reduce it to the relative path shape expected by /view.
            output_marker = "/output/"
            lower_normalized = normalized.lower()
            marker_index = lower_normalized.rfind(output_marker)
            if marker_index >= 0:
                normalized = normalized[marker_index + len(output_marker):]

            normalized = normalized.lstrip("/").rstrip("/")
            if not normalized:
                return None

            filename = os.path.basename(normalized)
            if not filename or "." not in filename:
                return None

            subfolder = os.path.dirname(normalized)
            if subfolder in ("", "."):
                subfolder = ""

            return filename, subfolder

        def _push(row: Dict[str, Any]) -> None:
            filename = row.get("filename")
            subfolder = row.get("subfolder") if isinstance(row.get("subfolder"), str) else ""
            if not isinstance(filename, str) or not filename:
                for key in path_like_keys:
                    raw_path = row.get(key)
                    if not isinstance(raw_path, str) or not raw_path:
                        continue
                    split_path = _split_path(raw_path)
                    if split_path is None:
                        continue
                    filename, derived_subfolder = split_path
                    if not subfolder:
                        subfolder = derived_subfolder
                    break
                else:
                    return

            file_type = row.get("type") if isinstance(row.get("type"), str) and row.get("type") else "output"
            dedupe = f"{file_type}:{subfolder}:{filename}"
            if dedupe in seen:
                return
            seen.add(dedupe)
            refs.append({"filename": filename, "subfolder": subfolder, "type": file_type})

        def _push_path_value(raw_value: Any) -> None:
            if not isinstance(raw_value, str):
                return
            split_path = _split_path(raw_value)
            if split_path is None:
                return
            filename, subfolder = split_path
            dedupe = f"output:{subfolder}:{filename}"
            if dedupe in seen:
                return
            seen.add(dedupe)
            refs.append({"filename": filename, "subfolder": subfolder, "type": "output"})

        for _node_id, node_output in sorted(outputs.items(), key=lambda kv: kv[0]):
            if not isinstance(node_output, dict):
                continue
            for key in preferred_keys:
                value = node_output.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _push(item)
                elif isinstance(value, dict):
                    _push(value)
            for key in path_like_keys:
                value = node_output.get(key)
                if isinstance(value, str):
                    _push({key: value})
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            _push({key: item})
            for value in node_output.values():
                if isinstance(value, dict):
                    _push(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _push(item)
                        else:
                            _push_path_value(item)
                else:
                    _push_path_value(value)
        return refs

    def _process_install_node_bundles_item(self, item: Dict[str, Any]) -> None:
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        if not item_id or not lease_id:
            return
        if self.mining_only:
            self._agent_ack(item_id, lease_id, "command_ignored_stale")
            return
        self._wait_for_video_gen_v2_bootstrap_ready(item_id, lease_id)

        bundle_ids = [bundle_id for bundle_id in payload.get("bundleIds", []) if isinstance(bundle_id, str) and bundle_id]
        verify_class_types = [class_type for class_type in payload.get("verifyClassTypes", []) if isinstance(class_type, str) and class_type]
        required_install_signatures_raw = payload.get("requiredInstallSignatures")
        required_install_signature_bundle_ids: Set[str] = set()
        if isinstance(required_install_signatures_raw, dict):
            for bundle_id in bundle_ids:
                signature = required_install_signatures_raw.get(bundle_id)
                if isinstance(signature, str) and re.match(r"^[0-9a-fA-F]{64}$", signature.strip()):
                    required_install_signature_bundle_ids.add(bundle_id)
        if not verify_class_types and (self.server_type or "").strip() in ("video_gen_v2", "video_gen_v2_salad", "video_gen_v3", "video_gen_v4"):
            seen_verify_classes: Set[str] = set()
            for bundle_id in bundle_ids:
                for class_type in self._video_gen_v2_bundle_verify_class_types(bundle_id):
                    if class_type not in seen_verify_classes:
                        seen_verify_classes.add(class_type)
                        verify_class_types.append(class_type)
        bundle_specs = payload.get("bundleSpecs") if isinstance(payload.get("bundleSpecs"), dict) else {}
        if not bundle_ids:
            self._agent_ack(item_id, lease_id, "command_ignored_stale")
            return
        furgen_video_tools_required_class_types = self._furgen_video_tools_required_class_types_for_install(
            bundle_ids,
            verify_class_types,
        )

        if self._local_comfy_has_all_class_types(verify_class_types) and not required_install_signature_bundle_ids:
            compat_changed = self._ensure_node_bundle_runtime_compatibility(
                bundle_ids,
                bundle_specs=bundle_specs,
                required_class_types=furgen_video_tools_required_class_types,
            )
            if compat_changed:
                self._stop_idle_prl_mining_for_work("install_node_bundles_comfy_restart")
                self._remove_local_readiness_file()
                self._restart_local_comfy_and_wait(
                    prefer_process_restart=True,
                    verify_class_types=verify_class_types,
                    timeout_seconds=max(300.0, 120.0 * max(1, len(bundle_ids))),
                )
            self._remember_node_bundle_verify_class_types(
                verify_class_types,
                bundle_ids=bundle_ids,
                bundle_specs=bundle_specs,
            )
            self._write_local_readiness_file()
            self._agent_ack(item_id, lease_id, "command_succeeded")
            return
        if required_install_signature_bundle_ids:
            logging.info(
                "Installing node bundle(s) despite existing verification classes because install signatures are required: %s",
                ",".join(sorted(required_install_signature_bundle_ids)),
            )

        had_readiness_marker_before_install = self._local_readiness_file_present()
        comfy_restart_attempted = False
        self._remove_local_readiness_file()

        try:
            server_type = (self.server_type or "").strip()
            asset_gen_v5_server_types = (
                "asset_gen_v5",
                "asset_gen_v5_lite",
                "asset_gen_v6_lite",
                "asset_gen_v7_lite",
                "foxy_all",
            )
            legacy_bundle_ids: List[str] = []
            for bundle_id in bundle_ids:
                spec = bundle_specs.get(bundle_id) if isinstance(bundle_specs, dict) else None
                if isinstance(spec, dict) and spec:
                    logging.info("Installing node bundle installSource=spec serverType=%s bundleId=%s", server_type, bundle_id)
                    if not self._install_node_bundle_from_spec(
                        bundle_id,
                        spec,
                        required_class_types=furgen_video_tools_required_class_types,
                    ):
                        raise RuntimeError(f"Unsupported Firestore install spec for {server_type} bundle {bundle_id}")
                else:
                    legacy_bundle_ids.append(bundle_id)

            if legacy_bundle_ids:
                logging.warning(
                    "Installing node bundle(s) installSource=legacyFallback serverType=%s bundleIds=%s",
                    server_type,
                    ",".join(legacy_bundle_ids),
                )
            if server_type in asset_gen_v5_server_types and legacy_bundle_ids:
                script_path = self._resolve_asset_gen_v5_script()
                if script_path is None:
                    raise RuntimeError(f"Unable to locate asset_gen_v5.sh on {server_type} instance.")
                subprocess.run(
                    ["bash", str(script_path), "install-bundles", *legacy_bundle_ids],
                    cwd=str(self.workspace),
                    env=os.environ.copy(),
                    check=True,
                    timeout=max(1800, 300 * max(1, len(legacy_bundle_ids))),
                )
            elif server_type in ("video_gen_v2", "video_gen_v2_salad", "video_gen_v3", "video_gen_v4") and legacy_bundle_ids:
                # Newer video server types can share compatible bundle catalogs; the
                # installers are server-type agnostic (git nodes + managed
                # FurgenVideoTools copies).
                for bundle_id in legacy_bundle_ids:
                    self._install_video_gen_v2_node_bundle(
                        bundle_id,
                        required_class_types=furgen_video_tools_required_class_types,
                    )
            elif legacy_bundle_ids:
                raise RuntimeError(f"install_node_bundles is not supported on server_type={self.server_type}")
            self._ensure_node_bundle_runtime_compatibility(
                bundle_ids,
                bundle_specs=bundle_specs,
                required_class_types=furgen_video_tools_required_class_types,
            )
            self._stop_idle_prl_mining_for_work("install_node_bundles_comfy_restart")
            self._remove_local_readiness_file()
            comfy_restart_attempted = True
            self._restart_local_comfy_and_wait(
                verify_class_types=verify_class_types or None,
                timeout_seconds=max(300.0, 120.0 * max(1, len(bundle_ids))),
            )
            self._remember_node_bundle_verify_class_types(
                verify_class_types,
                bundle_ids=bundle_ids,
                bundle_specs=bundle_specs,
            )
            self._write_local_readiness_file()
            self._agent_ack(item_id, lease_id, "command_succeeded")
        except Exception as exc:
            self._remove_local_readiness_file()
            if had_readiness_marker_before_install and not comfy_restart_attempted:
                self._restore_local_readiness_file_after_pre_restart_failure(str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS])
            self._agent_ack(
                item_id,
                lease_id,
                "command_failed",
                error_code="install_node_bundles_failed",
                error_message=str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS],
            )

    def _agent_handle_cancel_command(self, item: Dict[str, Any]) -> None:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        item_id = item.get("itemId")
        lease_id = item.get("leaseId")
        if not isinstance(item_id, str) or not isinstance(lease_id, str):
            return

        job_id = payload.get("jobId") if isinstance(payload.get("jobId"), str) else ""
        execution_attempt = payload.get("executionAttempt")
        attempt_epoch = payload.get("attemptEpoch")
        if not job_id or not isinstance(execution_attempt, (int, float)) or not isinstance(attempt_epoch, (int, float)):
            self._agent_ack(item_id, lease_id, "command_ignored_stale")
            return

        matched = False
        with self._lock:
            for lease in self._active_exec_by_item.values():
                if (
                    lease.job_id == job_id
                    and int(lease.execution_attempt) == int(execution_attempt)
                    and int(lease.attempt_epoch) == int(attempt_epoch)
                ):
                    lease.cancel_requested = True
                    lease.cancel_reason = payload.get("reason") if isinstance(payload.get("reason"), str) else "cancel_requested"
                    matched = True
                    break
        if matched:
            self._agent_ack(
                item_id,
                lease_id,
                "command_succeeded",
                tuple_fields={
                    "jobId": job_id,
                    "executionAttempt": int(execution_attempt),
                    "attemptEpoch": int(attempt_epoch),
                },
            )
        else:
            self._agent_ack(
                item_id,
                lease_id,
                "command_ignored_stale",
                tuple_fields={
                    "jobId": job_id,
                    "executionAttempt": int(execution_attempt),
                    "attemptEpoch": int(attempt_epoch),
                },
            )

    def _agent_handle_restart_comfy_command(self, item: Dict[str, Any]) -> None:
        item_id = item.get("itemId")
        lease_id = item.get("leaseId")
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        if not isinstance(item_id, str) or not isinstance(lease_id, str):
            return
        if self.mining_only:
            self._agent_ack(item_id, lease_id, "command_ignored_stale")
            return
        if not self._comfy_restart_process_isolated():
            self._agent_ack(
                item_id,
                lease_id,
                "command_failed",
                error_code="restart_comfy_process_group_unsafe",
                error_message="Dependency agent is not isolated from the ComfyUI supervisor process group.",
            )
            return

        prefer_process_restart = payload.get("preferProcessRestart") is not False
        allow_supervisor_restart = payload.get("allowSupervisorRestart") is not False
        self._stop_idle_prl_mining_for_work("restart_comfy")
        try:
            self._write_interrupted_comfy_restart_intent(item_id, lease_id)
        except Exception as exc:
            self._agent_ack(
                item_id,
                lease_id,
                "command_failed",
                error_code="restart_comfy_intent_write_failed",
                error_message=str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS],
            )
            return
        self._comfy_restart_command_active.set()
        self._remove_local_readiness_file()
        try:
            verify_class_types = self._node_bundle_restart_verify_class_types()
            self._restart_local_comfy_and_wait(
                prefer_process_restart=prefer_process_restart,
                allow_supervisor_restart=allow_supervisor_restart,
                verify_class_types=verify_class_types or None,
                timeout_seconds=300.0,
                require_readiness_marker=False,
            )
            self._write_local_readiness_file()
            self._clear_interrupted_comfy_restart_intent()
            logging.info(
                "Periodic ComfyUI restart completed; readiness restored and acknowledging success itemId=%s verifiedClassTypes=%d",
                item_id,
                len(verify_class_types),
            )
            self._agent_ack(item_id, lease_id, "command_succeeded")
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass
        except Exception as exc:
            if self._restore_local_readiness_file_after_pre_restart_failure(str(exc)):
                self._clear_interrupted_comfy_restart_intent()
            self._agent_ack(
                item_id,
                lease_id,
                "command_failed",
                error_code="restart_comfy_failed",
                error_message=str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS],
            )
        finally:
            self._comfy_restart_command_active.clear()

    def _agent_handle_prl_miner_command(self, item: Dict[str, Any]) -> None:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        action = str(payload.get("action") or "").strip().lower()
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""
        try:
            if action == "start" and self._gpu_coordinator.configured:
                prepared_payload = {**payload, "forceRestart": True}
                try:
                    # DNS/TLS checks, download/install/checksum and stray cleanup
                    # happen before the coordinator begins its five-second
                    # STARTING registration window.
                    prepared = self._idle_prl_miner.prepare_gated(prepared_payload)
                except Exception as exc:
                    if item_id and lease_id:
                        self._agent_ack(
                            item_id,
                            lease_id,
                            "command_failed",
                            error_code=(
                                "prl_network_preflight_failed"
                                if isinstance(exc, PrlNetworkPreflightError)
                                else "prl_miner_failed"
                            ),
                            error_message=str(exc)[:MAX_AGENT_ERROR_MESSAGE_CHARS],
                        )
                    return
                if not self._acquire_mining_gpu_lease("prl_miner_start_command"):
                    # The backend command expresses desired state. Preserve it
                    # locally and acknowledge acceptance; the idle loop retries
                    # admission without starting an unleased miner.
                    self._idle_prl_miner.defer_start(payload, "gpu_coordinator_busy")
                    if item_id and lease_id:
                        self._agent_ack(item_id, lease_id, "command_succeeded")
                    return
                gate_path = ""
                try:
                    gated = self._idle_prl_miner.start_gated(prepared)
                    gate_path = str(gated.get("gatePath") or "")
                    self._register_mining_process_with_gpu_coordinator()
                    self._idle_prl_miner.release_start_gate(gate_path)
                    if item_id and lease_id:
                        self._agent_ack(item_id, lease_id, "command_succeeded")
                except Exception as exc:
                    self._idle_prl_miner.cancel_gated_start("mining_process_registration_failed")
                    self._idle_prl_miner.defer_start(payload, "gpu_coordinator_fenced_during_start")
                    self._release_mining_gpu_lease("prl_miner_start_exception")
                    # Fencing/foreground races preserve desired state and retry
                    # locally; preparation failures above remain command errors.
                    if item_id and lease_id:
                        self._agent_ack(item_id, lease_id, "command_succeeded")
                    logging.info("Deferred gated PRL miner launch after fencing race: %s", exc)
                return
            if action == "start":
                self._free_local_comfy_for_idle_prl_mining("prl_miner_start_command")
            command_item = item
            if action == "stop" and self._gpu_coordinator.configured:
                command_item = dict(item)
                command_item["payload"] = {**payload, "forceStop": True}
            self._idle_prl_miner.handle_command(command_item, self._agent_ack)
            if action == "stop":
                self._release_mining_gpu_lease("prl_miner_stop_command")
        except Exception:
            if action == "start":
                self._idle_prl_miner.stop_if_running("mining_process_registration_failed")
                self._release_mining_gpu_lease("prl_miner_start_exception")
            raise
        finally:
            self._force_idle_prl_runtime_refresh("prl_miner_command")

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

    def _is_retryable_download_error(
        self,
        err: Exception,
        item: Optional[Dict[str, Any]] = None,
    ) -> bool:
        msg = str(err).lower()
        resolved = item.get("resolved") if isinstance(item, dict) else None
        source_url = resolved.get("url") if isinstance(resolved, dict) else None
        source_host = ""
        source_path = ""
        if isinstance(source_url, str) and source_url:
            try:
                parsed_source = urllib.parse.urlparse(source_url)
                source_host = (parsed_source.hostname or "").lower()
                source_path = parsed_source.path.lower()
            except Exception:
                pass

        # Hugging Face resolve URLs redirect to short-lived Xet/CDN URLs. Large,
        # segmented downloads can receive a 403/416 or aria2 exit 22 after the
        # redirect expires or its range state goes stale. Re-fetching the resolve
        # URL produces a fresh signed redirect and aria2 resumes the partial file.
        # Treat only this narrow, resumable shape as transient; ordinary 4xx
        # responses (including missing/private models) remain terminal.
        is_huggingface_resolve = (
            source_host in {"huggingface.co", "www.huggingface.co"}
            and "/resolve/" in source_path
        )
        if is_huggingface_resolve:
            status_codes = _http_status_codes_from_error(err)
            if any(code in {403, 416} for code in status_codes):
                return True
            if "aria2c failed (exit=22)" in msg and not status_codes:
                return True

        if _is_permanent_http_error(err):
            return False
        # Configuration / policy errors won't resolve without human action.
        non_retry_substrings = [
            "missing resolved download info",
            "missing hf_token",
            "missing civitai_token",
            "unsupported auth type",
            "download domain not allowed",
            "invalid download url",
            "destrelativepath must be relative",
            "destrelativepath escapes base dir",
            "wget not found",
            "dm_download_tool",
        ]
        return not any(s in msg for s in non_retry_substrings)

    def _forget_retry(self, dep_id: str, failed: bool = False) -> None:
        if not dep_id:
            return
        with self._lock:
            self._state.retry.pop(dep_id, None)
            self._downloading.discard(dep_id)
            if not failed:
                self._state.failed.discard(dep_id)
            self._save_state()

    def _local_retry_item_if_current(self, dep_id: str, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item_id = entry.get("itemId") if isinstance(entry.get("itemId"), str) and entry.get("itemId") else dep_id
        try:
            current = self._fetch_queue_item(item_id)
        except ApiError as e:
            if e.status == 404:
                if isinstance(e.body, str) and '"exists":false' in e.body.replace(" ", ""):
                    logging.info("Dropping local retry for %s: queue item no longer exists", dep_id)
                    self._forget_retry(dep_id)
                    return None
                logging.warning("Dependency queue item validation endpoint returned 404 for %s; preserving local retry", dep_id)
                resolved = entry.get("resolved")
                if isinstance(resolved, dict):
                    return {
                        "itemId": item_id,
                        "depId": dep_id,
                        "op": "download",
                        "resolved": resolved,
                    }
            raise

        if not current:
            logging.info("Dropping local retry for %s: queue item no longer exists", dep_id)
            self._forget_retry(dep_id)
            return None

        state = current.get("state")
        cancel_reason = current.get("cancelReason")
        if isinstance(state, str) and state.lower() in NON_RETRYABLE_QUEUE_STATES:
            logging.info("Dropping local retry for %s: queue item state is %s", dep_id, state)
            self._forget_retry(dep_id)
            return None
        if isinstance(cancel_reason, str) and cancel_reason.strip():
            logging.info("Dropping local retry for %s: queue item was cancelled (%s)", dep_id, cancel_reason.strip())
            self._forget_retry(dep_id)
            return None

        if self._clear_retry_if_forced(dep_id, current, entry):
            entry = {}

        resolved = current.get("resolved") if isinstance(current.get("resolved"), dict) else entry.get("resolved")
        if not isinstance(resolved, dict):
            logging.info("Dropping local retry for %s: queue item has no resolved download instructions", dep_id)
            self._forget_retry(dep_id, failed=True)
            return None

        return {
            **current,
            "itemId": item_id,
            "depId": dep_id,
            "op": current.get("op") if isinstance(current.get("op"), str) else "download",
            "resolved": resolved,
        }

    def _compute_retry_delay_seconds(self, attempts: int, last_error: str) -> float:
        # Exponential backoff with jitter. Capped so we keep making progress.
        base = 120.0  # 2 minutes
        cap = 30.0 * 60.0  # 30 minutes
        delay = min(cap, base * (2 ** max(0, int(attempts) - 1)))

        le = (last_error or "").lower()
        if "existing file appears in progress" in le:
            return 15.0
        if "429" in le or "too many requests" in le:
            delay = max(delay, 5.0 * 60.0)
        if "timed out" in le or "timeout" in le:
            delay = max(delay, 2.0 * 60.0)

        jitter = delay * 0.2
        return max(10.0, delay + random.uniform(-jitter, jitter))

    def _format_backoff_error(self, dep_id: str, last_error: str, next_attempt_at_ms: int) -> str:
        seconds = max(0, int((next_attempt_at_ms - _now_ms()) / 1000))
        prefix = f"Backoff active for {dep_id}; next attempt in ~{seconds}s."
        if last_error:
            return f"{prefix} Last error: {last_error}"
        return prefix

    def _force_retry_after_ms(self, item: Dict[str, Any]) -> Optional[int]:
        raw = item.get("forceRetryAfterMs")
        if isinstance(raw, (int, float)) and raw > 0:
            return int(raw)
        return None

    def _clear_retry_if_forced(self, dep_id: str, item: Dict[str, Any], retry_entry: Optional[Dict[str, Any]]) -> bool:
        force_after_ms = self._force_retry_after_ms(item)
        if not dep_id or force_after_ms is None:
            return False

        last_attempt = 0
        if isinstance(retry_entry, dict):
            raw_last_attempt = retry_entry.get("lastAttemptAtMs")
            if isinstance(raw_last_attempt, (int, float)):
                last_attempt = int(raw_last_attempt)

        if force_after_ms <= last_attempt:
            return False

        logging.info(
            "Clearing local dependency retry backoff due to forceRetryAfterMs. depId=%s forceRetryAfterMs=%d lastAttemptAtMs=%d",
            dep_id,
            force_after_ms,
            last_attempt,
        )
        self._forget_retry(dep_id)
        return True

    def _schedule_download_retry(self, item: Dict[str, Any], err: Exception) -> int:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            return _now_ms() + 60_000

        resolved = item.get("resolved")
        if not isinstance(resolved, dict):
            # We can't retry without resolved info; keep it as a hard failure.
            return _now_ms() + 60_000

        err_msg = str(err)[:MAX_AGENT_ERROR_MESSAGE_CHARS]
        now = _now_ms()
        with self._lock:
            prev = self._state.retry.get(dep_id) if isinstance(self._state.retry.get(dep_id), dict) else {}
            attempts = int(prev.get("attempts")) if isinstance(prev.get("attempts"), int) else 0
            attempts = max(0, attempts) + 1
            delay_s = self._compute_retry_delay_seconds(attempts, err_msg)
            next_at = now + int(delay_s * 1000)
            self._state.retry[dep_id] = {
                "itemId": item.get("itemId") or dep_id,
                "resolved": resolved,
                "attempts": attempts,
                "nextAttemptAtMs": next_at,
                "lastError": err_msg,
                "lastAttemptAtMs": now,
                **({"requestedByJobId": item.get("requestedByJobId")} if isinstance(item.get("requestedByJobId"), str) else {}),
            }
            self._state.failed.add(dep_id)
            self._downloading.discard(dep_id)
            self._save_state()
        return next_at

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
        allowed_domains = self._download_allowed_domains_for_item(item, resolved)
        if expected_size_bytes <= 0:
            inferred_remote_size = self._resolve_remote_expected_size_bytes(url, auth_header)
            if inferred_remote_size > 0:
                expected_size_bytes = inferred_remote_size
                logging.info(
                    "Using remote HEAD size for %s: %d bytes",
                    dep_id,
                    int(expected_size_bytes),
                )
        self._update_download_activity(
            dep_id,
            dest_relative_path=dest_rel,
            expected_bytes=int(expected_size_bytes),
            downloaded_bytes=0,
            stage="preparing",
            tool=self._resolve_download_tool(),
        )

        def _progress_cb(stage_name: str, tool_name: str) -> Callable[[int, int], None]:
            def _cb(processed_bytes: int, total_bytes: int) -> None:
                self._update_download_activity(
                    dep_id,
                    dest_relative_path=dest_rel,
                    expected_bytes=int(total_bytes or expected_size_bytes or 0),
                    downloaded_bytes=int(processed_bytes),
                    stage=stage_name,
                    tool=tool_name,
                )
            return _cb

        # Fast path: if the file already exists (e.g., legacy provisioning), treat as installed.
        if dest_abs.exists():
            size_matches = True
            existing_stat: Optional[os.stat_result] = None
            modified_age_seconds: Optional[float] = None
            try:
                existing_stat = dest_abs.stat()
                modified_age_seconds = max(0.0, time.time() - float(existing_stat.st_mtime))
            except Exception:
                existing_stat = None
                modified_age_seconds = None
            if expected_size_bytes > 0:
                try:
                    stat_ref = existing_stat if existing_stat is not None else dest_abs.stat()
                    size_matches = int(stat_ref.st_size) == int(expected_size_bytes)
                except Exception:
                    size_matches = False
            if isinstance(sha256_expected, str) and sha256_expected:
                actual_existing = sha256_file(
                    dest_abs,
                    progress_cb=_progress_cb("verifying_existing", "local"),
                )
                if actual_existing.lower() != sha256_expected.lower():
                    logging.warning(
                        "Existing file sha256 mismatch for %s; re-downloading. expected=%s got=%s path=%s",
                        dep_id,
                        sha256_expected,
                        actual_existing,
                        str(dest_abs),
                    )
                elif not size_matches:
                    logging.warning(
                        "Existing file size mismatch for %s; re-downloading. expected=%s got=%s path=%s",
                        dep_id,
                        int(expected_size_bytes),
                        int(dest_abs.stat().st_size),
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
                        self._state.retry.pop(dep_id, None)
                        self._downloading.discard(dep_id)
                        self._record_dependency_verification_locked(
                            dep_id,
                            dest_rel,
                            dest_abs,
                            actual_existing,
                        )
                        self._save_state()
                    self._clear_download_activity(dep_id)
                    return
            else:
                if (
                    modified_age_seconds is not None
                    and modified_age_seconds < float(EXISTING_FILE_STABLE_SECONDS)
                ):
                    raise RuntimeError(
                        "Existing file appears in progress for "
                        f"{dep_id}; modified {modified_age_seconds:.1f}s ago."
                    )
                if not size_matches:
                    logging.warning(
                        "Existing file size mismatch for %s; re-downloading. expected=%s got=%s path=%s",
                        dep_id,
                        int(expected_size_bytes),
                        int(dest_abs.stat().st_size),
                        str(dest_abs),
                    )
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
                        self._state.retry.pop(dep_id, None)
                        self._downloading.discard(dep_id)
                        self._record_dependency_verification_locked(
                            dep_id,
                            dest_rel,
                            dest_abs,
                            "",
                        )
                        self._save_state()
                    self._clear_download_activity(dep_id)
                    return

        did_evict = self._ensure_space_for_download(expected_size_bytes, dep_id, dest_abs=dest_abs)
        if did_evict and _now_ms() - int(self._last_heartbeat_ms) >= 2000:
            # Eviction changes inventory; push an early heartbeat to reduce scheduling race windows.
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass

        try:
            resolved_tool = self._resolve_download_tool()
            checksum_verified = False
            if resolved_tool == "aria2":
                try:
                    aria2_download(
                        url=url,
                        dest_partial=partial,
                        auth_header=auth_header,
                        expected_size_bytes=int(expected_size_bytes),
                        timeout_seconds=float(self.download_timeout_seconds),
                        allowed_domains=allowed_domains,
                        debug=self.download_debug,
                        progress_cb=_progress_cb("downloading", "aria2"),
                    )
                    if isinstance(sha256_expected, str) and sha256_expected:
                        aria2_actual = sha256_file(
                            partial,
                            progress_cb=_progress_cb("verifying_download", "local"),
                        )
                        if aria2_actual.lower() != sha256_expected.lower():
                            raise RuntimeError(
                                f"sha256 mismatch for {dep_id}: expected {sha256_expected}, got {aria2_actual}"
                            )
                        checksum_verified = True
                except Exception as aria2_error:
                    # Parallel range requests can produce a full-sized but corrupt
                    # file on some signed CDN/Xet endpoints, and occasionally stop
                    # making byte progress indefinitely. Never accept those bytes:
                    # restart from zero through a single-stream downloader and keep
                    # the catalog checksum as the source of truth.
                    try:
                        if partial.exists():
                            partial.unlink()
                    except Exception:
                        pass
                    fallback_tool = "huggingface_hub"
                    logging.warning(
                        "aria2 download failed validation for %s; retrying from zero with %s: %s",
                        dep_id,
                        fallback_tool,
                        str(aria2_error),
                    )
                    try:
                        huggingface_hub_download(
                            url=url,
                            dest_partial=partial,
                            auth_header=auth_header,
                            expected_size_bytes=int(expected_size_bytes),
                            progress_cb=_progress_cb("downloading_fallback", "huggingface_hub"),
                        )
                    except Exception as huggingface_error:
                        fallback_tool = "wget" if _command_exists("wget") else "python"
                        logging.warning(
                            "huggingface_hub fallback unavailable for %s; retrying with %s: %s",
                            dep_id,
                            fallback_tool,
                            str(huggingface_error),
                        )
                    if fallback_tool == "wget":
                        wget_download(
                            url=url,
                            dest_partial=partial,
                            auth_header=auth_header,
                            expected_size_bytes=int(expected_size_bytes),
                            timeout_seconds=float(self.download_timeout_seconds),
                            allowed_domains=allowed_domains,
                            debug=self.download_debug,
                            progress_cb=_progress_cb("downloading_fallback", "wget"),
                        )
                    elif fallback_tool == "python":
                        http_download(
                            url=url,
                            dest_partial=partial,
                            auth_header=auth_header,
                            expected_size_bytes=int(expected_size_bytes),
                            timeout_seconds=float(self.download_timeout_seconds),
                            chunk_size=int(self.download_chunk_size),
                            allowed_domains=allowed_domains,
                            verbose=self.verbose_progress,
                            debug=self.download_debug,
                            progress_cb=_progress_cb("downloading_fallback", "python"),
                        )
            elif resolved_tool == "wget":
                wget_download(
                    url=url,
                    dest_partial=partial,
                    auth_header=auth_header,
                    expected_size_bytes=int(expected_size_bytes),
                    timeout_seconds=float(self.download_timeout_seconds),
                    allowed_domains=allowed_domains,
                    debug=self.download_debug,
                    progress_cb=_progress_cb("downloading", "wget"),
                )
            elif resolved_tool == "python":
                http_download(
                    url=url,
                    dest_partial=partial,
                    auth_header=auth_header,
                    expected_size_bytes=int(expected_size_bytes),
                    timeout_seconds=float(self.download_timeout_seconds),
                    chunk_size=int(self.download_chunk_size),
                    allowed_domains=allowed_domains,
                    verbose=self.verbose_progress,
                    debug=self.download_debug,
                    progress_cb=_progress_cb("downloading", "python"),
                )
            else:
                raise RuntimeError(f"Unsupported DM_DOWNLOAD_TOOL: {self.download_tool}")

            if isinstance(sha256_expected, str) and sha256_expected and not checksum_verified:
                actual = sha256_file(
                    partial,
                    progress_cb=_progress_cb("verifying_download", "local"),
                )
                if actual.lower() != sha256_expected.lower():
                    try:
                        if partial.exists():
                            partial.unlink()
                    except Exception:
                        pass
                    raise RuntimeError(f"sha256 mismatch for {dep_id}: expected {sha256_expected}, got {actual}")
        except Exception as e:
            # Keep partial downloads for retryable errors so future retries can resume.
            # Pass the queue item here as well as in the outer retry handler. The
            # Hugging Face resumability policy depends on the original /resolve/
            # URL; without the item this inner cleanup path misclassifies aria2
            # redirect failures as terminal and deletes the resumable partial
            # before the outer handler schedules the retry.
            retryable = self._is_retryable_download_error(e, item)
            if not retryable:
                try:
                    if partial.exists():
                        partial.unlink()
                except Exception:
                    pass
            self._clear_download_activity(dep_id)
            raise

        # Make the accepted bytes durable before advertising them. Hashing the
        # partial caught transfer corruption, but the old path trusted the rename
        # itself and immediately marked the dependency installed. Re-open and
        # hash the final path after fsync/rename so the installed marker proves
        # the exact bytes ComfyUI will read.
        with partial.open("rb") as partial_handle:
            os.fsync(partial_handle.fileno())
        os.replace(str(partial), str(dest_abs))
        try:
            directory_fd = os.open(str(dest_abs.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as fsync_error:
            logging.warning("Dependency directory fsync failed for %s: %s", dep_id, fsync_error)

        final_actual_sha = ""
        if isinstance(sha256_expected, str) and sha256_expected:
            final_actual_sha = sha256_file(
                dest_abs,
                progress_cb=_progress_cb("verifying_final_path", "local"),
            )
            if final_actual_sha.lower() != sha256_expected.lower():
                try:
                    dest_abs.unlink()
                except OSError:
                    pass
                with self._lock:
                    self._invalidate_dependency_verification_locked(dep_id, mark_failed=True)
                    self._downloading.discard(dep_id)
                    self._save_state()
                raise RuntimeError(
                    f"final path sha256 mismatch for {dep_id}: expected {sha256_expected}, got {final_actual_sha}"
                )
        elif expected_size_bytes > 0 and int(dest_abs.stat().st_size) != expected_size_bytes:
            final_size_bytes = int(dest_abs.stat().st_size)
            try:
                dest_abs.unlink()
            except OSError:
                pass
            raise RuntimeError(
                f"final path size mismatch for {dep_id}: expected {expected_size_bytes}, got {final_size_bytes}"
            )

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
            self._state.retry.pop(dep_id, None)
            self._downloading.discard(dep_id)
            self._record_dependency_verification_locked(
                dep_id,
                dest_rel,
                dest_abs,
                final_actual_sha,
            )

            policy = self._dynamic_policy()
            if policy.get("enabled"):
                freed = self._evict_dynamic_locked(required_free_bytes=int(policy.get("minFreeBytes") or 0), protect={dep_id})
                should_heartbeat = freed > 0

            self._save_state()

        self._clear_download_activity(dep_id)
        if should_heartbeat and _now_ms() - int(self._last_heartbeat_ms) >= 2000:
            # This will update installedDynamicDepIds after eviction.
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass

    def _touch_target_requires_download(self, item: Dict[str, Any]) -> bool:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            raise RuntimeError("Touch queue item missing depId")
        resolved = item.get("resolved")
        if not isinstance(resolved, dict):
            raise RuntimeError("Touch queue item missing resolved dependency info")
        dest_rel = resolved.get("destRelativePath")
        if not isinstance(dest_rel, str) or not dest_rel:
            raise RuntimeError("Touch queue item missing resolved destination path")
        return not self._verify_resolved_dependency_file(item, require_full_hash=True)

    def _touch_item(self, item: Dict[str, Any]) -> None:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            raise RuntimeError("Touch queue item missing depId")
        if self._touch_target_requires_download(item):
            raise RuntimeError(f"Touch target is missing or size-mismatched for dependency {dep_id}")
        resolved = item.get("resolved")
        kind = resolved.get("kind") if isinstance(resolved, dict) else None
        dest_rel = resolved.get("destRelativePath") if isinstance(resolved, dict) else None

        with self._lock:
            self._state.failed.discard(dep_id)
            is_dynamic = (isinstance(kind, str) and kind.lower() == "dynamic") or (dep_id in self._state.installed_dynamic)
            if is_dynamic:
                self._touch_dynamic_locked(dep_id, dest_rel if isinstance(dest_rel, str) else None)
            else:
                self._state.installed_static.add(dep_id)
                self._state.installed_dynamic.discard(dep_id)
                self._state.lru.pop(dep_id, None)
            self._state.retry.pop(dep_id, None)
            self._save_state()

    def _delete_item(self, item: Dict[str, Any]) -> None:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            raise RuntimeError("Queue item missing depId")

        resolved = item.get("resolved")
        dest_rel: Optional[str] = None
        if isinstance(resolved, dict) and isinstance(resolved.get("destRelativePath"), str):
            dest_rel = resolved.get("destRelativePath")

        with self._lock:
            if dep_id in self._downloading:
                raise RuntimeError(f"Cannot delete {dep_id} while it is downloading")
            if not dest_rel:
                entry = self._state.lru.get(dep_id)
                if isinstance(entry, dict) and isinstance(entry.get("destRelativePath"), str):
                    dest_rel = entry.get("destRelativePath")

        if not dest_rel:
            raise RuntimeError("Resolved dependency missing destRelativePath")

        dest_abs = safe_join(self.comfyui_dir, dest_rel)
        partial = dest_abs.with_suffix(dest_abs.suffix + ".partial")
        deleted_paths: List[str] = []
        freed_bytes = 0

        for candidate in (dest_abs, partial):
            try:
                if not candidate.exists():
                    continue
                if candidate.is_dir():
                    raise RuntimeError(f"Refusing to delete directory for dependency {dep_id}: {candidate}")
                size = int(candidate.stat().st_size)
                candidate.unlink()
                freed_bytes += max(0, size)
                deleted_paths.append(str(candidate))
            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(f"Failed to delete {candidate}: {e}") from e

        with self._lock:
            prev = self._state.lru.pop(dep_id, None) or {}
            prev_size = prev.get("sizeBytes") if isinstance(prev.get("sizeBytes"), int) else freed_bytes
            self._dynamic_bytes_used = max(0, int(self._dynamic_bytes_used) - int(prev_size))
            self._state.installed_dynamic.discard(dep_id)
            self._state.installed_static.discard(dep_id)
            self._state.failed.discard(dep_id)
            self._state.retry.pop(dep_id, None)
            self._downloading.discard(dep_id)
            self._state.verified.pop(dep_id, None)
            self._session_hash_verified_dep_ids.discard(dep_id)
            self._save_state()

        logging.info(
            "Deleted dependency %s (%d bytes, %d paths): %s",
            dep_id,
            int(freed_bytes),
            len(deleted_paths),
            ", ".join(deleted_paths) if deleted_paths else str(dest_abs),
        )

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

        if op not in ("download", "touch", "delete"):
            self._post_status(item, "failed", error=f"Unknown op: {op}")
            return

        cancel_reason = item.get("cancelReason")
        item_state = item.get("state")
        if isinstance(cancel_reason, str) and cancel_reason.strip():
            if dep_id:
                self._forget_retry(dep_id)
            self._post_status(item, "cancelled", error=f"Dependency queue item cancelled: {cancel_reason.strip()}")
            return
        if isinstance(item_state, str) and item_state.lower() in NON_RETRYABLE_QUEUE_STATES:
            if dep_id:
                self._forget_retry(dep_id)
            return

        if op == "download":
            now = _now_ms()
            if dep_id:
                with self._lock:
                    retry_entry = self._state.retry.get(dep_id) if isinstance(self._state.retry.get(dep_id), dict) else None
                if retry_entry and self._clear_retry_if_forced(dep_id, item, retry_entry):
                    retry_entry = None
                next_at = int(retry_entry.get("nextAttemptAtMs")) if retry_entry and isinstance(retry_entry.get("nextAttemptAtMs"), int) else None
                last_err = retry_entry.get("lastError") if retry_entry and isinstance(retry_entry.get("lastError"), str) else ""
                if next_at is not None and now < next_at:
                    # Avoid hammering the same dep when we're intentionally backing off.
                    self._post_status(item, "retrying", error=self._format_backoff_error(dep_id, last_err, next_at))
                    return

            if dep_id:
                with self._lock:
                    # Fail closed while replacement bytes are in flight. The old
                    # implementation left the dependency advertised as installed,
                    # so a job could race a repair download and use corrupt bytes.
                    self._invalidate_dependency_verification_locked(dep_id)
                    self._downloading.add(dep_id)
                    self._save_state()
                resolved = item.get("resolved") if isinstance(item.get("resolved"), dict) else {}
                expected_size_raw = resolved.get("expectedSizeBytes") if isinstance(resolved, dict) else None
                expected_size_bytes = int(expected_size_raw) if isinstance(expected_size_raw, (int, float)) and expected_size_raw > 0 else 0
                dest_rel = resolved.get("destRelativePath") if isinstance(resolved, dict) and isinstance(resolved.get("destRelativePath"), str) else None
                self._update_download_activity(
                    dep_id,
                    dest_relative_path=dest_rel,
                    expected_bytes=expected_size_bytes,
                    downloaded_bytes=0,
                    stage="starting",
                    tool=self._resolve_download_tool(),
                )
            self._post_status(item, "running")

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
                err_msg = str(e)
                retryable = self._is_retryable_download_error(e, item)
                if dep_id:
                    self._clear_download_activity(dep_id)
                if retryable:
                    next_at = self._schedule_download_retry(item, e)
                    if dep_id:
                        logging.warning(
                            "Download failed; will retry. itemId=%s depId=%s nextAttemptInSec=%d err=%s",
                            item_id,
                            dep_id,
                            max(0, int((next_at - _now_ms()) / 1000)),
                            err_msg,
                        )
                    self._post_status(item, "retrying", error=self._format_backoff_error(dep_id or item_id, err_msg, next_at))
                    return

                with self._lock:
                    if dep_id:
                        self._state.retry.pop(dep_id, None)
                        self._state.failed.add(dep_id)
                        self._downloading.discard(dep_id)
                        self._save_state()
                logging.warning("Download failed (non-retryable) itemId=%s depId=%s: %s", item_id, dep_id, err_msg)
                status_response = self._post_status(item, "failed", error=err_msg)
                if (
                    dep_id
                    and isinstance(status_response, dict)
                    and status_response.get("clearFailedDepId") is True
                ):
                    # The backend failed the requesting job and cancelled this
                    # shared dependency item. Do not advertise a permanent local
                    # failure that would block a different queued job from
                    # re-queuing the same dependency.
                    self._forget_retry(dep_id)
                    try:
                        self._heartbeat(queue_depth=None)
                    except Exception:
                        pass
                return

        if op == "delete":
            self._post_status(item, "running")
            try:
                self._delete_item(item)
                self._post_status(item, "succeeded")
                try:
                    self._heartbeat(queue_depth=None)
                except Exception:
                    pass
            except Exception as e:
                self._post_status(item, "failed", error=str(e))
            return

        # touch. If the file was evicted after scheduling, promote this queue
        # item into the normal download path so retries/backoff remain intact.
        try:
            touch_requires_download = self._touch_target_requires_download(item)
        except Exception as e:
            self._post_status(item, "failed", error=str(e))
            return
        if touch_requires_download:
            logging.warning(
                "Touch found missing or size-mismatched dependency; downloading instead. depId=%s",
                dep_id,
            )
            self._process_item({**item, "op": "download"})
            return
        self._post_status(item, "running")
        try:
            self._touch_item(item)
            self._post_status(item, "succeeded")
            try:
                self._heartbeat(queue_depth=None)
            except Exception:
                pass
        except Exception as e:
            self._post_status(item, "failed", error=str(e))

    def _seconds_until_expiry(self, value: Optional[str]) -> Optional[int]:
        ms = _iso_to_ms(value)
        if ms is None:
            return None
        return int((int(ms) - int(self._server_now_ms())) / 1000)

    def _agent_refresh_urls(self, lease: AgentExecuteLease, command_state: Dict[str, Any]) -> None:
        url_refresh = command_state.get("urlRefresh")
        if not isinstance(url_refresh, dict):
            return
        refresh_token = url_refresh.get("refreshToken")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise RuntimeError("Missing refreshToken for url-refresh")

        endpoint = url_refresh.get("refreshEndpoint") if isinstance(url_refresh.get("refreshEndpoint"), str) else "/agent/url-refresh"
        body = {
            "schemaVersion": 1,
            "instanceId": self._resolved_instance_id,
            "jobId": lease.job_id,
            "executionAttempt": lease.execution_attempt,
            "attemptEpoch": lease.attempt_epoch,
            "leaseId": lease.lease_id,
            "refreshToken": refresh_token,
        }
        resp = self._agent_api("POST", endpoint, body=body, timeout_seconds=30.0, use_token=True, include_secret=True)
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        if isinstance(data.get("inputFiles"), list):
            command_state["inputFiles"] = data.get("inputFiles")
        if isinstance(data.get("outputTargets"), list):
            command_state["outputTargets"] = data.get("outputTargets")
        if isinstance(data.get("refreshToken"), str) and data.get("refreshToken"):
            url_refresh["refreshToken"] = data.get("refreshToken")
        if isinstance(data.get("refreshTokenExpiresAt"), str):
            url_refresh["refreshTokenExpiresAt"] = data.get("refreshTokenExpiresAt")

    def _agent_maybe_refresh_urls(self, lease: AgentExecuteLease, command_state: Dict[str, Any], force: bool = False) -> None:
        url_refresh = command_state.get("urlRefresh")
        if not isinstance(url_refresh, dict):
            return

        min_remaining_sec = url_refresh.get("minRemainingSec")
        if not isinstance(min_remaining_sec, (int, float)):
            min_remaining_sec = 180
        threshold = max(30, int(min_remaining_sec))

        expiring = force
        if not expiring:
            candidates: List[Optional[str]] = []
            input_files = command_state.get("inputFiles")
            if isinstance(input_files, list):
                for row in input_files:
                    if isinstance(row, dict) and isinstance(row.get("downloadUrlExpiresAt"), str):
                        candidates.append(row.get("downloadUrlExpiresAt"))
            output_targets = command_state.get("outputTargets")
            if isinstance(output_targets, list):
                for row in output_targets:
                    if not isinstance(row, dict):
                        continue
                    if isinstance(row.get("uploadUrlExpiresAt"), str):
                        candidates.append(row.get("uploadUrlExpiresAt"))
                    if isinstance(row.get("stagedUploadUrlExpiresAt"), str):
                        candidates.append(row.get("stagedUploadUrlExpiresAt"))
                    if isinstance(row.get("verifyHeadUrlExpiresAt"), str):
                        candidates.append(row.get("verifyHeadUrlExpiresAt"))
            if isinstance(url_refresh.get("refreshTokenExpiresAt"), str):
                candidates.append(url_refresh.get("refreshTokenExpiresAt"))

            for value in candidates:
                secs = self._seconds_until_expiry(value if isinstance(value, str) else None)
                if secs is not None and secs < threshold:
                    expiring = True
                    break

        if expiring:
            self._agent_refresh_urls(lease, command_state)

    def _select_output_ref(
        self,
        refs: List[Dict[str, str]],
        target: Dict[str, Any],
        used_indexes: Set[int],
    ) -> Optional[Tuple[int, Dict[str, str]]]:
        expected_filename = target.get("sourceFilename")
        if isinstance(expected_filename, str) and expected_filename:
            expected_basename = os.path.basename(expected_filename)
            for idx, row in enumerate(refs):
                if idx in used_indexes:
                    continue
                filename = row.get("filename")
                if isinstance(filename, str) and os.path.basename(filename) == expected_basename:
                    return idx, row
            if target.get("requireExactSourceFilename") is True:
                return None

        expected_ext = ""
        attempt_path = target.get("attemptObjectPath")
        if isinstance(attempt_path, str) and attempt_path:
            expected_ext = Path(attempt_path).suffix.lower()

        if expected_ext:
            for idx, row in enumerate(refs):
                if idx in used_indexes:
                    continue
                filename = row.get("filename")
                if isinstance(filename, str) and Path(filename).suffix.lower() == expected_ext:
                    return idx, row

        for idx, row in enumerate(refs):
            if idx in used_indexes:
                continue
            return idx, row
        return None

    def _upload_output_artifact(
        self,
        lease: AgentExecuteLease,
        target: Dict[str, Any],
        filename: str,
        local_output: Path,
        bytes_written: int,
        sha256_sum: str,
    ) -> Dict[str, Any]:
        logical_key = target.get("logicalOutputKey") if isinstance(target.get("logicalOutputKey"), str) else ""
        if not logical_key:
            logical_key = f"output_{Path(filename).stem}"

        content_type = target.get("contentType") if isinstance(target.get("contentType"), str) and target.get("contentType") else None
        if not content_type:
            guessed, _enc = mimetypes.guess_type(filename)
            content_type = guessed or "application/octet-stream"

        fast_path_max_bytes = target.get("fastPathMaxBytes")
        fast_path_threshold = int(fast_path_max_bytes) if isinstance(fast_path_max_bytes, (int, float)) else None
        staged_upload_url = target.get("stagedUploadUrl") if isinstance(target.get("stagedUploadUrl"), str) and target.get("stagedUploadUrl") else None
        staged_upload_method = target.get("stagedUploadMethod") if isinstance(target.get("stagedUploadMethod"), str) else ""
        should_stage = (
            fast_path_threshold is not None and
            int(bytes_written) > int(fast_path_threshold) and
            staged_upload_url is not None and
            staged_upload_method == "gcs_resumable_session_put"
        )

        attempt_object_path = (
            target.get("attemptObjectPath")
            if isinstance(target.get("attemptObjectPath"), str) and target.get("attemptObjectPath")
            else filename
        )
        final_object_path = (
            target.get("finalObjectPath")
            if isinstance(target.get("finalObjectPath"), str) and target.get("finalObjectPath")
            else attempt_object_path
        )

        if should_stage:
            upload_started_ms = _now_ms()
            gcs_resumable_upload_file(
                staged_upload_url,
                local_output,
                content_type,
                timeout_seconds=max(300.0, float(self.download_timeout_seconds)),
                chunk_size=8 * 1024 * 1024,
            )
            upload_ms = max(0, _now_ms() - upload_started_ms)
            out_meta: Dict[str, Any] = {
                "logicalOutputKey": logical_key,
                "attemptObjectPath": attempt_object_path,
                "finalObjectPath": final_object_path,
                "bytes": bytes_written,
                "sha256": sha256_sum,
                "contentType": content_type,
                "deliveryPath": "gcs_staged",
                "uploadTiming": {
                    "agentUploadMs": upload_ms,
                    "agentUploadAttempts": 1,
                    "deliveryPath": "gcs_staged",
                },
            }
            bucket = target.get("bucket") if isinstance(target.get("bucket"), str) and target.get("bucket") else None
            if bucket:
                out_meta["bucket"] = bucket
            source_filename = target.get("sourceFilename") if isinstance(target.get("sourceFilename"), str) and target.get("sourceFilename") else filename
            if source_filename:
                out_meta["sourceFilename"] = source_filename
            return out_meta

        upload_url = target.get("uploadUrl")
        if not isinstance(upload_url, str) or not upload_url:
            raise RuntimeError("Missing uploadUrl in output target.")

        upload_headers: Dict[str, str] = {"Content-Type": content_type}
        upload_headers_raw = target.get("uploadHeaders")
        if isinstance(upload_headers_raw, dict):
            for hk, hv in upload_headers_raw.items():
                if isinstance(hk, str) and hk and isinstance(hv, str):
                    upload_headers[hk] = hv

        upload_method = target.get("uploadMethod") if isinstance(target.get("uploadMethod"), str) else ""

        def build_upload_headers() -> Dict[str, str]:
            headers = dict(upload_headers)
            if upload_method == "agent_api_put":
                headers["X-DM-Instance-Id"] = str(self._resolved_instance_id or "")
                headers["X-Agent-Job-Id"] = lease.job_id
                headers["X-Agent-Execution-Attempt"] = str(int(lease.execution_attempt))
                headers["X-Agent-Attempt-Epoch"] = str(int(lease.attempt_epoch))
                headers["X-Agent-Lease-Id"] = lease.lease_id
                headers["X-Agent-Logical-Output-Key"] = logical_key
                headers["X-Agent-Source-Filename"] = filename
                if self._agent_access_token:
                    headers["Authorization"] = f"Bearer {self._agent_access_token}"
            return headers

        status = 0
        body = ""
        upload_attempts = max(1, int(getattr(self, "agent_upload_retry_attempts", 1) or 1))
        attempts_used = 0
        upload_started_ms = _now_ms()
        for attempt_idx in range(upload_attempts):
            attempts_used = attempt_idx + 1
            try:
                status, body, _resp_headers = http_put_file_stream(
                    upload_url,
                    local_output,
                    headers=build_upload_headers(),
                    timeout_seconds=max(120.0, float(self.download_timeout_seconds)),
                )
            except (OSError, socket.timeout, TimeoutError, http.client.HTTPException) as e:
                if attempt_idx >= upload_attempts - 1:
                    raise RuntimeError(f"Output upload network failure: {e}") from e
                self._sleep_agent_api_retry("output upload", attempt_idx, e)
                continue

            if status in (401, 403) and upload_method == "agent_api_put" and attempt_idx < upload_attempts - 1:
                self._agent_access_token = None
                self._agent_access_token_expires_at_ms = 0
                try:
                    self._agent_register()
                except Exception as refresh_err:
                    logging.warning("Agent token refresh before output upload retry failed: %s", refresh_err)
                self._sleep_agent_api_retry("output upload auth", attempt_idx, RuntimeError(f"status={status}"))
                continue

            if (status in RETRYABLE_HTTP_STATUS_CODES or status >= 500) and attempt_idx < upload_attempts - 1:
                self._sleep_agent_api_retry("output upload", attempt_idx, RuntimeError(f"status={status}: {body[:200]}"))
                continue
            break
        upload_ms = max(0, _now_ms() - upload_started_ms)
        if status == 412:
            verify_url = target.get("verifyHeadUrl")
            if isinstance(verify_url, str) and verify_url:
                head_status, head_headers = http_head(verify_url, timeout_seconds=30.0)
                if head_status in (200, 204):
                    remote_len = head_headers.get("content-length")
                    if isinstance(remote_len, str) and remote_len.isdigit() and int(remote_len) == bytes_written:
                        status = 200
        if status < 200 or status >= 300:
            raise RuntimeError(f"Output upload failed (status={status}): {body[:200]}")

        response_payload = _json_loads_or_none(body) if isinstance(body, str) and body else None
        response_data = response_payload.get("data") if isinstance(response_payload, dict) and isinstance(response_payload.get("data"), dict) else {}
        public_url = response_data.get("cdnUrl") if isinstance(response_data.get("cdnUrl"), str) and response_data.get("cdnUrl") else None
        backend_timing = response_data.get("uploadTiming") if isinstance(response_data.get("uploadTiming"), dict) else None

        out_meta = {
            "logicalOutputKey": logical_key,
            "attemptObjectPath": attempt_object_path,
            "finalObjectPath": (
                response_data.get("destinationPath")
                if isinstance(response_data.get("destinationPath"), str) and response_data.get("destinationPath")
                else final_object_path
            ),
            "bytes": bytes_written,
            "sha256": sha256_sum,
            "contentType": content_type,
            "deliveryPath": "direct_bunny",
            "uploadTiming": {
                "agentUploadMs": upload_ms,
                "agentUploadAttempts": attempts_used,
                "deliveryPath": "direct_bunny",
                **({"backend": backend_timing} if backend_timing else {}),
            },
            **({"publicUrl": public_url} if public_url else {}),
        }
        source_filename = target.get("sourceFilename") if isinstance(target.get("sourceFilename"), str) and target.get("sourceFilename") else filename
        if source_filename:
            out_meta["sourceFilename"] = source_filename
        return out_meta

    def _process_agent_execute_item(self, item: Dict[str, Any]) -> None:
        item_id = item.get("itemId")
        lease_id = item.get("leaseId")
        payload = item.get("payload")
        if not isinstance(item_id, str) or not item_id:
            return
        if not isinstance(lease_id, str) or not lease_id:
            return
        if not isinstance(payload, dict):
            try:
                self._agent_ack(item_id, lease_id, "command_ignored_stale")
            except Exception:
                pass
            return

        job_id = payload.get("jobId")
        execution_attempt = payload.get("executionAttempt")
        attempt_epoch = payload.get("attemptEpoch")
        if (
            not isinstance(job_id, str)
            or not job_id
            or not isinstance(execution_attempt, (int, float))
            or not isinstance(attempt_epoch, (int, float))
        ):
            try:
                self._agent_ack(item_id, lease_id, "command_ignored_stale")
            except Exception:
                pass
            return

        lease = AgentExecuteLease(
            item_id=item_id,
            lease_id=lease_id,
            job_id=job_id,
            execution_attempt=int(execution_attempt),
            attempt_epoch=int(attempt_epoch),
            started_at_ms=_now_ms(),
            command_id=payload.get("commandId") if isinstance(payload.get("commandId"), str) else "",
            payload=payload,
        )
        self._register_active_lease(lease)

        event_version = 0
        terminal_sent = False
        tmp_root = Path(tempfile.mkdtemp(prefix=f"agent_exec_{job_id}_{lease.execution_attempt}_{lease.attempt_epoch}_"))

        def emit(event_type: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            nonlocal event_version
            event_version += 1
            return self._agent_event(lease, event_version, event_type, payload=extra)

        def emit_best_effort(event_type: str, extra: Optional[Dict[str, Any]] = None) -> None:
            try:
                emit(event_type, extra)
            except Exception as e:
                logging.debug("Best-effort agent event failed: jobId=%s type=%s err=%s", lease.job_id, event_type, e)

        def emit_durable(event_type: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            nonlocal event_version
            event_version += 1
            durable_version = event_version
            attempts = max(1, int(getattr(self, "agent_terminal_event_retry_attempts", 1) or 1))
            last_error: Optional[Exception] = None
            for attempt_idx in range(attempts):
                try:
                    result = self._agent_event(lease, durable_version, event_type, payload=extra)
                    if result.get("accepted") is not True:
                        raise RuntimeError(
                            f"Durable agent event {event_type} was not accepted: {result.get('reason') or result}"
                        )
                    return result
                except Exception as e:
                    last_error = e
                    if attempt_idx >= attempts - 1 or not self._is_retryable_agent_control_error(e):
                        break
                    self._sleep_agent_api_retry(f"terminal event {event_type}", attempt_idx, e)
            if last_error:
                raise last_error
            raise RuntimeError(f"Durable agent event {event_type} failed without an error")

        try:
            if not self._coalesce_agent_lifecycle_events(lease):
                try:
                    emit("job_dispatched", {"commandId": lease.command_id} if lease.command_id else None)
                except Exception as e:
                    logging.debug("job_dispatched emit failed for %s: %s", lease.job_id, e)

            required_dep_ids_raw = payload.get("requiredDepIds")
            required_dep_ids = [d for d in required_dep_ids_raw if isinstance(d, str) and d] if isinstance(required_dep_ids_raw, list) else []
            timeouts = payload.get("timeouts") if isinstance(payload.get("timeouts"), dict) else {}
            dep_wait_timeout_sec = int(timeouts.get("dependencyWaitTimeoutSec")) if isinstance(timeouts.get("dependencyWaitTimeoutSec"), (int, float)) else 900
            execution_timeout_sec = int(timeouts.get("executionTimeoutSec")) if isinstance(timeouts.get("executionTimeoutSec"), (int, float)) else 2400

            if required_dep_ids:
                dep_wait_started = _now_ms()
                last_wait_emit_ms = 0
                while True:
                    if self._is_cancel_requested(lease):
                        self._comfy_interrupt()
                        emit_durable("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before execution started."})
                        terminal_sent = True
                        return

                    verified = self._verified_installed_dep_ids_for_execution(required_dep_ids)
                    missing = [dep for dep in required_dep_ids if dep not in verified]
                    if not missing:
                        break

                    now_ms = _now_ms()
                    if now_ms - dep_wait_started > max(1, dep_wait_timeout_sec) * 1000:
                        emit_durable(
                            "job_failed",
                            {
                                "errorCode": "dependencies_timeout",
                                "errorMessage": f"Dependencies did not become ready in {dep_wait_timeout_sec}s.",
                            },
                        )
                        terminal_sent = True
                        return

                    if last_wait_emit_ms == 0 or now_ms - last_wait_emit_ms >= self.agent_waiting_deps_event_ms:
                        emit_best_effort("waiting_dependencies", {"missingDepIds": missing[:200]})
                        last_wait_emit_ms = now_ms
                    time.sleep(self.agent_dependency_wait_poll_seconds)

            if self._is_cancel_requested(lease):
                self._comfy_interrupt()
                emit_durable("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before input preparation."})
                terminal_sent = True
                return

            command_state: Dict[str, Any] = {
                "inputFiles": payload.get("inputFiles") if isinstance(payload.get("inputFiles"), list) else [],
                "outputTargets": (
                    (payload.get("outputPlan") or {}).get("targets")
                    if isinstance(payload.get("outputPlan"), dict) and isinstance((payload.get("outputPlan") or {}).get("targets"), list)
                    else []
                ),
                "urlRefresh": payload.get("urlRefresh") if isinstance(payload.get("urlRefresh"), dict) else {},
            }
            self._agent_maybe_refresh_urls(lease, command_state, force=False)

            input_files_initial = command_state.get("inputFiles")
            if isinstance(input_files_initial, list) and input_files_initial:
                input_tmp_dir = tmp_root / "inputs"
                input_tmp_dir.mkdir(parents=True, exist_ok=True)

                for idx in range(len(input_files_initial)):
                    input_files_live = command_state.get("inputFiles")
                    if not isinstance(input_files_live, list) or idx >= len(input_files_live):
                        break
                    row = input_files_live[idx]
                    if not isinstance(row, dict):
                        continue
                    if self._is_cancel_requested(lease):
                        self._comfy_interrupt()
                        emit_durable("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested while downloading inputs."})
                        terminal_sent = True
                        return

                    self._agent_maybe_refresh_urls(lease, command_state, force=False)
                    download_url = row.get("downloadUrl")
                    if not isinstance(download_url, str) or not download_url:
                        raise RuntimeError(f"Input file #{idx} missing downloadUrl")

                    name = row.get("name") if isinstance(row.get("name"), str) and row.get("name") else f"input_{idx}"
                    temp_name = f"{idx:02d}_{os.path.basename(name)}"
                    local_path = input_tmp_dir / temp_name
                    http_download_to_file(
                        download_url,
                        local_path,
                        timeout_seconds=float(self.download_timeout_seconds),
                        chunk_size=int(self.download_chunk_size),
                    )
                    expected_sha = row.get("sha256")
                    if isinstance(expected_sha, str) and expected_sha:
                        actual_sha = sha256_file(local_path)
                        if actual_sha.lower() != expected_sha.lower():
                            raise RuntimeError(f"input_checksum_mismatch for {name}: expected {expected_sha} got {actual_sha}")

                    self._copy_input_to_comfy(local_path, name)

            lease.ready_at_ms = _now_ms()
            if not self._coalesce_agent_lifecycle_events(lease):
                emit_best_effort("inputs_ready", None)

            if self._is_cancel_requested(lease):
                self._comfy_interrupt()
                emit_durable("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before prompt submit."})
                terminal_sent = True
                return

            workflow = self._parse_workflow_from_payload(payload)
            self._ensure_runtime_assets_for_workflow(workflow)
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.stage = "executing"
            self._stop_idle_prl_mining_for_work("execute_job")
            lease.execute_started_at_ms = _now_ms()
            prompt_id = self._comfy_submit_prompt(workflow, client_id=f"{job_id}-{uuid.uuid4().hex[:12]}")
            lease.prompt_submitted_at_ms = _now_ms()
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.prompt_id = prompt_id

            if not self._coalesce_agent_lifecycle_events(lease):
                emit_best_effort("prompt_submitted", {"promptId": prompt_id})
                emit_durable("execution_started", {"promptId": prompt_id})

            start_exec_ms = _now_ms()
            last_progress_emit_ms = start_exec_ms
            history_entry: Dict[str, Any] = {}
            history_errors = 0
            while True:
                if self._is_cancel_requested(lease):
                    self._comfy_interrupt()
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_cancelled")
                    emit_durable(
                        "job_cancelled",
                        {
                            "promptId": prompt_id,
                            "errorCode": "cancel_requested",
                            "errorMessage": "Cancellation requested during execution.",
                        },
                    )
                    terminal_sent = True
                    return

                if _now_ms() - start_exec_ms > max(1, execution_timeout_sec) * 1000:
                    self._comfy_interrupt()
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_timeout")
                    emit_durable(
                        "job_failed",
                        {
                            "promptId": prompt_id,
                            "errorCode": "execution_timeout",
                            "errorMessage": f"Execution exceeded timeout ({execution_timeout_sec}s).",
                        },
                    )
                    terminal_sent = True
                    return

                try:
                    history_entry = self._comfy_get_history(prompt_id)
                    history_errors = 0
                except Exception as history_err:
                    history_errors += 1
                    if history_errors >= 10:
                        raise RuntimeError(f"Repeated /history lookup failures: {history_err}")
                    logging.debug("Transient /history lookup failure for prompt %s: %s", prompt_id, history_err)
                    time.sleep(0.5)
                    continue
                status_obj = history_entry.get("status") if isinstance(history_entry.get("status"), dict) else {}
                status_str = str(status_obj.get("status_str") or status_obj.get("status") or "").strip().lower()
                failed = status_obj.get("failed") is True or status_str in ("failed", "error")
                completed = status_obj.get("completed") is True or status_str in ("success", "succeeded", "completed")

                if failed:
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_failed")
                    emit_durable(
                        "job_failed",
                        {
                            "promptId": prompt_id,
                            "errorCode": "comfy_execution_failed",
                            "errorMessage": (json.dumps(status_obj)[:MAX_AGENT_ERROR_MESSAGE_CHARS] if status_obj else "ComfyUI execution failed."),
                        },
                    )
                    terminal_sent = True
                    return

                if completed:
                    break

                if _now_ms() - last_progress_emit_ms >= self.agent_progress_event_ms:
                    emit_best_effort("execution_progress", {"promptId": prompt_id})
                    last_progress_emit_ms = _now_ms()
                time.sleep(0.5)

            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.stage = "uploading"
            self._mark_agent_gpu_work_finished(lease, "comfy_execution_complete", final_stage="uploading")
            output_commit_payload: Dict[str, Any] = {"promptId": prompt_id}
            if self._coalesce_agent_lifecycle_events(lease):
                output_commit_payload["coalescedLifecycle"] = self._coalesced_agent_lifecycle_payload(lease)
            emit_durable("output_commit_started", output_commit_payload)

            output_targets_initial = command_state.get("outputTargets")
            if not isinstance(output_targets_initial, list) or len(output_targets_initial) == 0:
                raise RuntimeError("Missing output targets for execute_job command.")

            refs = self._collect_history_output_refs(history_entry)
            if not refs:
                raise RuntimeError("No output files found in ComfyUI history.")

            output_tmp_dir = tmp_root / "outputs"
            output_tmp_dir.mkdir(parents=True, exist_ok=True)
            used_ref_indexes: Set[int] = set()
            uploaded_outputs: List[Dict[str, Any]] = []

            for target_idx in range(len(output_targets_initial)):
                output_targets_live = command_state.get("outputTargets")
                if not isinstance(output_targets_live, list) or target_idx >= len(output_targets_live):
                    break
                target = output_targets_live[target_idx]
                if not isinstance(target, dict):
                    continue
                self._agent_maybe_refresh_urls(lease, command_state, force=False)
                output_targets_live = command_state.get("outputTargets")
                if not isinstance(output_targets_live, list) or target_idx >= len(output_targets_live):
                    break
                target = output_targets_live[target_idx]

                selected = self._select_output_ref(refs, target, used_ref_indexes)
                if selected is None:
                    if target.get("required") is True:
                        raise RuntimeError(
                            f"Required output not found in ComfyUI history: {target.get('sourceFilename') or target.get('logicalOutputKey')}"
                        )
                    continue
                ref_idx, ref = selected
                used_ref_indexes.add(ref_idx)

                filename = ref.get("filename") if isinstance(ref.get("filename"), str) else ""
                subfolder = ref.get("subfolder") if isinstance(ref.get("subfolder"), str) else ""
                file_type = ref.get("type") if isinstance(ref.get("type"), str) else "output"
                if not filename:
                    continue

                local_output = output_tmp_dir / f"{len(uploaded_outputs):02d}_{os.path.basename(filename)}"
                http_download_to_file(
                    self._comfy_view_url(filename=filename, subfolder=subfolder if subfolder else None, file_type=file_type),
                    local_output,
                    timeout_seconds=max(60.0, float(self.download_timeout_seconds)),
                    chunk_size=int(self.download_chunk_size),
                )
                bytes_written = int(local_output.stat().st_size)
                sha256_sum = sha256_file(local_output)
                out_meta = self._upload_output_artifact(
                    lease,
                    target,
                    filename,
                    local_output,
                    bytes_written,
                    sha256_sum,
                )
                uploaded_outputs.append(out_meta)
                try:
                    emit_best_effort("output_uploaded", out_meta)
                except Exception as e:
                    logging.debug(
                        "output_uploaded emit failed for %s/%s: %s",
                        lease.job_id,
                        out_meta.get("logicalOutputKey"),
                        e,
                    )

            if not uploaded_outputs:
                raise RuntimeError("No outputs were uploaded.")

            try:
                emit_durable("job_completed", {"promptId": prompt_id, "outputs": uploaded_outputs})
            except Exception as terminal_err:
                logging.error(
                    "job_completed terminal event failed after retries; leaving job for backend recovery instead of marking failed: jobId=%s err=%s",
                    lease.job_id,
                    terminal_err,
                )
                return
            terminal_sent = True
        except Exception as e:
            if not terminal_sent:
                event_type = "job_cancelled" if self._is_cancel_requested(lease) else "job_failed"
                err_code = "cancel_requested" if event_type == "job_cancelled" else "execution_error"
                error_text = str(e)
                node_contract: Optional[Dict[str, Any]] = None
                if event_type == "job_failed" and (
                    "missing_node_type" in error_text.lower()
                    or ("node" in error_text.lower() and "not found" in error_text.lower())
                ):
                    self._invalidate_local_node_contract_probe()
                    node_contract = self._local_node_contract_runtime(force=True)
                    self._remove_local_readiness_file()
                    err_code = "node_contract_missing"
                prompt_id = None
                with self._lock:
                    active = self._active_exec_by_item.get(lease.item_id)
                    if active and isinstance(active.prompt_id, str):
                        prompt_id = active.prompt_id
                payload: Dict[str, Any] = {
                    "errorCode": err_code,
                    "errorMessage": error_text[:MAX_AGENT_ERROR_MESSAGE_CHARS],
                }
                if node_contract is not None:
                    payload["nodeContract"] = node_contract
                if prompt_id:
                    payload["promptId"] = prompt_id
                try:
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_error")
                    emit_durable(event_type, payload)
                    terminal_sent = True
                except Exception as event_err:
                    logging.warning("Failed emitting terminal event for %s: %s", lease.job_id, event_err)
            logging.error(
                "execute_job failed: itemId=%s jobId=%s attempt=%d epoch=%d err=%s",
                lease.item_id,
                lease.job_id,
                int(lease.execution_attempt),
                int(lease.attempt_epoch),
                e,
            )
        finally:
            self._finish_active_lease(lease.item_id)
            self._request_agent_queue_poll()
            try:
                shutil.rmtree(str(tmp_root), ignore_errors=True)
            except Exception:
                pass

    def _submit_agent_prefetch(self, lease: AgentExecuteLease) -> None:
        if self._agent_prefetch_executor is None:
            raise RuntimeError("Agent prefetch executor is not initialized")
        future = self._agent_prefetch_executor.submit(self._prefetch_agent_execute_lease, lease)
        with self._lock:
            self._agent_prefetch_inflight.add(future)

    def _submit_agent_execute(self, lease: AgentExecuteLease) -> None:
        if self._agent_execute_executor is None:
            raise RuntimeError("Agent execute executor is not initialized")
        future = self._agent_execute_executor.submit(self._execute_agent_ready_lease, lease)
        with self._lock:
            self._agent_execute_inflight.add(future)

    def _submit_agent_upload(self, lease: AgentExecuteLease) -> None:
        if self._agent_upload_executor is None:
            raise RuntimeError("Agent upload executor is not initialized")
        lease.upload_enqueued_at_ms = _now_ms()
        future = self._agent_upload_executor.submit(self._upload_agent_outputs, lease)
        with self._lock:
            self._agent_upload_inflight.add(future)

    def _submit_agent_maintenance_item(self, item: Dict[str, Any]) -> None:
        item_type = item.get("type")
        is_prl_miner = item_type == "prl_miner"
        executor = self._agent_prl_miner_executor if is_prl_miner else self._agent_maintenance_executor
        if executor is None:
            raise RuntimeError("Agent PRL miner executor is not initialized" if is_prl_miner else "Agent maintenance executor is not initialized")
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""
        if item_id and lease_id:
            with self._lock:
                self._active_maintenance_by_item[item_id] = {
                    "itemId": item_id,
                    "leaseId": lease_id,
                    "stage": f"maintenance:{item_type}" if isinstance(item_type, str) and item_type else "maintenance",
                }

        def _run() -> None:
            try:
                if item_type == "restart_comfy":
                    self._agent_handle_restart_comfy_command(item)
                elif item_type == "prl_miner":
                    self._agent_handle_prl_miner_command(item)
                else:
                    self._process_install_node_bundles_item(item)
            finally:
                if item_id:
                    with self._lock:
                        active = self._active_maintenance_by_item.get(item_id)
                        if isinstance(active, dict) and active.get("leaseId") == lease_id:
                            self._active_maintenance_by_item.pop(item_id, None)

        future = executor.submit(_run)
        with self._lock:
            if is_prl_miner:
                self._agent_prl_miner_inflight.add(future)
            else:
                self._agent_maintenance_inflight.add(future)
        self._request_agent_queue_poll()

    def _prefetch_agent_execute_lease(self, lease: AgentExecuteLease) -> None:
        retain_lease = False
        terminal_sent = False
        try:
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if not active:
                    return
                active.stage = "prefetching"

            if not self._coalesce_agent_lifecycle_events(lease):
                try:
                    self._emit_agent_event(lease, "job_dispatched", {"commandId": lease.command_id} if lease.command_id else None)
                except Exception as e:
                    logging.debug("job_dispatched emit failed for %s: %s", lease.job_id, e)

            if self._is_cancel_requested(lease):
                self._emit_agent_event_durable(
                    lease,
                    "job_cancelled",
                    {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before input prefetch."},
                )
                terminal_sent = True
                return

            prefetched_inputs: List[Dict[str, Any]] = []
            input_files_initial = lease.command_state.get("inputFiles")
            if isinstance(input_files_initial, list) and input_files_initial:
                for idx in range(len(input_files_initial)):
                    input_files_live = lease.command_state.get("inputFiles")
                    if not isinstance(input_files_live, list) or idx >= len(input_files_live):
                        break
                    row = input_files_live[idx]
                    if not isinstance(row, dict):
                        continue
                    if self._is_cancel_requested(lease):
                        self._emit_agent_event_durable(
                            lease,
                            "job_cancelled",
                            {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested while prefetching inputs."},
                        )
                        terminal_sent = True
                        return
                    prefetched_inputs.append(self._ensure_cached_input(lease, row, idx))

            required_dep_ids_raw = lease.payload.get("requiredDepIds")
            required_dep_ids = [d for d in required_dep_ids_raw if isinstance(d, str) and d] if isinstance(required_dep_ids_raw, list) else []
            timeouts = lease.payload.get("timeouts") if isinstance(lease.payload.get("timeouts"), dict) else {}
            dep_wait_timeout_sec = int(timeouts.get("dependencyWaitTimeoutSec")) if isinstance(timeouts.get("dependencyWaitTimeoutSec"), (int, float)) else 900
            if required_dep_ids:
                dep_wait_started = _now_ms()
                last_wait_emit_ms = 0
                with self._lock:
                    active = self._active_exec_by_item.get(lease.item_id)
                    if active:
                        active.stage = "waiting_dependencies"
                while True:
                    if self._is_cancel_requested(lease):
                        self._emit_agent_event_durable(
                            lease,
                            "job_cancelled",
                            {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested while waiting for dependencies."},
                        )
                        terminal_sent = True
                        return

                    verified = self._verified_installed_dep_ids_for_execution(required_dep_ids)
                    missing = [dep for dep in required_dep_ids if dep not in verified]
                    if not missing:
                        break

                    now_ms = _now_ms()
                    if now_ms - dep_wait_started > max(1, dep_wait_timeout_sec) * 1000:
                        self._emit_agent_event_durable(
                            lease,
                            "job_failed",
                            {
                                "errorCode": "dependencies_timeout",
                                "errorMessage": f"Dependencies did not become ready in {dep_wait_timeout_sec}s.",
                            },
                        )
                        terminal_sent = True
                        return

                    if last_wait_emit_ms == 0 or now_ms - last_wait_emit_ms >= self.agent_waiting_deps_event_ms:
                        self._emit_agent_event_best_effort(lease, "waiting_dependencies", {"missingDepIds": missing[:200]})
                        last_wait_emit_ms = now_ms
                    time.sleep(self.agent_dependency_wait_poll_seconds)

            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if not active:
                    return
                active.prefetched_inputs = prefetched_inputs
                active.stage = "ready"
                active.ready_at_ms = _now_ms()
                self._enqueue_ready_locked(active)
            retain_lease = True
            self._stop_idle_prl_mining_for_work("execute_job")
            if not self._coalesce_agent_lifecycle_events(lease):
                try:
                    self._emit_agent_event_best_effort(lease, "inputs_ready", None)
                except Exception as e:
                    logging.debug("inputs_ready emit failed for %s: %s", lease.job_id, e)
            self._drain_ready_agent_leases()
            self._request_agent_queue_poll()
        except Exception as e:
            if not terminal_sent:
                event_type = "job_cancelled" if self._is_cancel_requested(lease) else "job_failed"
                err_code = "cancel_requested" if event_type == "job_cancelled" else "prefetch_error"
                try:
                    self._emit_agent_event_durable(
                        lease,
                        event_type,
                        {"errorCode": err_code, "errorMessage": str(e)[:MAX_AGENT_ERROR_MESSAGE_CHARS]},
                    )
                    terminal_sent = True
                except Exception as event_err:
                    logging.warning("Failed emitting prefetch terminal event for %s: %s", lease.job_id, event_err)
            logging.error(
                "execute_job prefetch failed: itemId=%s jobId=%s attempt=%d epoch=%d err=%s",
                lease.item_id,
                lease.job_id,
                int(lease.execution_attempt),
                int(lease.attempt_epoch),
                e,
            )
        finally:
            if not retain_lease:
                self._cleanup_agent_lease(lease)

    def _execute_agent_ready_lease(self, lease: AgentExecuteLease) -> None:
        retain_lease = False
        terminal_sent = False
        node_timing_collector: Optional[ComfyNodeTimingCollector] = None

        def attach_node_timings(payload: Dict[str, Any], terminal_status: str) -> None:
            if node_timing_collector is None:
                return
            history_terminal_ns = time.monotonic_ns()
            node_timing_collector.wait_terminal(timeout_seconds=0.1)
            if not node_timing_collector.wait_terminal(timeout_seconds=0.0):
                node_timing_collector.finish_from_history(terminal_status, observed_ns=history_terminal_ns)
            timing = node_timing_collector.snapshot()
            if timing.get("nodeCount", 0) or timing.get("cachedNodeCount", 0):
                payload["comfyNodeTimings"] = timing

        try:
            timeouts = lease.payload.get("timeouts") if isinstance(lease.payload.get("timeouts"), dict) else {}
            execution_timeout_sec = int(timeouts.get("executionTimeoutSec")) if isinstance(timeouts.get("executionTimeoutSec"), (int, float)) else 2400

            if self._is_cancel_requested(lease):
                self._comfy_interrupt()
                self._emit_agent_event_durable(
                    lease,
                    "job_cancelled",
                    {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before prompt submit."},
                )
                terminal_sent = True
                return

            # Finish all disk/dependency/input preparation before entering the
            # shared FIFO. A Comfy job becomes admission-eligible only when its
            # next meaningful step needs the GPU.
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                prefetched_inputs = list(active.prefetched_inputs) if active else list(lease.prefetched_inputs)

            for entry in prefetched_inputs:
                cache_path = entry.get("cache_path")
                input_name = entry.get("name")
                if not isinstance(cache_path, str) or not cache_path:
                    continue
                if not isinstance(input_name, str) or not input_name:
                    input_name = f"input_{uuid.uuid4().hex}"
                self._copy_input_to_comfy(Path(cache_path), input_name)

            workflow = self._parse_workflow_from_payload(lease.payload)
            self._ensure_runtime_assets_for_workflow(workflow)

            try:
                admission_claim_token = self._wait_for_comfy_gpu_admission(
                    lease,
                    estimated_duration_ms=max(1, execution_timeout_sec) * 1000,
                )
                self._stop_idle_prl_mining_for_work("execute_job")
                lease.gpu_coordinator_lease = self._acquire_gpu_lease(
                    "comfy",
                    lease.job_id,
                    metadata_provider=self._comfy_gpu_process_metadata,
                    admission_ticket_id=lease.gpu_admission_ticket_id,
                    admission_claim_token=admission_claim_token,
                )
            except GPUCoordinatorBusy as exc:
                self._release_comfy_gpu_admission(lease, "gpu_handoff_busy")
                self._defer_ready_lease_for_gpu_coordinator(
                    lease,
                    exc.retry_after_seconds,
                    str(exc),
                )
                retain_lease = True
                return
            except GPUCoordinatorUnavailable as exc:
                self._release_comfy_gpu_admission(lease, "gpu_handoff_unavailable")
                self._defer_ready_lease_for_gpu_coordinator(lease, 5.0, str(exc))
                retain_lease = True
                return
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                execute_started_at_ms = _now_ms()
                if active:
                    active.stage = "executing"
                    active.execute_started_at_ms = execute_started_at_ms
                lease.execute_started_at_ms = execute_started_at_ms
            if not self._local_comfy_reachable(timeout_seconds=5.0):
                logging.warning("Local ComfyUI is not reachable before prompt submit; restarting before jobId=%s", lease.job_id)
                self._restart_local_comfy_and_wait(
                    prefer_process_restart=True,
                    timeout_seconds=300.0,
                    skip_if_reachable=True,
                )
            # Pre-submit baseline. A job that OOMs mid-sample reports only the
            # torch-side numbers in its exception, so without this there is no
            # record of how much VRAM was already spoken for before it started,
            # nor of which flags the listener was actually running under.
            self._log_comfy_runtime_snapshot(
                f"job start jobId={lease.job_id}",
                self._comfy_runtime_snapshot(force_refresh=True),
            )
            client_id = f"{lease.job_id}-{uuid.uuid4().hex[:12]}"
            requested_prompt_id = str(uuid.uuid4()) if self.comfy_node_timing_enabled else None
            if requested_prompt_id is not None:
                node_timing_collector = self._start_comfy_node_timing_collector(
                    workflow,
                    client_id=client_id,
                    prompt_id=requested_prompt_id,
                )
            prompt_id = self._comfy_submit_prompt(
                workflow,
                client_id=client_id,
                prompt_id=requested_prompt_id,
            )
            if node_timing_collector is not None and prompt_id != requested_prompt_id:
                node_timing_collector.stop()
                node_timing_collector = None
            lease.prompt_submitted_at_ms = _now_ms()
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.prompt_id = prompt_id

            if not self._coalesce_agent_lifecycle_events(lease):
                self._emit_agent_event_durable(lease, "execution_started", {"promptId": prompt_id})

            start_exec_ms = _now_ms()
            last_progress_emit_ms = start_exec_ms
            history_entry: Dict[str, Any] = {}
            history_errors = 0
            while True:
                if self._is_cancel_requested(lease):
                    self._comfy_interrupt()
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_cancelled")
                    cancel_payload: Dict[str, Any] = {
                        "promptId": prompt_id,
                        "errorCode": "cancel_requested",
                        "errorMessage": "Cancellation requested during execution.",
                    }
                    attach_node_timings(cancel_payload, "cancelled")
                    self._emit_agent_event_durable(
                        lease,
                        "job_cancelled",
                        cancel_payload,
                    )
                    terminal_sent = True
                    return

                if _now_ms() - start_exec_ms > max(1, execution_timeout_sec) * 1000:
                    self._comfy_interrupt()
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_timeout")
                    timeout_payload: Dict[str, Any] = {
                        "promptId": prompt_id,
                        "errorCode": "execution_timeout",
                        "errorMessage": f"Execution exceeded timeout ({execution_timeout_sec}s).",
                    }
                    attach_node_timings(timeout_payload, "timeout")
                    self._emit_agent_event_durable(
                        lease,
                        "job_failed",
                        timeout_payload,
                    )
                    terminal_sent = True
                    return

                try:
                    history_entry = self._comfy_get_history(prompt_id)
                    history_errors = 0
                except Exception as history_err:
                    history_errors += 1
                    if history_errors >= 10:
                        raise RuntimeError(f"Repeated /history lookup failures: {history_err}")
                    logging.debug("Transient /history lookup failure for prompt %s: %s", prompt_id, history_err)
                    time.sleep(0.5)
                    continue
                status_obj = history_entry.get("status") if isinstance(history_entry.get("status"), dict) else {}
                status_str = str(status_obj.get("status_str") or status_obj.get("status") or "").strip().lower()
                failed = status_obj.get("failed") is True or status_str in ("failed", "error")
                completed = status_obj.get("completed") is True or status_str in ("success", "succeeded", "completed")

                if failed:
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_failed")
                    failed_payload: Dict[str, Any] = {
                        "promptId": prompt_id,
                        "errorCode": "comfy_execution_failed",
                        "errorMessage": (json.dumps(status_obj)[:MAX_AGENT_ERROR_MESSAGE_CHARS] if status_obj else "ComfyUI execution failed."),
                    }
                    attach_node_timings(failed_payload, "failed")
                    self._emit_agent_event_durable(
                        lease,
                        "job_failed",
                        failed_payload,
                    )
                    terminal_sent = True
                    return

                if completed:
                    break

                if _now_ms() - last_progress_emit_ms >= self.agent_progress_event_ms:
                    self._emit_agent_event_best_effort(lease, "execution_progress", {"promptId": prompt_id})
                    last_progress_emit_ms = _now_ms()
                time.sleep(0.5)

            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.stage = "uploading"
                    active.history_entry = history_entry
                    active.prompt_id = prompt_id
            lease.history_entry = history_entry
            lease.prompt_id = prompt_id
            self._arm_post_job_comfy_recycle(lease)
            self._mark_agent_gpu_work_finished(lease, "comfy_execution_complete", final_stage="uploading")
            output_commit_payload: Dict[str, Any] = {"promptId": prompt_id}
            if self._coalesce_agent_lifecycle_events(lease):
                output_commit_payload["coalescedLifecycle"] = self._coalesced_agent_lifecycle_payload(lease)
            attach_node_timings(output_commit_payload, "success")
            self._emit_agent_event_durable(lease, "output_commit_started", output_commit_payload)

            self._submit_agent_upload(lease)
            retain_lease = True
        except Exception as e:
            if not terminal_sent:
                event_type = "job_cancelled" if self._is_cancel_requested(lease) else "job_failed"
                err_code = "cancel_requested" if event_type == "job_cancelled" else "execution_error"
                error_text = str(e)
                node_contract: Optional[Dict[str, Any]] = None
                if event_type == "job_failed" and (
                    "missing_node_type" in error_text.lower()
                    or ("node" in error_text.lower() and "not found" in error_text.lower())
                ):
                    self._invalidate_local_node_contract_probe()
                    node_contract = self._local_node_contract_runtime(force=True)
                    self._remove_local_readiness_file()
                    err_code = "node_contract_missing"
                prompt_id = None
                with self._lock:
                    active = self._active_exec_by_item.get(lease.item_id)
                    if active and isinstance(active.prompt_id, str):
                        prompt_id = active.prompt_id
                payload: Dict[str, Any] = {
                    "errorCode": err_code,
                    "errorMessage": error_text[:MAX_AGENT_ERROR_MESSAGE_CHARS],
                }
                if node_contract is not None:
                    payload["nodeContract"] = node_contract
                if prompt_id:
                    payload["promptId"] = prompt_id
                attach_node_timings(payload, event_type)
                try:
                    self._mark_agent_gpu_work_finished(lease, "comfy_execution_error")
                    self._emit_agent_event_durable(lease, event_type, payload)
                    terminal_sent = True
                except Exception as event_err:
                    logging.warning("Failed emitting execute terminal event for %s: %s", lease.job_id, event_err)
            logging.error(
                "execute_job execute failed: itemId=%s jobId=%s attempt=%d epoch=%d err=%s",
                lease.item_id,
                lease.job_id,
                int(lease.execution_attempt),
                int(lease.attempt_epoch),
                e,
            )
        finally:
            if node_timing_collector is not None:
                node_timing_collector.stop()
            if not retain_lease:
                # No upload hand-off happened, so _upload_agent_outputs will
                # never run _complete_post_job_comfy_recycle for this lease.
                # Arm and complete inline so a job that died mid-sample cannot
                # leave its fragmented ComfyUI process serving the next job.
                try:
                    self._arm_post_job_comfy_recycle(lease, outcome="failure")
                    self._complete_post_job_comfy_recycle(lease)
                except Exception as recycle_exc:
                    logging.warning(
                        "Post-failure ComfyUI recycle raised for jobId=%s itemId=%s: %s",
                        lease.job_id,
                        lease.item_id,
                        recycle_exc,
                    )
                self._cleanup_agent_lease(lease)

    def _upload_agent_outputs(self, lease: AgentExecuteLease) -> None:
        terminal_sent = False
        output_commit_started_ms = _now_ms()
        lease.upload_started_at_ms = output_commit_started_ms
        upload_worker_queue_ms = (
            max(0, output_commit_started_ms - int(lease.upload_enqueued_at_ms))
            if int(lease.upload_enqueued_at_ms or 0) > 0
            else 0
        )
        try:
            output_targets_initial = lease.command_state.get("outputTargets")
            if not isinstance(output_targets_initial, list) or len(output_targets_initial) == 0:
                raise RuntimeError("Missing output targets for execute_job command.")

            history_entry = lease.history_entry if isinstance(lease.history_entry, dict) else {}
            refs = self._collect_history_output_refs(history_entry)
            if not refs:
                raise RuntimeError("No output files found in ComfyUI history.")

            tmp_root = Path(lease.tmp_root) if isinstance(lease.tmp_root, str) and lease.tmp_root else Path(tempfile.mkdtemp(prefix=f"agent_exec_{lease.job_id}_upload_"))
            output_tmp_dir = tmp_root / "outputs"
            output_tmp_dir.mkdir(parents=True, exist_ok=True)
            used_ref_indexes: Set[int] = set()
            uploaded_outputs: List[Dict[str, Any]] = []

            for target_idx in range(len(output_targets_initial)):
                if self._is_cancel_requested(lease):
                    raise RuntimeError(lease.cancel_reason or "lease_stale_during_upload")

                output_targets_live = lease.command_state.get("outputTargets")
                if not isinstance(output_targets_live, list) or target_idx >= len(output_targets_live):
                    break
                target = output_targets_live[target_idx]
                if not isinstance(target, dict):
                    continue
                self._agent_maybe_refresh_urls(lease, lease.command_state, force=False)
                output_targets_live = lease.command_state.get("outputTargets")
                if not isinstance(output_targets_live, list) or target_idx >= len(output_targets_live):
                    break
                target = output_targets_live[target_idx]

                selected = self._select_output_ref(refs, target, used_ref_indexes)
                if selected is None:
                    continue
                ref_idx, ref = selected
                used_ref_indexes.add(ref_idx)

                filename = ref.get("filename") if isinstance(ref.get("filename"), str) else ""
                subfolder = ref.get("subfolder") if isinstance(ref.get("subfolder"), str) else ""
                file_type = ref.get("type") if isinstance(ref.get("type"), str) else "output"
                if not filename:
                    continue

                local_output = output_tmp_dir / f"{len(uploaded_outputs):02d}_{os.path.basename(filename)}"
                download_started_ms = _now_ms()
                http_download_to_file(
                    self._comfy_view_url(filename=filename, subfolder=subfolder if subfolder else None, file_type=file_type),
                    local_output,
                    timeout_seconds=max(60.0, float(self.download_timeout_seconds)),
                    chunk_size=int(self.download_chunk_size),
                )
                download_ms = max(0, _now_ms() - download_started_ms)
                bytes_written = int(local_output.stat().st_size)
                hash_started_ms = _now_ms()
                sha256_sum = sha256_file(local_output)
                hash_ms = max(0, _now_ms() - hash_started_ms)
                out_meta = self._upload_output_artifact(
                    lease,
                    target,
                    filename,
                    local_output,
                    bytes_written,
                    sha256_sum,
                )
                timing = out_meta.get("uploadTiming") if isinstance(out_meta.get("uploadTiming"), dict) else {}
                out_meta["uploadTiming"] = {
                    **timing,
                    "agentDownloadFromComfyMs": download_ms,
                    "agentHashMs": hash_ms,
                    "agentUploadWorkerQueueMs": upload_worker_queue_ms,
                }
                uploaded_outputs.append(out_meta)
                try:
                    self._emit_agent_event_best_effort(lease, "output_uploaded", out_meta)
                except Exception as e:
                    logging.debug(
                        "output_uploaded emit failed for %s/%s: %s",
                        lease.job_id,
                        out_meta.get("logicalOutputKey"),
                        e,
                    )

            if not uploaded_outputs:
                raise RuntimeError("No outputs were uploaded.")

            completion_payload: Dict[str, Any] = {
                "outputs": uploaded_outputs,
                "agentTiming": {
                    "uploadWorkerQueueMs": upload_worker_queue_ms,
                    "outputCommitToTerminalEventStartMs": max(0, _now_ms() - output_commit_started_ms),
                    "uploadWorkerCapacity": int(getattr(self, "agent_max_upload_workers", 0) or 0),
                },
            }
            if isinstance(lease.prompt_id, str) and lease.prompt_id:
                completion_payload["promptId"] = lease.prompt_id
            try:
                terminal_emit_started_ms = _now_ms()
                self._emit_agent_event_durable(lease, "job_completed", completion_payload)
                logging.info(
                    "Agent job_completed event posted: jobId=%s terminalEventMs=%d uploadWorkerQueueMs=%d outputs=%d",
                    lease.job_id,
                    max(0, _now_ms() - terminal_emit_started_ms),
                    upload_worker_queue_ms,
                    len(uploaded_outputs),
                )
            except Exception as terminal_err:
                logging.error(
                    "job_completed terminal event failed after retries; leaving job for backend recovery instead of marking failed: jobId=%s err=%s",
                    lease.job_id,
                    terminal_err,
                )
                return
            terminal_sent = True
        except Exception as e:
            if not terminal_sent:
                error_code = "upload_error"
                error_text = str(e)
                payload: Dict[str, Any] = {
                    "errorCode": error_code,
                    "errorMessage": error_text[:MAX_AGENT_ERROR_MESSAGE_CHARS],
                }
                if isinstance(lease.prompt_id, str) and lease.prompt_id:
                    payload["promptId"] = lease.prompt_id
                try:
                    self._emit_agent_event_durable(lease, "job_failed", payload)
                    terminal_sent = True
                except Exception as event_err:
                    logging.warning("Failed emitting upload terminal event for %s: %s", lease.job_id, event_err)
            logging.error(
                "execute_job upload failed: itemId=%s jobId=%s attempt=%d epoch=%d err=%s",
                lease.item_id,
                lease.job_id,
                int(lease.execution_attempt),
                int(lease.attempt_epoch),
                e,
            )
        finally:
            self._complete_post_job_comfy_recycle(lease)
            self._cleanup_agent_lease(lease)

    def _process_agent_queue_item(self, item: Dict[str, Any]) -> None:
        item_type = item.get("type") if isinstance(item.get("type"), str) else ""
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""

        if item_type == "execute_job":
            if not item_id or not lease_id:
                return
            payload = item.get("payload")
            if not isinstance(payload, dict):
                try:
                    self._agent_ack(item_id, lease_id, "command_ignored_stale")
                except Exception:
                    pass
                return

            job_id = payload.get("jobId")
            execution_attempt = payload.get("executionAttempt")
            attempt_epoch = payload.get("attemptEpoch")
            if (
                not isinstance(job_id, str)
                or not job_id
                or not isinstance(execution_attempt, (int, float))
                or not isinstance(attempt_epoch, (int, float))
            ):
                try:
                    self._agent_ack(item_id, lease_id, "command_ignored_stale")
                except Exception:
                    pass
                return
            lease_decision = self._prepare_logical_execute_lease(
                item_id,
                job_id,
                int(execution_attempt),
                int(attempt_epoch),
            )
            if lease_decision != "accept" and lease_decision != "accept_superseding":
                logging.warning(
                    "Ignored duplicate/stale logical execute lease without disturbing active work: itemId=%s jobId=%s attempt=%d epoch=%d decision=%s",
                    item_id,
                    job_id,
                    int(execution_attempt),
                    int(attempt_epoch),
                    lease_decision,
                )
                return
            if self.mining_only:
                lease = AgentExecuteLease(
                    item_id=item_id,
                    lease_id=lease_id,
                    job_id=job_id,
                    execution_attempt=int(execution_attempt),
                    attempt_epoch=int(attempt_epoch),
                    started_at_ms=_now_ms(),
                    lease_order=self._next_agent_lease_order(),
                    command_id=payload.get("commandId") if isinstance(payload.get("commandId"), str) else "",
                    payload=payload,
                )
                self._register_active_lease(lease)
                try:
                    self._emit_agent_event(lease, "job_dispatched", {"commandId": lease.command_id} if lease.command_id else None)
                    self._emit_agent_event_durable(
                        lease,
                        "job_failed",
                        {
                            "errorCode": "mining_only_instance",
                            "errorMessage": "This instance is configured for PRL mining only and does not execute jobs.",
                        },
                    )
                finally:
                    self._cleanup_agent_lease(lease)
                return
            lease = AgentExecuteLease(
                item_id=item_id,
                lease_id=lease_id,
                job_id=job_id,
                execution_attempt=int(execution_attempt),
                attempt_epoch=int(attempt_epoch),
                started_at_ms=_now_ms(),
                lease_order=self._next_agent_lease_order(),
                command_id=payload.get("commandId") if isinstance(payload.get("commandId"), str) else "",
                payload=payload,
                command_state={
                    "inputFiles": payload.get("inputFiles") if isinstance(payload.get("inputFiles"), list) else [],
                    "outputTargets": (
                        (payload.get("outputPlan") or {}).get("targets")
                        if isinstance(payload.get("outputPlan"), dict) and isinstance((payload.get("outputPlan") or {}).get("targets"), list)
                        else []
                    ),
                    "urlRefresh": payload.get("urlRefresh") if isinstance(payload.get("urlRefresh"), dict) else {},
                },
                tmp_root=str(tempfile.mkdtemp(prefix=f"agent_exec_{job_id}_{int(execution_attempt)}_{int(attempt_epoch)}_")),
            )
            self._register_active_lease(lease)
            self._submit_agent_prefetch(lease)
            return

        if item_type == "cancel_job":
            self._agent_handle_cancel_command(item)
            return

        if item_type == "install_node_bundles":
            self._submit_agent_maintenance_item(item)
            return

        if item_type == "restart_comfy":
            self._submit_agent_maintenance_item(item)
            return

        if item_type == "prl_miner":
            self._submit_agent_maintenance_item(item)
            return

        if item_id and lease_id:
            try:
                self._agent_ack(
                    item_id,
                    lease_id,
                    "command_failed",
                    error_code="unknown_command_type",
                    error_message=f"Unsupported command type '{item_type}'",
                )
            except Exception as e:
                logging.warning("Failed to ack unknown command type for item=%s: %s", item_id, e)

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
        logging.info("Allowed dependency download domains: %s", ",".join(sorted(self.allowed_domains)))
        if self.input_allowed_domains:
            logging.info("Allowed input prefetch download domains: %s", ",".join(sorted(self.input_allowed_domains)))
        else:
            logging.info("Allowed input prefetch download domains: unrestricted")
        logging.info(
            "Download settings: tool=%s (resolved=%s) timeout=%.1fs chunkMiB=%d verbose=%s debug=%s",
            self.download_tool,
            self._resolve_download_tool(),
            float(self.download_timeout_seconds),
            int(self.download_chunk_size / (1024 * 1024)),
            "yes" if self.verbose_progress else "no",
            "yes" if self.download_debug else "no",
        )
        logging.info("Dependency polling every %.1fs, dependency heartbeat every %.1fs, max_parallel_downloads=%d", self.poll_seconds, self.heartbeat_seconds, self.max_parallel)
        logging.info(
            "Agent control: enabled=%s poll=%.1fs activeHeartbeat=%.1fs idleHeartbeat=%.1fs queueWait=%ds rtdbSignalWait=%s rtdbSignalSafetyMin=%.1fs fullCapacityPoll=%.1fs progressEvent=%.1fs waitingDepsEvent=%.1fs localComfy=%s readinessFile=%s maxExecWorkers=%d maxUploadWorkers=%d restartComfyAfterSuccessfulJob=%s restartComfyAfterFailedJob=%s miningOnly=%s",
            "yes" if self.agent_control_enabled else "no",
            self.agent_poll_seconds,
            self.agent_heartbeat_seconds,
            self.agent_idle_heartbeat_seconds,
            int(self.agent_queue_wait_sec),
            "yes" if self.agent_rtdb_signal_wait_enabled else "no",
            float(self.agent_rtdb_signal_safety_min_seconds),
            self.agent_full_capacity_poll_seconds,
            self.agent_progress_event_ms / 1000.0,
            self.agent_waiting_deps_event_ms / 1000.0,
            self.agent_local_comfy_base_url,
            self.agent_local_readiness_file,
            int(self.agent_max_execute_workers),
            int(self.agent_max_upload_workers),
            "yes" if self.restart_comfy_after_successful_job else "no",
            "yes" if self.restart_comfy_after_failed_job else "no",
            "yes" if self.mining_only else "no",
        )
        logging.info(
            "Self-update: enabled=%s allowDowngrade=%s scriptPath=%s retrySeconds=%.0f",
            "yes" if self.self_update_enabled else "no",
            "yes" if self.self_update_allow_downgrade else "no",
            str(self.self_script_path),
            float(self.self_update_retry_seconds),
        )
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

        self._repair_video_gen_v2_comfy_launch_contract(restart_if_unreachable=True)
        self._repair_comfy_watchdog_process_isolation()

        dep_executor = ThreadPoolExecutor(max_workers=self.max_parallel)
        dep_inflight: Set[Future[None]] = set()
        agent_aux_workers = max(2, int(self.agent_max_execute_workers))
        self._agent_prefetch_executor = ThreadPoolExecutor(max_workers=agent_aux_workers)
        self._agent_execute_executor = ThreadPoolExecutor(max_workers=max(1, int(self.agent_max_execute_workers)))
        self._agent_upload_executor = ThreadPoolExecutor(max_workers=max(1, int(self.agent_max_upload_workers)))
        self._agent_maintenance_executor = ThreadPoolExecutor(max_workers=1)
        self._agent_prl_miner_executor = ThreadPoolExecutor(max_workers=1)
        with self._lock:
            self._agent_prefetch_inflight.clear()
            self._agent_execute_inflight.clear()
            self._agent_upload_inflight.clear()
            self._agent_maintenance_inflight.clear()
            self._agent_prl_miner_inflight.clear()

        next_dep_poll_at_ms = 0
        next_agent_poll_at_ms = 0

        # Best-effort early register for agent control channel.
        self._maybe_register_agent_control()

        while not self._stop.is_set():
            try:
                now = _now_ms()
                if now >= self._next_interrupted_comfy_restart_recovery_ms:
                    self._next_interrupted_comfy_restart_recovery_ms = now + 5_000
                    try:
                        self._recover_interrupted_comfy_restart_readiness()
                    except Exception as exc:
                        logging.warning("Interrupted ComfyUI restart recovery failed: %s", exc)
                    try:
                        self._repair_orphaned_readiness_marker()
                    except Exception as exc:
                        logging.warning("Orphaned readiness marker repair failed: %s", exc)
                agent_poll_wakeup_requested = False
                if self._dependency_poll_wakeup.is_set():
                    self._dependency_poll_wakeup.clear()
                    next_dep_poll_at_ms = 0
                if self._agent_poll_wakeup.is_set():
                    self._agent_poll_wakeup.clear()
                    next_agent_poll_at_ms = 0
                    agent_poll_wakeup_requested = True

                # Keep both worker sets clean.
                done_dep = {f for f in dep_inflight if f.done()}
                dep_inflight -= done_dep
                for f in done_dep:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error("Unhandled dependency worker error: %s", e)

                for inflight_name, label in (
                    ("_agent_prefetch_inflight", "prefetch"),
                    ("_agent_execute_inflight", "execute"),
                    ("_agent_upload_inflight", "upload"),
                    ("_agent_maintenance_inflight", "maintenance"),
                    ("_agent_prl_miner_inflight", "prl_miner"),
                ):
                    with self._lock:
                        inflight = getattr(self, inflight_name)
                        done = {f for f in inflight if f.done()}
                        inflight -= done
                    for f in done:
                        try:
                            f.result()
                        except Exception as e:
                            logging.error("Unhandled agent %s worker error: %s", label, e)

                self._resume_idle_prl_mining_if_idle("agent_loop_idle")

                # Heartbeats.
                if not self.mining_only and now - self._last_heartbeat_ms >= int(self.heartbeat_seconds * 1000):
                    try:
                        self._heartbeat(queue_depth=None)
                    except ApiError as e:
                        if e.status in (401, 403):
                            logging.warning("Dependency heartbeat unauthorized (status=%d); re-registering dependency channel.", e.status)
                            try:
                                self._register()
                            except Exception as re:
                                logging.error("Dependency re-register failed: %s", re)
                        else:
                            logging.error("Dependency heartbeat API error: %s", e)
                    except Exception as e:
                        logging.error("Dependency heartbeat failed: %s", e)

                self._maybe_register_agent_control()
                if self.agent_control_enabled and self._agent_channel_supported and self._agent_access_token:
                    agent_heartbeat_interval_seconds = self._agent_heartbeat_interval_seconds()
                    if now - self._last_agent_heartbeat_ms >= int(agent_heartbeat_interval_seconds * 1000):
                        try:
                            self._agent_heartbeat()
                        except ApiError as e:
                            if e.status in (401, 403):
                                logging.warning("Agent heartbeat unauthorized (status=%d); token refresh required.", e.status)
                                self._agent_access_token = None
                                self._agent_access_token_expires_at_ms = 0
                                self._last_agent_heartbeat_ms = now
                            else:
                                logging.warning("Agent heartbeat API error (status=%d): %s", e.status, e)
                                self._last_agent_heartbeat_ms = now
                        except Exception as e:
                            logging.warning("Agent heartbeat failed: %s", e)
                            self._last_agent_heartbeat_ms = now

                    self._drain_ready_agent_leases()

                with self._lock:
                    active_leases = list(self._active_exec_by_item.values())
                    downloading_count = len(self._downloading)
                    maintenance_count = len(self._agent_maintenance_inflight)
                    prl_miner_command_count = len(self._agent_prl_miner_inflight)
                pending_self_update = self._pending_self_update is not None

                if pending_self_update and not active_leases and len(dep_inflight) == 0 and downloading_count == 0 and maintenance_count == 0 and prl_miner_command_count == 0:
                    # A failed or backoff-delayed self-update must not stall queue intake.
                    # Keep the process working unless _perform_pending_self_update() actually
                    # execs into the new script.
                    self._perform_pending_self_update()

                # A pending self-update must not drain the instance or pause queue intake.
                # Otherwise a failed or deferred update can leave the process heartbeating
                # but refusing both dependency work and agent jobs indefinitely.
                # Only perform the restart when the process is actually idle.
                if not self.mining_only and now >= next_dep_poll_at_ms:
                    available_dep_slots = _available_dependency_slots(self.max_parallel, len(dep_inflight))
                    items: List[Dict[str, Any]] = []
                    if available_dep_slots > 0:
                        try:
                            # Direct RTDB reads claim every returned item. Never claim
                            # more work than the local executor can start, or the excess
                            # claims sit locally, expire, and churn through stale recovery.
                            items = self._fetch_queue(limit=available_dep_slots)
                        except ApiError as e:
                            if e.status in (401, 403):
                                logging.warning("Dependency queue unauthorized (status=%d); re-registering.", e.status)
                                try:
                                    self._register()
                                except Exception as re:
                                    logging.error("Dependency re-register failed: %s", re)
                                items = []
                            else:
                                raise

                    queue_depth = len(items)
                    queued_dep_ids: Set[str] = set()
                    for it in items:
                        d = it.get("depId")
                        if isinstance(d, str) and d:
                            queued_dep_ids.add(d)
                    if queue_depth > 0 and now - self._last_heartbeat_ms >= int(5 * 1000):
                        try:
                            self._heartbeat(queue_depth=queue_depth + len(dep_inflight))
                        except Exception:
                            pass

                    due_retry_items: List[Dict[str, Any]] = []
                    retry_candidates: List[Tuple[str, Dict[str, Any]]] = []
                    retry_changed = False
                    # Fresh queue claims already reserve executor capacity. Local
                    # retries may use only the slots that remain after those claims.
                    retry_cap = max(0, available_dep_slots - len(items))
                    if retry_cap > 0:
                        with self._lock:
                            downloading_now = set(self._downloading)
                            for dep_id, entry in list(self._state.retry.items()):
                                if len(due_retry_items) >= retry_cap:
                                    break
                                if dep_id in queued_dep_ids or dep_id in downloading_now:
                                    continue
                                if not isinstance(entry, dict):
                                    continue
                                next_at = entry.get("nextAttemptAtMs")
                                if not isinstance(next_at, int) or next_at > now:
                                    continue
                                resolved = entry.get("resolved")
                                if not isinstance(resolved, dict):
                                    self._state.retry.pop(dep_id, None)
                                    retry_changed = True
                                    continue
                                retry_candidates.append((dep_id, dict(entry)))
                            if retry_changed:
                                self._save_state()

                    for dep_id, entry in retry_candidates:
                        if len(due_retry_items) >= retry_cap:
                            break
                        try:
                            current_item = self._local_retry_item_if_current(dep_id, entry)
                        except ApiError as e:
                            if e.status in (401, 403):
                                logging.warning("Dependency retry validation unauthorized (status=%d); re-registering.", e.status)
                                try:
                                    self._register()
                                except Exception as re:
                                    logging.error("Dependency re-register failed: %s", re)
                                break
                            logging.warning("Dependency retry validation API error for %s: %s", dep_id, e)
                            break
                        except Exception as e:
                            logging.warning("Dependency retry validation failed for %s: %s", dep_id, e)
                            break
                        if current_item is not None:
                            due_retry_items.append(current_item)

                    for it in due_retry_items:
                        if self._stop.is_set() or len(dep_inflight) >= self.max_parallel:
                            break
                        dep_inflight.add(dep_executor.submit(self._process_item, it))

                    for item in items:
                        if self._stop.is_set():
                            break
                        if len(dep_inflight) >= self.max_parallel:
                            break
                        dep_inflight.add(dep_executor.submit(self._process_item, item))

                    next_dep_poll_at_ms = now + int(max(0.2, float(self._coordination_dependency_poll_seconds())) * 1000)

                # Agent queue polling/dispatch.
                if (
                    self.agent_control_enabled
                    and self._agent_channel_supported
                    and self._agent_access_token
                    and now >= next_agent_poll_at_ms
                ):
                    with self._lock:
                        active_execute_count, active_prefetch_count, active_upload_count = self._agent_stage_counts_locked()
                        has_ready_items = any(
                            (
                                (lease := self._active_exec_by_item.get(item_id)) is not None and
                                lease.stage == "ready"
                            )
                            for item_id in self._ready_agent_item_ids
                        )
                    execute_capacity = self._agent_effective_execute_capacity()
                    prefetch_capacity = self._agent_effective_prefetch_capacity()
                    execute_and_prefetch_budget = max(
                        0,
                        (execute_capacity + prefetch_capacity) -
                        (active_execute_count + active_prefetch_count),
                    )
                    if execute_and_prefetch_budget <= 0 and not agent_poll_wakeup_requested:
                        next_agent_poll_at_ms = now + int(self.agent_full_capacity_poll_seconds * 1000)
                        continue

                    poll_limit = max(1, min(20, execute_and_prefetch_budget + 2))

                    queue_wait_sec = self.agent_queue_wait_sec
                    if (
                        has_ready_items
                        or active_execute_count > 0
                        or active_prefetch_count > 0
                        or active_upload_count > 0
                        or maintenance_count > 0
                    ):
                        # Once an instance is hot, keep queue fetches non-blocking so the
                        # main loop can react immediately when execution capacity opens up.
                        queue_wait_sec = 0
                    elif self._coordination_should_use_safety_polls():
                        # RTDB wakeups already signal new queue work; safety polls should
                        # avoid adding their own long-poll tail.
                        queue_wait_sec = 0

                    try:
                        agent_items = self._agent_fetch_queue(limit=poll_limit, wait_sec=queue_wait_sec)
                    except ApiError as e:
                        if e.status in (401, 403):
                            logging.warning("Agent queue unauthorized (status=%d); forcing agent re-register.", e.status)
                            self._agent_access_token = None
                            self._agent_access_token_expires_at_ms = 0
                            agent_items = []
                        elif e.status in (404, 405):
                            self._agent_channel_supported = False
                            logging.warning("Agent queue endpoint unavailable (status=%d). Disabling agent control channel for this process.", e.status)
                            agent_items = []
                        else:
                            raise

                    for item in agent_items:
                        if self._stop.is_set():
                            break
                        try:
                            self._process_agent_queue_item(item)
                        except Exception as e:
                            item_id = item.get("itemId")
                            lease_id = item.get("leaseId")
                            logging.warning("Failed processing agent queue item %s: %s", item_id, e)
                            if isinstance(item_id, str) and isinstance(lease_id, str):
                                try:
                                    self._agent_ack(
                                        item_id,
                                        lease_id,
                                        "command_failed",
                                        error_code="agent_processing_error",
                                        error_message=str(e),
                                    )
                                except Exception:
                                    pass

                    next_agent_poll_at_ms = now + int(max(0.2, float(self._coordination_agent_poll_seconds())) * 1000)

                wait_seconds = max(0.05, 0.5 + random.uniform(-0.05, 0.05))
                self._loop_wakeup.wait(timeout=wait_seconds)
                self._loop_wakeup.clear()
            except Exception as e:
                logging.error("Main loop error: %s", e)
                _sleep_with_jitter(5.0)

        try:
            self._idle_prl_miner.stop_if_running("agent_shutdown")
        except Exception as e:
            logging.warning("Failed stopping idle PRL miner during shutdown: %s", e)

        dep_executor.shutdown(wait=False, cancel_futures=True)
        if self._agent_prefetch_executor is not None:
            self._agent_prefetch_executor.shutdown(wait=False, cancel_futures=True)
        if self._agent_execute_executor is not None:
            self._agent_execute_executor.shutdown(wait=False, cancel_futures=True)
        if self._agent_upload_executor is not None:
            self._agent_upload_executor.shutdown(wait=False, cancel_futures=True)
        if self._agent_maintenance_executor is not None:
            self._agent_maintenance_executor.shutdown(wait=False, cancel_futures=True)
        if self._agent_prl_miner_executor is not None:
            self._agent_prl_miner_executor.shutdown(wait=False, cancel_futures=True)
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

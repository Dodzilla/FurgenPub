#!/usr/bin/env python3
"""
Furgen Content Server - Dependency Manager Agent (Vast.ai instances)

This is a single-file agent intended to run on each ComfyUI instance.
It polls the backend for dependency queue items and downloads missing artifacts
into the ComfyUI workspace, reporting status + inventory back to the backend.

Backend endpoints used (relative to FCS_API_BASE_URL, which should end with /api):
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
  - DM_ALLOWED_DOMAINS      (comma-separated allowlist; default: huggingface.co,hf.co,civitai.com)
  - DM_DOWNLOAD_TOOL             (default: wget; options: wget, python)
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
  - DM_AGENT_POLL_SECONDS         (poll cadence for /agent/queue; default: 1)
  - DM_AGENT_HEARTBEAT_SECONDS    (heartbeat cadence for /agent/heartbeat; default: 5)
  - DM_AGENT_QUEUE_WAIT_SEC       (long-poll waitSec for /agent/queue; default: 2)
  - DM_LOCAL_COMFY_BASE_URL       (local ComfyUI URL; default: http://127.0.0.1:8188)
  - DM_LOCAL_READINESS_FILE       (readiness marker file in Comfy input dir; default: provisioning_complete.txt)
  - DM_AGENT_MAX_EXEC_WORKERS     (local execute_job worker cap; default: 2)

Queue item expectations:
  - The backend should include a `resolved` object for download items with:
      { url, auth, destRelativePath, sha256?, expectedSizeBytes?, kind? }
  - For touch items, `resolved` should include at least:
      { destRelativePath, kind }
    The backend in this repo enriches /dependencies/queue responses accordingly.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import logging
import mimetypes
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


AGENT_VERSION = "dm-agent-py/0.4.1"


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


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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
        try:
            dest_partial.unlink()
        except Exception:
            pass
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
        try:
            dest_partial.unlink()
        except Exception:
            pass
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

    proc = subprocess.run(cmd + [url], capture_output=True, text=True)

    stderr = proc.stderr or ""
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
            raise RuntimeError(
                f"Incomplete download for {safe_url}: got {actual_size} bytes, expected {int(expected_size_bytes)} bytes"
            )


def http_download_to_file(
    url: str,
    dest_path: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 60.0,
    chunk_size: int = 8 * 1024 * 1024,
) -> None:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        with dest_path.open("wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)


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


def http_put_file_stream(
    url: str,
    file_path: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 300.0,
    chunk_size: int = 8 * 1024 * 1024,
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

    req_headers = {"Content-Length": str(int(file_path.stat().st_size))}
    if headers:
        req_headers.update(headers)

    try:
        conn.putrequest("PUT", path_with_query)
        for k, v in req_headers.items():
            conn.putheader(k, v)
        conn.endheaders()

        with file_path.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                conn.send(chunk)

        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        out_headers = {k.lower(): v for k, v in dict(resp.headers).items()}
        return int(resp.status), raw, out_headers
    finally:
        try:
            conn.close()
        except Exception:
            pass


@dataclass
class LocalState:
    installed_static: Set[str]
    installed_dynamic: Set[str]
    failed: Set[str]
    # For dynamic deps only: depId -> {destRelativePath, sizeBytes, lastTouchedAtMs}
    lru: Dict[str, Dict[str, Any]]
    # Download retry schedule: depId -> {itemId, resolved, attempts, nextAttemptAtMs, lastError, lastAttemptAtMs}
    retry: Dict[str, Dict[str, Any]]

    @staticmethod
    def empty() -> "LocalState":
        return LocalState(installed_static=set(), installed_dynamic=set(), failed=set(), lru={}, retry={})


@dataclass
class AgentExecuteLease:
    item_id: str
    lease_id: str
    job_id: str
    execution_attempt: int
    attempt_epoch: int
    started_at_ms: int
    command_id: str = ""
    cancel_requested: bool = False
    cancel_reason: str = ""
    prompt_id: Optional[str] = None


class DependencyAgent:
    def __init__(self) -> None:
        self.api_base_url = (_env_str("FCS_API_BASE_URL") or "").rstrip("/")
        self.server_type = (_env_str("SERVER_TYPE") or "").strip()
        self.shared_secret = _env_str("DEPENDENCY_MANAGER_SHARED_SECRET")
        self.instance_bootstrap_token = _env_str("DM_INSTANCE_BOOTSTRAP_TOKEN") or _env_str("AGENT_INSTANCE_BOOTSTRAP_TOKEN")
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
        self.download_debug = _env_bool("DM_DOWNLOAD_DEBUG", False)
        self.download_tool = (_env_str("DM_DOWNLOAD_TOOL") or "wget").strip().lower()

        self.download_timeout_seconds = max(30.0, min(3600.0, _env_float("DM_DOWNLOAD_TIMEOUT_SECONDS", 300.0)))
        chunk_mib = _env_int("DM_DOWNLOAD_CHUNK_MIB", 1)
        chunk_mib = max(1, min(32, chunk_mib))
        self.download_chunk_size = int(chunk_mib) * 1024 * 1024

        # Agent control channel knobs (execute pull mode).
        self.agent_control_enabled = _env_bool("DM_AGENT_CONTROL_ENABLED", True)
        self.agent_poll_seconds = max(0.5, _env_float("DM_AGENT_POLL_SECONDS", 1.0))
        self.agent_heartbeat_seconds = max(2.0, _env_float("DM_AGENT_HEARTBEAT_SECONDS", 5.0))
        self.agent_queue_wait_sec = max(0, min(20, _env_int("DM_AGENT_QUEUE_WAIT_SEC", 2)))
        self.agent_local_comfy_base_url = (_env_str("DM_LOCAL_COMFY_BASE_URL", "http://127.0.0.1:8188") or "http://127.0.0.1:8188").rstrip("/")
        self.agent_local_readiness_file = _env_str("DM_LOCAL_READINESS_FILE", "provisioning_complete.txt") or "provisioning_complete.txt"
        self.agent_max_execute_workers = max(1, min(8, _env_int("DM_AGENT_MAX_EXEC_WORKERS", 2)))

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

        self._agent_access_token: Optional[str] = None
        self._agent_access_token_expires_at_ms = 0
        self._agent_bootstrap_profile: Dict[str, Any] = {}
        self._agent_server_time_offset_ms = 0
        self._agent_channel_supported = self.agent_control_enabled
        self._next_agent_register_attempt_ms = 0
        self._last_agent_heartbeat_ms = 0
        self._agent_max_concurrent_execute_jobs = 1
        self._active_exec_by_item: Dict[str, AgentExecuteLease] = {}

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

    # Include shared secret by default to avoid token races causing 401 loops
    # (token is still used when present; backend accepts either).
    def _headers(self, use_token: bool = True, include_secret: bool = True) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if use_token and self._token:
            h["Authorization"] = f"Bearer {self._token}"
        if include_secret and self.shared_secret:
            h["X-DM-Secret"] = self.shared_secret
        return h

    def _agent_headers(self, use_token: bool = True, include_secret: bool = False) -> Dict[str, str]:
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

    def _agent_api(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 30.0,
        use_token: bool = True,
        include_secret: bool = False,
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

        status, resp = api_json(
            method,
            url,
            body=body,
            headers=self._agent_headers(use_token=use_token, include_secret=include_secret),
            timeout_seconds=timeout_seconds,
        )
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

    def _local_readiness_file_path(self) -> Path:
        candidate = Path(self.agent_local_readiness_file)
        if candidate.is_absolute():
            return candidate
        return self.comfyui_dir / "input" / self.agent_local_readiness_file

    def _local_readiness_file_present(self) -> bool:
        try:
            return self._local_readiness_file_path().exists()
        except Exception:
            return False

    def _local_comfy_reachable(self, timeout_seconds: float = 2.0) -> bool:
        url = f"{self.agent_local_comfy_base_url}/queue"
        try:
            status, _resp = api_json("GET", url, timeout_seconds=timeout_seconds)
            return status == 200
        except Exception:
            return False

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
            return LocalState(installed_static=installed_static, installed_dynamic=installed_dynamic, failed=failed, lru=lru, retry=retry)
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

    def _agent_effective_execute_capacity(self) -> int:
        return max(1, min(int(self.agent_max_execute_workers), int(self._agent_max_concurrent_execute_jobs)))

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
                "downloadTool": self.download_tool,
            },
        }
        if self.instance_bootstrap_token:
            body["instanceBootstrapToken"] = self.instance_bootstrap_token
        elif self._token:
            # Reuse dependency-channel token as bootstrap proof when a dedicated
            # bootstrap token is not explicitly provided.
            body["instanceBootstrapToken"] = self._token

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
        if isinstance(max_concurrent, (int, float)) and max_concurrent > 0:
            self._agent_max_concurrent_execute_jobs = max(1, int(max_concurrent))
        else:
            self._agent_max_concurrent_execute_jobs = 1

        self._agent_access_token = token
        self._agent_access_token_expires_at_ms = int(token_exp_ms)
        self._agent_bootstrap_profile = profile
        self._agent_channel_supported = True
        logging.info(
            "Registered agent control channel: instanceId=%s maxConcurrentExecuteJobs=%d tokenExpiresAt=%s",
            self._resolved_instance_id,
            int(self._agent_max_concurrent_execute_jobs),
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
            include_secret=False,
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
            body["errorMessage"] = str(error_message)[:500]
        if tuple_fields:
            for key in ("jobId", "executionAttempt", "attemptEpoch"):
                if key in tuple_fields:
                    body[key] = tuple_fields[key]
        resp = self._agent_api("POST", "/agent/ack", body=body, timeout_seconds=30.0, use_token=True, include_secret=False)
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
        if payload:
            for k, v in payload.items():
                body[k] = v

        digest_body = {k: v for k, v in body.items() if k != "eventDigest"}
        body["eventDigest"] = _sha256_hex_bytes(_canonical_json_bytes(digest_body))

        resp = self._agent_api("POST", "/agent/event", body=body, timeout_seconds=30.0, use_token=True, include_secret=False)
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

    def _collect_active_leases(self) -> List[Dict[str, Any]]:
        with self._lock:
            active = list(self._active_exec_by_item.values())
        out: List[Dict[str, Any]] = []
        for lease in active:
            out.append(
                {
                    "itemId": lease.item_id,
                    "leaseId": lease.lease_id,
                    "jobId": lease.job_id,
                    "executionAttempt": lease.execution_attempt,
                    "attemptEpoch": lease.attempt_epoch,
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

        held_leases = self._collect_active_leases()
        local_comfy = self._local_comfy_reachable()
        readiness_present = self._local_readiness_file_present()
        queue_depth = len(held_leases)

        body: Dict[str, Any] = {
            "schemaVersion": 1,
            "instanceId": self._resolved_instance_id,
            "localComfyReachable": bool(local_comfy),
            "localReadinessFilePresent": bool(readiness_present),
            "localReadinessFile": self.agent_local_readiness_file,
            "queueDepth": int(queue_depth),
            "heldLeases": held_leases,
            "runningItemIds": [row["itemId"] for row in held_leases if isinstance(row.get("itemId"), str)],
            "maxConcurrentExecuteJobs": int(self._agent_effective_execute_capacity()),
            "agentVersion": AGENT_VERSION,
            "capabilities": {
                "dependencyChannel": True,
                "agentPullExecution": True,
            },
        }
        resp = self._agent_api("POST", "/agent/heartbeat", body=body, timeout_seconds=30.0, use_token=True, include_secret=False)
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        self._last_agent_heartbeat_ms = _now_ms()

        lease_results = data.get("leases")
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

        cancel_signals = data.get("cancelSignals")
        if isinstance(cancel_signals, list):
            for signal in cancel_signals:
                if isinstance(signal, dict):
                    self._mark_cancel_signal(signal)
        return data

    def _register_active_lease(self, lease: AgentExecuteLease) -> None:
        with self._lock:
            self._active_exec_by_item[lease.item_id] = lease

    def _finish_active_lease(self, item_id: str) -> None:
        with self._lock:
            self._active_exec_by_item.pop(item_id, None)

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

    def _current_installed_dep_ids(self) -> Set[str]:
        with self._lock:
            self._reconcile_lru_locked()
            out = set(self._state.installed_static)
            out.update(self._state.installed_dynamic)
        return out

    def _comfy_api_json(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 30.0,
    ) -> Tuple[int, Optional[Any]]:
        ep = endpoint if endpoint.startswith("/") else "/" + endpoint
        return api_json(method, f"{self.agent_local_comfy_base_url}{ep}", body=body, timeout_seconds=timeout_seconds)

    def _comfy_submit_prompt(self, workflow: Dict[str, Any], client_id: str) -> str:
        status, resp = self._comfy_api_json(
            "POST",
            "/prompt",
            body={"prompt": workflow, "client_id": client_id},
            timeout_seconds=60.0,
        )
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected /prompt response: {status} {resp}")
        prompt_id = resp.get("prompt_id")
        if not isinstance(prompt_id, str) or not prompt_id:
            raise RuntimeError(f"/prompt did not return prompt_id: {resp}")
        return prompt_id

    def _comfy_get_history(self, prompt_id: str) -> Dict[str, Any]:
        status, resp = self._comfy_api_json("GET", f"/history/{urllib.parse.quote(prompt_id)}", timeout_seconds=30.0)
        if status != 200 or not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected /history response: {status} {resp}")
        entry = resp.get(prompt_id)
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
        raise RuntimeError(f"Unsupported workflowRef.mode: {mode}")

    def _copy_input_to_comfy(self, source_path: Path, desired_name: str) -> Path:
        safe_name = os.path.basename(desired_name) or f"input_{uuid.uuid4().hex}"
        dest = self.comfyui_dir / "input" / safe_name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(dest))
        return dest

    def _comfy_view_url(self, filename: str, subfolder: Optional[str], file_type: str = "output") -> str:
        params: List[Tuple[str, str]] = [("filename", filename), ("type", file_type)]
        if subfolder:
            params.append(("subfolder", subfolder))
        return f"{self.agent_local_comfy_base_url}/view?{urllib.parse.urlencode(params)}"

    def _collect_history_output_refs(self, history_entry: Dict[str, Any]) -> List[Dict[str, str]]:
        outputs = history_entry.get("outputs")
        if not isinstance(outputs, dict):
            return []

        refs: List[Dict[str, str]] = []
        seen: Set[str] = set()
        preferred_keys = ("images", "gifs", "videos", "audios", "audio", "latents", "meshes", "files")

        def _push(row: Dict[str, Any]) -> None:
            filename = row.get("filename")
            if not isinstance(filename, str) or not filename:
                return
            subfolder = row.get("subfolder") if isinstance(row.get("subfolder"), str) else ""
            file_type = row.get("type") if isinstance(row.get("type"), str) and row.get("type") else "output"
            dedupe = f"{file_type}:{subfolder}:{filename}"
            if dedupe in seen:
                return
            seen.add(dedupe)
            refs.append({"filename": filename, "subfolder": subfolder, "type": file_type})

        for _node_id, node_output in sorted(outputs.items(), key=lambda kv: kv[0]):
            if not isinstance(node_output, dict):
                continue
            for key in preferred_keys:
                value = node_output.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _push(item)
            for value in node_output.values():
                if isinstance(value, dict):
                    _push(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _push(item)
        return refs

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

    def _is_retryable_download_error(self, err: Exception) -> bool:
        msg = str(err).lower()
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

    def _compute_retry_delay_seconds(self, attempts: int, last_error: str) -> float:
        # Exponential backoff with jitter. Capped so we keep making progress.
        base = 120.0  # 2 minutes
        cap = 30.0 * 60.0  # 30 minutes
        delay = min(cap, base * (2 ** max(0, int(attempts) - 1)))

        le = (last_error or "").lower()
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

    def _schedule_download_retry(self, item: Dict[str, Any], err: Exception) -> int:
        dep_id = item.get("depId")
        if not isinstance(dep_id, str) or not dep_id:
            return _now_ms() + 60_000

        resolved = item.get("resolved")
        if not isinstance(resolved, dict):
            # We can't retry without resolved info; keep it as a hard failure.
            return _now_ms() + 60_000

        err_msg = str(err)[:500]
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
                        self._state.retry.pop(dep_id, None)
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
                    self._state.retry.pop(dep_id, None)
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
            if self.download_tool == "wget":
                wget_download(
                    url=url,
                    dest_partial=partial,
                    auth_header=auth_header,
                    expected_size_bytes=int(expected_size_bytes),
                    timeout_seconds=float(self.download_timeout_seconds),
                    allowed_domains=self.allowed_domains,
                    debug=self.download_debug,
                )
            elif self.download_tool == "python":
                http_download(
                    url=url,
                    dest_partial=partial,
                    auth_header=auth_header,
                    expected_size_bytes=int(expected_size_bytes),
                    timeout_seconds=float(self.download_timeout_seconds),
                    chunk_size=int(self.download_chunk_size),
                    allowed_domains=self.allowed_domains,
                    verbose=self.verbose_progress,
                    debug=self.download_debug,
                )
            else:
                raise RuntimeError(f"Unsupported DM_DOWNLOAD_TOOL: {self.download_tool}")

            if isinstance(sha256_expected, str) and sha256_expected:
                actual = sha256_file(partial)
                if actual.lower() != sha256_expected.lower():
                    try:
                        if partial.exists():
                            partial.unlink()
                    except Exception:
                        pass
                    raise RuntimeError(f"sha256 mismatch for {dep_id}: expected {sha256_expected}, got {actual}")
        except Exception as e:
            # Keep partial downloads for retryable errors so future retries can resume.
            retryable = self._is_retryable_download_error(e)
            if not retryable:
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
            self._state.retry.pop(dep_id, None)
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
            now = _now_ms()
            if dep_id:
                with self._lock:
                    retry_entry = self._state.retry.get(dep_id) if isinstance(self._state.retry.get(dep_id), dict) else None
                    next_at = int(retry_entry.get("nextAttemptAtMs")) if retry_entry and isinstance(retry_entry.get("nextAttemptAtMs"), int) else None
                    last_err = retry_entry.get("lastError") if retry_entry and isinstance(retry_entry.get("lastError"), str) else ""
                if next_at is not None and now < next_at:
                    # Avoid hammering the same dep when we're intentionally backing off.
                    self._post_status(item, "retrying", error=self._format_backoff_error(dep_id, last_err, next_at))
                    return

            if dep_id:
                with self._lock:
                    self._downloading.add(dep_id)
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
                retryable = self._is_retryable_download_error(e)
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
                        self._state.failed.add(dep_id)
                        self._downloading.discard(dep_id)
                        self._save_state()
                logging.warning("Download failed (non-retryable) itemId=%s depId=%s: %s", item_id, dep_id, err_msg)
                self._post_status(item, "failed", error=err_msg)
                return

        # touch
        self._post_status(item, "running")
        try:
            self._touch_item(item)
            self._post_status(item, "succeeded")
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
        resp = self._agent_api("POST", endpoint, body=body, timeout_seconds=30.0, use_token=True, include_secret=False)
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
        )
        self._register_active_lease(lease)

        event_version = 0
        terminal_sent = False
        tmp_root = Path(tempfile.mkdtemp(prefix=f"agent_exec_{job_id}_{lease.execution_attempt}_{lease.attempt_epoch}_"))

        def emit(event_type: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            nonlocal event_version
            event_version += 1
            return self._agent_event(lease, event_version, event_type, payload=extra)

        try:
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
                        emit("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before execution started."})
                        terminal_sent = True
                        return

                    installed = self._current_installed_dep_ids()
                    missing = [dep for dep in required_dep_ids if dep not in installed]
                    if not missing:
                        break

                    now_ms = _now_ms()
                    if now_ms - dep_wait_started > max(1, dep_wait_timeout_sec) * 1000:
                        emit(
                            "job_failed",
                            {
                                "errorCode": "dependencies_timeout",
                                "errorMessage": f"Dependencies did not become ready in {dep_wait_timeout_sec}s.",
                            },
                        )
                        terminal_sent = True
                        return

                    if last_wait_emit_ms == 0 or now_ms - last_wait_emit_ms >= 15_000:
                        emit("waiting_dependencies", {"missingDepIds": missing[:200]})
                        last_wait_emit_ms = now_ms
                    time.sleep(2.0)

            if self._is_cancel_requested(lease):
                self._comfy_interrupt()
                emit("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before input preparation."})
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
                        emit("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested while downloading inputs."})
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

            emit("inputs_ready", None)

            if self._is_cancel_requested(lease):
                self._comfy_interrupt()
                emit("job_cancelled", {"errorCode": "cancel_requested", "errorMessage": "Cancellation requested before prompt submit."})
                terminal_sent = True
                return

            workflow = self._parse_workflow_from_payload(payload)
            prompt_id = self._comfy_submit_prompt(workflow, client_id=f"{job_id}-{uuid.uuid4().hex[:12]}")
            with self._lock:
                active = self._active_exec_by_item.get(lease.item_id)
                if active:
                    active.prompt_id = prompt_id

            emit("prompt_submitted", {"promptId": prompt_id})
            emit("execution_started", {"promptId": prompt_id})

            start_exec_ms = _now_ms()
            last_progress_emit_ms = 0
            history_entry: Dict[str, Any] = {}
            history_errors = 0
            while True:
                if self._is_cancel_requested(lease):
                    self._comfy_interrupt()
                    emit(
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
                    emit(
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
                    time.sleep(2.0)
                    continue
                status_obj = history_entry.get("status") if isinstance(history_entry.get("status"), dict) else {}
                status_str = str(status_obj.get("status_str") or status_obj.get("status") or "").strip().lower()
                failed = status_obj.get("failed") is True or status_str in ("failed", "error")
                completed = status_obj.get("completed") is True or status_str in ("success", "succeeded", "completed")

                if failed:
                    emit(
                        "job_failed",
                        {
                            "promptId": prompt_id,
                            "errorCode": "comfy_execution_failed",
                            "errorMessage": (json.dumps(status_obj)[:500] if status_obj else "ComfyUI execution failed."),
                        },
                    )
                    terminal_sent = True
                    return

                if completed:
                    break

                if _now_ms() - last_progress_emit_ms >= 15_000:
                    emit("execution_progress", {"promptId": prompt_id})
                    last_progress_emit_ms = _now_ms()
                time.sleep(2.0)

            emit("output_commit_started", {"promptId": prompt_id})

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

                upload_url = target.get("uploadUrl")
                if not isinstance(upload_url, str) or not upload_url:
                    raise RuntimeError("Missing uploadUrl in output target.")
                content_type = target.get("contentType") if isinstance(target.get("contentType"), str) and target.get("contentType") else None
                if not content_type:
                    guessed, _enc = mimetypes.guess_type(filename)
                    content_type = guessed or "application/octet-stream"

                status, body, _resp_headers = http_put_file_stream(
                    upload_url,
                    local_output,
                    headers={"Content-Type": content_type},
                    timeout_seconds=max(120.0, float(self.download_timeout_seconds)),
                )
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

                logical_key = target.get("logicalOutputKey") if isinstance(target.get("logicalOutputKey"), str) else f"output_{len(uploaded_outputs)}"
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

                out_meta = {
                    "logicalOutputKey": logical_key,
                    "attemptObjectPath": attempt_object_path,
                    "finalObjectPath": final_object_path,
                    "bytes": bytes_written,
                    "sha256": sha256_sum,
                    "contentType": content_type,
                }
                uploaded_outputs.append(out_meta)
                try:
                    emit("output_uploaded", out_meta)
                except Exception as e:
                    logging.debug("output_uploaded emit failed for %s/%s: %s", lease.job_id, logical_key, e)

            if not uploaded_outputs:
                raise RuntimeError("No outputs were uploaded.")

            emit("job_completed", {"promptId": prompt_id, "outputs": uploaded_outputs})
            terminal_sent = True
        except Exception as e:
            if not terminal_sent:
                event_type = "job_cancelled" if self._is_cancel_requested(lease) else "job_failed"
                err_code = "cancel_requested" if event_type == "job_cancelled" else "execution_error"
                prompt_id = None
                with self._lock:
                    active = self._active_exec_by_item.get(lease.item_id)
                    if active and isinstance(active.prompt_id, str):
                        prompt_id = active.prompt_id
                payload: Dict[str, Any] = {
                    "errorCode": err_code,
                    "errorMessage": str(e)[:500],
                }
                if prompt_id:
                    payload["promptId"] = prompt_id
                try:
                    emit(event_type, payload)
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
            try:
                shutil.rmtree(str(tmp_root), ignore_errors=True)
            except Exception:
                pass

    def _process_agent_queue_item(
        self,
        item: Dict[str, Any],
        agent_executor: ThreadPoolExecutor,
        agent_inflight: Set[Future[None]],
    ) -> None:
        item_type = item.get("type") if isinstance(item.get("type"), str) else ""
        item_id = item.get("itemId") if isinstance(item.get("itemId"), str) else ""
        lease_id = item.get("leaseId") if isinstance(item.get("leaseId"), str) else ""

        if item_type == "execute_job":
            if not item_id or not lease_id:
                return
            with self._lock:
                if item_id in self._active_exec_by_item:
                    # Duplicate lease delivery for an already-active item.
                    try:
                        self._agent_ack(item_id, lease_id, "command_ignored_stale")
                    except Exception:
                        pass
                    return
            agent_inflight.add(agent_executor.submit(self._process_agent_execute_item, item))
            return

        if item_type == "cancel_job":
            self._agent_handle_cancel_command(item)
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
        logging.info("Allowed download domains: %s", ",".join(sorted(self.allowed_domains)))
        logging.info(
            "Download settings: tool=%s timeout=%.1fs chunkMiB=%d verbose=%s debug=%s",
            self.download_tool,
            float(self.download_timeout_seconds),
            int(self.download_chunk_size / (1024 * 1024)),
            "yes" if self.verbose_progress else "no",
            "yes" if self.download_debug else "no",
        )
        logging.info("Dependency polling every %.1fs, dependency heartbeat every %.1fs, max_parallel_downloads=%d", self.poll_seconds, self.heartbeat_seconds, self.max_parallel)
        logging.info(
            "Agent control: enabled=%s poll=%.1fs heartbeat=%.1fs queueWait=%ds localComfy=%s readinessFile=%s maxExecWorkers=%d",
            "yes" if self.agent_control_enabled else "no",
            self.agent_poll_seconds,
            self.agent_heartbeat_seconds,
            int(self.agent_queue_wait_sec),
            self.agent_local_comfy_base_url,
            self.agent_local_readiness_file,
            int(self.agent_max_execute_workers),
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

        dep_executor = ThreadPoolExecutor(max_workers=self.max_parallel)
        dep_inflight: Set[Future[None]] = set()
        agent_executor = ThreadPoolExecutor(max_workers=self.agent_max_execute_workers)
        agent_inflight: Set[Future[None]] = set()

        next_dep_poll_at_ms = 0
        next_agent_poll_at_ms = 0

        # Best-effort early register for agent control channel.
        self._maybe_register_agent_control()

        while not self._stop.is_set():
            try:
                now = _now_ms()

                # Keep both worker sets clean.
                done_dep = {f for f in dep_inflight if f.done()}
                dep_inflight -= done_dep
                for f in done_dep:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error("Unhandled dependency worker error: %s", e)

                done_agent = {f for f in agent_inflight if f.done()}
                agent_inflight -= done_agent
                for f in done_agent:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error("Unhandled agent worker error: %s", e)

                # Heartbeats.
                if now - self._last_heartbeat_ms >= int(self.heartbeat_seconds * 1000):
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
                    if now - self._last_agent_heartbeat_ms >= int(self.agent_heartbeat_seconds * 1000):
                        try:
                            self._agent_heartbeat()
                        except ApiError as e:
                            if e.status in (401, 403):
                                logging.warning("Agent heartbeat unauthorized (status=%d); token refresh required.", e.status)
                                self._agent_access_token = None
                                self._agent_access_token_expires_at_ms = 0
                            else:
                                logging.warning("Agent heartbeat API error (status=%d): %s", e.status, e)
                        except Exception as e:
                            logging.warning("Agent heartbeat failed: %s", e)

                # Dependency queue polling/dispatch.
                if now >= next_dep_poll_at_ms:
                    try:
                        items = self._fetch_queue(limit=25)
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
                    retry_changed = False
                    retry_cap = max(0, int(self.max_parallel) - len(dep_inflight))
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
                                due_retry_items.append(
                                    {
                                        "itemId": entry.get("itemId") if isinstance(entry.get("itemId"), str) else dep_id,
                                        "depId": dep_id,
                                        "op": "download",
                                        "resolved": resolved,
                                    }
                                )
                            if retry_changed:
                                self._save_state()

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

                    next_dep_poll_at_ms = now + int(max(0.2, float(self.poll_seconds)) * 1000)

                # Agent queue polling/dispatch.
                if (
                    self.agent_control_enabled
                    and self._agent_channel_supported
                    and self._agent_access_token
                    and now >= next_agent_poll_at_ms
                ):
                    with self._lock:
                        active_exec_count = len(self._active_exec_by_item)
                    execute_capacity = self._agent_effective_execute_capacity()
                    available_exec_slots = max(0, execute_capacity - active_exec_count)
                    poll_limit = max(1, min(20, available_exec_slots + 3))

                    try:
                        agent_items = self._agent_fetch_queue(limit=poll_limit, wait_sec=self.agent_queue_wait_sec)
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
                            self._process_agent_queue_item(item, agent_executor, agent_inflight)
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

                    next_agent_poll_at_ms = now + int(max(0.2, float(self.agent_poll_seconds)) * 1000)

                _sleep_with_jitter(0.5, jitter_ratio=0.1)
            except Exception as e:
                logging.error("Main loop error: %s", e)
                _sleep_with_jitter(5.0)

        dep_executor.shutdown(wait=False, cancel_futures=True)
        agent_executor.shutdown(wait=False, cancel_futures=True)
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

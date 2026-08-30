#!/usr/bin/env python3

import json
import os
import re
import fcntl
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from asset_gen_v7_lite_coordinator import (
    CoordinatorError,
    GPUCoordinator,
    LeaseConflict,
    SnapshotStore,
    StaleLease,
)

GATEWAY_HOST = os.environ.get("QWEN_GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.environ.get("QWEN_GATEWAY_PORT", "8080"))
LLAMA_BASE_URL = os.environ.get("QWEN_LLAMA_BASE_URL", "http://127.0.0.1:8081").rstrip("/")
COMFY_BASE_URL = os.environ.get("DM_LOCAL_COMFY_BASE_URL", "http://127.0.0.1:8188").rstrip("/")
API_KEY = os.environ.get("INFERENCE_INSTANCE_API_KEY", "").strip()
UPSTREAM_TIMEOUT_SECONDS = int(os.environ.get("QWEN_UPSTREAM_TIMEOUT_SECONDS", "900"))
SLEEP_TIMEOUT_SECONDS = int(os.environ.get("QWEN_SLEEP_TIMEOUT_SECONDS", "30"))
SLEEP_POLL_SECONDS = float(os.environ.get("QWEN_SLEEP_POLL_SECONDS", "0.1"))
MAX_BODY_BYTES = int(os.environ.get("QWEN_MAX_BODY_BYTES", str(32 * 1024 * 1024)))
RELEASE_TTL_SECONDS = int(os.environ.get("QWEN_RELEASE_TTL_SECONDS", "900"))
COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = int(os.environ.get("GPU_COORDINATOR_PORT", "8189"))
COORDINATOR_MODE = os.environ.get("GPU_COORDINATOR_MODE", "shadow").strip().lower()
if COORDINATOR_MODE not in {"shadow", "enforcing", "legacy"}:
    raise RuntimeError("GPU_COORDINATOR_MODE must be shadow, enforcing, or legacy")
ADMISSION_MODE = os.environ.get("GPU_ADMISSION_MODE", "off").strip().lower()
if ADMISSION_MODE not in {"off", "shadow", "enforcing"}:
    raise RuntimeError("GPU_ADMISSION_MODE must be off, shadow, or enforcing")
ADMIT_MAX_WAIT_SECONDS = max(0.0, min(60.0, float(os.environ.get("QWEN_ADMIT_MAX_WAIT_SECONDS", "10"))))
MAX_GATEWAY_WAITERS = max(1, min(64, int(os.environ.get("QWEN_MAX_WAITERS", "4"))))
ADMISSION_VALIDATION_URL = os.environ.get(
    "GPU_ADMISSION_VALIDATION_URL",
    "https://us-central1-furgencontentserver.cloudfunctions.net/inferenceApi/v1/internal/admission/validate",
).strip()
SERVER_TYPE = os.environ.get("SERVER_TYPE", "asset_gen_v7_lite").strip()


def bool_env(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return bool(default)
    return value.strip().lower() in {"1", "true", "yes", "on"}


WARM_RESIDENCY_ENABLED = bool_env("WARM_RESIDENCY_ENABLED", False)
SNAPSHOT_WRITE_ENABLED = bool_env("SNAPSHOT_WRITE_ENABLED", False)
SNAPSHOT_RESTORE_ENABLED = bool_env("SNAPSHOT_RESTORE_ENABLED", False)
SNAPSHOT_PATH = os.environ.get("QWEN_SNAPSHOT_PATH", "/workspace/cache/qwen-slots")
SNAPSHOT_FINGERPRINT = os.environ.get("QWEN_SNAPSHOT_FINGERPRINT", "unconfigured")
LLAMA_PID_FILE = os.environ.get("QWEN_LLAMA_PID_FILE", "/workspace/logs/asset_gen_v7_lite_llama.pid")
LLAMA_LOG_FILE = os.environ.get("QWEN_LLAMA_LOG_FILE", "/workspace/logs/asset_gen_v7_lite_llama.log")
LLAMA_COMMAND_FILE = os.environ.get("QWEN_LLAMA_COMMAND_FILE", "/workspace/logs/asset_gen_v7_lite_llama_command.json")
COORDINATOR_STATE_FILE = os.environ.get("GPU_COORDINATOR_STATE_FILE", "/workspace/logs/asset_gen_v7_lite_coordinator.json")
COORDINATOR_EPOCH_FILE = os.environ.get("GPU_COORDINATOR_EPOCH_FILE", "/workspace/logs/asset_gen_v7_lite_coordinator.epoch")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
GPU_LOCK = threading.Lock()
ADMIT_SEMAPHORE = threading.BoundedSemaphore(MAX_GATEWAY_WAITERS)
STAGE_METRICS_LOCK = threading.Lock()
STAGE_METRICS = {
    "gatewayWaiters": 0,
    "gatewayWaitersMax": 0,
    "gpuExecutions": 0,
    "gpuExecutionsMax": 0,
    "gpuExecutionsStarted": 0,
    "gpuExecutionsCompleted": 0,
}
RELEASES_LOCK = threading.Lock()
RELEASES = {}
INFLIGHT_LOCK = threading.Lock()
INFLIGHT = {}
CANCELLED_REQUESTS = {}
GATEWAY_BOOT_ID = uuid.uuid4().hex
COORDINATOR = None
COORDINATOR_LOCK_STREAM = None
REQUIRED_TOOL_CAPABILITIES = (
    "supports_tools",
    "supports_tool_calls",
    "supports_object_arguments",
    "supports_parallel_tool_calls",
)


def update_stage_metric(name, delta=0, maximum_name=None):
    with STAGE_METRICS_LOCK:
        STAGE_METRICS[name] = max(0, int(STAGE_METRICS.get(name, 0)) + int(delta))
        if maximum_name:
            STAGE_METRICS[maximum_name] = max(
                int(STAGE_METRICS.get(maximum_name, 0)),
                STAGE_METRICS[name],
            )


def stage_metrics_snapshot():
    with STAGE_METRICS_LOCK:
        return dict(STAGE_METRICS)


def http_request(url, method="GET", payload=None, timeout=10, authorize_backend=False):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if authorize_backend and API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.headers.get("Content-Type", "application/json"), response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers.get("Content-Type", "application/json"), error.read()


def open_http_response(url, method="GET", payload=None, timeout=10, authorize_backend=False):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if authorize_backend and API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        return error


def json_response_bytes(payload):
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def validate_admission_claim(server_type, ticket_id, claim_token, request_id):
    if not ADMISSION_VALIDATION_URL:
        raise RuntimeError("GPU admission validation URL is not configured")
    status, _, body = http_request(
        ADMISSION_VALIDATION_URL,
        method="POST",
        payload={
            "server_type": server_type,
            "ticket_id": ticket_id,
            "claim_token": claim_token,
            "request_id": request_id,
        },
        timeout=5,
        authorize_backend=True,
    )
    try:
        payload = json.loads(body)
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    return status == 200 and payload.get("valid") is True


def free_comfy_models(preserve_cache=False):
    status, _, body = http_request(
        f"{COMFY_BASE_URL}/free",
        method="POST",
        payload={"unload_models": True, "free_memory": not preserve_cache},
        timeout=30,
    )
    if status < 200 or status >= 300:
        raise RuntimeError(f"ComfyUI /free returned {status}: {body[:500]!r}")


def llama_props():
    status, _, body = http_request(f"{LLAMA_BASE_URL}/props", timeout=10, authorize_backend=True)
    if status < 200 or status >= 300:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def llama_is_sleeping():
    props = llama_props()
    if not props:
        return False
    return bool(props.get("is_sleeping") or props.get("sleeping"))


def force_llama_sleep():
    deadline = time.monotonic() + SLEEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if llama_is_sleeping():
            return True
        time.sleep(SLEEP_POLL_SECONDS)
    return False


def get_coordinator():
    global COORDINATOR
    if COORDINATOR is None:
        store = SnapshotStore(
            SNAPSHOT_PATH,
            SNAPSHOT_FINGERPRINT,
            ttl_seconds=int(os.environ.get("QWEN_SNAPSHOT_TTL_SECONDS", str(48 * 3600))),
            quota_bytes=int(os.environ.get("QWEN_SNAPSHOT_QUOTA_BYTES", str(48 * 1024**3))),
            min_free_bytes=int(os.environ.get("QWEN_SNAPSHOT_MIN_FREE_BYTES", str(20 * 1024**3))),
            max_entry_bytes=int(os.environ.get("QWEN_SNAPSHOT_MAX_ENTRY_BYTES", str(16 * 1024**3))),
        )
        COORDINATOR = GPUCoordinator(
            LLAMA_BASE_URL,
            COMFY_BASE_URL,
            http_request,
            store,
            llama_pid_file=LLAMA_PID_FILE,
            llama_log_file=LLAMA_LOG_FILE,
            llama_command_file=LLAMA_COMMAND_FILE,
            state_file=COORDINATOR_STATE_FILE,
            epoch_file=COORDINATOR_EPOCH_FILE,
            enabled=COORDINATOR_MODE != "legacy",
            enforce_transitions=COORDINATOR_MODE == "enforcing",
            warm_residency=WARM_RESIDENCY_ENABLED,
            snapshot_write=SNAPSHOT_WRITE_ENABLED,
            snapshot_restore=SNAPSHOT_RESTORE_ENABLED,
            mining_grace_seconds=int(os.environ.get("GPU_MINING_GRACE_SECONDS", "30")),
            comfy_release_vram_max_bytes=int(os.environ.get("GPU_COMFY_RELEASE_VRAM_MAX_BYTES", str(3 * 1024**3))),
            comfy_idle_baseline_bytes=(
                int(os.environ["GPU_COMFY_IDLE_BASELINE_BYTES"])
                if os.environ.get("GPU_COMFY_IDLE_BASELINE_BYTES") else None
            ),
            comfy_release_vram_headroom_bytes=int(
                os.environ.get("GPU_COMFY_RELEASE_VRAM_HEADROOM_BYTES", str(512 * 1024**2))
            ),
            tts_config_path=(os.environ.get("TTS_RESIDENCY_CONFIG") or
                             ("/workspace/.fcs/tts/config.json" if os.path.isfile("/workspace/.fcs/tts/config.json") else None)),
        )
    return COORDINATOR


def prune_releases(now=None):
    now = time.time() if now is None else now
    cutoff = now - RELEASE_TTL_SECONDS
    with RELEASES_LOCK:
        for request_id in [key for key, value in RELEASES.items() if value.get("updated_at", 0) < cutoff]:
            RELEASES.pop(request_id, None)


def set_release(request_id, phase, **values):
    now = time.time()
    with RELEASES_LOCK:
        previous = RELEASES.get(request_id, {})
        merged = {
            **previous,
            **values,
            "request_id": request_id,
            "gateway_boot_id": GATEWAY_BOOT_ID,
            "phase": phase,
            "updated_at": now,
        }
        merged["safe"] = phase == "safe" or (phase == "request_complete" and merged.get("gpu_released") is True)
        RELEASES[request_id] = merged
    if phase in {"request_complete", "safe"} or values.get("request_complete") is True:
        persisted = get_coordinator().record_completion(request_id, **merged)
        with RELEASES_LOCK:
            RELEASES[request_id] = {**merged, **persisted, "gateway_boot_id": GATEWAY_BOOT_ID}
    prune_releases(now)


def get_release(request_id):
    prune_releases()
    with RELEASES_LOCK:
        value = RELEASES.get(request_id)
        if value:
            return dict(value)
    persisted = get_coordinator().get_completion(request_id)
    return {**persisted, "gateway_boot_id": GATEWAY_BOOT_ID} if persisted else None


def register_inflight(request_id, backend):
    with INFLIGHT_LOCK:
        cancelled = request_id in CANCELLED_REQUESTS
        if not cancelled:
            INFLIGHT[request_id] = backend
    if cancelled:
        backend.close()
        raise ConnectionResetError("request was cancelled before upstream registration")


def unregister_inflight(request_id, backend=None):
    with INFLIGHT_LOCK:
        current = INFLIGHT.get(request_id)
        if backend is None or current is backend:
            INFLIGHT.pop(request_id, None)


def cancel_inflight(request_id):
    with INFLIGHT_LOCK:
        cutoff = time.monotonic() - RELEASE_TTL_SECONDS
        for stale_id in [key for key, cancelled_at in CANCELLED_REQUESTS.items() if cancelled_at < cutoff]:
            CANCELLED_REQUESTS.pop(stale_id, None)
        backend = INFLIGHT.get(request_id)
        CANCELLED_REQUESTS[request_id] = time.monotonic()
    if backend is None:
        # The cancellation marker is intentionally registered even before an
        # upstream socket exists, so retries and early disconnects are
        # idempotently accepted and a later registration is closed at once.
        return True
    try:
        backend.close()
    finally:
        unregister_inflight(request_id, backend)
    return True


def request_was_cancelled(request_id):
    with INFLIGHT_LOCK:
        return request_id in CANCELLED_REQUESTS


def clear_cancelled(request_id):
    with INFLIGHT_LOCK:
        CANCELLED_REQUESTS.pop(request_id, None)


def record_safe_rejection(request_id, code):
    try:
        set_release(
            request_id,
            "request_complete",
            code=code,
            request_failed=True,
            request_complete=True,
            gpu_released=True,
            sleeping=not get_coordinator().llama_running(),
        )
    except Exception as error:
        print(f"Failed persisting safe rejection receipt request={request_id} code={code}: {error}", flush=True)


def finalize_request_release(request_id, coordinator_lease, started_at, response_ready_ms, cache_state):
    release_completed_ms = round((time.monotonic() - started_at) * 1000)
    if COORDINATOR_MODE in {"enforcing", "shadow"}:
        released = get_coordinator().release(
            coordinator_lease["fencingToken"],
            coordinator_lease["epoch"],
            keep_warm=WARM_RESIDENCY_ENABLED,
            reason="request_complete",
        )
        if COORDINATOR_MODE == "shadow" and not force_llama_sleep():
            raise CoordinatorError("Inference backend did not sleep after request completion")
        set_release(
            request_id,
            "request_complete",
            response_ready_ms=response_ready_ms,
            release_completed_ms=release_completed_ms,
            request_complete=True,
            gpu_released=released["gpuReleased"],
            sleeping=not get_coordinator().llama_running() if COORDINATOR_MODE == "enforcing" else True,
            cache=cache_state,
        )
        return
    if not force_llama_sleep():
        raise CoordinatorError("Inference backend did not sleep after request completion")
    set_release(
        request_id,
        "request_complete",
        response_ready_ms=response_ready_ms,
        release_completed_ms=release_completed_ms,
        request_complete=True,
        gpu_released=True,
        sleeping=True,
        cache=cache_state,
    )


def health_payload():
    statuses = {"gatewayBootId": GATEWAY_BOOT_ID}
    try:
        comfy_status, _, _ = http_request(f"{COMFY_BASE_URL}/system_stats", timeout=5)
        statuses["comfyui"] = comfy_status == 200
    except Exception:
        statuses["comfyui"] = False
    try:
        llama_status, _, _ = http_request(f"{LLAMA_BASE_URL}/health", timeout=10, authorize_backend=True)
        statuses["llama"] = llama_status == 200
    except Exception:
        statuses["llama"] = False
    props = llama_props() if statuses["llama"] else None
    capabilities = props.get("chat_template_caps", {}) if isinstance(props, dict) else {}
    statuses["tool_calling"] = all(capabilities.get(name) is True for name in REQUIRED_TOOL_CAPABILITIES)
    statuses["sleeping"] = bool(props and (props.get("is_sleeping") or props.get("sleeping")))
    statuses["gpu_busy"] = GPU_LOCK.locked()
    coordinator = get_coordinator()
    coordinator_status = coordinator.quick_status()
    inference_readiness = coordinator.inference_readiness()
    statuses["coordinator"] = {
        "mode": COORDINATOR_MODE,
        "warmResidency": WARM_RESIDENCY_ENABLED,
        "snapshotWrite": SNAPSHOT_WRITE_ENABLED,
        "snapshotRestore": SNAPSHOT_RESTORE_ENABLED,
        "state": coordinator_status["state"],
        "inferenceReadiness": inference_readiness,
        "metrics": coordinator_status.get("metrics", {}),
        "lease": {
            key: coordinator_status.get("lease", {}).get(key)
            for key in ("holder", "workId", "state")
            if isinstance(coordinator_status.get("lease"), dict) and key in coordinator_status["lease"]
        },
    }
    statuses["admission"] = {
        "mode": ADMISSION_MODE,
        "maxWaitSeconds": ADMIT_MAX_WAIT_SECONDS,
        "maxWaiters": MAX_GATEWAY_WAITERS,
    }
    statuses["concurrency"] = stage_metrics_snapshot()
    if statuses["llama"]:
        inference_ready = statuses["tool_calling"]
    elif COORDINATOR_MODE == "enforcing":
        inference_ready = inference_readiness["ready"]
    else:
        inference_ready = False
    statuses["ready"] = statuses["comfyui"] and inference_ready
    return statuses


def cache_response_headers(request_id, cache_state, restore_pending=False):
    classification = cache_state.get("classification", "unkeyed")
    if restore_pending and classification == "restored":
        status = "restore_pending"
    elif cache_state.get("restoreFailed"):
        status = "error"
    elif cache_state.get("skipped"):
        status = "ineligible"
    else:
        status = {
            "resident": "hit",
            "restored": "restored",
            "cold": "miss",
            "unkeyed": "disabled",
        }.get(classification, "disabled")
    residency = {
        "resident": "warm",
        "restored": "restored",
    }.get(classification, "cold")
    if restore_pending and classification == "restored":
        residency = "cold"
    return {
        "X-Furgen-Request-Complete-Id": request_id,
        "X-Furgen-Gpu-Release-Id": request_id,
        "X-Furgen-Gateway-Boot-Id": GATEWAY_BOOT_ID,
        "X-Furgen-Prompt-Cache-Status": status,
        "X-Furgen-Inference-Residency": residency,
    }


def cache_metadata_from_response(response_payload):
    response_payload = response_payload if isinstance(response_payload, dict) else {}
    usage = response_payload.get("usage", {})
    usage = usage if isinstance(usage, dict) else {}
    details = usage.get("prompt_tokens_details", {})
    details = details if isinstance(details, dict) else {}
    timings = response_payload.get("timings", {})
    timings = timings if isinstance(timings, dict) else {}
    prompt_tokens = int(usage.get("prompt_tokens") or timings.get("prompt_n") or 0)
    cached_tokens = int(details.get("cached_tokens") or timings.get("cache_n") or 0)
    uncached_tokens = max(0, prompt_tokens - cached_tokens)
    prompt_ms = float(timings.get("prompt_ms") or 0)
    if prompt_ms <= 0 and uncached_tokens:
        prompt_ms = uncached_tokens * float(os.environ.get("QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN", "0.5"))
    if uncached_tokens > 0 and prompt_ms > 0:
        cold_prefill_ms = prompt_ms / uncached_tokens * max(1, prompt_tokens)
    else:
        cold_prefill_ms = prompt_tokens * float(os.environ.get("QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN", "0.5"))
    return {
        "promptTokens": prompt_tokens,
        "cachedTokens": cached_tokens,
        "coldPrefillMs": max(0, round(cold_prefill_ms)),
    }


def record_cache_observation(cache_handle, cache_state, response_payload):
    metadata = cache_metadata_from_response(response_payload)
    metadata["classification"] = cache_state.get("classification")
    result = get_coordinator().mark_cache_dirty(cache_handle, metadata) or {}
    if result.get("restoreIneffective"):
        cache_state.update({
            "classification": "cold",
            "restored": False,
            "restoreFailed": True,
            "skipped": "restore_ineffective",
        })
    return metadata


def observe_stream_chunk(parse_buffer, observation, chunk):
    parse_buffer += chunk
    while b"\n" in parse_buffer:
        raw_line, parse_buffer = parse_buffer.split(b"\n", 1)
        if not raw_line.startswith(b"data:"):
            continue
        raw_data = raw_line[5:].strip()
        if not raw_data or raw_data == b"[DONE]":
            continue
        try:
            event = json.loads(raw_data)
        except (TypeError, ValueError):
            continue
        if isinstance(event, dict):
            if isinstance(event.get("usage"), dict):
                observation["usage"] = event["usage"]
            if isinstance(event.get("timings"), dict):
                observation["timings"] = event["timings"]
    # A normal llama SSE line is small. Never let malformed/non-SSE upstream
    # data create an unbounded side buffer while the response is relayed.
    if len(parse_buffer) > 2 * 1024 * 1024:
        parse_buffer = b""
    return parse_buffer


def redact_unkeyed_stream_chunk(parse_buffer, chunk):
    parse_buffer += chunk
    output = bytearray()
    while b"\n" in parse_buffer:
        raw_line, parse_buffer = parse_buffer.split(b"\n", 1)
        rendered = raw_line
        if raw_line.startswith(b"data:"):
            raw_data = raw_line[5:].strip()
            if raw_data and raw_data != b"[DONE]":
                try:
                    event = json.loads(raw_data)
                except (TypeError, ValueError):
                    event = None
                usage = event.get("usage") if isinstance(event, dict) else None
                details = usage.get("prompt_tokens_details") if isinstance(usage, dict) else None
                if isinstance(details, dict) and "cached_tokens" in details:
                    details.pop("cached_tokens", None)
                    rendered = b"data: " + json_response_bytes(event)
        output.extend(rendered + b"\n")
    if len(parse_buffer) > 2 * 1024 * 1024:
        output.extend(parse_buffer)
        parse_buffer = b""
    return parse_buffer, bytes(output)


class JsonHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"{self.log_date_time_string()} {self.client_address[0]} {fmt % args}", flush=True)

    def send_bytes(self, status, content_type, body, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Furgen-Gateway-Boot-Id", GATEWAY_BOOT_ID)
        for key, value in (headers or {}).items():
            self.send_header(key, str(value))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def send_json(self, status, payload, headers=None):
        self.send_bytes(status, "application/json", json_response_bytes(payload), headers=headers)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValueError("request body is empty or too large")
        return json.loads(self.rfile.read(length))


class CoordinatorHandler(JsonHandler):
    """Loopback-only dependency-agent contract, served on port 8189."""

    server_version = "FurgenGPUCoordinator/1.0"

    def _loopback(self):
        return self.client_address[0] in {"127.0.0.1", "::1"}

    def do_GET(self):
        if not self._loopback():
            self.send_json(403, {"error": {"code": "loopback_required", "message": "Coordinator is loopback only."}})
            return
        if self.path not in {"/v1/gpu/status", "/v1/gpu/admission-recovery"}:
            self.send_json(404, {"error": {"code": "not_found", "message": "Not found"}})
            return
        if self.path == "/v1/gpu/admission-recovery":
            coordinator_status = get_coordinator().status()
            local_lease = coordinator_status.get("lease") if isinstance(coordinator_status, dict) else None
            lease_state = str(local_lease.get("state") or "") if isinstance(local_lease, dict) else ""
            diagnostics_busy = coordinator_status.get("diagnosticsBusy") is True
            safe_to_clear = (
                not GPU_LOCK.locked()
                and not diagnostics_busy
                and (not isinstance(local_lease, dict) or lease_state == "WARM")
                and coordinator_status.get("ttsResidency", {}).get("state") not in {"warming", "evicting"}
            )
            self.send_json(200, {
                "safeToClearAdmission": safe_to_clear,
                "coordinatorState": coordinator_status.get("state"),
                "leaseState": lease_state or None,
                "diagnosticsBusy": diagnostics_busy,
            })
            return
        status = get_coordinator().status()
        status["config"] = {
            "mode": COORDINATOR_MODE,
            "warmResidencyEnabled": WARM_RESIDENCY_ENABLED,
            "snapshotWriteEnabled": SNAPSHOT_WRITE_ENABLED,
            "snapshotRestoreEnabled": SNAPSHOT_RESTORE_ENABLED,
        }
        self.send_json(200, status)

    def do_POST(self):
        if not self._loopback():
            self.send_json(403, {"error": {"code": "loopback_required", "message": "Coordinator is loopback only."}})
            return
        try:
            payload = self.read_json()
            coordinator = get_coordinator()
            if self.path == "/v1/gpu/tts/validate":
                self.send_json(200, {"valid": bool(coordinator.tts and coordinator.tts.validate(payload))})
                return
            if self.path == "/v1/gpu/tts/heartbeat":
                result = coordinator.tts.idle_heartbeat(payload.get("foregroundDemand") is not False) if coordinator.tts else {"enabled": False}
                self.send_json(200, result)
                return
            if self.path == "/v1/gpu/tts/idle":
                result = coordinator.tts.idle_tick(payload.get("foregroundDemand") is not False) if coordinator.tts else {"canMine": True, "enabled": False}
                self.send_json(200, result)
                return
            if self.path == "/v1/gpu/drain":
                self.send_json(200, coordinator.begin_drain())
                return
            if self.path == "/v1/gpu/leases/acquire":
                admission_claim_id = str(payload.get("admissionClaimId") or "").strip()
                admission_ticket_id = str(payload.get("admissionTicketId") or "").strip()
                holder = str(payload.get("holder", ""))
                if ADMISSION_MODE == "enforcing" and holder in {"inference", "comfy"} and not REQUEST_ID_RE.fullmatch(admission_claim_id):
                    self.send_json(409, {"error": {"code": "admission_claim_required", "message": "A valid admission claim is required."}})
                    return
                if ADMISSION_MODE == "enforcing" and holder == "comfy":
                    work_id = str(payload.get("workId", "")).strip()
                    if not REQUEST_ID_RE.fullmatch(admission_ticket_id) or not REQUEST_ID_RE.fullmatch(work_id):
                        self.send_json(409, {"error": {"code": "admission_claim_required", "message": "A complete Comfy admission claim is required."}})
                        return
                    try:
                        admission_valid = validate_admission_claim(
                            SERVER_TYPE,
                            admission_ticket_id,
                            admission_claim_id,
                            work_id,
                        )
                    except Exception as error:
                        self.send_json(503, {"error": {"code": "admission_validation_failed", "message": str(error)}})
                        return
                    if not admission_valid:
                        self.send_json(409, {"error": {"code": "admission_claim_revoked", "message": "The Comfy admission claim is no longer current."}})
                        return
                lease = coordinator.acquire(
                    holder,
                    str(payload.get("workId", "")),
                    int(payload.get("ttlMs") or 60_000),
                    metadata=payload.get("metadata") or {
                        key: payload[key] for key in ("pid", "processStartTime", "processGroupId") if key in payload
                    },
                )
                self.send_json(200, {"lease": lease, **lease})
                return
            if self.path == "/v1/gpu/reconcile":
                lease = coordinator.reconcile(
                    str(payload.get("holder", "")),
                    str(payload.get("workId", "")),
                    metadata=payload.get("metadata") or {},
                )
                self.send_json(200, {"lease": lease, **lease})
                return
            match = re.fullmatch(r"/v1/gpu/leases/([A-Fa-f0-9]{32})/(renew|release)", self.path)
            if not match:
                self.send_json(404, {"error": {"code": "not_found", "message": "Not found"}})
                return
            token, action = match.groups()
            if action == "renew":
                lease = coordinator.renew(
                    token,
                    int(payload.get("epoch") or 0),
                    int(payload.get("ttlMs") or 60_000),
                    metadata=payload.get("metadata") or {
                        key: payload[key] for key in ("pid", "processStartTime", "processGroupId") if key in payload
                    },
                )
                self.send_json(200, {"lease": lease, **lease})
            else:
                result = coordinator.release(
                    token,
                    int(payload.get("epoch") or 0),
                    keep_warm=bool(payload.get("keepWarm", True)),
                    reason=payload.get("reason"),
                )
                self.send_json(200, result)
        except LeaseConflict as error:
            self.send_json(409, {"error": {"code": error.code, "message": str(error)}}, headers={"Retry-After": str(error.retry_after)})
        except StaleLease as error:
            self.send_json(409, {"error": {"code": error.code, "message": str(error)}})
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            self.send_json(400, {"error": {"code": "invalid_request", "message": str(error)}})
        except Exception as error:
            self.send_json(503, {"error": {"code": "coordinator_failure", "message": str(error)}})


class Handler(JsonHandler):
    server_version = "FurgenQwenGateway/4.0"

    def authorized(self):
        if not API_KEY:
            self.send_json(503, {"error": {"message": "Instance API key is not configured.", "code": "instance_key_unconfigured"}})
            return False
        if self.headers.get("Authorization", "") != f"Bearer {API_KEY}":
            self.send_json(401, {"error": {"message": "Unauthorized", "code": "unauthorized"}})
            return False
        return True

    def do_GET(self):
        if self.path == "/health":
            payload = health_payload()
            self.send_json(200 if payload["ready"] else 503, payload)
            return
        if not self.authorized():
            return
        if self.path == "/v1/gpu/coordinator-attestation":
            coordinator = get_coordinator()
            status = coordinator.quick_status()
            self.send_json(200, {
                "mode": COORDINATOR_MODE,
                "enforcing": COORDINATOR_MODE == "enforcing" and coordinator.enforce_transitions,
                "epoch": status["epoch"],
                "state": status["state"],
                "draining": status["draining"],
            })
            return
        if self.path == "/v1/gpu/admission-recovery":
            coordinator_status = get_coordinator().status()
            local_lease = coordinator_status.get("lease") if isinstance(coordinator_status, dict) else None
            lease_state = str(local_lease.get("state") or "") if isinstance(local_lease, dict) else ""
            diagnostics_busy = coordinator_status.get("diagnosticsBusy") is True
            safe_to_clear = (
                not GPU_LOCK.locked()
                and not diagnostics_busy
                and (not isinstance(local_lease, dict) or lease_state == "WARM")
            )
            self.send_json(200, {
                "gatewayBootId": GATEWAY_BOOT_ID,
                "safeToClearAdmission": safe_to_clear,
                "coordinatorState": coordinator_status.get("state"),
                "leaseState": lease_state or None,
                "diagnosticsBusy": diagnostics_busy,
            })
            return
        if self.path.startswith("/v1/gpu/releases/"):
            request_id = self.path.removeprefix("/v1/gpu/releases/")
            if not REQUEST_ID_RE.fullmatch(request_id):
                self.send_json(400, {"error": {"message": "Invalid release id.", "code": "release_id_invalid"}})
                return
            release = get_release(request_id)
            if release is None:
                self.send_json(404, {"error": {"message": "Release state not found.", "code": "release_not_found"}})
                return
            self.send_json(200, release)
            return
        if self.path == "/v1/slots":
            with INFLIGHT_LOCK:
                inflight = len(INFLIGHT)
            self.send_json(200, {
                "total": 1,
                "busy": 1 if GPU_LOCK.locked() else 0,
                "inflight": inflight,
                "admissionMode": ADMISSION_MODE,
            })
            return
        if self.path not in ("/v1/models", "/metrics", "/props"):
            self.send_json(404, {"error": {"message": "Not found", "code": "not_found"}})
            return
        try:
            status, content_type, body = http_request(
                f"{LLAMA_BASE_URL}{self.path}", timeout=30, authorize_backend=True
            )
            self.send_bytes(status, content_type, body)
        except Exception as error:
            self.send_json(502, {"error": {"message": "Inference backend unavailable.", "code": "backend_unavailable", "detail": str(error)}})

    def do_POST(self):
        if self.path not in {"/v1/chat/completions", "/v1/cancel"}:
            self.send_json(404, {"error": {"message": "Not found", "code": "not_found"}})
            return
        if not self.authorized():
            return
        if self.path == "/v1/cancel":
            try:
                payload = self.read_json()
            except (ValueError, json.JSONDecodeError) as error:
                self.send_json(400, {"error": {"message": str(error), "code": "invalid_request"}})
                return
            request_id = str(payload.get("request_id") or "").strip()
            if not REQUEST_ID_RE.fullmatch(request_id):
                self.send_json(400, {"error": {"message": "A valid request_id is required.", "code": "request_id_required"}})
                return
            cancelled = cancel_inflight(request_id)
            self.send_json(200, {"request_id": request_id, "cancelled": cancelled})
            return
        request_id = self.headers.get("X-Furgen-Request-Id", "").strip()
        if not REQUEST_ID_RE.fullmatch(request_id):
            self.send_json(400, {"error": {"message": "X-Furgen-Request-Id is required.", "code": "request_id_required"}})
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0 or length > MAX_BODY_BYTES:
            record_safe_rejection(request_id, "request_too_large")
            self.send_json(413, {"error": {"message": "Request body is empty or too large.", "code": "request_too_large"}})
            return
        body = self.rfile.read(length)
        try:
            request_payload = json.loads(body)
        except json.JSONDecodeError:
            record_safe_rejection(request_id, "invalid_json")
            self.send_json(400, {"error": {"message": "Invalid JSON.", "code": "invalid_json"}})
            return
        cache_handle = self.headers.get("X-Furgen-Prompt-Cache-Handle", "").strip() or None
        if cache_handle and not re.fullmatch(r"[A-Za-z0-9_-]{1,16}\.[a-f0-9]{64}", cache_handle):
            record_safe_rejection(request_id, "prompt_cache_handle_invalid")
            self.send_json(400, {"error": {"message": "Invalid prompt cache handle.", "code": "prompt_cache_handle_invalid"}})
            return
        # Never leak caller-selected keys to llama.cpp. Functions sends only an
        # opaque, tenant-scoped HMAC in the private header above.
        request_payload.pop("prompt_cache_key", None)
        admission_claim_id = self.headers.get("X-Furgen-Admission-Claim-Id", "").strip()
        admission_ticket_id = self.headers.get("X-Furgen-Admission-Ticket-Id", "").strip()
        admission_server_type = self.headers.get("X-Furgen-Admission-Server-Type", "").strip()
        if ADMISSION_MODE == "enforcing":
            if not all(REQUEST_ID_RE.fullmatch(value) for value in (
                admission_claim_id,
                admission_ticket_id,
                admission_server_type,
            )):
                record_safe_rejection(request_id, "admission_claim_required")
                self.send_json(409, {"error": {"message": "A valid admission claim is required.", "code": "admission_claim_required"}})
                return
            try:
                admission_valid = validate_admission_claim(
                    admission_server_type,
                    admission_ticket_id,
                    admission_claim_id,
                    request_id,
                )
            except Exception as error:
                record_safe_rejection(request_id, "admission_validation_failed")
                self.send_json(503, {"error": {"message": "Admission claim validation failed.", "code": "admission_validation_failed", "detail": str(error)}})
                return
            if not admission_valid:
                record_safe_rejection(request_id, "admission_claim_revoked")
                self.send_json(409, {"error": {"message": "The admission claim is no longer current.", "code": "admission_claim_revoked"}})
                return
        if not ADMIT_SEMAPHORE.acquire(blocking=False):
            status = 503 if ADMISSION_MODE == "enforcing" else 429
            code = "gpu_handoff_failed" if ADMISSION_MODE == "enforcing" else "queue_full"
            record_safe_rejection(request_id, code)
            self.send_json(status, {"error": {"message": "The bounded gateway handoff is unavailable.", "code": code}}, headers={"Retry-After": "1"})
            return
        update_stage_metric("gatewayWaiters", 1, "gatewayWaitersMax")
        lock_wait = ADMIT_MAX_WAIT_SECONDS if ADMISSION_MODE == "enforcing" else 0
        if not GPU_LOCK.acquire(timeout=lock_wait):
            update_stage_metric("gatewayWaiters", -1)
            ADMIT_SEMAPHORE.release()
            status = 503 if ADMISSION_MODE == "enforcing" else 429
            code = "gpu_handoff_failed" if ADMISSION_MODE == "enforcing" else "queue_timeout"
            record_safe_rejection(request_id, code)
            self.send_json(status, {"error": {"message": "The gateway handoff wait expired.", "code": code}}, headers={"Retry-After": "1"})
            return
        update_stage_metric("gatewayWaiters", -1)
        update_stage_metric("gpuExecutions", 1, "gpuExecutionsMax")
        update_stage_metric("gpuExecutionsStarted", 1)
        response_sent = False
        started_at = time.monotonic()
        coordinator_lease = None
        release_finalized = False
        cache_state = {"classification": "unkeyed", "restored": False}
        set_release(request_id, "preparing", started_at=time.time())
        try:
            if COORDINATOR_MODE in {"enforcing", "shadow"}:
                handoff_deadline = time.monotonic() + ADMIT_MAX_WAIT_SECONDS
                while True:
                    try:
                        coordinator_lease = get_coordinator().acquire(
                            "inference", request_id, UPSTREAM_TIMEOUT_SECONDS * 1000 + 60_000
                        )
                        break
                    except LeaseConflict:
                        if ADMISSION_MODE != "enforcing" or time.monotonic() >= handoff_deadline:
                            raise
                        time.sleep(min(0.25, max(0.01, handoff_deadline - time.monotonic())))
                if COORDINATOR_MODE == "enforcing":
                    cache_state = get_coordinator().prepare_cache(cache_handle)
                    request_payload["id_slot"] = 0
                    request_payload["cache_prompt"] = True
                    if cache_handle and request_payload.get("stream") is True:
                        stream_options = request_payload.setdefault("stream_options", {})
                        if isinstance(stream_options, dict):
                            stream_options["include_usage"] = True
                else:
                    free_comfy_models(preserve_cache=True)
            else:
                free_comfy_models(preserve_cache=False)
            set_release(request_id, "inference")
            if request_payload.get("stream") is True:
                backend = open_http_response(
                    f"{LLAMA_BASE_URL}/v1/chat/completions",
                    method="POST",
                    payload=request_payload,
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                    authorize_backend=True,
                )
                register_inflight(request_id, backend)
                status = getattr(backend, "status", None) or getattr(backend, "code", 502)
                content_type = backend.headers.get("Content-Type", "text/event-stream; charset=utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("X-Furgen-Gateway-Boot-Id", GATEWAY_BOOT_ID)
                # Streaming headers precede final usage telemetry, so a loaded
                # snapshot remains unverified until the terminal usage event.
                for header, value in cache_response_headers(request_id, cache_state, restore_pending=True).items():
                    self.send_header(header, value)
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.flush()
                response_sent = True
                stream_observation = {}
                stream_parse_buffer = b""
                stream_redaction_buffer = b""
                try:
                    read_chunk = getattr(backend, "read1", backend.read)
                    while True:
                        chunk = read_chunk(16 * 1024)
                        if not chunk:
                            break
                        if cache_handle and COORDINATOR_MODE == "enforcing":
                            stream_parse_buffer = observe_stream_chunk(stream_parse_buffer, stream_observation, chunk)
                        output_chunk = chunk
                        if not cache_handle:
                            stream_redaction_buffer, output_chunk = redact_unkeyed_stream_chunk(stream_redaction_buffer, chunk)
                        if output_chunk:
                            self.wfile.write(output_chunk)
                            self.wfile.flush()
                    if stream_redaction_buffer:
                        self.wfile.write(stream_redaction_buffer)
                        self.wfile.flush()
                finally:
                    unregister_inflight(request_id, backend)
                    backend.close()
                response_ready_ms = round((time.monotonic() - started_at) * 1000)
                set_release(request_id, "draining", response_ready_ms=response_ready_ms)
                if cache_handle and 200 <= status < 300 and COORDINATOR_MODE == "enforcing":
                    record_cache_observation(cache_handle, cache_state, stream_observation)
            else:
                backend = open_http_response(
                    f"{LLAMA_BASE_URL}/v1/chat/completions",
                    method="POST",
                    payload=request_payload,
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                    authorize_backend=True,
                )
                register_inflight(request_id, backend)
                try:
                    status = getattr(backend, "status", None) or getattr(backend, "code", 502)
                    content_type = backend.headers.get("Content-Type", "application/json")
                    response_body = backend.read()
                finally:
                    unregister_inflight(request_id, backend)
                    backend.close()
                response_ready_ms = round((time.monotonic() - started_at) * 1000)
                set_release(request_id, "draining", response_ready_ms=response_ready_ms)
                response_payload = None
                try:
                    response_payload = json.loads(response_body)
                except (TypeError, ValueError):
                    pass
                if not cache_handle and isinstance(response_payload, dict):
                    usage_for_redaction = response_payload.get("usage")
                    details_for_redaction = usage_for_redaction.get("prompt_tokens_details") if isinstance(usage_for_redaction, dict) else None
                    if isinstance(details_for_redaction, dict):
                        details_for_redaction.pop("cached_tokens", None)
                        response_body = json_response_bytes(response_payload)
                if cache_handle and 200 <= status < 300 and COORDINATOR_MODE == "enforcing":
                    response_payload = response_payload if isinstance(response_payload, dict) else {}
                    record_cache_observation(cache_handle, cache_state, response_payload)
                finalize_request_release(
                    request_id,
                    coordinator_lease,
                    started_at,
                    response_ready_ms,
                    cache_state,
                )
                coordinator_lease = None
                release_finalized = True
                self.send_bytes(
                    status,
                    content_type,
                    response_body,
                    headers=cache_response_headers(request_id, cache_state),
                )
                response_sent = True
            if not release_finalized:
                finalize_request_release(
                    request_id,
                    coordinator_lease,
                    started_at,
                    response_ready_ms,
                    cache_state,
                )
                coordinator_lease = None
                release_finalized = True
        except LeaseConflict as error:
            code = "gpu_handoff_failed" if ADMISSION_MODE == "enforcing" else error.code
            record_safe_rejection(request_id, code)
            if not response_sent:
                self.send_json(503, {"error": {"message": str(error), "code": code}}, headers={"Retry-After": "5"})
        except Exception as error:
            if request_was_cancelled(request_id) or isinstance(error, (BrokenPipeError, ConnectionResetError)):
                code = "client_disconnected"
            elif isinstance(error, CoordinatorError):
                code = "gpu_handoff_failed"
            else:
                code = "gateway_failure"
            if coordinator_lease and COORDINATOR_MODE in {"enforcing", "shadow"}:
                try:
                    released = get_coordinator().release(
                        coordinator_lease["fencingToken"],
                        coordinator_lease["epoch"],
                        keep_warm=code == "client_disconnected" and WARM_RESIDENCY_ENABLED,
                        reason=code,
                    )
                    slept = True if COORDINATOR_MODE == "enforcing" else force_llama_sleep()
                    if slept:
                        set_release(request_id, "request_complete", code=code, request_failed=True, request_complete=True, gpu_released=released["gpuReleased"], sleeping=COORDINATOR_MODE == "shadow")
                    else:
                        set_release(request_id, "error", code="gpu_release_failed", request_failed=True, sleeping=False)
                except Exception:
                    set_release(request_id, "error", code="gpu_release_failed", request_failed=True)
            else:
                slept = force_llama_sleep()
                if slept:
                    set_release(request_id, "request_complete", code=code, request_failed=True, request_complete=True, sleeping=True, gpu_released=True)
                else:
                    set_release(request_id, "error", code="gpu_release_failed", request_failed=True, sleeping=False)
            if not response_sent:
                status = 503 if code == "gpu_handoff_failed" else 502
                message = "GPU handoff failed." if code == "gpu_handoff_failed" else "Inference gateway failure."
                headers = {"Retry-After": "5"} if code == "gpu_handoff_failed" else None
                self.send_json(status, {"error": {"message": message, "code": code, "detail": str(error)}}, headers=headers)
        finally:
            clear_cancelled(request_id)
            update_stage_metric("gpuExecutions", -1)
            update_stage_metric("gpuExecutionsCompleted", 1)
            GPU_LOCK.release()
            ADMIT_SEMAPHORE.release()


def main():
    global COORDINATOR_LOCK_STREAM
    if not API_KEY:
        raise SystemExit("INFERENCE_INSTANCE_API_KEY is required")
    lock_path = os.environ.get("GPU_COORDINATOR_LOCK_FILE", "/workspace/logs/asset_gen_v7_lite_coordinator.lock")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    COORDINATOR_LOCK_STREAM = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(COORDINATOR_LOCK_STREAM.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        raise SystemExit("another GPU coordinator already owns the worker lock") from error
    get_coordinator()
    coordinator_server = ThreadingHTTPServer((COORDINATOR_HOST, COORDINATOR_PORT), CoordinatorHandler)
    coordinator_thread = threading.Thread(target=coordinator_server.serve_forever, daemon=True)
    coordinator_thread.start()
    print(f"GPU coordinator listening on {COORDINATOR_HOST}:{COORDINATOR_PORT} ({COORDINATOR_MODE})", flush=True)
    server = ThreadingHTTPServer((GATEWAY_HOST, GATEWAY_PORT), Handler)
    print(f"Qwen gateway listening on {GATEWAY_HOST}:{GATEWAY_PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

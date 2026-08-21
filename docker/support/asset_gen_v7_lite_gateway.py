#!/usr/bin/env python3

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
GPU_LOCK = threading.Lock()
RELEASES_LOCK = threading.Lock()
RELEASES = {}
REQUIRED_TOOL_CAPABILITIES = (
    "supports_tools",
    "supports_tool_calls",
    "supports_object_arguments",
    "supports_parallel_tool_calls",
)


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


def free_comfy_models():
    status, _, body = http_request(
        f"{COMFY_BASE_URL}/free",
        method="POST",
        payload={"unload_models": True, "free_memory": True},
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
        RELEASES[request_id] = {
            **previous,
            **values,
            "request_id": request_id,
            "phase": phase,
            "safe": phase == "safe",
            "updated_at": now,
        }
    prune_releases(now)


def get_release(request_id):
    prune_releases()
    with RELEASES_LOCK:
        value = RELEASES.get(request_id)
        return dict(value) if value else None


def health_payload():
    statuses = {}
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
    statuses["ready"] = statuses["comfyui"] and statuses["llama"] and statuses["tool_calling"]
    return statuses


class Handler(BaseHTTPRequestHandler):
    server_version = "FurgenQwenGateway/3.0"

    def log_message(self, fmt, *args):
        print(f"{self.log_date_time_string()} {self.client_address[0]} {fmt % args}", flush=True)

    def send_bytes(self, status, content_type, body, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, str(value))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def send_json(self, status, payload, headers=None):
        self.send_bytes(status, "application/json", json_response_bytes(payload), headers=headers)

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
        if self.path != "/v1/chat/completions":
            self.send_json(404, {"error": {"message": "Not found", "code": "not_found"}})
            return
        if not self.authorized():
            return
        request_id = self.headers.get("X-Furgen-Request-Id", "").strip()
        if not REQUEST_ID_RE.fullmatch(request_id):
            self.send_json(400, {"error": {"message": "X-Furgen-Request-Id is required.", "code": "request_id_required"}})
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0 or length > MAX_BODY_BYTES:
            self.send_json(413, {"error": {"message": "Request body is empty or too large.", "code": "request_too_large"}})
            return
        body = self.rfile.read(length)
        try:
            request_payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_json(400, {"error": {"message": "Invalid JSON.", "code": "invalid_json"}})
            return
        if not GPU_LOCK.acquire(blocking=False):
            self.send_json(429, {"error": {"message": "GPU is busy.", "code": "gpu_busy"}})
            return
        response_sent = False
        started_at = time.monotonic()
        set_release(request_id, "preparing", started_at=time.time())
        try:
            free_comfy_models()
            set_release(request_id, "inference")
            if request_payload.get("stream") is True:
                backend = open_http_response(
                    f"{LLAMA_BASE_URL}/v1/chat/completions",
                    method="POST",
                    payload=request_payload,
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                    authorize_backend=True,
                )
                status = getattr(backend, "status", None) or getattr(backend, "code", 502)
                content_type = backend.headers.get("Content-Type", "text/event-stream; charset=utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("X-Furgen-Gpu-Release-Id", request_id)
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.flush()
                response_sent = True
                try:
                    read_chunk = getattr(backend, "read1", backend.read)
                    while True:
                        chunk = read_chunk(16 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                finally:
                    backend.close()
                response_ready_ms = round((time.monotonic() - started_at) * 1000)
                set_release(request_id, "draining", response_ready_ms=response_ready_ms)
            else:
                status, content_type, response_body = http_request(
                    f"{LLAMA_BASE_URL}/v1/chat/completions",
                    method="POST",
                    payload=request_payload,
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                    authorize_backend=True,
                )
                response_ready_ms = round((time.monotonic() - started_at) * 1000)
                set_release(request_id, "draining", response_ready_ms=response_ready_ms)
                self.send_bytes(
                    status,
                    content_type,
                    response_body,
                    headers={"X-Furgen-Gpu-Release-Id": request_id},
                )
                response_sent = True
            slept = force_llama_sleep()
            release_completed_ms = round((time.monotonic() - started_at) * 1000)
            if slept:
                set_release(
                    request_id,
                    "safe",
                    response_ready_ms=response_ready_ms,
                    release_completed_ms=release_completed_ms,
                    sleeping=True,
                )
            else:
                set_release(
                    request_id,
                    "error",
                    code="gpu_release_failed",
                    response_ready_ms=response_ready_ms,
                    release_completed_ms=release_completed_ms,
                    sleeping=False,
                )
        except Exception as error:
            slept = force_llama_sleep()
            code = "client_disconnected" if isinstance(error, (BrokenPipeError, ConnectionResetError)) else "gateway_failure"
            if slept:
                set_release(request_id, "safe", code=code, request_failed=True, sleeping=True)
            else:
                set_release(request_id, "error", code="gpu_release_failed", request_failed=True, sleeping=False)
            if not response_sent:
                self.send_json(502, {"error": {"message": "Inference gateway failure.", "code": "gateway_failure", "detail": str(error)}})
        finally:
            GPU_LOCK.release()


def main():
    if not API_KEY:
        raise SystemExit("INFERENCE_INSTANCE_API_KEY is required")
    server = ThreadingHTTPServer((GATEWAY_HOST, GATEWAY_PORT), Handler)
    print(f"Qwen gateway listening on {GATEWAY_HOST}:{GATEWAY_PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

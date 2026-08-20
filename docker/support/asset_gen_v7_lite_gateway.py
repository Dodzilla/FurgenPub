#!/usr/bin/env python3

import json
import os
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
MAX_BODY_BYTES = int(os.environ.get("QWEN_MAX_BODY_BYTES", str(16 * 1024 * 1024)))
GPU_LOCK = threading.Lock()


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


def llama_is_sleeping():
    status, _, body = http_request(f"{LLAMA_BASE_URL}/props", timeout=10, authorize_backend=True)
    if status < 200 or status >= 300:
        return False
    try:
        props = json.loads(body)
    except json.JSONDecodeError:
        return False
    return bool(props.get("is_sleeping") or props.get("sleeping"))


def force_llama_sleep():
    deadline = time.monotonic() + SLEEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if llama_is_sleeping():
            return True
        time.sleep(1)
    return False


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
    statuses["sleeping"] = llama_is_sleeping() if statuses["llama"] else False
    statuses["ready"] = statuses["comfyui"] and statuses["llama"]
    return statuses


class Handler(BaseHTTPRequestHandler):
    server_version = "FurgenQwenGateway/1.0"

    def log_message(self, fmt, *args):
        print(f"{self.log_date_time_string()} {self.client_address[0]} {fmt % args}", flush=True)

    def send_bytes(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, payload):
        self.send_bytes(status, "application/json", json_response_bytes(payload))

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
        if request_payload.get("stream") is True:
            self.send_json(400, {"error": {"message": "Streaming is not supported.", "code": "stream_unsupported"}})
            return

        if not GPU_LOCK.acquire(blocking=False):
            self.send_json(429, {"error": {"message": "GPU is busy.", "code": "gpu_busy"}})
            return
        try:
            free_comfy_models()
            status, content_type, response_body = http_request(
                f"{LLAMA_BASE_URL}/v1/chat/completions",
                method="POST",
                payload=request_payload,
                timeout=UPSTREAM_TIMEOUT_SECONDS,
                authorize_backend=True,
            )
            slept = force_llama_sleep()
            if not slept:
                self.send_json(502, {"error": {"message": "Inference completed but the model did not release GPU memory.", "code": "gpu_release_failed"}})
                return
            self.send_bytes(status, content_type, response_body)
        except Exception as error:
            force_llama_sleep()
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

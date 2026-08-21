import importlib.util
import http.client
import json
import os
import socket
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


SUPPORT_DIR = Path(__file__).resolve().parents[1]
MODULE_PATH = SUPPORT_DIR / "asset_gen_v7_lite_gateway.py"


def load_gateway():
    os.environ["INFERENCE_INSTANCE_API_KEY"] = "test-instance-key"
    spec = importlib.util.spec_from_file_location("asset_gen_v7_lite_gateway_test", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.API_KEY = "test-instance-key"
    module.SLEEP_TIMEOUT_SECONDS = 2
    module.SLEEP_POLL_SECONDS = 0.01
    return module


class BackendState:
    def __init__(self):
        self.sleeping = True
        self.last_payload = None
        self.sleep_after_response = True
        self.backend_disconnected = False


def backend_handler(state):
    class BackendHandler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            return

        def send_json(self, payload, status=200):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/system_stats" or self.path == "/health":
                self.send_json({"status": "ok"})
                return
            if self.path == "/props":
                self.send_json({
                    "is_sleeping": state.sleeping,
                    "chat_template_caps": {
                        "supports_tools": True,
                        "supports_tool_calls": True,
                        "supports_object_arguments": True,
                        "supports_parallel_tool_calls": True,
                    },
                })
                return
            self.send_json({"error": "not found"}, 404)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/free":
                self.send_json({"ok": True})
                return
            if self.path != "/v1/chat/completions":
                self.send_json({"error": "not found"}, 404)
                return
            state.last_payload = payload
            state.sleeping = False
            if payload.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                chunks = (
                    b'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\\"id\\":"}}]}}]}\n\n',
                    b'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"7}"}}]},"finish_reason":"tool_calls"}]}\n\n',
                    b"data: [DONE]\n\n",
                )
                if payload.get("test_long_stream"):
                    chunks = tuple(
                        [chunks[0]] +
                        [b'data: {"choices":[{"delta":{"content":"' + (b"x" * 65536) + b'"}}]}\n\n'] * 1000 +
                        [chunks[-1]]
                    )
                try:
                    for chunk in chunks:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                        time.sleep(0.01)
                except (BrokenPipeError, ConnectionResetError):
                    state.backend_disconnected = True
                finally:
                    if state.sleep_after_response:
                        state.sleeping = True
                return
            if state.sleep_after_response:
                state.sleeping = True
            self.send_json({
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [{
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "lookup", "arguments": "{\"id\":7}"},
                        }],
                    },
                }],
            })

    return BackendHandler


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.gateway = load_gateway()
        self.state = BackendState()
        handler = backend_handler(self.state)
        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, daemon=True)
        self.backend_thread.start()
        backend_url = f"http://127.0.0.1:{self.backend.server_address[1]}"
        self.gateway.LLAMA_BASE_URL = backend_url
        self.gateway.COMFY_BASE_URL = backend_url
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.gateway.Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.backend.shutdown()
        self.backend.server_close()

    def request(self, path, method="GET", payload=None, request_id=None):
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Authorization": "Bearer test-instance-key", "Content-Type": "application/json"}
        if request_id:
            headers["X-Furgen-Request-Id"] = request_id
        request = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, dict(response.headers), response.read()

    def wait_for_safe_release(self, request_id):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            _, _, release_body = self.request(f"/v1/gpu/releases/{request_id}")
            release = json.loads(release_body)
            if release["safe"]:
                return release
            time.sleep(0.02)
        self.fail(f"release {request_id} did not become safe")

    def wait_for_release_phase(self, request_id, expected_phase):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            _, _, release_body = self.request(f"/v1/gpu/releases/{request_id}")
            release = json.loads(release_body)
            if release["phase"] == expected_phase:
                return release
            time.sleep(0.02)
        self.fail(f"release {request_id} did not reach {expected_phase}")

    def test_health_requires_tool_capabilities(self):
        with urllib.request.urlopen(self.base_url + "/health", timeout=5) as response:
            payload = json.loads(response.read())
        self.assertTrue(payload["ready"])
        self.assertTrue(payload["tool_calling"])

        original = self.gateway.REQUIRED_TOOL_CAPABILITIES
        self.gateway.REQUIRED_TOOL_CAPABILITIES = (*original, "missing_capability")
        try:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(self.base_url + "/health", timeout=5)
            self.assertEqual(caught.exception.code, 503)
        finally:
            self.gateway.REQUIRED_TOOL_CAPABILITIES = original

    def test_non_streaming_function_call_is_forwarded_and_released(self):
        payload = {
            "model": "qwen",
            "messages": [{"role": "user", "content": "Look up 7"}],
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
            "tool_choice": "required",
        }
        status, headers, body = self.request(
            "/v1/chat/completions", method="POST", payload=payload, request_id="request-nonstream"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-Furgen-Gpu-Release-Id"], "request-nonstream")
        self.assertEqual(json.loads(body)["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(self.state.last_payload["tools"][0]["function"]["name"], "lookup")
        self.assertTrue(self.wait_for_safe_release("request-nonstream")["safe"])

    def test_lock_contention_returns_busy_without_forwarding(self):
        self.gateway.GPU_LOCK.acquire()
        try:
            request = urllib.request.Request(
                self.base_url + "/v1/chat/completions",
                data=json.dumps({"model": "qwen", "messages": [{"role": "user", "content": "hello"}]}).encode(),
                headers={
                    "Authorization": "Bearer test-instance-key",
                    "Content-Type": "application/json",
                    "X-Furgen-Request-Id": "request-busy",
                },
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request, timeout=5)
            self.assertEqual(caught.exception.code, 429)
            self.assertIsNone(self.state.last_payload)
        finally:
            self.gateway.GPU_LOCK.release()

    def test_sleep_failure_keeps_release_unsafe(self):
        self.state.sleep_after_response = False
        self.gateway.SLEEP_TIMEOUT_SECONDS = 0.05
        self.request(
            "/v1/chat/completions",
            method="POST",
            payload={"model": "qwen", "messages": [{"role": "user", "content": "hello"}]},
            request_id="request-no-sleep",
        )
        release = self.wait_for_release_phase("request-no-sleep", "error")
        self.assertFalse(release["safe"])
        self.assertEqual(release["code"], "gpu_release_failed")

    def test_streaming_tool_call_relay_preserves_sse_and_release(self):
        payload = {
            "model": "qwen",
            "stream": True,
            "messages": [{"role": "user", "content": "Look up 7"}],
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
        }
        status, headers, body = self.request(
            "/v1/chat/completions", method="POST", payload=payload, request_id="request-stream"
        )
        self.assertEqual(status, 200)
        self.assertTrue(headers["Content-Type"].startswith("text/event-stream"))
        self.assertIn(b'"name":"lookup"', body)
        self.assertIn(b"data: [DONE]", body)
        release = self.wait_for_safe_release("request-stream")
        self.assertTrue(release["safe"])
        self.assertTrue(release["sleeping"])

    def test_stream_disconnect_cancels_backend_and_releases_gpu(self):
        payload = {
            "model": "qwen",
            "stream": True,
            "test_long_stream": True,
            "messages": [{"role": "user", "content": "Generate slowly"}],
        }
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=5)
        connection.request(
            "POST",
            "/v1/chat/completions",
            body=json.dumps(payload),
            headers={
                "Authorization": "Bearer test-instance-key",
                "Content-Type": "application/json",
                "X-Furgen-Request-Id": "request-disconnect",
            },
        )
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertTrue(response.read(32))
        response_socket = getattr(getattr(response.fp, "raw", None), "_sock", None)
        if response_socket:
            response_socket.shutdown(socket.SHUT_RDWR)
        response.close()
        connection.close()
        release = self.wait_for_safe_release("request-disconnect")
        self.assertTrue(release["safe"])
        self.assertTrue(release["sleeping"])
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not self.state.backend_disconnected:
            time.sleep(0.02)
        self.assertTrue(self.state.backend_disconnected)


if __name__ == "__main__":
    unittest.main()

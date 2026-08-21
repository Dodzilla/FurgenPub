import importlib.util
import http.client
import json
import os
import socket
import sys
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
    os.environ["GPU_COORDINATOR_MODE"] = "legacy"
    os.environ["QWEN_SNAPSHOT_PATH"] = "/tmp/furgen-qwen-gateway-tests"
    os.environ["GPU_COORDINATOR_STATE_FILE"] = "/tmp/furgen-qwen-gateway-tests-coordinator.json"
    os.environ["GPU_COORDINATOR_EPOCH_FILE"] = "/tmp/furgen-qwen-gateway-tests-coordinator.epoch"
    sys.path.insert(0, str(SUPPORT_DIR))
    spec = importlib.util.spec_from_file_location("asset_gen_v7_lite_gateway_test", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.API_KEY = "test-instance-key"
    module.SLEEP_TIMEOUT_SECONDS = 2
    module.SLEEP_POLL_SECONDS = 0.01
    return module


class SupportScriptContractTests(unittest.TestCase):
    def test_dependency_watchdog_persists_coordinator_fail_closed_settings(self):
        source = (SUPPORT_DIR / "asset_gen_v5_lite.sh").read_text(encoding="utf-8")
        persist_start = source.index("function dependency_manager_persist_agent_env()")
        persist_end = source.index("function dependency_manager_render_watchdog()", persist_start)
        persist_block = source[persist_start:persist_end]

        self.assertIn("DM_GPU_COORDINATOR_URL", persist_block)
        self.assertIn("DM_GPU_COORDINATOR_REQUIRED", persist_block)
        self.assertIn("GPU_ADMISSION_MODE", persist_block)
        self.assertIn("DM_GPU_ADMISSION_MAX_DEPTH", persist_block)


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

    def request(self, path, method="GET", payload=None, request_id=None, cache_handle=None):
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Authorization": "Bearer test-instance-key", "Content-Type": "application/json"}
        if request_id:
            headers["X-Furgen-Request-Id"] = request_id
        if cache_handle:
            headers["X-Furgen-Prompt-Cache-Handle"] = cache_handle
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

    def test_health_does_not_treat_saved_llama_command_as_ready_after_failed_handoff(self):
        class Coordinator:
            llama_argv = ["/workspace/bin/llama-server"]

            @staticmethod
            def quick_status():
                return {"state": "IDLE"}

            @staticmethod
            def inference_readiness():
                return {
                    "ready": False,
                    "reason": "comfy_vram_not_released",
                    "llamaConfigured": True,
                    "llamaRunning": False,
                    "comfyUsedBytes": 3_500 * 1024**2,
                    "comfyReleaseEffectiveMaxBytes": 3 * 1024**3,
                }

        original_mode = self.gateway.COORDINATOR_MODE
        original_http = self.gateway.http_request
        original_get_coordinator = self.gateway.get_coordinator
        self.gateway.COORDINATOR_MODE = "enforcing"
        self.gateway.get_coordinator = lambda: Coordinator()

        def health_http(url, **_kwargs):
            if url.endswith("/system_stats"):
                return 200, "application/json", b"{}"
            raise OSError("llama stopped")

        self.gateway.http_request = health_http
        try:
            payload = self.gateway.health_payload()
            self.assertFalse(payload["ready"])
            self.assertFalse(payload["llama"])
            self.assertEqual(
                payload["coordinator"]["inferenceReadiness"]["reason"],
                "comfy_vram_not_released",
            )
        finally:
            self.gateway.COORDINATOR_MODE = original_mode
            self.gateway.http_request = original_http
            self.gateway.get_coordinator = original_get_coordinator

    def test_request_complete_is_not_safe_while_gpu_remains_warm(self):
        self.gateway.set_release(
            "request-warm",
            "request_complete",
            request_complete=True,
            gpu_released=False,
        )
        release = self.gateway.get_release("request-warm")
        self.assertTrue(release["request_complete"])
        self.assertFalse(release["safe"])
        self.assertFalse(release["gpu_released"])

    def test_authenticated_coordinator_attestation_reports_live_mode(self):
        status, _, body = self.request("/v1/gpu/coordinator-attestation")
        payload = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(payload["mode"], "legacy")
        self.assertFalse(payload["enforcing"])
        self.assertIsInstance(payload["epoch"], int)

    def test_admission_recovery_fails_closed_while_coordinator_diagnostics_are_busy(self):
        class Coordinator:
            @staticmethod
            def status():
                return {"state": "IDLE", "lease": None, "diagnosticsBusy": True}

        original_get_coordinator = self.gateway.get_coordinator
        self.gateway.get_coordinator = lambda: Coordinator()
        try:
            status, _, body = self.request("/v1/gpu/admission-recovery")
            payload = json.loads(body)
            self.assertEqual(status, 200)
            self.assertFalse(payload["safeToClearAdmission"])
            self.assertTrue(payload["diagnosticsBusy"])
        finally:
            self.gateway.get_coordinator = original_get_coordinator

    def test_enforcing_admission_rejects_requests_without_a_claim(self):
        original_mode = self.gateway.ADMISSION_MODE
        self.gateway.ADMISSION_MODE = "enforcing"
        try:
            request = urllib.request.Request(
                self.base_url + "/v1/chat/completions",
                data=json.dumps({"model": "qwen", "messages": [{"role": "user", "content": "hello"}]}).encode(),
                headers={
                    "Authorization": "Bearer test-instance-key",
                    "Content-Type": "application/json",
                    "X-Furgen-Request-Id": "request-without-admission",
                },
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request, timeout=5)
            self.assertEqual(caught.exception.code, 409)
            self.assertEqual(json.loads(caught.exception.read())["error"]["code"], "admission_claim_required")
        finally:
            self.gateway.ADMISSION_MODE = original_mode

    def test_release_acknowledgements_are_gateway_boot_identified(self):
        self.gateway.set_release("request-boot-id", "request_complete", gpu_released=True, request_complete=True)
        status, headers, body = self.request("/v1/gpu/releases/request-boot-id")
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-Furgen-Gateway-Boot-Id"], self.gateway.GATEWAY_BOOT_ID)
        self.assertEqual(json.loads(body)["gateway_boot_id"], self.gateway.GATEWAY_BOOT_ID)

    def test_cache_metadata_prefers_llama_prompt_timing(self):
        metadata = self.gateway.cache_metadata_from_response({
            "usage": {"prompt_tokens": 100, "prompt_tokens_details": {"cached_tokens": 25}},
            "timings": {"prompt_ms": 321.4},
        })
        self.assertEqual(metadata["promptTokens"], 100)
        self.assertEqual(metadata["cachedTokens"], 25)
        self.assertEqual(metadata["coldPrefillMs"], 429)

    def test_cache_metadata_uses_calibrated_prefill_fallback(self):
        original = os.environ.get("QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN")
        os.environ["QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN"] = "2.5"
        try:
            metadata = self.gateway.cache_metadata_from_response({
                "usage": {"prompt_tokens": 100, "prompt_tokens_details": {"cached_tokens": 20}},
            })
            self.assertEqual(metadata["coldPrefillMs"], 250)
        finally:
            if original is None:
                os.environ.pop("QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN", None)
            else:
                os.environ["QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN"] = original

    def test_ineffective_restore_is_reported_as_a_cold_error(self):
        class Coordinator:
            def mark_cache_dirty(self, _handle, _metadata):
                return {"restoreIneffective": True}

        original = self.gateway.get_coordinator
        self.gateway.get_coordinator = lambda: Coordinator()
        cache_state = {"classification": "restored", "restored": True}
        try:
            self.gateway.record_cache_observation(
                "v1." + "a" * 64,
                cache_state,
                {
                    "usage": {"prompt_tokens": 100, "prompt_tokens_details": {"cached_tokens": 0}},
                    "timings": {"prompt_ms": 50},
                },
            )
        finally:
            self.gateway.get_coordinator = original
        headers = self.gateway.cache_response_headers("request-restore", cache_state)
        self.assertEqual(headers["X-Furgen-Prompt-Cache-Status"], "error")
        self.assertEqual(headers["X-Furgen-Inference-Residency"], "cold")
        self.assertEqual(cache_state["skipped"], "restore_ineffective")

    def test_streaming_restore_headers_remain_pending_until_usage(self):
        cache_state = {"classification": "restored", "restored": True}
        pending = self.gateway.cache_response_headers("request-stream-restore", cache_state, restore_pending=True)
        verified = self.gateway.cache_response_headers("request-nonstream-restore", cache_state)
        self.assertEqual(pending["X-Furgen-Prompt-Cache-Status"], "restore_pending")
        self.assertEqual(pending["X-Furgen-Inference-Residency"], "cold")
        self.assertEqual(verified["X-Furgen-Prompt-Cache-Status"], "restored")
        self.assertEqual(verified["X-Furgen-Inference-Residency"], "restored")

    def test_split_sse_usage_and_timings_are_observed_for_stream_cache(self):
        observation = {}
        buffer = self.gateway.observe_stream_chunk(
            b"",
            observation,
            b'data: {"usage":{"prompt_tokens":80,"prompt_tokens_details":{"cached_tokens":20}},',
        )
        buffer = self.gateway.observe_stream_chunk(
            buffer,
            observation,
            b'"timings":{"prompt_ms":42.5}}\n\ndata: [DONE]\n\n',
        )
        self.assertEqual(buffer, b"")
        self.assertEqual(observation["usage"]["prompt_tokens"], 80)
        self.assertEqual(observation["timings"]["prompt_ms"], 42.5)

    def test_unkeyed_stream_redacts_cached_token_usage(self):
        buffer, output = self.gateway.redact_unkeyed_stream_chunk(
            b"",
            b'data: {"usage":{"prompt_tokens":10,"prompt_tokens_details":{"cached_tokens":8}}}\n\n',
        )
        self.assertEqual(buffer, b"")
        self.assertNotIn(b"cached_tokens", output)
        self.assertIn(b'"prompt_tokens":10', output)

    def test_non_streaming_function_call_is_forwarded_and_released(self):
        payload = {
            "model": "qwen",
            "prompt_cache_key": "must-not-reach-worker-backend",
            "messages": [{"role": "user", "content": "Look up 7"}],
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
            "tool_choice": "required",
        }
        status, headers, body = self.request(
            "/v1/chat/completions", method="POST", payload=payload, request_id="request-nonstream"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-Furgen-Gpu-Release-Id"], "request-nonstream")
        self.assertEqual(headers["X-Furgen-Request-Complete-Id"], "request-nonstream")
        self.assertEqual(headers["X-Furgen-Prompt-Cache-Status"], "disabled")
        self.assertEqual(headers["X-Furgen-Inference-Residency"], "cold")
        self.assertEqual(json.loads(body)["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(self.state.last_payload["tools"][0]["function"]["name"], "lookup")
        self.assertNotIn("prompt_cache_key", self.state.last_payload)
        release = self.wait_for_safe_release("request-nonstream")
        self.assertTrue(release["safe"])
        self.assertEqual(release["phase"], "request_complete")

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

    def test_rejects_non_hmac_prompt_cache_handle(self):
        request = urllib.request.Request(
            self.base_url + "/v1/chat/completions",
            data=json.dumps({"model": "qwen", "messages": [{"role": "user", "content": "hello"}]}).encode(),
            headers={
                "Authorization": "Bearer test-instance-key",
                "Content-Type": "application/json",
                "X-Furgen-Request-Id": "request-cache-invalid",
                "X-Furgen-Prompt-Cache-Handle": "raw-caller-cache-key",
            },
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=5)
        self.assertEqual(caught.exception.code, 400)

    def test_accepts_configurable_cache_key_id_and_rejects_traversal(self):
        status, _, _ = self.request(
            "/v1/chat/completions",
            method="POST",
            payload={"model": "qwen", "messages": [{"role": "user", "content": "hello"}]},
            request_id="request-cache-v2",
            cache_handle="v2." + "a" * 64,
        )
        self.assertEqual(status, 200)
        request = urllib.request.Request(
            self.base_url + "/v1/chat/completions",
            data=json.dumps({"model": "qwen", "messages": [{"role": "user", "content": "hello"}]}).encode(),
            headers={
                "Authorization": "Bearer test-instance-key",
                "Content-Type": "application/json",
                "X-Furgen-Request-Id": "request-cache-traversal",
                "X-Furgen-Prompt-Cache-Handle": "../v2." + "a" * 64,
            },
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=5)
        self.assertEqual(caught.exception.code, 400)

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
        self.assertEqual(release["phase"], "request_complete")
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

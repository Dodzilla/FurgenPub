#!/usr/bin/env python3
"""CPU-only tests for the official Breeze fast-all Unix-socket runtime."""

from __future__ import annotations

import ast
import base64
import hashlib
import json
import os
import socket
import stat
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path



SUPPORT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SUPPORT_DIR))

from dataclasses import dataclass, replace

from asset_gen_v7_lite_tts_runtime import (  # noqa: E402
    PACKAGES,
    STARTED_ID_LIMIT,
    OfficialGpuBackend,
    PermitClient,
    PermitWatch,
    TtsError,
    TtsRuntimeApp,
    _apply_sampling,
    assert_package_pins,
    create_server,
    encode_audio_result,
    load_runtime_config,
    parse_generate_params,
    parse_permit,
    require_cuda,
    verify_source_revision,
)
from tts_profiles import (  # noqa: E402
    COMPACT_PREFILL,
    COMPACT_TEXT,
    EXPANDED_PREFILL,
    EXPANDED_TEXT,
    STOCK_PREFILL_BATCH1,
    STOCK_TEXT_BATCH1,
    UnsupportedProfile,
    apply_sparse_buckets,
    canonical_fingerprint_payload,
    cap_max_new_tokens,
    decode_f32le,
    disable_joint_text_encoder_merge,
    encode_f32le,
    fingerprint_hex,
    guard_prepared_profile,
    inspect_prepared_inputs,
    load_profile_spec,
    patch_sparse_bucket,
    profile_payload,
    validate_checkpoint_hashes,
    write_float32_wav,
)


OFFICIAL_FAST = Path("/tmp/breeze-official-fast-profile/configs/fast.json")


def _module_level_imports(path):
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def _digest(payload):
    return hashlib.sha256(payload).hexdigest()


def _write_config(directory, profile="compact"):
    source = Path(directory) / "source"
    checkpoint = Path(directory) / "ckpt"
    cache = Path(directory) / "cache"
    source.mkdir()
    checkpoint.mkdir()
    cache.mkdir()
    (checkpoint / "weights.bin").write_bytes(b"ckpt")
    config = {
        "sourceDir": str(source),
        "checkpointDir": str(checkpoint),
        "profile": profile,
        "cacheDir": str(cache),
        "checkpointHashes": {"weights.bin": _digest(b"ckpt")},
        "sourceRevision": "abc123",
        "version": "v-test",
    }
    path = Path(directory) / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path, config


def _permit(kind="generate", epoch=1, token="a" * 32, work="work-1"):
    return {"kind": kind, "epoch": epoch, "fencingToken": token, "workId": work}


def _params(**overrides):
    params = {
        "text": "Hello there.",
        "instruction": "Speak clearly and naturally.",
        "cfg_scale": 1.0,
        "max_new_tokens": 1500,
        "temperature": 0.9,
        "top_k": 50,
        "top_p": 1.0,
        "repetition_penalty": 1.1,
        "depth_temperature": 0.9,
        "depth_top_k": 50,
        "depth_top_p": 1.0,
        "seed": 42,
    }
    params.update(overrides)
    return params


def _rpc(socket_path, payload, timeout=5):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(socket_path)
        sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        return json.loads(buf.decode("utf-8"))
    finally:
        sock.close()


class FakePermits:
    def __init__(self, valid=True):
        self.valid = valid
        self.calls = []

    def validate(self, permit):
        self.calls.append(dict(permit))
        if callable(self.valid):
            return bool(self.valid(permit))
        return bool(self.valid)


class FakeGpuBackend:
    def __init__(self, spec, inspection=None):
        self.spec = spec
        self.inspection = inspection or {
            "text_jobs": [{"batch_size": 1, "token_length": 40, "segments": [40]}],
            "prefill_len": 80,
            "branch_batch_size": 1,
            "cfg_scale": 1.0,
        }
        self.warmup_calls = 0
        self.generate_calls = 0
        self.capture_calls = 0
        self.recorded_sampling = []
        self.block = None
        self.started = threading.Event()
        self.runtime = None
        self.manifest = None
        self.fingerprint = None
        self.peaks = None
        self.idle = None
        self.warmup_ms = None
        self.package_versions = dict(PACKAGES)
        self.raise_cuda = False
        self.return_audio_on_cancel = False

    def warmup(self, cancel_event=None):
        self.warmup_calls += 1
        self.runtime = object()
        self.manifest = {"status": "ready", "profile": {"name": self.spec.name}, "frozen": True}
        self.fingerprint = "fake-fp"
        self.peaks = {"allocatedBytes": 11, "reservedBytes": 22}
        self.idle = {"allocatedBytes": 3, "reservedBytes": 4}
        self.warmup_ms = 15.0
        return {
            "manifest": self.manifest,
            "fingerprint": self.fingerprint,
            "peaks": self.peaks,
            "idle": self.idle,
            "durationMs": self.warmup_ms,
            "packages": dict(self.package_versions),
        }

    def generate(self, params, cancel_event=None):
        self.generate_calls += 1
        guard_prepared_profile(self.spec, self.inspection)
        max_frames = cap_max_new_tokens(params["max_new_tokens"], self.inspection["prefill_len"])
        self.recorded_sampling.append(
            {
                "backbone": {
                    "temperature": params["temperature"],
                    "top_k": params["top_k"],
                    "top_p": params["top_p"],
                    "repetition_penalty": params["repetition_penalty"],
                },
                "depth": {
                    "temperature": params["depth_temperature"],
                    "top_k": params["depth_top_k"],
                    "top_p": params["depth_top_p"],
                },
                "max_new_tokens": max_frames,
                "instruction": params["instruction"],
                "template": "ref_edit_tata" if params.get("reference") else "tts_instruction",
            }
        )
        self.started.set()
        if self.block is not None:
            while not self.block.wait(0.02):
                if cancel_event is not None and cancel_event.is_set():
                    break
        if cancel_event is not None and cancel_event.is_set() and not self.return_audio_on_cancel:
            raise TtsError("cancelled", "generation cancelled")
        if self.raise_cuda:
            raise RuntimeError("CUDA error: device-side assert")
        self.capture_calls += 1
        samples = [0.0, 0.5, -0.25]
        return {
            "audio": encode_audio_result(samples),
            "timing": {
                "prepareMs": 1.0,
                "generationMs": 2.0,
                "firstAudioMs": 1.5,
                "peakAllocatedBytes": 8,
                "peakReservedBytes": 9,
                "prefillLen": int(self.inspection["prefill_len"]),
                "maxNewTokens": int(max_frames),
            },
        }


class StdlibImportTests(unittest.TestCase):
    def test_runtime_and_profiles_are_stdlib_at_import(self):
        forbidden = {"torch", "transformers", "qwen_tts", "numpy", "soundfile", "breeze_infer", "models"}
        runtime_names = _module_level_imports(SUPPORT_DIR / "asset_gen_v7_lite_tts_runtime.py")
        profile_names = _module_level_imports(SUPPORT_DIR / "tts_profiles.py")
        self.assertFalse(forbidden & runtime_names)
        self.assertFalse(forbidden & profile_names)
        self.assertIn("tts_profiles", runtime_names)


class ProfileMappingTests(unittest.TestCase):
    def test_stock_32_round_and_boundaries(self):
        spec = load_profile_spec("stock")
        self.assertEqual(spec.bucket_mode, "round32")
        self.assertEqual(spec.text_bucket(1, 32), 32)
        self.assertEqual(spec.text_bucket(1, 33), 64)
        self.assertEqual(spec.text_bucket(1, 256), 256)
        self.assertEqual(spec.prefill_bucket(1, 33), 64)
        with self.assertRaises(UnsupportedProfile):
            spec.text_bucket(1, 257)
        with self.assertRaises(UnsupportedProfile):
            spec.prefill_bucket(1, 257)
        self.assertEqual(spec.text_lengths[1], STOCK_TEXT_BATCH1)
        self.assertEqual(spec.prefill_lengths[1], STOCK_PREFILL_BATCH1)

    def test_expanded_every_32_through_declared_max(self):
        spec = load_profile_spec("expanded")
        self.assertEqual(spec.text_lengths[1], EXPANDED_TEXT)
        self.assertEqual(spec.text_lengths[2], EXPANDED_TEXT)
        self.assertEqual(spec.prefill_lengths[1], EXPANDED_PREFILL)
        self.assertEqual(spec.prefill_lengths[2], EXPANDED_PREFILL)
        self.assertEqual(spec.text_bucket(2, 1000), 1024)
        self.assertEqual(spec.prefill_bucket(2, 1500), 1504)
        with self.assertRaises(UnsupportedProfile):
            spec.text_bucket(1, 1025)
        with self.assertRaises(UnsupportedProfile):
            spec.prefill_bucket(1, 1537)

    def test_compact_sparse_smallest_declared_and_padding(self):
        spec = load_profile_spec("compact")
        self.assertEqual(spec.bucket_mode, "sparse")
        self.assertEqual(spec.text_lengths[1], COMPACT_TEXT)
        self.assertEqual(spec.prefill_lengths[1], COMPACT_PREFILL)
        self.assertEqual(spec.text_bucket(1, 32), 32)
        self.assertEqual(spec.text_bucket(1, 33), 128)
        self.assertEqual(spec.text_bucket(2, 129), 256)
        self.assertEqual(spec.prefill_bucket(1, 100), 128)
        self.assertEqual(spec.prefill_bucket(2, 513), 768)
        self.assertEqual(spec.prefill_bucket(1, 1536), 1536)
        with self.assertRaises(UnsupportedProfile):
            spec.text_bucket(1, 1025)
        with self.assertRaises(UnsupportedProfile):
            spec.prefill_bucket(1, 1537)
        seq = list(range(100))
        bucket = spec.prefill_bucket(1, len(seq))
        padded = [0] * (bucket - len(seq)) + seq
        self.assertEqual(len(padded), 128)
        self.assertEqual(padded[28:], seq)
        self.assertEqual(padded[:28], [0] * 28)

    def test_sparse_bucket_patch_matches_compact_selector(self):
        class Cache:
            def _bucket(self, length):
                return ((int(length) + 31) // 32) * 32

        cache = Cache()
        self.assertEqual(cache._bucket(33), 64)
        patch_sparse_bucket(cache, COMPACT_TEXT)
        self.assertEqual(cache._bucket(33), 128)
        self.assertEqual(cache._bucket(32), 32)

    def test_joint_cfg_merge_disabled_to_keep_text_batch_in_profile(self):
        class Runtime:
            def _merge_cfg_branches(self, inputs):
                return inputs

        runtime = Runtime()
        disable_joint_text_encoder_merge(runtime)
        self.assertIsNone(runtime._merge_cfg_branches({"cfg_scale": 4.0}))

    def test_stock_payload_matches_official_fast_json(self):
        if not OFFICIAL_FAST.is_file():
            self.skipTest("official fast.json is not checked out")
        official = json.loads(OFFICIAL_FAST.read_text(encoding="utf-8"))
        payload = profile_payload("stock", source_dir=OFFICIAL_FAST.parents[1])
        self.assertEqual(payload["stages"]["text_encoder"], official["stages"]["text_encoder"])
        self.assertEqual(
            payload["stages"]["backbone_prefill"], official["stages"]["backbone_prefill"]
        )


class GuardAndAudioTests(unittest.TestCase):
    def test_guard_rejects_out_of_profile_before_execution(self):
        spec = load_profile_spec("stock")
        inspection = {
            "text_jobs": [{"batch_size": 1, "token_length": 300, "segments": [300]}],
            "prefill_len": 80,
            "branch_batch_size": 1,
        }
        with self.assertRaises(UnsupportedProfile):
            guard_prepared_profile(spec, inspection)
        inspection = {
            "text_jobs": [{"batch_size": 1, "token_length": 40, "segments": [40]}],
            "prefill_len": 300,
            "branch_batch_size": 1,
        }
        with self.assertRaises(UnsupportedProfile):
            guard_prepared_profile(spec, inspection)

    def test_context_cap_matches_comfy(self):
        self.assertEqual(cap_max_new_tokens(1500, 100), 1500)
        self.assertEqual(cap_max_new_tokens(3000, 100), 2048 - 1 - 100)
        with self.assertRaises(Exception):
            cap_max_new_tokens(1500, 1984)

    def test_inspect_prepared_inputs_cfg_and_no_cfg(self):
        no_cfg = inspect_prepared_inputs(
            {"text_ids_len": [40], "input_ids": type("T", (), {"shape": (1, 80)})()}
        )
        self.assertEqual(no_cfg["branch_batch_size"], 1)
        self.assertEqual(no_cfg["prefill_len"], 80)
        cfg = inspect_prepared_inputs(
            {
                "text_ids_len": [40, 20],
                "input_ids": type("T", (), {"shape": (1, 90)})(),
                "cfg_scale": 4.0,
                "cfg_negative_prompt_ids": type("T", (), {"shape": (1, 70)})(),
                "cfg_negative_text_ids_len": [30],
            }
        )
        self.assertEqual(cfg["branch_batch_size"], 2)
        self.assertEqual(cfg["prefill_len"], 90)
        self.assertEqual(len(cfg["text_jobs"]), 2)

    def test_audio_limits_and_roundtrip(self):
        samples = [0.0, 1.0, -1.0, 0.25]
        encoded = encode_f32le(samples)
        self.assertEqual(decode_f32le(encoded, 4), samples)
        payload = {
            "params": _params(),
            "referenceAudio": {
                "data": base64.b64encode(encoded).decode("ascii"),
                "sampleRate": 24000,
                "samples": 4,
            },
        }
        payload["params"]["ref_text"] = "exact transcript"
        parsed = parse_generate_params(payload)
        self.assertEqual(parsed["reference"]["count"], 4)
        self.assertEqual(parsed["instruction"], "Speak clearly and naturally.")
        self.assertEqual(
            parse_generate_params({"params": {"text": "Hello there."}})["instruction"],
            "Speak clearly and naturally.",
        )
        over = {
            "params": _params(ref_text="x"),
            "referenceAudio": {
                "data": base64.b64encode(encode_f32le([0.0] * 8)).decode("ascii"),
                "sampleRate": 1,
                "samples": 61,
            },
        }
        with self.assertRaises(TtsError):
            parse_generate_params(over)

    def test_wav_permissions_and_header(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ref.wav"
            write_float32_wav(path, [0.0, 0.5], 24000)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertTrue(path.read_bytes().startswith(b"RIFF"))


class PermitAndServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config_path, self.config = _write_config(self.temp.name, "compact")
        self.spec = load_profile_spec("compact")
        self.backend = FakeGpuBackend(self.spec)
        self.permits = FakePermits(True)
        self.app = TtsRuntimeApp(
            load_runtime_config(self.config_path),
            "http://127.0.0.1:8189",
            backend=self.backend,
            permit_client=self.permits,
            poll_interval=0.05,
            terminator=self._terminate,
        )
        self.exits = []
        socket_path = str(Path(self.temp.name) / "tts.sock")
        self.server = create_server(socket_path, self.app)
        self.socket_path = socket_path
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        def _stop_server():
            self.server.shutdown()
            self.server.server_close()
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)

        self.addCleanup(_stop_server)

    def _terminate(self, code=1):
        self.exits.append(code)

    def _warmup(self):
        return _rpc(
            self.socket_path,
            {"method": "warmup", "permit": _permit("warmup")},
        )

    def test_socket_mode_0600(self):
        mode = stat.S_IMODE(os.stat(self.socket_path).st_mode)
        self.assertEqual(mode, 0o600)

    def test_health_before_warmup(self):
        response = _rpc(self.socket_path, {"method": "health"})
        self.assertTrue(response["ok"])
        self.assertFalse(response["result"]["ready"])
        self.assertEqual(response["result"]["stage"], "idle")
        self.assertFalse(response["result"]["profileCertified"])
        self.assertNotIn("text", json.dumps(response))

    def test_permit_rejection_does_not_load_gpu(self):
        self.permits.valid = False
        created = []

        def factory(_config):
            created.append("loader")
            raise AssertionError("GPU loader must not run")

        app = TtsRuntimeApp(
            self.config,
            "http://127.0.0.1:8189",
            backend=factory,
            permit_client=self.permits,
        )
        response = app.handle({"method": "warmup", "permit": _permit("warmup")})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_permit")
        self.assertEqual(created, [])
        self.assertEqual(self.backend.warmup_calls, 0)

    def test_warmup_then_generate_roundtrip(self):
        warmed = self._warmup()
        self.assertTrue(warmed["ok"])
        self.assertTrue(warmed["result"]["ready"])
        self.assertEqual(warmed["result"]["peakAllocatedBytes"], 11)
        self.assertFalse(warmed["result"]["profileCertified"])
        samples = encode_audio_result([0.1, -0.2])
        response = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "req-1",
                "permit": _permit("generate"),
                "params": _params(ref_text="exact transcript"),
                "referenceAudio": {
                    "data": samples["data"],
                    "sampleRate": 16000,
                    "samples": samples["samples"],
                },
            },
        )
        self.assertTrue(response["ok"], response)
        self.assertEqual(response["result"]["audio"]["sampleRate"], 24000)
        decoded = decode_f32le(
            base64.b64decode(response["result"]["audio"]["data"]),
            response["result"]["audio"]["samples"],
        )
        self.assertEqual(decoded, [0.0, 0.5, -0.25])
        self.assertEqual(self.backend.recorded_sampling[0]["template"], "ref_edit_tata")
        self.assertEqual(
            self.backend.recorded_sampling[0]["instruction"],
            "Speak clearly and naturally.",
        )

    def test_missing_ref_text_rejected(self):
        self._warmup()
        samples = encode_audio_result([0.1])
        response = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "req-ref",
                "permit": _permit("generate", work="w-ref"),
                "params": _params(),
                "referenceAudio": {
                    "data": samples["data"],
                    "sampleRate": 24000,
                    "samples": 1,
                },
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_request")

    def test_unsupported_profile_does_not_capture(self):
        self._warmup()
        self.backend.inspection = {
            "text_jobs": [{"batch_size": 1, "token_length": 2000, "segments": [2000]}],
            "prefill_len": 80,
            "branch_batch_size": 1,
        }
        response = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "req-long",
                "permit": _permit("generate", work="w-long"),
                "params": _params(),
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "unsupported_profile")
        self.assertEqual(self.backend.capture_calls, 0)

    def test_distinct_backbone_and_depth_sampling(self):
        self._warmup()
        first = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "s1",
                "permit": _permit("generate", work="s1"),
                "params": _params(temperature=0.4, depth_temperature=1.3, depth_top_k=7),
            },
        )
        second = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "s2",
                "permit": _permit("generate", work="s2"),
                "params": _params(temperature=0.4, depth_temperature=0.2, depth_top_k=3),
            },
        )
        self.assertTrue(first["ok"] and second["ok"])
        a, b = self.backend.recorded_sampling
        self.assertEqual(a["backbone"]["temperature"], b["backbone"]["temperature"])
        self.assertNotEqual(a["depth"]["temperature"], b["depth"]["temperature"])
        self.assertNotEqual(a["depth"]["top_k"], b["depth"]["top_k"])

    def test_busy_duplicate_cancel_and_health_during_work(self):
        self._warmup()
        self.backend.block = threading.Event()
        self.backend.return_audio_on_cancel = True
        result = {}

        def generate():
            result["response"] = _rpc(
                self.socket_path,
                {
                    "method": "generate",
                    "requestId": "live",
                    "permit": _permit("generate", work="live"),
                    "params": _params(),
                },
                timeout=8,
            )

        worker = threading.Thread(target=generate)
        worker.start()
        self.assertTrue(self.backend.started.wait(2))
        health = _rpc(self.socket_path, {"method": "health"})
        self.assertTrue(health["ok"])
        self.assertEqual(health["result"]["stage"], "generating")
        busy = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "other",
                "permit": _permit("generate", work="other"),
                "params": _params(),
            },
        )
        self.assertFalse(busy["ok"])
        self.assertEqual(busy["error"]["code"], "busy")
        dup = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "live",
                "permit": _permit("generate", work="live-dup"),
                "params": _params(),
            },
        )
        self.assertFalse(dup["ok"])
        self.assertEqual(dup["error"]["code"], "duplicate_request")
        cancelled = _rpc(self.socket_path, {"method": "cancel", "requestId": "live"})
        self.assertTrue(cancelled["ok"])
        self.backend.block.set()
        worker.join(5)
        self.assertTrue(worker.is_alive() is False)
        self.assertFalse(result["response"]["ok"])
        self.assertEqual(result["response"]["error"]["code"], "cancelled")

    def test_cuda_failure_is_not_generic_success(self):
        self._warmup()
        seen = []

        def sync(_torch):
            seen.append(self.app.health()["inFlight"])

        self.app.synchronize = sync
        self.backend.raise_cuda = True
        response = _rpc(
            self.socket_path,
            {
                "method": "generate",
                "requestId": "cuda",
                "permit": _permit("generate", work="cuda"),
                "params": _params(),
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "cuda_error")
        self.assertNotIn("device-side", response["error"]["message"])
        self.assertTrue(seen)
        self.assertIsNotNone(seen[0])
        health = _rpc(self.socket_path, {"method": "health"})
        self.assertFalse(health["result"]["ready"])
        self.assertEqual(health["result"]["stage"], "failed")
        self.assertIsNone(health["result"]["inFlight"])

    def test_invalid_warmup_permit_terminates(self):
        calls = {"n": 0}

        def valid(_permit):
            calls["n"] += 1
            return calls["n"] <= 2

        self.permits.valid = valid
        self.backend.warmup = lambda cancel_event=None: time.sleep(0.2) or FakeGpuBackend.warmup(
            self.backend, cancel_event
        )
        response = self.app.handle({"method": "warmup", "permit": _permit("warmup")})
        self.assertTrue(self.exits)
        self.assertGreaterEqual(calls["n"], 3)
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_permit")
        self.assertFalse(self.app.ready)


class PermitHttpTests(unittest.TestCase):
    def test_only_200_valid_true_admits(self):
        state = {"status": 200, "body": {"valid": True}}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                body = json.dumps(state["body"]).encode("utf-8")
                self.send_response(state["status"])
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        client = PermitClient(f"http://127.0.0.1:{server.server_address[1]}")
        self.assertTrue(client.validate(_permit("warmup")))
        state["body"] = {"valid": False}
        self.assertFalse(client.validate(_permit("warmup")))
        state["body"] = {"valid": True}
        state["status"] = 403
        self.assertFalse(client.validate(_permit("warmup")))


class HashAndFingerprintTests(unittest.TestCase):
    def test_hash_mismatch_and_fingerprint_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.bin"
            path.write_bytes(b"one")
            with self.assertRaises(Exception):
                validate_checkpoint_hashes(directory, {"a.bin": _digest(b"two")})
            self.assertTrue(validate_checkpoint_hashes(directory, {"a.bin": _digest(b"one")}))
        payload = {
            "sourceRevision": "r",
            "checkpointHashes": {"a.bin": "aa"},
            "profile": "compact",
            "version": "1",
            "torch": "2.10.0+cu130",
        }
        self.assertEqual(len(fingerprint_hex(payload)), 64)

    def test_apply_sparse_buckets_only_for_compact(self):
        class Cache:
            def _bucket(self, length):
                return 64

        class Runtime:
            def __init__(self):
                self.model = type("M", (), {})()
                self.model._fast_text_encoder_graph_cache = Cache()
                self._backbone_prefill_graphs = {1: Cache()}

        compact = load_profile_spec("compact")
        runtime = Runtime()
        apply_sparse_buckets(runtime, compact)
        self.assertEqual(runtime.model._fast_text_encoder_graph_cache._bucket(33), 128)
        stock = load_profile_spec("stock")
        runtime = Runtime()
        apply_sparse_buckets(runtime, stock)
        self.assertEqual(runtime.model._fast_text_encoder_graph_cache._bucket(33), 64)

    def test_parse_permit_kind_must_match(self):
        with self.assertRaises(TtsError):
            parse_permit({"permit": _permit("generate")}, "warmup")

    def test_source_revision_and_pins(self):
        verify_source_revision("/tmp", "abc", git_output=("abc", ""))
        with self.assertRaises(TtsError):
            verify_source_revision("/tmp", "abc", git_output=("def", ""))
        with self.assertRaises(TtsError):
            verify_source_revision("/tmp", "abc", git_output=("abc", " M models/fast_streaming.py"))
        verify_source_revision("/tmp", "abc", git_output=("abc", "?? scratch.bin\n"))
        assert_package_pins(PACKAGES)
        with self.assertRaises(TtsError):
            assert_package_pins({**PACKAGES, "torch": "2.9.1"})
        require_cuda(True)
        with self.assertRaises(TtsError):
            require_cuda(False)
        payload = canonical_fingerprint_payload(
            source_revision="r",
            checkpoint_hashes={"a.bin": "aa"},
            profile="compact",
            version="1",
            torch_version="2.10.0+cu130",
            transformers_version="4.57.3",
            qwen_tts_version="0.1.1",
            numpy_version="2.2.0",
            cuda_version="13.0",
            device_capability=(12, 0),
        )
        self.assertEqual(payload["transformers"], "4.57.3")
        self.assertEqual(payload["qwen_tts"], "0.1.1")
        self.assertEqual(payload["numpy"], "2.2.0")
        self.assertEqual(len(fingerprint_hex(payload)), 64)

    def test_duplicate_ids_are_bounded(self):
        app = TtsRuntimeApp(
            {
                "sourceDir": "/tmp",
                "checkpointDir": "/tmp",
                "profile": "compact",
                "cacheDir": "/tmp",
                "checkpointHashes": {"a": "b"},
                "sourceRevision": "r",
                "version": "v",
            },
            "http://127.0.0.1:8189",
            backend=FakeGpuBackend(load_profile_spec("compact")),
            permit_client=FakePermits(True),
        )
        for index in range(STARTED_ID_LIMIT + 7):
            app._finish(f"id-{index}", consume=True, stage="ready", ready=True)
        self.assertEqual(len(app.started_ids), STARTED_ID_LIMIT)
        self.assertNotIn("id-0", app.started_ids)
        self.assertIn(f"id-{STARTED_ID_LIMIT + 6}", app.started_ids)

    def test_disarmed_watcher_cannot_cancel_later_request(self):
        fired = []

        class SlowPermits:
            def validate(self, _permit):
                time.sleep(0.12)
                return False

        watch = PermitWatch(SlowPermits(), _permit("generate"), lambda: fired.append("old"), interval=0.05)
        watch.start()
        time.sleep(0.06)
        started = time.monotonic()
        watch.stop()
        self.assertLess(time.monotonic() - started, 0.05)
        time.sleep(0.2)
        self.assertEqual(fired, [])


@dataclass
class _FastConfig:
    max_new_tokens: int = 1500
    repetition_penalty: float = 1.1
    temperature: object = None
    top_k: object = None
    top_p: object = None
    do_sample: object = None


class _DepthGraph:
    def __init__(self, max_k, codebook_size):
        self._max_k = max_k
        self.codec_codebook_size = codebook_size
        self.top_k = None

    def set_temperature(self, value):
        return None

    def set_top_p(self, value):
        return None

    def set_top_k(self, value):
        self.top_k = int(value)


def _sampling_params(**overrides):
    params = {
        "temperature": 0.9,
        "top_k": 50,
        "top_p": 1.0,
        "repetition_penalty": 1.1,
        "depth_temperature": 0.9,
        "depth_top_k": 50,
        "depth_top_p": 1.0,
    }
    params.update(overrides)
    return params


def _fake_np():
    class _Arr(list):
        def tolist(self):
            return list(self)

    class _NP:
        float32 = "float32"

        @staticmethod
        def asarray(audio, dtype=None):
            return list(audio)

        @staticmethod
        def concatenate(chunks):
            return _Arr(item for chunk in chunks for item in chunk)

    return _NP()


class SeedAndDepthSamplingTests(unittest.TestCase):
    def _runtime(self, graph):
        return type(
            "Runtime",
            (),
            {
                "config": _FastConfig(),
                "model": type(
                    "Model",
                    (),
                    {
                        "generation_config": type("G", (), {})(),
                        "depth_decoder": type("D", (), {"generation_config": type("G", (), {})()})(),
                    },
                )(),
                "_depth_decoder_graph": graph,
            },
        )()

    def test_seed_is_applied_after_prepare_inputs(self):
        order = []
        backend = OfficialGpuBackend({})
        backend.runtime = self._runtime(_DepthGraph(1024, 2048))
        backend.replace = replace
        backend.spec = load_profile_spec("stock")
        backend.tokenizer = object()
        backend.codec = object()
        backend.model = backend.runtime.model
        backend.get_template = lambda name: name
        backend._torch = type("Torch", (), {"cuda": type("Cuda", (), {"is_available": staticmethod(lambda: False)})()})()
        backend.np = _fake_np()

        def prepare_inputs(*_args, **_kwargs):
            order.append("prepare")
            return {
                "text_ids_len": [32],
                "input_ids": type("T", (), {"shape": (1, 80)})(),
            }

        def set_all_seeds(seed):
            order.append(("seed", int(seed)))

        class Chunk:
            audio = [0.0, 0.25]

        backend.prepare_inputs = prepare_inputs
        backend.set_all_seeds = set_all_seeds
        backend.runtime.iter_audio_chunks = lambda *_args, **_kwargs: iter([Chunk()])
        params = parse_generate_params({"params": _params(seed=301)})
        backend._generate(params)
        self.assertEqual(order, ["prepare", ("seed", 301)])

    def test_nonpositive_seed_does_not_reset_rng(self):
        seeded = []
        backend = OfficialGpuBackend({})
        backend.runtime = self._runtime(_DepthGraph(1024, 2048))
        backend.replace = replace
        backend.spec = load_profile_spec("stock")
        backend.tokenizer = object()
        backend.codec = object()
        backend.model = backend.runtime.model
        backend.get_template = lambda name: name
        backend._torch = type("Torch", (), {"cuda": type("Cuda", (), {"is_available": staticmethod(lambda: False)})()})()
        backend.np = _fake_np()
        backend.prepare_inputs = lambda *_args, **_kwargs: {
            "text_ids_len": [32],
            "input_ids": type("T", (), {"shape": (1, 80)})(),
        }
        backend.set_all_seeds = seeded.append
        backend.runtime.iter_audio_chunks = lambda *_args, **_kwargs: iter(
            [type("Chunk", (), {"audio": [0.0]})()]
        )
        backend._generate(parse_generate_params({"params": _params(seed=0)}))
        self.assertEqual(seeded, [])

    def test_depth_top_k_zero_rejected_when_workspace_smaller_than_codebook(self):
        graph = _DepthGraph(max_k=1024, codebook_size=2048)
        runtime = self._runtime(graph)
        with self.assertRaises(TtsError) as raised:
            _apply_sampling(runtime, replace, _sampling_params(depth_top_k=0), 1500)
        self.assertEqual(raised.exception.code, "unsupported_profile")
        self.assertIsNone(graph.top_k)

    def test_depth_top_k_zero_accepted_when_workspace_covers_codebook(self):
        graph = _DepthGraph(max_k=2048, codebook_size=2048)
        runtime = self._runtime(graph)
        _apply_sampling(runtime, replace, _sampling_params(depth_top_k=0), 1500)
        self.assertEqual(graph.top_k, 0)
        graph = _DepthGraph(max_k=1024, codebook_size=1024)
        runtime = self._runtime(graph)
        _apply_sampling(runtime, replace, _sampling_params(depth_top_k=0), 1500)
        self.assertEqual(graph.top_k, 0)

    def test_positive_depth_top_k_still_capped_by_workspace(self):
        graph = _DepthGraph(max_k=1024, codebook_size=2048)
        runtime = self._runtime(graph)
        _apply_sampling(runtime, replace, _sampling_params(depth_top_k=50), 1500)
        self.assertEqual(graph.top_k, 50)
        with self.assertRaises(TtsError) as raised:
            _apply_sampling(runtime, replace, _sampling_params(depth_top_k=1025), 1500)
        self.assertEqual(raised.exception.code, "unsupported_profile")


if __name__ == "__main__":
    unittest.main()


class OutputBudgetTest(unittest.TestCase):
    def test_sparse_padding_cannot_silently_shorten_output(self):
        from tts_profiles import guard_output_budget, UnsupportedProfile
        guard_output_budget(1500, 544)  # expanded, actual prefill 513
        with self.assertRaises(UnsupportedProfile):
            guard_output_budget(1500, 768)  # compact must fall back
        guard_output_budget(2047 - 512, 512)
        with self.assertRaises(UnsupportedProfile):
            guard_output_budget(2047 - 500, 512)


class InferenceThreadTest(unittest.TestCase):
    def test_rpc_thread_can_update_buffers_created_by_warmup(self):
        try:
            import torch
        except ImportError:
            self.skipTest("requires Torch; also run with the canary runtime Python")
        from asset_gen_v7_lite_tts_runtime import OfficialGpuBackend
        from concurrent.futures import ThreadPoolExecutor
        with torch.inference_mode():
            sampling_buffer = torch.zeros(1)
        backend = object.__new__(OfficialGpuBackend)
        backend._torch = torch
        backend._generate = lambda params, cancel: sampling_buffer.fill_(params["temperature"]).item()
        with ThreadPoolExecutor(max_workers=1) as executor:
            value = executor.submit(backend.generate, {"temperature": 0.9}).result()
        self.assertAlmostEqual(value, 0.9, places=6)

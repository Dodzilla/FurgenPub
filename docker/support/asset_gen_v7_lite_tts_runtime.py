#!/usr/bin/env python3
"""Official Breeze fast-all Unix-socket runtime for asset_gen_v7_lite.

Working package pins (isolated env, not imported at process start):
  torch 2.10.0+cu130, transformers 4.57.3, qwen-tts 0.1.1.

The listener is standard-library only. Torch and the official Breeze checkout
are imported inside an admitted warmup after POST /v1/gpu/tts/validate returns
HTTP 200 {"valid": true}. Original checkpoints are never rewritten.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path

from tts_profiles import (
    DEFAULT_INSTRUCTION,
    MAX_REFERENCE_SECONDS,
    MAX_SEQ_LEN,
    MIN_NEW_TOKENS,
    OUTPUT_SAMPLE_RATE,
    PROFILES,
    ProfileError,
    UnsupportedProfile,
    apply_sparse_buckets,
    canonical_fingerprint_payload,
    cap_max_new_tokens,
    decode_f32le,
    disable_joint_text_encoder_merge,
    encode_f32le,
    fingerprint_hex,
    guard_prepared_profile,
    guard_output_budget,
    inspect_prepared_inputs,
    load_profile_spec,
    profile_payload,
    validate_checkpoint_hashes,
    write_float32_wav,
)


MAX_REQUEST_BYTES = 32 * 1024 * 1024
PERMIT_POLL_SECONDS = 1.0
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
PERMIT_KINDS = {"warmup", "generate"}
SOCKET_MODE = 0o600
STARTED_ID_LIMIT = 256
PACKAGES = {
    "torch": "2.10.0+cu130",
    "transformers": "4.57.3",
    "qwen_tts": "0.1.1",
}

# Pinned working set for the isolated official env. Comfy remains on its own
# transformers pin; this process must not import those packages at startup.
WORKING_PACKAGES = PACKAGES


class TtsError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _json_dumps(payload):
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _success(result):
    return {"ok": True, "result": result}


def _error(code, message):
    return {"ok": False, "error": {"code": str(code), "message": sanitize_error_message(message)}}


def sanitize_error_message(message):
    text = " ".join(str(message or "error").split())
    if len(text) > 240:
        text = text[:237] + "..."
    lowered = text.lower()
    for needle in ("input_ids", "attention_mask", "text_ids", "cfg_negative", "token_id"):
        if needle in lowered:
            return "request failed"
    return text or "error"


def terminate_process(code=1):
    os._exit(int(code))


def read_json_line(sock, max_bytes=MAX_REQUEST_BYTES):
    buf = bytearray()
    while True:
        chunk = sock.recv(min(4096, max_bytes + 1 - len(buf)))
        if not chunk:
            break
        newline = chunk.find(b"\n")
        if newline >= 0:
            buf.extend(chunk[:newline])
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise TtsError("invalid_request", "request exceeds 32MiB")
    if len(buf) > max_bytes:
        raise TtsError("invalid_request", "request exceeds 32MiB")
    if not buf:
        raise TtsError("invalid_request", "empty request")
    try:
        payload = json.loads(buf.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TtsError("invalid_request", "request is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise TtsError("invalid_request", "request must be a JSON object")
    return payload


def write_json_line(sock, payload):
    data = (_json_dumps(payload) + "\n").encode("utf-8")
    sock.sendall(data)


def load_runtime_config(path):
    path = Path(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TtsError("invalid_request", "config must be a JSON object")
    required = (
        "sourceDir",
        "checkpointDir",
        "profile",
        "cacheDir",
        "checkpointHashes",
        "sourceRevision",
        "version",
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise TtsError("invalid_request", "config is missing required fields")
    profile = str(payload["profile"]).strip()
    if profile not in PROFILES:
        raise TtsError("invalid_request", "profile must be stock, expanded, or compact")
    hashes = payload["checkpointHashes"]
    if not isinstance(hashes, dict) or not hashes:
        raise TtsError("invalid_request", "checkpointHashes must be a non-empty object")
    return {
        "sourceDir": str(Path(payload["sourceDir"])),
        "checkpointDir": str(Path(payload["checkpointDir"])),
        "profile": profile,
        "cacheDir": str(Path(payload["cacheDir"])),
        "checkpointHashes": {str(key): str(value) for key, value in hashes.items()},
        "sourceRevision": str(payload["sourceRevision"]),
        "version": str(payload["version"]),
        "packagePins": dict(payload.get("packagePins") or {}),
    }


def parse_permit(payload, expected_kind):
    permit = payload.get("permit")
    if not isinstance(permit, dict):
        raise TtsError("invalid_permit", "permit is required")
    kind = str(permit.get("kind") or "").strip()
    if kind not in PERMIT_KINDS or kind != expected_kind:
        raise TtsError("invalid_permit", "permit kind is invalid")
    epoch = permit.get("epoch")
    fencing = permit.get("fencingToken")
    work_id = permit.get("workId")
    if epoch is None or fencing in (None, "") or work_id in (None, ""):
        raise TtsError("invalid_permit", "permit is incomplete")
    try:
        epoch = int(epoch)
    except (TypeError, ValueError) as exc:
        raise TtsError("invalid_permit", "permit epoch is invalid") from exc
    return {
        "kind": kind,
        "epoch": epoch,
        "fencingToken": str(fencing),
        "workId": str(work_id),
    }


def parse_request_id(payload, required=True):
    value = payload.get("requestId")
    if value in (None, ""):
        if required:
            raise TtsError("invalid_request", "requestId is required")
        return None
    value = str(value)
    if not REQUEST_ID_RE.fullmatch(value):
        raise TtsError("invalid_request", "requestId is invalid")
    return value


class PermitClient:
    def __init__(self, coordinator_url, timeout=5, opener=None):
        self.url = str(coordinator_url).rstrip("/") + "/v1/gpu/tts/validate"
        self.timeout = float(timeout)
        self.opener = opener

    def validate(self, permit):
        body = _json_dumps(permit).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            if self.opener is not None:
                response = self.opener.open(request, timeout=self.timeout)
            else:
                response = urllib.request.urlopen(request, timeout=self.timeout)
            with response:
                if getattr(response, "status", 200) != 200:
                    return False
                payload = json.loads(response.read().decode("utf-8") or "null")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, OSError):
            return False
        return isinstance(payload, dict) and payload.get("valid") is True


class PermitWatch:
    def __init__(self, client, permit, on_invalid, interval=PERMIT_POLL_SECONDS):
        self.client = client
        self.permit = permit
        self.on_invalid = on_invalid
        self.interval = max(0.05, float(interval))
        self._stop = threading.Event()
        self._alive = True
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="tts-permit-watch", daemon=True)

    def _armed(self):
        with self._lock:
            return self._alive and not self._stop.is_set()

    def _run(self):
        while not self._stop.wait(self.interval):
            if not self._armed():
                return
            valid = self.client.validate(self.permit)
            if not self._armed():
                return
            if not valid:
                self.on_invalid()
                return

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        """Disarm immediately. Do not join a possibly blocked validate() on the GPU path."""
        with self._lock:
            self._alive = False
            self._stop.set()


def verify_source_revision(source_dir, expected, git_output=None):
    source_dir = Path(source_dir)
    expected = str(expected or "").strip()
    if not expected:
        raise TtsError("invalid_request", "sourceRevision is required")
    if git_output is None:
        try:
            head = subprocess.check_output(
                ["git", "-C", str(source_dir), "rev-parse", "HEAD"],
                text=True,
                timeout=10,
                stderr=subprocess.DEVNULL,
            ).strip()
            porcelain = subprocess.check_output(
                ["git", "-C", str(source_dir), "status", "--porcelain", "-uno"],
                text=True,
                timeout=10,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise TtsError("invalid_request", "source revision cannot be verified") from exc
    else:
        head, porcelain = git_output
    if head != expected:
        raise TtsError("invalid_request", "sourceRevision does not match git HEAD")
    dirty = [line for line in str(porcelain or "").splitlines() if line.strip() and not line.startswith("??")]
    if dirty:
        raise TtsError("invalid_request", "source tree has dirty tracked files")
    return head


def assert_package_pins(versions, required=None):
    required = required or PACKAGES
    for name, expected in required.items():
        actual = str((versions or {}).get(name) or "")
        if actual != str(expected):
            raise TtsError("invalid_request", f"{name} pin mismatch")
    return True


def require_cuda(available):
    if not available:
        raise TtsError("cuda_error", "CUDA is required")
    return True


def parse_reference_audio(payload):
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise TtsError("invalid_request", "referenceAudio must be an object")
    try:
        samples = int(payload.get("samples"))
        sample_rate = int(payload.get("sampleRate"))
    except (TypeError, ValueError) as exc:
        raise TtsError("invalid_request", "referenceAudio fields are invalid") from exc
    if sample_rate <= 0 or samples <= 0:
        raise TtsError("invalid_request", "referenceAudio must be non-empty")
    if samples / float(sample_rate) > MAX_REFERENCE_SECONDS:
        raise TtsError("invalid_request", "reference audio exceeds 60s")
    data = payload.get("data")
    if not isinstance(data, str) or not data:
        raise TtsError("invalid_request", "referenceAudio.data is required")
    try:
        raw = base64.b64decode(data, validate=False)
    except (ValueError, TypeError) as exc:
        raise TtsError("invalid_request", "referenceAudio.data is not base64") from exc
    values = decode_f32le(raw, samples)
    return {"samples": values, "sampleRate": sample_rate, "count": samples}


def parse_generate_params(payload):
    params = payload.get("params")
    if not isinstance(params, dict):
        raise TtsError("invalid_request", "params are required")
    text = str(params.get("text") or "").strip()
    if not text:
        raise TtsError("invalid_request", "text cannot be empty")
    instruction = str(params.get("instruction") or "").strip() or DEFAULT_INSTRUCTION
    try:
        cfg_scale = float(params.get("cfg_scale", 1.0))
        max_new_tokens = int(params.get("max_new_tokens", 1500))
        temperature = float(params.get("temperature", 0.9))
        top_k = int(params.get("top_k", 50))
        top_p = float(params.get("top_p", 1.0))
        repetition_penalty = float(params.get("repetition_penalty", 1.1))
        depth_temperature = float(params.get("depth_temperature", 0.9))
        depth_top_k = int(params.get("depth_top_k", 50))
        depth_top_p = float(params.get("depth_top_p", 1.0))
        seed = int(params.get("seed", 42))
    except (TypeError, ValueError) as exc:
        raise TtsError("invalid_request", "generation controls are invalid") from exc
    if not (cfg_scale > 0) or cfg_scale != cfg_scale:
        raise TtsError("invalid_request", "cfg_scale must be greater than 0")
    if max_new_tokens < MIN_NEW_TOKENS:
        raise TtsError("invalid_request", "max_new_tokens must be at least 64")
    if repetition_penalty <= 0:
        raise TtsError("invalid_request", "repetition_penalty must be > 0")
    reference = parse_reference_audio(payload.get("referenceAudio"))
    ref_text = str(params.get("ref_text") or "").strip()
    if reference is not None and not ref_text:
        raise TtsError("invalid_request", "reference audio requires its exact transcript")
    if reference is None and ref_text:
        raise TtsError("invalid_request", "ref_text requires referenceAudio")
    return {
        "text": text,
        "instruction": instruction,
        "cfg_scale": cfg_scale,
        "max_new_tokens": max_new_tokens,
        "temperature": temperature,
        "top_k": top_k,
        "top_p": top_p,
        "repetition_penalty": repetition_penalty,
        "depth_temperature": depth_temperature,
        "depth_top_k": depth_top_k,
        "depth_top_p": depth_top_p,
        "seed": seed,
        "ref_text": ref_text,
        "reference": reference,
    }


def encode_audio_result(samples, sample_rate=OUTPUT_SAMPLE_RATE):
    values = [float(value) for value in samples]
    return {
        "data": base64.b64encode(encode_f32le(values)).decode("ascii"),
        "sampleRate": int(sample_rate),
        "samples": len(values),
    }


class OfficialGpuBackend:
    """Loads official fast-all only after the caller has admitted a warmup permit."""

    def __init__(self, config):
        self.config = config
        self.runtime = None
        self.tokenizer = None
        self.model = None
        self.codec = None
        self.manifest = None
        self.fingerprint_payload = None
        self.fingerprint = None
        self.package_versions = dict(PACKAGES)
        self.peaks = None
        self.idle = None
        self.warmup_ms = None
        self._torch = None

    def warmup(self, cancel_event=None):
        started = time.monotonic()
        source_dir = Path(self.config["sourceDir"]).resolve()
        checkpoint_dir = Path(self.config["checkpointDir"]).resolve()
        cache_dir = Path(self.config["cacheDir"]).resolve()
        cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(cache_dir, 0o700)
        validate_checkpoint_hashes(checkpoint_dir, self.config["checkpointHashes"])
        verify_source_revision(source_dir, self.config["sourceRevision"])
        isolated = str(source_dir)
        if isolated not in sys.path:
            sys.path.insert(0, isolated)

        import numpy as np
        import torch
        from dataclasses import replace

        self._torch = torch
        require_cuda(bool(torch.cuda.is_available()))
        self.package_versions = {
            "torch": str(getattr(torch, "__version__", "")),
            "transformers": _package_version("transformers", ""),
            "qwen_tts": _package_version("qwen-tts", "") or _package_version("qwen_tts", ""),
            "numpy": str(getattr(np, "__version__", "")),
        }
        assert_package_pins(self.package_versions)
        dependency_versions = {name: _package_version(name, "") for name in self.config.get("packagePins", {})}
        if self.config.get("packagePins"):
            assert_package_pins(dependency_versions, self.config["packagePins"])

        from breeze_infer.runtime import load_runtime, set_all_seeds, update_generation_config_for_breeze
        from breeze_infer.templates import get_template, prepare_inputs
        from models.fast_streaming import FastBreezeStreamingRuntime, FastStreamingConfig
        from models.warmup_profile import parse_warmup_profile

        device_name = torch.cuda.get_device_name(0)
        capability = tuple(torch.cuda.get_device_capability(0))
        cuda_version = getattr(getattr(torch, "version", None), "cuda", None)
        self.fingerprint_payload = canonical_fingerprint_payload(
            source_revision=self.config["sourceRevision"],
            checkpoint_hashes=self.config["checkpointHashes"],
            profile=self.config["profile"],
            version=self.config["version"],
            torch_version=self.package_versions["torch"],
            cuda_version=cuda_version,
            device_name=device_name,
            device_capability=capability,
            transformers_version=self.package_versions["transformers"],
            qwen_tts_version=self.package_versions["qwen_tts"],
            numpy_version=self.package_versions["numpy"],
            extra_versions=dependency_versions,
        )
        self.fingerprint = fingerprint_hex(self.fingerprint_payload)
        inductor = cache_dir / self.fingerprint / "inductor"
        triton = cache_dir / self.fingerprint / "triton"
        inductor.mkdir(parents=True, exist_ok=True, mode=0o700)
        triton.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.environ["TORCHINDUCTOR_CACHE_DIR"] = str(inductor)
        os.environ["TRITON_CACHE_DIR"] = str(triton)
        try:
            torch._inductor.config.fx_graph_cache = True
            torch._inductor.config.autotune_local_cache = True
        except Exception:
            pass

        view = materialize_attention_view(checkpoint_dir, cache_dir / self.fingerprint)
        torch.cuda.reset_peak_memory_stats()
        tokenizer, model, codec = load_runtime(
            view,
            device="cuda:0",
            attn_implementation="eager",
        )
        _force_text_encoder_sdpa(model)
        update_generation_config_for_breeze(model)
        runtime = FastBreezeStreamingRuntime(
            model,
            codec,
            FastStreamingConfig(
                max_new_tokens=1500,
                max_seq_len=MAX_SEQ_LEN,
                fast_all=True,
                repetition_penalty=1.1,
                collect_timing=True,
            ),
            tokenizer=tokenizer,
        )
        spec = load_profile_spec(self.config["profile"], source_dir=source_dir)
        payload = profile_payload(self.config["profile"], source_dir=source_dir)
        profile = parse_warmup_profile(payload, source=self.config["profile"])
        profile = replace(profile, codec_chunk_frames=runtime.codec_chunk_frames)
        if cancel_event is not None and cancel_event.is_set():
            raise TtsError("cancelled", "warmup cancelled")
        manifest = runtime.warmup_from_profile(profile)
        apply_sparse_buckets(runtime, spec)
        disable_joint_text_encoder_merge(runtime)
        _synchronize(torch)
        self.runtime = runtime
        self.tokenizer = tokenizer
        self.model = model
        self.codec = codec
        self.manifest = manifest
        self.peaks = _memory_peaks(torch)
        self.idle = _memory_idle(torch)
        self.warmup_ms = (time.monotonic() - started) * 1000.0
        self.np = np
        self.set_all_seeds = set_all_seeds
        self.prepare_inputs = prepare_inputs
        self.get_template = get_template
        self.replace = replace
        self.spec = spec
        return {
            "manifest": manifest,
            "fingerprint": self.fingerprint,
            "fingerprintPayload": self.fingerprint_payload,
            "peaks": self.peaks,
            "idle": self.idle,
            "durationMs": self.warmup_ms,
            "packages": dict(self.package_versions),
        }

    def generate(self, params, cancel_event=None):
        if self.runtime is None:
            raise TtsError("not_ready", "runtime is not warmed")
        torch = self._torch
        started = time.monotonic()
        prepare_started = started
        first_audio_ms = None
        ref_path = None
        if torch.cuda.is_available():
            torch.cuda.reset_peak_memory_stats()
        try:
            if params["reference"] is not None:
                handle, ref_path = tempfile.mkstemp(prefix="breeze_ref_", suffix=".wav")
                os.close(handle)
                os.chmod(ref_path, 0o600)
                write_float32_wav(
                    ref_path,
                    params["reference"]["samples"],
                    params["reference"]["sampleRate"],
                )
            request = {
                "id": "generate",
                "text": params["text"],
                "instruction": params["instruction"],
                "speaker": "S0",
            }
            template_name = "tts_instruction"
            if ref_path is not None:
                request["ref_audio_path"] = str(ref_path)
                request["ref_text"] = params["ref_text"]
                template_name = "ref_edit_tata"
            if params["seed"] > 0:
                self.set_all_seeds(params["seed"])
            inputs = self.prepare_inputs(
                self.tokenizer,
                self.codec,
                self.model,
                [request],
                self.get_template(template_name),
                guidance_scale=params["cfg_scale"],
                guidance_scale_ref=None,
                guidance_scale_ins=None,
            )
            inspection = inspect_prepared_inputs(inputs)
            prepared_profile = guard_prepared_profile(self.spec, inspection)
            max_frames = cap_max_new_tokens(params["max_new_tokens"], inspection["prefill_len"])
            guard_output_budget(max_frames, prepared_profile["prefill_bucket"])
            _apply_sampling(self.runtime, self.replace, params, max_frames)
            prepare_ms = (time.monotonic() - prepare_started) * 1000.0
            chunks = []
            iterator = self.runtime.iter_audio_chunks(inputs, request_id="generate")
            try:
                for chunk in iterator:
                    if cancel_event is not None and cancel_event.is_set():
                        iterator.close()
                        _synchronize(torch)
                        raise TtsError("cancelled", "generation cancelled")
                    if first_audio_ms is None:
                        first_audio_ms = (time.monotonic() - started) * 1000.0
                    audio = getattr(chunk, "audio", chunk)
                    chunks.append(self.np.asarray(audio, dtype=self.np.float32))
            finally:
                close = getattr(iterator, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
            if not chunks:
                raise TtsError("generation_failed", "generation produced no audio")
            audio = self.np.concatenate(chunks)
            _synchronize(torch)
            generation_ms = (time.monotonic() - started) * 1000.0
            peaks = _memory_peaks(torch)
            sample_rate = int(getattr(self.runtime, "sample_rate", OUTPUT_SAMPLE_RATE))
            return {
                "audio": encode_audio_result(audio.tolist(), sample_rate),
                "timing": {
                    "prepareMs": prepare_ms,
                    "generationMs": generation_ms,
                    "firstAudioMs": first_audio_ms,
                    "peakAllocatedBytes": peaks.get("allocatedBytes"),
                    "peakReservedBytes": peaks.get("reservedBytes"),
                    "prefillLen": int(inspection["prefill_len"]),
                    "maxNewTokens": int(max_frames),
                },
            }
        except UnsupportedProfile:
            raise
        except ProfileError:
            raise
        except TtsError:
            raise
        except AssertionError as exc:
            raise TtsError("unsupported_profile", "sampling control is outside the warmed graph") from exc
        except Exception as exc:
            raise _translate_cuda(exc) from exc
        finally:
            if ref_path is not None:
                try:
                    os.unlink(ref_path)
                except OSError:
                    pass
            _synchronize(self._torch)


def _apply_sampling(runtime, replace, params, max_frames):
    runtime.config = replace(
        runtime.config,
        max_new_tokens=int(max_frames),
        repetition_penalty=float(params["repetition_penalty"]),
        temperature=None,
        top_k=None,
        top_p=None,
    )
    generation = getattr(runtime.model, "generation_config", None)
    if generation is not None:
        generation.temperature = float(params["temperature"])
        generation.top_k = int(params["top_k"])
        generation.top_p = float(params["top_p"])
        generation.do_sample = True
        generation.max_new_tokens = int(max_frames)
    depth_gen = getattr(getattr(runtime.model, "depth_decoder", None), "generation_config", None)
    if depth_gen is not None:
        depth_gen.temperature = float(params["depth_temperature"])
        depth_gen.top_k = int(params["depth_top_k"])
        depth_gen.top_p = float(params["depth_top_p"])
        depth_gen.do_sample = True
    graph = getattr(runtime, "_depth_decoder_graph", None)
    if graph is None:
        return
    try:
        if hasattr(graph, "set_temperature"):
            graph.set_temperature(float(params["depth_temperature"]))
        if hasattr(graph, "set_top_p"):
            graph.set_top_p(float(params["depth_top_p"]))
        if hasattr(graph, "set_top_k"):
            top_k = int(params["depth_top_k"])
            max_k = int(getattr(graph, "_max_k", 1024))
            if top_k > max_k:
                raise TtsError("unsupported_profile", "depth top_k exceeds the warmed graph")
            graph.set_top_k(top_k)
    except TtsError:
        raise
    except AssertionError as exc:
        raise TtsError("unsupported_profile", "depth sampling is outside the warmed graph") from exc


def _force_text_encoder_sdpa(model):
    encoder = getattr(model, "text_encoder", None)
    if encoder is not None and getattr(encoder, "config", None) is not None:
        encoder.config._attn_implementation = "sdpa"
        if hasattr(encoder.config, "preferred_attn_implementation"):
            encoder.config.preferred_attn_implementation = "sdpa"
    config = getattr(model, "config", None)
    text_config = getattr(config, "text_encoder_config", None) if config is not None else None
    if text_config is not None:
        try:
            text_config._attn_implementation = "sdpa"
            text_config.preferred_attn_implementation = "sdpa"
        except Exception:
            pass


def materialize_attention_view(checkpoint_dir, cache_root):
    """Private config view: keep original files; prefer SDPA when FlashAttention 2 is absent."""
    checkpoint_dir = Path(checkpoint_dir)
    config_path = checkpoint_dir / "config.json"
    if not config_path.is_file():
        return checkpoint_dir
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    text_config = payload.get("text_encoder_config")
    if not isinstance(text_config, dict):
        return checkpoint_dir
    preferred = str(
        text_config.get("preferred_attn_implementation")
        or text_config.get("_attn_implementation")
        or ""
    )
    flash_ok = False
    try:
        import flash_attn  # noqa: F401

        flash_ok = True
    except Exception:
        flash_ok = False
    if preferred != "flash_attention_2" or flash_ok:
        return checkpoint_dir
    text_config["preferred_attn_implementation"] = "sdpa"
    text_config["_attn_implementation"] = "sdpa"
    payload["text_encoder_config"] = text_config
    view = Path(cache_root) / "checkpoint-view"
    view.mkdir(parents=True, exist_ok=True, mode=0o700)
    for item in checkpoint_dir.iterdir():
        dest = view / item.name
        if dest.exists() or dest.is_symlink():
            dest.unlink()
        if item.name == "config.json":
            dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            os.chmod(dest, 0o600)
        else:
            dest.symlink_to(item)
    return view


def _package_version(name, fallback):
    dist = name.replace("_", "-")
    module_name = name.replace("-", "_")
    try:
        from importlib.metadata import version

        return version(dist)
    except Exception:
        module = sys.modules.get(module_name)
        return str(getattr(module, "__version__", fallback) or fallback)


def _memory_peaks(torch):
    if torch is None or not torch.cuda.is_available():
        return {"allocatedBytes": 0, "reservedBytes": 0}
    return {
        "allocatedBytes": int(torch.cuda.max_memory_allocated()),
        "reservedBytes": int(torch.cuda.max_memory_reserved()),
    }


def _memory_idle(torch):
    if torch is None or not torch.cuda.is_available():
        return {"allocatedBytes": 0, "reservedBytes": 0}
    return {
        "allocatedBytes": int(torch.cuda.memory_allocated()),
        "reservedBytes": int(torch.cuda.memory_reserved()),
    }


def _synchronize(torch):
    if torch is None:
        return
    try:
        if torch.cuda.is_available():
            torch.cuda.synchronize()
    except Exception as exc:
        raise _translate_cuda(exc) from exc


def _translate_cuda(exc):
    name = type(exc).__name__
    message = str(exc)
    combined = f"{name} {message}".lower()
    if "cuda" in combined or name.lower().startswith("cuda"):
        return TtsError("cuda_error", "CUDA execution failed")
    if isinstance(exc, UnsupportedProfile):
        return TtsError("unsupported_profile", str(exc) or "input is outside the warmed profile")
    if isinstance(exc, ProfileError):
        return TtsError(getattr(exc, "code", "invalid_request"), str(exc))
    return TtsError("generation_failed", sanitize_error_message(message or name))


class TtsRuntimeApp:
    def __init__(
        self,
        config,
        coordinator_url,
        backend=None,
        permit_client=None,
        poll_interval=PERMIT_POLL_SECONDS,
        terminator=None,
    ):
        self.config = config
        self.coordinator_url = str(coordinator_url).rstrip("/")
        self.permits = permit_client or PermitClient(self.coordinator_url)
        self.poll_interval = float(poll_interval)
        self.terminator = terminator or terminate_process
        self.backend_factory = backend
        self.backend = None if backend is None or callable(backend) else backend
        self.lock = threading.Lock()
        self.gpu_lock = threading.Lock()
        self.cancel_events = {}
        self.in_flight = None
        self.started_ids = set()
        self.started_order = deque()
        self.boot_ms = int(time.time() * 1000)
        self.synchronize = _synchronize
        self.stage = "idle"
        self.ready = False
        self.last_error = None
        self.spec = load_profile_spec(config["profile"])
        self.config_fingerprint = fingerprint_hex(
            canonical_fingerprint_payload(
                source_revision=config["sourceRevision"],
                checkpoint_hashes=config["checkpointHashes"],
                profile=config["profile"],
                version=config["version"],
            )
        )

    def _backend(self):
        if self.backend is not None:
            return self.backend
        factory = self.backend_factory or OfficialGpuBackend
        self.backend = factory(self.config) if callable(factory) else factory
        return self.backend

    def health(self):
        with self.lock:
            backend = self.backend
            result = {
                "stage": self.stage,
                "ready": bool(self.ready),
                "profile": self.config["profile"],
                "version": self.config["version"],
                "sourceRevision": self.config["sourceRevision"],
                "fingerprint": getattr(backend, "fingerprint", None) or self.config_fingerprint,
                "profileCertified": False,
                "packages": PACKAGES,
                "inFlight": None
                if self.in_flight is None
                else {
                    "method": self.in_flight.get("method"),
                    "requestId": self.in_flight.get("requestId"),
                },
            }
            if backend is not None:
                if getattr(backend, "manifest", None) is not None:
                    result["manifest"] = backend.manifest
                if getattr(backend, "peaks", None) is not None:
                    result["peakAllocatedBytes"] = backend.peaks.get("allocatedBytes")
                    result["peakReservedBytes"] = backend.peaks.get("reservedBytes")
                if getattr(backend, "idle", None) is not None:
                    result["idleAllocatedBytes"] = backend.idle.get("allocatedBytes")
                    result["idleReservedBytes"] = backend.idle.get("reservedBytes")
                if getattr(backend, "warmup_ms", None) is not None:
                    result["warmupDurationMs"] = backend.warmup_ms
                if getattr(backend, "package_versions", None):
                    result["packages"] = dict(backend.package_versions)
            if self.last_error:
                result["lastErrorCode"] = self.last_error
            return result

    def handle(self, request):
        if not isinstance(request, dict):
            return _error("invalid_request", "request must be a JSON object")
        method = str(request.get("method") or "").strip()
        try:
            if method == "health":
                return _success(self.health())
            if method == "cancel":
                return self._cancel(request)
            if method == "warmup":
                return self._warmup(request)
            if method == "generate":
                return self._generate(request)
            raise TtsError("invalid_request", "unknown method")
        except TtsError as exc:
            return _error(exc.code, exc.message)
        except UnsupportedProfile as exc:
            return _error("unsupported_profile", str(exc) or "input is outside the warmed profile")
        except ProfileError as exc:
            return _error(getattr(exc, "code", "invalid_request"), str(exc))
        except Exception as exc:
            return _error(_translate_cuda(exc).code, _translate_cuda(exc).message)

    def _set_stage(self, stage, ready=None, error=None):
        with self.lock:
            self.stage = stage
            if ready is not None:
                self.ready = bool(ready)
            if error is not None:
                self.last_error = error

    def _begin(self, method, request_id=None):
        with self.lock:
            if request_id and (
                request_id in self.started_ids
                or (self.in_flight and self.in_flight.get("requestId") == request_id)
            ):
                raise TtsError("duplicate_request", "requestId already used")
            if self.in_flight is not None:
                raise TtsError("busy", "runtime is busy")
            cancel = threading.Event()
            self.in_flight = {"method": method, "requestId": request_id, "cancel": cancel}
            if request_id:
                self.cancel_events[request_id] = cancel
            return cancel

    def _finish(self, request_id=None, consume=False, stage=None, ready=None, error=None):
        with self.lock:
            self.in_flight = None
            if request_id:
                self.cancel_events.pop(request_id, None)
                if consume:
                    if request_id not in self.started_ids:
                        self.started_ids.add(request_id)
                        self.started_order.append(request_id)
                    while len(self.started_order) > STARTED_ID_LIMIT:
                        old = self.started_order.popleft()
                        self.started_ids.discard(old)
            if stage is not None:
                self.stage = stage
            if ready is not None:
                self.ready = bool(ready)
            if error is not None:
                self.last_error = error

    def _cancel(self, request):
        request_id = parse_request_id(request, required=True)
        with self.lock:
            event = self.cancel_events.get(request_id)
            in_flight = self.in_flight
        if event is None or in_flight is None or in_flight.get("requestId") != request_id:
            raise TtsError("not_found", "no matching in-flight generation")
        event.set()
        return _success({"cancelled": True, "requestId": request_id})

    def _require_permit(self, permit):
        if not self.permits.validate(permit):
            raise TtsError("invalid_permit", "permit rejected")

    def _warmup(self, request):
        permit = parse_permit(request, "warmup")
        if self.ready and getattr(self.backend, "manifest", None) is not None:
            self._require_permit(permit)
            return _success(self.health())
        cancel = self._begin("warmup")
        acquired = False
        watch = None
        try:
            self._require_permit(permit)
            if not self.gpu_lock.acquire(blocking=False):
                raise TtsError("busy", "runtime is busy")
            acquired = True
            self._require_permit(permit)
            self._set_stage("warming", ready=False)

            def on_invalid():
                self._set_stage("failed", ready=False, error="invalid_permit")
                self.terminator(1)

            watch = PermitWatch(self.permits, permit, on_invalid, self.poll_interval).start()
            backend = self._backend()
            result = backend.warmup(cancel_event=cancel)
            self.synchronize(getattr(backend, "_torch", None))
            if not self.permits.validate(permit):
                raise TtsError("invalid_permit", "permit was revoked")
            if cancel.is_set():
                raise TtsError("cancelled", "warmup cancelled")
            self._finish(stage="ready", ready=True, error="")
            health = self.health()
            health.update(
                {
                    "manifest": result.get("manifest"),
                    "fingerprint": result.get("fingerprint") or health.get("fingerprint"),
                    "peakAllocatedBytes": (result.get("peaks") or {}).get("allocatedBytes"),
                    "peakReservedBytes": (result.get("peaks") or {}).get("reservedBytes"),
                    "idleAllocatedBytes": (result.get("idle") or {}).get("allocatedBytes"),
                    "idleReservedBytes": (result.get("idle") or {}).get("reservedBytes"),
                    "warmupDurationMs": result.get("durationMs"),
                    "profileCertified": False,
                }
            )
            return _success(health)
        except Exception as exc:
            tts_exc = exc if isinstance(exc, TtsError) else _translate_cuda(exc)
            try:
                self.synchronize(getattr(self.backend, "_torch", None))
            except Exception as sync_exc:
                tts_exc = _translate_cuda(sync_exc)
            stage = "failed" if tts_exc.code in {"cuda_error", "invalid_permit"} else "idle"
            self._finish(stage=stage, ready=False, error=tts_exc.code)
            raise tts_exc
        finally:
            if watch is not None:
                watch.stop()
            if acquired:
                self.gpu_lock.release()

    def _generate(self, request):
        request_id = parse_request_id(request, required=True)
        permit = parse_permit(request, "generate")
        params = parse_generate_params(request)
        cancel = self._begin("generate", request_id)
        acquired = False
        watch = None
        consume = False
        permit_lost = threading.Event()
        try:
            if not self.ready:
                raise TtsError("not_ready", "runtime is not warmed")
            self._require_permit(permit)
            if not self.gpu_lock.acquire(blocking=False):
                raise TtsError("busy", "runtime is busy")
            acquired = True
            self._require_permit(permit)
            consume = True
            self._set_stage("generating")

            def on_invalid():
                permit_lost.set()
                cancel.set()

            watch = PermitWatch(self.permits, permit, on_invalid, self.poll_interval).start()
            backend = self._backend()
            result = backend.generate(params, cancel_event=cancel)
            self.synchronize(getattr(backend, "_torch", None))
            if permit_lost.is_set() or not self.permits.validate(permit):
                raise TtsError("invalid_permit", "permit was revoked")
            if cancel.is_set():
                raise TtsError("cancelled", "generation cancelled")
            self._finish(request_id, consume=True, stage="ready", ready=True, error="")
            return _success(result)
        except Exception as exc:
            tts_exc = exc if isinstance(exc, TtsError) else _translate_cuda(exc)
            if tts_exc.code == "cancelled" and permit_lost.is_set():
                tts_exc = TtsError("invalid_permit", "permit was revoked")
            try:
                self.synchronize(getattr(self.backend, "_torch", None))
            except Exception as sync_exc:
                tts_exc = _translate_cuda(sync_exc)
            gpu_fault = tts_exc.code == "cuda_error" or (
                consume and not isinstance(exc, (TtsError, UnsupportedProfile, ProfileError))
            )
            if gpu_fault:
                stage, ready_state = "failed", False
            elif self.ready:
                stage, ready_state = "ready", True
            else:
                stage, ready_state = "idle", False
            self._finish(
                request_id,
                consume=consume,
                stage=stage,
                ready=ready_state,
                error=tts_exc.code,
            )
            raise tts_exc
        finally:
            if watch is not None:
                watch.stop()
            if acquired:
                self.gpu_lock.release()


class _Handler(socketserver.StreamRequestHandler):
    timeout = 600

    def handle(self):
        app = self.server.app
        try:
            request = read_json_line(self.connection)
            response = app.handle(request)
        except TtsError as exc:
            response = _error(exc.code, exc.message)
        except Exception:
            response = _error("internal_error", "internal error")
        try:
            write_json_line(self.connection, response)
        except OSError:
            return


class ThreadingUnixStreamServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def server_bind(self):
        super().server_bind()
        os.chmod(self.server_address, SOCKET_MODE)


def create_server(socket_path, app):
    socket_path = str(socket_path)
    if os.path.exists(socket_path):
        os.unlink(socket_path)
    Path(socket_path).parent.mkdir(parents=True, exist_ok=True)
    server = ThreadingUnixStreamServer(socket_path, _Handler)
    server.app = app
    os.chmod(socket_path, SOCKET_MODE)
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(description="Official Breeze fast-all TTS runtime")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--coordinator", default="http://127.0.0.1:8189")
    args = parser.parse_args(argv)
    config = load_runtime_config(args.config)
    app = TtsRuntimeApp(config, args.coordinator)
    server = create_server(args.socket, app)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        if os.path.exists(args.socket):
            os.unlink(args.socket)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
        raise

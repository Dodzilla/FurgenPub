#!/usr/bin/env python3
"""Stdlib-only Breeze fast-all serving profiles and request guards.

Stock matches upstream configs/fast.json. Expanded declares every 32-token
bucket through text 1024 and prefill 1536 for branch batches 1 and 2. Compact
declares a sparse set and selects the smallest declared bucket >= length so
existing left/right padding, positions, and KV context budget stay intact.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import types
from pathlib import Path


PROFILES = ("stock", "expanded", "compact")
MAX_SEQ_LEN = 2048
MIN_NEW_TOKENS = 64
MAX_REFERENCE_SECONDS = 60.0
OUTPUT_SAMPLE_RATE = 24000
DEFAULT_INSTRUCTION = "Speak clearly and naturally."
TOKEN_GRANULARITY = 32
CFG_SCALES = (1.0, 4.0)
BRANCH_BATCHES = (1, 2)

STOCK_TEXT_BATCH1 = tuple(range(32, 257, 32))
STOCK_TEXT_BATCH2 = tuple(range(32, 513, 32))
STOCK_PREFILL_BATCH1 = tuple(range(32, 257, 32))
STOCK_PREFILL_BATCH2 = tuple(range(32, 513, 32))

EXPANDED_TEXT = tuple(range(32, 1025, 32))
EXPANDED_PREFILL = tuple(range(32, 1537, 32))

COMPACT_TEXT = (32, 128, 256, 512, 768, 1024)
COMPACT_PREFILL = (128, 256, 512, 768, 1024, 1280, 1536)

STOCK_WARMUP_REQUEST = {
    "template": "tts_instruction",
    "text": "The cat sat on the mat.",
    "instruction": "Speak naturally and clearly.",
    "speaker": "S0",
    "seed": 42,
}


class ProfileError(ValueError):
    code = "invalid_request"


class UnsupportedProfile(ValueError):
    code = "unsupported_profile"


def round_up_32(length):
    length = int(length)
    if length <= 0:
        raise UnsupportedProfile("token length must be positive")
    return ((length + TOKEN_GRANULARITY - 1) // TOKEN_GRANULARITY) * TOKEN_GRANULARITY


def smallest_declared_at_least(length, declared):
    length = int(length)
    if length <= 0:
        raise UnsupportedProfile("token length must be positive")
    for size in declared:
        if int(size) >= length:
            return int(size)
    raise UnsupportedProfile("input exceeds the warmed profile")


def select_bucket(length, declared, mode):
    declared = tuple(int(size) for size in declared)
    if mode == "sparse":
        return smallest_declared_at_least(length, declared)
    rounded = round_up_32(length)
    if rounded not in declared:
        raise UnsupportedProfile("input exceeds the warmed profile")
    return rounded


def _text_graphs(lengths_by_batch):
    graphs = []
    for batch_size, lengths in lengths_by_batch:
        for token_length in lengths:
            graphs.append({"batch_size": int(batch_size), "token_length": int(token_length)})
    return graphs


def _prefill_graphs(lengths_by_batch):
    graphs = []
    for branch_batch_size, lengths in lengths_by_batch:
        for sequence_length in lengths:
            graphs.append(
                {
                    "branch_batch_size": int(branch_batch_size),
                    "sequence_length": int(sequence_length),
                }
            )
    return graphs


def _service_payload(name, text_graphs, prefill_graphs, warmup_request):
    return {
        "schema_version": 1,
        "name": name,
        "service": {
            "concurrency": 1,
            "freeze_after_warmup": True,
            "cfg_scales": list(CFG_SCALES),
        },
        "stages": {
            "text_encoder": {"graphs": text_graphs},
            "backbone_prefill": {"graphs": prefill_graphs},
            "backbone_decode": {
                "graphs": [{"branch_batch_size": batch} for batch in BRANCH_BATCHES]
            },
            "depth_decoder": {"graphs": [{"batch_size": batch} for batch in BRANCH_BATCHES]},
            "codec": {"graphs": [{"num_lanes": 1, "chunk_frames": 1}]},
        },
        "warmup_request": dict(warmup_request),
    }


def stock_profile_payload(source_dir=None):
    if source_dir is not None:
        path = Path(source_dir) / "configs" / "fast.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ProfileError("stock warmup profile must be an object")
        return payload
    return _service_payload(
        "fast",
        _text_graphs(((1, STOCK_TEXT_BATCH1), (2, STOCK_TEXT_BATCH2))),
        _prefill_graphs(((1, STOCK_PREFILL_BATCH1), (2, STOCK_PREFILL_BATCH2))),
        STOCK_WARMUP_REQUEST,
    )


def expanded_profile_payload():
    return _service_payload(
        "expanded",
        _text_graphs(((1, EXPANDED_TEXT), (2, EXPANDED_TEXT))),
        _prefill_graphs(((1, EXPANDED_PREFILL), (2, EXPANDED_PREFILL))),
        STOCK_WARMUP_REQUEST,
    )


def compact_profile_payload():
    return _service_payload(
        "compact",
        _text_graphs(((1, COMPACT_TEXT), (2, COMPACT_TEXT))),
        _prefill_graphs(((1, COMPACT_PREFILL), (2, COMPACT_PREFILL))),
        STOCK_WARMUP_REQUEST,
    )


def profile_payload(name, source_dir=None):
    name = str(name or "").strip()
    if name == "stock":
        return stock_profile_payload(source_dir)
    if name == "expanded":
        return expanded_profile_payload()
    if name == "compact":
        return compact_profile_payload()
    raise ProfileError("profile must be stock, expanded, or compact")


class ProfileSpec:
    def __init__(self, name, payload):
        self.name = name
        self.payload = payload
        self.bucket_mode = "sparse" if name == "compact" else "round32"
        self.text_lengths = {}
        self.prefill_lengths = {}
        stages = payload["stages"]
        for graph in stages["text_encoder"]["graphs"]:
            self.text_lengths.setdefault(int(graph["batch_size"]), set()).add(
                int(graph["token_length"])
            )
        for graph in stages["backbone_prefill"]["graphs"]:
            self.prefill_lengths.setdefault(int(graph["branch_batch_size"]), set()).add(
                int(graph["sequence_length"])
            )
        for batch, lengths in list(self.text_lengths.items()):
            self.text_lengths[batch] = tuple(sorted(lengths))
        for batch, lengths in list(self.prefill_lengths.items()):
            self.prefill_lengths[batch] = tuple(sorted(lengths))
        self.text_batches = tuple(sorted(self.text_lengths))
        self.prefill_batches = tuple(sorted(self.prefill_lengths))

    def text_bucket(self, batch_size, length):
        batch_size = int(batch_size)
        if batch_size not in self.text_lengths:
            raise UnsupportedProfile("text encoder batch is outside the warmed profile")
        return select_bucket(length, self.text_lengths[batch_size], self.bucket_mode)

    def prefill_bucket(self, branch_batch_size, length):
        branch_batch_size = int(branch_batch_size)
        if branch_batch_size not in self.prefill_lengths:
            raise UnsupportedProfile("prefill batch is outside the warmed profile")
        return select_bucket(length, self.prefill_lengths[branch_batch_size], self.bucket_mode)


def load_profile_spec(name, source_dir=None):
    return ProfileSpec(name, profile_payload(name, source_dir=source_dir))


def inspect_prepared_inputs(inputs):
    """Derive text-encoder jobs and prefill length from official prepare_inputs output."""
    cond_text = [int(value) for value in _as_int_list(inputs.get("text_ids_len"))]
    cond_prefill = int(_dim1(inputs.get("input_ids")))
    cfg_scale = float(inputs.get("cfg_scale", 1.0) or 1.0)
    has_negative = inputs.get("cfg_negative_prompt_ids") is not None
    text_jobs = [_text_job(cond_text)]
    if cfg_scale != 1.0 and has_negative:
        uncond_text = [int(value) for value in _as_int_list(inputs.get("cfg_negative_text_ids_len"))]
        uncond_prefill = int(_dim1(inputs.get("cfg_negative_prompt_ids")))
        text_jobs.append(_text_job(uncond_text))
        return {
            "text_jobs": text_jobs,
            "prefill_len": max(cond_prefill, uncond_prefill),
            "branch_batch_size": 2,
            "cfg_scale": cfg_scale,
        }
    return {
        "text_jobs": text_jobs,
        "prefill_len": cond_prefill,
        "branch_batch_size": 1,
        "cfg_scale": 1.0 if cfg_scale == 1.0 else cfg_scale,
    }


def _text_job(segment_lengths):
    lengths = [int(value) for value in segment_lengths if int(value) > 0]
    if not lengths:
        raise UnsupportedProfile("prompt produced no text tokens")
    return {"batch_size": len(lengths), "token_length": max(lengths), "segments": lengths}


def guard_prepared_profile(spec, inspection):
    for job in inspection["text_jobs"]:
        spec.text_bucket(job["batch_size"], job["token_length"])
    spec.prefill_bucket(inspection["branch_batch_size"], inspection["prefill_len"])
    return {
        "text_buckets": [
            spec.text_bucket(job["batch_size"], job["token_length"])
            for job in inspection["text_jobs"]
        ],
        "prefill_bucket": spec.prefill_bucket(
            inspection["branch_batch_size"], inspection["prefill_len"]
        ),
        "prefill_len": int(inspection["prefill_len"]),
        "branch_batch_size": int(inspection["branch_batch_size"]),
    }


def guard_output_budget(max_frames, prefill_bucket, max_seq_len=MAX_SEQ_LEN):
    if int(max_frames) > int(max_seq_len) - 1 - int(prefill_bucket):
        raise UnsupportedProfile("padded prefill would shorten the permitted output")


def cap_max_new_tokens(requested, prefill_len, max_seq_len=MAX_SEQ_LEN):
    requested = int(requested)
    prefill_len = int(prefill_len)
    if requested < MIN_NEW_TOKENS:
        raise ProfileError("max_new_tokens must be at least 64")
    max_frames = min(requested, int(max_seq_len) - 1 - prefill_len)
    if max_frames < MIN_NEW_TOKENS:
        raise ProfileError("prompt leaves fewer than 64 audio frames of context")
    return max_frames


def patch_sparse_bucket(cache, declared):
    """Replace official 32-round _bucket with smallest-declared >= length."""
    declared = tuple(sorted({int(size) for size in declared}))

    def _bucket(_self, length):
        return smallest_declared_at_least(length, declared)

    cache._bucket = types.MethodType(_bucket, cache)
    cache._declared_buckets = declared
    return cache


def apply_sparse_buckets(runtime, spec):
    if spec.bucket_mode != "sparse":
        return
    text_cache = getattr(getattr(runtime, "model", None), "_fast_text_encoder_graph_cache", None)
    if text_cache is not None:
        union = sorted({size for lengths in spec.text_lengths.values() for size in lengths})
        patch_sparse_bucket(text_cache, union)
    graphs = getattr(runtime, "_backbone_prefill_graphs", {}) or {}
    for branch_batch_size, cache in graphs.items():
        declared = spec.prefill_lengths.get(int(branch_batch_size), ())
        if declared:
            patch_sparse_bucket(cache, declared)


def disable_joint_text_encoder_merge(runtime):
    """Keep text-encoder graph batch in {1, 2} for CFG by encoding branches separately.

    Official joint CFG merge concatenates cond/uncond and can request batch-4
    graphs for clone (2 text segments × 2 branches). Stock/expanded/compact only
    declare text batches 1 and 2. Returning None preserves official fallback to
    per-branch merge, which stays inside those warmed keys.
    """
    runtime._merge_cfg_branches = lambda _inputs: None


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def validate_checkpoint_hashes(checkpoint_dir, checkpoint_hashes):
    checkpoint_dir = Path(checkpoint_dir)
    if not isinstance(checkpoint_hashes, dict) or not checkpoint_hashes:
        raise ProfileError("checkpointHashes must map relative paths to sha256 digests")
    for relative, expected in sorted(checkpoint_hashes.items()):
        relative = str(relative)
        if not relative or relative.startswith("/") or ".." in Path(relative).parts:
            raise ProfileError("checkpoint hash path is not a relative file")
        path = checkpoint_dir / relative
        if not path.is_file():
            raise ProfileError("checkpoint file is missing")
        actual = sha256_file(path)
        if actual != str(expected).strip().lower():
            raise ProfileError("checkpoint hash mismatch")
    return True


def canonical_fingerprint_payload(
    *,
    source_revision,
    checkpoint_hashes,
    profile,
    version,
    torch_version=None,
    cuda_version=None,
    device_name=None,
    device_capability=None,
    transformers_version=None,
    qwen_tts_version=None,
    numpy_version=None,
    extra_versions=None,
):
    hashes = {
        str(path): str(digest).strip().lower()
        for path, digest in sorted((checkpoint_hashes or {}).items())
    }
    payload = {
        "sourceRevision": str(source_revision or ""),
        "checkpointHashes": hashes,
        "profile": str(profile),
        "version": str(version or ""),
    }
    if torch_version is not None:
        payload["torch"] = str(torch_version)
    if transformers_version is not None:
        payload["transformers"] = str(transformers_version)
    if qwen_tts_version is not None:
        payload["qwen_tts"] = str(qwen_tts_version)
    if numpy_version is not None:
        payload["numpy"] = str(numpy_version)
    if cuda_version is not None:
        payload["cuda"] = str(cuda_version)
    if device_name is not None:
        payload["deviceName"] = str(device_name)
    if device_capability is not None:
        payload["deviceCapability"] = list(device_capability)
    if extra_versions:
        payload["dependencies"] = {
            str(name): str(value) for name, value in sorted(extra_versions.items())
        }
    return payload


def fingerprint_hex(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def encode_f32le(samples):
    samples = [float(value) for value in samples]
    return struct.pack("<" + ("f" * len(samples)), *samples)


def decode_f32le(payload, samples):
    samples = int(samples)
    if samples < 0:
        raise ProfileError("samples must be >= 0")
    expected = samples * 4
    if len(payload) != expected:
        raise ProfileError("reference audio byte length does not match samples")
    if samples == 0:
        return []
    return list(struct.unpack("<" + ("f" * samples), payload))


def write_float32_wav(path, samples, sample_rate):
    """Write a mono IEEE-float WAV using only the standard library."""
    sample_rate = int(sample_rate)
    if sample_rate <= 0:
        raise ProfileError("sampleRate must be > 0")
    pcm = encode_f32le(samples)
    n = len(samples)
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + len(pcm),
        b"WAVE",
        b"fmt ",
        16,
        3,
        1,
        sample_rate,
        sample_rate * 4,
        4,
        32,
        b"data",
        len(pcm),
    )
    path = Path(path)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, header)
        os.write(fd, pcm)
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return n


def _as_int_list(value):
    if value is None:
        return []
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        value = tolist()
    if isinstance(value, (int, float)):
        return [int(value)]
    if isinstance(value, list):
        flat = []
        for item in value:
            if isinstance(item, list):
                flat.extend(int(part) for part in item)
            else:
                flat.append(int(item))
        return flat
    try:
        return [int(item) for item in value]
    except TypeError:
        return [int(value)]


def _dim1(value):
    shape = getattr(value, "shape", None)
    if shape is not None:
        return int(shape[1] if len(shape) > 1 else shape[0])
    if isinstance(value, list):
        if value and isinstance(value[0], list):
            return len(value[0])
        return len(value)
    return int(value)

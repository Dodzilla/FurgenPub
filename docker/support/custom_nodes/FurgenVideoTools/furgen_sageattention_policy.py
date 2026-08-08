"""Numerically stable SageAttention2 dispatch policy for Furgen video workers."""

from __future__ import annotations

import functools
import json
import os
from pathlib import Path
from typing import Any


SM120_SAFE_POLICY = "sm120_qk_int8_pv_fp16_per_thread_fp32"
POLICY_LOG_PREFIX = "FURGEN_SAGEATTENTION2_POLICY"


def _normalize_capability(capability: Any) -> tuple[int, int] | None:
    try:
        major, minor = capability
        return int(major), int(minor)
    except (TypeError, ValueError):
        return None


def policy_for_capability(capability: Any) -> str | None:
    """Return the managed SageAttention2 policy for a CUDA capability."""

    normalized = _normalize_capability(capability)
    if normalized is not None and normalized[0] == 12:
        return SM120_SAFE_POLICY
    return None


def _status(*, active: bool, policy: str | None, capability: Any, reason: str) -> dict[str, Any]:
    normalized = _normalize_capability(capability)
    return {
        "active": bool(active),
        "policy": policy,
        "cudaCapability": (
            f"{normalized[0]}.{normalized[1]}" if normalized is not None else None
        ),
        "reason": reason,
    }


def _record_runtime_status(status: dict[str, Any]) -> None:
    configured_path = os.environ.get("VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH", "").strip()
    verify_path = Path(configured_path or "/workspace/sageattention2_runtime.json")
    if not configured_path and not verify_path.exists():
        return
    try:
        payload: dict[str, Any] = {}
        if verify_path.exists():
            loaded = json.loads(verify_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload.update(loaded)
        payload.update(
            {
                "comfyPolicyActive": status.get("active") is True,
                "comfyPolicy": status.get("policy"),
                "comfyPolicyCapability": status.get("cudaCapability"),
                "comfyPolicyReason": status.get("reason"),
            }
        )
        verify_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = verify_path.with_name(f".{verify_path.name}.tmp")
        temp_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temp_path, verify_path)
    except Exception as exc:
        print(f"{POLICY_LOG_PREFIX} RUNTIME_STATUS_ERROR {type(exc).__name__}: {exc}")


def install_sageattention_policy(
    *,
    torch_module: Any = None,
    comfy_attention_module: Any = None,
    safe_kernel: Any = None,
) -> dict[str, Any]:
    """Patch ComfyUI's SageAttention2 callable on SM 12.x GPUs.

    SageAttention 2.2 auto-dispatches SM120 to its FP8 value kernel. That path
    can produce non-finite LTX-Video 2.3 audio/video latents. The managed path
    retains SageAttention2's INT8 Q/K CUDA implementation, while using FP16 V
    and full FP32 accumulation. Per-thread Q/K quantization is intentional: the
    first per-warp FP16 call in a fresh SM120 process can return an all-zero
    result with the current wheel.
    """

    def finish(result: dict[str, Any]) -> dict[str, Any]:
        _record_runtime_status(result)
        return result

    try:
        if torch_module is None:
            import torch as torch_module

        if not torch_module.cuda.is_available():
            return finish(_status(
                active=False,
                policy=None,
                capability=None,
                reason="cuda_unavailable",
            ))

        capability = torch_module.cuda.get_device_capability()
        policy = policy_for_capability(capability)
        if policy is None:
            return finish(_status(
                active=False,
                policy=None,
                capability=capability,
                reason="upstream_dispatch_preserved",
            ))

        if comfy_attention_module is None:
            import comfy.ldm.modules.attention as comfy_attention_module

        if safe_kernel is None:
            from sageattention.core import sageattn_qk_int8_pv_fp16_cuda as safe_kernel

        upstream = getattr(comfy_attention_module, "sageattn", None)
        if upstream is None:
            return finish(_status(
                active=False,
                policy=policy,
                capability=capability,
                reason="comfy_sageattention_not_configured",
            ))
        if getattr(upstream, "_furgen_sageattention2_policy", None) == policy:
            return finish(_status(
                active=True,
                policy=policy,
                capability=capability,
                reason="already_active",
            ))

        @functools.wraps(upstream)
        def stable_sageattn(
            q,
            k,
            v,
            tensor_layout="HND",
            is_causal=False,
            sm_scale=None,
            return_lse=False,
            **kwargs,
        ):
            kernel_kwargs = dict(kwargs)
            kernel_kwargs.pop("qk_quant_gran", None)
            kernel_kwargs.pop("pv_accum_dtype", None)
            return safe_kernel(
                q,
                k,
                v,
                tensor_layout=tensor_layout,
                is_causal=is_causal,
                qk_quant_gran="per_thread",
                sm_scale=sm_scale,
                pv_accum_dtype="fp32",
                return_lse=return_lse,
                **kernel_kwargs,
            )

        stable_sageattn._furgen_sageattention2_policy = policy
        stable_sageattn._furgen_sageattention2_upstream = upstream
        comfy_attention_module.sageattn = stable_sageattn
        result = _status(
            active=True,
            policy=policy,
            capability=capability,
            reason="installed",
        )
        print(
            f"{POLICY_LOG_PREFIX} active={policy} "
            f"cuda={result['cudaCapability']} qk=per_thread pv=fp16 accum=fp32"
        )
        return finish(result)
    except Exception as exc:
        print(f"{POLICY_LOG_PREFIX} ERROR {type(exc).__name__}: {exc}")
        return finish(_status(
            active=False,
            policy=None,
            capability=None,
            reason=f"install_error:{type(exc).__name__}",
        ))


SAGEATTENTION_POLICY_STATUS = install_sageattention_policy()


__all__ = [
    "SM120_SAFE_POLICY",
    "SAGEATTENTION_POLICY_STATUS",
    "install_sageattention_policy",
    "policy_for_capability",
]

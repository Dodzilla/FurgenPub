"""Numerically stable SageAttention2 dispatch policy for Furgen video workers."""

from __future__ import annotations

import functools
import json
import os
from pathlib import Path
from typing import Any


SM120_SAFE_POLICY = "sm120_qk_int8_pv_fp16_triton"
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

    SageAttention 2.2 auto-dispatches SM120 to its FP8 value kernel. The CUDA
    FP16-value path can also drive LTX-Video 2.3 audio/video latents non-finite,
    even with per-thread Q/K quantization and FP32 accumulation. Use the
    SageAttention2 Triton FP16-value kernel instead; its per-block Q/K
    quantization and FP32-buffered accumulation remain finite for this model.
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
            from sageattention.core import sageattn_qk_int8_pv_fp16_triton as safe_kernel

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
            kernel_kwargs.pop("quantization_backend", None)
            return safe_kernel(
                q,
                k,
                v,
                tensor_layout=tensor_layout,
                quantization_backend="triton",
                is_causal=is_causal,
                sm_scale=sm_scale,
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
            f"cuda={result['cudaCapability']} qk=per_block pv=fp16 "
            "accum=fp32_buffered backend=triton"
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


def bounded_eager_int8_linear(upstream, *, chunk_bytes=64 * 1024 * 1024):
    """Bound eager INT8 temporaries while preserving its per-row arithmetic."""
    import torch

    reported = False

    @functools.wraps(upstream)
    def linear(x, weight, weight_scale, bias=None, out_dtype=torch.bfloat16,
               convrot=False, convrot_groupsize=256, input_act=None):
        nonlocal reported
        # The original path validates unsupported shapes and preserves autograd.
        rows = x.numel() // x.shape[-1] if x.ndim and x.shape[-1] else 0
        columns = weight.shape[0] if weight.ndim == 2 else 0
        element_bytes = torch.empty((), dtype=out_dtype).element_size()
        if (not x.is_contiguous() or rows % 32 or not rows or not columns
                or x.shape[-1] != weight.shape[-1]
                or any(t.requires_grad for t in (x, weight, weight_scale, bias)
                       if isinstance(t, torch.Tensor))
                or rows * columns * element_bytes <= chunk_bytes):
            return upstream(x, weight, weight_scale, bias, out_dtype,
                            convrot, convrot_groupsize, input_act)
        # Account for INT32 accumulation, scaling and input/rotation scratch.
        row_bytes = columns * (12 + 2 * element_bytes) + x.shape[-1] * (12 + 2 * x.element_size())
        chunk_rows = max(32, (chunk_bytes // row_bytes // 32) * 32)
        if not reported:
            print(f"FURGEN_EAGER_INT8_MEMORY_POLICY bounded rows={rows} columns={columns} "
                  f"dtype={out_dtype} chunk_rows={chunk_rows}")
            reported = True
        flat = x.view(rows, x.shape[-1])
        output = torch.empty((rows, columns), dtype=out_dtype, device=x.device)
        for start in range(0, rows, chunk_rows):
            part = upstream(flat[start:start + chunk_rows], weight, weight_scale,
                            bias, out_dtype, convrot, convrot_groupsize, input_act)
            output[start:start + part.shape[0]].copy_(part)
            del part
        return output.view(*x.shape[:-1], columns)

    linear._furgen_bounded_int8 = True
    return linear


def install_eager_int8_memory_policy():
    """Override only the known eager implementation; preserve backend routing."""
    import inspect
    try:
        import comfy_kitchen.backends.eager as eager
        from comfy_kitchen.backends.eager import quantization
        from comfy_kitchen.registry import registry
        upstream = eager.int8_linear
        if getattr(upstream, "_furgen_bounded_int8", False):
            return {"active": True, "reason": "already_installed"}
        expected = ("x", "weight", "weight_scale", "bias", "out_dtype",
                    "convrot", "convrot_groupsize", "input_act")
        if tuple(inspect.signature(upstream).parameters) != expected:
            return {"active": False, "reason": "unsupported_signature"}
        if registry.get_implementation("int8_linear", backend="eager") is not upstream:
            return {"active": False, "reason": "unexpected_registry_binding"}
        wrapped = bounded_eager_int8_linear(upstream)
        eager.int8_linear = wrapped
        # Keep direct module callers consistent with registry dispatch.
        if quantization.int8_linear is upstream:
            quantization.int8_linear = wrapped
        if registry.get_implementation("int8_linear", backend="eager") is not wrapped:
            eager.int8_linear = upstream
            if quantization.int8_linear is wrapped:
                quantization.int8_linear = upstream
            return {"active": False, "reason": "registry_binding_failed"}
        print("FURGEN_EAGER_INT8_MEMORY_POLICY active=bounded_rows chunk_bytes=67108864")
        return {"active": True, "reason": "installed"}
    except (ImportError, AttributeError) as exc:
        return {"active": False, "reason": type(exc).__name__}


EAGER_INT8_MEMORY_POLICY_STATUS = install_eager_int8_memory_policy()


SAGEATTENTION_POLICY_STATUS = install_sageattention_policy()


__all__ = [
    "SM120_SAFE_POLICY",
    "SAGEATTENTION_POLICY_STATUS",
    "install_sageattention_policy",
    "policy_for_capability",
]

import ast
import importlib.util
import json
import math
import subprocess
import sys
import types
from pathlib import Path

import pytest
import torch


FORBIDDEN_GENERATION_SHIMS = {
    "LatentMotionSharpener",
    "LatentTemporalInpainter",
    "LTXAddVideoICLoRAGuide",
    "LTXVImgToVideoConditionOnly",
    "RIFEInterpolation",
}


def test_video_compat_nodes_do_not_shadow_generation_nodes():
    compat_path = Path(__file__).parents[1] / "custom_nodes" / "furgen_video_compat_nodes.py"
    tree = ast.parse(compat_path.read_text(encoding="utf-8"), filename=str(compat_path))

    mapped = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "NODE_CLASS_MAPPINGS" for target in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            continue
        for key in node.value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                mapped.add(key.value)

    assert not (mapped & FORBIDDEN_GENERATION_SHIMS)


def _load_furgen_video_tools():
    support_dir = Path(__file__).parents[1]
    package_dir = support_dir / "custom_nodes" / "FurgenVideoTools"
    folder_paths = types.ModuleType("folder_paths")
    folder_paths.get_annotated_filepath = lambda value: value
    folder_paths.get_output_directory = lambda: "/tmp"
    folder_paths.get_temp_directory = lambda: "/tmp"
    folder_paths.get_save_image_path = lambda prefix, output_dir: (output_dir, prefix, 0, "", prefix)
    sys.modules["folder_paths"] = folder_paths
    spec = importlib.util.spec_from_file_location("furgen_video_tools_test", package_dir / "furgen_video_tools.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _load_sageattention_policy():
    support_dir = Path(__file__).parents[1]
    policy_path = support_dir / "custom_nodes" / "FurgenVideoTools" / "furgen_sageattention_policy.py"
    spec = importlib.util.spec_from_file_location("furgen_sageattention_policy_test", policy_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_sageattention_policy_only_overrides_sm12x():
    module = _load_sageattention_policy()

    assert module.policy_for_capability((12, 0)) == module.SM120_SAFE_POLICY
    assert module.policy_for_capability((12, 1)) == module.SM120_SAFE_POLICY
    assert module.policy_for_capability((8, 9)) is None
    assert module.policy_for_capability(None) is None


def test_sageattention_policy_uses_triton_fp16_value_kernel():
    module = _load_sageattention_policy()
    calls = []

    class FakeCuda:
        @staticmethod
        def is_available():
            return True

        @staticmethod
        def get_device_capability():
            return 12, 0

    class FakeTorch:
        cuda = FakeCuda()

    def upstream(*args, **kwargs):
        raise AssertionError("upstream FP8 dispatcher should not be called on SM120")

    def safe_kernel(*args, **kwargs):
        calls.append((args, kwargs))
        return "safe-output"

    comfy_attention = types.SimpleNamespace(sageattn=upstream)
    status = module.install_sageattention_policy(
        torch_module=FakeTorch(),
        comfy_attention_module=comfy_attention,
        safe_kernel=safe_kernel,
    )

    output = comfy_attention.sageattn(
        "q",
        "k",
        "v",
        tensor_layout="NHD",
        is_causal=True,
        sm_scale=0.25,
        return_lse=True,
        qk_quant_gran="per_warp",
        pv_accum_dtype="fp16",
        attn_mask=None,
    )

    assert status == {
        "active": True,
        "policy": module.SM120_SAFE_POLICY,
        "cudaCapability": "12.0",
        "reason": "installed",
    }
    assert output == "safe-output"
    assert len(calls) == 1
    assert calls[0][1] == {
        "tensor_layout": "NHD",
        "quantization_backend": "triton",
        "is_causal": True,
        "sm_scale": 0.25,
        "return_lse": True,
        "attn_mask": None,
    }


def test_sageattention_policy_preserves_upstream_dispatch_on_pre_sm12_gpu():
    module = _load_sageattention_policy()

    class FakeCuda:
        @staticmethod
        def is_available():
            return True

        @staticmethod
        def get_device_capability():
            return 8, 9

    class FakeTorch:
        cuda = FakeCuda()

    upstream = object()
    comfy_attention = types.SimpleNamespace(sageattn=upstream)
    status = module.install_sageattention_policy(
        torch_module=FakeTorch(),
        comfy_attention_module=comfy_attention,
        safe_kernel=lambda *args, **kwargs: None,
    )

    assert status["active"] is False
    assert status["reason"] == "upstream_dispatch_preserved"
    assert comfy_attention.sageattn is upstream


def test_furgen_video_tools_registers_tail_context_utility_nodes():
    module = _load_furgen_video_tools()

    assert "FurgenGetImageRangeFromBatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenPrependImageToBatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenSeamScaleStabilize" in module.NODE_CLASS_MAPPINGS
    assert "FurgenTrimAudioDuration" in module.NODE_CLASS_MAPPINGS
    assert "FurgenBoundaryGradeMatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLatentGuideTemporalMask" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLTXVAddLatentGuideTemporal" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLTXGuideAttentionAdjust" in module.NODE_CLASS_MAPPINGS
    assert "FurgenAssertFiniteImages" in module.NODE_CLASS_MAPPINGS
    assert "FurgenAssertFiniteLatent" in module.NODE_CLASS_MAPPINGS
    assert "FCSConcatVideosV4" in module.NODE_CLASS_MAPPINGS
    assert "FCSAnalyzeVideo" in module.NODE_CLASS_MAPPINGS


def _make_test_video(path, size="96x64", duration=1.2, frequency=440):
    subprocess.run([
        "ffmpeg", "-y", "-v", "error",
        "-f", "lavfi", "-i", f"color=c=red:s={size}:r=24:d={duration}",
        "-f", "lavfi", "-i", f"sine=frequency={frequency}:sample_rate=48000:duration={duration}",
        "-shortest", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(path),
    ], check=True)


def test_video_analysis_preserves_proxy_audio_density_and_stable_names(tmp_path, monkeypatch):
    module = _load_furgen_video_tools()
    monkeypatch.setattr(module.folder_paths, "get_output_directory", lambda: str(tmp_path))
    monkeypatch.setattr(module.folder_paths, "get_save_image_path", lambda prefix, output: (output, prefix, 0, "", prefix))
    source = tmp_path / "source.mp4"
    _make_test_video(source, duration=1.2)
    node = module.FCSAnalyzeVideo()
    first = node.analyze_video(str(source), "content_abc123", "analysis_test", True)
    second = node.analyze_video(str(source), "content_abc123", "analysis_test", True)
    assert first["result"] == second["result"]
    assert [row["filename"] for row in first["ui"]["files"]] == [
        "analysis_test_00001-proxy.mp4",
        "analysis_test_00001-storyboard.webp",
        "analysis_test_00001-analysis.json",
    ]
    proxy = tmp_path / "analysis_test_00001-proxy.mp4"
    manifest_path = tmp_path / "analysis_test_00001-analysis.json"
    streams = json.loads(subprocess.run([
        "ffprobe", "-v", "error", "-show_streams", "-of", "json", str(proxy),
    ], capture_output=True, text=True, check=True).stdout)["streams"]
    assert {stream["codec_type"] for stream in streams} == {"video", "audio"}
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["storyboard"]["cues"]) == 3
    assert manifest["waveform"]["peaks"]
    assert manifest["cors"] == {"allowOrigin": "*"}


def test_video_analysis_storyboard_density_is_half_second_capped_at_240():
    module = _load_furgen_video_tools()
    assert module._storyboard_geometry(60, 1920, 1080)[:3] == (120, 12, 10)
    assert module._storyboard_geometry(600, 1080, 1920)[:3] == (240, 12, 20)


def test_precision_video_ducking_depth_is_a_bounded_db_attenuation():
    module = _load_furgen_video_tools()
    assert module._ducking_compressor_options(0) == (0.0, 0.0)
    depth, wet_mix = module._ducking_compressor_options(12)
    assert depth == 12.0
    assert math.isclose(1.0 - wet_mix, 10 ** (-12 / 20), rel_tol=1e-9)
    depth, wet_mix = module._ducking_compressor_options(100)
    assert depth == 24.0
    assert math.isclose(1.0 - wet_mix, 10 ** (-24 / 20), rel_tol=1e-9)


def test_precision_video_render_handles_trimmed_music_loop_ducking_and_mixed_orientation(tmp_path, monkeypatch):
    module = _load_furgen_video_tools()
    monkeypatch.setattr(module.folder_paths, "get_output_directory", lambda: str(tmp_path))
    monkeypatch.setattr(module.folder_paths, "get_save_image_path", lambda prefix, output: (output, prefix, 0, "", prefix))
    clip_a, clip_b, music = tmp_path / "a.mp4", tmp_path / "b.mp4", tmp_path / "music.mp4"
    _make_test_video(clip_a, "96x64", 1.2, 440)
    _make_test_video(clip_b, "64x96", 1.2, 660)
    _make_test_video(music, "64x64", 0.3, 220)
    manifest = {
        "clips": [
            {
                "sourceVideoUrl": str(clip_a), "trimEndSeconds": 1.2,
                "framing": {"mode": "fit", "fitBackground": "blur"},
                "audio": {"gain": 0.8, "fadeInSeconds": 0.1},
                "transitionAfter": {"type": "crossfade", "durationSeconds": 0.2},
            },
            {
                "sourceVideoUrl": str(clip_b), "trimEndSeconds": 1.2,
                "framing": {"mode": "custom", "panX": 0.25, "panY": 0.75, "zoom": 1.5},
                "audio": {"fadeOutSeconds": 0.1},
            },
        ],
        "soundtrack": {
            "sourceAudioUrl": str(music), "trimStartSeconds": 0, "trimEndSeconds": 0.3,
            "loop": True, "gain": 0.4, "ducking": {"enabled": True, "depthDb": 12},
        },
    }
    result = module.FCSConcatVideosV4().concat_videos_v4(
        json.dumps(manifest), 96, 64, 30, "equalPower", "precision_test", "yuv420p", 23, True,
    )
    assert result["result"][0][0] is True
    output = tmp_path / "precision_test_00001-audio.mp4"
    probe = json.loads(subprocess.run([
        "ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(output),
    ], capture_output=True, text=True, check=True).stdout)
    assert {stream["codec_type"] for stream in probe["streams"]} == {"video", "audio"}
    video = next(stream for stream in probe["streams"] if stream["codec_type"] == "video")
    assert (video["width"], video["height"]) == (96, 64)
    assert 2.1 <= float(probe["format"]["duration"]) <= 2.3


def test_furgen_tail_context_utility_nodes_slice_images_and_audio():
    module = _load_furgen_video_tools()

    images = torch.arange(12, dtype=torch.float32).view(12, 1, 1, 1)
    sliced, _mask = module.FurgenGetImageRangeFromBatch().slice(images, -1, 8)
    assert sliced.flatten().tolist() == list(range(4, 12))

    prepended, = module.FurgenPrependImageToBatch().prepend(
        torch.full((1, 1, 1, 1), 0.25),
        torch.arange(4, dtype=torch.float32).view(4, 1, 1, 1),
    )
    assert prepended.shape[0] == 5
    assert prepended.flatten().tolist() == [0.25, 0.0, 1.0, 2.0, 3.0]

    audio = {"waveform": torch.arange(24, dtype=torch.float32).view(1, 1, 24), "sample_rate": 24}
    trimmed, = module.FurgenTrimAudioDuration().trim(audio, 8 / 24, 5 / 24)
    assert trimmed["sample_rate"] == 24
    assert trimmed["waveform"].flatten().tolist() == [8, 9, 10, 11, 12]


def test_boundary_grade_uses_one_luma_gain_for_the_entire_clip():
    module = _load_furgen_video_tools()
    images = torch.stack((
        torch.full((2, 2, 3), 0.40),
        torch.full((2, 2, 3), 0.20),
    ))
    reference = torch.full((1, 2, 2, 3), 0.41)

    corrected, = module.FurgenBoundaryGradeMatch().match(
        images, reference, "luma_gain", 1.0, 0.95, 1.05, 0.0,
    )

    assert torch.allclose(corrected[0], reference[0], atol=1e-6)
    assert torch.allclose(corrected[1], torch.full((2, 2, 3), 0.205), atol=1e-6)
    assert torch.allclose(corrected[1] / corrected[0], images[1] / images[0], atol=1e-6)


def test_boundary_grade_rgb_mode_is_bounded_and_preserves_extra_channels():
    module = _load_furgen_video_tools()
    images = torch.tensor([[[[0.20, 0.40, 0.50, 0.70]]]], dtype=torch.float32)
    reference = torch.tensor([[[[0.30, 0.36, 0.60]]]], dtype=torch.float32)

    corrected, = module.FurgenBoundaryGradeMatch().match(
        images, reference, "rgb_gain", 1.0, 0.90, 1.10, 0.0,
    )

    assert torch.allclose(
        corrected[0, 0, 0],
        torch.tensor([0.22, 0.36, 0.55, 0.70]),
        atol=1e-6,
    )


def test_boundary_grade_black_boundary_is_finite_and_neutral():
    module = _load_furgen_video_tools()
    images = torch.zeros((2, 2, 2, 3), dtype=torch.float32)
    reference = torch.ones((1, 2, 2, 3), dtype=torch.float32)

    corrected, = module.FurgenBoundaryGradeMatch().match(
        images, reference, "luma_gain", 1.0, 0.95, 1.05, 0.0,
    )

    assert torch.isfinite(corrected).all()
    assert torch.equal(corrected, images)


def test_furgen_latent_guide_temporal_mask_adds_front_loaded_noise_mask():
    module = _load_furgen_video_tools()

    samples = torch.ones((2, 128, 5, 3, 4), dtype=torch.float32)
    latent = {"samples": samples}
    masked, = module.FurgenLatentGuideTemporalMask().apply(
        latent,
        "linear_fade",
        1,
        3,
        1.0,
        0.0,
    )

    assert masked is not latent
    assert masked["samples"] is samples
    assert masked["noise_mask"].shape == (2, 1, 5, 3, 4)
    # LTX guide masks use 1-strength. Frame 0 is fully guided, then it fades off.
    assert torch.allclose(masked["noise_mask"][0, 0, :, 0, 0], torch.tensor([0.0, 1 / 3, 2 / 3, 1.0, 1.0]))


def test_furgen_seam_scale_stabilize_reduces_early_zoom_drift():
    cv2 = pytest.importorskip("cv2")
    import numpy as np

    module = _load_furgen_video_tools()

    rng = np.random.default_rng(1234)
    reference = np.zeros((96, 96, 3), dtype=np.uint8)
    for _ in range(80):
        x = int(rng.integers(6, 90))
        y = int(rng.integers(6, 90))
        color = tuple(int(v) for v in rng.integers(60, 255, size=3))
        cv2.circle(reference, (x, y), int(rng.integers(2, 5)), color, -1)
    for x in range(8, 96, 16):
        cv2.line(reference, (x, 0), (95 - x // 2, 95), (180, 180, 180), 1)

    zoom = np.array([[1.08, 0.0, -4.2], [0.0, 1.08, -3.5]], dtype=np.float32)
    drifted = cv2.warpAffine(reference, zoom, (96, 96), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_REPLICATE)
    reference_t = torch.from_numpy(reference.astype(np.float32) / 255.0).unsqueeze(0)
    drifted_t = torch.from_numpy(drifted.astype(np.float32) / 255.0).unsqueeze(0)
    images = torch.cat((reference_t, drifted_t, drifted_t), dim=0)

    stabilized, = module.FurgenSeamScaleStabilize().stabilize(
        reference_t,
        images,
        1,
        0,
        1.0,
        0.2,
        8,
    )

    before = torch.mean((images[1] - reference_t[0]) ** 2).item()
    after = torch.mean((stabilized[1] - reference_t[0]) ** 2).item()
    assert after < before * 0.7
    assert torch.allclose(stabilized[0], images[0])
    assert torch.allclose(stabilized[2], images[2])


def test_furgen_ltxv_add_latent_guide_temporal_schedule_collapses_for_single_latent_frame():
    module = _load_furgen_video_tools()

    samples = torch.ones((1, 128, 1, 2, 2), dtype=torch.float32)
    hard = module._temporal_noise_mask(samples, None, "hard_cut", 1, 0, 1.0, 0.0)
    fade = module._temporal_noise_mask(samples, None, "linear_fade", 1, 6, 1.0, 0.0)

    assert torch.allclose(hard, fade)
    assert torch.allclose(hard[:, :, :, 0, 0].flatten(), torch.tensor([0.0]))


def test_furgen_ltxv_add_latent_guide_temporal_schedule_differs_for_multi_latent_frames():
    module = _load_furgen_video_tools()

    samples = torch.ones((1, 128, 4, 2, 2), dtype=torch.float32)
    hard = module._temporal_noise_mask(samples, None, "hard_cut", 1, 0, 1.0, 0.0)
    fade = module._temporal_noise_mask(samples, None, "linear_fade", 1, 3, 1.0, 0.0)

    assert torch.allclose(hard[:, 0, :, 0, 0], torch.tensor([[0.0, 1.0, 1.0, 1.0]]))
    assert torch.allclose(fade[:, 0, :, 0, 0], torch.tensor([[0.0, 1 / 3, 2 / 3, 1.0]]))
    assert not torch.allclose(hard, fade)


def test_furgen_ltx_guide_attention_adjust_sets_or_drops_entries():
    module = _load_furgen_video_tools()

    tensor = torch.ones((1, 4))
    conditioning = [[tensor, {"guide_attention_entries": [{"strength": 1.0}, {"strength": 0.5}], "keep": True}]]
    positive, negative = module.FurgenLTXGuideAttentionAdjust().adjust(
        conditioning,
        conditioning,
        "set_last",
        0.25,
        1,
    )

    assert positive[0][0] is tensor
    assert positive[0][1]["keep"] is True
    assert positive[0][1]["guide_attention_entries"] == [{"strength": 1.0}, {"strength": 0.25}]
    assert negative[0][1]["guide_attention_entries"] == [{"strength": 1.0}, {"strength": 0.25}]
    assert conditioning[0][1]["guide_attention_entries"] == [{"strength": 1.0}, {"strength": 0.5}]

    dropped, _ = module.FurgenLTXGuideAttentionAdjust().adjust(
        conditioning,
        conditioning,
        "drop_last",
        0.0,
        1,
    )
    assert dropped[0][1]["guide_attention_entries"] == [{"strength": 1.0}]


def test_furgen_assert_finite_images_fails_on_nan():
    module = _load_furgen_video_tools()

    ok = torch.zeros((2, 4, 4, 3), dtype=torch.float32)
    returned, = module.FurgenAssertFiniteImages().check(ok, "ok")
    assert returned is ok

    bad = ok.clone()
    bad[1, 0, 0, 0] = float("nan")
    try:
        module.FurgenAssertFiniteImages().check(bad, "after_decode")
    except ValueError as exc:
        assert "after_decode" in str(exc)
        assert "non-finite IMAGE tensor" in str(exc)
    else:
        raise AssertionError("expected finite image check to fail")


def test_furgen_assert_finite_latent_fails_on_inf_mask():
    module = _load_furgen_video_tools()

    latent = {
        "samples": torch.zeros((1, 128, 2, 3, 4), dtype=torch.float32),
        "noise_mask": torch.ones((1, 1, 2, 1, 1), dtype=torch.float32),
    }
    returned, = module.FurgenAssertFiniteLatent().check(latent, "ok", True)
    assert returned is latent

    bad = dict(latent)
    bad["noise_mask"] = latent["noise_mask"].clone()
    bad["noise_mask"][0, 0, 1, 0, 0] = float("inf")
    try:
        module.FurgenAssertFiniteLatent().check(bad, "guide_mask", True)
    except ValueError as exc:
        assert "guide_mask" in str(exc)
        assert "non-finite latent.noise_mask" in str(exc)
    else:
        raise AssertionError("expected finite latent check to fail")


def test_furgen_assert_finite_latent_checks_nested_tensor_leaves():
    module = _load_furgen_video_tools()

    class Nested:
        def __init__(self, tensors):
            self.tensors = tensors

    latent = {
        "samples": Nested([
            torch.zeros((1, 128, 2, 3, 4), dtype=torch.float32),
            torch.ones((1, 16, 8), dtype=torch.float32),
        ]),
        "noise_mask": Nested([torch.ones((1, 1, 2, 1, 1), dtype=torch.float32)]),
    }
    returned, = module.FurgenAssertFiniteLatent().check(latent, "av", True)
    assert returned is latent

    latent["samples"].tensors[1][0, 0, 0] = float("nan")
    try:
        module.FurgenAssertFiniteLatent().check(latent, "av", True)
    except ValueError as exc:
        assert "latent.samples[1]" in str(exc)
        assert "non-finite" in str(exc)
    else:
        raise AssertionError("expected nested finite latent check to fail")

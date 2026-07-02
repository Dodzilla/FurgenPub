import ast
import importlib.util
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


def test_furgen_video_tools_registers_tail_context_utility_nodes():
    module = _load_furgen_video_tools()

    assert "FurgenGetImageRangeFromBatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenPrependImageToBatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenSeamScaleStabilize" in module.NODE_CLASS_MAPPINGS
    assert "FurgenTrimAudioDuration" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLatentGuideTemporalMask" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLTXVAddLatentGuideTemporal" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLTXGuideAttentionAdjust" in module.NODE_CLASS_MAPPINGS
    assert "FurgenAssertFiniteImages" in module.NODE_CLASS_MAPPINGS
    assert "FurgenAssertFiniteLatent" in module.NODE_CLASS_MAPPINGS


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

import importlib.util
import sys
import types
from pathlib import Path

import numpy as np
import pytest
import torch


cv2 = pytest.importorskip("cv2")


def _load_module():
    support_dir = Path(__file__).parents[1]
    folder_paths = types.ModuleType("folder_paths")
    folder_paths.get_annotated_filepath = lambda value: value
    folder_paths.get_output_directory = lambda: "/tmp"
    folder_paths.get_temp_directory = lambda: "/tmp"
    folder_paths.get_save_image_path = lambda prefix, output_dir: (output_dir, prefix, 0, "", prefix)
    sys.modules["folder_paths"] = folder_paths
    source = support_dir / "custom_nodes" / "FurgenVideoTools" / "furgen_video_tools.py"
    spec = importlib.util.spec_from_file_location("furgen_video_tools_scene_color_test", source)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def module():
    return _load_module()


def _srgb_to_linear(rgb):
    rgb = np.clip(rgb, 0.0, 1.0)
    return np.where(rgb <= 0.04045, rgb / 12.92, ((rgb + 0.055) / 1.055) ** 2.4)


def _linear_to_srgb(rgb):
    rgb = np.maximum(rgb, 0.0)
    return np.where(rgb <= 0.0031308, rgb * 12.92, 1.055 * rgb ** (1.0 / 2.4) - 0.055)


def _linear_to_ycbcr(rgb):
    y = rgb[..., 0] * 0.2126 + rgb[..., 1] * 0.7152 + rgb[..., 2] * 0.0722
    return np.stack((y, (rgb[..., 2] - y) / 1.8556, (rgb[..., 0] - y) / 1.5748), axis=-1)


def _ycbcr_to_linear(ycbcr):
    y, cb, cr = ycbcr[..., 0], ycbcr[..., 1], ycbcr[..., 2]
    red = y + 1.5748 * cr
    blue = y + 1.8556 * cb
    green = (y - 0.2126 * red - 0.0722 * blue) / 0.7152
    return np.stack((red, green, blue), axis=-1)


def _texture(seed=17, height=128, width=160):
    rng = np.random.default_rng(seed)
    image = rng.random((height, width, 3), dtype=np.float32) * 0.45 + 0.18
    image = cv2.GaussianBlur(image, (0, 0), 0.8)
    for _ in range(70):
        x = int(rng.integers(5, width - 5))
        y = int(rng.integers(5, height - 5))
        radius = int(rng.integers(2, 6))
        color = tuple(float(value) for value in rng.random(3) * 0.55 + 0.20)
        cv2.circle(image, (x, y), radius, color, -1)
    for x in range(12, width, 24):
        cv2.line(image, (x, 0), (width - 1 - x // 3, height - 1), (0.72, 0.35, 0.24), 1)
    return np.clip(image, 0.0, 1.0).astype(np.float32)


def _apply_known_inverse_grade(reference, gain, cb, cr):
    """Create a source for which gain/cb/cr is the exact correcting grade."""
    target = _linear_to_ycbcr(_srgb_to_linear(reference))
    gained_source = target.copy()
    gained_source[..., 1] -= cb
    gained_source[..., 2] -= cr
    source_linear = _ycbcr_to_linear(gained_source) / gain
    return np.clip(_linear_to_srgb(source_linear), 0.0, 1.0).astype(np.float32)


def _run(node, images, reference, **overrides):
    options = {
        "reference_mode": "external_anchor",
        "wet_dry": 1.0,
        "gain_min": 0.70,
        "gain_max": 1.40,
        "correct_chroma": True,
        "max_chroma_offset": 0.08,
        "temporal_smoothing": 0.0,
        "cut_sensitivity": 0.5,
        "analysis_width": 160,
        "preserve_highlights": 0.0,
    }
    options.update(overrides)
    return node.stabilize(images, reference, **options)[0]


def test_node_is_registered_with_approved_interface(module):
    assert module.NODE_CLASS_MAPPINGS["FurgenSceneAwareColorStabilize"] is module.FurgenSceneAwareColorStabilize
    inputs = module.FurgenSceneAwareColorStabilize.INPUT_TYPES()["required"]
    assert tuple(inputs) == (
        "images", "reference", "reference_mode", "wet_dry", "gain_min", "gain_max",
        "correct_chroma", "max_chroma_offset", "temporal_smoothing", "cut_sensitivity",
        "analysis_width", "preserve_highlights",
    )
    assert inputs["reference_mode"][0] == ["external_anchor", "first_stable_frame"]
    assert inputs["wet_dry"][1]["default"] == 1.0
    assert inputs["gain_min"][1]["default"] == 0.90
    assert inputs["gain_max"][1]["default"] == 1.12
    assert inputs["analysis_width"][1]["default"] == 320


def test_estimator_recovers_linear_gain_and_residual_chroma(module):
    reference = _texture()
    expected_gain, expected_cb, expected_cr = 1.10, 0.012, -0.009
    source = _apply_known_inverse_grade(reference, expected_gain, expected_cb, expected_cr)
    node = module.FurgenSceneAwareColorStabilize()
    key = node._analyze(cv2, torch.from_numpy(reference), 160)
    current = node._analyze(cv2, torch.from_numpy(source), 160)

    estimate = node._estimate(cv2, key, current)

    assert estimate["count"] >= node.MIN_PATCHES
    assert estimate["coverage"] >= node.MIN_COVERAGE
    assert estimate["gain"] == pytest.approx(expected_gain, rel=0.02)
    assert estimate["cb"] == pytest.approx(expected_cb, abs=0.003)
    assert estimate["cr"] == pytest.approx(expected_cr, abs=0.003)


def test_drift_ramp_is_corrected_against_keyframe_without_pairwise_accumulation(module):
    reference = _texture(seed=23)
    frames = []
    for index in range(7):
        moved = cv2.warpAffine(
            reference,
            np.float32([[1.0, 0.0, index * 1.4], [0.0, 1.0, index * 0.7]]),
            (reference.shape[1], reference.shape[0]),
            borderMode=cv2.BORDER_REFLECT,
        )
        frames.append(_apply_known_inverse_grade(moved, 1.0 + index * 0.018, 0.0, 0.0))
    images = torch.from_numpy(np.stack(frames))
    output = _run(module.FurgenSceneAwareColorStabilize(), images, torch.from_numpy(reference)[None])

    input_linear = _srgb_to_linear(images.numpy())
    output_linear = _srgb_to_linear(output.numpy())
    input_luma = _linear_to_ycbcr(input_linear)[..., 0].mean(axis=(1, 2))
    output_luma = _linear_to_ycbcr(output_linear)[..., 0].mean(axis=(1, 2))

    assert abs(float(output_luma[-1] / output_luma[0]) - 1.0) < 0.025
    assert abs(float(output_luma[-1] / output_luma[0]) - 1.0) < abs(float(input_luma[-1] / input_luma[0]) - 1.0) * 0.35


def test_clean_homographic_pan_keeps_neutral_grade(module):
    reference = _texture(seed=31, height=144, width=192)
    node = module.FurgenSceneAwareColorStabilize()
    key = node._analyze(cv2, torch.from_numpy(reference), 192)
    for offset in (2, 5, 9):
        moved = cv2.warpAffine(
            reference,
            np.float32([[1.0, 0.0, offset], [0.0, 1.0, offset * 0.4]]),
            (192, 144),
            borderMode=cv2.BORDER_REFLECT,
        )
        estimate = node._estimate(cv2, key, node._analyze(cv2, torch.from_numpy(moved), 192))
        assert estimate["gain"] == pytest.approx(1.0, abs=0.005)
        assert estimate["cb"] == 0.0
        assert estimate["cr"] == 0.0
        output = _run(
            node, torch.from_numpy(moved)[None], torch.from_numpy(reference)[None],
            analysis_width=192, preserve_highlights=0.0,
        )
        assert torch.allclose(output[0], torch.from_numpy(moved), atol=2e-6)


def test_clean_mild_zoom_stays_inside_neutrality_gate(module):
    reference = _texture(seed=37, height=144, width=192)
    node = module.FurgenSceneAwareColorStabilize()
    key = node._analyze(cv2, torch.from_numpy(reference), 192)
    for scale, offset in ((1.02, 7.0), (1.04, 12.0)):
        transform = np.float32([
            [scale, 0.0, offset - (scale - 1.0) * 96.0],
            [0.0, scale, offset * 0.25 - (scale - 1.0) * 72.0],
        ])
        moved = cv2.warpAffine(
            reference, transform, (192, 144), borderMode=cv2.BORDER_REFLECT,
        )
        estimate = node._estimate(
            cv2, key, node._analyze(cv2, torch.from_numpy(moved), 192),
        )
        assert abs(estimate["gain"] - 1.0) <= 0.005
        assert estimate["cb"] == 0.0
        assert estimate["cr"] == 0.0
        output = _run(
            node, torch.from_numpy(moved)[None], torch.from_numpy(reference)[None],
            analysis_width=192, preserve_highlights=0.0,
        )
        assert torch.allclose(output[0], torch.from_numpy(moved), atol=2e-6)


def test_large_pan_keeps_shared_shot_grade_through_coast_and_rebase(module):
    reference = _texture(seed=41)
    targets = []
    sources = []
    for offset in (0, 20, 40, 60):
        target = cv2.warpAffine(
            reference,
            np.float32([[1.0, 0.0, offset], [0.0, 1.0, offset * 0.2]]),
            (reference.shape[1], reference.shape[0]),
            borderMode=cv2.BORDER_REFLECT,
        )
        targets.append(target)
        sources.append(_apply_known_inverse_grade(target, 1.08, 0.0, 0.0))
    images = torch.from_numpy(np.stack(sources))

    output = _run(
        module.FurgenSceneAwareColorStabilize(), images,
        torch.from_numpy(reference)[None], analysis_width=160,
        preserve_highlights=0.0,
    )

    target_batch = torch.from_numpy(np.stack(targets))
    assert torch.mean(torch.abs(output - target_batch)).item() < 0.003


def test_hard_cut_does_not_carry_the_previous_palette(module):
    first = _texture(seed=43)
    second = np.zeros_like(first)
    for x in range(0, second.shape[1], 12):
        second[:, x:x + 5] = (0.12, 0.70, 0.22)
    for y in range(6, second.shape[0], 24):
        cv2.rectangle(second, (0, y), (second.shape[1] - 1, y + 4), (0.05, 0.18, 0.85), -1)
    drifted = _apply_known_inverse_grade(first, 1.09, 0.01, -0.008)
    images = torch.from_numpy(np.stack((drifted, drifted, drifted, second, second)))

    output = _run(module.FurgenSceneAwareColorStabilize(), images, torch.from_numpy(first)[None])

    assert torch.allclose(output[3], images[3], atol=2e-5)
    assert torch.allclose(output[4], images[4], atol=2e-5)


def test_localized_orb_matches_cannot_veto_a_similarly_textured_hard_cut(module):
    rng = np.random.default_rng(200)
    shared_patch = rng.random((56, 56, 3), dtype=np.float32) * 0.60 + 0.20
    first = np.full((128, 160, 3), 0.12, dtype=np.float32)
    second = np.full_like(first, 0.12)
    for x in range(4, 160, 12):
        first[:, x:x + 4] = (0.70, 0.15, 0.12)
    for y in range(4, 128, 12):
        second[y:y + 4, :] = (0.12, 0.65, 0.18)
    first[:56, :56] = shared_patch
    second[:56, :56] = shared_patch

    node = module.FurgenSceneAwareColorStabilize()
    key = node._analyze(cv2, torch.from_numpy(first), 160)
    current = node._analyze(cv2, torch.from_numpy(second), 160)
    estimate = node._estimate(cv2, key, current)
    affine = node._coarse_affine_details(cv2, key, current)
    assert estimate["ratio"] < node.COAST_RATIO
    assert affine is not None and affine["inlier_count"] >= 8
    assert affine["coverage"] < 0.18
    assert not node._strong_affine_cut_veto(affine)
    assert node._is_cut(cv2, key, current, estimate["ratio"], 0.50)

    drifted = _apply_known_inverse_grade(first, 1.09, 0.01, -0.008)
    images = torch.from_numpy(np.stack((drifted, drifted, second, second, second, second)))
    output = _run(node, images, torch.from_numpy(first)[None], analysis_width=160)

    # A missed cut would COAST then promote while carrying the old 1.09 grade.
    assert torch.allclose(output[2:], images[2:], atol=2e-5)


def test_persistent_stable_geometry_lighting_jump_is_not_undone(module):
    reference = _texture(seed=47)
    brighter = np.clip(_linear_to_srgb(_srgb_to_linear(reference) * 1.22), 0.0, 1.0).astype(np.float32)
    images = torch.from_numpy(np.stack((reference, reference, brighter, brighter, brighter)))

    output = _run(module.FurgenSceneAwareColorStabilize(), images, torch.from_numpy(reference)[None])

    # The first jump coasts and the second rebases; neither is pulled back to the old exposure.
    assert torch.mean(torch.abs(output[2] - images[2])).item() < 2e-5
    assert torch.mean(torch.abs(output[3] - images[3])).item() < 2e-5


def test_feature_poor_track_loss_is_finite_and_neutral(module):
    reference = torch.full((1, 96, 128, 3), 0.35, dtype=torch.float32)
    images = torch.stack((reference[0], torch.full_like(reference[0], 0.55), torch.full_like(reference[0], 0.20)))

    output = _run(module.FurgenSceneAwareColorStabilize(), images, reference, analysis_width=128)

    assert torch.isfinite(output).all()
    assert torch.allclose(output, images)


def test_first_stable_frame_ignores_a_bad_first_frame(module):
    stable = _texture(seed=59)
    bad = np.flip(_texture(seed=61), axis=1).copy()
    node = module.FurgenSceneAwareColorStabilize()
    analyses = [node._analyze(cv2, torch.from_numpy(frame), 160) for frame in (bad, stable, stable, stable, stable)]

    selected = node._stable_index(cv2, analyses)

    assert selected != 0
    images = torch.from_numpy(np.stack((bad, stable, stable, stable, stable)))
    output = _run(node, images, torch.from_numpy(bad)[None], reference_mode="first_stable_frame")
    assert torch.mean(torch.abs(output[1:] - images[1:])).item() < 2e-5


def test_one_frame_flash_does_not_poison_following_grade(module):
    reference = _texture(seed=67)
    dim = _apply_known_inverse_grade(reference, 1.05, 0.0, 0.0)
    flash = np.clip(_linear_to_srgb(_srgb_to_linear(reference) * 1.8), 0.0, 1.0).astype(np.float32)
    images = torch.from_numpy(np.stack((dim, dim, flash, dim, dim)))

    output = _run(module.FurgenSceneAwareColorStabilize(), images, torch.from_numpy(reference)[None])

    before = torch.mean(torch.abs(output[1] - torch.from_numpy(reference))).item()
    after = torch.mean(torch.abs(output[4] - torch.from_numpy(reference))).item()
    assert after <= before + 0.005
    assert torch.mean(output[2]).item() > torch.mean(output[1]).item() * 1.15


def test_dissolve_and_occlusion_remain_finite_without_two_frame_oscillation(module):
    first = _texture(seed=71)
    second = _texture(seed=73)
    frames = []
    for amount in np.linspace(0.0, 1.0, 9):
        frame = first * (1.0 - amount) + second * amount
        if 0.25 < amount < 0.75:
            frame = frame.copy()
            cv2.rectangle(frame, (50, 20), (115, 105), (0.15, 0.15, 0.15), -1)
        frames.append(frame.astype(np.float32))
    images = torch.from_numpy(np.stack(frames))

    output = _run(module.FurgenSceneAwareColorStabilize(), images, torch.from_numpy(first)[None])

    assert torch.isfinite(output).all()
    deltas = torch.mean(torch.abs(output - images), dim=(1, 2, 3)).numpy()
    alternating = np.abs(np.diff(deltas, n=2))
    assert float(alternating.max(initial=0.0)) < 0.08


def test_temporal_grade_damps_two_frame_exposure_oscillation(module):
    reference = _texture(seed=74)
    frames = [
        _apply_known_inverse_grade(
            reference, 1.04 + (0.006 if index % 2 else -0.006),
            0.002 if index % 2 else -0.002, 0.0,
        )
        for index in range(24)
    ]
    images = torch.from_numpy(np.stack(frames))
    output = _run(
        module.FurgenSceneAwareColorStabilize(), images,
        torch.from_numpy(reference)[None], temporal_smoothing=0.50,
    )

    def frame_luma(batch):
        linear = _srgb_to_linear(batch.numpy())
        return _linear_to_ycbcr(linear)[..., 0].mean(axis=(1, 2))

    input_luma = frame_luma(images)
    output_luma = frame_luma(output)
    input_step = float(np.mean(np.abs(np.diff(input_luma))) / np.mean(input_luma))
    output_step = float(np.mean(np.abs(np.diff(output_luma))) / np.mean(output_luma))
    assert output_step < 0.01
    assert output_step < input_step * 0.75


def test_highlight_protection_does_not_increase_clipping(module):
    reference = np.clip(_texture(seed=75) * 1.22, 0.0, 0.985).astype(np.float32)
    source = _apply_known_inverse_grade(reference, 1.08, 0.0, 0.0)
    images = torch.from_numpy(np.stack((source, source, source)))

    output = _run(
        module.FurgenSceneAwareColorStabilize(), images,
        torch.from_numpy(reference)[None], preserve_highlights=0.75,
    )

    input_clipped = float((images[..., :3] >= 0.999).float().mean())
    output_clipped = float((output[..., :3] >= 0.999).float().mean())
    assert (output_clipped - input_clipped) * 100.0 <= 0.25


def test_global_grade_preserves_gain_normalized_high_frequency_structure(module):
    reference = _texture(seed=77)
    source = _apply_known_inverse_grade(reference, 1.08, 0.012, -0.009)
    images = torch.from_numpy(np.stack((source, source)))
    output = _run(
        module.FurgenSceneAwareColorStabilize(), images,
        torch.from_numpy(reference)[None], preserve_highlights=0.0,
    )

    def normalized_highpass(frame):
        linear = _srgb_to_linear(frame)
        luma = _linear_to_ycbcr(linear)[..., 0]
        luma = luma / max(float(luma.mean()), 1e-8)
        return cv2.Laplacian(luma.astype(np.float32), cv2.CV_32F)

    source_highpass = normalized_highpass(source)
    output_highpass = normalized_highpass(output[0].numpy())
    energy_ratio = float(np.mean(output_highpass ** 2) / np.mean(source_highpass ** 2))
    correlation = float(np.corrcoef(source_highpass.ravel(), output_highpass.ravel())[0, 1])
    assert 0.95 <= energy_ratio <= 1.05
    assert correlation >= 0.999


@pytest.mark.parametrize("dtype", [torch.float32, torch.float16])
def test_preserves_shape_dtype_device_alpha_and_input(module, dtype):
    rgb = torch.from_numpy(_texture(seed=79, height=96, width=128)).to(dtype=dtype)
    alpha = torch.linspace(0.0, 1.0, 96 * 128, dtype=dtype).reshape(96, 128, 1)
    reference = torch.cat((rgb, alpha), dim=-1)[None]
    images = torch.cat((reference, reference * torch.tensor([0.97, 0.97, 0.97, 1.0], dtype=dtype)), dim=0)
    original = images.clone()

    output = _run(module.FurgenSceneAwareColorStabilize(), images, reference, analysis_width=128)

    assert output.shape == images.shape
    assert output.dtype == images.dtype
    assert output.device == images.device
    assert torch.equal(output[..., 3:], images[..., 3:])
    assert torch.equal(images, original)


def test_full_resolution_processing_is_one_frame_at_a_time(module, monkeypatch):
    node = module.FurgenSceneAwareColorStabilize()
    original_apply = node._apply
    batch_sizes = []

    def recording_apply(frame, *args, **kwargs):
        batch_sizes.append(int(frame.shape[0]))
        return original_apply(frame, *args, **kwargs)

    monkeypatch.setattr(node, "_apply", recording_apply)
    reference = torch.from_numpy(_texture(seed=83, height=96, width=128))[None]
    images = reference.expand(12, -1, -1, -1).clone()

    output = _run(node, images, reference, analysis_width=128)

    assert output.shape == images.shape
    assert batch_sizes == [1] * 12


def test_zero_wet_dry_is_a_true_passthrough(module):
    images = torch.rand((3, 64, 64, 3), generator=torch.Generator().manual_seed(91))
    output = _run(module.FurgenSceneAwareColorStabilize(), images, images[:1], wet_dry=0.0)
    assert output is images

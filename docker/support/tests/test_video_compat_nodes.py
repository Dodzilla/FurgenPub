import ast
import importlib.util
import sys
import types
from pathlib import Path

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
    assert "FurgenTrimAudioDuration" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLatentGuideTemporalMask" in module.NODE_CLASS_MAPPINGS
    assert "FurgenLTXGuideAttentionAdjust" in module.NODE_CLASS_MAPPINGS


def test_furgen_tail_context_utility_nodes_slice_images_and_audio():
    module = _load_furgen_video_tools()

    images = torch.arange(12, dtype=torch.float32).view(12, 1, 1, 1)
    sliced, _mask = module.FurgenGetImageRangeFromBatch().slice(images, -1, 8)
    assert sliced.flatten().tolist() == list(range(4, 12))

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

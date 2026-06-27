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
    assert "FurgenPrependImageToBatch" in module.NODE_CLASS_MAPPINGS
    assert "FurgenTrimAudioDuration" in module.NODE_CLASS_MAPPINGS


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

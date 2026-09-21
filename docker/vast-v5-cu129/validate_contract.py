"""Static, fail-closed validation for the public v5 sealed runtime inputs."""
import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
V5 = ROOT / "docker" / "vast-v5-cu129"
PACKAGE = ROOT / "docker" / "support" / "custom_nodes" / "FurgenH3VideoTools"

for base in (V5, PACKAGE):
    for path in base.rglob("*"):
        if path.is_file() and path.suffix in {".py", ".json", ".md", ".txt", ""}:
            text = path.read_text(encoding="utf-8", errors="replace")
            assert ("l" + "tx") not in text.lower(), f"excluded video-family token in {path}"

bundles = json.loads((V5 / "bundles.template.json").read_text())
assert set(bundles) == {"video_gen_v5_h3_runtime_nodes", "video_gen_v5_nvidia_rtx_nodes"}
repos = [step["repository"] for bundle in bundles.values() for step in bundle["steps"]
         if step["type"] == "git_custom_node"]
assert len(repos) == len(set(repos)), "a third-party repository is installed more than once"
for bundle in bundles.values():
    for step in bundle["steps"]:
        if step["type"] == "git_custom_node":
            assert len(step["ref"]) == 40 and all(c in "0123456789abcdef" for c in step["ref"])

source = (PACKAGE / "furgen_h3_video_tools.py").read_text()
tree = ast.parse(source)
classes = {node.name for node in tree.body if isinstance(node, ast.ClassDef)}
required = set(bundles["video_gen_v5_h3_runtime_nodes"]["compatibility"]["steps"][0]["requiredClassTypes"])
assert required <= classes
assert {"EZHttpPostNode", "EZEmptyDictNode", "EZAssocStrNode", "ImpactExecutionOrderController"} <= classes
assert {"FCSConcatVideosV4GlobalGrid", "FCSExposeCompositionTiming"} <= classes
assert "rembg" not in source.lower() and "boto" not in source.lower()

dockerfile = (V5 / "Dockerfile").read_text()
assert dockerfile.count("torch==2.10.0+cu129") == 1
assert "import color_matcher, cv2, imageio_ffmpeg, requests" in dockerfile
assert 'version("nvidia-vfx") == "0.1.0.1"' in dockerfile
assert "modelsIncluded" in (V5 / "seal_manifest.py").read_text()
print("v5 sealed runtime contract validated")

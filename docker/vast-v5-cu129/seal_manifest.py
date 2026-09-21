"""Seal v5 code, bundle specs, and package versions after final runtime pins."""
import hashlib
import importlib.metadata
import json
import os
import subprocess
from pathlib import Path


root = Path("/opt/furgen/v5")
comfy = root / "ComfyUI"
bundles = json.loads((root / "bundles.json").read_text())
sealed = {}
for bundle_id, spec in bundles.items():
    directories = set()
    for step in spec["steps"] + spec.get("compatibility", {}).get("steps", []):
        if step["type"] == "git_custom_node":
            directories.add(step["directoryName"])
        elif step["type"] == "furgen_support_custom_node":
            directories.add(step["packageName"])
    files = {}
    for directory in directories:
        base = comfy / "custom_nodes" / directory
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix not in {".py", ".json", ".txt", ".toml", ".yaml", ".yml"}:
                continue
            if ".git" in path.parts or "__pycache__" in path.parts:
                continue
            files[str(path.relative_to(comfy))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not files:
        raise RuntimeError(f"Bundle {bundle_id} did not seal any code")
    sealed[bundle_id] = {"spec": spec, "files": files}

manifest = {
    "schema": 2,
    "serverType": "video_gen_v5",
    "comfyCommit": subprocess.check_output(["git", "-C", str(comfy), "rev-parse", "HEAD"], text=True).strip(),
    "torchVersion": importlib.metadata.version("torch"),
    "cuda": "12.9",
    "furgenPubCommit": os.environ["FURGENPUB_COMMIT"],
    "modelsIncluded": False,
    "nodeBundles": sealed,
    "packageVersions": {
        distribution.metadata["Name"].lower().replace("_", "-"): distribution.version
        for distribution in importlib.metadata.distributions()
    },
    "dependencyAgentSha256": hashlib.sha256((root / "dependency_agent_v1.py").read_bytes()).hexdigest(),
}
(root / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")

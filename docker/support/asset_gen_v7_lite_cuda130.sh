#!/bin/bash
# Fresh-worker CUDA 13.0 framework bootstrap for the Vast CUDA 12.9 base.
set -euo pipefail
"${FURGEN_COMFY_PYTHON:-/venv/main/bin/python}" - <<'PY'
import importlib.metadata as metadata
import os
from pathlib import Path
import socket
import subprocess
import sys

pins = {"torch": "2.10.0+cu130", "torchvision": "0.25.0+cu130", "torchaudio": "2.10.0+cu130"}
if any(metadata.version(name) != version for name, version in pins.items()):
    root = Path(os.environ.get("DM_COMFYUI_DIR", "/workspace/ComfyUI"))
    if (root / "input/provisioned_asset_gen_v7_lite.txt").exists():
        raise SystemExit("Refusing CUDA framework replacement on a provisioned worker")
    for port in (8080, 8188, 8189):
        with socket.socket() as probe:
            probe.settimeout(1)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise SystemExit("Refusing CUDA framework replacement on a listening worker")
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-cache-dir",
                    "--index-url", "https://download.pytorch.org/whl/cu130",
                    *[f"{name}=={version}" for name, version in pins.items()]], check=True)
if any(metadata.version(name) != version for name, version in pins.items()):
    raise SystemExit("CUDA 13.0 framework pins were not established")
import torch
if torch.version.cuda != "13.0":
    raise SystemExit("Expected the CUDA 13.0 PyTorch runtime")
print("v7 CUDA 13.0 framework verified: " + str(pins), flush=True)
PY

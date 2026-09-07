#!/usr/bin/env python3
"""Pinned CPU installation for v7 replacements; never starts GPU work."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

SOURCE = "https://github.com/breezeblue-ai/breeze-tts.git"
REVISION = "ca632ce6c4d05f7985da4eab29b1a5d445b43f7b"
PINS = ["transformers==4.57.3", "qwen-tts==0.1.1", "tokenizers==0.22.2", "huggingface-hub==0.36.2", "sox==1.5.0", "onnxruntime==1.29.0",
        "accelerate==1.12.0", "librosa==1.0.0", "soundfile==0.14.0", "einops==0.8.2"]


def run(argv):
    subprocess.run(argv, check=True)


def install(args):
    actual = os.environ.get("DM_INSTANCE_ID") or os.environ.get("VAST_CONTAINERLABEL", "").removeprefix("C.")
    if not args.instance.isdigit() or args.instance != actual or os.environ.get("SERVER_TYPE") != "asset_gen_v7_lite":
        raise ValueError("Installation requires the current numeric v7 worker identity")
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    source = root / "source" / REVISION
    source.parent.mkdir(exist_ok=True)
    if not source.exists():
        run(["git", "clone", "--filter=blob:none", "--no-checkout", SOURCE, str(source)])
        run(["git", "-C", str(source), "checkout", "--detach", REVISION])
    actual = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if actual != REVISION or subprocess.check_output(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=no"], text=True).strip():
        raise RuntimeError("Official source revision/cleanliness mismatch")
    # Read-only framework inheritance avoids a second multi-GB Torch install.
    # Runtime verifies exact versions before GPU initialization on every boot.
    venv = root / "venv-torch210-cu130-transformers4573"
    if not venv.exists():
        run([args.python, "-m", "venv", "--system-site-packages", str(venv)])
    python = str(venv / "bin/python")
    probe = subprocess.run([python, "-c", "import importlib.metadata as m,json; print(json.dumps({k:m.version(k) for k in " + repr([p.split("==")[0] for p in PINS]) + "}))"], capture_output=True, text=True)
    expected_pins = dict(item.split("==", 1) for item in PINS)
    if probe.returncode or json.loads(probe.stdout) != expected_pins:
        run([python, "-m", "pip", "install", "--no-deps", *PINS])
    versions = subprocess.check_output([python, "-m", "pip", "freeze", "--all"], text=True)
    (root / "environment.lock.txt").write_text(versions)
    hashes = json.loads(Path(args.checkpoint_manifest).read_text())
    checkpoint = Path(args.checkpoint).resolve()
    for name, expected in hashes.items():
        path = checkpoint / name
        if not path.resolve().is_relative_to(checkpoint):
            raise RuntimeError("Checkpoint escaped root: " + name)
        if not path.is_file() and getattr(args, "allow_missing_checkpoint", False):
            continue
        if not path.is_file():
            raise RuntimeError("Checkpoint file missing or escaped root: " + name)
        with path.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != expected:
            raise RuntimeError("Checkpoint hash mismatch: " + name)
    config = {"enabled": False, "canaryInstanceIds": [args.instance], "version": args.version,
              "sourceDir": str(source), "sourceRevision": REVISION,
              "checkpointDir": str(checkpoint), "checkpointHashes": hashes,
              "packagePins": dict(item.split("==", 1) for item in PINS),
              "stateDir": str(root), "cacheDir": str(root / "cache"), "python": python,
              "runtimeScript": str(Path(__file__).with_name("asset_gen_v7_lite_tts_runtime.py")),
              "coordinatorUrl": "http://127.0.0.1:8189", "profile": "stock",
              "diagnosticsEnabled": False, "routingApproved": False, "coexistenceApproved": False,
              "prioritizeAfterInference": False,
              "measuredPeaks": {}, "validUntilMs": 0,
              "environmentLockSha256": hashlib.sha256(versions.encode()).hexdigest()}
    destination = root / "config.installed.json"
    destination.write_text(json.dumps(config, indent=2) + "\n")
    os.chmod(destination, 0o600)
    print(json.dumps({"installed": True, "enabled": False, "config": str(destination), "sourceRevision": REVISION}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--root", default="/workspace/.fcs/tts")
    parser.add_argument("--python", default="/venv/main/bin/python")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--checkpoint-manifest", required=True)
    parser.add_argument("--allow-missing-checkpoint", action="store_true")
    install(parser.parse_args())

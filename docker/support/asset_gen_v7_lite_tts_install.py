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
        "soxr==0.5.0.post1", "joblib==1.5.2", "msgpack==1.1.1", "scikit-learn==1.7.2", "flatbuffers==25.9.23", "protobuf==6.32.1", "threadpoolctl==3.6.0", "accelerate==1.12.0", "librosa==1.0.0", "soundfile==0.14.0", "einops==0.8.2"]


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
    # A slim replacement image must not accidentally supply required imports.
    run([python, "-c", "import librosa, soxr, qwen_tts, transformers, onnxruntime"])
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
    if getattr(args, "production_policy", None):
        activate_production(config, Path(args.production_policy), root)
    print(json.dumps({"installed": True, "enabled": False, "config": str(destination), "sourceRevision": REVISION}))


def activate_production(config, policy_path, root):
    """Only activate a measured immutable runtime/hardware policy, never guess budgets."""
    policy = json.loads(policy_path.read_text())
    if (policy.get("sourceRevision") != config["sourceRevision"] or
            policy.get("packagePins") != config["packagePins"] or
            policy.get("checkpointHashes") != config["checkpointHashes"] or
            policy.get("version") != config["version"] or
            policy.get("profile") != "stock" or not policy.get("measuredRuntimeFingerprint")):
        raise RuntimeError("Production TTS policy does not match installed runtime")
    for name, expected in policy.get("supportHashes", {}).items():
        path = Path(__file__).with_name(name)
        if Path(name).name != name or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError("Production TTS support hash mismatch")
    if set(policy.get("supportHashes", {})) != {"asset_gen_v7_lite_tts_runtime.py", "tts_profiles.py"}:
        raise RuntimeError("Missing production runtime source hashes")
    probe = r"""
import importlib.metadata as m,json,sys,torch,numpy
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1]).parent))
from tts_profiles import canonical_fingerprint_payload,fingerprint_hex
c=json.load(open(sys.argv[1]));p=canonical_fingerprint_payload(source_revision=c['sourceRevision'],checkpoint_hashes=c['checkpointHashes'],profile=c['profile'],version=c['version'],torch_version=torch.__version__,cuda_version=torch.version.cuda,device_name=torch.cuda.get_device_name(0),device_capability=torch.cuda.get_device_capability(0),transformers_version=m.version('transformers'),qwen_tts_version=m.version('qwen-tts'),numpy_version=numpy.__version__,extra_versions={k:m.version(k) for k in c['packagePins']})
print(json.dumps({'fingerprint':fingerprint_hex(p),'deviceCount':torch.cuda.device_count(),'vram':torch.cuda.get_device_properties(0).total_memory}))
"""
    # Modules live with the pinned support, not in the policy state directory.
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent))
    measured = json.loads(subprocess.check_output([config["python"], "-c", probe, str(root / "config.installed.json")], env=env, text=True))
    if measured["deviceCount"] != 1 or measured["vram"] < 31 * 1024**3 or measured["fingerprint"] != policy["measuredRuntimeFingerprint"]:
        raise RuntimeError("Unvalidated TTS runtime/hardware; replacement cannot silently use fallback")
    active = {**config, **{k:v for k,v in policy.items() if k not in {"supportHashes", "canaryInstanceIds"}}}
    active.update(enabled=True, diagnosticsEnabled=False, routingApproved=True, validUntilMs=253402300799000,
                  canaryInstanceIds=config["canaryInstanceIds"])
    destination = root / "config.json"
    temporary = root / "config.activate.json"
    temporary.write_text(json.dumps(active, indent=2) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--root", default="/workspace/.fcs/tts")
    parser.add_argument("--python", default="/venv/main/bin/python")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--checkpoint-manifest", required=True)
    parser.add_argument("--allow-missing-checkpoint", action="store_true")
    parser.add_argument("--production-policy")
    install(parser.parse_args())

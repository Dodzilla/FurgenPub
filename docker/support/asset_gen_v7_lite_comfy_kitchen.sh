#!/bin/bash
# Pinned native Comfy Kitchen runtime for v7; never resolves framework packages.
set -euo pipefail
COMFY_DIR="${DM_COMFYUI_DIR:-${WORKSPACE:-/workspace}/ComfyUI}"
PYTHON="${FURGEN_COMFY_PYTHON:-/venv/main/bin/python}"
PIN="e01fb4c56b7a88149d469b99cbbfe3223d715054"
if [[ -n "${COMFYUI_PIN_COMMIT:-}" && "${COMFYUI_PIN_COMMIT}" != "${PIN}" ]]; then
    echo "ERROR: v7 Comfy Kitchen requires the reviewed core pin ${PIN}." >&2
    exit 1
fi
case "${1:-}" in
    install-core)
        "${PYTHON}" - "${COMFY_DIR}" "${PIN}" <<'PY'
import importlib.metadata as metadata
import json
import pathlib
import re
import subprocess
import sys
import tempfile

root, pin = pathlib.Path(sys.argv[1]), sys.argv[2]
protected = ("torch", "torchaudio", "torchvision", "transformers", "tokenizers", "huggingface-hub")
before = {name: metadata.version(name) for name in protected}
root.mkdir(parents=True, exist_ok=True)
def git(*args):
    return subprocess.check_output(["git", "-c", f"safe.directory={root}", "-C", str(root), *args], text=True).strip()
def restore_attention_patch(stash):
    try:
        git("stash", "apply", stash)
    except subprocess.CalledProcessError:
        # Native CK's core also added xformers' optional scale keyword. The
        # preserved FCS fallback replaces that same line. Forward-port only
        # this exact, recognized conflict; leave every other merge untouched.
        rel = "comfy/ldm/modules/attention.py"
        if git("diff", "--name-only", "--diff-filter=U").splitlines() != [rel]:
            raise
        path = root / rel
        source = path.read_text()
        start, middle, end = "<<<<<<< Updated upstream\n", "=======\n", ">>>>>>> Stashed changes\n"
        upstream = '    out = xformers.ops.memory_efficient_attention(q, k, v, attn_bias=mask, scale=kwargs.get("scale", None))\n'
        old_call = "out = xformers.ops.memory_efficient_attention(q, k, v, attn_bias=mask)"
        if any(source.count(marker) != 1 for marker in (start, middle, end)):
            raise
        left, conflict = source.split(start)
        current, conflict = conflict.split(middle)
        preserved, right = conflict.split(end)
        if current != upstream or preserved.count(old_call) != 1 or "FCS xformers fallback patch" not in preserved:
            raise
        preserved = preserved.replace(old_call, upstream.strip())
        path.write_text(left + preserved + right)
        git("add", "--", rel)
        git("restore", "--staged", "--", rel)
        print("Preserved FCS xformers fallback and native optional scale keyword", flush=True)
if not (root / ".git").exists():
    if (root / "main.py").exists():
        raise SystemExit("Refusing to replace a non-git ComfyUI installation")
    git("init")
current = git("rev-parse", "HEAD") if (root / ".git/HEAD").exists() and (root / "main.py").exists() else None
if git("ls-files", "-u"):
    raise SystemExit("Unresolved core merge; recover the preserved patch before retrying")
if current != pin:
    # The base image's FCS xformers fallback is a user-owned tracked patch.
    # Preserve it in an explicit recoverable stash and reapply to the new core.
    dirty = git("diff", "--name-only").splitlines()
    if git("diff", "--cached", "--name-only") or any(p != "comfy/ldm/modules/attention.py" for p in dirty):
        raise SystemExit("Refusing core update with unrelated tracked changes")
    stash = None
    if dirty:
        diff = git("diff", "--", dirty[0])
        if "FCS xformers fallback patch" not in diff:
            raise SystemExit("Unrecognized attention patch; review before core update")
        git("stash", "push", "-m", "v7-ck-preserved-attention-patch", "--", dirty[0])
        stash = git("rev-parse", "refs/stash")
        print(f"Preserved existing attention patch in git stash {stash}", flush=True)
    try:
        git("fetch", "--depth", "1", "https://github.com/Comfy-Org/ComfyUI.git", pin)
        git("checkout", "--detach", pin)
        if stash:
            restore_attention_patch(stash)
    except Exception:
        print(f"Core update failed; previous core={current}, recoverable stash={stash}", file=sys.stderr)
        raise

# --no-deps prevents indirect torch/Transformers resolution as well as direct
# installs. Existing framework versions remain byte-for-byte in place.
requirements = []
for line in (root / "requirements.txt").read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    name = re.split(r"[<>=!~\[; ]", line, 1)[0].lower().replace("_", "-")
    if name not in protected:
        requirements.append(line)
with tempfile.TemporaryDirectory(prefix="v7-ck-requirements-") as tmp:
    path = pathlib.Path(tmp) / "requirements.txt"
    path.write_text("\n".join(requirements) + "\n")
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-deps", "--no-cache-dir", "-r", str(path)], check=True)
# The template distribution splits its bundled JSON/media into exact-version
# packages. Resolve only that closed, non-runtime namespace, still --no-deps.
template_assets = sorted({req.split(";", 1)[0].strip() for req in metadata.requires("comfyui-workflow-templates") or []})
if any(not re.fullmatch(r"comfyui[-_]workflow[-_]templates[-_][a-zA-Z0-9_-]+==[0-9.]+", req) for req in template_assets):
    raise SystemExit("Unexpected template dependency; review before installing")
if template_assets:
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-deps", "--no-cache-dir", *template_assets], check=True)
after = {name: metadata.version(name) for name in protected}
if before != after:
    raise SystemExit("Protected framework versions changed")
for name, expected in (("comfy-kitchen", "0.2.31"), ("comfy-aimdo", "0.4.13")):
    if metadata.version(name) != expected:
        raise SystemExit(f"Unexpected {name} version")
print(json.dumps({"core": git("rev-parse", "HEAD"), "frameworkUnchanged": after,
                  "comfyKitchen": metadata.version("comfy-kitchen"), "comfyAimdo": metadata.version("comfy-aimdo")}))
PY
        ;;
    configure-launcher)
        "${PYTHON}" - "${DM_COMFYUI_SUPERVISOR_LAUNCH_SCRIPT:-/opt/supervisor-scripts/comfyui.sh}" <<'PY'
import pathlib
import re
import sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
runtime_install = "    uv pip --no-cache-dir install -r requirements.txt\n"
runtime_guard = '    if [[ "${SERVER_TYPE:-}" != "asset_gen_v7_lite" ]]; then\n' + runtime_install + "    fi\n"
if source.count(runtime_install) != 1 or source.count(runtime_guard) > 1:
    raise SystemExit("Unrecognized automatic requirements installer; review before launching v7")
if runtime_guard not in source:
    source = source.replace(runtime_install, runtime_guard)
start = "# FURGEN v7 native Comfy Kitchen attention (managed)"
end = "# /FURGEN v7 native Comfy Kitchen attention"
source = re.sub(re.escape(start) + r"\n.*?" + re.escape(end) + r"\n", "", source, flags=re.S)
# Anchor AFTER Launch ComfyUI, not merely after the current bootstrap: legacy
# bundle installs remove/reinsert the bootstrap immediately BEFORE Launch.
# This stays after persisted env on every supervisor/agent restart.
block = start + '''
if [[ "${SERVER_TYPE:-}" == "asset_gen_v7_lite" ]]; then
    COMFYUI_ARGS="$(COMFYUI_ARGS="${COMFYUI_ARGS:-}" "${FURGEN_COMFY_PYTHON:-/venv/main/bin/python}" -c 'import os; flags = {"--use-sage-attention", "--use-pytorch-cross-attention", "--use-flash-attention", "--use-split-cross-attention", "--use-quad-cross-attention", "--use-ck-attention"}; print(" ".join(x for x in os.environ["COMFYUI_ARGS"].split() if x not in flags) + " --use-ck-attention")')"
    export COMFYUI_ARGS
fi
''' + end + "\n"
marker = "# Launch ComfyUI\n"
env_marker = "# /FURGEN dependency agent watchdog bootstrap\n"
if source.count(marker) != 1 or source.count(env_marker) > 1 or (env_marker in source and source.index(env_marker) > source.index(marker)):
    raise SystemExit("Expected exactly one post-env launcher anchor")
source = source.replace(marker, marker + block)
path.write_text(source)
print("Configured durable v7 Comfy Kitchen attention after persisted-env bootstrap")
PY
        bash -n "${DM_COMFYUI_SUPERVISOR_LAUNCH_SCRIPT:-/opt/supervisor-scripts/comfyui.sh}"
        ;;
    *) echo "Usage: $0 {install-core|configure-launcher}" >&2; exit 2 ;;
esac

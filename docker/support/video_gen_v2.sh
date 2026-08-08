#!/bin/bash

set -x

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"
export SERVER_TYPE="${SERVER_TYPE:-video_gen_v2}"
export FURGEN_COMFYUI_START_ALLOWED_FILE="${FURGEN_COMFYUI_START_ALLOWED_FILE:-${WORKSPACE}/.furgen_comfyui_start_allowed}"
FURGENPUB_RAW_BASE_URL="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}"
VIDEO_GEN_V2_IMAGE_FILTERS_REPO="${VIDEO_GEN_V2_IMAGE_FILTERS_REPO:-https://github.com/spacepxl/ComfyUI-Image-Filters}"
VIDEO_GEN_V2_IMAGE_FILTERS_PIN="${VIDEO_GEN_V2_IMAGE_FILTERS_PIN:-bbb3fb0045461adf3602faeedaf40af57090d4e2}"
VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT="${VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT:-opencv-contrib-python==4.10.0.84}"
VIDEO_GEN_V2_LTX_CONTEXT_WINDOWS_COMFYUI_PIN="${VIDEO_GEN_V2_LTX_CONTEXT_WINDOWS_COMFYUI_PIN:-cd77c551d6c7efa46a8ba514fd6f4e04aac76b4d}"
export VIDEO_GEN_V2_SAGEATTENTION_VERSION="${VIDEO_GEN_V2_SAGEATTENTION_VERSION:-2.2.0}"
export VIDEO_GEN_V2_SAGEATTENTION_SOURCE_COMMIT="${VIDEO_GEN_V2_SAGEATTENTION_SOURCE_COMMIT:-eb615cf6cf4d221338033340ee2de1c37fbdba4a}"
export VIDEO_GEN_V2_SAGEATTENTION_WHEEL_BASE_URL="${VIDEO_GEN_V2_SAGEATTENTION_WHEEL_BASE_URL:-https://github.com/Comfy-Org/wheels/releases/download/sageattention-latest}"
export VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH="${VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH:-${WORKSPACE}/sageattention2_runtime.json}"

mkdir -p "${WORKSPACE}" "${DM_COMFYUI_DIR}" || true

if [[ -z "$DM_INSTANCE_ID" && -n "$VAST_CONTAINERLABEL" ]]; then
    DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
    export DM_INSTANCE_ID
fi

source /venv/main/bin/activate
COMFYUI_DIR="${DM_COMFYUI_DIR}"
# Leave the Vast image's bundled ComfyUI version in place by default. Set
# COMFYUI_PIN to an explicit commit/tag only when we need to override the image.
COMFYUI_PIN="${COMFYUI_PIN:-}"

# NOTE:
# - Do NOT put Hugging Face tokens in this file (or in git clone URLs).
# - Export `HF_TOKEN` (or `HUGGINGFACE_HUB_TOKEN`) in the container environment instead.

# Allow either env var name; keep existing `HF_TOKEN` usage below.
# Avoid leaking tokens in logs if xtrace is enabled.
__xtrace_was_on=0
case "$-" in
    *x*) __xtrace_was_on=1; set +x ;;
esac
HF_TOKEN="${HF_TOKEN:-${HUGGINGFACE_HUB_TOKEN:-}}"
[[ "$__xtrace_was_on" -eq 1 ]] && set -x
unset __xtrace_was_on

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
    "ca-certificates"
    "curl"
    "libgnutls30"
)

PIP_PACKAGES=(
    "flash-attn"
    "triton"
    "kornia<0.8"
    "onnxruntime"
    # For authenticated snapshot downloads from Hugging Face (avoids git/LFS auth issues)
    "huggingface_hub>=0.20.0"
    # Ensure Impact-Pack imports succeed even if its requirements
    # fail due to VCS deps (e.g., git+sam2). piexif is small and safe.
    "piexif"
)

NODES=(
    "https://github.com/ltdrdata/ComfyUI-Manager"
    "https://github.com/cubiq/ComfyUI_essentials"
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"

    # Video processing nodes
    "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite"
    "https://github.com/GACLove/ComfyUI-VFI"
    "https://github.com/Lightricks/ComfyUI-LTXVideo"
    "https://github.com/TenStrip/10S-Comfy-nodes"

    # WanVideo nodes
    "https://github.com/kijai/ComfyUI-WanVideoWrapper"
    "https://github.com/kijai/ComfyUI-KJNodes"

    # Other nodes
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
    "https://github.com/evanspearman/ComfyMath"
    "https://github.com/kijai/ComfyUI-MelBandRoFormer"
    "https://github.com/ClownsharkBatwing/RES4LYF"
)

# Hugging Face repo snapshots (download the whole repo into a folder).
# Used for LLM/GGUF/etc where a single "resolve/main/file" URL isn't enough.
#
### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

function provisioning_restore_comfyui_checkout() {
    local src
    if [[ -f "${COMFYUI_DIR}/main.py" ]]; then
        return 0
    fi

    for src in \
        "/opt/workspace-internal/ComfyUI" \
        "/workspace-internal/ComfyUI" \
        "/opt/ComfyUI" \
        "/ComfyUI"
    do
        if [[ -f "${src}/main.py" ]]; then
            printf "Restoring bundled ComfyUI checkout from %s to %s\n" "${src}" "${COMFYUI_DIR}"
            mkdir -p "${COMFYUI_DIR}" || true
            if command -v rsync >/dev/null 2>&1; then
                rsync -a --ignore-existing "${src}/" "${COMFYUI_DIR}/" || true
            else
                cp -an "${src}/." "${COMFYUI_DIR}/" || true
            fi
            break
        fi
    done

    if [[ ! -f "${COMFYUI_DIR}/main.py" ]]; then
        printf "WARN: ComfyUI main.py is still missing at %s; supervisor start may fail until image checkout is restored.\n" "${COMFYUI_DIR}"
    fi
}

function provisioning_stop_wrong_port_comfyui() {
    /venv/main/bin/python - <<'PY' || true
import os
import signal
import time

target_port = "8188"
own_pids = {os.getpid(), os.getppid()}
killed = []

for name in os.listdir("/proc"):
    if not name.isdigit():
        continue
    pid = int(name)
    if pid in own_pids:
        continue
    try:
        raw = open(f"/proc/{pid}/cmdline", "rb").read()
    except OSError:
        continue
    parts = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if not parts or not any(part.endswith("main.py") or part == "main.py" for part in parts):
        continue
    try:
        cwd = os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        cwd = ""
    if not ("ComfyUI" in cwd or any("ComfyUI" in part for part in parts)):
        continue

    port = ""
    for idx, part in enumerate(parts):
        if part == "--port" and idx + 1 < len(parts):
            port = parts[idx + 1]
        elif part.startswith("--port="):
            port = part.split("=", 1)[1]
    if port in ("", target_port):
        continue

    print(f"Terminating ComfyUI running on stale port {port}: pid={pid}", flush=True)
    try:
        os.kill(pid, signal.SIGTERM)
        killed.append(pid)
    except ProcessLookupError:
        pass

if killed:
    time.sleep(2)
    for pid in killed:
        if os.path.exists(f"/proc/{pid}"):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
PY
}

# Modular pinning for custom nodes
# Map: folder name -> commit/tag. Extend/override via COMFY_NODE_PINS env var.
# Example: COMFY_NODE_PINS="ComfyUI-Impact-Pack=61bd8397a18e7e7668e6a24e95168967768c2bed,ComfyUI-Manager=v2.22"
declare -A NODE_PINS
# Defaults from furry_all_v7.sh where available; latest HEAD otherwise
NODE_PINS[ComfyUI-Impact-Pack]="61bd8397a18e7e7668e6a24e95168967768c2bed"
NODE_PINS[comfyui_controlnet_aux]="cc6b232f4a47f0cdf70f4e1bfa24b74bd0d75bf1"
NODE_PINS[ComfyUI-Impact-Subpack]="50c7b71a6a224734cc9b21963c6d1926816a97f1"
NODE_PINS[ComfyUI-KJNodes]="7b1327192e4729085788a3020a9cbb095e0c7811"
NODE_PINS[ComfyUI-Manager]="b5a2bed5396e6be8a2d1970793f5ce2f1e74c8c2"
NODE_PINS[ComfyUI_essentials]="9d9f4bedfc9f0321c19faf71855e228c93bd0dc9"
NODE_PINS[was-node-suite-comfyui]="ea935d1044ae5a26efa54ebeb18fe9020af49a45"
NODE_PINS[ComfyUI_Comfyroll_CustomNodes]="d78b780ae43fcf8c6b7c6505e6ffb4584281ceca"
NODE_PINS[ComfyUI-ComfyCouple]="6c815b13e6269b7ade1dd3a49ef67de71a0014eb"
NODE_PINS[LoopsGroundingDino]="8d84e5501d147d974ba4b6bfeb5de67c324523a0"
NODE_PINS[ComfyUI-RMBG]="b28ce10b51e1d505a2ebf2608184119f0cf662d3"
NODE_PINS[ComfyUI-VideoHelperSuite]="08e8df15db24da292d4b7f943c460dc2ab442b24"
NODE_PINS[10S_Nodes]="fb6edfed97abaf246a826812536eef018d7a1c3b"

# New repos (latest as of now)
NODE_PINS[ComfyUI-Frame-Interpolation]="a969c01dbccd9e5510641be04eb51fe93f6bfc3d"
NODE_PINS[ComfyUI-GGUF]="be2a08330d7ec232d684e50ab938870d7529471e"
NODE_PINS[rgthree-comfy]="2b9eb36d3e1741e88dbfccade0e08137f7fa2bfb"
NODE_PINS[ComfyUI-Custom-Scripts]="f2838ed5e59de4d73cde5c98354b87a8d3200190"
NODE_PINS[ComfyUI-WanVideoWrapper]="b982b4ef0c41cb1c83ae53980860c3598a53814e"

function load_node_pins_from_env() {
    [[ -z "$COMFY_NODE_PINS" ]] && return 0
    local payload entries
    payload="$COMFY_NODE_PINS"
    payload="${payload// /,}"
    IFS=',' read -r -a entries <<< "$payload"
    for entry in "${entries[@]}"; do
        [[ -z "$entry" ]] && continue
        local name="${entry%%=*}"
        local ref="${entry#*=}"
        if [[ -n "$name" && -n "$ref" ]]; then
            NODE_PINS["$name"]="$ref"
        fi
    done
}

function pin_node_if_requested() {
    local dir="$1"; shift
    local path="$1"
    local pin_ref="${NODE_PINS[$dir]}"
    if [[ -n "$pin_ref" ]]; then
        printf "Pinning %s to %s...\n" "$dir" "$pin_ref"
        (
            cd "$path" && git fetch --all --tags && git checkout --force "$pin_ref"
        ) || echo "WARN: Failed to pin $dir to $pin_ref"
    fi
}

function node_dir_for_repo() {
    local repo="$1"
    local dir="${repo##*/}"
    dir="${dir%.git}"
    case "$dir" in
        10S-Comfy-nodes) dir="10S_Nodes" ;;
    esac
    printf "%s" "$dir"
}

function provisioning_update_comfyui() {
    echo "DEBUG: Checking for ComfyUI git repository in ${COMFYUI_DIR}"
    if [[ -z "${COMFYUI_PIN}" ]]; then
        echo "DEBUG: COMFYUI_PIN is unset; preserving the ComfyUI version bundled in the Vast image."
        return 0
    fi
    if [[ -d "${COMFYUI_DIR}/.git" ]]; then
        printf "Updating ComfyUI to pinned version (%s)...\n" "${COMFYUI_PIN:0:7}"
        if ! (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch --all --tags
            echo "DEBUG: Checking out pinned commit..."
            git checkout --force "${COMFYUI_PIN}"
        ); then
            echo "ERROR: Failed to checkout pinned ComfyUI commit ${COMFYUI_PIN}."
            return 1
        fi
        if [ -f "${COMFYUI_DIR}/requirements.txt" ]; then
            printf "Installing ComfyUI requirements...\n"
            pip install --no-cache-dir -r "${COMFYUI_DIR}/requirements.txt"
        else
            echo "DEBUG: requirements.txt not found in ${COMFYUI_DIR}"
        fi
    else
        echo "DEBUG: ComfyUI git repository not found."
    fi
}

function provisioning_install_ltx_context_windows() {
    local previous_pin="${COMFYUI_PIN}"
    local update_status=0
    COMFYUI_PIN="${VIDEO_GEN_V2_LTX_CONTEXT_WINDOWS_COMFYUI_PIN}"
    provisioning_update_comfyui || update_status=$?
    COMFYUI_PIN="${previous_pin}"
    if [[ "${update_status}" -ne 0 ]]; then
        return "${update_status}"
    fi
    if ! grep -R "LTXVContextWindows" "${COMFYUI_DIR}/comfy_extras" >/dev/null 2>&1; then
        printf "ERROR: LTXVContextWindows was not found after updating ComfyUI to %s.\n" "${VIDEO_GEN_V2_LTX_CONTEXT_WINDOWS_COMFYUI_PIN}"
        return 1
    fi
}

function provisioning_start() {
    local soft_failures=0

    provisioning_print_header
    provisioning_hold_comfyui_start_gate || true
    provisioning_restore_comfyui_checkout || true
    provisioning_configure_comfyui_launch_args || true
    provisioning_stop_wrong_port_comfyui || true
    provisioning_update_comfyui
    provisioning_get_apt_packages
    load_node_pins_from_env
    provisioning_get_nodes || {
        printf "WARN: Provisioning step 'provisioning_get_nodes' failed with exit code %s; continuing.\n" "$?"
        soft_failures=1
    }
    provisioning_fix_python_compatibility || return 1
    provisioning_install_furgen_video_tools_node || return 1
    # Safety pass: re-apply any per-node requirements and ensure Impact-Pack deps
    provisioning_ensure_node_requirements
    provisioning_get_pip_packages || {
        printf "WARN: Provisioning step 'provisioning_get_pip_packages' failed with exit code %s; continuing.\n" "$?"
        soft_failures=1
    }
    provisioning_install_sageattention2 || return 1
    provisioning_fix_python_compatibility || return 1
    provisioning_install_furgen_compat_nodes || return 1
    provisioning_print_end || return 1
    if [[ "$soft_failures" -ne 0 ]]; then
        printf "Provisioning completed with non-fatal warnings.\n"
    fi
}

function provisioning_configure_comfyui_launch_args() {
    local launch_script
    launch_script="/opt/supervisor-scripts/comfyui.sh"
    if [[ ! -f "${launch_script}" ]]; then
        printf "WARN: ComfyUI launch script not found for args normalization: %s\n" "${launch_script}"
        return 0
    fi

    /venv/main/bin/python - "${launch_script}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
original = source

managed_pattern = re.compile(
    r"# FURGEN ComfyUI launch args normalization\n(?:.*\n)*?# /FURGEN ComfyUI launch args normalization\n",
    re.MULTILINE,
)
source = managed_pattern.sub("", source)
legacy_args_pattern = re.compile(
    r'COMFYUI_ARGS=\$\{COMFYUI_ARGS:---disable-auto-launch --port 18188 --enable-cors-header\}\n'
    r'if \[\[ " \$\{COMFYUI_ARGS\} " != \*" --disable-cuda-malloc "\* \]\]; then\n'
    r'    COMFYUI_ARGS="\$\{COMFYUI_ARGS\} --disable-cuda-malloc"\n'
    r'fi\n',
    re.MULTILINE,
)
source = legacy_args_pattern.sub("", source)
source = re.sub(
    r'^COMFYUI_ARGS=\$\{COMFYUI_ARGS:---disable-auto-launch --port 18188 --enable-cors-header\}\n',
    "",
    source,
    flags=re.MULTILINE,
)

block = (
    "# FURGEN ComfyUI launch args normalization\n"
    "COMFYUI_ARGS=${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header --use-sage-attention}\n"
    "if [[ \" ${COMFYUI_ARGS} \" != *\" --use-sage-attention \"* ]]; then\n"
    "    COMFYUI_ARGS=\"${COMFYUI_ARGS} --use-sage-attention\"\n"
    "fi\n"
    "COMFYUI_ARGS=\"${COMFYUI_ARGS// --disable-cuda-malloc/}\"\n"
    "COMFYUI_ARGS=\"${COMFYUI_ARGS//--disable-cuda-malloc/}\"\n"
    "video_gen_v2_disable_cuda_malloc=\"$(printf '%s' \"${VIDEO_GEN_V2_COMFY_DISABLE_CUDA_MALLOC:-false}\" | tr '[:upper:]' '[:lower:]')\"\n"
    "if [[ \"${video_gen_v2_disable_cuda_malloc}\" == \"1\" || \"${video_gen_v2_disable_cuda_malloc}\" == \"true\" ]]; then\n"
    "    COMFYUI_ARGS=\"${COMFYUI_ARGS} --disable-cuda-malloc\"\n"
    "fi\n"
    "unset video_gen_v2_disable_cuda_malloc\n"
    "furgen_comfyui_port=\"$(printf '%s\\n' \"${COMFYUI_ARGS}\" | sed -n 's/.*--port[ =]\\([0-9][0-9]*\\).*/\\1/p' | tail -n 1)\"\n"
    "furgen_comfyui_port=\"${furgen_comfyui_port:-8188}\"\n"
    "if [[ \"$(printf '%s' \"${FURGEN_COMFYUI_PORT_CLEANUP:-true}\" | tr '[:upper:]' '[:lower:]')\" != \"false\" ]]; then\n"
    "    FURGEN_COMFYUI_PORT=\"${furgen_comfyui_port}\" /venv/main/bin/python - <<'PY'\n"
    "import os\n"
    "import signal\n"
    "import time\n"
    "\n"
    "port = int(os.environ.get('FURGEN_COMFYUI_PORT') or '8188')\n"
    "port_hex = f'{port:04X}'\n"
    "listen_inodes = set()\n"
    "for proc_net in ('/proc/net/tcp', '/proc/net/tcp6'):\n"
    "    try:\n"
    "        rows = open(proc_net, encoding='utf-8').read().splitlines()[1:]\n"
    "    except OSError:\n"
    "        continue\n"
    "    for row in rows:\n"
    "        cols = row.split()\n"
    "        if len(cols) > 9 and cols[3] == '0A' and cols[1].rsplit(':', 1)[-1].upper() == port_hex:\n"
    "            listen_inodes.add(cols[9])\n"
    "\n"
    "if not listen_inodes:\n"
    "    raise SystemExit(0)\n"
    "\n"
    "own_pids = {os.getpid(), os.getppid()}\n"
    "killed = []\n"
    "for name in os.listdir('/proc'):\n"
    "    if not name.isdigit():\n"
    "        continue\n"
    "    pid = int(name)\n"
    "    if pid in own_pids:\n"
    "        continue\n"
    "    fd_dir = f'/proc/{pid}/fd'\n"
    "    try:\n"
    "        fds = os.listdir(fd_dir)\n"
    "    except OSError:\n"
    "        continue\n"
    "    matched = False\n"
    "    for fd in fds:\n"
    "        try:\n"
    "            target = os.readlink(os.path.join(fd_dir, fd))\n"
    "        except OSError:\n"
    "            continue\n"
    "        if target.startswith('socket:[') and target[8:-1] in listen_inodes:\n"
    "            matched = True\n"
    "            break\n"
    "    if not matched:\n"
    "        continue\n"
    "    try:\n"
    "        cmdline = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\\0', b' ').decode('utf-8', 'replace').strip()\n"
    "    except OSError:\n"
    "        cmdline = ''\n"
    "    print(f'Terminating stale listener on Comfy port {port}: pid={pid} {cmdline[:200]}', flush=True)\n"
    "    try:\n"
    "        os.kill(pid, signal.SIGTERM)\n"
    "        killed.append(pid)\n"
    "    except ProcessLookupError:\n"
    "        pass\n"
    "\n"
    "if killed:\n"
    "    time.sleep(2)\n"
    "    for pid in killed:\n"
    "        if os.path.exists(f'/proc/{pid}'):\n"
    "            try:\n"
    "                os.kill(pid, signal.SIGKILL)\n"
    "            except ProcessLookupError:\n"
    "                pass\n"
    "PY\n"
    "fi\n"
    "furgen_readiness_file=\"${WORKSPACE:-/workspace}/ComfyUI/input/provisioned_furry_all.txt\"\n"
    "rm -f \"${furgen_readiness_file}\" || true\n"
    "furgen_start_gate_enabled=\"$(printf '%s' \"${FURGEN_COMFYUI_BOOTSTRAP_GATE_ENABLED:-true}\" | tr '[:upper:]' '[:lower:]')\"\n"
    "if [[ \"${furgen_start_gate_enabled}\" != \"0\" && \"${furgen_start_gate_enabled}\" != \"false\" ]]; then\n"
    "    furgen_start_gate_file=\"${FURGEN_COMFYUI_START_ALLOWED_FILE:-${WORKSPACE:-/workspace}/.furgen_comfyui_start_allowed}\"\n"
    "    furgen_start_gate_timeout=\"${FURGEN_COMFYUI_BOOTSTRAP_GATE_TIMEOUT_SEC:-3600}\"\n"
    "    furgen_start_gate_waited=0\n"
    "    if [[ ! -f \"${furgen_start_gate_file}\" ]]; then\n"
    "        echo \"Waiting for Furgen video_gen_v2 bootstrap gate: ${furgen_start_gate_file}\"\n"
    "    fi\n"
    "    while [[ ! -f \"${furgen_start_gate_file}\" ]]; do\n"
    "        if [[ \"${furgen_start_gate_timeout}\" =~ ^[0-9]+$ && \"${furgen_start_gate_timeout}\" -gt 0 && \"${furgen_start_gate_waited}\" -ge \"${furgen_start_gate_timeout}\" ]]; then\n"
    "            echo \"ERROR: Timed out waiting for Furgen video_gen_v2 bootstrap gate: ${furgen_start_gate_file}\" >&2\n"
    "            exit 74\n"
    "        fi\n"
    "        sleep 2\n"
    "        furgen_start_gate_waited=$((furgen_start_gate_waited + 2))\n"
    "    done\n"
    "    if [[ \"${furgen_start_gate_waited}\" -gt 0 ]]; then\n"
    "        echo \"Furgen video_gen_v2 bootstrap gate released after ${furgen_start_gate_waited}s.\"\n"
    "    fi\n"
    "fi\n"
    "(\n"
    "    furgen_ready_port=\"${furgen_comfyui_port}\"\n"
    "    furgen_ready_file=\"${furgen_readiness_file}\"\n"
    "    furgen_sage_verify_path=\"${VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH:-${WORKSPACE:-/workspace}/sageattention2_runtime.json}\"\n"
    "    for furgen_ready_attempt in $(seq 1 300); do\n"
    "        if curl -fsS --max-time 2 \"http://127.0.0.1:${furgen_ready_port}/queue\" >/dev/null 2>&1; then\n"
    "            if [[ ! -s \"${furgen_sage_verify_path}\" ]]; then\n"
    "                echo \"WARN: SageAttention2 runtime verification is missing; readiness marker not written.\" >&2\n"
    "                sleep 2\n"
    "                continue\n"
    "            fi\n"
    "            if ! ps -eo args= | grep -Eq '[p]ython([0-9.]+)? .*main\\.py .*--use-sage-attention'; then\n"
    "                echo \"WARN: ComfyUI is reachable without --use-sage-attention; readiness marker not written.\" >&2\n"
    "                sleep 2\n"
    "                continue\n"
    "            fi\n"
    "            mkdir -p \"$(dirname \"${furgen_ready_file}\")\"\n"
    "            echo \"Provisioning completed with SageAttention2 and ComfyUI ready at $(date)\" > \"${furgen_ready_file}\"\n"
    "            exit 0\n"
    "        fi\n"
    "        sleep 2\n"
    "    done\n"
    "    echo \"WARN: ComfyUI did not become ready with SageAttention2 on port ${furgen_ready_port}; readiness marker not written.\" >&2\n"
    ") &\n"
    "# Bypass Vast's unbuffer-based pty wrapper for Comfy. The wrapper can exit\n"
    "# cleanly while long GPU jobs are still running, causing supervisor to\n"
    "# restart Comfy and strand queued_on_comfy jobs.\n"
    "export DISABLE_PTY=\"${DISABLE_PTY:-true}\"\n"
    "unset furgen_comfyui_port furgen_readiness_file furgen_start_gate_enabled furgen_start_gate_file furgen_start_gate_timeout furgen_start_gate_waited\n"
    "# /FURGEN ComfyUI launch args normalization\n"
)

anchor = "# Launch ComfyUI\n"
if anchor in source:
    insert_at = source.find(anchor)
else:
    launch_idx = source.find("python main.py")
    insert_at = source.rfind("\n", 0, launch_idx) + 1 if launch_idx != -1 else len(source)

patched = source[:insert_at] + block + source[insert_at:]

if patched != original:
    path.write_text(patched, encoding="utf-8")
    print("Applied ComfyUI launch args normalization patch.")
else:
    print("ComfyUI launch args normalization already present.")
PY

    chmod +x "${launch_script}" || true
}

function provisioning_get_apt_packages() {
    if [[ ${#APT_PACKAGES[@]} -eq 0 ]]; then
        return 0
    fi

    local packages_to_install=("${APT_PACKAGES[@]}")
    if command -v dpkg-query >/dev/null 2>&1; then
        packages_to_install=()
        local package_name
        for package_name in "${APT_PACKAGES[@]}"; do
            if dpkg-query -W -f='${Status}' "$package_name" 2>/dev/null | grep -Fq "install ok installed"; then
                printf "Apt package already installed: %s\n" "$package_name"
            else
                packages_to_install+=("$package_name")
            fi
        done
    fi

    if [[ ${#packages_to_install[@]} -eq 0 ]]; then
        printf "All apt package prerequisites are already installed; skipping apt-get update/install.\n"
        return 0
    fi

    printf "Installing missing apt package prerequisites: %s\n" "${packages_to_install[*]}"
    if command -v apt-get >/dev/null 2>&1; then
        if command -v sudo >/dev/null 2>&1; then
            sudo apt-get update
            sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages_to_install[@]}"
        else
            apt-get update
            DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages_to_install[@]}"
        fi
    elif [[ -n ${APT_INSTALL:-} ]]; then
        sudo ${APT_INSTALL} "${packages_to_install[@]}"
    else
        printf "WARN: No apt installer available; skipping package install: %s\n" "${packages_to_install[*]}"
        return 1
    fi
}

function provisioning_get_pip_packages() {
    if [[ -n $PIP_PACKAGES ]]; then
            pip install --no-cache-dir ${PIP_PACKAGES[@]}
    fi
}

function provisioning_install_sageattention2() {
    local runtime_key cuda_capability wheel_name wheel_url wheel_sha256 wheel_size wheel_path actual_size actual_sha256

    runtime_key="$(python - <<'PY' || true
import platform
import sys

import torch

torch_version = torch.__version__.split("+", 1)[0].split(".")
torch_series = ".".join(torch_version[:2])
cuda_runtime = str(torch.version.cuda or "")
python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
machine = platform.machine().lower()
print(f"{torch_series}|{cuda_runtime}|{python_tag}|{machine}")
PY
)"

    cuda_capability="$(python - <<'PY' || true
import torch

if not torch.cuda.is_available():
    raise SystemExit(1)
major, minor = torch.cuda.get_device_capability()
print(f"{major}.{minor}")
PY
)"

    case "${runtime_key}|${cuda_capability}" in
        # The Comfy-Org cu128/cu129 Linux wheels do not contain an SM 12.0
        # kernel image. Their cu130 torch2.10 wheel is ABI-compatible with the
        # cu128/cu129 torch2.10 runtimes and is verified below on the real GPU.
        "2.10|12.8|cp312|x86_64|12.0"|"2.10|12.8|cp312|amd64|12.0"|\
        "2.10|12.9|cp312|x86_64|12.0"|"2.10|12.9|cp312|amd64|12.0")
            wheel_name="sageattention-2.2.0+cu130torch2.10-cp312-cp312-manylinux_2_34_x86_64.manylinux_2_35_x86_64.whl"
            wheel_sha256="f0f8a1b9ba89719ab69c4481c1c94e959c4950d53f92bf0c96e3652e81544b21"
            wheel_size="27466865"
            ;;
        *)
            case "${runtime_key}" in
                "2.10|12.8|cp312|x86_64"|"2.10|12.8|cp312|amd64")
                    wheel_name="sageattention-2.2.0+cu128torch2.10-cp312-cp312-manylinux_2_34_x86_64.manylinux_2_35_x86_64.whl"
                    wheel_sha256="78adfed40544519b77d2d11a10b216011c91c17a7ae1634f616807c1e9c3d1aa"
                    wheel_size="27418276"
                    ;;
                "2.10|12.9|cp312|x86_64"|"2.10|12.9|cp312|amd64")
                    wheel_name="sageattention-2.2.0+cu129torch2.10-cp312-cp312-manylinux_2_34_x86_64.manylinux_2_35_x86_64.whl"
                    wheel_sha256="5e5cda462f73306cd8c201516d736357fc577ab5b9372dc5069362b4eab85dcf"
                    wheel_size="33809602"
                    ;;
                "2.10|13.0|cp312|x86_64"|"2.10|13.0|cp312|amd64")
                    wheel_name="sageattention-2.2.0+cu130torch2.10-cp312-cp312-manylinux_2_34_x86_64.manylinux_2_35_x86_64.whl"
                    wheel_sha256="f0f8a1b9ba89719ab69c4481c1c94e959c4950d53f92bf0c96e3652e81544b21"
                    wheel_size="27466865"
                    ;;
                *)
                    printf "ERROR: No pinned SageAttention2 wheel is registered for runtime %s.\n" "${runtime_key:-unknown}"
                    return 1
                    ;;
            esac
            ;;
    esac

    wheel_url="${VIDEO_GEN_V2_SAGEATTENTION_WHEEL_BASE_URL%/}/${wheel_name//+/%2B}"
    wheel_path="/tmp/${wheel_name}"
    printf "Installing pinned SageAttention2 %s for runtime %s on compute capability %s.\n" \
        "${VIDEO_GEN_V2_SAGEATTENTION_VERSION}" "${runtime_key}" "${cuda_capability:-unknown}"
    curl -fL --retry 3 --retry-all-errors -o "${wheel_path}" "${wheel_url}" || return 1

    actual_size="$(stat -c '%s' "${wheel_path}")"
    if [[ "${actual_size}" != "${wheel_size}" ]]; then
        printf "ERROR: SageAttention2 wheel size mismatch: expected %s, got %s.\n" "${wheel_size}" "${actual_size}"
        return 1
    fi
    actual_sha256="$(sha256sum "${wheel_path}" | awk '{print $1}')"
    if [[ "${actual_sha256}" != "${wheel_sha256}" ]]; then
        printf "ERROR: SageAttention2 wheel SHA-256 mismatch: expected %s, got %s.\n" "${wheel_sha256}" "${actual_sha256}"
        return 1
    fi

    python -m pip uninstall -y sageattention || true
    python -m pip install --no-cache-dir --no-deps --force-reinstall "${wheel_path}" || return 1

    VIDEO_GEN_V2_SAGEATTENTION_VERSION="${VIDEO_GEN_V2_SAGEATTENTION_VERSION}" \
    VIDEO_GEN_V2_SAGEATTENTION_SOURCE_COMMIT="${VIDEO_GEN_V2_SAGEATTENTION_SOURCE_COMMIT}" \
    VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH="${VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH}" \
    VIDEO_GEN_V2_SAGEATTENTION_EXPECTED_WHEEL_SHA256="${wheel_sha256}" \
    VIDEO_GEN_V2_SAGEATTENTION_RUNTIME_KEY="${runtime_key}" \
    python - <<'PY' || return 1
import importlib.metadata
import json
import os
from pathlib import Path

import torch
from sageattention import sageattn

expected = os.environ["VIDEO_GEN_V2_SAGEATTENTION_VERSION"]
actual = importlib.metadata.version("sageattention")
if actual.split("+", 1)[0] != expected:
    raise RuntimeError(f"SageAttention version mismatch: expected {expected}, got {actual}")
if not torch.cuda.is_available():
    raise RuntimeError("SageAttention2 runtime verification requires a CUDA GPU")

device = torch.device("cuda")
major, minor = torch.cuda.get_device_capability(device)
q = torch.randn((1, 8, 257, 128), device=device, dtype=torch.bfloat16)
k = torch.randn_like(q)
v = torch.randn_like(q)
if major == 12:
    from sageattention.core import sageattn_qk_int8_pv_fp16_cuda

    kernel_policy = "sm120_qk_int8_pv_fp16_per_thread_fp32"
    out = sageattn_qk_int8_pv_fp16_cuda(
        q,
        k,
        v,
        tensor_layout="HND",
        is_causal=False,
        qk_quant_gran="per_thread",
        pv_accum_dtype="fp32",
    )
else:
    kernel_policy = "upstream_auto_dispatch"
    out = sageattn(q, k, v, tensor_layout="HND", is_causal=False)
torch.cuda.synchronize()
if (
    out.shape != q.shape
    or not torch.isfinite(out).all().item()
    or not torch.count_nonzero(out).item()
):
    raise RuntimeError(f"SageAttention2 kernel returned invalid output: {tuple(out.shape)}")

payload = {
    "attentionBackend": "sageattention2",
    "package": "sageattention",
    "version": actual,
    "sourceCommit": os.environ["VIDEO_GEN_V2_SAGEATTENTION_SOURCE_COMMIT"],
    "wheelSha256": os.environ["VIDEO_GEN_V2_SAGEATTENTION_EXPECTED_WHEEL_SHA256"],
    "runtimeKey": os.environ["VIDEO_GEN_V2_SAGEATTENTION_RUNTIME_KEY"],
    "torchVersion": torch.__version__,
    "torchCudaRuntime": torch.version.cuda,
    "cudaCapability": f"{major}.{minor}",
    "deviceName": torch.cuda.get_device_name(device),
    "kernelPolicy": kernel_policy,
    "kernelSmokePassed": True,
}
verify_path = Path(os.environ["VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH"])
verify_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
print("SAGEATTENTION2_RUNTIME_OK " + json.dumps(payload, sort_keys=True))
PY
}

function provisioning_purge_python_bytecode_for_package() {
    local package_name="$1"
    python - "$package_name" <<'PY' || return 1
import importlib.util
import pathlib
import shutil
import sys

package_name = sys.argv[1]
spec = importlib.util.find_spec(package_name)
roots = []
if spec is not None:
    if spec.submodule_search_locations:
        roots.extend(pathlib.Path(path) for path in spec.submodule_search_locations)
    elif spec.origin:
        roots.append(pathlib.Path(spec.origin).parent)

removed = 0
for root in roots:
    if not root.exists():
        continue
    for path in root.rglob("__pycache__"):
        shutil.rmtree(path, ignore_errors=True)
        removed += 1
    for path in root.rglob("*.pyc"):
        try:
            path.unlink()
            removed += 1
        except FileNotFoundError:
            pass

print(f"Purged {removed} cached bytecode entries for {package_name}")
PY
}

function provisioning_fix_python_compatibility() {
    printf "Enforcing video_gen_v2 Python compatibility pins...\n"
    pip install --no-cache-dir "kornia<0.8" || return 1
    provisioning_purge_python_bytecode_for_package "kornia" || return 1
    python - <<'PY' || return 1
from kornia.geometry.transform.pyramid import pad
print("Verified kornia pyramid.pad import for ComfyUI-LTXVideo")
PY
}

function provisioning_install_furgen_video_tools_node() {
    local script_dir src_dir dest_dir remote_base
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    src_dir="${script_dir}/custom_nodes/FurgenVideoTools"
    dest_dir="${COMFYUI_DIR}/custom_nodes/FurgenVideoTools"
    remote_base="${FURGENPUB_RAW_BASE_URL%/}/custom_nodes/FurgenVideoTools"

    mkdir -p "${COMFYUI_DIR}/custom_nodes"
    rm -rf "${dest_dir}"
    mkdir -p "${dest_dir}"

    if [[ -d "${src_dir}" && -f "${src_dir}/furgen_video_tools.py" && -f "${src_dir}/furgen_sageattention_policy.py" ]] \
        && grep -q "FurgenTemporalUnsharpMask" "${src_dir}/furgen_video_tools.py" \
        && grep -q "sm120_qk_int8_pv_fp16_per_thread_fp32" "${src_dir}/furgen_sageattention_policy.py"; then
        cp -R "${src_dir}/." "${dest_dir}/"
        printf "Installed managed custom node: FurgenVideoTools (local copy)\n"
        return 0
    elif [[ -d "${src_dir}" ]]; then
        printf "WARN: Local FurgenVideoTools source is incomplete; using pinned remote copy.\n"
    fi

    printf "Downloading managed custom node from %s\n" "${remote_base}"
    curl -fsSL "${remote_base}/__init__.py" -o "${dest_dir}/__init__.py" || {
        printf "ERROR: Failed to download FurgenVideoTools __init__.py from %s\n" "${remote_base}"
        return 1
    }
    curl -fsSL "${remote_base}/furgen_video_tools.py" -o "${dest_dir}/furgen_video_tools.py" || {
        printf "ERROR: Failed to download FurgenVideoTools implementation from %s\n" "${remote_base}"
        return 1
    }
    curl -fsSL "${remote_base}/furgen_sageattention_policy.py" -o "${dest_dir}/furgen_sageattention_policy.py" || {
        printf "ERROR: Failed to download FurgenVideoTools SageAttention2 policy from %s\n" "${remote_base}"
        return 1
    }
    if ! grep -q "FurgenTemporalUnsharpMask" "${dest_dir}/furgen_video_tools.py"; then
        printf "ERROR: Downloaded FurgenVideoTools implementation is missing FurgenTemporalUnsharpMask from %s\n" "${remote_base}"
        return 1
    fi
    if ! grep -q "sm120_qk_int8_pv_fp16_per_thread_fp32" "${dest_dir}/furgen_sageattention_policy.py"; then
        printf "ERROR: Downloaded FurgenVideoTools SageAttention2 policy is invalid from %s\n" "${remote_base}"
        return 1
    fi

    printf "Installed managed custom node: FurgenVideoTools (downloaded)\n"
}

function provisioning_install_furgen_compat_nodes() {
    local compat_path script_dir local_source remote_url python_bin
    compat_path="${COMFYUI_DIR}/custom_nodes/furgen_video_compat_nodes.py"
    printf "Installing Furgen video compatibility nodes: %s\n" "$compat_path"
    mkdir -p "$(dirname "$compat_path")" || return 1

    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local_source="${script_dir}/custom_nodes/furgen_video_compat_nodes.py"
    if [[ -f "$local_source" ]]; then
        cp -f "$local_source" "$compat_path" || return 1
    else
        remote_url="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}/custom_nodes/furgen_video_compat_nodes.py"
        python_bin="$(command -v python3 || command -v python || true)"
        if [[ -z "$python_bin" ]]; then
            printf "ERROR: Python is required to download Furgen video compat nodes.\n"
            return 1
        fi
        "$python_bin" - "$remote_url" "$compat_path" <<'PY' || return 1
import pathlib
import sys
import urllib.request

url = sys.argv[1]
output_path = pathlib.Path(sys.argv[2])
output_path.parent.mkdir(parents=True, exist_ok=True)
request = urllib.request.Request(url, headers={"User-Agent": "furgen-video-gen-v2-bootstrap/1.0"})
with urllib.request.urlopen(request, timeout=60) as response:
    output_path.write_bytes(response.read())
PY
    fi

    python -m py_compile "$compat_path" || python3 -m py_compile "$compat_path" || return 1
}
function provisioning_get_nodes() {
    for repo in "${NODES[@]}"; do
        dir="$(node_dir_for_repo "$repo")"
        path="${COMFYUI_DIR}/custom_nodes/${dir}"
        requirements="${path}/requirements.txt"
        if [[ -d $path ]]; then
            if [[ ${AUTO_UPDATE,,} != "false" ]]; then
                printf "Updating node: %s...\n" "${repo}"
                ( cd "$path" && git config --global --add safe.directory "$(pwd)" && git pull )
            fi
            pin_node_if_requested "$dir" "$path"
            if [[ -e $requirements ]]; then
               pip install --no-cache-dir -r "$requirements"
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive
            pin_node_if_requested "$dir" "$path"
            if [[ -e $requirements ]]; then
                pip install --no-cache-dir -r "${requirements}"
            fi
        fi
    done
}

function provisioning_install_image_filters_opencv() {
    printf "Installing deterministic OpenCV package for ComfyUI-Image-Filters: %s\n" "${VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT}"
    if [[ "$(printf '%s' "${VIDEO_GEN_V2_IMAGE_FILTERS_UNINSTALL_CONFLICTING_OPENCV_VARIANTS:-true}" | tr '[:upper:]' '[:lower:]')" != "false" ]]; then
        pip uninstall -y opencv-python opencv-python-headless opencv-contrib-python-headless || true
    fi
    pip install --no-cache-dir "${VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT}" || return 1
    python - <<'PY' || return 1
import cv2
from cv2.ximgproc import guidedFilter

print(f"Verified cv2 ximgproc.guidedFilter import for ComfyUI-Image-Filters (cv2={cv2.__version__})")
PY
}

function provisioning_install_image_filters_nodes() {
    local repo dir path requirements
    repo="${VIDEO_GEN_V2_IMAGE_FILTERS_REPO}"
    dir="ComfyUI-Image-Filters"
    path="${COMFYUI_DIR}/custom_nodes/${dir}"
    requirements="${path}/requirements.txt"

    mkdir -p "${COMFYUI_DIR}/custom_nodes"
    if [[ -d "${path}" ]]; then
        printf "Updating node bundle: %s...\n" "${repo}"
        (
            cd "${path}" && \
            git config --global --add safe.directory "$(pwd)" && \
            git fetch --all --tags --prune
        ) || return 1
    else
        printf "Downloading node bundle: %s...\n" "${repo}"
        git clone "${repo}" "${path}" --recursive || return 1
    fi

    printf "Pinning %s to %s...\n" "${dir}" "${VIDEO_GEN_V2_IMAGE_FILTERS_PIN}"
    (
        cd "${path}" && git checkout --force "${VIDEO_GEN_V2_IMAGE_FILTERS_PIN}"
    ) || return 1

    if [[ -e "${requirements}" ]]; then
        printf "Skipping upstream %s; installing managed OpenCV dependency instead.\n" "${requirements}"
    fi
    provisioning_install_image_filters_opencv || return 1
}

function provisioning_install_requested_bundles() {
    if [[ "$#" -eq 0 ]]; then
        printf "ERROR: No bundle ids provided to install-bundles.\n"
        return 1
    fi

    local bundle_id
    for bundle_id in "$@"; do
        case "${bundle_id}" in
            video_gen_v2_image_filters_nodes)
                provisioning_install_image_filters_nodes || return 1
                ;;
            video_gen_v2_furgen_color_nodes)
                provisioning_install_furgen_video_tools_node || return 1
                ;;
            video_gen_v2_furgen_color_nodes_v2)
                provisioning_install_furgen_video_tools_node || return 1
                ;;
            video_gen_v2_ltx_context_windows)
                provisioning_install_ltx_context_windows || return 1
                ;;
            *)
                printf "ERROR: Unknown video_gen_v2 bundle id '%s'.\n" "${bundle_id}"
                return 1
                ;;
        esac
    done
}

# Best-effort: for all custom nodes with a requirements.txt,
# attempt to apply them again to cover cases where a VCS line
# (e.g., git+https) caused the resolver to abort before installing
# lightweight deps like piexif used by Impact-Pack.
function provisioning_ensure_node_requirements() {
    shopt -s nullglob
    local req
    for req in "${COMFYUI_DIR}"/custom_nodes/*/requirements.txt; do
        printf "Re-applying requirements: %s\n" "$req"
        pip install --no-cache-dir -r "$req" || true
    done
}

function provisioning_get_files() {
    if [[ -z $2 ]]; then return 1; fi
    
    dir="$1"
    mkdir -p "$dir"
    shift
    arr=("$@")
    printf "Downloading %s model(s) to %s...\n" "${#arr[@]}" "$dir"
    for url in "${arr[@]}"; do
        printf "Downloading: %s\n" "${url}"
        provisioning_download "${url}" "${dir}"
        printf "\n"
    done
}

function provisioning_print_header() {
    printf "\n##############################################\n#                                            #\n#          Provisioning container            #\n#                                            #\n#         This will take some time           #\n#                                            #\n# Your container will be ready on completion #\n#                                            #\n##############################################\n\n"
}

function provisioning_bootstrap_gate_enabled() {
    local enabled
    enabled="$(printf '%s' "${FURGEN_COMFYUI_BOOTSTRAP_GATE_ENABLED:-true}" | tr '[:upper:]' '[:lower:]')"
    [[ "$enabled" != "0" && "$enabled" != "false" ]]
}

function provisioning_hold_comfyui_start_gate() {
    provisioning_bootstrap_gate_enabled || return 0
    mkdir -p "$(dirname "${FURGEN_COMFYUI_START_ALLOWED_FILE}")" "${WORKSPACE}/ComfyUI/input" || true
    rm -f "${FURGEN_COMFYUI_START_ALLOWED_FILE}" "${WORKSPACE}/ComfyUI/input/provisioned_furry_all.txt" || true
    printf "Holding ComfyUI start gate until video_gen_v2 bootstrap finishes: %s\n" "${FURGEN_COMFYUI_START_ALLOWED_FILE}"
}

function provisioning_release_comfyui_start_gate() {
    provisioning_bootstrap_gate_enabled || return 0
    mkdir -p "$(dirname "${FURGEN_COMFYUI_START_ALLOWED_FILE}")" || true
    touch "${FURGEN_COMFYUI_START_ALLOWED_FILE}" || return 1
    printf "Released ComfyUI start gate: %s\n" "${FURGEN_COMFYUI_START_ALLOWED_FILE}"
}

function provisioning_print_end() {
    # The ComfyUI launch script writes this marker only after local Comfy
    # responds. Remove stale markers here so a failed launch cannot look ready.
    echo "Clearing stale provisioning completion marker..."
    mkdir -p "${WORKSPACE}/ComfyUI/input"
    rm -f "${WORKSPACE}/ComfyUI/input/provisioned_furry_all.txt"
    provisioning_release_comfyui_start_gate || return 1

    printf "\nProvisioning complete: Application will start now; readiness marker will be written after ComfyUI responds locally.\n\n"
}

function provisioning_has_valid_hf_token() {
    [[ -n "$HF_TOKEN" ]] || return 1
    url="https://huggingface.co/api/whoami-v2"

    # Avoid leaking tokens in logs if xtrace is enabled.
    local xtrace_was_on=0
    case "$-" in
        *x*) xtrace_was_on=1; set +x ;;
    esac

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $HF_TOKEN" \
        -H "Content-Type: application/json")

    [[ "$xtrace_was_on" -eq 1 ]] && set -x

    # Check if the token is valid
    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Best-effort check whether the token can access a given model repo.
# Returns:
# - 0: accessible (200)
# - 1: not accessible (401/403/429/other)
function provisioning_hf_can_access_repo() {
    local repo_id="$1"
    [[ -n "$HF_TOKEN" ]] || return 1
    local url="https://huggingface.co/api/models/${repo_id}"
    local code

    # Avoid leaking tokens in logs if xtrace is enabled.
    local xtrace_was_on=0
    case "$-" in
        *x*) xtrace_was_on=1; set +x ;;
    esac

    code="$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" -H "Authorization: Bearer $HF_TOKEN")"

    [[ "$xtrace_was_on" -eq 1 ]] && set -x
    [[ "$code" -eq 200 ]]
}

function provisioning_download_hf_snapshot() {
    local repo_id="$1"
    local dest_dir="$2"

    mkdir -p "$dest_dir"

    if [[ -z "$HF_TOKEN" ]]; then
        echo "WARN: HF_TOKEN is not set; skipping Hugging Face snapshot download for ${repo_id}"
        echo "      Set HF_TOKEN (or HUGGINGFACE_HUB_TOKEN) in your container environment and reprovision."
        return 0
    fi

    # Avoid leaking tokens in logs if xtrace is enabled.
    local xtrace_was_on=0
    case "$-" in
        *x*) xtrace_was_on=1; set +x ;;
    esac

    HF_REPO_ID="$repo_id" HF_DEST_DIR="$dest_dir" python - <<'PY'
import os
from huggingface_hub import snapshot_download

repo_id = os.environ["HF_REPO_ID"]
dest_dir = os.environ["HF_DEST_DIR"]
token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")

snapshot_download(
    repo_id=repo_id,
    local_dir=dest_dir,
    local_dir_use_symlinks=False,
    resume_download=True,
    token=token,
)
print(f"Downloaded {repo_id} -> {dest_dir}")
PY

    [[ "$xtrace_was_on" -eq 1 ]] && set -x
}

function provisioning_has_valid_civitai_token() {
    [[ -n "$CIVITAI_TOKEN" ]] || return 1
    url="https://civitai.com/api/v1/models?hidden=1&limit=1"

    # Avoid leaking tokens in logs if xtrace is enabled.
    local xtrace_was_on=0
    case "$-" in
        *x*) xtrace_was_on=1; set +x ;;
    esac

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $CIVITAI_TOKEN" \
        -H "Content-Type: application/json")

    [[ "$xtrace_was_on" -eq 1 ]] && set -x

    # Check if the token is valid
    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Download from $1 URL to $2 file path
function provisioning_download() {
    local auth_token=""
    if [[ -n $HF_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?huggingface\.co(/|$|\?) ]]; then
        auth_token="$HF_TOKEN"
    elif 
        [[ -n $CIVITAI_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?civitai\.com(/|$|\?) ]]; then
        auth_token="$CIVITAI_TOKEN"
    fi
    if [[ -n $auth_token ]];then
        # Avoid leaking tokens in logs if xtrace is enabled.
        local xtrace_was_on=0
        case "$-" in
            *x*) xtrace_was_on=1; set +x ;;
        esac
        wget --header="Authorization: Bearer $auth_token" -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
        [[ "$xtrace_was_on" -eq 1 ]] && set -x
    else
        wget -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    fi
}

function dependency_manager_is_disabled() {
    local dm_agent_disable
    dm_agent_disable="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    [[ "$dm_agent_disable" == "1" || "$dm_agent_disable" == "true" ]]
}

function dependency_manager_download_agent_url() {
    local url="$1"
    local output_path="$2"
    local python_bin

    python_bin="$(command -v python3 || true)"
    if [[ -z "$python_bin" && -x /venv/main/bin/python ]]; then
        python_bin="/venv/main/bin/python"
    fi
    if [[ -z "$python_bin" ]]; then
        printf "WARN: Dependency manager: no python interpreter available to download %s\n" "$url"
        return 1
    fi

    "$python_bin" - "$url" "$output_path" <<'PY'
import pathlib
import sys
import urllib.request

url = sys.argv[1]
output_path = pathlib.Path(sys.argv[2])
output_path.parent.mkdir(parents=True, exist_ok=True)
request = urllib.request.Request(url, headers={"User-Agent": "furgen-video-gen-v2-bootstrap/1.0"})
with urllib.request.urlopen(request, timeout=120) as response:
    output_path.write_bytes(response.read())
PY
}

function dependency_manager_start_agent() {
    # Allow opt-out.
    if dependency_manager_is_disabled; then
        echo "Dependency manager: DM_AGENT_DISABLE set; skipping agent start."
        return 0
    fi

    local agent_path log_path agent_url
    agent_path="${DM_AGENT_PATH:-${WORKSPACE}/dependency_agent_v1.py}"
    log_path="${DM_AGENT_LOG_PATH:-${WORKSPACE}/dependency_agent.log}"
    agent_url="${DM_AGENT_URL:-${AGENT_URL:-}}"

    # If already running, do nothing.
    if command -v pgrep >/dev/null 2>&1; then
        if pgrep -f "$agent_path" >/dev/null 2>&1; then
            echo "Dependency manager: agent already running ($agent_path)."
            return 0
        fi
    fi

    # Ensure base dirs exist (agent uses disk_usage on DM_COMFYUI_DIR, which must exist).
    mkdir -p "$(dirname "$agent_path")" || true
    mkdir -p "${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}" || true

    # Install agent to WORKSPACE (prefer explicit URL, else bundled copy, else GitHub raw fallback).
    if [[ -n "$agent_url" ]]; then
        echo "Dependency manager: downloading agent from DM_AGENT_URL/AGENT_URL."
        dependency_manager_download_agent_url "$agent_url" "$agent_path" || {
            echo "WARN: Dependency manager: failed to download agent from $agent_url"
            return 0
        }
    else
        local script_dir bundled_path fallback_url
        script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        bundled_path="${script_dir}/../scripts/dependency_agent_v1.py"
        if [[ -f "$bundled_path" ]]; then
            echo "Dependency manager: installing bundled agent from $bundled_path."
            cp -f "$bundled_path" "$agent_path" || {
                echo "WARN: Dependency manager: failed to copy bundled agent from $bundled_path"
                return 0
            }
        else
            fallback_url="https://raw.githubusercontent.com/Dodzilla/FurgenPub/main/docker/scripts/dependency_agent_v1.py"
            echo "Dependency manager: downloading agent from fallback URL ($fallback_url)."
            dependency_manager_download_agent_url "$fallback_url" "$agent_path" || {
                echo "WARN: Dependency manager: failed to download agent from fallback URL"
                return 0
            }
        fi
    fi

    chmod +x "$agent_path" || true

    # Start in background. Use bash -lc so template-injected env vars are visible (per docs).
    echo "Dependency manager: starting agent; log=$log_path"
    nohup bash -lc "source /venv/main/bin/activate && python3 '$agent_path' >> '$log_path' 2>&1" >/dev/null 2>&1 &
}

function dependency_manager_install_agent_artifact() {
    local agent_path agent_url script_dir bundled_path fallback_url
    agent_path="${DM_AGENT_PATH:-${WORKSPACE}/dependency_agent_v1.py}"
    agent_url="${DM_AGENT_URL:-${AGENT_URL:-}}"

    mkdir -p "$(dirname "$agent_path")" || true
    mkdir -p "${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}" || true

    if [[ -n "$agent_url" ]]; then
        echo "Dependency manager: downloading agent from DM_AGENT_URL/AGENT_URL."
        dependency_manager_download_agent_url "$agent_url" "$agent_path" || {
            echo "WARN: Dependency manager: failed to download agent from $agent_url"
            return 1
        }
    else
        script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        bundled_path="${script_dir}/../scripts/dependency_agent_v1.py"
        if [[ -f "$bundled_path" ]]; then
            echo "Dependency manager: installing bundled agent from $bundled_path."
            cp -f "$bundled_path" "$agent_path" || {
                echo "WARN: Dependency manager: failed to copy bundled agent from $bundled_path"
                return 1
            }
        else
            fallback_url="https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py"
            echo "Dependency manager: downloading agent from fallback URL ($fallback_url)."
            dependency_manager_download_agent_url "$fallback_url" "$agent_path" || {
                echo "WARN: Dependency manager: failed to download agent from fallback URL"
                return 1
            }
        fi
    fi

    chmod +x "$agent_path" || true
}

function dependency_manager_persist_agent_env() {
    local env_path key value xtrace_was_on
    env_path="${DM_AGENT_ENV_PATH:-${WORKSPACE}/dependency_agent.env}"

    mkdir -p "$(dirname "$env_path")" || true

    xtrace_was_on=0
    case "$-" in
        *x*) xtrace_was_on=1; set +x ;;
    esac

    : > "$env_path" || {
        [[ "$xtrace_was_on" -eq 1 ]] && set -x
        echo "WARN: Dependency manager: failed to write env file at $env_path"
        return 0
    }

    for key in \
        WORKSPACE \
        DM_COMFYUI_DIR \
        SERVER_TYPE \
        FCS_API_BASE_URL \
        DEPENDENCY_MANAGER_SHARED_SECRET \
        DM_INSTANCE_ID \
        DM_INSTANCE_IP \
        VAST_CONTAINERLABEL \
        DM_AGENT_DISABLE \
        DM_AGENT_PATH \
        DM_AGENT_LOG_PATH \
        DM_AGENT_PID_PATH \
        DM_AGENT_ENV_PATH \
        DM_AGENT_URL \
        AGENT_URL \
        DEPENDENCY_AGENT_TARGET_VERSION \
        DEPENDENCY_AGENT_RELEASE_VERSION \
        DEPENDENCY_AGENT_UPDATE_URL \
        DEPENDENCY_AGENT_PUBLIC_URL \
        DEPENDENCY_AGENT_UPDATE_SHA256 \
        DEPENDENCY_AGENT_RELEASE_SHA256 \
        DM_AGENT_WATCHDOG_PATH \
        DM_AGENT_WATCHDOG_LOG_PATH \
        DM_AGENT_WATCHDOG_PID_PATH \
        DM_AGENT_WATCHDOG_SECONDS \
        HF_TOKEN \
        CIVITAI_TOKEN \
        DM_LOCAL_COMFY_BASE_URL \
        DM_LOCAL_READINESS_FILE \
        FURGEN_COMFYUI_START_ALLOWED_FILE \
        FURGEN_COMFYUI_BOOTSTRAP_GATE_ENABLED \
        FURGEN_COMFYUI_BOOTSTRAP_GATE_TIMEOUT_SEC \
        COMFY_NODE_PINS \
        COMFYUI_PIN_COMMIT
    do
        if [[ "${!key+x}" == "x" ]]; then
            value="${!key}"
            printf 'export %s=%q\n' "$key" "$value" >> "$env_path" || true
        fi
    done

    printf 'export DM_AGENT_ENV_PATH=%q\n' "$env_path" >> "$env_path" || true
    chmod 600 "$env_path" || true
    [[ "$xtrace_was_on" -eq 1 ]] && set -x
}

function dependency_manager_render_watchdog() {
    local watchdog_path
    watchdog_path="${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.sh}"

    mkdir -p "$(dirname "$watchdog_path")" || true

    cat > "$watchdog_path" <<'EOF'
#!/bin/bash
# Consolidated dependency-agent supervisor — liveness-only.
#
# This is the single canonical source for the dependency-agent watchdog. It is
# stamped verbatim into every FurgenPub docker/support/*.sh watchdog heredoc by
# scripts/generate-support-watchdogs.js, replacing the 13 previously-divergent
# copies. Taking a change here to the fleet requires republishing the support
# scripts + reprovisioning (a baked watchdog cannot be hot-patched on a running
# instance).
#
# DESIGN — why this exists (see the 2026-07-06 dueling-watchdog incident):
#   1. ONE authority for the agent version: the SERVER. The agent self-updates
#      in-process (os.execv) from register/heartbeat responses. This supervisor
#      NEVER kills the agent for a version/sha mismatch — it only restarts a DEAD
#      agent. Removing version enforcement here is what eliminates the
#      "watchdog (baked pin A) fights agent (server pin B) -> N processes" leak.
#   2. NO baked version/sha to drift. The agent binary is fetched from a STABLE,
#      unversioned loader URL (the coordination /agent-releases redirect, which
#      resolves to the current release from the live config/agentRelease doc),
#      and ONLY when the file is missing. No target_version, no target_sha256.
#   3. Hard singleton via flock: at most one supervisor and one agent per host.
#      The agent runs under an exclusive lock that survives its os.execv
#      self-update, so a second agent can never start.
#
# Contrast with the OLD watchdog it replaces:
#   - OLD: target_version="${DEPENDENCY_AGENT_TARGET_VERSION:-...:-dm-agent-py/0.10.15}"
#          then killed the agent whenever running version/sha != target. REMOVED.
#   - OLD: re-downloaded from the baked (versioned) DM_AGENT_URL to "repair"
#          version drift. REMOVED (download only when the file is missing).
#   - OLD: pidfile/pgrep liveness with no mutual exclusion. REPLACED with flock.

set -u

WORKSPACE="${WORKSPACE:-/workspace}"
env_path="${DM_AGENT_ENV_PATH:-${WORKSPACE}/dependency_agent.env}"
if [[ -r "$env_path" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$env_path"
    set +a
fi
WORKSPACE="${WORKSPACE:-/workspace}"
DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
agent_path="${DM_AGENT_PATH:-${WORKSPACE}/dependency_agent_v1.py}"
log_path="${DM_AGENT_LOG_PATH:-${WORKSPACE}/dependency_agent.log}"
pid_path="${DM_AGENT_PID_PATH:-${WORKSPACE}/dependency_agent.pid}"
lock_dir="${DM_AGENT_LOCK_DIR:-${WORKSPACE}/.fcs/locks}"
watchdog_lock="${lock_dir}/dependency_agent_watchdog.lock"
agent_lock="${lock_dir}/dependency_agent.lock"
watchdog_pid_path="${DM_AGENT_WATCHDOG_PID_PATH:-${WORKSPACE}/dependency_agent_watchdog.pid}"
poll_seconds="${DM_AGENT_WATCHDOG_POLL_SECONDS:-15}"

# Loader for the missing-file bootstrap ONLY. This is not where version
# correctness comes from: whatever this fetches, the agent self-updates to the
# server-pinned version in-process right after it starts. So we just need a URL
# that yields a working agent. Priority: an explicit stable loader, then the
# baked DM_AGENT_URL/AGENT_URL, then the public main-branch raw file.
# (Deliberately NOT derived from FCS_API_BASE_URL: the /agent-releases redirect is
# served by the `api` function, not the coordination base FCS_API_BASE_URL points
# at, so that path would 404.)
loader_url="${DM_AGENT_LOADER_URL:-${DM_AGENT_URL:-${AGENT_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py}}}"

log() { echo "dependency-agent watchdog: $*"; }

is_disabled() {
    local v
    v="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    [[ "$v" == "1" || "$v" == "true" ]]
}

have_flock() { command -v flock >/dev/null 2>&1; }

# A live agent holds the exclusive agent_lock for its whole lifetime (including
# across its in-process self-update). If we can acquire the lock, no agent holds
# it. Fall back to pgrep/pidfile when flock is unavailable.
agent_running() {
    if have_flock && [[ -e "$agent_lock" ]]; then
        if flock -n "$agent_lock" true 2>/dev/null; then
            return 1
        fi
        return 0
    fi
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "$agent_path" >/dev/null 2>&1; then
        return 0
    fi
    if [[ -f "$pid_path" ]]; then
        local pid
        pid="$(cat "$pid_path" 2>/dev/null || true)"
        [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && return 0
    fi
    return 1
}

# Liveness-only: fetch the agent ONLY when the file is missing/empty. Version
# changes are the agent's own in-process self-update from the server — never a
# re-download to "correct" a version here.
download_agent_if_missing() {
    mkdir -p "$(dirname "$agent_path")" "${DM_COMFYUI_DIR}" 2>/dev/null || true
    if [[ -s "$agent_path" ]]; then
        chmod +x "$agent_path" 2>/dev/null || true
        return 0
    fi
    log "agent file missing; downloading from stable loader $loader_url"
    if curl -fsSL "$loader_url" -o "$agent_path"; then
        chmod +x "$agent_path" 2>/dev/null || true
        return 0
    fi
    log "WARN: failed to download agent from $loader_url"
    return 1
}

start_agent() {
    download_agent_if_missing || return 0
    log "starting agent; log=$log_path"
    # The agent runs under an exclusive flock. A second agent that races to start
    # fails the non-blocking lock and exits, so at most one agent ever runs. The
    # lock is held across the agent's os.execv self-update (the fd survives exec),
    # so there is no window for a duplicate during version transitions.
    if have_flock; then
        nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; exec flock -n '$agent_lock' python3 '$agent_path' >> '$log_path' 2>&1" >/dev/null 2>&1 &
    else
        nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; exec python3 '$agent_path' >> '$log_path' 2>&1" >/dev/null 2>&1 &
    fi
    echo $! > "$pid_path"
}

is_disabled && exit 0
mkdir -p "$lock_dir" 2>/dev/null || true

# Singleton supervisor: at most one supervise loop per host.
#  - With flock: hold an exclusive lock for the loop's lifetime.
#  - Without flock: fall back to a pidfile liveness guard (best-effort, matches
#    the legacy watchdog) so a second invocation still refuses to spawn a rival.
exec 9>"$watchdog_lock"
if have_flock; then
    if ! flock -n 9; then
        log "another supervisor holds the lock; exiting"
        exit 0
    fi
elif [[ -f "$watchdog_pid_path" ]]; then
    existing_pid="$(cat "$watchdog_pid_path" 2>/dev/null || true)"
    if [[ "$existing_pid" =~ ^[0-9]+$ ]] && kill -0 "$existing_pid" 2>/dev/null; then
        log "another supervisor (pid $existing_pid) is running; exiting"
        exit 0
    fi
fi
echo "$$" > "$watchdog_pid_path"

log "supervising (poll=${poll_seconds}s, loader=${loader_url})"
while true; do
    if ! is_disabled && ! agent_running; then
        start_agent
    fi
    sleep "$poll_seconds"
done
EOF

    chmod +x "$watchdog_path" || true
}

function dependency_manager_stop_stale_watchdogs() {
    local watchdog_path current_target current_url pid proc_target proc_url
    watchdog_path="$1"
    current_target="${DEPENDENCY_AGENT_TARGET_VERSION:-${DEPENDENCY_AGENT_RELEASE_VERSION:-}}"
    current_url="${DM_AGENT_URL:-${AGENT_URL:-${DEPENDENCY_AGENT_UPDATE_URL:-${DEPENDENCY_AGENT_PUBLIC_URL:-}}}}"

    if ! command -v pgrep >/dev/null 2>&1; then
        return 0
    fi

    pgrep -f "$watchdog_path" 2>/dev/null | while read -r pid; do
        [[ "$pid" =~ ^[0-9]+$ ]] || continue
        [[ "$pid" == "$$" ]] && continue

        proc_target=""
        proc_url=""
        if [[ -r "/proc/$pid/environ" ]]; then
            proc_target="$(tr '\0' '\n' < "/proc/$pid/environ" | awk -F= '$1=="DEPENDENCY_AGENT_TARGET_VERSION" || $1=="DEPENDENCY_AGENT_RELEASE_VERSION" {print $2; exit}' || true)"
            proc_url="$(tr '\0' '\n' < "/proc/$pid/environ" | awk -F= '$1=="DM_AGENT_URL" || $1=="AGENT_URL" || $1=="DEPENDENCY_AGENT_UPDATE_URL" || $1=="DEPENDENCY_AGENT_PUBLIC_URL" {print $2; exit}' || true)"
        fi

        if [[ -n "$current_target" && "$proc_target" != "$current_target" ]]; then
            echo "Dependency manager: stopping stale watchdog pid=$pid target=${proc_target:-unknown} expected=$current_target."
            kill "$pid" 2>/dev/null || true
            continue
        fi
        if [[ -n "$current_url" && "$proc_url" != "$current_url" ]]; then
            echo "Dependency manager: stopping stale watchdog pid=$pid url=${proc_url:-unknown}."
            kill "$pid" 2>/dev/null || true
        fi
    done

    sleep 1
}

function dependency_manager_configure_supervisor_watchdog() {
    local launch_script python_bin
    launch_script="/opt/supervisor-scripts/comfyui.sh"

    if [[ ! -f "$launch_script" ]]; then
        return 0
    fi

    if command -v python >/dev/null 2>&1; then
        python_bin="$(command -v python)"
    elif command -v python3 >/dev/null 2>&1; then
        python_bin="$(command -v python3)"
    else
        echo "WARN: Dependency manager: no python interpreter available to patch $launch_script"
        return 0
    fi

    "$python_bin" - "$launch_script" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")

block = (
    "# FURGEN dependency agent watchdog bootstrap\n"
    "dm_agent_env_path=\"${DM_AGENT_ENV_PATH:-${WORKSPACE:-/workspace}/dependency_agent.env}\"\n"
    "furgen_saved_comfyui_args=\"${COMFYUI_ARGS-}\"\n"
    "furgen_had_comfyui_args=0\n"
    "if [[ \"${COMFYUI_ARGS+x}\" == \"x\" ]]; then furgen_had_comfyui_args=1; fi\n"
    "if [[ -r \"${dm_agent_env_path}\" ]]; then\n"
    "    set -a\n"
    "    source \"${dm_agent_env_path}\"\n"
    "    set +a\n"
    "fi\n"
    "if [[ \"${furgen_had_comfyui_args}\" == \"1\" ]]; then\n"
    "    COMFYUI_ARGS=\"${furgen_saved_comfyui_args}\"\n"
    "else\n"
    "    unset COMFYUI_ARGS\n"
    "fi\n"
    "unset furgen_saved_comfyui_args furgen_had_comfyui_args\n"
    "dm_agent_disable=\"$(printf '%s' \"${DM_AGENT_DISABLE:-}\" | tr '[:upper:]' '[:lower:]')\"\n"
    "if [[ \"${dm_agent_disable}\" != \"1\" && \"${dm_agent_disable}\" != \"true\" ]]; then\n"
    "    watchdog_path=\"${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.sh}\"\n"
    "    watchdog_log_path=\"${DM_AGENT_WATCHDOG_LOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.log}\"\n"
    "    if [[ -x \"${watchdog_path}\" ]]; then\n"
    "        if ! command -v pgrep >/dev/null 2>&1 || ! pgrep -f \"${watchdog_path}\" >/dev/null 2>&1; then\n"
    "            if setsid --help 2>&1 | grep -q -- '--fork'; then\n"
    "                nohup setsid -f \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
    "            else\n"
    "                nohup setsid \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
    "            fi\n"
    "        fi\n"
    "    fi\n"
    "fi\n"
    "# /FURGEN dependency agent watchdog bootstrap\n"
)

pattern = re.compile(
    r"# FURGEN dependency agent watchdog bootstrap\n.*?# /FURGEN dependency agent watchdog bootstrap\n",
    re.DOTALL,
)
source = pattern.sub("", source)

anchor = "# Launch ComfyUI\n"
if anchor in source:
    insert_at = source.find(anchor)
else:
    launch_idx = source.find("python main.py")
    insert_at = source.rfind("\n", 0, launch_idx) + 1 if launch_idx != -1 else len(source)

patched = source[:insert_at] + block + source[insert_at:]

if patched != path.read_text(encoding="utf-8"):
    path.write_text(patched, encoding="utf-8")
    print("Applied dependency agent watchdog bootstrap patch.")
else:
    print("Dependency agent watchdog bootstrap already present.")
PY

    chmod +x "$launch_script" || true
}

function dependency_manager_start_agent() {
    local watchdog_path watchdog_log_path

    if dependency_manager_is_disabled; then
        echo "Dependency manager: DM_AGENT_DISABLE set; skipping agent start."
        return 0
    fi

    dependency_manager_install_agent_artifact || true
    dependency_manager_persist_agent_env
    dependency_manager_render_watchdog
    dependency_manager_configure_supervisor_watchdog

    watchdog_path="${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.sh}"
    watchdog_log_path="${DM_AGENT_WATCHDOG_LOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.log}"

    dependency_manager_stop_stale_watchdogs "$watchdog_path"

    if command -v pgrep >/dev/null 2>&1; then
        if pgrep -f "$watchdog_path" >/dev/null 2>&1; then
            echo "Dependency manager: watchdog already running ($watchdog_path)."
            return 0
        fi
    fi

    echo "Dependency manager: starting agent watchdog; log=$watchdog_log_path"
    if setsid --help 2>&1 | grep -q -- '--fork'; then
        nohup setsid -f "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
    else
        nohup setsid "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
    fi
}

function provisioning_start_comfyui_direct() {
    local comfy_dir direct_script log_path
    comfy_dir="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
    direct_script="${WORKSPACE}/start_comfy_direct.sh"
    log_path="${WORKSPACE}/comfyui_direct_start.log"

    if [[ ! -f "${comfy_dir}/main.py" ]]; then
        echo "WARN: Direct ComfyUI start skipped; main.py not found in ${comfy_dir}"
        return 1
    fi

    cat > "${direct_script}" <<'EOF'
#!/bin/bash
set -euo pipefail
export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
export COMFYUI_ARGS="--disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header --use-sage-attention"
export COMFYUI_ARGS
export COMFYUI_LOG_PATH="${COMFYUI_LOG_PATH:-${WORKSPACE}/comfyui_direct_start.log}"
mkdir -p "$(dirname "${COMFYUI_LOG_PATH}")" || true
exec > >(tee -a "${COMFYUI_LOG_PATH}" >/proc/1/fd/1) 2>&1
cd "${DM_COMFYUI_DIR}"
if [[ -f /venv/main/bin/activate ]]; then
    source /venv/main/bin/activate
fi
exec env LD_PRELOAD="${LD_PRELOAD:-libtcmalloc_minimal.so.4}" python main.py ${COMFYUI_ARGS}
EOF
    chmod +x "${direct_script}" || true
    echo "Starting ComfyUI directly: ${direct_script}"
    COMFYUI_LOG_PATH="${log_path}" nohup "${direct_script}" >/dev/null 2>&1 &
}

function provisioning_start_comfyui_logged_launch_script() {
    local launch_script log_path
    launch_script="$1"
    log_path="$2"

    nohup bash -lc '
set -o pipefail
launch_script="$1"
log_path="$2"
mkdir -p "$(dirname "$log_path")" || true
export SERVERLESS="${SERVERLESS:-true}"
bash "$launch_script" 2>&1 | tee -a "$log_path" >/proc/1/fd/1
' _ "$launch_script" "$log_path" >/dev/null 2>&1 &
}

function provisioning_wait_for_local_comfyui() {
    local attempts readiness_file ready_port
    attempts="${1:-90}"
    ready_port="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}"
    readiness_file="${WORKSPACE}/ComfyUI/input/${DM_LOCAL_READINESS_FILE:-provisioned_furry_all.txt}"

    for _ in $(seq 1 "${attempts}"); do
        if curl -fsS --max-time 2 "${ready_port%/}/queue" >/dev/null 2>&1 && \
           [[ -s "${VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH}" ]] && \
           ps -eo args= | grep -Eq '[p]ython([0-9.]+)? .*main\.py .*--use-sage-attention'; then
            mkdir -p "$(dirname "${readiness_file}")" || true
            echo "Provisioning completed with SageAttention2 and ComfyUI ready at $(date)" > "${readiness_file}" || true
            return 0
        fi
        sleep 2
    done
    return 1
}

function provisioning_start_comfyui_after_bootstrap() {
    local launch_script service_name
    launch_script="/opt/supervisor-scripts/comfyui.sh"
    provisioning_release_comfyui_start_gate || true

    if curl -fsS --max-time 2 "http://127.0.0.1:8188/queue" >/dev/null 2>&1 && \
       [[ -s "${VIDEO_GEN_V2_SAGEATTENTION_VERIFY_PATH}" ]] && \
       ps -eo args= | grep -Eq '[p]ython([0-9.]+)? .*main\.py .*--use-sage-attention'; then
        echo "ComfyUI is already locally reachable with SageAttention2 after provisioning."
        return 0
    fi

    if command -v supervisorctl >/dev/null 2>&1; then
        for service_name in comfyui comfy comfyui-server; do
            if supervisorctl restart "$service_name"; then
                echo "Requested ComfyUI supervisor restart for service: $service_name"
                return 0
            fi
            if supervisorctl start "$service_name"; then
                echo "Requested ComfyUI supervisor start for service: $service_name"
                return 0
            fi
        done
    fi

    if [[ -x "$launch_script" || -f "$launch_script" ]]; then
        echo "Starting ComfyUI launch script directly: $launch_script"
        provisioning_start_comfyui_logged_launch_script "$launch_script" "${WORKSPACE}/comfyui_manual_start.log"
        if provisioning_wait_for_local_comfyui 45; then
            echo "ComfyUI became locally reachable after launch script start."
            return 0
        fi
        echo "WARN: ComfyUI launch script did not become reachable; trying direct start."
        provisioning_start_comfyui_direct || true
        provisioning_wait_for_local_comfyui 90 || true
        return 0
    fi

    echo "WARN: ComfyUI launch script not found; trying direct start: $launch_script"
    provisioning_start_comfyui_direct || true
    provisioning_wait_for_local_comfyui 90 || true
    return 0
}

case "${1:-}" in
    install-bundles)
        shift
        provisioning_install_requested_bundles "$@" || {
            echo "ERROR: video_gen_v2 bundle installation failed."
            exit 1
        }
        exit 0
        ;;
esac

# Start the dependency manager agent before package/provisioning work. The agent
# resolves its downloader at use time, so a later aria2 install is still picked
# up before queued dependency downloads begin.
dependency_manager_start_agent

# Best-effort aria2 install so model downloads can use multi-connection
# transfers (the agent falls back to wget when aria2c is absent).
if ! command -v aria2c >/dev/null 2>&1; then
    echo "Installing aria2 for multi-connection downloads..."
    apt_runner=""
    if command -v sudo >/dev/null 2>&1; then apt_runner="sudo"; fi
    ($apt_runner apt-get update -qq >/dev/null 2>&1 || true) && \
        $apt_runner apt-get install -y -qq aria2 >/dev/null 2>&1 || \
        echo "WARN: aria2 install failed; dependency agent will fall back to wget."
fi

provisioning_status=0

# Allow user to disable provisioning if they started with a script they didn't want.
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start || provisioning_status=$?
else
    provisioning_release_comfyui_start_gate || true
fi

# Re-apply the watchdog bootstrap after provisioning in case image startup scripts
# were regenerated while ComfyUI or custom nodes were updated.
dependency_manager_start_agent
if [[ "$provisioning_status" -ne 0 ]]; then
    echo "ERROR: video_gen_v2 provisioning failed with exit code ${provisioning_status}; leaving ComfyUI start gate closed."
    exit "$provisioning_status"
fi
provisioning_start_comfyui_after_bootstrap

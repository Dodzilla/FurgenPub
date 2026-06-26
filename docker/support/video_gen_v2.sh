#!/bin/bash

set -x

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"
export SERVER_TYPE="${SERVER_TYPE:-video_gen_v2}"
FURGENPUB_RAW_BASE_URL="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}"
VIDEO_GEN_V2_IMAGE_FILTERS_REPO="${VIDEO_GEN_V2_IMAGE_FILTERS_REPO:-https://github.com/spacepxl/ComfyUI-Image-Filters}"
VIDEO_GEN_V2_IMAGE_FILTERS_PIN="${VIDEO_GEN_V2_IMAGE_FILTERS_PIN:-bbb3fb0045461adf3602faeedaf40af57090d4e2}"
VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT="${VIDEO_GEN_V2_IMAGE_FILTERS_OPENCV_REQUIREMENT:-opencv-contrib-python==4.10.0.84}"

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
    "sageattention"
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

function provisioning_start() {
    local soft_failures=0

    provisioning_print_header
    provisioning_configure_comfyui_launch_args || true
    provisioning_update_comfyui
    provisioning_get_apt_packages
    load_node_pins_from_env
    provisioning_get_nodes || {
        printf "WARN: Provisioning step 'provisioning_get_nodes' failed with exit code %s; continuing.\n" "$?"
        soft_failures=1
    }
    provisioning_install_furgen_video_tools_node || return 1
    # Safety pass: re-apply any per-node requirements and ensure Impact-Pack deps
    provisioning_ensure_node_requirements
    provisioning_get_pip_packages || {
        printf "WARN: Provisioning step 'provisioning_get_pip_packages' failed with exit code %s; continuing.\n" "$?"
        soft_failures=1
    }
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

block = (
    "# FURGEN ComfyUI launch args normalization\n"
    "COMFYUI_ARGS=${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header}\n"
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
    "unset furgen_comfyui_port\n"
    "# Bypass Vast's unbuffer-based pty wrapper for Comfy. The wrapper can exit\n"
    "# cleanly while long GPU jobs are still running, causing supervisor to\n"
    "# restart Comfy and strand queued_on_comfy jobs.\n"
    "export DISABLE_PTY=\"${DISABLE_PTY:-true}\"\n"
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

function provisioning_fix_python_compatibility() {
    printf "Enforcing video_gen_v2 Python compatibility pins...\n"
    pip install --no-cache-dir "kornia<0.8" || return 1
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

    if [[ -d "${src_dir}" && -f "${src_dir}/furgen_video_tools.py" ]] && grep -q "FurgenTemporalUnsharpMask" "${src_dir}/furgen_video_tools.py"; then
        cp -R "${src_dir}/." "${dest_dir}/"
        printf "Installed managed custom node: FurgenVideoTools (local copy)\n"
        return 0
    elif [[ -d "${src_dir}" ]]; then
        printf "WARN: Local FurgenVideoTools source is missing FurgenTemporalUnsharpMask; using pinned remote copy.\n"
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
    if ! grep -q "FurgenTemporalUnsharpMask" "${dest_dir}/furgen_video_tools.py"; then
        printf "ERROR: Downloaded FurgenVideoTools implementation is missing FurgenTemporalUnsharpMask from %s\n" "${remote_base}"
        return 1
    fi

    printf "Installed managed custom node: FurgenVideoTools (downloaded)\n"
}

function provisioning_install_furgen_compat_nodes() {
    local compat_path
    compat_path="${COMFYUI_DIR}/custom_nodes/furgen_video_compat_nodes.py"
    printf "Installing Furgen video compatibility nodes: %s\n" "$compat_path"
    mkdir -p "$(dirname "$compat_path")" || return 1
    cat > "$compat_path" <<'PY'
import io

import numpy as np
import requests
import torch
from PIL import Image, ImageOps


class AnyType(str):
    def __ne__(self, other):
        return False


ANY_TYPE = AnyType("*")


class ImpactExecutionOrderController:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "signal": (ANY_TYPE,),
                "value": (ANY_TYPE,),
            }
        }

    RETURN_TYPES = (ANY_TYPE, ANY_TYPE)
    RETURN_NAMES = ("signal", "value")
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, signal, value):
        return signal, value


class EZLoadImgFromUrlNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"url": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("IMAGE", "MASK")
    RETURN_NAMES = ("image", "mask")
    FUNCTION = "load"
    CATEGORY = "Furgen/compat"

    def load(self, url):
        response = requests.get(url, timeout=45)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content))
        image = ImageOps.exif_transpose(image).convert("RGBA")

        rgba = np.array(image).astype(np.float32) / 255.0
        rgb = torch.from_numpy(rgba[:, :, :3])[None,]
        mask = torch.from_numpy(1.0 - rgba[:, :, 3])[None,]
        return rgb, mask


NODE_CLASS_MAPPINGS = {
    "ImpactExecutionOrderController": ImpactExecutionOrderController,
    "EZLoadImgFromUrlNode": EZLoadImgFromUrlNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ImpactExecutionOrderController": "Execution Order Controller",
    "EZLoadImgFromUrlNode": "Load Img From URL (EZ)",
}
PY
    python -m py_compile "$compat_path" || return 1
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

function provisioning_print_end() {
    # Create provisioning completion marker
    echo "Creating provisioning completion marker..."
    mkdir -p "${WORKSPACE}/ComfyUI/input"
    echo "Provisioning completed at $(date)" > "${WORKSPACE}/ComfyUI/input/provisioned_furry_all.txt"

    printf "\nProvisioning complete:  Application will start now\n\n"
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
    local env_path key value
    env_path="${DM_AGENT_ENV_PATH:-${WORKSPACE}/dependency_agent.env}"

    mkdir -p "$(dirname "$env_path")" || true
    : > "$env_path" || {
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
        COMFYUI_ARGS \
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
}

function dependency_manager_render_watchdog() {
    local watchdog_path
    watchdog_path="${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.sh}"

    mkdir -p "$(dirname "$watchdog_path")" || true

    cat > "$watchdog_path" <<'EOF'
#!/bin/bash

set -u

WORKSPACE="${WORKSPACE:-/workspace}"
DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
agent_path="${DM_AGENT_PATH:-${WORKSPACE}/dependency_agent_v1.py}"
log_path="${DM_AGENT_LOG_PATH:-${WORKSPACE}/dependency_agent.log}"
pid_path="${DM_AGENT_PID_PATH:-${WORKSPACE}/dependency_agent.pid}"
watchdog_pid_path="${DM_AGENT_WATCHDOG_PID_PATH:-${WORKSPACE}/dependency_agent_watchdog.pid}"
agent_url="${DM_AGENT_URL:-${AGENT_URL:-}}"
if [[ -z "$agent_url" ]]; then
    agent_url="${DEPENDENCY_AGENT_UPDATE_URL:-${DEPENDENCY_AGENT_PUBLIC_URL:-}}"
fi
target_version="${DEPENDENCY_AGENT_TARGET_VERSION:-${DEPENDENCY_AGENT_RELEASE_VERSION:-dm-agent-py/0.10.15}}"
target_sha256="${DEPENDENCY_AGENT_UPDATE_SHA256:-${DEPENDENCY_AGENT_RELEASE_SHA256:-}}"
fallback_url="https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py"

dependency_manager_is_disabled() {
    local dm_agent_disable
    dm_agent_disable="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    [[ "$dm_agent_disable" == "1" || "$dm_agent_disable" == "true" ]]
}

dependency_manager_agent_running() {
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "$agent_path" >/dev/null 2>&1; then
        return 0
    fi

    if [[ -f "$pid_path" ]]; then
        local pid
        pid="$(cat "$pid_path" 2>/dev/null || true)"
        if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi

    return 1
}

dependency_manager_python_bin() {
    local python_bin
    python_bin="$(command -v python3 || true)"
    if [[ -z "$python_bin" && -x /venv/main/bin/python ]]; then
        python_bin="/venv/main/bin/python"
    fi
    printf '%s' "$python_bin"
}

dependency_manager_agent_version() {
    local path="$1" python_bin
    python_bin="$(dependency_manager_python_bin)"
    if [[ -z "$python_bin" || ! -s "$path" ]]; then
        return 1
    fi
    "$python_bin" - "$path" <<'PY'
import pathlib
import re
import sys

try:
    text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
except Exception:
    sys.exit(1)
match = re.search(r'^\s*AGENT_VERSION\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
if not match:
    sys.exit(1)
print(match.group(1).strip())
PY
}

dependency_manager_agent_sha256() {
    local path="$1" python_bin
    python_bin="$(dependency_manager_python_bin)"
    if [[ -z "$python_bin" || ! -s "$path" ]]; then
        return 1
    fi
    "$python_bin" - "$path" <<'PY'
import hashlib
import pathlib
import sys

try:
    print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
except Exception:
    sys.exit(1)
PY
}

dependency_manager_agent_is_stale() {
    if [[ ! -s "$agent_path" ]]; then
        return 0
    fi
    if [[ -n "$target_version" ]]; then
        local current_version
        current_version="$(dependency_manager_agent_version "$agent_path" 2>/dev/null || true)"
        if [[ "$current_version" != "$target_version" ]]; then
            echo "Dependency manager: agent version stale current=${current_version:-unknown} target=$target_version."
            return 0
        fi
    fi
    if [[ "$target_sha256" =~ ^[0-9a-fA-F]{64}$ ]]; then
        local current_sha
        current_sha="$(dependency_manager_agent_sha256 "$agent_path" 2>/dev/null || true)"
        if [[ "${current_sha,,}" != "${target_sha256,,}" ]]; then
            echo "Dependency manager: agent SHA stale current=${current_sha:-unknown} target=${target_sha256,,}."
            return 0
        fi
    fi
    return 1
}

dependency_manager_stop_agent() {
    local pid
    if [[ -f "$pid_path" ]]; then
        pid="$(cat "$pid_path" 2>/dev/null || true)"
        if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    fi
    if command -v pgrep >/dev/null 2>&1; then
        pgrep -f "$agent_path" 2>/dev/null | while read -r pid; do
            if [[ "$pid" =~ ^[0-9]+$ ]]; then
                kill "$pid" 2>/dev/null || true
            fi
        done
    fi
    sleep 2
    if command -v pgrep >/dev/null 2>&1; then
        pgrep -f "$agent_path" 2>/dev/null | while read -r pid; do
            if [[ "$pid" =~ ^[0-9]+$ ]]; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        done
    fi
    rm -f "$pid_path" || true
}

dependency_manager_download_agent_url() {
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
request = urllib.request.Request(url, headers={"User-Agent": "furgen-video-gen-v2-watchdog/1.0"})
with urllib.request.urlopen(request, timeout=120) as response:
    output_path.write_bytes(response.read())
PY
}

dependency_manager_install_agent_if_missing() {
    mkdir -p "$(dirname "$agent_path")" || true
    mkdir -p "${DM_COMFYUI_DIR}" || true

    if [[ -s "$agent_path" ]] && ! dependency_manager_agent_is_stale; then
        chmod +x "$agent_path" || true
        return 0
    fi

    if [[ -s "$agent_path" ]]; then
        echo "Dependency manager: watchdog repairing stale agent at $agent_path."
    fi

    if [[ -n "$agent_url" ]]; then
        echo "Dependency manager: watchdog downloading agent from DM_AGENT_URL/AGENT_URL."
        dependency_manager_download_agent_url "$agent_url" "$agent_path" || {
            echo "WARN: Dependency manager: watchdog failed to download agent from $agent_url"
            return 1
        }
    else
        echo "Dependency manager: watchdog downloading agent from fallback URL ($fallback_url)."
        dependency_manager_download_agent_url "$fallback_url" "$agent_path" || {
            echo "WARN: Dependency manager: watchdog failed to download agent from fallback URL"
            return 1
        }
    fi

    chmod +x "$agent_path" || true
}

dependency_manager_start_agent_once() {
    if dependency_manager_agent_running; then
        if dependency_manager_agent_is_stale; then
            dependency_manager_install_agent_if_missing || return 0
            echo "Dependency manager: watchdog restarting stale dependency agent."
            dependency_manager_stop_agent
        else
            return 0
        fi
    fi

    dependency_manager_install_agent_if_missing || return 0

    echo "Dependency manager: watchdog starting agent; log=$log_path"
    nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; python3 '$agent_path' >> '$log_path' 2>&1" >/dev/null 2>&1 &
    echo $! > "$pid_path"
}

if dependency_manager_is_disabled; then
    exit 0
fi

mkdir -p "$(dirname "$watchdog_pid_path")" || true
if [[ -f "$watchdog_pid_path" ]]; then
    existing_pid="$(cat "$watchdog_pid_path" 2>/dev/null || true)"
    if [[ "$existing_pid" =~ ^[0-9]+$ ]] && kill -0 "$existing_pid" 2>/dev/null; then
        echo "Dependency manager: watchdog already running with pid=$existing_pid."
        exit 0
    fi
fi

echo $$ > "$watchdog_pid_path"
cleanup() {
    if [[ -f "$watchdog_pid_path" ]] && [[ "$(cat "$watchdog_pid_path" 2>/dev/null || true)" == "$$" ]]; then
        rm -f "$watchdog_pid_path"
    fi
}
trap cleanup EXIT INT TERM

dependency_manager_start_agent_once
while true; do
    sleep "${DM_AGENT_WATCHDOG_SECONDS:-15}"
    dependency_manager_start_agent_once
done
EOF

    chmod +x "$watchdog_path" || true
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
    "if [[ -r \"${dm_agent_env_path}\" ]]; then\n"
    "    set -a\n"
    "    source \"${dm_agent_env_path}\"\n"
    "    set +a\n"
    "fi\n"
    "dm_agent_disable=\"$(printf '%s' \"${DM_AGENT_DISABLE:-}\" | tr '[:upper:]' '[:lower:]')\"\n"
    "if [[ \"${dm_agent_disable}\" != \"1\" && \"${dm_agent_disable}\" != \"true\" ]]; then\n"
    "    watchdog_path=\"${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.sh}\"\n"
    "    watchdog_log_path=\"${DM_AGENT_WATCHDOG_LOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent_watchdog.log}\"\n"
    "    if [[ -x \"${watchdog_path}\" ]]; then\n"
    "        if ! command -v pgrep >/dev/null 2>&1 || ! pgrep -f \"${watchdog_path}\" >/dev/null 2>&1; then\n"
    "            nohup \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
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

    if command -v pgrep >/dev/null 2>&1; then
        if pgrep -f "$watchdog_path" >/dev/null 2>&1; then
            echo "Dependency manager: watchdog already running ($watchdog_path)."
            return 0
        fi
    fi

    echo "Dependency manager: starting agent watchdog; log=$watchdog_log_path"
    nohup "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
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

# Allow user to disable provisioning if they started with a script they didn't want
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start
fi

# Re-apply the watchdog bootstrap after provisioning in case image startup scripts
# were regenerated while ComfyUI or custom nodes were updated.
dependency_manager_start_agent

#!/bin/bash

set -x

source /venv/main/bin/activate
COMFYUI_DIR=${WORKSPACE}/ComfyUI

# NOTE:
# - Do NOT put Hugging Face tokens in this file (or in git clone URLs).
# - Export `HF_TOKEN` (or `HUGGINGFACE_HUB_TOKEN`) in the container environment instead.
# - If you get HTTP 403 for Gemma repos, you likely need to accept the model license on Hugging Face first.

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
)

PIP_PACKAGES=(
    "flash_attn"
    "triton"
    "sageattention"
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

    # Video processing nodes
    "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite"
    "https://github.com/Fannovel16/ComfyUI-Frame-Interpolation"
    "https://github.com/city96/ComfyUI-GGUF"
    "https://github.com/Lightricks/ComfyUI-LTXVideo"

    # Helper nodes
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/ltdrdata/ComfyUI-Impact-Subpack"
    "https://github.com/rgthree/rgthree-comfy"
    "https://github.com/pythongosssss/ComfyUI-Custom-Scripts"
    "https://github.com/WASasquatch/was-node-suite-comfyui"

    # WanVideo nodes
    "https://github.com/kijai/ComfyUI-WanVideoWrapper"
    "https://github.com/kijai/ComfyUI-KJNodes"

    # Furry/ControlNet nodes
    "https://github.com/Fannovel16/comfyui_controlnet_aux"
    "https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/Dodzilla/ComfyUI-ComfyCouple"
    "https://github.com/Dodzilla/LoopsGroundingDino"

    # Other nodes
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
    "https://github.com/evanspearman/ComfyMath"
)

WORKFLOWS=(
)

CHECKPOINT_MODELS=(
    #"https://huggingface.co/LoopsBoops/furarch/resolve/main/yiffymix_v62Noobxl.safetensors"
    "https://huggingface.co/Lightricks/LTX-2/resolve/main/ltx-2-19b-dev-fp8.safetensors"
)

BBOX_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/face_yolov8m.pt"
)

UNET_MODELS=(
)

GROUNDING_MODELS=(
)

LORA_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/FurryRealism.safetensors"
)

VAE_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1_VAE_fp32.safetensors"
)

TEXT_ENCODERS_MODELS=(
    #"https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/umt5-xxl-enc-fp8_e4m3fn.safetensors"
)

UPSCALE_MODELS=(
    "https://huggingface.co/ai-forever/Real-ESRGAN/resolve/main/RealESRGAN_x2.pth"
)

CONTROLNET_MODELS=(
    #"https://huggingface.co/LoopsBoops/furarch/resolve/main/xinsir_controlnet_promax.safetensors"
)

# Added arrays to mirror wan_video_full.sh
DIFFUSION_MODELS=(
)

CLIPVISION_MODELS=(
    #"https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/clip_vision/clip_vision_h.safetensors"
)

LATENT_UPSCALE_MODELS=(
    "https://huggingface.co/Lightricks/LTX-2/resolve/main/ltx-2-spatial-upscaler-x2-1.0.safetensors"
)

FRAME_INTERPOLATION_MODELS=(
    "https://huggingface.co/nguu/film-pytorch/resolve/887b2c42bebcb323baf6c3b6d59304135699b575/film_net_fp32.pt"
)

# Hugging Face repo snapshots (download the whole repo into a folder).
# Used for LLM/GGUF/etc where a single "resolve/main/file" URL isn't enough.
#
# Override via env:
# - `GEMMA_REPO_ID` (default below)
# - `GEMMA_DEST_DIR` (default below)
GEMMA_REPO_ID="${GEMMA_REPO_ID:-google/gemma-3-12b-it-qat-q4_0-unquantized}"
GEMMA_DEST_DIR="${GEMMA_DEST_DIR:-${COMFYUI_DIR}/models/text_encoders/${GEMMA_REPO_ID##*/}}"
GEMMA_DOWNLOAD="${GEMMA_DOWNLOAD:-true}"

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

function provisioning_update_comfyui() {
    echo "DEBUG: Checking for ComfyUI git repository in ${COMFYUI_DIR}"
    if [[ -d "${COMFYUI_DIR}/.git" ]]; then
        printf "Updating ComfyUI to pinned version (3cd7b32)...\n"
        (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch
            echo "DEBUG: Checking out pinned commit..."
            git checkout 3cd7b32f1b7e7e90395cefe7d9f9b1f89276d8ce
        )
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
    provisioning_print_header
    provisioning_update_comfyui
    provisioning_get_apt_packages
    load_node_pins_from_env
    provisioning_get_nodes
    # Safety pass: re-apply any per-node requirements and ensure Impact-Pack deps
    provisioning_ensure_node_requirements
    provisioning_get_pip_packages
    provisioning_download_gemma_repo
    provisioning_get_files \
        "${COMFYUI_DIR}/models/checkpoints" \
        "${CHECKPOINT_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/ultralytics/bbox" \
        "${BBOX_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/unet" \
        "${UNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/loras" \
        "${LORA_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/controlnet" \
        "${CONTROLNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/vae" \
        "${VAE_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/text_encoders" \
        "${TEXT_ENCODERS_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/upscale_models" \
        "${UPSCALE_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/diffusion_models" \
        "${DIFFUSION_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/clip_vision" \
        "${CLIPVISION_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/frame_interpolation" \
        "${FRAME_INTERPOLATION_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/latent_upscale_models" \
        "${LATENT_UPSCALE_MODELS[@]}"
    provisioning_print_end
}

function provisioning_get_apt_packages() {
    if [[ -n $APT_PACKAGES ]]; then
            sudo $APT_INSTALL ${APT_PACKAGES[@]}
    fi
}

function provisioning_get_pip_packages() {
    if [[ -n $PIP_PACKAGES ]]; then
            pip install --no-cache-dir ${PIP_PACKAGES[@]}
    fi
}

function provisioning_get_nodes() {
    for repo in "${NODES[@]}"; do
        dir="${repo##*/}"
        dir="${dir%.git}"
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

function provisioning_download_gemma_repo() {
    local repo_id="$GEMMA_REPO_ID"
    local dest_dir="$GEMMA_DEST_DIR"

    if [[ "${GEMMA_DOWNLOAD,,}" == "false" || "${GEMMA_DOWNLOAD}" == "0" ]]; then
        echo "Skipping Gemma download (GEMMA_DOWNLOAD=${GEMMA_DOWNLOAD})."
        return 0
    fi

    echo "Preparing Gemma download: ${repo_id} -> ${dest_dir}"

    if [[ -z "$HF_TOKEN" ]]; then
        echo "WARN: HF_TOKEN not set; skipping Gemma download."
        echo "      Set HF_TOKEN (or HUGGINGFACE_HUB_TOKEN) in your container environment."
        return 0
    fi

    # Quick hint for common 403 cause (gated models).
    if ! provisioning_hf_can_access_repo "$repo_id"; then
        echo "WARN: Cannot confirm access to ${repo_id} with current HF_TOKEN."
        echo "      If you see HTTP 403, open the model page on Hugging Face and accept the license/terms for this repo."
        echo "      If you see HTTP 429, your egress IP may be rate-limited; changing outbound IP is the usual fix."
    fi

    provisioning_download_hf_snapshot "$repo_id" "$dest_dir"
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

# Allow user to disable provisioning if they started with a script they didn't want
echo "DEBUG: Checking for /.noprovisioning file..."
if [[ ! -f /.noprovisioning ]]; then
    echo "DEBUG: /.noprovisioning not found. Starting provisioning process."
    provisioning_start
else
    echo "DEBUG: /.noprovisioning found. Skipping provisioning."
fi

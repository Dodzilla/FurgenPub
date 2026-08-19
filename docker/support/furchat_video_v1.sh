#!/bin/bash

set -x

export WORKSPACE="${WORKSPACE:-/workspace}"

source /venv/main/bin/activate
COMFYUI_DIR="${WORKSPACE}/ComfyUI"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$COMFYUI_DIR}"

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
    "https://github.com/ClownsharkBatwing/RES4LYF"
)

WORKFLOWS=(
)

CHECKPOINT_MODELS=(
    #"https://huggingface.co/LoopsBoops/furarch/resolve/main/yiffymix_v62Noobxl.safetensors"
    "https://huggingface.co/Lightricks/LTX-2/resolve/main/ltx-2-19b-dev.safetensors"
)

BBOX_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/face_yolov8m.pt"
)

UNET_MODELS=(
)

GROUNDING_MODELS=(
)

LORA_MODELS=(
    #"https://huggingface.co/LoopsBoops/furarch/resolve/main/FurryRealism.safetensors"
    "https://huggingface.co/Lightricks/LTX-2/resolve/main/ltx-2-19b-distilled-lora-384.safetensors"
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

function dependency_manager_start_agent() {
    # Allow opt-out.
    local dm_agent_disable
    dm_agent_disable="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$dm_agent_disable" == "1" || "$dm_agent_disable" == "true" ]]; then
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
        curl -fsSL "$agent_url" -o "$agent_path" || {
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
            curl -fsSL "$fallback_url" -o "$agent_path" || {
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
        curl -fsSL "$agent_url" -o "$agent_path" || {
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
            curl -fsSL "$fallback_url" -o "$agent_path" || {
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

# Identity-verified liveness for a candidate pid.
#
# `kill -0` alone is NOT sufficient and caused a permanent supervisor wedge on
# 2026-08-19 (video_gen_v4 instance 48062383): the pidfile held a pid that had
# been recycled as a THREAD id of ComfyUI (which spawns 200+ threads the moment
# it starts, immediately after the agent is launched). `kill -0` succeeds
# against a thread id, so the supervisor concluded a dead agent was alive and
# stopped restarting it — the box sat with no agent until it was rebooted.
#
# `pgrep -f "$agent_path"` has the mirror-image problem: it matches ANY process
# whose command line merely mentions the path (an ssh command, a grep, an
# editor), which also reads as a false "agent is alive".
#
# So a pid only counts as the agent when its own /proc cmdline is a python
# invocation of the agent path. Threads share the parent's cmdline, so a
# recycled thread id is rejected: ComfyUI's cmdline never names the agent.
pid_is_agent_process() {
    local pid="$1" cmdline
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    # No procfs (non-Linux image): degrade to bare liveness rather than
    # refusing to ever see a running agent.
    [[ -r "/proc/$pid/cmdline" ]] || return 0
    cmdline="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
    [[ "$cmdline" == *python* && "$cmdline" == *"$agent_path"* ]]
}

# A live agent holds the exclusive agent_lock for its whole lifetime (including
# across its in-process self-update). If we can acquire the lock, no agent holds
# it. Fall back to identity-verified pgrep/pidfile when flock is unavailable.
agent_running() {
    if have_flock && [[ -e "$agent_lock" ]]; then
        if flock -n "$agent_lock" true 2>/dev/null; then
            return 1
        fi
        return 0
    fi
    if command -v pgrep >/dev/null 2>&1; then
        local candidate
        while read -r candidate; do
            pid_is_agent_process "$candidate" && return 0
        done < <(pgrep -f -- "$agent_path" 2>/dev/null || true)
    fi
    if [[ -f "$pid_path" ]]; then
        local pid
        pid="$(cat "$pid_path" 2>/dev/null || true)"
        pid_is_agent_process "$pid" && return 0
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

    local dm_agent_disable
    dm_agent_disable="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$dm_agent_disable" == "1" || "$dm_agent_disable" == "true" ]]; then
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
    if setsid --help 2>&1 | grep -q -- '--fork'; then
        nohup setsid -f "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
    else
        nohup setsid "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
    fi
}

# Start the dependency manager agent (best-effort; safe if required env vars are missing).
dependency_manager_start_agent

# Allow user to disable provisioning if they started with a script they didn't want
echo "DEBUG: Checking for /.noprovisioning file..."
if [[ ! -f /.noprovisioning ]]; then
    echo "DEBUG: /.noprovisioning not found. Starting provisioning process."
    provisioning_start
else
    echo "DEBUG: /.noprovisioning found. Skipping provisioning."
fi

# Re-apply the watchdog bootstrap after provisioning in case image startup scripts
# were regenerated while ComfyUI or custom nodes were updated.
dependency_manager_start_agent

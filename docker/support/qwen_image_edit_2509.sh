#!/bin/bash

# Activate Python venv for ComfyUI
source /venv/main/bin/activate
COMFYUI_DIR="${WORKSPACE}/ComfyUI"

# Define packages (if any) and custom nodes (no new nodes needed for Qwen beyond core)
APT_PACKAGES=(
)
PIP_PACKAGES=(
)
NODES=(
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
)
WORKFLOWS=()

# Model lists – add Qwen models to appropriate categories
CHECKPOINT_MODELS=(
    # (optional) add other SD checkpoints if needed
)
BBOX_MODELS=(
    # (optional) e.g. YOLO face bbox
)
EMBEDDING_MODELS=(
    # (optional) textual inversions
)
UNET_MODELS=(
    # Qwen Image Edit 2509 diffusion model (UNet) – FP8
    "https://huggingface.co/lightx2v/Qwen-Image-Lightning/resolve/main/Qwen-Image/qwen_image_fp8_e4m3fn_scaled.safetensors"
)
TEXT_ENCODER_MODELS=(
    # Qwen 2.5 VL (7B) text encoder FP8
    "https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI/resolve/main/split_files/text_encoders/qwen_2.5_vl_7b_fp8_scaled.safetensors"
)
VAE_MODELS=(
    # Qwen Image VAE
    "https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI/resolve/main/split_files/vae/qwen_image_vae.safetensors"
)
LORA_MODELS=(
    # (optional) Qwen-Image-Edit Lightning LoRA for faster convergence (8-step version)
    "https://huggingface.co/lightx2v/Qwen-Image-Lightning/resolve/main/Qwen-Image-Lightning-4steps-V2.0.safetensors"
)

CONTROLNET_MODELS=(
    # (any ControlNet models you use, unchanged)
)
SAM2_MODELS=(
    # (Segment Anything v2 model, unchanged)
)
GROUNDING_MODELS=(
    # (GroundingDINO model files, unchanged)
)
# ... (the rest of model categories like UPSCALE_MODELS if any, unchanged) ...

### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

# Modular pinning for custom nodes
# Map: folder name -> commit/tag. Extend/override via COMFY_NODE_PINS env var.
# Example: COMFY_NODE_PINS="ComfyUI-Impact-Pack=4186fbd4f4d7fff87c2a5dac8e69ab1031ca1259,ComfyUI-Manager=v2.22"
declare -A NODE_PINS

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
        printf "Updating ComfyUI to pinned version (483b3e6)...\n"
        (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch
            echo "DEBUG: Checking out pinned commit..."
            git checkout 483b3e6
        )
        # Install any new core requirements if present
        if [ -f "${COMFYUI_DIR}/requirements.txt" ]; then
            printf "Installing ComfyUI requirements...\n"
            pip install --no-cache-dir -r "${COMFYUI_DIR}/requirements.txt"
        else
            echo "DEBUG: requirements.txt not found in ${COMFYUI_DIR}"
        fi
    else
        echo "DEBUG: ComfyUI directory not found!"
    fi
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
                (
                    cd "$path" && git config --global --add safe.directory "$(pwd)" && git pull
                )
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
    # Create provisioning completion marker(s)
    echo "Creating provisioning completion marker..."
    mkdir -p "${WORKSPACE}/ComfyUI/input"
    echo "Provisioning completed at $(date)" > "${WORKSPACE}/ComfyUI/input/provisioned_qwen_image_edit_2509.txt"
    # Also write common marker used by other scripts (for compatibility)
    echo "Provisioning completed at $(date)" > "${WORKSPACE}/ComfyUI/input/provisioned_furry_all.txt"

    printf "\nProvisioning complete:  Application will start now\n\n"
}

function provisioning_has_valid_hf_token() {
    [[ -n "$HF_TOKEN" ]] || return 1
    url="https://huggingface.co/api/whoami-v2"

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $HF_TOKEN" \
        -H "Content-Type: application/json")

    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

function provisioning_has_valid_civitai_token() {
    [[ -n "$CIVITAI_TOKEN" ]] || return 1
    url="https://civitai.com/api/v1/models?hidden=1&limit=1"

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $CIVITAI_TOKEN" \
        -H "Content-Type: application/json")

    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Download from $1 URL to $2 directory
function provisioning_download() {
    if [[ -n $HF_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?huggingface\.co(/|$|\?) ]]; then
        auth_token="$HF_TOKEN"
    elif [[ -n $CIVITAI_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?civitai\.com(/|$|\?) ]]; then
        auth_token="$CIVITAI_TOKEN"
    fi
    if [[ -n $auth_token ]]; then
        wget --header="Authorization: Bearer $auth_token" -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    else
        wget -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    fi
}

# Main provisioning steps
function provisioning_start() {
    provisioning_print_header
    provisioning_update_comfyui
    provisioning_get_apt_packages
    load_node_pins_from_env
    provisioning_get_nodes
    provisioning_get_pip_packages
    # Download models to respective folders:
    provisioning_get_files \
        "${COMFYUI_DIR}/models/checkpoints" \
        "${CHECKPOINT_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/ultralytics/bbox" \
        "${BBOX_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/embeddings" \
        "${EMBEDDING_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/unet" \
        "${UNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/text_encoders" \
        "${TEXT_ENCODER_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/loras" \
        "${LORA_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/controlnet" \
        "${CONTROLNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/sam2" \
        "${SAM2_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/grounding-dino" \
        "${GROUNDING_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/vae" \
        "${VAE_MODELS[@]}"
    provisioning_print_end
}

# Allow user to disable provisioning if they started with a script they didn't want
echo "DEBUG: Checking for /.noprovisioning file..."
if [[ ! -f /.noprovisioning ]]; then
    echo "DEBUG: /.noprovisioning not found. Starting provisioning process."
    provisioning_start
else
    echo "DEBUG: /.noprovisioning found. Skipping provisioning."
fi

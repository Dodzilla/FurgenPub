#!/bin/bash

source /venv/main/bin/activate
COMFYUI_DIR=${WORKSPACE}/ComfyUI

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
    #"package-2"
)

PIP_PACKAGES=(
    #"onnxruntime-gpu"
)

NODES=(
    "https://github.com/Fannovel16/comfyui_controlnet_aux"
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/ltdrdata/ComfyUI-Impact-Subpack"
    "https://github.com/kijai/ComfyUI-KJNodes"
    "https://github.com/ltdrdata/ComfyUI-Manager"
    "https://github.com/cubiq/ComfyUI_essentials"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
    "https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
    "https://github.com/Dodzilla/ComfyUI-ComfyCouple"
    "https://github.com/Dodzilla/LoopsGroundingDino"
    "https://github.com/1038lab/ComfyUI-RMBG"
    "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite"
)

WORKFLOWS=(
)

CHECKPOINT_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/yiffymix_v62Noobxl.safetensors"
)

BBOX_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/face_yolov8m.pt"
)

EMBEDDING_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/embeddings_safe_neg.pt"
)

UNET_MODELS=(
)

GROUNDING_MODELS=(
    "https://huggingface.co/ShilongLiu/GroundingDINO/resolve/main/GroundingDINO_SwinB.cfg.py"
    "https://huggingface.co/ShilongLiu/GroundingDINO/resolve/main/groundingdino_swinb_cogcoor.pth"
)

SAM2_MODELS=(
    "https://dl.fbaipublicfiles.com/segment_anything_2/092824/sam2.1_hiera_large.pt"
)

LORA_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/FurryRealism.safetensors"
)

VAE_MODELS=(
    # "https://huggingface.co/stabilityai/sd-vae-ft-mse-original/resolve/main/vae-ft-mse-840000-ema-pruned.safetensors"
)

CONTROLNET_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/xinsir_controlnet_promax.safetensors"
)



### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

# Modular pinning for custom nodes
# Map: folder name -> commit/tag. Extend/override via COMFY_NODE_PINS env var.
# Example: COMFY_NODE_PINS="ComfyUI-Impact-Pack=4186fbd4f4d7fff87c2a5dac8e69ab1031ca1259,ComfyUI-Manager=v2.22"
declare -A NODE_PINS
# Default pin per request
NODE_PINS[ComfyUI-Impact-Pack]="4186fbd4f4d7fff87c2a5dac8e69ab1031ca1259"
NODE_PINS[comfyui_controlnet_aux]="cc6b232f4a47f0cdf70f4e1bfa24b74bd0d75bf1"
NODE_PINS[ComfyUI-Impact-Subpack]="50c7b71a6a224734cc9b21963c6d1926816a97f1"
NODE_PINS[ComfyUI-KJNodes]="3fcd22f2fe2be69c3229f192362b91888277cbcb"
NODE_PINS[ComfyUI-Manager]="b5a2bed5396e6be8a2d1970793f5ce2f1e74c8c2"
NODE_PINS[ComfyUI_essentials]="9d9f4bedfc9f0321c19faf71855e228c93bd0dc9"
NODE_PINS[was-node-suite-comfyui]="ea935d1044ae5a26efa54ebeb18fe9020af49a45"
NODE_PINS[ComfyUI_Comfyroll_CustomNodes]="d78b780ae43fcf8c6b7c6505e6ffb4584281ceca"
NODE_PINS[easy-comfy-nodes-async]="45cc063f5fe5dd81d9bfc7204000509e76baa7fb"
NODE_PINS[ComfyUI-ComfyCouple]="6c815b13e6269b7ade1dd3a49ef67de71a0014eb"
NODE_PINS[LoopsGroundingDino]="8d84e5501d147d974ba4b6bfeb5de67c324523a0"
NODE_PINS[ComfyUI-RMBG]="b28ce10b51e1d505a2ebf2608184119f0cf662d3"
NODE_PINS[ComfyUI-VideoHelperSuite]="08e8df15db24da292d4b7f943c460dc2ab442b24"

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
    provisioning_get_pip_packages
    provisioning_get_files \
        "${COMFYUI_DIR}/models/embeddings" \
        "${EMBEDDING_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/sam2" \
        "${SAM2_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/grounding-dino" \
        "${GROUNDING_MODELS[@]}"
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
        "${COMFYUI_DIR}/models/upscale_models" \
        "${UPSCALE_MODELS[@]}"
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
                ( cd "$path" && git pull )
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
    # Create provisioning completion marker
    echo "Creating provisioning completion marker..."
    echo "Provisioning completed at $(date)" > "${WORKSPACE}/ComfyUI/input/provisioned_furry_all.txt"

    printf "\nProvisioning complete:  Application will start now\n\n"
}

function provisioning_has_valid_hf_token() {
    [[ -n "$HF_TOKEN" ]] || return 1
    url="https://huggingface.co/api/whoami-v2"

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $HF_TOKEN" \
        -H "Content-Type: application/json")

    # Check if the token is valid
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

    # Check if the token is valid
    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Download from $1 URL to $2 file path
function provisioning_download() {
    if [[ -n $HF_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?huggingface\.co(/|$|\?) ]]; then
        auth_token="$HF_TOKEN"
    elif 
        [[ -n $CIVITAI_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?civitai\.com(/|$|\?) ]]; then
        auth_token="$CIVITAI_TOKEN"
    fi
    if [[ -n $auth_token ]];then
        wget --header="Authorization: Bearer $auth_token" -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    else
        wget -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    fi
}

# Allow user to disable provisioning if they started with a script they didn't want
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start
fi

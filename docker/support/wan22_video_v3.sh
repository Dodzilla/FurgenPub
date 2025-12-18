#!/bin/bash

set -x

source /venv/main/bin/activate
COMFYUI_DIR=${WORKSPACE}/ComfyUI

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
)

PIP_PACKAGES=(
    "flash_attn"
    "triton"
    "sageattention"
    "onnxruntime"
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
    "https://github.com/1038lab/ComfyUI-RMBG"

    # Other nodes
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
)

WORKFLOWS=(
)

CHECKPOINT_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/yiffymix_v62Noobxl.safetensors"
)

BBOX_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/face_yolov8m.pt"
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
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/SVI_Wan2.2-I2V-A14B_high_noise_lora_v2.0.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/SVI_Wan2.2-I2V-A14B_low_noise_lora_v2.0.safetensors"

    # https://civitai.com/models/1954774?modelVersionId=2212384
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-cunilingus-I2V-106epoc-high.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-cunilingus-I2V-72epoc-low.safetensors"

    # https://civitai.com/models/1343431?modelVersionId=2191270
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_BounceHighWan2_2.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_BounceLowWan2_2.safetensors"

    # https://civitai.com/models/1331682?modelVersionId=2098396
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan2.2_i2v_highnoise_pov_missionary_v1.0.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan2.2_i2v_lownoise_pov_missionary_v1.0.safetensors"

    # https://civitai.com/models/2008663?modelVersionId=2273467
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_slop_twerk_HighNoise_merged3_7_v2.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_slop_twerk_LowNoise_merged3_7_v2.safetensors"

    # https://civitai.com/models/2007360?modelVersionId=2272102
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_fingering_pussy_i2v2.2hi_v10.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_fingering_pussy_i2v2.2lo_v10.safetensors"

    # https://civitai.com/models/1782485?modelVersionId=2235244
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_Furry_Enhancer_v3_high.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_Furry_Enhancer_v3_low.safetensors"

    # https://civitai.com/models/1967237?modelVersionId=2230133
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-I2V-BreastPlay-HIGH-v2.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-I2V-BreastPlay-LOW-v2.safetensors"

    # https://civitai.com/models/1962545?modelVersionId=2221382
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/Wan22_Cum_high_noise_1.V1.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/Wan22_Cum_low_noise_1.V1.safetensors"

    # https://civitai.com/models/1952032?modelVersionId=2209481
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_Sensual_fingering_v1_high_noise.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_Sensual_fingering_v1_low_noise.safetensors"

    # https://civitai.com/models/1944129/slop-bounce-wan-22-i2v
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_breast_bounce_test_HighNoise-000005.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_breast_bounce_test_LowNoise-000005.safetensors"

    # https://civitai.com/models/1937327?modelVersionId=2208830
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_jerking_off_HIGH_14B.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_jerking_off_LOW_14B.safetensors"

    # https://civitai.com/models/1918611/breast-expansion-wan-22-i2v?modelVersionId=2204414
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_i2v_BE_v5_high_noise.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_i2v_BE_v5_low_noise.safetensors"

    # https://civitai.com/models/1428098?modelVersionId=2156435
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22.r3v3rs3_c0wg1rl-14b-High-i2v_e70.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22.r3v3rs3_c0wg1rl-14b-Low-i2v_e70.safetensors"

    # https://civitai.com/models/1934947/dancing-c?modelVersionId=2189993
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_dance_c_high_250905.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_dance_c_low_250905.safetensors"

    # https://civitai.com/models/1922973/f4c3spl4sh-cumshot-i2v-wan-22-video-lora-k3nk?modelVersionId=2178869
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-f4c3spl4sh-100epoc-high-k3nk.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-f4c3spl4sh-154epoc-low-k3nk.safetensors"

    # https://civitai.com/models/1849369/missionary-sex-multiple-angles-t2v-and-i2v?modelVersionId=2176200
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_missionary_HIGH_14B.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_missionary_LOW_14B.safetensors"

    # https://civitai.com/models/1874099/wan-22-i2v-pov-cowgirl?modelVersionId=2169837
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-I2V-POV-Cowgirl-HIGH-v1.0-fixed.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22-I2V-POV-Cowgirl-LOW-v1.0-fixed.safetensors"

    # https://civitai.com/models/1875500/mating-press-wan-22?modelVersionId=2122806
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_mating_press_high.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_mating_press_low.safetensors"

    # https://civitai.com/models/1811313?modelVersionId=2190476
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_DR34ML4Y_I2V_14B_HIGH.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_DR34ML4Y_I2V_14B_LOW.safetensors"

    "https://huggingface.co/LoopsBoops/furarch/resolve/main/FurryRealism.safetensors"
)

VAE_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1_VAE_fp32.safetensors"
)

TEXT_ENCODERS_MODELS=(
    "https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors"
)

UPSCALE_MODELS=(
    "https://huggingface.co/ai-forever/Real-ESRGAN/resolve/main/RealESRGAN_x2.pth"
)

CONTROLNET_MODELS=(
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/xinsir_controlnet_promax.safetensors"
)

# Added arrays to mirror wan_video_full.sh
DIFFUSION_MODELS=(
    # "https://huggingface.co/Comfy-Org/Wan_2.2_ComfyUI_Repackaged/resolve/main/split_files/diffusion_models/wan2.2_i2v_high_noise_14B_fp16.safetensors"
    # "https://huggingface.co/Comfy-Org/Wan_2.2_ComfyUI_Repackaged/resolve/main/split_files/diffusion_models/wan2.2_i2v_low_noise_14B_fp16.safetensors"
    "https://huggingface.co/lightx2v/Wan2.2-Distill-Models/resolve/main/wan2.2_i2v_A14b_high_noise_lightx2v_4step.safetensors"
    "https://huggingface.co/lightx2v/Wan2.2-Distill-Models/resolve/main/wan2.2_i2v_A14b_low_noise_lightx2v_4step.safetensors"
)

CLIPVISION_MODELS=(
    "https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/clip_vision/clip_vision_h.safetensors"
)

FRAME_INTERPOLATION_MODELS=(
    "https://huggingface.co/nguu/film-pytorch/resolve/887b2c42bebcb323baf6c3b6d59304135699b575/film_net_fp32.pt"
)

### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

# Modular pinning for custom nodes
# Map: folder name -> commit/tag. Extend/override via COMFY_NODE_PINS env var.
# Example: COMFY_NODE_PINS="ComfyUI-Impact-Pack=4186fbd4f4d7fff87c2a5dac8e69ab1031ca1259,ComfyUI-Manager=v2.22"
declare -A NODE_PINS
# Defaults from furry_all_v7.sh where available; latest HEAD otherwise
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

# New repos (latest as of now)
NODE_PINS[ComfyUI-Frame-Interpolation]="a969c01dbccd9e5510641be04eb51fe93f6bfc3d"
NODE_PINS[ComfyUI-GGUF]="be2a08330d7ec232d684e50ab938870d7529471e"
NODE_PINS[rgthree-comfy]="2b9eb36d3e1741e88dbfccade0e08137f7fa2bfb"
NODE_PINS[ComfyUI-Custom-Scripts]="f2838ed5e59de4d73cde5c98354b87a8d3200190"
NODE_PINS[ComfyUI-WanVideoWrapper]="4c4e7defc20e89d1e0e3f95ce2b9ec9cd743db74"

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
        printf "Updating ComfyUI to pinned version (b873051)...\n"
        (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch
            echo "DEBUG: Checking out pinned commit..."
            git checkout b873051
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
echo "DEBUG: Checking for /.noprovisioning file..."
if [[ ! -f /.noprovisioning ]]; then
    echo "DEBUG: /.noprovisioning not found. Starting provisioning process."
    provisioning_start
else
    echo "DEBUG: /.noprovisioning found. Skipping provisioning."
fi

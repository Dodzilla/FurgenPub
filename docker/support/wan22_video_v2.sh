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
    # WanVideo LoRAs
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_lightning_high_noise_model.safetensors"
    "https://huggingface.co/LoopsBoops/furarch/resolve/main/wan22_lightning_low_noise_model.safetensors"

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
)

VAE_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1_VAE_fp32.safetensors"
)

TEXT_ENCODERS_MODELS=(
    # "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/umt5-xxl-enc-bf16.safetensors"
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
    "https://huggingface.co/Comfy-Org/Wan_2.2_ComfyUI_Repackaged/resolve/main/split_files/diffusion_models/wan2.2_i2v_high_noise_14B_fp16.safetensors"
    "https://huggingface.co/Comfy-Org/Wan_2.2_ComfyUI_Repackaged/resolve/main/split_files/diffusion_models/wan2.2_i2v_low_noise_14B_fp16.safetensors"
)

CLIPVISION_MODELS=(
    "https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/clip_vision/clip_vision_h.safetensors"
)

FRAME_INTERPOLATION_MODELS=(
    "https://huggingface.co/nguu/film-pytorch/resolve/887b2c42bebcb323baf6c3b6d59304135699b575/film_net_fp32.pt"
)

### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

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
    provisioning_get_nodes
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
        path="${COMFYUI_DIR}/custom_nodes/${dir}"
        requirements="${path}/requirements.txt"
        if [[ -d $path ]]; then
            if [[ ${AUTO_UPDATE,,} != "false" ]]; then
                printf "Updating node: %s...\n" "${repo}"
                ( cd "$path" && git config --global --add safe.directory "$(pwd)" && git pull )
                if [[ -e $requirements ]]; then
                   pip install --no-cache-dir -r "$requirements"
                fi
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive
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
echo "DEBUG: Checking for /.noprovisioning file..."
if [[ ! -f /.noprovisioning ]]; then
    echo "DEBUG: /.noprovisioning not found. Starting provisioning process."
    provisioning_start
else
    echo "DEBUG: /.noprovisioning found. Skipping provisioning."
fi
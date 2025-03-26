#!/bin/bash

# This script prepares containers for ComfyUI and WanVideo models
# Verbose mode for error detection
set -e  # Stops the script in case of errors
set -x  # Prints each command to screen (for debugging)

# Default workflow
DEFAULT_WORKFLOW="https://raw.githubusercontent.com/ertubul/comfyui-wanvideo/refs/heads/main/wanvideo-ertubul-720p.json"

# Required ComfyUI nodes
NODES=(
    "https://github.com/ltdrdata/ComfyUI-Manager"
    "https://github.com/cubiq/ComfyUI_essentials"
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/Fannovel16/comfyui_controlnet_aux"
    "https://github.com/BlenderNeko/ComfyUI_ADV_CLIP_emb"
    "https://github.com/BlenderNeko/ComfyUI_Noise"
    "https://github.com/jags111/efficiency-nodes-comfyui"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
    "https://github.com/city96/SD-Latent-Upscaler"
    "https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/kijai/ComfyUI-DepthAnythingV2"
    "https://github.com/Isi-dev/ComfyUI-Img2DrawingAssistants"
    "https://github.com/ltdrdata/ComfyUI-Impact-Subpack"
)

# Model files
MODELS=(
    ["https://civitai.com/api/download/models/494387?type=Model&format=SafeTensor&size=full&fp=fp16"]="oneFORALLPonyFantasy_v20DPO.safetensors"
)

DIFFUSION_MODELS=(
    # ["https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-T2V-14B_fp8_e4m3fn.safetensors"]="Wan2_1-T2V-14B_fp8_e4m3fn.safetensors"
    # ["https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-I2V-14B-480P_fp8_e4m3fn.safetensors"]="Wan2_1-I2V-14B-480P_fp8_e4m3fn.safetensors"
    # ["https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-I2V-14B-720P_fp8_e4m3fn.safetensors"]="Wan2_1-I2V-14B-720P_fp8_e4m3fn.safetensors"
    # ["https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/diffusion_models/wan2.1_i2v_480p_14B_bf16.safetensors"]="wan2.1_i2v_480p_14B_bf16.safetensors"
    # ["https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/diffusion_models/wan2.1_i2v_720p_14B_fp16.safetensors"]="wan2.1_i2v_720p_14B_fp16.safetensors"
)

# Text encoders
TEXTENCODERS_MODELS=(
)

# LoRA models
LORA_MODELS=(
    ["https://civitai.com/api/download/models/244808?type=Model&format=SafeTensor"]="princess_xl_v2.safetensors"
)

# WanVideo VAE
VAE_MODELS=(
)

# CLIP Vision models
CLIPVISION_MODELS=(
)

# Upscale models
UPSCALE_MODELS=(
    ["https://furgenai.b-cdn.net/models/4x_NMKD-Siax_200k.pth"]="4x_NMKD-Siax_200k.pth"
)

### SCRIPT FUNCTIONS ###

function provisioning_start() {
    # Load environment variables
    if [[ ! -d /opt/environments/python ]]; then 
        export MAMBA_BASE=true
    fi
    source /opt/ai-dock/etc/environment.sh
    source /opt/ai-dock/bin/venv-set.sh comfyui

    # Start message
    provisioning_print_header
    
    # Set ComfyUI to the correct branch
    echo "Checking ComfyUI branch..."
    if [[ "$COMFYUI_BRANCH" != "master" ]]; then
        echo "⚠️ ComfyUI branch is not 'master'. Attempting to change..."
        cd "$WORKSPACE/ComfyUI"
        git fetch --all
        git checkout master || echo "❌ Could not change branch. Continuing with current one."
        cd /
    else
        echo "✅ ComfyUI is on the correct branch: master"
    fi
    
    # HF Token check
    if [[ -n "$HF_TOKEN" ]]; then
        # Token exists but contains placeholder or square brackets
        if [[ $HF_TOKEN == *"{"* || $HF_TOKEN == *"}"* ]]; then
            echo "WARNING: HF_TOKEN contains { } characters. Cleaning..."
            # Clean placeholder brackets
            export HF_TOKEN=$(echo $HF_TOKEN | sed 's/[{}]//g')
            echo "HF_TOKEN corrected: ${HF_TOKEN:0:3}...${HF_TOKEN: -3}"
        else
            echo "HF_TOKEN available: ${HF_TOKEN:0:3}...${HF_TOKEN: -3}"
        fi
        
        # Test if token is valid
        provisioning_test_hf_token
    else
        echo "WARNING: HF_TOKEN not set. Hugging Face model downloads may fail."
    fi
    
    # Model directories and permissions
    echo "Creating and setting permissions for model directories..."
    mkdir -p "${WORKSPACE}/ComfyUI/models/diffusion_models"
    mkdir -p "${WORKSPACE}/ComfyUI/models/clip_vision"
    mkdir -p "${WORKSPACE}/ComfyUI/models/text_encoders"
    mkdir -p "${WORKSPACE}/ComfyUI/models/vae"
    mkdir -p "${WORKSPACE}/ComfyUI/models/frame_interpolation"
    
    # Give full permissions
    # chmod -R 777 "${WORKSPACE}/ComfyUI/models"
    
    # Download base nodes
    provisioning_get_nodes "${NODES[@]}"
    
    # Download model files
    echo "Downloading models..."
    for url in "${!MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/checkpoints" "${MODELS[$url]}"
    done
    
    echo "Downloading diffusion models..."
    for url in "${!DIFFUSION_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/diffusion_models" "${DIFFUSION_MODELS[$url]}"
    done
    
    echo "Downloading text encoder models..."
    for url in "${!TEXTENCODERS_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/text_encoders" "${TEXTENCODERS_MODELS[$url]}"
    done
    
    echo "Downloading VAE models..."
    for url in "${!VAE_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/vae" "${VAE_MODELS[$url]}"
    done

    echo "Downloading LoRA models..."
    for url in "${!LORA_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/loras" "${LORA_MODELS[$url]}"
    done
    
    echo "Downloading CLIP Vision models..."
    for url in "${!CLIPVISION_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/clip_vision" "${CLIPVISION_MODELS[$url]}"
    done

    echo "Downloading upscale models..."
    for url in "${!UPSCALE_MODELS[@]}"; do
        provisioning_download "$url" "${WORKSPACE}/ComfyUI/models/upscale_models" "${UPSCALE_MODELS[$url]}"
    done
    
    # Check downloaded models
    echo "Checking downloaded models..."
    provisioning_verify_downloads

    echo "Installing sageattention"
    pip install sageattention
    
    # Create provisioning completion marker
    echo "Creating provisioning completion marker..."
    echo "Provisioning completed at $(date)" > "${WORKSPACE}/ComfyUI/input/provisioned.txt"
    
    # Completion message
    provisioning_print_end
}

function provisioning_test_hf_token() {
    echo "Testing Hugging Face token..."
    url="https://huggingface.co/api/whoami-v2"
    response=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $HF_TOKEN" \
        -H "Content-Type: application/json")
    
    if [ "$response" -eq 200 ]; then
        echo "✅ HF_TOKEN is valid. Model downloading will work."
    else
        echo "⚠️ WARNING: HF_TOKEN is not valid! (HTTP response code: $response)"
        echo "We'll try downloading without a token, but you might hit rate limits."
    fi
}

function provisioning_get_nodes() {
    echo "Downloading ComfyUI nodes..."
    for repo in "$@"; do
        dir="${repo##*/}"
        path="/opt/ComfyUI/custom_nodes/${dir}"
        requirements="${path}/requirements.txt"
        
        if [[ -d $path ]]; then
            echo "📦 Updating node: ${repo}"
            ( cd "$path" && git pull )
        else
            echo "📥 Downloading node: ${repo}"
            git clone --depth 1 "${repo}" "${path}"
        fi
        
        if [[ -e $requirements ]]; then
            echo "🧰 Installing requirements: ${requirements}"
            if [[ -z $MAMBA_BASE ]]; then
                "$COMFYUI_VENV_PIP" install --no-cache-dir -r "$requirements"
            else
                micromamba run -n comfyui pip install --no-cache-dir -r "$requirements"
            fi
        fi
        install_script="${path}/install-manual.py"
        if [[ -e $install_script ]]; then
            printf "Running install script: %s\n" "${install_script}"
            "$COMFYUI_VENV_PYTHON" "${install_script}"
        fi
    done
}

function provisioning_download() {
    url="$1"
    output_dir="$2"
    custom_filename="$3"  # New parameter for custom filename
    
    # Use custom filename if provided, otherwise use URL basename
    if [[ -n "$custom_filename" ]]; then
        filename="$custom_filename"
    else
        filename=$(basename "$url" | sed 's/\?.*//')  # Remove URL parameters
    fi
    
    echo "📥 Downloading: ${url} -> ${output_dir}/${filename}"
    
    # Create directory and set permissions
    mkdir -p "$output_dir"
    
    # Download attempts - try 3 times
    max_retries=3
    retry_count=0
    success=false
    
    while [ $retry_count -lt $max_retries ] && [ "$success" != "true" ]; do
        # Use HF_TOKEN if available and URL is from huggingface.co
        if [[ -n "$HF_TOKEN" && "$url" == *"huggingface.co"* ]]; then
            echo "🔑 Using Hugging Face token (attempt $((retry_count+1))/$max_retries)..."
            wget --header="Authorization: Bearer $HF_TOKEN" \
                 --content-disposition \
                 --show-progress \
                 --continue \
                 -O "$output_dir/$filename" "$url" && success=true
        else
            wget --content-disposition \
                 --show-progress \
                 --continue \
                 -O "$output_dir/$filename" "$url" && success=true
        fi
        
        # If download not successful, wait and retry
        if [ "$success" != "true" ]; then
            retry_count=$((retry_count+1))
            if [ $retry_count -lt $max_retries ]; then
                echo "⚠️ Download failed. Retrying... ($retry_count/$max_retries)"
                sleep 5  # Wait a bit before retrying
            fi
        fi
    done
    
    # Check download result
    if [ "$success" == "true" ]; then
        file_size=$(du -h "$output_dir/$filename" | cut -f1)
        echo "✅ Successfully downloaded: $output_dir/$filename ($file_size)"
        return 0
    else
        echo "❌ ERROR: File $url could not be downloaded after $max_retries attempts!"
        return 1
    fi
}

function provisioning_verify_downloads() {
    echo "🔍 Verifying downloaded files..."
    
    # List model directories and show file count
    for dir in "${WORKSPACE}/ComfyUI/models"/*; do
        if [ -d "$dir" ]; then
            file_count=$(find "$dir" -type f | wc -l)
            dir_size=$(du -sh "$dir" | cut -f1)
            echo "📁 Directory: $dir - $file_count files ($dir_size)"
            
            # Show file list
            if [ "$file_count" -gt 0 ]; then
                find "$dir" -type f -name "*.safetensors" -o -name "*.pt" | while read file; do
                    file_size=$(du -h "$file" | cut -f1)
                    echo "  - $(basename "$file") ($file_size)"
                done
            else
                echo "  ⚠️ WARNING: No files found in this directory!"
            fi
        fi
    done
    
    # Create download summary
    echo "📊 Download summary:"
    echo "-----------------------"
    echo "✅ Diffusion models: $(find "${WORKSPACE}/ComfyUI/models/diffusion_models" -type f | wc -l) files"
    echo "✅ Text encoders: $(find "${WORKSPACE}/ComfyUI/models/text_encoders" -type f | wc -l) files"
    echo "✅ VAE models: $(find "${WORKSPACE}/ComfyUI/models/vae" -type f | wc -l) files"
    echo "✅ CLIP Vision: $(find "${WORKSPACE}/ComfyUI/models/clip_vision" -type f | wc -l) files"
    echo "✅ Frame Interpolation: $(find "${WORKSPACE}/ComfyUI/models/frame_interpolation" -type f | wc -l) files"
    echo "-----------------------"
}

function provisioning_print_header() {
    printf "\n##############################################\n#                                            #\n#          WanVideo Container Setup          #\n#                                            #\n#         This will take some time           #\n#                                            #\n# Your container will be ready on completion #\n#                                            #\n##############################################\n\n"
}

function provisioning_print_end() {
    printf "\n##############################################\n#                                            #\n#          Setup completed!                  #\n#                                            #\n#    Starting ComfyUI interface...          #\n#                                            #\n##############################################\n\n"
}

# Run the main function
provisioning_start
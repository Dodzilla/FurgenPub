#!/bin/bash

# This script prepares containers for ComfyUI and WanVideo models
# Verbose mode for error detection
set -e  # Stops the script in case of errors
set -x  # Prints each command to screen (for debugging)

# Default workflow
DEFAULT_WORKFLOW="https://raw.githubusercontent.com/ertubul/comfyui-wanvideo/refs/heads/main/wanvideo-ertubul-720p.json"

# Required ComfyUI nodes
NODES=(
    # Core components
    "https://github.com/ltdrdata/ComfyUI-Manager"
    "https://github.com/cubiq/ComfyUI_essentials"
    
    # Video processing nodes
    "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite"
    "https://github.com/Fannovel16/ComfyUI-Frame-Interpolation"
    "https://github.com/city96/ComfyUI-GGUF"
    
    # Helper nodes
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/rgthree/rgthree-comfy"
    "https://github.com/pythongosssss/ComfyUI-Custom-Scripts"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
    
    # WanVideo nodes
    "https://github.com/kijai/ComfyUI-WanVideoWrapper"
    "https://github.com/kijai/ComfyUI-KJNodes"
)

# Model files
DIFFUSION_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-T2V-14B_fp8_e4m3fn.safetensors"
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-I2V-14B-480P_fp8_e4m3fn.safetensors"
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1-I2V-14B-720P_fp8_e4m3fn.safetensors"
)

# Text encoders
TEXTENCODERS_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/open-clip-xlm-roberta-large-vit-huge-14_visual_fp16.safetensors"
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/umt5-xxl-enc-bf16.safetensors"
    "https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors"
)

# WanVideo VAE
VAE_MODELS=(
    "https://huggingface.co/Kijai/WanVideo_comfy/resolve/main/Wan2_1_VAE_bf16.safetensors"
)

# CLIP Vision models
CLIPVISION_MODELS=(
    "https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files/clip_vision/clip_vision_h.safetensors"
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
    chmod -R 777 "${WORKSPACE}/ComfyUI/models"
    
    # Download base nodes
    provisioning_get_nodes "${NODES[@]}"
    
    # Download model files
    echo "Downloading diffusion models..."
    for model in "${DIFFUSION_MODELS[@]}"; do
        provisioning_download "$model" "${WORKSPACE}/ComfyUI/models/diffusion_models"
        # Even if download fails, continue with the next model
    done
    
    echo "Downloading text encoder models..."
    for model in "${TEXTENCODERS_MODELS[@]}"; do
        provisioning_download "$model" "${WORKSPACE}/ComfyUI/models/text_encoders"
        # Even if download fails, continue with the next model
    done
    
    echo "Downloading VAE models..."
    for model in "${VAE_MODELS[@]}"; do
        provisioning_download "$model" "${WORKSPACE}/ComfyUI/models/vae"
        # Even if download fails, continue with the next model
    done
    
    echo "Downloading CLIP Vision models..."
    for model in "${CLIPVISION_MODELS[@]}"; do
        provisioning_download "$model" "${WORKSPACE}/ComfyUI/models/clip_vision"
        # Even if download fails, continue with the next model
    done

    # Download FILM model
    echo "Downloading FILM Frame Interpolation model..."
    provisioning_download "https://huggingface.co/nguu/film-pytorch/resolve/887b2c42bebcb323baf6c3b6d59304135699b575/film_net_fp32.pt" "${WORKSPACE}/ComfyUI/models/frame_interpolation"
    
    # Set default workflow
    if [[ -n "$DEFAULT_WORKFLOW" ]]; then
        provisioning_get_default_workflow
    fi
    
    # Check downloaded models
    echo "Checking downloaded models..."
    provisioning_verify_downloads
    
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
    done
}

function provisioning_get_default_workflow() {
    echo "Downloading default workflow: ${DEFAULT_WORKFLOW}"
    workflow_json=$(curl -s "$DEFAULT_WORKFLOW")
    if [[ -n $workflow_json ]]; then
        echo "export const defaultGraph = $workflow_json;" > /opt/ComfyUI/web/scripts/defaultGraph.js
        echo "✅ Default workflow set successfully."
    else
        echo "❌ ERROR: Could not download the default workflow!"
    fi
}

function provisioning_download() {
    url="$1"
    output_dir="$2"
    filename=$(basename "$url" | sed 's/\?.*//')  # Remove URL parameters
    
    echo "📥 Downloading: ${url} -> ${output_dir}/${filename}"
    
    # Create directory and set permissions
    mkdir -p "$output_dir"
    chmod 777 "$output_dir"
    
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
                 -P "$output_dir" "$url" && success=true
        else
            wget --content-disposition \
                 --show-progress \
                 --continue \
                 -P "$output_dir" "$url" && success=true
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
        # Verify downloaded file
        find "$output_dir" -type f -name "$filename" -o -name "$(basename "$url" | cut -d? -f1)" | while read file; do
            file_size=$(du -h "$file" | cut -f1)
            echo "✅ Successfully downloaded: $file ($file_size)"
            # Give everyone access to the file
            chmod 666 "$file"
        done
        return 0
    else
        echo "❌ ERROR: File $url could not be downloaded after $max_retries attempts!"
        # Return 1 but don't exit the script - this allows the loop to continue with next model
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
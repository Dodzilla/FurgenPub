#!/bin/bash

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"

if [[ -z "$DM_INSTANCE_ID" && -n "$VAST_CONTAINERLABEL" ]]; then
    DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
    export DM_INSTANCE_ID
fi

source /venv/main/bin/activate
COMFYUI_DIR="${DM_COMFYUI_DIR}"

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
    #"package-2"
)

PIP_PACKAGES=(
    "flash-attn"
)

NODES=(
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/cubiq/ComfyUI_essentials"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
    "https://github.com/Fannovel16/comfyui_controlnet_aux"
    "https://github.com/PozzettiAndrea/ComfyUI-SAM3"
    "https://github.com/ltdrdata/ComfyUI-Manager"
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
    "https://github.com/Dodzilla/ComfyUI-ComfyCouple"
    "https://github.com/scottmudge/ComfyUI-NAG"
    "https://github.com/DarioFT/ComfyUI-Qwen3-TTS"
)


### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

# Modular pinning for custom nodes
# Map: folder name -> commit/tag. Extend/override via COMFY_NODE_PINS env var.
# Example: COMFY_NODE_PINS="ComfyUI-Impact-Pack=6a517ebe06fea2b74fc41b3bd089c0d7173eeced,ComfyUI-Manager=d8e3f531c7348dedd39602cc85a438caf09589e5"
declare -A NODE_PINS
# Default pin per request
NODE_PINS[ComfyUI-Impact-Pack]="6a517ebe06fea2b74fc41b3bd089c0d7173eeced"
NODE_PINS[comfyui_controlnet_aux]="136f125c89aed92ced1b6fbb491e13719b72fcc0"
NODE_PINS[ComfyUI-Manager]="d8e3f531c7348dedd39602cc85a438caf09589e5"
NODE_PINS[ComfyUI_essentials]="9d9f4bedfc9f0321c19faf71855e228c93bd0dc9"
NODE_PINS[was-node-suite-comfyui]="ea935d1044ae5a26efa54ebeb18fe9020af49a45"
NODE_PINS[ComfyUI_Comfyroll_CustomNodes]="d78b780ae43fcf8c6b7c6505e6ffb4584281ceca"
NODE_PINS[ComfyUI-ComfyCouple]="6c815b13e6269b7ade1dd3a49ef67de71a0014eb"
NODE_PINS[ComfyUI-NAG]="c6f27116a8259f5b501d498a09e51c82fa72e35f"

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
        printf "Updating ComfyUI to pinned version (4e6a1b6)...\n"
        (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch
            echo "DEBUG: Checking out pinned commit..."
            git checkout 4e6a1b66a93ef91848bc4bbf2a84e0ea98efcfc9
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
    provisioning_install_qwen3_tts_requirements
    provisioning_get_pip_packages
    # models are now installed by DM agent
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

function provisioning_install_qwen3_tts_requirements() {
    local node_path requirements_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Qwen3-TTS"
    requirements_path="${node_path}/requirements.txt"

    if [[ -e "${requirements_path}" ]]; then
        printf "Installing ComfyUI-Qwen3-TTS requirements (explicit pass)...\n"
        pip install --no-cache-dir -r "${requirements_path}"
    else
        printf "WARN: ComfyUI-Qwen3-TTS requirements.txt not found: %s\n" "${requirements_path}"
    fi
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

function dependency_manager_start_agent() {
    # Allow opt-out.
    if [[ "${DM_AGENT_DISABLE,,}" == "1" || "${DM_AGENT_DISABLE,,}" == "true" ]]; then
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
            fallback_url="https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py"
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

# Allow user to disable provisioning if they started with a script they didn't want
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start
fi

# Start the dependency manager agent (best-effort; safe if required env vars are missing).
dependency_manager_start_agent

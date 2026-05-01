#!/bin/bash
set -euo pipefail

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"
export COMFYUI_DIR="${DM_COMFYUI_DIR}"
export SERVER_TYPE="${SERVER_TYPE:-lora_gen_v1}"

if [[ -z "${DM_INSTANCE_ID:-}" && -n "${VAST_CONTAINERLABEL:-}" ]]; then
    export DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
fi

if [[ -f /venv/main/bin/activate ]]; then
    source /venv/main/bin/activate
fi

COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT:-e87858e9743f92222cdb478f1f835135750b6a0b}"
FURGENPUB_RAW_BASE_URL="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}"
FCS_LORA_FACTORY_DIR="${FCS_LORA_FACTORY_DIR:-/data/lora_factory}"
FCS_LORA_TRAIN_COMMAND="${FCS_LORA_TRAIN_COMMAND:-python3 /workspace/lora_gen_v1_train.py --config {config}}"
LORA_GEN_V1_INSTALL_TOOLKIT="${LORA_GEN_V1_INSTALL_TOOLKIT:-false}"
LORA_GEN_V1_TOOLKIT_REPO="${LORA_GEN_V1_TOOLKIT_REPO:-https://github.com/ostris/ai-toolkit.git}"
LORA_GEN_V1_TOOLKIT_REF="${LORA_GEN_V1_TOOLKIT_REF:-main}"
LORA_GEN_V1_INSTALL_MUSUBI="${LORA_GEN_V1_INSTALL_MUSUBI:-true}"
LORA_GEN_V1_MUSUBI_REPO="${LORA_GEN_V1_MUSUBI_REPO:-https://github.com/kohya-ss/musubi-tuner.git}"
LORA_GEN_V1_MUSUBI_REF="${LORA_GEN_V1_MUSUBI_REF:-main}"
export FCS_LORA_FACTORY_DIR FCS_LORA_TRAIN_COMMAND

APT_PACKAGES=(
    "ffmpeg"
    "git"
    "git-lfs"
    "aria2"
    "build-essential"
    "ninja-build"
    "libgl1"
    "libopengl0"
    "libglib2.0-0"
)

NODES=(
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
)

declare -A NODE_PINS
NODE_PINS[ComfyUI-Impact-Pack]="6a517ebe06fea2b74fc41b3bd089c0d7173eeced"
NODE_PINS[easy-comfy-nodes-async]="d4c651a65e885a05ce5ce09468a2597ab1f7925c"

function node_dir_from_repo() {
    local repo="$1"
    local dir="${repo##*/}"
    printf "%s" "${dir%.git}"
}

function pin_repo() {
    local dir="$1"
    local repo_path="$2"
    local pin="${NODE_PINS[$dir]:-}"
    if [[ -z "${pin}" ]]; then
        echo "ERROR: Missing pin for ${dir}."
        return 1
    fi
    (
        cd "${repo_path}"
        git fetch --all --tags
        git checkout --force "${pin}"
    )
}

function provisioning_update_comfyui() {
    if [[ -d "${COMFYUI_DIR}/.git" ]]; then
        echo "Updating ComfyUI to ${COMFYUI_PIN_COMMIT}..."
        (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)" || true
            git fetch --all --tags
            git checkout --force "${COMFYUI_PIN_COMMIT}"
        )
        if [[ -f "${COMFYUI_DIR}/requirements.txt" ]]; then
            pip install --no-cache-dir -r "${COMFYUI_DIR}/requirements.txt"
        fi
    fi
}

function provisioning_get_apt_packages() {
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${APT_PACKAGES[@]}"
    fi
}

function provisioning_get_nodes() {
    mkdir -p "${COMFYUI_DIR}/custom_nodes"
    local repo dir dest requirements
    for repo in "${NODES[@]}"; do
        dir="$(node_dir_from_repo "${repo}")"
        dest="${COMFYUI_DIR}/custom_nodes/${dir}"
        requirements="${dest}/requirements.txt"
        if [[ ! -d "${dest}/.git" ]]; then
            rm -rf "${dest}"
            git clone "${repo}" "${dest}" --recursive
        fi
        pin_repo "${dir}" "${dest}"
        if [[ "${dir}" == "ComfyUI-Impact-Pack" ]]; then
            pip install --no-cache-dir "opencv-python-headless==4.11.0.86" "piexif==1.1.3" "segment-anything==1.0"
        elif [[ -f "${requirements}" ]]; then
            pip install --no-cache-dir -r "${requirements}"
        fi
    done
}

function provisioning_install_furgen_lora_factory_node() {
    local script_dir src_dir dest_dir remote_base
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    src_dir="${script_dir}/custom_nodes/FurgenLoraFactory"
    dest_dir="${COMFYUI_DIR}/custom_nodes/FurgenLoraFactory"
    remote_base="${FURGENPUB_RAW_BASE_URL%/}/custom_nodes/FurgenLoraFactory"

    rm -rf "${dest_dir}"
    mkdir -p "${dest_dir}"
    if [[ -d "${src_dir}" ]]; then
        cp -R "${src_dir}/." "${dest_dir}/"
    else
        curl -fsSL "${remote_base}/__init__.py" -o "${dest_dir}/__init__.py"
        curl -fsSL "${remote_base}/furgen_lora_factory.py" -o "${dest_dir}/furgen_lora_factory.py"
    fi
}

function provisioning_install_lora_train_runner() {
    local script_dir src_path dest_path remote_url
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    src_path="${script_dir}/lora_gen_v1_train.py"
    dest_path="${WORKSPACE}/lora_gen_v1_train.py"
    remote_url="${FURGENPUB_RAW_BASE_URL%/}/lora_gen_v1_train.py"

    if [[ -f "${src_path}" ]]; then
        cp "${src_path}" "${dest_path}"
    else
        curl -fsSL "${remote_url}" -o "${dest_path}"
    fi
    chmod +x "${dest_path}" || true
}

function provisioning_install_training_backends() {
    mkdir -p "${WORKSPACE}/training_backends" "${FCS_LORA_FACTORY_DIR}"
    if [[ "${LORA_GEN_V1_INSTALL_TOOLKIT,,}" == "true" ]]; then
        local toolkit_dir="${WORKSPACE}/training_backends/ai-toolkit"
        if [[ ! -d "${toolkit_dir}/.git" ]]; then
            git clone "${LORA_GEN_V1_TOOLKIT_REPO}" "${toolkit_dir}" --recursive
        fi
        (
            cd "${toolkit_dir}"
            git fetch --all --tags
            git checkout --force "${LORA_GEN_V1_TOOLKIT_REF}"
            if [[ -f requirements.txt ]]; then
                pip install --no-cache-dir -r requirements.txt
            fi
        )
    fi

    if [[ "${LORA_GEN_V1_INSTALL_MUSUBI:-false}" == "true" ]]; then
        local musubi_dir="${WORKSPACE}/training_backends/musubi-tuner"
        if [[ ! -d "${musubi_dir}/.git" ]]; then
            git clone "${LORA_GEN_V1_MUSUBI_REPO}" "${musubi_dir}" --recursive
        fi
        (
            cd "${musubi_dir}"
            git fetch --all --tags
            git checkout --force "${LORA_GEN_V1_MUSUBI_REF}"
            if [[ -f requirements.txt ]]; then
                pip install --no-cache-dir -r requirements.txt
            fi
            pip install --no-cache-dir "accelerate>=1.12.0"
        )
    fi
}

function provisioning_configure_environment() {
    mkdir -p "${COMFYUI_DIR}/input" "${COMFYUI_DIR}/output" "${FCS_LORA_FACTORY_DIR}"
    local env_file="/etc/profile.d/furgen_lora_gen_v1.sh"
    cat > "${env_file}" <<EOF
export SERVER_TYPE="${SERVER_TYPE}"
export DM_COMFYUI_DIR="${COMFYUI_DIR}"
export FCS_LORA_FACTORY_DIR="${FCS_LORA_FACTORY_DIR}"
export FCS_LORA_TRAIN_COMMAND="${FCS_LORA_TRAIN_COMMAND}"
export PYTORCH_CUDA_ALLOC_CONF="\${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"
EOF
    chmod 0644 "${env_file}" || true
}

function provisioning_print_end() {
    echo "Provisioning completed at $(date)" > "${COMFYUI_DIR}/input/provisioned_lora_gen_v1.txt"
}

function dependency_manager_start_agent() {
    if [[ -z "${FCS_API_BASE_URL:-}" || -z "${DEPENDENCY_MANAGER_SHARED_SECRET:-}" ]]; then
        echo "Skipping dependency manager agent; FCS_API_BASE_URL or DEPENDENCY_MANAGER_SHARED_SECRET is unset."
        return 0
    fi

    local agent_url agent_path log_path watchdog_path watchdog_log_path
    agent_url="${DEPENDENCY_AGENT_PUBLIC_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py}"
    agent_path="${WORKSPACE}/dependency_agent_v1.py"
    log_path="${WORKSPACE}/dependency_agent.log"
    watchdog_path="${WORKSPACE}/dependency_agent_watchdog.sh"
    watchdog_log_path="${WORKSPACE}/dependency_agent_watchdog.log"

    curl -fsSL "${agent_url}" -o "${agent_path}"
    cat > "${watchdog_path}" <<'EOF'
#!/bin/bash
set -u
WORKSPACE="${WORKSPACE:-/workspace}"
AGENT_PATH="${AGENT_PATH:-$WORKSPACE/dependency_agent_v1.py}"
LOG_PATH="${LOG_PATH:-$WORKSPACE/dependency_agent.log}"
while true; do
  if ! pgrep -f "$AGENT_PATH" >/dev/null 2>&1; then
    nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; python3 '$AGENT_PATH' >> '$LOG_PATH' 2>&1" >/dev/null 2>&1 &
  fi
  sleep 15
done
EOF
    chmod +x "${agent_path}" "${watchdog_path}" || true
    nohup "${watchdog_path}" >> "${watchdog_log_path}" 2>&1 &
}

function provisioning_start() {
    provisioning_update_comfyui
    provisioning_get_apt_packages
    provisioning_get_nodes
    provisioning_install_furgen_lora_factory_node
    provisioning_install_lora_train_runner
    provisioning_install_training_backends
    provisioning_configure_environment
    provisioning_print_end
}

dependency_manager_start_agent

case "${1:-start}" in
    start|"")
        if [[ ! -f /.noprovisioning ]]; then
            provisioning_start
        fi
        ;;
    *)
        echo "ERROR: Unknown lora_gen_v1 command: ${1}"
        exit 1
        ;;
esac

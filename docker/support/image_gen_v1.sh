#!/bin/bash

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"

if [[ -z "$DM_INSTANCE_ID" && -n "$VAST_CONTAINERLABEL" ]]; then
    DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
    export DM_INSTANCE_ID
fi

source /venv/main/bin/activate
COMFYUI_DIR="${DM_COMFYUI_DIR}"
export SERVER_TYPE="${SERVER_TYPE:-image_gen_v1}"
COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT:-a0ae3f3bd46b9e58f43fccfe17077873bf16f905}"

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
NODE_PINS[ComfyUI-SAM3]="978bb763cfadcad41363eba016e57686b414c27b"
NODE_PINS[easy-comfy-nodes-async]="d4c651a65e885a05ce5ce09468a2597ab1f7925c"

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
        printf "Updating ComfyUI to pinned version (%s)...\n" "${COMFYUI_PIN_COMMIT:0:7}"
        if ! (
            cd "${COMFYUI_DIR}"
            git config --global --add safe.directory "$(pwd)"
            echo "DEBUG: Current directory: $(pwd)"
            echo "DEBUG: Fetching git updates..."
            git fetch --all --tags
            echo "DEBUG: Checking out pinned commit..."
            git checkout --force "${COMFYUI_PIN_COMMIT}"
        ); then
            echo "ERROR: Failed to checkout pinned ComfyUI commit ${COMFYUI_PIN_COMMIT}."
            return 1
        fi
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

function provisioning_verify_flux_kv_cache_support() {
    local flux_nodes_file
    flux_nodes_file="${COMFYUI_DIR}/comfy_extras/nodes_flux.py"

    if [[ ! -f "${flux_nodes_file}" ]]; then
        printf "ERROR: Flux nodes file not found while verifying KV cache support: %s\n" "${flux_nodes_file}"
        return 1
    fi

    if ! grep -Fq "FluxKVCache" "${flux_nodes_file}"; then
        printf "ERROR: Pinned ComfyUI checkout does not expose FluxKVCache.\n"
        printf "ERROR: Checked %s at pin %s\n" "${flux_nodes_file}" "${COMFYUI_PIN_COMMIT}"
        return 1
    fi

    if ! grep -Fq "FluxKontextMultiReferenceLatentMethod" "${flux_nodes_file}"; then
        printf "ERROR: Pinned ComfyUI checkout does not expose FluxKontextMultiReferenceLatentMethod.\n"
        printf "ERROR: Checked %s at pin %s\n" "${flux_nodes_file}" "${COMFYUI_PIN_COMMIT}"
        return 1
    fi

    printf "Verified Flux KV cache node support at pin %s.\n" "${COMFYUI_PIN_COMMIT}"
}

function provisioning_install_impact_pack_runtime_requirements() {
    local node_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Impact-Pack"

    if [[ ! -d "${node_path}" ]]; then
        printf "ERROR: ComfyUI-Impact-Pack directory missing; LatentPixelScale cannot be installed.\n"
        return 1
    fi

    printf "Installing ComfyUI-Impact-Pack runtime dependencies (opencv-python-headless, piexif, segment-anything)...\n"
    pip install --no-cache-dir "opencv-python-headless==4.11.0.86" "piexif==1.1.3" "segment-anything==1.0" || {
        printf "ERROR: Failed to install Impact-Pack runtime dependencies.\n"
        return 1
    }
}

function provisioning_verify_latent_pixel_scale_support() {
    local node_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Impact-Pack"

    if [[ ! -d "${node_path}" ]]; then
        printf "ERROR: ComfyUI-Impact-Pack directory missing while verifying LatentPixelScale.\n"
        return 1
    fi

    if ! grep -R --include='*.py' -Fq "LatentPixelScale" "${node_path}"; then
        printf "ERROR: ComfyUI-Impact-Pack checkout does not expose LatentPixelScale.\n"
        printf "ERROR: Checked %s at pin %s\n" "${node_path}" "${NODE_PINS[ComfyUI-Impact-Pack]}"
        return 1
    fi

    printf "Verified LatentPixelScale support in ComfyUI-Impact-Pack at pin %s.\n" "${NODE_PINS[ComfyUI-Impact-Pack]}"
}

function provisioning_start() {
    provisioning_print_header || return 1
    if [[ "${FURGEN_BAKED:-0}" == "1" ]]; then
        # Baked images ship ComfyUI (pinned), custom nodes, and python deps preinstalled,
        # so readiness only waits on model downloads handled by the dependency agent.
        echo "Baked image detected (FURGEN_BAKED=1); skipping ComfyUI/node/pip provisioning."
        load_node_pins_from_env || return 1
        provisioning_verify_flux_kv_cache_support || return 1
        provisioning_verify_latent_pixel_scale_support || return 1
        provisioning_print_end || return 1
        return 0
    fi
    provisioning_update_comfyui || return 1
    provisioning_verify_flux_kv_cache_support || return 1
    provisioning_get_apt_packages || return 1
    load_node_pins_from_env || return 1
    provisioning_get_nodes || return 1
    provisioning_install_impact_pack_runtime_requirements || return 1
    provisioning_verify_latent_pixel_scale_support || return 1
    provisioning_get_pip_packages || {
        printf "WARN: Optional image_gen_v1 pip package install failed; continuing after required node verification.\n"
    }
    # models are now installed by DM agent
    provisioning_print_end || return 1
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
               if [[ "${dir}" == "ComfyUI-Impact-Pack" ]]; then
                   printf "Skipping full ComfyUI-Impact-Pack requirements; installing pinned runtime subset instead.\n"
               else
                   pip install --no-cache-dir -r "$requirements"
               fi
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive
            pin_node_if_requested "$dir" "$path"
            if [[ -e $requirements ]]; then
                if [[ "${dir}" == "ComfyUI-Impact-Pack" ]]; then
                    printf "Skipping full ComfyUI-Impact-Pack requirements; installing pinned runtime subset instead.\n"
                else
                    pip install --no-cache-dir -r "${requirements}"
                fi
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

function dependency_manager_is_disabled() {
    local dm_agent_disable
    dm_agent_disable="$(printf '%s' "${DM_AGENT_DISABLE:-}" | tr '[:upper:]' '[:lower:]')"
    [[ "$dm_agent_disable" == "1" || "$dm_agent_disable" == "true" ]]
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
        DEPENDENCY_AGENT_TARGET_VERSION \
        DEPENDENCY_AGENT_RELEASE_VERSION \
        DEPENDENCY_AGENT_UPDATE_URL \
        DEPENDENCY_AGENT_PUBLIC_URL \
        DEPENDENCY_AGENT_UPDATE_SHA256 \
        DEPENDENCY_AGENT_RELEASE_SHA256 \
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

function dependency_manager_start_agent() {
    # Allow opt-out.
    if dependency_manager_is_disabled; then
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

# A live agent holds the exclusive agent_lock for its whole lifetime (including
# across its in-process self-update). If we can acquire the lock, no agent holds
# it. Fall back to pgrep/pidfile when flock is unavailable.
agent_running() {
    if have_flock && [[ -e "$agent_lock" ]]; then
        if flock -n "$agent_lock" true 2>/dev/null; then
            return 1
        fi
        return 0
    fi
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "$agent_path" >/dev/null 2>&1; then
        return 0
    fi
    if [[ -f "$pid_path" ]]; then
        local pid
        pid="$(cat "$pid_path" 2>/dev/null || true)"
        [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && return 0
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
    "            nohup \"${watchdog_path}\" >> \"${watchdog_log_path}\" 2>&1 &\n"
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

    if dependency_manager_is_disabled; then
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
    nohup "$watchdog_path" >> "$watchdog_log_path" 2>&1 &
}

# Best-effort aria2 install before the agent starts so model downloads can use
# multi-connection transfers (the agent falls back to wget when aria2c is absent).
if ! command -v aria2c >/dev/null 2>&1; then
    echo "Installing aria2 for multi-connection downloads..."
    apt_runner=""
    if command -v sudo >/dev/null 2>&1; then apt_runner="sudo"; fi
    ($apt_runner apt-get update -qq >/dev/null 2>&1 || true) && \
        $apt_runner apt-get install -y -qq aria2 >/dev/null 2>&1 || \
        echo "WARN: aria2 install failed; dependency agent will fall back to wget."
fi

# Start the dependency manager agent (best-effort; safe if required env vars are missing).
dependency_manager_start_agent

# Allow user to disable provisioning if they started with a script they didn't want
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start || {
        echo "ERROR: image_gen_v1 provisioning failed."
        exit 1
    }
fi

# Re-apply the watchdog bootstrap after provisioning in case image startup scripts
# were regenerated while ComfyUI or custom nodes were updated.
dependency_manager_start_agent

#!/bin/bash

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"

if [[ -z "$DM_INSTANCE_ID" && -n "$VAST_CONTAINERLABEL" ]]; then
    DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
    export DM_INSTANCE_ID
fi

source /venv/main/bin/activate
COMFYUI_DIR="${DM_COMFYUI_DIR}"
# Keep asset_gen_v1 as the default while still allowing template-level override.
export SERVER_TYPE="${SERVER_TYPE:-asset_gen_v1}"
COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT:-185c61dc26cdc631a1fd57b53744b67393a97fc6}"
FURGENPUB_PIN_REF="${FURGENPUB_PIN_REF:-6444185e393660470095d75d9556dca2e5b163fc}"

TRELLIS2_ENABLE="${TRELLIS2_ENABLE:-true}"
TRELLIS2_ATTN_BACKEND="${TRELLIS2_ATTN_BACKEND:-flash_attn}"
TRELLIS2_MODEL_REPO="${TRELLIS2_MODEL_REPO:-microsoft/TRELLIS.2-4B}"
TRELLIS2_DINOV3_REPO="${TRELLIS2_DINOV3_REPO:-camenduru/dinov3-vitl16-pretrain-lvd1689m}"
TRELLIS2_DINOV3_FALLBACK_REPO="${TRELLIS2_DINOV3_FALLBACK_REPO:-camenduru/dinov3-vitl16-pretrain-lvd1689m}"
TRELLIS2_INSTALL_DINOV3="${TRELLIS2_INSTALL_DINOV3:-true}"
TRELLIS2_FLASH_ATTN_ALLOW_SOURCE_BUILD="${TRELLIS2_FLASH_ATTN_ALLOW_SOURCE_BUILD:-true}"
TRELLIS2_FLASH_ATTN_SOURCE_BUILD_TIMEOUT_SECONDS="${TRELLIS2_FLASH_ATTN_SOURCE_BUILD_TIMEOUT_SECONDS:-900}"

# If flash-attn install fails, we automatically fall back to xformers.
TRELLIS2_RESOLVED_ATTN_BACKEND="${TRELLIS2_ATTN_BACKEND}"

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
    "sox"
    "build-essential"
    "ninja-build"
    "libgl1"
    "libopengl0"
    "libglib2.0-0"
)

PIP_PACKAGES=(
    #"package-2"
)

TRELLIS2_RUNTIME_PIP_PACKAGES=(
    "trimesh"
)

NODES=(
    "https://github.com/cubiq/ComfyUI_essentials"
    "https://github.com/ltdrdata/ComfyUI-Impact-Pack"
    "https://github.com/scottmudge/ComfyUI-NAG"
    "https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/visualbruno/ComfyUI-Trellis2"
    "https://github.com/Dodzilla/easy-comfy-nodes-async"
    "https://github.com/DarioFT/ComfyUI-Qwen3-TTS"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
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
NODE_PINS[ComfyUI-Qwen3-TTS]="a2b5176d84ff101e3f2ab49876e9d9f2c38b7ee2"
NODE_PINS[ComfyUI-Trellis2]="07574666fbe7c82939cec5f69373b8f0958caae1"

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

function node_dir_from_repo() {
    local repo="$1"
    local dir="${repo##*/}"
    dir="${dir%.git}"
    printf "%s" "$dir"
}

function validate_required_repo_pins() {
    local missing=0
    local repo dir

    if [[ -z "${COMFYUI_PIN_COMMIT}" ]]; then
        printf "ERROR: COMFYUI_PIN_COMMIT is empty; refusing unpinned ComfyUI provisioning.\n"
        return 1
    fi

    for repo in "${NODES[@]}"; do
        dir="$(node_dir_from_repo "$repo")"
        if [[ -z "${NODE_PINS[$dir]:-}" ]]; then
            printf "ERROR: Missing NODE_PINS entry for repo %s (dir %s).\n" "$repo" "$dir"
            missing=1
        fi
    done

    if [[ $missing -ne 0 ]]; then
        printf "ERROR: One or more node repos are not pinned. Set COMFY_NODE_PINS or update NODE_PINS.\n"
        return 1
    fi

    return 0
}

function pin_node_to_ref() {
    local dir="$1"; shift
    local path="$1"
    local pin_ref="${NODE_PINS[$dir]:-}"
    if [[ -z "${pin_ref}" ]]; then
        printf "ERROR: No pin defined for node directory %s.\n" "$dir"
        return 1
    fi
    printf "Pinning %s to %s...\n" "$dir" "$pin_ref"
    (
        cd "$path" && git fetch --all --tags && git checkout --force "$pin_ref"
    ) || {
        printf "ERROR: Failed to pin %s to %s.\n" "$dir" "$pin_ref"
        return 1
    }
    return 0
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

function provisioning_start() {
    provisioning_print_header
    provisioning_update_comfyui
    provisioning_patch_comfyui_xformers_fallback
    provisioning_get_apt_packages
    load_node_pins_from_env
    validate_required_repo_pins
    provisioning_get_nodes
    provisioning_install_qwen3_tts_requirements
    provisioning_install_trellis2_runtime_requirements
    provisioning_configure_trellis2_runtime
    printf "Skipping Trellis2 model downloads in provisioning (managed by dependency manager static deps)...\n"
    provisioning_get_pip_packages
    # models are now installed by DM agent
    provisioning_print_end
}

function provisioning_patch_comfyui_xformers_fallback() {
    local attention_file
    attention_file="${COMFYUI_DIR}/comfy/ldm/modules/attention.py"

    if [[ ! -f "${attention_file}" ]]; then
        printf "WARN: Comfy attention file missing, skipping xformers fallback patch: %s\n" "${attention_file}"
        return 0
    fi

    /venv/main/bin/python - "${attention_file}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")

if "FCS xformers fallback patch" in source:
    print("Comfy xformers fallback patch already applied.")
    raise SystemExit(0)

old_snapshot_anchor = "    if skip_reshape:\n        # b h k d -> b k h d\n"
new_snapshot_block = (
    "    # FCS xformers fallback patch: preserve original tensors for fallback.\n"
    "    q_in, k_in, v_in, mask_in = q, k, v, mask\n\n"
    "    if skip_reshape:\n"
    "        # b h k d -> b k h d\n"
)

old_attention_call = "    out = xformers.ops.memory_efficient_attention(q, k, v, attn_bias=mask)\n"
new_attention_block = (
    "    try:\n"
    "        out = xformers.ops.memory_efficient_attention(q, k, v, attn_bias=mask)\n"
    "    except NotImplementedError as e:\n"
    "        logging.warning(\n"
    "            \"FCS xformers fallback patch: unsupported xformers attention kernel; falling back to PyTorch SDPA. %s\",\n"
    "            e,\n"
    "        )\n"
    "        return attention_pytorch(\n"
    "            q_in,\n"
    "            k_in,\n"
    "            v_in,\n"
    "            heads,\n"
    "            mask_in,\n"
    "            skip_reshape=skip_reshape,\n"
    "            skip_output_reshape=skip_output_reshape,\n"
    "            **kwargs,\n"
    "        )\n"
)

changed = False
if old_snapshot_anchor in source:
    source = source.replace(old_snapshot_anchor, new_snapshot_block, 1)
    changed = True
else:
    print("WARN: Could not locate skip_reshape anchor; xformers fallback patch not applied.", file=sys.stderr)

if old_attention_call in source:
    source = source.replace(old_attention_call, new_attention_block, 1)
    changed = True
else:
    print("WARN: Could not locate xformers attention call; xformers fallback patch not applied.", file=sys.stderr)

if not changed:
    raise SystemExit(0)

path.write_text(source, encoding="utf-8")
print("Applied Comfy xformers fallback patch.")
PY
}

function provisioning_get_apt_packages() {
    if [[ ${#APT_PACKAGES[@]} -eq 0 ]]; then
        return 0
    fi

    printf "Installing apt package prerequisites: %s\n" "${APT_PACKAGES[*]}"
    if command -v apt-get >/dev/null 2>&1; then
        if command -v sudo >/dev/null 2>&1; then
            sudo apt-get update
            sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${APT_PACKAGES[@]}"
        else
            apt-get update
            DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${APT_PACKAGES[@]}"
        fi
    elif [[ -n "${APT_INSTALL:-}" ]]; then
        # Compatibility fallback for environments that predefine APT_INSTALL.
        if command -v sudo >/dev/null 2>&1; then
            sudo ${APT_INSTALL} "${APT_PACKAGES[@]}"
        else
            ${APT_INSTALL} "${APT_PACKAGES[@]}"
        fi
    else
        printf "WARN: apt-get and APT_INSTALL are both unavailable; skipping apt package installation.\n"
    fi
}

function provisioning_get_pip_packages() {
    if [[ -n $PIP_PACKAGES ]]; then
            pip install --no-cache-dir ${PIP_PACKAGES[@]}
    fi
}

function provisioning_get_nodes() {
    local repo dir path requirements
    for repo in "${NODES[@]}"; do
        dir="$(node_dir_from_repo "$repo")"
        path="${COMFYUI_DIR}/custom_nodes/${dir}"
        requirements="${path}/requirements.txt"
        if [[ -d $path ]]; then
            if [[ ${AUTO_UPDATE,,} != "false" && -z "${NODE_PINS[$dir]:-}" ]]; then
                printf "Updating node: %s...\n" "${repo}"
                ( cd "$path" && git pull )
            fi
            pin_node_to_ref "$dir" "$path" || return 1
            if [[ -e $requirements ]]; then
               pip install --no-cache-dir -r "$requirements"
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive
            pin_node_to_ref "$dir" "$path" || return 1
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

function provisioning_install_trellis2_runtime_requirements() {
    if [[ "${TRELLIS2_ENABLE,,}" != "true" ]]; then
        return 0
    fi

    local node_path wheels_dir
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Trellis2"
    wheels_dir="${node_path}/wheels/Linux/Torch291"

    if [[ ! -d "${node_path}" ]]; then
        printf "WARN: Trellis2 node directory not found: %s\n" "${node_path}"
        return 0
    fi

    # Trellis2 Linux wheels are built against CUDA 12 runtime and require these binary wheels.
    if [[ -d "${wheels_dir}" ]]; then
        printf "Installing Trellis2 binary wheels from %s...\n" "${wheels_dir}"
        # o_voxel wheel depends on git-sourced cumesh/flex_gemm. We install those wheels directly
        # and install o_voxel without deps to avoid rebuilding against a mismatched local CUDA toolkit.
        pip install --no-cache-dir plyfile zstandard
        for wheel in "${wheels_dir}"/*.whl; do
            [[ -e "${wheel}" ]] || continue
            local wheel_name
            wheel_name="$(basename "${wheel}")"
            if [[ "${wheel_name}" == o_voxel-* ]]; then
                pip install --no-cache-dir --no-deps "${wheel}"
            else
                pip install --no-cache-dir "${wheel}"
            fi
        done
        if ! /venv/main/bin/python -c "import o_voxel" >/dev/null 2>&1; then
            local ovoxel_wheel
            ovoxel_wheel="$(find "${wheels_dir}" -maxdepth 1 -type f -name "o_voxel-*.whl" | head -n 1)"
            if [[ -n "${ovoxel_wheel}" ]]; then
                pip install --no-cache-dir --no-deps "${ovoxel_wheel}" || true
            fi
        fi
    else
        printf "WARN: Trellis2 wheels directory missing: %s\n" "${wheels_dir}"
    fi

    printf "Installing CUDA 12 runtime compatibility package for Trellis2...\n"
    pip install --no-cache-dir --upgrade nvidia-cuda-runtime-cu12

    printf "Installing rembg + onnxruntime-gpu for Trellis2 preprocessing...\n"
    pip install --no-cache-dir "onnxruntime-gpu==1.22.0" "rembg[gpu]==2.0.69"

    if ! /venv/main/bin/python -c "import trimesh" >/dev/null 2>&1; then
        printf "Installing additional Trellis2 runtime Python deps: %s\n" "${TRELLIS2_RUNTIME_PIP_PACKAGES[*]}"
        pip install --no-cache-dir "${TRELLIS2_RUNTIME_PIP_PACKAGES[@]}"
    fi

    # Make CUDA runtime libraries discoverable for both current shell and future service runs.
    local cuda_runtime_paths=()
    while IFS= read -r runtime_path; do
        [[ -z "${runtime_path}" ]] && continue
        cuda_runtime_paths+=("${runtime_path}")
    done < <(find /venv/main/lib -type d \( -path "*/site-packages/nvidia/cuda_runtime/lib" -o -path "*/site-packages/nvidia/cu13/lib" -o -path "*/site-packages/nvidia/*/lib" \) 2>/dev/null)

    if [[ ${#cuda_runtime_paths[@]} -eq 0 ]]; then
        printf "WARN: Could not find CUDA runtime library directories under /venv/main/lib.\n"
    else
        local as_root=""
        if command -v sudo >/dev/null 2>&1; then
            as_root="sudo"
        fi

        for runtime_path in "${cuda_runtime_paths[@]}"; do
            export LD_LIBRARY_PATH="${runtime_path}:${LD_LIBRARY_PATH:-}"
            local conf_name
            conf_name="$(echo "${runtime_path}" | tr '/.' '__')"
            printf "%s\n" "${runtime_path}" | ${as_root} tee "/etc/ld.so.conf.d/trellis2_${conf_name}.conf" >/dev/null || true
        done

        # Persist for non-interactive shells that may start ComfyUI/agent under supervisor.
        {
            printf "export LD_LIBRARY_PATH=\""
            local first=1
            for runtime_path in "${cuda_runtime_paths[@]}"; do
                if [[ ${first} -eq 0 ]]; then
                    printf ":"
                fi
                first=0
                printf "%s" "${runtime_path}"
            done
            printf ":\${LD_LIBRARY_PATH:-}\"\n"
        } | ${as_root} tee /etc/profile.d/trellis2_cuda.sh >/dev/null || true
        ${as_root} chmod 644 /etc/profile.d/trellis2_cuda.sh || true

        ${as_root} ldconfig || true
    fi

    # Ensure libcudart.so.12 is visible through the default linker path.
    local libcudart_candidate=""
    libcudart_candidate="$(find /venv/main/lib -type f -name "libcudart.so.12*" 2>/dev/null | head -n 1)"
    if [[ -n "${libcudart_candidate}" ]]; then
        local as_root=""
        if command -v sudo >/dev/null 2>&1; then
            as_root="sudo"
        fi
        ${as_root} ln -sf "${libcudart_candidate}" /usr/local/lib/libcudart.so.12 || true
        ${as_root} mkdir -p /usr/lib/x86_64-linux-gnu || true
        ${as_root} ln -sf "${libcudart_candidate}" /usr/lib/x86_64-linux-gnu/libcudart.so.12 || true
        ${as_root} ldconfig || true
    else
        printf "WARN: libcudart.so.12 was not found after installing nvidia-cuda-runtime-cu12.\n"
    fi

    TRELLIS2_RESOLVED_ATTN_BACKEND="${TRELLIS2_ATTN_BACKEND}"
    if [[ "${TRELLIS2_RESOLVED_ATTN_BACKEND,,}" == "flash_attn" ]]; then
        if ! /venv/main/bin/python -c "import flash_attn" >/dev/null 2>&1; then
            printf "Installing flash-attn (binary wheel preferred)...\n"
            if ! pip install --no-cache-dir --only-binary=:all: flash-attn; then
                if [[ "${TRELLIS2_FLASH_ATTN_ALLOW_SOURCE_BUILD,,}" == "true" ]] && command -v nvcc >/dev/null 2>&1; then
                    printf "flash-attn wheel unavailable; attempting bounded source build...\n"
                    export MAX_JOBS="${MAX_JOBS:-8}"
                    if command -v timeout >/dev/null 2>&1; then
                        timeout "${TRELLIS2_FLASH_ATTN_SOURCE_BUILD_TIMEOUT_SECONDS}" \
                            pip install --no-cache-dir --no-build-isolation flash-attn || true
                    else
                        pip install --no-cache-dir --no-build-isolation flash-attn || true
                    fi
                fi
            fi

            if ! /venv/main/bin/python -c "import flash_attn" >/dev/null 2>&1; then
                TRELLIS2_RESOLVED_ATTN_BACKEND="xformers"
                printf "WARN: flash-attn is unavailable; falling back to xformers backend.\n"
            fi
        fi
    fi
}

function provisioning_configure_trellis2_runtime() {
    if [[ "${TRELLIS2_ENABLE,,}" != "true" ]]; then
        return 0
    fi

    local launch_script
    launch_script="/opt/supervisor-scripts/comfyui.sh"
    if [[ ! -f "${launch_script}" ]]; then
        printf "WARN: Trellis2 runtime launch script not found: %s\n" "${launch_script}"
        return 0
    fi

    if ! grep -q "Trellis2 CUDA runtime compatibility block" "${launch_script}"; then
        cat <<'EOF' >> "${launch_script}"
# Trellis2 CUDA runtime compatibility block
for _trellis_cuda_lib in \
    /venv/main/lib/python3.12/site-packages/nvidia/cuda_runtime/lib \
    /venv/main/lib/python3.12/site-packages/nvidia/cu13/lib; do
    if [ -d "${_trellis_cuda_lib}" ]; then
        export LD_LIBRARY_PATH="${_trellis_cuda_lib}:${LD_LIBRARY_PATH:-}"
    fi
done
unset _trellis_cuda_lib
EOF
    fi

    if grep -q '^export ATTN_BACKEND=' "${launch_script}"; then
        sed -i "s|^export ATTN_BACKEND=.*|export ATTN_BACKEND=\"\\\${ATTN_BACKEND:-${TRELLIS2_RESOLVED_ATTN_BACKEND}}\"|" "${launch_script}"
    else
        printf "\nexport ATTN_BACKEND=\"\${ATTN_BACKEND:-%s}\"\n" "${TRELLIS2_RESOLVED_ATTN_BACKEND}" >> "${launch_script}"
    fi

    chmod +x "${launch_script}" || true
}

function provisioning_ensure_trellis2_core_models() {
    if [[ "${TRELLIS2_ENABLE,,}" != "true" ]]; then
        return 0
    fi

    local model_dir
    model_dir="${COMFYUI_DIR}/models/microsoft/TRELLIS.2-4B"

    printf "Ensuring Trellis2 core checkpoints are present...\n"
    /venv/main/bin/python - "${model_dir}" "${TRELLIS2_MODEL_REPO}" <<'PY'
import os
import sys
from huggingface_hub import hf_hub_download

local_dir = sys.argv[1]
repo_id = sys.argv[2]

required_files = [
    "pipeline.json",
    "ckpts/ss_flow_img_dit_1_3B_64_bf16.json",
    "ckpts/ss_flow_img_dit_1_3B_64_bf16.safetensors",
    "ckpts/shape_dec_next_dc_f16c32_fp16.json",
    "ckpts/shape_dec_next_dc_f16c32_fp16.safetensors",
    "ckpts/shape_enc_next_dc_f16c32_fp16.json",
    "ckpts/shape_enc_next_dc_f16c32_fp16.safetensors",
    "ckpts/tex_dec_next_dc_f16c32_fp16.json",
    "ckpts/tex_dec_next_dc_f16c32_fp16.safetensors",
    "ckpts/slat_flow_img2shape_dit_1_3B_512_bf16.json",
    "ckpts/slat_flow_img2shape_dit_1_3B_512_bf16.safetensors",
    "ckpts/slat_flow_img2shape_dit_1_3B_1024_bf16.json",
    "ckpts/slat_flow_img2shape_dit_1_3B_1024_bf16.safetensors",
    "ckpts/slat_flow_imgshape2tex_dit_1_3B_512_bf16.json",
    "ckpts/slat_flow_imgshape2tex_dit_1_3B_512_bf16.safetensors",
    "ckpts/slat_flow_imgshape2tex_dit_1_3B_1024_bf16.json",
    "ckpts/slat_flow_imgshape2tex_dit_1_3B_1024_bf16.safetensors",
]

for rel_path in required_files:
    abs_path = os.path.join(local_dir, rel_path)
    if os.path.exists(abs_path):
        continue
    print(f"Downloading missing Trellis2 file: {rel_path}", flush=True)
    hf_hub_download(repo_id=repo_id, filename=rel_path, local_dir=local_dir)
PY
}

function provisioning_download_hf_file_to_path() {
    local url output_path
    url="$1"
    output_path="$2"

    mkdir -p "$(dirname "${output_path}")"

    if [[ -n $HF_TOKEN && $url =~ ^https://([a-zA-Z0-9_-]+\.)?huggingface\.co(/|$|\?) ]]; then
        curl -fsSL -H "Authorization: Bearer ${HF_TOKEN}" "${url}" -o "${output_path}"
    else
        curl -fsSL "${url}" -o "${output_path}"
    fi
}

function provisioning_validate_trellis2_dinov3_config() {
    local config_path
    config_path="$1"

    /venv/main/bin/python - "${config_path}" <<'PY'
import json
import sys

cfg = json.load(open(sys.argv[1], "r", encoding="utf-8"))
if int(cfg.get("hidden_size", 0)) != 1024:
    raise SystemExit(1)
PY
}

function provisioning_download_trellis2_dinov3_repo() {
    local repo model_dir
    repo="$1"
    model_dir="${COMFYUI_DIR}/models/facebook/dinov3-vitl16-pretrain-lvd1689m"

    local files=(
        "config.json"
        "model.safetensors"
        "preprocessor_config.json"
        "README.md"
        "LICENSE.md"
        ".gitattributes"
    )

    for file in "${files[@]}"; do
        local url
        url="https://huggingface.co/${repo}/resolve/main/${file}?download=true"
        provisioning_download_hf_file_to_path "${url}" "${model_dir}/${file}" || return 1
    done
}

function provisioning_ensure_trellis2_dinov3_model() {
    if [[ "${TRELLIS2_ENABLE,,}" != "true" || "${TRELLIS2_INSTALL_DINOV3,,}" != "true" ]]; then
        return 0
    fi

    local model_dir current_hidden
    model_dir="${COMFYUI_DIR}/models/facebook/dinov3-vitl16-pretrain-lvd1689m"
    current_hidden=""

    if [[ -f "${model_dir}/config.json" ]]; then
        current_hidden=$(/venv/main/bin/python - "${model_dir}/config.json" <<'PY' 2>/dev/null || true
import json
import sys

cfg = json.load(open(sys.argv[1], "r", encoding="utf-8"))
print(cfg.get("hidden_size", ""))
PY
)
    fi

    if [[ -f "${model_dir}/model.safetensors" && "${current_hidden}" == "1024" ]]; then
        printf "Trellis2 Dinov3 model already present and valid.\n"
        return 0
    fi

    printf "Downloading Trellis2 Dinov3 model from %s...\n" "${TRELLIS2_DINOV3_REPO}"
    if provisioning_download_trellis2_dinov3_repo "${TRELLIS2_DINOV3_REPO}" && \
        provisioning_validate_trellis2_dinov3_config "${model_dir}/config.json"; then
        printf "Trellis2 Dinov3 model download complete.\n"
        return 0
    fi

    if [[ "${TRELLIS2_DINOV3_FALLBACK_REPO}" != "${TRELLIS2_DINOV3_REPO}" ]]; then
        printf "WARN: Primary Dinov3 repo failed. Retrying with fallback: %s\n" "${TRELLIS2_DINOV3_FALLBACK_REPO}"
        if provisioning_download_trellis2_dinov3_repo "${TRELLIS2_DINOV3_FALLBACK_REPO}" && \
            provisioning_validate_trellis2_dinov3_config "${model_dir}/config.json"; then
            printf "Trellis2 Dinov3 fallback download complete.\n"
            return 0
        fi
    fi

    printf "WARN: Unable to download a valid Trellis2 Dinov3 model. Trellis2 runtime generation may fail.\n"
    return 0
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
            fallback_url="https://raw.githubusercontent.com/Dodzilla/FurgenPub/${FURGENPUB_PIN_REF}/docker/scripts/dependency_agent_v1.py"
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
    provisioning_start || {
        echo "ERROR: Provisioning failed."
        exit 1
    }
fi

# Start the dependency manager agent (best-effort; safe if required env vars are missing).
dependency_manager_start_agent

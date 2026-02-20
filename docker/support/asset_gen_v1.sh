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
FURGENPUB_PIN_REF="${FURGENPUB_PIN_REF:-cb79ba9b924246c101c6a4ec69d578c016a6e0e7}"

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
    "https://github.com/kana112233/ComfyUI-kaola-moss-ttsd"
    "https://github.com/WASasquatch/was-node-suite-comfyui"
)

# Some nodes pull optional heavy source-build dependencies that are not
# required for asset_gen_v1 workflows and can stall provisioning.
SKIP_NODE_REQUIREMENTS=(
    "ComfyUI-Impact-Pack"
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
NODE_PINS[ComfyUI-kaola-moss-ttsd]="e3bba1ac47617207d6fb4d48da4ee65e632bfe19"

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

function should_skip_node_requirements() {
    local dir="$1"
    local skip_dir
    for skip_dir in "${SKIP_NODE_REQUIREMENTS[@]}"; do
        if [[ "$dir" == "$skip_dir" ]]; then
            return 0
        fi
    done
    return 1
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
    provisioning_print_header || return 1
    provisioning_update_comfyui || return 1
    provisioning_patch_comfyui_xformers_fallback || return 1
    provisioning_configure_pytorch_allocator_env || true
    provisioning_get_apt_packages || return 1
    load_node_pins_from_env
    validate_required_repo_pins || return 1
    provisioning_get_nodes || return 1
    provisioning_patch_trellis2_allocator_override || return 1
    provisioning_install_impact_pack_runtime_requirements || return 1
    provisioning_install_qwen3_tts_requirements || return 1
    provisioning_install_moss_ttsd_requirements || return 1
    provisioning_patch_moss_ttsd_runtime || return 1
    provisioning_install_transformers_compat_shim || return 1
    provisioning_install_trellis2_runtime_requirements || return 1
    provisioning_configure_trellis2_runtime || return 1
    provisioning_configure_comfyui_launch_args || true
    provisioning_configure_pytorch_allocator_env || true
    printf "Skipping Trellis2 model downloads in provisioning (managed by dependency manager static deps)...\n"
    provisioning_get_pip_packages || return 1
    if ! provisioning_verify_qwen3_tts_node; then
        printf "Qwen3-TTS validation failed after dependency installs; retrying targeted repair...\n"
        provisioning_repair_qwen3_tts_stack || return 1
        provisioning_verify_qwen3_tts_node || return 1
    fi
    # models are now installed by DM agent
    provisioning_print_end || return 1
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
    "    force_pytorch = False\n"
    "    device_capability = None\n"
    "    if torch.cuda.is_available():\n"
    "        try:\n"
    "            device_capability = torch.cuda.get_device_capability(q_in.device)\n"
    "        except Exception:\n"
    "            try:\n"
    "                device_capability = torch.cuda.get_device_capability()\n"
    "            except Exception:\n"
    "                device_capability = None\n"
    "    if device_capability and device_capability[0] >= 10:\n"
    "        force_pytorch = True\n"
    "\n"
    "    if force_pytorch:\n"
    "        if not getattr(attention_xformers, \"_fcs_sm_warning_emitted\", False):\n"
    "            logging.info(\n"
    "                \"FCS xformers fallback patch: skipping xformers on compute capability %s; using PyTorch SDPA.\",\n"
    "                device_capability,\n"
    "            )\n"
    "            attention_xformers._fcs_sm_warning_emitted = True\n"
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
    "\n"
    "    try:\n"
    "        out = xformers.ops.memory_efficient_attention(q, k, v, attn_bias=mask)\n"
    "    except NotImplementedError as e:\n"
    "        if not getattr(attention_xformers, \"_fcs_xformers_warning_emitted\", False):\n"
    "            logging.warning(\n"
    "                \"FCS xformers fallback patch: unsupported xformers attention kernel; falling back to PyTorch SDPA. %s\",\n"
    "                e,\n"
    "            )\n"
    "            attention_xformers._fcs_xformers_warning_emitted = True\n"
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
            if should_skip_node_requirements "$dir"; then
               printf "Skipping requirements install for node %s to avoid slow source-build dependencies.\n" "$dir"
            elif [[ -e $requirements ]]; then
               pip install --no-cache-dir -r "$requirements" || {
                   printf "ERROR: Failed to install requirements for node %s (%s)\n" "$dir" "$requirements"
                   return 1
               }
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive || {
                printf "ERROR: Failed to clone node repo %s\n" "${repo}"
                return 1
            }
            pin_node_to_ref "$dir" "$path" || return 1
            if should_skip_node_requirements "$dir"; then
                printf "Skipping requirements install for node %s to avoid slow source-build dependencies.\n" "$dir"
            elif [[ -e $requirements ]]; then
                pip install --no-cache-dir -r "${requirements}" || {
                    printf "ERROR: Failed to install requirements for node %s (%s)\n" "$dir" "$requirements"
                    return 1
                }
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
        pip install --no-cache-dir -r "${requirements_path}" || {
            printf "ERROR: Failed to install ComfyUI-Qwen3-TTS requirements: %s\n" "${requirements_path}"
            return 1
        }
        # Enforce the known-good stack expected by qwen-tts.
        pip install --no-cache-dir --upgrade \
            "qwen-tts==0.1.1" \
            "transformers==4.57.3" \
            "accelerate==1.12.0" \
            "modelscope" \
            "soundfile" \
            "safetensors" \
            "librosa" || {
            printf "ERROR: Failed to install pinned Qwen3-TTS runtime dependencies.\n"
            return 1
        }
    else
        printf "ERROR: ComfyUI-Qwen3-TTS requirements.txt not found: %s\n" "${requirements_path}"
        return 1
    fi
}

function provisioning_repair_qwen3_tts_stack() {
    local node_path requirements_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Qwen3-TTS"
    requirements_path="${node_path}/requirements.txt"

    if [[ ! -d "${node_path}" ]]; then
        printf "ERROR: ComfyUI-Qwen3-TTS node path is missing: %s\n" "${node_path}"
        return 1
    fi

    if [[ -e "${requirements_path}" ]]; then
        pip install --no-cache-dir --upgrade -r "${requirements_path}" || {
            printf "ERROR: Qwen3-TTS repair install failed from requirements: %s\n" "${requirements_path}"
            return 1
        }
    fi

    pip install --no-cache-dir --upgrade \
        "qwen-tts==0.1.1" \
        "transformers==4.57.3" \
        "accelerate==1.12.0" \
        "modelscope" \
        "soundfile" \
        "safetensors" \
        "librosa" || {
        printf "ERROR: Qwen3-TTS repair install failed for pinned runtime dependencies.\n"
        return 1
    }

    return 0
}

function provisioning_verify_qwen3_tts_node() {
    local node_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Qwen3-TTS"

    if [[ ! -d "${node_path}" ]]; then
        printf "ERROR: Qwen3-TTS custom node directory missing: %s\n" "${node_path}"
        return 1
    fi

    printf "Validating ComfyUI-Qwen3-TTS node loadability...\n"
    /venv/main/bin/python - "${node_path}" <<'PY'
import os
import sys

node_path = sys.argv[1]
init_path = os.path.join(node_path, "__init__.py")
nodes_path = os.path.join(node_path, "nodes.py")
errors = []

try:
    import qwen_tts  # noqa: F401
except Exception as exc:
    errors.append(f"import qwen_tts failed: {exc}")

try:
    required_nodes = ("Qwen3Loader", "Qwen3VoiceDesign")
    contents = ""
    for source_path in (init_path, nodes_path):
        if os.path.exists(source_path):
            with open(source_path, "r", encoding="utf-8") as fh:
                contents += fh.read()

    missing_nodes = [node for node in required_nodes if node not in contents]
    if missing_nodes:
        errors.append(f"missing required node symbols in source: {missing_nodes}")
except Exception as exc:
    errors.append(f"failed to inspect ComfyUI-Qwen3-TTS source: {exc}")

if errors:
    print("QWEN3_TTS_VALIDATION_FAILED")
    for err in errors:
        print(err)
    raise SystemExit(1)

print("QWEN3_TTS_VALIDATION_OK")
PY
}

function provisioning_install_moss_ttsd_requirements() {
    local node_path requirements_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-kaola-moss-ttsd"
    requirements_path="${node_path}/requirements.txt"

    if [[ -e "${requirements_path}" ]]; then
        printf "Installing ComfyUI-kaola-moss-ttsd requirements (explicit pass)...\n"
        pip install --no-cache-dir -r "${requirements_path}"
    else
        printf "WARN: ComfyUI-kaola-moss-ttsd requirements.txt not found: %s\n" "${requirements_path}"
    fi
}

function provisioning_install_impact_pack_runtime_requirements() {
    local node_path
    node_path="${COMFYUI_DIR}/custom_nodes/ComfyUI-Impact-Pack"
    if [[ ! -d "${node_path}" ]]; then
        printf "WARN: ComfyUI-Impact-Pack directory missing, skipping runtime dependency fix.\n"
        return 0
    fi

    printf "Installing ComfyUI-Impact-Pack runtime dependencies (piexif, segment-anything)...\n"
    pip install --no-cache-dir "piexif==1.1.3" "segment-anything==1.0" || {
        printf "ERROR: Failed to install Impact-Pack runtime dependencies.\n"
        return 1
    }
}

function provisioning_patch_moss_ttsd_runtime() {
    local node_file
    node_file="${COMFYUI_DIR}/custom_nodes/ComfyUI-kaola-moss-ttsd/nodes_voice_generator.py"

    if [[ ! -f "${node_file}" ]]; then
        printf "WARN: MOSS node file missing, skipping runtime compatibility patch: %s\n" "${node_file}"
        return 0
    fi

    /venv/main/bin/python - "${node_file}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
changed = False

helper_anchor = "except ImportError:\n    folder_paths = None\n\n"
helper_block = '''# FURGEN_MOSS_MODELING_COMPAT_PATCH
def _furgen_patch_moss_modeling_files(model_path):
    candidates = []
    if model_path:
        candidates.append(os.path.join(model_path, "modeling_moss_tts.py"))

    hf_modules_root = os.path.join(os.path.expanduser("~"), ".hf_home", "modules", "transformers_modules")
    if os.path.isdir(hf_modules_root):
        for root, _, files in os.walk(hf_modules_root):
            if "modeling_moss_tts.py" in files:
                candidates.append(os.path.join(root, "modeling_moss_tts.py"))

    seen = set()
    for candidate in candidates:
        if candidate in seen or not os.path.isfile(candidate):
            continue
        seen.add(candidate)

        try:
            with open(candidate, "r", encoding="utf-8") as fh:
                text = fh.read()
        except Exception:
            continue

        patched = text
        patched = patched.replace(
            "def get_input_embeddings(self, input_ids: Optional[torch.LongTensor]) -> torch.Tensor:",
            "def get_input_embeddings(self, input_ids: Optional[torch.LongTensor] = None) -> torch.Tensor:",
        )
        patched = patched.replace(
            "def get_input_embeddings(self, input_ids: torch.LongTensor) -> torch.Tensor:",
            "def get_input_embeddings(self, input_ids: Optional[torch.LongTensor] = None) -> torch.Tensor:",
        )
        if "inputs_embeds = self.language_model.get_input_embeddings()(input_ids[..., 0])" in patched and "if input_ids is None:" not in patched:
            patched = patched.replace(
                "        inputs_embeds = self.language_model.get_input_embeddings()(input_ids[..., 0])",
                "        # Hugging Face tie-weights path may call get_input_embeddings() with no args.\\n"
                "        if input_ids is None:\\n"
                "            return self.language_model.get_input_embeddings()\\n\\n"
                "        inputs_embeds = self.language_model.get_input_embeddings()(input_ids[..., 0])",
            )
        patched = patched.replace(
            "        pre_exclude_mask0 = torch.tensor([self.config.pad_token_id, self.config.audio_assistant_gen_slot_token_id, self.config.audio_assistant_delay_slot_token_id, self.config.audio_end_token_id], device=device)",
            "        pad_token_id = self.config.pad_token_id if self.config.pad_token_id is not None else (self.config.eos_token_id if getattr(self.config, \\"eos_token_id\\", None) is not None else 0)\\n"
            "        pre_exclude_mask0 = torch.tensor([pad_token_id, self.config.audio_assistant_gen_slot_token_id, self.config.audio_assistant_delay_slot_token_id, self.config.audio_end_token_id], device=device)",
        )

        if patched != text:
            try:
                with open(candidate, "w", encoding="utf-8") as fh:
                    fh.write(patched)
                print(f"[FURGEN_MOSS] Patched modeling file: {candidate}")
            except Exception as exc:
                print(f"[FURGEN_MOSS] WARN: failed to write {candidate}: {exc}")


def _furgen_apply_moss_config_fallback(model, processor, prefix):
    try:
        if getattr(model.config, "pad_token_id", None) is None:
            fallback_pad = None
            if hasattr(processor, "tokenizer"):
                fallback_pad = getattr(processor.tokenizer, "pad_token_id", None)
                if fallback_pad is None:
                    fallback_pad = getattr(processor.tokenizer, "eos_token_id", None)
            if fallback_pad is None:
                fallback_pad = 0
            model.config.pad_token_id = int(fallback_pad)
            print(f"[{prefix}] Applied fallback pad_token_id={model.config.pad_token_id}")

        if hasattr(processor, "model_config") and getattr(processor.model_config, "pad_token_id", None) is None:
            processor.model_config.pad_token_id = model.config.pad_token_id

        token_fallbacks = {
            "audio_assistant_gen_slot_token_id": "<|audio_assistant_gen_slot|>",
            "audio_assistant_delay_slot_token_id": "<|audio_assistant_delay_slot|>",
            "audio_end_token_id": "<|audio_end|>",
            "audio_start_token_id": "<|audio_start|>",
            "audio_user_slot_token_id": "<|audio_user_slot|>",
        }
        for attr_name, token_name in token_fallbacks.items():
            if getattr(model.config, attr_name, None) is None:
                recovered = None
                if hasattr(processor, "model_config"):
                    recovered = getattr(processor.model_config, attr_name, None)
                if recovered is None and hasattr(processor, "tokenizer"):
                    token_id = processor.tokenizer.convert_tokens_to_ids(token_name)
                    if token_id is not None and token_id >= 0:
                        recovered = int(token_id)
                if recovered is not None:
                    setattr(model.config, attr_name, int(recovered))
    except Exception as exc:
        print(f"[{prefix}] config fallback warning: {exc}")


'''
if "FURGEN_MOSS_MODELING_COMPAT_PATCH" not in source and helper_anchor in source:
    source = source.replace(helper_anchor, helper_anchor + helper_block, 1)
    changed = True

for marker in (
    '        print(f"[MOSS-VoiceGenerator] Loading model to {device} with {dtype}...")\n',
    '        print(f"[MOSS-SoundEffect] Loading model to {device} w/ {dtype}...")\n',
):
    idx = source.find(marker)
    if idx != -1:
        line_start = source.rfind("\n", 0, idx) + 1
        add_line = "        _furgen_patch_moss_modeling_files(model_path)\n"
        if add_line not in source[max(0, line_start - 240):line_start + 240]:
            source = source[:line_start] + add_line + source[line_start:]
            changed = True

for class_name, prefix in (
    ("MossVoiceGeneratorLoadModel", "MOSS-VoiceGenerator"),
    ("MossSoundEffectLoadModel", "MOSS-SoundEffect"),
):
    class_idx = source.find(f"class {class_name}:")
    if class_idx == -1:
        continue
    eval_idx = source.find("            model.eval()\n", class_idx)
    if eval_idx == -1:
        continue
    add_line = f'            _furgen_apply_moss_config_fallback(model, processor, "{prefix}")\n'
    insert_at = eval_idx + len("            model.eval()\n")
    if add_line not in source[eval_idx:eval_idx + 320]:
        source = source[:insert_at] + add_line + source[insert_at:]
        changed = True

if changed:
    path.write_text(source, encoding="utf-8")
    print("Applied MOSS runtime compatibility patch.")
else:
    print("MOSS runtime compatibility patch already present.")
PY
}

function provisioning_install_transformers_compat_shim() {
    local sitecustomize_path
    sitecustomize_path="$(
        /venv/main/bin/python - <<'PY'
import site
import sys

paths = [p for p in site.getsitepackages() if "site-packages" in p]
target_dir = paths[0] if paths else next((p for p in sys.path if p and "site-packages" in p), "")
if not target_dir:
    raise SystemExit(1)
print(f"{target_dir}/sitecustomize.py")
PY
    )" || {
        printf "WARN: Unable to determine sitecustomize.py path for transformers compatibility shim.\n"
        return 0
    }

    printf "Ensuring transformers compatibility shims at %s\n" "${sitecustomize_path}"
    /venv/main/bin/python - "${sitecustomize_path}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8") if path.exists() else ""

blocks = [
    (
        "FURGEN_TRANSFORMERS_PRETRAINED_SHIM",
        """# FURGEN_TRANSFORMERS_PRETRAINED_SHIM
try:
    from transformers import configuration_utils as _fcs_tr_cfg
    if not hasattr(_fcs_tr_cfg, "PreTrainedConfig") and hasattr(_fcs_tr_cfg, "PretrainedConfig"):
        _fcs_tr_cfg.PreTrainedConfig = _fcs_tr_cfg.PretrainedConfig
except Exception:
    pass
""",
    ),
    (
        "FURGEN_MOSS_PROCESSING_UTILS_SHIM",
        """# FURGEN_MOSS_PROCESSING_UTILS_SHIM
try:
    from transformers import processing_utils as _fcs_pu
    if not hasattr(_fcs_pu, "MODALITY_TO_BASE_CLASS_MAPPING"):
        _fcs_pu.MODALITY_TO_BASE_CLASS_MAPPING = {}
    _fcs_pu.MODALITY_TO_BASE_CLASS_MAPPING.setdefault("audio_tokenizer", "PreTrainedModel")
except Exception:
    pass
""",
    ),
    (
        "FURGEN_MOSS_AUDIO_TOKENIZER_CLASSCHECK_SHIM",
        """# FURGEN_MOSS_AUDIO_TOKENIZER_CLASSCHECK_SHIM
try:
    from transformers import processing_utils as _fcs_pu
    if hasattr(_fcs_pu, "ProcessorMixin"):
        _fcs_orig_check = _fcs_pu.ProcessorMixin.check_argument_for_proper_class
        def _fcs_patched_check(self, argument_name, argument_value):
            if argument_name == "audio_tokenizer":
                return None
            return _fcs_orig_check(self, argument_name, argument_value)
        _fcs_pu.ProcessorMixin.check_argument_for_proper_class = _fcs_patched_check
except Exception:
    pass
""",
    ),
    (
        "FURGEN_MOSS_PROCESSOR_INIT_SHIM",
        """# FURGEN_MOSS_PROCESSOR_INIT_SHIM
try:
    from transformers import processing_utils as _fcs_pu
    if hasattr(_fcs_pu, "ProcessorMixin"):
        _fcs_orig_init = _fcs_pu.ProcessorMixin.__init__
        def _fcs_patched_init(self, *args, **kwargs):
            if "audio_tokenizer" in kwargs:
                _tok = kwargs.get("audio_tokenizer")
                kwargs.setdefault("tokenizer", _tok)
                kwargs.setdefault("feature_extractor", _tok)
            return _fcs_orig_init(self, *args, **kwargs)
        _fcs_pu.ProcessorMixin.__init__ = _fcs_patched_init
except Exception:
    pass
""",
    ),
    (
        "FURGEN_MOSS_CLASSCHECK_BYPASS2",
        """# FURGEN_MOSS_CLASSCHECK_BYPASS2
try:
    from transformers import processing_utils as _fcs_pu
    if hasattr(_fcs_pu, "ProcessorMixin"):
        _fcs_prev_check = _fcs_pu.ProcessorMixin.check_argument_for_proper_class
        def _fcs_check_bypass2(self, argument_name, argument_value):
            if argument_name in ("audio_tokenizer", "feature_extractor"):
                return None
            return _fcs_prev_check(self, argument_name, argument_value)
        _fcs_pu.ProcessorMixin.check_argument_for_proper_class = _fcs_check_bypass2
except Exception:
    pass
""",
    ),
    (
        "FURGEN_MOSS_INITIALIZATION_ALIAS_SHIM",
        """# FURGEN_MOSS_INITIALIZATION_ALIAS_SHIM
try:
    import transformers as _fcs_tr
    import torch.nn.init as _fcs_torch_init
    if not hasattr(_fcs_tr, "initialization"):
        _fcs_tr.initialization = _fcs_torch_init
except Exception:
    pass
""",
    ),
]

changed = False
for marker, block in blocks:
    if marker in source:
        continue
    if source and not source.endswith("\n"):
        source += "\n"
    source += block
    changed = True

if changed:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    print(f"Updated compatibility shims at {path}")
else:
    print(f"Compatibility shims already present at {path}")
PY

    return 0
}

function provisioning_patch_trellis2_allocator_override() {
    if [[ "${TRELLIS2_ENABLE,,}" != "true" ]]; then
        return 0
    fi

    local node_file
    node_file="${COMFYUI_DIR}/custom_nodes/ComfyUI-Trellis2/nodes.py"
    if [[ ! -f "${node_file}" ]]; then
        printf "WARN: Trellis2 node file missing, skipping allocator override patch: %s\n" "${node_file}"
        return 0
    fi

    /venv/main/bin/python - "${node_file}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")

if "FURGEN hotfix: do not mutate allocator env" in source:
    print("Trellis2 allocator override patch already present.")
    raise SystemExit(0)

pattern = re.compile(r'(?m)^(\s*)os\.environ\["PYTORCH_CUDA_ALLOC_CONF"\]\s*=.*$')

def _repl(match):
    indent = match.group(1)
    return (
        f"{indent}# FURGEN hotfix: do not mutate allocator env during node import; this can crash torch 2.9 CUDA init.\n"
        f'{indent}# os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"'
    )

patched, count = pattern.subn(_repl, source, count=1)
if count == 0:
    print("WARN: Could not locate Trellis2 allocator env assignment; skipping patch.")
    raise SystemExit(0)

path.write_text(patched, encoding="utf-8")
print("Applied Trellis2 allocator override patch.")
PY
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

    /venv/main/bin/python - "${launch_script}" "${TRELLIS2_RESOLVED_ATTN_BACKEND}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
backend = sys.argv[2]
source = path.read_text(encoding="utf-8")
original = source

managed_pattern = re.compile(
    r"# FURGEN Trellis2 runtime block \(managed\)\n(?:.*\n)*?# /FURGEN Trellis2 runtime block\n",
    re.MULTILINE,
)
legacy_pattern = re.compile(
    r"# Trellis2 CUDA runtime compatibility block\n(?:.*\n)*?unset _trellis_cuda_lib\n",
    re.MULTILINE,
)

source = managed_pattern.sub("", source)
source = legacy_pattern.sub("", source)
source = re.sub(r'(?m)^export ATTN_BACKEND=.*\n', "", source)

block = (
    "# FURGEN Trellis2 runtime block (managed)\n"
    "for _trellis_cuda_lib in \\\n"
    "    /venv/main/lib/python3.12/site-packages/nvidia/cuda_runtime/lib \\\n"
    "    /venv/main/lib/python3.12/site-packages/nvidia/cu13/lib; do\n"
    "    if [ -d \"${_trellis_cuda_lib}\" ]; then\n"
    "        export LD_LIBRARY_PATH=\"${_trellis_cuda_lib}:${LD_LIBRARY_PATH:-}\"\n"
    "    fi\n"
    "done\n"
    "unset _trellis_cuda_lib\n"
    f"export ATTN_BACKEND=\"${{ATTN_BACKEND:-{backend}}}\"\n"
    "# /FURGEN Trellis2 runtime block\n"
)

anchor = "unset PYTORCH_CUDA_ALLOC_CONF\n"
if anchor in source:
    insert_at = source.find(anchor) + len(anchor)
    if insert_at < len(source) and source[insert_at] != "\n":
        source = source[:insert_at] + "\n" + source[insert_at:]
        insert_at += 1
else:
    if source.startswith("#!"):
        insert_at = source.find("\n")
        insert_at = insert_at + 1 if insert_at != -1 else len(source)
    else:
        insert_at = 0

patched = source[:insert_at] + block + source[insert_at:]
if patched != original:
    path.write_text(patched, encoding="utf-8")
    print("Applied Trellis2 launch runtime patch.")
else:
    print("Trellis2 launch runtime patch already present.")
PY

    chmod +x "${launch_script}" || true
}

function provisioning_configure_comfyui_launch_args() {
    local launch_script
    launch_script="/opt/supervisor-scripts/comfyui.sh"
    if [[ ! -f "${launch_script}" ]]; then
        printf "WARN: ComfyUI launch script not found for args normalization: %s\n" "${launch_script}"
        return 0
    fi

    /venv/main/bin/python - "${launch_script}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
original = source

managed_pattern = re.compile(
    r"# FURGEN ComfyUI launch args normalization\n(?:.*\n)*?# /FURGEN ComfyUI launch args normalization\n",
    re.MULTILINE,
)
source = managed_pattern.sub("", source)
legacy_args_pattern = re.compile(
    r'COMFYUI_ARGS=\$\{COMFYUI_ARGS:---disable-auto-launch --port 18188 --enable-cors-header\}\n'
    r'if \[\[ " \$\{COMFYUI_ARGS\} " != \*" --disable-cuda-malloc "\* \]\]; then\n'
    r'    COMFYUI_ARGS="\$\{COMFYUI_ARGS\} --disable-cuda-malloc"\n'
    r'fi\n',
    re.MULTILINE,
)
source = legacy_args_pattern.sub("", source)

block = (
    "# FURGEN ComfyUI launch args normalization\n"
    "COMFYUI_ARGS=${COMFYUI_ARGS:---disable-auto-launch --port 18188 --enable-cors-header}\n"
    "if [[ \" ${COMFYUI_ARGS} \" != *\" --disable-cuda-malloc \"* ]]; then\n"
    "    COMFYUI_ARGS=\"${COMFYUI_ARGS} --disable-cuda-malloc\"\n"
    "fi\n"
    "# /FURGEN ComfyUI launch args normalization\n"
)

anchor = "# Launch ComfyUI\n"
if anchor in source:
    insert_at = source.find(anchor)
else:
    launch_idx = source.find("python main.py")
    insert_at = source.rfind("\n", 0, launch_idx) + 1 if launch_idx != -1 else len(source)

patched = source[:insert_at] + block + source[insert_at:]

if patched != original:
    path.write_text(patched, encoding="utf-8")
    print("Applied ComfyUI launch args normalization patch.")
else:
    print("ComfyUI launch args normalization already present.")
PY

    chmod +x "${launch_script}" || true
}

function provisioning_configure_pytorch_allocator_env() {
    local launch_script
    launch_script="/opt/supervisor-scripts/comfyui.sh"
    if [[ ! -f "${launch_script}" ]]; then
        printf "WARN: ComfyUI launch script not found for allocator env normalization: %s\n" "${launch_script}"
        return 0
    fi

    /venv/main/bin/python - "${launch_script}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text(encoding="utf-8")

block = (
    "# FURGEN PyTorch allocator env normalization\n"
    "# PyTorch 2.9 can crash CUDA init when allocator vars disagree or mutate.\n"
    "if [ -n \"${PYTORCH_ALLOC_CONF:-}\" ] || [ -n \"${PYTORCH_CUDA_ALLOC_CONF:-}\" ]; then\n"
    "    echo \"INFO: Clearing PYTORCH allocator env overrides for CUDA init stability.\"\n"
    "fi\n"
    "unset PYTORCH_ALLOC_CONF\n"
    "unset PYTORCH_CUDA_ALLOC_CONF\n"
)

pattern = re.compile(
    r"# FURGEN PyTorch allocator env normalization\n"
    r"# PyTorch 2\.9 can crash CUDA init when allocator vars disagree or mutate\.\n"
    r"(?:.*\n){0,14}?unset PYTORCH_CUDA_ALLOC_CONF\n",
    re.MULTILINE,
)

cleaned = re.sub(pattern, "", source)

if cleaned.startswith("#!"):
    first_line, rest = cleaned.split("\n", 1)
    patched = f"{first_line}\n{block}{rest.lstrip()}"
else:
    patched = f"{block}{cleaned.lstrip()}"

if patched != source:
    path.write_text(patched, encoding="utf-8")
PY

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

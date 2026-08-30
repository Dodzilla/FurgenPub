#!/bin/bash

set -euo pipefail

export SERVER_TYPE="asset_gen_v7_lite"
export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
export DM_LOCAL_COMFY_BASE_URL="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}"
export DM_LOCAL_READINESS_FILE="${DM_LOCAL_READINESS_FILE:-provisioned_asset_gen_v7_lite.txt}"
export COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT:-e01fb4c56b7a88149d469b99cbbfe3223d715054}"
export ASSET_GEN_V5_IGNORED_BUNDLE_IDS="${ASSET_GEN_V5_IGNORED_BUNDLE_IDS:-asset_gen_v6_lite_comfy_core_v0251,asset_gen_v6_lite_runtime_helpers}"
export FURGENPUB_RAW_BASE_URL="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}"
BASE_SCRIPT="${WORKSPACE}/asset_gen_v5_lite.sh"
INFERENCE_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_inference.sh"
GATEWAY_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_gateway.py"
COORDINATOR_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_coordinator.py"
KITCHEN_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_comfy_kitchen.sh"
READINESS_PATH="${DM_COMFYUI_DIR}/input/${DM_LOCAL_READINESS_FILE}"
MODEL_PATH="${DM_COMFYUI_DIR}/models/llm/Qwen3.8-27B-Uncensored-Q5_K_M.gguf"
VISION_PATH="${DM_COMFYUI_DIR}/models/llm/Qwen3.8-27B-Uncensored-vision-f16.gguf"

download_support_file() {
    local name="$1" destination="$2"
    curl -fL --retry 5 --retry-delay 3 "${FURGENPUB_RAW_BASE_URL}/${name}" -o "${destination}"
    chmod +x "${destination}"
}

ensure_comfy_core() {
    if ! command -v git >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update
        apt-get install -y --no-install-recommends git ca-certificates
    fi
    download_support_file asset_gen_v7_lite_comfy_kitchen.sh "${KITCHEN_SCRIPT}"
    bash "${KITCHEN_SCRIPT}" install-core
}

download_support_file asset_gen_v5_lite.sh "${BASE_SCRIPT}"

command="${1:-start}"
if [[ "${command}" == "install-bundles" ]]; then
    shift
    exec env SERVER_TYPE="${SERVER_TYPE}" DM_ASSET_GEN_V5_LITE_SCRIPT="${BASE_SCRIPT}" \
        COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT}" \
        ASSET_GEN_V5_IGNORED_BUNDLE_IDS="${ASSET_GEN_V5_IGNORED_BUNDLE_IDS}" \
        bash "${BASE_SCRIPT}" install-bundles "$@"
fi
if [[ "${command}" != "start" && -n "${command}" ]]; then
    echo "ERROR: Unknown asset_gen_v7_lite command: ${command}" >&2
    exit 1
fi

ensure_comfy_core
bash "${KITCHEN_SCRIPT}" configure-launcher
rm -f "${READINESS_PATH}"
# Preserve Comfy's CPU node/object cache while allowing model VRAM to be
# revoked by the local coordinator.  Normalize old service config safely.
export COMFYUI_ARGS="${COMFYUI_ARGS:---disable-auto-launch --listen 0.0.0.0 --port 8188 --enable-cors-header}"
COMFYUI_ARGS="$(printf '%s' "${COMFYUI_ARGS}" | tr '\t\r\n' '   ')"
COMFYUI_ARGS="${COMFYUI_ARGS// --cache-none/}"
COMFYUI_ARGS="${COMFYUI_ARGS//--cache-none/}"
COMFYUI_ARGS="$(printf '%s\n' "${COMFYUI_ARGS}" | sed -E 's/(^|[[:space:]])--cache-ram([[:space:]]+[0-9.]+){0,2}//g')"
COMFYUI_ARGS="${COMFYUI_ARGS} --cache-ram 16 40"
for attention_flag in --use-sage-attention --use-pytorch-cross-attention --use-flash-attention --use-split-cross-attention --use-quad-cross-attention --use-ck-attention; do
    COMFYUI_ARGS=" ${COMFYUI_ARGS} "
    while [[ "${COMFYUI_ARGS}" == *" ${attention_flag} "* ]]; do
        COMFYUI_ARGS="${COMFYUI_ARGS// ${attention_flag} / }"
    done
done
COMFYUI_ARGS="${COMFYUI_ARGS} --use-ck-attention"
unset attention_flag
export COMFYUI_ARGS
env SERVER_TYPE="${SERVER_TYPE}" DM_LOCAL_READINESS_FILE="${DM_LOCAL_READINESS_FILE}" \
    COMFYUI_PIN_COMMIT="${COMFYUI_PIN_COMMIT}" \
    ASSET_GEN_V5_IGNORED_BUNDLE_IDS="${ASSET_GEN_V5_IGNORED_BUNDLE_IDS}" \
    DM_ASSET_GEN_V5_LITE_SCRIPT="${BASE_SCRIPT}" bash "${BASE_SCRIPT}" start
bash "${KITCHEN_SCRIPT}" configure-launcher

echo "Waiting for the pinned Qwen model and vision projector dependencies..."
model_ready=0
for _ in $(seq 1 420); do
    if [[ -f "${MODEL_PATH}" ]] && \
       [[ "$(stat -c '%s' "${MODEL_PATH}" 2>/dev/null || true)" == "19535701408" ]] && \
       [[ -f "${VISION_PATH}" ]] && \
       [[ "$(stat -c '%s' "${VISION_PATH}" 2>/dev/null || true)" == "927606912" ]]; then
        model_ready=1
        break
    fi
    sleep 5
done
if [[ "${model_ready}" != "1" ]]; then
    echo "ERROR: Qwen model or vision projector dependency did not become ready." >&2
    tail -n 250 "${WORKSPACE}/dependency_agent.log" >&2 || true
    exit 1
fi

download_support_file asset_gen_v7_lite_inference.sh "${INFERENCE_SCRIPT}"
download_support_file asset_gen_v7_lite_gateway.py "${GATEWAY_SCRIPT}"
download_support_file asset_gen_v7_lite_coordinator.py "${COORDINATOR_SCRIPT}"
download_support_file asset_gen_v7_lite_tts.py "${WORKSPACE}/asset_gen_v7_lite_tts.py"
download_support_file asset_gen_v7_lite_tts_runtime.py "${WORKSPACE}/asset_gen_v7_lite_tts_runtime.py"
download_support_file asset_gen_v7_lite_tts_install.py "${WORKSPACE}/asset_gen_v7_lite_tts_install.py"
download_support_file tts_profiles.py "${WORKSPACE}/tts_profiles.py"
bash "${INFERENCE_SCRIPT}"

curl -fsS "${DM_LOCAL_COMFY_BASE_URL}/system_stats" >/dev/null
curl -fsS "http://127.0.0.1:8080/health" >/dev/null
mkdir -p "$(dirname "${READINESS_PATH}")"
printf 'asset_gen_v7_lite ready at %s\nmodel=%s\nsha256=%s\nllama_cpp=%s\n' \
    "$(date -u +%FT%TZ)" \
    "Qwen3.8-27B-Uncensored-Q5_K_M.gguf" \
    "24780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547" \
    "a94d563ed801d1da1b8c2432946de07d0231bb3d" \
    > "${READINESS_PATH}"
printf 'vision=%s\nvision_sha256=%s\n' \
    "Qwen3.8-27B-Uncensored-vision-f16.gguf" \
    "5ac423f8a29059dc24e51bc6a43e9380dcd57a9347f28b62591e0b3f60b7081c" \
    >> "${READINESS_PATH}"
echo "asset_gen_v7_lite provisioning complete."

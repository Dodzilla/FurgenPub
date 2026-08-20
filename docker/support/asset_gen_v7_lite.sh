#!/bin/bash

set -euo pipefail

export SERVER_TYPE="asset_gen_v7_lite"
export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
export DM_LOCAL_COMFY_BASE_URL="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}"
export DM_LOCAL_READINESS_FILE="${DM_LOCAL_READINESS_FILE:-provisioned_asset_gen_v7_lite.txt}"
FURGENPUB_RAW_BASE_URL="${FURGENPUB_RAW_BASE_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support}"
BASE_SCRIPT="${WORKSPACE}/asset_gen_v5_lite.sh"
INFERENCE_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_inference.sh"
GATEWAY_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_gateway.py"
READINESS_PATH="${DM_COMFYUI_DIR}/input/${DM_LOCAL_READINESS_FILE}"
MODEL_PATH="${DM_COMFYUI_DIR}/models/llm/Qwen3.8-27B-Uncensored-Q5_K_M.gguf"

download_support_file() {
    local name="$1" destination="$2"
    curl -fL --retry 5 --retry-delay 3 "${FURGENPUB_RAW_BASE_URL}/${name}" -o "${destination}"
    chmod +x "${destination}"
}

download_support_file asset_gen_v5_lite.sh "${BASE_SCRIPT}"

command="${1:-start}"
if [[ "${command}" == "install-bundles" ]]; then
    shift
    exec env SERVER_TYPE="${SERVER_TYPE}" DM_ASSET_GEN_V5_LITE_SCRIPT="${BASE_SCRIPT}" \
        bash "${BASE_SCRIPT}" install-bundles "$@"
fi
if [[ "${command}" != "start" && -n "${command}" ]]; then
    echo "ERROR: Unknown asset_gen_v7_lite command: ${command}" >&2
    exit 1
fi

rm -f "${READINESS_PATH}"
env SERVER_TYPE="${SERVER_TYPE}" DM_LOCAL_READINESS_FILE="${DM_LOCAL_READINESS_FILE}" \
    DM_ASSET_GEN_V5_LITE_SCRIPT="${BASE_SCRIPT}" bash "${BASE_SCRIPT}" start

echo "Waiting for the pinned Qwen dependency at ${MODEL_PATH}..."
model_ready=0
for _ in $(seq 1 420); do
    if [[ -f "${MODEL_PATH}" ]] && [[ "$(stat -c '%s' "${MODEL_PATH}" 2>/dev/null || true)" == "19535701408" ]]; then
        model_ready=1
        break
    fi
    sleep 5
done
if [[ "${model_ready}" != "1" ]]; then
    echo "ERROR: Qwen model dependency did not become ready." >&2
    tail -n 250 "${WORKSPACE}/dependency_agent.log" >&2 || true
    exit 1
fi

download_support_file asset_gen_v7_lite_inference.sh "${INFERENCE_SCRIPT}"
download_support_file asset_gen_v7_lite_gateway.py "${GATEWAY_SCRIPT}"
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
echo "asset_gen_v7_lite provisioning complete."

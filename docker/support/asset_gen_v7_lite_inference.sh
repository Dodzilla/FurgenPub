#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-/workspace}"
COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
MODEL_FILE="Qwen3.8-27B-Uncensored-Q5_K_M.gguf"
MODEL_PATH="${COMFYUI_DIR}/models/llm/${MODEL_FILE}"
MODEL_SIZE_BYTES="19535701408"
MODEL_SHA256="24780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"
MODEL_ALIAS="qwen3.8-27b-uncensored"
LLAMA_CPP_COMMIT="a94d563ed801d1da1b8c2432946de07d0231bb3d"
LLAMA_REPO="${WORKSPACE}/src/llama.cpp"
LLAMA_BUILD="${LLAMA_REPO}/build-asset-gen-v7-lite"
LLAMA_SERVER="${LLAMA_BUILD}/bin/llama-server"
LLAMA_PORT="8081"
GATEWAY_PORT="8080"
LOG_DIR="${WORKSPACE}/logs"
GATEWAY_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_gateway.py"

mkdir -p "${LOG_DIR}" "$(dirname "${MODEL_PATH}")" "${WORKSPACE}/src"

if [[ -z "${INFERENCE_INSTANCE_API_KEY:-}" ]]; then
    echo "ERROR: INFERENCE_INSTANCE_API_KEY is required." >&2
    exit 1
fi

verify_model() {
    if [[ ! -f "${MODEL_PATH}" ]]; then
        echo "ERROR: Model dependency missing: ${MODEL_PATH}" >&2
        return 1
    fi
    local size sha
    size="$(stat -c '%s' "${MODEL_PATH}")"
    [[ "${size}" == "${MODEL_SIZE_BYTES}" ]] || {
        echo "ERROR: Model size mismatch: expected ${MODEL_SIZE_BYTES}, got ${size}" >&2
        return 1
    }
    sha="$(sha256sum "${MODEL_PATH}" | awk '{print $1}')"
    [[ "${sha}" == "${MODEL_SHA256}" ]] || {
        echo "ERROR: Model SHA mismatch: expected ${MODEL_SHA256}, got ${sha}" >&2
        return 1
    }
}

build_llama_server() {
    if [[ -x "${LLAMA_SERVER}" && -f "${LLAMA_BUILD}/furgen-commit-${LLAMA_CPP_COMMIT}" ]]; then
        return 0
    fi
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    # The v6 CUDA 13.2 image intentionally ships the runtime libraries without
    # nvcc. Install only the pinned CUDA compiler package needed to build the
    # Blackwell (sm_120) llama.cpp target; avoid the much larger toolkit meta
    # package and keep all model transfer direct from Hugging Face to Vast.
    apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake curl git ninja-build pkg-config \
        cuda-nvcc-13-2 libcublas-dev-13-2
    export CUDACXX="${CUDACXX:-/usr/local/cuda-13.2/bin/nvcc}"
    if [[ ! -x "${CUDACXX}" ]]; then
        echo "ERROR: CUDA 13.2 nvcc was not installed at ${CUDACXX}." >&2
        return 1
    fi
    if [[ ! -d "${LLAMA_REPO}/.git" ]]; then
        git clone --filter=blob:none https://github.com/ggml-org/llama.cpp.git "${LLAMA_REPO}"
    fi
    git -C "${LLAMA_REPO}" fetch --depth 1 origin "${LLAMA_CPP_COMMIT}"
    git -C "${LLAMA_REPO}" checkout --detach "${LLAMA_CPP_COMMIT}"
    cmake -S "${LLAMA_REPO}" -B "${LLAMA_BUILD}" -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CUDA_COMPILER="${CUDACXX}" \
        -DCMAKE_CUDA_ARCHITECTURES=120 \
        -DGGML_CUDA=ON \
        -DLLAMA_BUILD_SERVER=ON
    local jobs
    jobs="$(nproc)"
    (( jobs > 8 )) && jobs=8
    cmake --build "${LLAMA_BUILD}" --target llama-server -j "${jobs}"
    touch "${LLAMA_BUILD}/furgen-commit-${LLAMA_CPP_COMMIT}"
}

stop_previous() {
    for pid_file in "${LOG_DIR}/asset_gen_v7_lite_llama.pid" "${LOG_DIR}/asset_gen_v7_lite_gateway.pid"; do
        if [[ -f "${pid_file}" ]]; then
            local pid
            pid="$(cat "${pid_file}" 2>/dev/null || true)"
            if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
                kill "${pid}" || true
                sleep 2
            fi
        fi
    done
}

launch() {
    curl -fsS -X POST -H 'Content-Type: application/json' \
        -d '{"unload_models":true,"free_memory":true}' \
        "${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}/free" >/dev/null
    stop_previous
    local llama_dir
    llama_dir="$(dirname "${LLAMA_SERVER}")"
    export LD_LIBRARY_PATH="${llama_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    nohup "${LLAMA_SERVER}" \
        --host 127.0.0.1 \
        --port "${LLAMA_PORT}" \
        --model "${MODEL_PATH}" \
        --alias "${MODEL_ALIAS}" \
        --ctx-size 131072 \
        --batch-size 2048 \
        --ubatch-size 512 \
        --parallel 1 \
        --n-gpu-layers 999 \
        --metrics \
        --flash-attn on \
        --cache-type-k q8_0 \
        --cache-type-v q8_0 \
        --cache-prompt \
        --cont-batching \
        --jinja \
        --sleep-idle-seconds 5 \
        --spec-type draft-mtp \
        --spec-draft-n-max 1 \
        --reasoning-format deepseek \
        --api-key "${INFERENCE_INSTANCE_API_KEY}" \
        >>"${LOG_DIR}/asset_gen_v7_lite_llama.log" 2>&1 &
    echo $! > "${LOG_DIR}/asset_gen_v7_lite_llama.pid"

    local healthy=0
    for _ in $(seq 1 240); do
        if curl -fsS -H "Authorization: Bearer ${INFERENCE_INSTANCE_API_KEY}" \
            "http://127.0.0.1:${LLAMA_PORT}/health" >/dev/null 2>&1; then
            healthy=1
            break
        fi
        sleep 5
    done
    if [[ "${healthy}" != "1" ]]; then
        tail -n 250 "${LOG_DIR}/asset_gen_v7_lite_llama.log" >&2 || true
        return 1
    fi

    nohup env \
        QWEN_GATEWAY_PORT="${GATEWAY_PORT}" \
        QWEN_LLAMA_BASE_URL="http://127.0.0.1:${LLAMA_PORT}" \
        INFERENCE_INSTANCE_API_KEY="${INFERENCE_INSTANCE_API_KEY}" \
        DM_LOCAL_COMFY_BASE_URL="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}" \
        python3 "${GATEWAY_SCRIPT}" \
        >>"${LOG_DIR}/asset_gen_v7_lite_gateway.log" 2>&1 &
    echo $! > "${LOG_DIR}/asset_gen_v7_lite_gateway.pid"

    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${GATEWAY_PORT}/health" >/dev/null 2>&1; then
            for _ in $(seq 1 30); do
                if curl -fsS -H "Authorization: Bearer ${INFERENCE_INSTANCE_API_KEY}" \
                    "http://127.0.0.1:${LLAMA_PORT}/props" 2>/dev/null | \
                    python3 -c 'import json,sys; raise SystemExit(0 if json.load(sys.stdin).get("is_sleeping") else 1)' 2>/dev/null; then
                    return 0
                fi
                sleep 1
            done
            echo "ERROR: llama-server did not enter idle sleep after startup." >&2
            return 1
        fi
        sleep 2
    done
    tail -n 200 "${LOG_DIR}/asset_gen_v7_lite_gateway.log" >&2 || true
    return 1
}

verify_model
build_llama_server
launch

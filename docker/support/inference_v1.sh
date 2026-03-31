#!/bin/bash

set -euo pipefail

WORK_ROOT="${WORK_ROOT:-${WORKSPACE:-/root/workspace}}"
LOG_DIR="${LOG_DIR:-${WORK_ROOT}/logs}"
CACHE_ROOT="${HF_HOME:-${WORK_ROOT}/.cache/huggingface}"
MODEL_REPO="${MODEL_REPO:-bartowski/Qwen_Qwen3.5-27B-GGUF}"
MODEL_FILE="${MODEL_FILE:-Qwen_Qwen3.5-27B-Q6_K.gguf}"
MODEL_ALIAS="${MODEL_ALIAS:-inference_v1}"
LLAMA_HOST="${LLAMA_HOST:-0.0.0.0}"
LLAMA_PORT="${LLAMA_PORT:-8080}"
LLAMA_CTX_SIZE="${LLAMA_CTX_SIZE:-224000}"
LLAMA_BATCH_SIZE="${LLAMA_BATCH_SIZE:-512}"
LLAMA_UBATCH_SIZE="${LLAMA_UBATCH_SIZE:-256}"
LLAMA_PARALLEL="${LLAMA_PARALLEL:-1}"
LLAMA_GPU_LAYERS="${LLAMA_GPU_LAYERS:-999}"
LLAMA_FLASH_ATTN="${LLAMA_FLASH_ATTN:-1}"
LLAMA_CACHE_TYPE_K="${LLAMA_CACHE_TYPE_K:-q8_0}"
LLAMA_CACHE_TYPE_V="${LLAMA_CACHE_TYPE_V:-q8_0}"
LLAMA_EXTRA_ARGS="${LLAMA_EXTRA_ARGS:-}"
INFERENCE_INSTANCE_API_KEY="${INFERENCE_INSTANCE_API_KEY:-}"
MODEL_DIR="${MODEL_DIR:-${WORK_ROOT}/models/inference_v1}"
MODEL_PATH="${MODEL_DIR}/${MODEL_FILE}"
HEALTH_URL="http://127.0.0.1:${LLAMA_PORT}/health"
SERVER_LOG="${LOG_DIR}/inference_v1.log"
SERVER_PID_FILE="${LOG_DIR}/inference_v1.pid"

mkdir -p "${WORK_ROOT}" "${LOG_DIR}" "${CACHE_ROOT}" "${MODEL_DIR}"
export HF_HOME="${CACHE_ROOT}"
export HUGGINGFACE_HUB_CACHE="${CACHE_ROOT}"

find_llama_server() {
  if command -v llama-server >/dev/null 2>&1; then
    command -v llama-server
    return 0
  fi
  for candidate in \
    /app/llama-server \
    /llama.cpp/build/bin/llama-server \
    /opt/llama.cpp/build/bin/llama-server \
    /app/llama.cpp/build/bin/llama-server; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

find_nvcc() {
  if command -v nvcc >/dev/null 2>&1; then
    command -v nvcc
    return 0
  fi
  for candidate in \
    /usr/local/cuda/bin/nvcc \
    /usr/bin/nvcc \
    /opt/cuda/bin/nvcc; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

install_build_deps() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install --no-install-recommends -y \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    git \
    ninja-build \
    pkg-config \
    python3-pip
}

ensure_pip() {
  if python3 -m pip --version >/dev/null 2>&1; then
    return 0
  fi

  if python3 -m ensurepip --upgrade >/dev/null 2>&1; then
    return 0
  fi

  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install --no-install-recommends -y python3-pip
}

build_llama_server() {
  local repo_dir="${WORK_ROOT}/src/llama.cpp"
  local build_dir="${repo_dir}/build"
  local nvcc_path

  install_build_deps
  mkdir -p "${WORK_ROOT}/src"

  if ! nvcc_path="$(find_nvcc)"; then
    echo "No CUDA compiler found for llama.cpp build fallback." >&2
    return 1
  fi
  export CUDACXX="${nvcc_path}"
  export PATH="$(dirname "${nvcc_path}"):${PATH}"

  if [[ ! -d "${repo_dir}/.git" ]]; then
    git clone --depth 1 https://github.com/ggml-org/llama.cpp.git "${repo_dir}"
  else
    git -C "${repo_dir}" fetch --depth 1 origin
    git -C "${repo_dir}" reset --hard origin/master
  fi

  cmake -S "${repo_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DGGML_CUDA=ON \
    -DLLAMA_BUILD_SERVER=ON
  cmake --build "${build_dir}" --target llama-server -j
  echo "${build_dir}/bin/llama-server"
}

install_hf_cli() {
  if python3 -c "import huggingface_hub" >/dev/null 2>&1; then
    return 0
  fi
  ensure_pip
  python3 -m pip install --no-cache-dir --upgrade huggingface_hub
}

download_model() {
  if [[ -f "${MODEL_PATH}" ]]; then
    echo "Model already present at ${MODEL_PATH}"
    return 0
  fi

  install_hf_cli

  MODEL_REPO="${MODEL_REPO}" MODEL_FILE="${MODEL_FILE}" MODEL_DIR="${MODEL_DIR}" HF_TOKEN="${HF_TOKEN:-}" python3 - <<'PY'
import os
from huggingface_hub import hf_hub_download

hf_hub_download(
    repo_id=os.environ["MODEL_REPO"],
    filename=os.environ["MODEL_FILE"],
    local_dir=os.environ["MODEL_DIR"],
    token=os.environ.get("HF_TOKEN") or None,
)
PY
}

launch_server() {
  local llama_server_path="$1"
  local llama_server_dir
  llama_server_dir="$(dirname "${llama_server_path}")"
  local -a cmd=(
    "${llama_server_path}"
    --host "${LLAMA_HOST}"
    --port "${LLAMA_PORT}"
    --model "${MODEL_PATH}"
    --alias "${MODEL_ALIAS}"
    --ctx-size "${LLAMA_CTX_SIZE}"
    --batch-size "${LLAMA_BATCH_SIZE}"
    --ubatch-size "${LLAMA_UBATCH_SIZE}"
    --parallel "${LLAMA_PARALLEL}"
    --n-gpu-layers "${LLAMA_GPU_LAYERS}"
    --metrics
  )

  if [[ "${LLAMA_FLASH_ATTN}" == "1" ]]; then
    cmd+=(--flash-attn on)
  fi

  if [[ -n "${LLAMA_CACHE_TYPE_K}" ]]; then
    cmd+=(--cache-type-k "${LLAMA_CACHE_TYPE_K}")
  fi

  if [[ -n "${LLAMA_CACHE_TYPE_V}" ]]; then
    cmd+=(--cache-type-v "${LLAMA_CACHE_TYPE_V}")
  fi

  if [[ -n "${INFERENCE_INSTANCE_API_KEY}" ]]; then
    cmd+=(--api-key "${INFERENCE_INSTANCE_API_KEY}")
  fi

  if [[ -n "${LLAMA_EXTRA_ARGS}" ]]; then
    # shellcheck disable=SC2206
    local extra_args=( ${LLAMA_EXTRA_ARGS} )
    cmd+=("${extra_args[@]}")
  fi

  export LD_LIBRARY_PATH="${llama_server_dir}:${LD_LIBRARY_PATH:-}"
  nohup "${cmd[@]}" >>"${SERVER_LOG}" 2>&1 &
  echo $! > "${SERVER_PID_FILE}"
}

wait_for_health() {
  local attempts="${1:-120}"
  local sleep_seconds="${2:-5}"
  local last_code=""

  for ((i=1; i<=attempts; i++)); do
    last_code="$(curl -s -o /dev/null -w "%{http_code}" "${HEALTH_URL}" || true)"
    if [[ "${last_code}" == "200" ]]; then
      echo "Inference server is healthy at ${HEALTH_URL}"
      return 0
    fi
    sleep "${sleep_seconds}"
  done

  echo "Inference server failed health check after ${attempts} attempts. Last code=${last_code}" >&2
  if [[ -f "${SERVER_LOG}" ]]; then
    tail -n 200 "${SERVER_LOG}" >&2 || true
  fi
  return 1
}

main() {
  local llama_server_path
  if ! llama_server_path="$(find_llama_server)"; then
    llama_server_path="$(build_llama_server)"
  fi

  download_model
  launch_server "${llama_server_path}"
  wait_for_health 180 5
}

main "$@"

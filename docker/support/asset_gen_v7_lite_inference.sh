#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-/workspace}"
COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/ComfyUI}"
MODEL_FILE="Qwen3.8-27B-Uncensored-Q5_K_M.gguf"
MODEL_PATH="${COMFYUI_DIR}/models/llm/${MODEL_FILE}"
MODEL_SIZE_BYTES="19535701408"
MODEL_SHA256="24780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"
VISION_FILE="Qwen3.8-27B-Uncensored-vision-f16.gguf"
VISION_PATH="${COMFYUI_DIR}/models/llm/${VISION_FILE}"
VISION_SIZE_BYTES="927606912"
VISION_SHA256="5ac423f8a29059dc24e51bc6a43e9380dcd57a9347f28b62591e0b3f60b7081c"
MODEL_ALIAS="qwen3.8-27b-uncensored"
LLAMA_CPP_COMMIT="a94d563ed801d1da1b8c2432946de07d0231bb3d"
LLAMA_REPO="${WORKSPACE}/src/llama.cpp"
LLAMA_BUILD="${LLAMA_REPO}/build-asset-gen-v7-lite"
LLAMA_SERVER="${LLAMA_BUILD}/bin/llama-server"
LLAMA_PORT="8081"
GATEWAY_PORT="8080"
LOG_DIR="${WORKSPACE}/logs"
GATEWAY_SCRIPT="${WORKSPACE}/asset_gen_v7_lite_gateway.py"
SNAPSHOT_PATH="${QWEN_SNAPSHOT_PATH:-${WORKSPACE}/cache/qwen-slots}"
GPU_COORDINATOR_MODE="${GPU_COORDINATOR_MODE:-shadow}"

mkdir -p "${LOG_DIR}" "$(dirname "${MODEL_PATH}")" "${WORKSPACE}/src" "${SNAPSHOT_PATH}"
chmod 700 "${SNAPSHOT_PATH}"

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
    if [[ ! -f "${VISION_PATH}" ]]; then
        echo "ERROR: Vision projector dependency missing: ${VISION_PATH}" >&2
        return 1
    fi
    size="$(stat -c '%s' "${VISION_PATH}")"
    [[ "${size}" == "${VISION_SIZE_BYTES}" ]] || {
        echo "ERROR: Vision projector size mismatch: expected ${VISION_SIZE_BYTES}, got ${size}" >&2
        return 1
    }
    sha="$(sha256sum "${VISION_PATH}" | awk '{print $1}')"
    [[ "${sha}" == "${VISION_SHA256}" ]] || {
        echo "ERROR: Vision projector SHA mismatch: expected ${VISION_SHA256}, got ${sha}" >&2
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
        cuda-nvcc-13-2 libcublas-dev-13-2 vmtouch
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

protect_comfy_cublas_resolution() {
    local launch_script="/opt/supervisor-scripts/comfyui.sh"
    [[ -f "${launch_script}" ]] || return 0
    /venv/main/bin/python3 - "${launch_script}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old = (
    "/venv/main/lib:/venv/main/lib/python3.12/site-packages/torch/lib:"
    "/opt/miniforge3/lib:/usr/local/cuda/lib64:/usr/lib/x86_64-linux-gnu:"
    "/venv/main/lib/python3.12/site-packages/nvidia/cu13/lib"
)
new = (
    "/venv/main/lib/python3.12/site-packages/nvidia/cu13/lib:"
    "/venv/main/lib:/venv/main/lib/python3.12/site-packages/torch/lib:"
    "/opt/miniforge3/lib:/usr/local/cuda/lib64:/usr/lib/x86_64-linux-gnu"
)
if old in source:
    path.write_text(source.replace(old, new), encoding="utf-8")
    print("Pinned ComfyUI to PyTorch's bundled CUDA 13 CUBLAS runtime.")
elif new not in source:
    raise SystemExit("ERROR: Unrecognized ComfyUI LD_LIBRARY_PATH bootstrap; refusing a mixed CUBLAS runtime.")
PY

    # SSH-style Vast templates do not have supervisor start ComfyUI while the
    # on-start hook owns /.provisioning. Allow this script's health-checked
    # launch to bypass only that wait; normal launches retain the guard.
    /venv/main/bin/python3 - "${launch_script}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old = 'while [ -f "/.provisioning" ]; do\n'
new = 'while [ -f "/.provisioning" ] && [[ "${FURGEN_SKIP_PROVISIONING_WAIT:-false}" != "true" ]]; do\n'
if old in source:
    path.write_text(source.replace(old, new, 1), encoding="utf-8")
elif new not in source:
    raise SystemExit("ERROR: Unrecognized ComfyUI provisioning wait guard.")
PY
}

ensure_comfy_running() {
    local comfy_base="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}"
    if curl -fsS "${comfy_base}/system_stats" >/dev/null 2>&1; then
        return 0
    fi
    mkdir -p "${LOG_DIR}"
    nohup env FURGEN_SKIP_PROVISIONING_WAIT=true SERVERLESS=true \
        /opt/supervisor-scripts/comfyui.sh \
        >>"${LOG_DIR}/asset_gen_v7_lite_comfy.log" 2>&1 &
    echo $! > "${LOG_DIR}/asset_gen_v7_lite_comfy.pid"
    for _ in $(seq 1 120); do
        if curl -fsS "${comfy_base}/system_stats" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    tail -n 200 "${LOG_DIR}/asset_gen_v7_lite_comfy.log" >&2 || true
    echo "ERROR: ComfyUI did not become locally reachable." >&2
    return 1
}

process_start_time() {
    local pid="$1"
    [[ "${pid}" =~ ^[0-9]+$ ]] || return 1
    awk '{print $22}' "/proc/${pid}/stat" 2>/dev/null
}

process_identity_matches() {
    local pid="$1" expected_start_time="$2"
    [[ -n "${expected_start_time}" ]] || return 1
    [[ "$(process_start_time "${pid}" || true)" == "${expected_start_time}" ]]
}

calibrate_comfy_gpu_baseline() {
    python3 - <<'PY'
import os
from pathlib import Path
import subprocess
import time

samples = []
for _ in range(5):
    result = subprocess.run(
        ["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        raise SystemExit(1)
    used = 0
    for row in result.stdout.splitlines():
        if not row.strip():
            continue
        raw_pid, raw_mib = row.split(",", 1)
        pid = int(raw_pid.strip())
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").lower()
            cwd = os.readlink(f"/proc/{pid}/cwd").lower()
        except OSError:
            continue
        if "comfyui" in cmdline or "comfyui" in cwd:
            used += int(raw_mib.strip()) * 1024**2
    samples.append(used)
    time.sleep(0.25)
if not samples:
    raise SystemExit(1)
print(max(samples[-3:]))
PY
}

assert_restart_quiescent() {
    local coordinator_status drain_response queue_status gateway_pid
    # A failed warm-owner eviction can still atomically set draining=true
    # before returning 503. Preserve that response and let the status fence
    # below decide whether active work is gone instead of mistaking it for a
    # legacy coordinator with no drain endpoint.
    drain_response="$(curl -sS --max-time 20 -X POST -H 'Content-Type: application/json' -d '{}' \
        "http://127.0.0.1:${GPU_COORDINATOR_PORT:-8189}/v1/gpu/drain" 2>/dev/null || true)"
    if [[ -n "${drain_response}" ]]; then
        local drained=0
        for _ in $(seq 1 900); do
            coordinator_status="$(curl -fsS --max-time 3 \
                "http://127.0.0.1:${GPU_COORDINATOR_PORT:-8189}/v1/gpu/status" 2>/dev/null || true)"
            if [[ -n "${coordinator_status}" ]] && python3 -c '
import json, sys
status = json.load(sys.stdin)
lease = status.get("lease") if isinstance(status.get("lease"), dict) else {}
state = str(lease.get("state") or "").upper()
raise SystemExit(0 if status.get("draining") is True and status.get("diagnosticsBusy") is not True and state not in {"ACTIVE", "RECOVERING", "STARTING"} else 42)
' <<<"${coordinator_status}"; then
                drained=1
                break
            fi
            sleep 1
        done
        if [[ "${drained}" != "1" ]]; then
            echo "ERROR: coordinator did not drain active GPU work before restart." >&2
            return 1
        fi
    else
        gateway_pid="$(cat "${LOG_DIR}/asset_gen_v7_lite_gateway.pid" 2>/dev/null || true)"
        if [[ "${gateway_pid}" =~ ^[0-9]+$ ]] && kill -0 "${gateway_pid}" 2>/dev/null; then
            echo "ERROR: refusing an in-place legacy gateway upgrade without a coordinator drain fence; recycle the idle worker." >&2
            return 1
        fi
    fi
    if [[ "${GPU_COORDINATOR_MODE}" == "enforcing" && -z "${drain_response}" ]]; then
        echo "ERROR: refusing enforcing-mode restart without a loopback coordinator drain fence." >&2
        return 1
    fi

    queue_status="$(curl -fsS --max-time 3 \
        "${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}/queue" 2>/dev/null || true)"
    if [[ -z "${queue_status}" ]] || ! python3 -c '
import json, sys
queue = json.load(sys.stdin)
running = queue.get("queue_running") or []
pending = queue.get("queue_pending") or []
raise SystemExit(0 if not running and not pending else 42)
' <<<"${queue_status}"; then
        echo "ERROR: refusing inference restart while ComfyUI queue state is active, pending, or unknown." >&2
        return 1
    fi
}

stop_previous() {
    local gateway_pid_file="${LOG_DIR}/asset_gen_v7_lite_gateway.pid"
    local gateway_start_file="${gateway_pid_file}.start"
    local llama_pid_file="${LOG_DIR}/asset_gen_v7_lite_llama.pid"
    local llama_start_file="${llama_pid_file}.start"
    local pid expected_start_time
    # Quiesce the owner first; otherwise its coordinator can observe a stopped
    # llama and restart it while provisioning is trying to replace processes.
    pid="$(cat "${gateway_pid_file}" 2>/dev/null || true)"
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
        expected_start_time="$(cat "${gateway_start_file}" 2>/dev/null || true)"
        if process_identity_matches "${pid}" "${expected_start_time}" && \
            tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null | grep -Fq "${GATEWAY_SCRIPT}"; then
            kill "${pid}" || true
            for _ in $(seq 1 50); do
                process_identity_matches "${pid}" "${expected_start_time}" || break
                sleep 0.1
            done
            if process_identity_matches "${pid}" "${expected_start_time}"; then
                kill -9 "${pid}" || true
                for _ in $(seq 1 50); do
                    process_identity_matches "${pid}" "${expected_start_time}" || break
                    sleep 0.1
                done
            fi
            if process_identity_matches "${pid}" "${expected_start_time}"; then
                echo "ERROR: prior inference gateway did not exit." >&2
                return 1
            fi
        else
            echo "ERROR: refusing to signal stale or non-gateway PID ${pid}." >&2
            return 1
        fi
    fi
    pid="$(cat "${llama_pid_file}" 2>/dev/null || true)"
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
        expected_start_time="$(cat "${llama_start_file}" 2>/dev/null || true)"
        if process_identity_matches "${pid}" "${expected_start_time}" && \
            tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null | grep -q 'llama-server'; then
            local pgid
            pgid="$(ps -o pgid= -p "${pid}" 2>/dev/null | tr -d ' ' || true)"
            if [[ "${pgid}" == "${pid}" ]]; then kill -TERM -- "-${pgid}" || true; else kill "${pid}" || true; fi
            for _ in $(seq 1 100); do
                process_identity_matches "${pid}" "${expected_start_time}" || break
                sleep 0.1
            done
            if process_identity_matches "${pid}" "${expected_start_time}"; then
                if [[ "${pgid}" == "${pid}" ]]; then kill -KILL -- "-${pgid}" || true; else kill -9 "${pid}" || true; fi
                for _ in $(seq 1 50); do
                    process_identity_matches "${pid}" "${expected_start_time}" || break
                    sleep 0.1
                done
            fi
            if process_identity_matches "${pid}" "${expected_start_time}"; then
                echo "ERROR: prior llama process did not exit." >&2
                return 1
            fi
        else
            echo "ERROR: refusing to signal stale non-llama PID ${pid}." >&2
            return 1
        fi
    fi
    rm -f "${gateway_pid_file}" "${gateway_start_file}" "${llama_pid_file}" "${llama_start_file}"
}

launch() {
    ensure_comfy_running
    assert_restart_quiescent
    stop_previous
    # The quiescence checks above prove no active lease can need adoption.
    # Retain the separately persisted epoch so every old fence remains stale.
    rm -f "${GPU_COORDINATOR_STATE_FILE:-${LOG_DIR}/asset_gen_v7_lite_coordinator.json}"
    curl -fsS -X POST -H 'Content-Type: application/json' \
        -d '{"unload_models":true,"free_memory":true}' \
        "${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}/free" >/dev/null
    local comfy_idle_baseline
    local comfy_release_max
    comfy_release_max="${GPU_COMFY_RELEASE_VRAM_MAX_BYTES:-3221225472}"
    if [[ ! "${comfy_release_max}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: GPU_COMFY_RELEASE_VRAM_MAX_BYTES must be an integer byte count." >&2
        return 1
    fi
    comfy_idle_baseline="${GPU_COMFY_IDLE_BASELINE_BYTES:-}"
    if [[ -z "${comfy_idle_baseline}" ]]; then
        comfy_idle_baseline="$(calibrate_comfy_gpu_baseline || true)"
    fi
    if [[ ! "${comfy_idle_baseline}" =~ ^[0-9]+$ ]]; then
        echo "WARNING: Comfy VRAM baseline calibration failed; using the absolute release ceiling." >&2
        comfy_idle_baseline=""
    else
        echo "Calibrated idle Comfy VRAM baseline: ${comfy_idle_baseline} bytes."
        if (( comfy_idle_baseline > comfy_release_max )); then
            echo "ERROR: idle Comfy VRAM baseline ${comfy_idle_baseline} exceeds hard ceiling ${comfy_release_max}." >&2
            return 1
        fi
    fi
    local llama_dir
    llama_dir="$(dirname "${LLAMA_SERVER}")"
    export LD_LIBRARY_PATH="${llama_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    # The common Vast limit for locked memory is only 64 KiB. Warm the complete
    # Q5 model and projector into the Linux page cache instead; this worker has
    # ample host RAM and mmap reloads can then avoid worker-disk I/O.
    vmtouch -t "${MODEL_PATH}" "${VISION_PATH}"
    local sleep_args=()
    if [[ "${GPU_COORDINATOR_MODE}" != "enforcing" ]]; then
        # Shadow and legacy retain the proven safety behavior during rollout.
        sleep_args=(--sleep-idle-seconds 1)
    fi
    nohup setsid "${LLAMA_SERVER}" \
        --host 127.0.0.1 \
        --port "${LLAMA_PORT}" \
        --model "${MODEL_PATH}" \
        --mmproj "${VISION_PATH}" \
        --alias "${MODEL_ALIAS}" \
        --ctx-size 131072 \
        --batch-size 2048 \
        --ubatch-size 512 \
        --parallel 1 \
        --n-gpu-layers 999 \
        --load-mode mmap \
        --image-max-tokens 4096 \
        --mtmd-batch-max-tokens 4096 \
        --metrics \
        --flash-attn on \
        --cache-type-k q8_0 \
        --cache-type-v q8_0 \
        --cache-prompt \
        --cont-batching \
        --jinja \
        --slot-save-path "${SNAPSHOT_PATH}" \
        "${sleep_args[@]}" \
        --spec-type draft-mtp \
        --spec-draft-n-max 1 \
        --reasoning-format deepseek \
        --api-key "${INFERENCE_INSTANCE_API_KEY}" \
        >>"${LOG_DIR}/asset_gen_v7_lite_llama.log" 2>&1 &
    echo $! > "${LOG_DIR}/asset_gen_v7_lite_llama.pid"
    process_start_time "$!" > "${LOG_DIR}/asset_gen_v7_lite_llama.pid.start"

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
        GPU_COORDINATOR_PORT="${GPU_COORDINATOR_PORT:-8189}" \
        GPU_COORDINATOR_MODE="${GPU_COORDINATOR_MODE}" \
        GPU_COORDINATOR_STATE_FILE="${GPU_COORDINATOR_STATE_FILE:-${LOG_DIR}/asset_gen_v7_lite_coordinator.json}" \
        GPU_COORDINATOR_EPOCH_FILE="${GPU_COORDINATOR_EPOCH_FILE:-${LOG_DIR}/asset_gen_v7_lite_coordinator.epoch}" \
        GPU_MINING_GRACE_SECONDS="${GPU_MINING_GRACE_SECONDS:-30}" \
        GPU_COMFY_RELEASE_VRAM_MAX_BYTES="${comfy_release_max}" \
        GPU_COMFY_IDLE_BASELINE_BYTES="${comfy_idle_baseline}" \
        GPU_COMFY_RELEASE_VRAM_HEADROOM_BYTES="${GPU_COMFY_RELEASE_VRAM_HEADROOM_BYTES:-536870912}" \
        WARM_RESIDENCY_ENABLED="${WARM_RESIDENCY_ENABLED:-false}" \
        SNAPSHOT_WRITE_ENABLED="${SNAPSHOT_WRITE_ENABLED:-false}" \
        SNAPSHOT_RESTORE_ENABLED="${SNAPSHOT_RESTORE_ENABLED:-false}" \
        QWEN_SNAPSHOT_PATH="${SNAPSHOT_PATH}" \
        QWEN_SNAPSHOT_TTL_SECONDS="${QWEN_SNAPSHOT_TTL_SECONDS:-172800}" \
        QWEN_SNAPSHOT_QUOTA_BYTES="${QWEN_SNAPSHOT_QUOTA_BYTES:-51539607552}" \
        QWEN_SNAPSHOT_MIN_FREE_BYTES="${QWEN_SNAPSHOT_MIN_FREE_BYTES:-21474836480}" \
        QWEN_SNAPSHOT_MAX_ENTRY_BYTES="${QWEN_SNAPSHOT_MAX_ENTRY_BYTES:-17179869184}" \
        QWEN_SNAPSHOT_FINGERPRINT="${QWEN_SNAPSHOT_FINGERPRINT:-fmt=llama-slot-v1:model=${MODEL_SHA256}:mmproj=${VISION_SHA256}:llama=${LLAMA_CPP_COMMIT}:tokenizer=model-embedded:chat=jinja-model-embedded:quant=q5_k_m:ctx=131072:slots=1:kvk=q8_0:kvv=q8_0:ngl=999:load=mmap:rope=model-default:yarn=model-default:flash=on:batch=2048:ubatch=512:cont=on:image-max=4096:mtmd-batch=4096:spec=mtp1:reasoning=deepseek:layout=v1}" \
        QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN="${QWEN_PREFILL_ESTIMATE_MS_PER_TOKEN:-0.5}" \
        QWEN_LLAMA_PID_FILE="${LOG_DIR}/asset_gen_v7_lite_llama.pid" \
        QWEN_LLAMA_LOG_FILE="${LOG_DIR}/asset_gen_v7_lite_llama.log" \
        QWEN_LLAMA_COMMAND_FILE="${QWEN_LLAMA_COMMAND_FILE:-${LOG_DIR}/asset_gen_v7_lite_llama_command.json}" \
        QWEN_SLEEP_POLL_SECONDS="0.1" \
        QWEN_MAX_BODY_BYTES="$((32 * 1024 * 1024))" \
        INFERENCE_INSTANCE_API_KEY="${INFERENCE_INSTANCE_API_KEY}" \
        DM_LOCAL_COMFY_BASE_URL="${DM_LOCAL_COMFY_BASE_URL:-http://127.0.0.1:8188}" \
        python3 "${GATEWAY_SCRIPT}" \
        >>"${LOG_DIR}/asset_gen_v7_lite_gateway.log" 2>&1 &
    echo $! > "${LOG_DIR}/asset_gen_v7_lite_gateway.pid"
    process_start_time "$!" > "${LOG_DIR}/asset_gen_v7_lite_gateway.pid.start"

    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${GATEWAY_PORT}/health" >/dev/null 2>&1; then
            if [[ "${GPU_COORDINATOR_MODE}" == "enforcing" ]]; then
                curl -fsS "http://127.0.0.1:${GPU_COORDINATOR_PORT:-8189}/v1/gpu/status" >/dev/null
                return 0
            fi
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
protect_comfy_cublas_resolution
launch

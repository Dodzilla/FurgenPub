#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-/workspace}"
SERVER_TYPE="${SERVER_TYPE:-prl_mining_v1}"
DM_MINING_ONLY="${DM_MINING_ONLY:-1}"
DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-${WORKSPACE}/mining-runtime}"
DM_AGENT_PATH="${DM_AGENT_PATH:-${WORKSPACE}/dependency_agent_v1.py}"
DM_AGENT_LOG_PATH="${DM_AGENT_LOG_PATH:-${WORKSPACE}/dependency_agent.log}"
DM_AGENT_PID_PATH="${DM_AGENT_PID_PATH:-${WORKSPACE}/dependency_agent.pid}"
DM_AGENT_WATCHDOG_PATH="${DM_AGENT_WATCHDOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.sh}"
DM_AGENT_WATCHDOG_LOG_PATH="${DM_AGENT_WATCHDOG_LOG_PATH:-${WORKSPACE}/dependency_agent_watchdog.log}"
DM_AGENT_WATCHDOG_PID_PATH="${DM_AGENT_WATCHDOG_PID_PATH:-${WORKSPACE}/dependency_agent_watchdog.pid}"
DM_AGENT_POLL_SECONDS="${DM_AGENT_POLL_SECONDS:-1}"
DM_AGENT_QUEUE_WAIT_SEC="${DM_AGENT_QUEUE_WAIT_SEC:-1}"
DM_AGENT_HEARTBEAT_SECONDS="${DM_AGENT_HEARTBEAT_SECONDS:-3}"
DM_AGENT_WATCHDOG_SECONDS="${DM_AGENT_WATCHDOG_SECONDS:-3}"
DM_POLL_SECONDS="${DM_POLL_SECONDS:-60}"
DM_HEARTBEAT_SECONDS="${DM_HEARTBEAT_SECONDS:-300}"
DM_AGENT_MAX_EXEC_WORKERS="${DM_AGENT_MAX_EXEC_WORKERS:-0}"
AGENT_URL="${DM_AGENT_URL:-${AGENT_URL:-}}"
FALLBACK_AGENT_URL="${FALLBACK_AGENT_URL:-https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py}"

export WORKSPACE SERVER_TYPE DM_MINING_ONLY DM_COMFYUI_DIR
export DM_AGENT_PATH DM_AGENT_LOG_PATH DM_AGENT_PID_PATH
export DM_AGENT_POLL_SECONDS DM_AGENT_QUEUE_WAIT_SEC DM_AGENT_HEARTBEAT_SECONDS
export DM_POLL_SECONDS DM_HEARTBEAT_SECONDS DM_AGENT_MAX_EXEC_WORKERS

if [[ -z "${DM_INSTANCE_ID:-}" && -n "${VAST_CONTAINERLABEL:-}" ]]; then
    DM_INSTANCE_ID="${VAST_CONTAINERLABEL#C.}"
    export DM_INSTANCE_ID
fi

mkdir -p "${WORKSPACE}" "${DM_COMFYUI_DIR}" "$(dirname "${DM_AGENT_PATH}")"

ensure_runtime_tools() {
    local needs_apt=0
    command -v python3 >/dev/null 2>&1 || needs_apt=1
    command -v curl >/dev/null 2>&1 || needs_apt=1
    if [[ "${needs_apt}" == "0" ]]; then
        return 0
    fi
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update
        apt-get install --no-install-recommends -y ca-certificates curl python3
        rm -rf /var/lib/apt/lists/*
    fi
}

install_agent() {
    local script_dir bundled_path
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    bundled_path="${script_dir}/../scripts/dependency_agent_v1.py"

    if [[ -n "${AGENT_URL}" ]]; then
        echo "PRL mining boot: downloading dependency agent from DM_AGENT_URL/AGENT_URL."
        curl -fsSL "${AGENT_URL}" -o "${DM_AGENT_PATH}"
    elif [[ -s "${bundled_path}" ]]; then
        echo "PRL mining boot: installing bundled dependency agent."
        cp -f "${bundled_path}" "${DM_AGENT_PATH}"
    else
        echo "PRL mining boot: downloading dependency agent from fallback URL."
        curl -fsSL "${FALLBACK_AGENT_URL}" -o "${DM_AGENT_PATH}"
    fi

    chmod +x "${DM_AGENT_PATH}" || true
}

agent_running() {
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "${DM_AGENT_PATH}" >/dev/null 2>&1; then
        return 0
    fi
    if [[ -f "${DM_AGENT_PID_PATH}" ]]; then
        local pid
        pid="$(cat "${DM_AGENT_PID_PATH}" 2>/dev/null || true)"
        if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

start_agent_once() {
    if agent_running; then
        return 0
    fi
    install_agent
    echo "PRL mining boot: starting dependency agent; log=${DM_AGENT_LOG_PATH}"
    nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; exec python3 '${DM_AGENT_PATH}' >> '${DM_AGENT_LOG_PATH}' 2>&1" >/dev/null 2>&1 &
    echo $! > "${DM_AGENT_PID_PATH}"
}

render_watchdog() {
    cat > "${DM_AGENT_WATCHDOG_PATH}" <<'EOF'
#!/bin/bash
set -euo pipefail

agent_path="${DM_AGENT_PATH:-${WORKSPACE:-/workspace}/dependency_agent_v1.py}"
pid_path="${DM_AGENT_PID_PATH:-${WORKSPACE:-/workspace}/dependency_agent.pid}"
log_path="${DM_AGENT_LOG_PATH:-${WORKSPACE:-/workspace}/dependency_agent.log}"
sleep_seconds="${DM_AGENT_WATCHDOG_SECONDS:-3}"

agent_running() {
    if command -v pgrep >/dev/null 2>&1 && pgrep -f "${agent_path}" >/dev/null 2>&1; then
        return 0
    fi
    if [[ -f "${pid_path}" ]]; then
        local pid
        pid="$(cat "${pid_path}" 2>/dev/null || true)"
        if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

while true; do
    if ! agent_running; then
        echo "PRL mining watchdog: starting dependency agent; log=${log_path}"
        nohup bash -lc "if [[ -f /venv/main/bin/activate ]]; then source /venv/main/bin/activate; fi; exec python3 '${agent_path}' >> '${log_path}' 2>&1" >/dev/null 2>&1 &
        echo $! > "${pid_path}"
    fi
    sleep "${sleep_seconds}"
done
EOF
    chmod +x "${DM_AGENT_WATCHDOG_PATH}" || true
}

start_watchdog() {
    render_watchdog
    if [[ -f "${DM_AGENT_WATCHDOG_PID_PATH}" ]]; then
        local existing_pid
        existing_pid="$(cat "${DM_AGENT_WATCHDOG_PID_PATH}" 2>/dev/null || true)"
        if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
            return 0
        fi
    fi
    nohup "${DM_AGENT_WATCHDOG_PATH}" >> "${DM_AGENT_WATCHDOG_LOG_PATH}" 2>&1 &
    echo $! > "${DM_AGENT_WATCHDOG_PID_PATH}"
}

ensure_runtime_tools
start_agent_once
start_watchdog

echo "PRL mining boot: agent is running in mining-only mode for SERVER_TYPE=${SERVER_TYPE}."

if [[ "${DM_MINING_KEEP_FOREGROUND:-1}" == "1" || "${DM_MINING_KEEP_FOREGROUND:-1}" == "true" ]]; then
    while true; do
        sleep 3600
    done
fi

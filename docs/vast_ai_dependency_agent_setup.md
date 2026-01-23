# Vast.ai dependency agent setup (dependency_manager_v1)

This runbook installs and runs the single-file dependency agent (`scripts/dependency_agent_v1.py`) on a Vast.ai ComfyUI instance so the backend can queue/download model dependencies on-demand.

## Preconditions (backend)

- Firebase Functions is deployed with the dependency endpoints mounted at `FCS_API_BASE_URL`:
  - `POST /api/dependencies/register`
  - `GET /api/dependencies/queue`
  - `POST /api/dependencies/status`
  - `POST /api/dependencies/heartbeat`
- `DEPENDENCY_MANAGER_SHARED_SECRET` is configured on the backend (Functions env/secret) and you know its value.
- The instance exists in Firestore as `vastInstances/{instanceId}` and has the correct `ip` field (used for registration when instanceId is not injected).
- (Recommended) `comfyUIServerTypes/{serverType}.modelManagement.mode = "dependency_manager_v1"`.
- (Recommended) `serverTypeDependencyProfiles/{profileId}` exists and `prefetchOnBoot=true` so core/warm static deps are queued at agent registration.

## Instance environment variables

Required:
- `FCS_API_BASE_URL` (must end with `/api`)  
  Example: `https://us-central1-<projectId>.cloudfunctions.net/api`
- `SERVER_TYPE`  
  Example: `furry-standard-v8`

Recommended auth:
- `DEPENDENCY_MANAGER_SHARED_SECRET` (same value as backend)

Optional identity overrides (use one if you can):
- `DM_INSTANCE_ID` (best; avoids IP detection entirely)
- `DM_INSTANCE_IP` (if you already know the public IP)

Optional download tokens (only required when dependency entries specify them):
- `HF_TOKEN`
- `CIVITAI_TOKEN`

Optional behavior knobs:
- `WORKSPACE` (default `/workspace`)
- `DM_COMFYUI_DIR` (default `$WORKSPACE/ComfyUI`)
- `DM_STATE_PATH` (default `$WORKSPACE/dependency_agent_state.json`)
- `DM_POLL_SECONDS` (default `5`)
- `DM_HEARTBEAT_SECONDS` (default `30`)
- `MAX_PARALLEL_DOWNLOADS` (default `1`)
- `DM_VERBOSE_PROGRESS` (`1`/`true` to log periodic download progress; default off)
- `DM_ALLOWED_DOMAINS` (default `huggingface.co,hf.co,civitai.com`)
- Dynamic cache eviction overrides (normally driven by `profile.dynamicPolicy` returned at register):
  - `DM_DYNAMIC_EVICTION_ENABLED` (`1`/`true` to enable)
  - `DM_DYNAMIC_MIN_FREE_BYTES` (e.g. `10GB`)
  - `DM_DYNAMIC_MAX_BYTES` (e.g. `50GB`)
  - `DM_EVICTION_BATCH_MAX` (e.g. `20`)
  - `DM_PIN_TTL_SECONDS` (default `1800`)

## Install + run (Vast template “onstart” / startup script)

1) Ensure the env vars above are set in the Vast template.

2) Add this snippet to your startup script (replace `AGENT_URL`):

```bash
set -euo pipefail

export WORKSPACE="${WORKSPACE:-/workspace}"
export DM_COMFYUI_DIR="${DM_COMFYUI_DIR:-$WORKSPACE/ComfyUI}"

AGENT_URL="https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py"
AGENT_PATH="$WORKSPACE/dependency_agent_v1.py"
LOG_PATH="$WORKSPACE/dependency_agent.log"

curl -fsSL "$AGENT_URL" -o "$AGENT_PATH"
chmod +x "$AGENT_PATH" || true

# Start in background. Use bash -lc so env vars from the template are visible.
nohup bash -lc "python3 '$AGENT_PATH' >> '$LOG_PATH' 2>&1" >/dev/null 2>&1 &
```

Notes:
- If your template starts ComfyUI separately, keep doing that; the agent only needs the ComfyUI workspace path, not the ComfyUI process itself.
- If you use a “core-ready” marker file, keep that logic in your provisioning script; the agent is independent of readiness gating.

## Install + run (manual SSH session)

```bash
export WORKSPACE=/workspace
export FCS_API_BASE_URL="https://us-central1-<projectId>.cloudfunctions.net/api"
export SERVER_TYPE="furry-standard-v8"
export DEPENDENCY_MANAGER_SHARED_SECRET="..."

curl -fsSL "https://<your-public-host>/dependency_agent_v1.py" -o "$WORKSPACE/dependency_agent_v1.py"
nohup python3 "$WORKSPACE/dependency_agent_v1.py" > "$WORKSPACE/dependency_agent.log" 2>&1 &
```

## Verification checklist

- Instance log contains a successful register line:  
  `Registered dependency agent: instanceId=...`
- Firestore `vastInstances/{instanceId}.dependencyManager.lastHeartbeatAt` is updating.
- Firestore `vastInstances/{instanceId}.dependencyManager.installedDepIdsStatic|installedDepIdsDynamic` reflects what’s on disk.
- Static prefetch downloads appear as queued/running items under `dependencyQueues/{instanceId}/items/*` (if `prefetchOnBoot=true`).
- Submitting a dependency-managed job results in:
  - job reservation to an instance
  - missing deps queued in `dependencyQueues/{instanceId}/items/{depId}`
  - job proceeds once agent heartbeats show deps installed

## Troubleshooting

- Register fails / instance not found:
  - Set `DM_INSTANCE_ID` (preferred) or `DM_INSTANCE_IP`.
  - Confirm Firestore `vastInstances/*` has the correct `ip`.
- Unauthorized:
  - Confirm `DEPENDENCY_MANAGER_SHARED_SECRET` matches backend `DEPENDENCY_MANAGER_SHARED_SECRET`.
- Download fails with “Missing HF_TOKEN/CIVITAI_TOKEN”:
  - Provide the required token env var in the template.
- Download fails with “Download domain not allowed”:
  - Add the host to `DM_ALLOWED_DOMAINS` (or change the dependency catalog entry to an allowed host).
- Disk space errors / eviction not happening:
  - Ensure `serverTypeDependencyProfiles/{profileId}.dynamicPolicy.enabled=true` (preferred), or set `DM_DYNAMIC_EVICTION_ENABLED=1`.
  - Set `dynamicPolicy.minFreeBytes` and/or `dynamicPolicy.maxDynamicBytes` to sane values for the instance disk size.

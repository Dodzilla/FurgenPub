# video_gen_v2 LTX Color Nodes Handoff

## Branch And Status

- Branch: `main`
- Base commit before implementation: `c1b6d59f77cc9486ea5e5a5cf2f44f15e7b5573d`
- Publication implementation commit SHA: `bebbf9c52093b84d68004332523ecf657f411e42`
- Starting working tree status: clean, `## main...origin/main`
- Current working tree status: clean after publication commits.
- Note: no local or remote `dev` branch exists in this checkout; implementation stayed in the provided checkout and did not create a worktree.

## Exact Files Changed

- `docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py`
- `docker/support/video_gen_v2.sh`
- `docker/scripts/dependency_agent_v1.py`
- `docs/video_gen_v2_ltx_color_nodes_handoff.md`

## Node Class Contract Implemented

- `FurgenExposureAdjust`
  - ComfyUI input: `images` as `IMAGE`.
  - Controls: `brightness_multiplier`, `contrast`, `gamma`, `saturation`.
  - Neutral values are exact pass-through by returning the original tensor object when all controls are `1.0`.
  - Non-neutral processing uses torch tensor operations only, preserves input shape, adjusts RGB channels, preserves any extra channels, and clamps output to `[0, 1]`.
  - Returns `IMAGE`.
- `FurgenReferenceColorMatch`
  - ComfyUI inputs: `images` as `IMAGE`, `reference` as `IMAGE`.
  - Controls: `mode`, `strength`.
  - Modes: `luma_mean_std`, `rgb_mean_std`, `rgb_mean_only`.
  - `strength=0.0` is exact pass-through by returning the original tensor object.
  - `strength=1.0` applies full correction; intermediate values blend from original to corrected.
  - Uses torch only, broadcasts a single reference image across an image batch, preserves shape, adjusts RGB channels, preserves any extra channels, and clamps output to `[0, 1]`.
  - Returns `IMAGE`.
- Registered both classes in `NODE_CLASS_MAPPINGS` and `NODE_DISPLAY_NAME_MAPPINGS` without removing existing `FCSConcatVideos`.

## Provisioning And Agent Behavior

- `docker/support/video_gen_v2.sh`
  - Adds default `FURGENPUB_RAW_BASE_URL=https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support`.
  - Installs managed `FurgenVideoTools` after cloned custom nodes are in place.
  - Uses the existing local-copy/raw-download pattern from `asset_gen_v5.sh`.
- `docker/scripts/dependency_agent_v1.py`
  - Adds bundle id `video_gen_v2_furgen_color_nodes`.
  - Bumps `AGENT_VERSION` to `dm-agent-py/0.10.32` so existing agents can self-update to this script.
  - Installs or updates `FurgenVideoTools` from a local FurgenPub support checkout when available.
  - Falls back to raw download from `FURGENPUB_RAW_BASE_URL`.
  - Verifies ComfyUI exposes `FurgenExposureAdjust` and `FurgenReferenceColorMatch` through the existing local class-type restart verification flow.
  - Keeps existing `video_gen_v2_10s_ltx_nodes` behavior intact.

## Tests Run

- `bash -n docker/support/video_gen_v2.sh`
  - Output: no output; exit code 0.
- `python3 -m py_compile docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py docker/scripts/dependency_agent_v1.py`
  - Output: no output; exit code 0.
- Local torch smoke test with stub `folder_paths`
  - Imported `docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py`.
  - Passed a `torch.rand(2, 3, 4, 3)` image batch through `FurgenExposureAdjust` with all neutral values.
  - Passed the same image batch through `FurgenReferenceColorMatch` with a single reference image and `strength=0.0`.
  - Asserted both neutral outputs preserve original tensor identity.
  - Ran full `luma_mean_std` correction at `strength=1.0` and asserted shape preservation plus clamp range.
  - Output: `smoke ok: neutral pass-through identity preserved; full match shape (2, 3, 4, 3)`.

## Dependency-Agent Version And SHA256

- Agent version: `dm-agent-py/0.10.32`
- `docker/scripts/dependency_agent_v1.py`: `66a3ebf7e2a3342916cc20197268c241a0c3674a1ce183734c3d9bfe13a2c9a6`

## Public Raw URL Expectation

- Expected branch: `main`
- `https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support/custom_nodes/FurgenVideoTools/__init__.py`
- `https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py`

## Readiness

READY_FOR_CONTENTSERVER=true

## Boundaries

- FurgenPub does not edit Firestore/backend policy.
- FurgenPub does not change `furgenai` workflow variants.
- FurgenPub does not deploy live.
- No sibling repos were modified.
- No push or deploy was performed.

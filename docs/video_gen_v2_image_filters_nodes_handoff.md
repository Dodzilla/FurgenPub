# video_gen_v2 Image-Filters Nodes Handoff

## Branch And Status

- Branch: `main`
- Base commit before implementation: `6db3c0274ba4b90c207f796e9716088190883592`
- Implementation commit SHA: `f2ec41125157efa3fd3b07e44a5aab6bd7ae5d10`
- Handoff/publication commit SHA: pending until this document is committed.
- Starting working tree status: clean, `## main...origin/main`
- Current working tree status before publication: ahead of `origin/main` with the implementation commit and this pending handoff document.
- Note: implementation stayed in the provided checkout on `main`; no new worktree or rollout branch was created.

## Exact Files Changed

- `docker/support/video_gen_v2.sh`
- `docker/scripts/dependency_agent_v1.py`
- `docs/video_gen_v2_image_filters_nodes_handoff.md`

## Upstream Image-Filters Pin

- Repository: `https://github.com/spacepxl/ComfyUI-Image-Filters`
- Pinned commit SHA: `bbb3fb0045461adf3602faeedaf40af57090d4e2`
- Upstream branch at pin time: `main`

## Bundle Contract

- Bundle id: `video_gen_v2_image_filters_nodes`
- Target server type: `video_gen_v2` and `video_gen_v2_salad` through the existing video bundle path.
- Required verification class types:
  - `AdainImage`
  - `BatchNormalizeImage`
  - `ColorMatchImage`
  - `ExposureAdjust`
  - `RemapRange`

## Install And Dependency Details

- `docker/support/video_gen_v2.sh`
  - Adds on-demand support for `bash docker/support/video_gen_v2.sh install-bundles video_gen_v2_image_filters_nodes`.
  - Clones `ComfyUI-Image-Filters` into `ComfyUI/custom_nodes/ComfyUI-Image-Filters`.
  - Checks out pinned commit `bbb3fb0045461adf3602faeedaf40af57090d4e2`.
  - Skips upstream `requirements.txt` intentionally because it installs all four OpenCV wheel variants.
  - Installs the managed OpenCV requirement `opencv-contrib-python==4.10.0.84`.
  - By default uninstalls conflicting OpenCV wheels before installing the contrib package: `opencv-python`, `opencv-python-headless`, `opencv-contrib-python-headless`.
  - Verifies `from cv2.ximgproc import guidedFilter`.
- `docker/scripts/dependency_agent_v1.py`
  - Adds bundle id `video_gen_v2_image_filters_nodes`.
  - Installs the upstream repo at the pinned commit with `install_requirements=False`.
  - Adds deterministic OpenCV handling for the Image-Filters bundle using `opencv-contrib-python==4.10.0.84`.
  - Adds class-type verification for the exact five required Image-Filters nodes.
  - Leaves existing `video_gen_v2_10s_ltx_nodes` and `video_gen_v2_furgen_color_nodes` support unchanged.

## Dependency-Agent Release

- Agent version: `dm-agent-py/0.10.33`
- Public raw URL after push: `https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py`
- `docker/scripts/dependency_agent_v1.py` SHA256: `713e94b2a88a1f515cc5191d4a2b440473cd26adf89acee2230936068477ac11`

## Tests Run

- `bash -n docker/support/video_gen_v2.sh`
  - Output: no output; exit code 0.
- `python3 -m py_compile docker/scripts/dependency_agent_v1.py`
  - Output: no output; exit code 0.
- Local cv2 smoke:
  - Command: `python3 - <<'PY' ... import cv2; from cv2.ximgproc import guidedFilter ... PY`
  - Output: `cv2 smoke ok: 4.13.0`
- Pinned upstream static class check:
  - Cloned `https://github.com/spacepxl/ComfyUI-Image-Filters`.
  - Checked out `bbb3fb0045461adf3602faeedaf40af57090d4e2`.
  - Parsed `nodes.py` and verified the required class names and mapping strings.
  - Output: `image filters static smoke ok: AdainImage, BatchNormalizeImage, ColorMatchImage, ExposureAdjust, RemapRange`
- Dependency-agent bundle string smoke:
  - Verified the new bundle id, required class types, upstream commit, and OpenCV pin are present in `docker/scripts/dependency_agent_v1.py`.
  - Output: `dependency-agent bundle string smoke ok`
- `git diff --check`
  - Output: no output; exit code 0.
- Existing repo tests:
  - No package-level test harness was found in this repo for dependency-agent or `video_gen_v2` provisioning.

## Risk And Caveat

- Upstream Image-Filters documents OpenCV/cv2 import conflicts when multiple OpenCV wheel variants or mismatched versions are installed.
- This implementation is intentionally dev-scoped and minimally invasive: the OpenCV cleanup only runs when `video_gen_v2_image_filters_nodes` is requested, not during default `video_gen_v2` provisioning.
- The bundle uses `opencv-contrib-python==4.10.0.84` because Image-Filters needs `cv2.ximgproc.guidedFilter`.
- If another node pack later requires a different OpenCV wheel variant, this Image-Filters lane may need a separate environment or a coordinated OpenCV pin.

## Readiness

READY_FOR_CONTENTSERVER=false

## Boundaries

- FurgenPub does not edit Firestore/backend bundle policy.
- FurgenPub does not change `furgenai` workflow variants.
- FurgenPub does not deploy live.
- No sibling repos were modified.

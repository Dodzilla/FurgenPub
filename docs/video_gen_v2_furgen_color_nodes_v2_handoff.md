# video_gen_v2 Furgen Color Nodes v2 Handoff

## Branch And Status

- Branch: `main`
- Base commit before implementation: `f00e96879b6ea59199f1a6812d1032c4aeebcdfc`
- Implementation commit SHA: `f637bc7ed1ce4ec19816b2a6527dfa9917ed4e3d`
- Handoff publication commit SHA: `f34021239a2e917601452ccf240d282636b93da5`
- Starting working tree status: clean, `## main...origin/main`
- Current publication status: pushed to `origin/main`; final readiness marker update is this document's latest commit.
- Note: implementation stayed in the provided checkout on `main`; no new worktree or rollout branch was created.

## Exact Files Changed

- `docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py`
- `docker/support/video_gen_v2.sh`
- `docker/scripts/dependency_agent_v1.py`
- `docs/video_gen_v2_furgen_color_nodes_v2_handoff.md`

## Bundle Contract

- Bundle id: `video_gen_v2_furgen_color_nodes_v2`
- Install key/spec type: `furgen_video_tools_v2`
- Server type: `video_gen_v2`
- Expansion: `on_demand`
- Package: managed FurgenPub custom node package `docker/support/custom_nodes/FurgenVideoTools`
- Required verification class types:
  - `FurgenAdaptiveExposureMatch`
  - `FurgenColorTransferMatch`
  - `FurgenTemporalToneSmooth`

## Node Contract Implemented

- `FurgenAdaptiveExposureMatch`
  - Inputs: `images`, `reference`, `strength`, `gain_min`, `gain_max`, `black_percentile`, `white_percentile`, `preserve_highlights`.
  - Uses Rec.709 luma and percentile-clipped robust luma mean.
  - Computes per-frame gain toward broadcast reference luma, clamps gain, applies highlight protection, blends by `strength`, preserves extra channels, clamps `[0, 1]`.
  - `strength=0.0` returns the original tensor object.
- `FurgenColorTransferMatch`
  - Inputs: `images`, `reference`, `mode`, `strength`, `luma_strength`, `chroma_strength`, `std_strength`, `std_min`, `std_max`, `preserve_highlights`.
  - Modes: `rgb_mean_std`, `ycbcr_mean_std`.
  - Broadcasts a single reference image across a frame batch.
  - Uses dependency-free torch RGB and Rec.709 YCbCr mean/std transfer with clamped std ratios, highlight protection, extra-channel preservation, and `[0, 1]` clamp.
  - Returns the original tensor object when `strength=0.0` or all component strengths are zero.
- `FurgenTemporalToneSmooth`
  - Inputs: `images`, `strength`, `luma_smoothing`, `chroma_smoothing`, `max_frame_gain_delta`, `preserve_first_frame`.
  - Operates over the batch dimension as a frame sequence.
  - Smooths luma gain and chroma offset correction factors frame to frame, limits per-frame correction changes, preserves extra channels, clamps `[0, 1]`.
  - `strength=0.0` returns the original tensor object.
  - `preserve_first_frame=true` leaves the first output frame unchanged.
- Existing v1 node classes remain registered and unchanged:
  - `FCSConcatVideos`
  - `FurgenExposureAdjust`
  - `FurgenReferenceColorMatch`

## Provisioning And Agent Behavior

- `docker/support/video_gen_v2.sh`
  - Adds on-demand `install-bundles` support for `video_gen_v2_furgen_color_nodes_v2`.
  - Reuses the existing local-copy/raw-download `provisioning_install_furgen_video_tools_node` path.
  - Does not add v2 nodes to default `video_gen_v2` startup behavior.
- `docker/scripts/dependency_agent_v1.py`
  - Adds install spec type alias `furgen_video_tools_v2`.
  - Adds bundle id `video_gen_v2_furgen_color_nodes_v2`.
  - Installs managed `FurgenVideoTools` through the existing local source/raw fallback.
  - Verifies the three v2 class types through the existing local Comfy restart verification flow.
  - Leaves existing `video_gen_v2_furgen_color_nodes`, `video_gen_v2_10s_ltx_nodes`, and `video_gen_v2_image_filters_nodes` support unchanged.

## Dependency-Agent Release

- Agent version: `dm-agent-py/0.10.36`
- Public raw URL after push: `https://raw.githubusercontent.com/Dodzilla/FurgenPub/refs/heads/main/docker/scripts/dependency_agent_v1.py`
- `docker/scripts/dependency_agent_v1.py` SHA256: `95c73640729a182f77995dc8ee44a51306298814efde34ca288eed6a4b92f222`

## Tests Run

- `bash -n docker/support/video_gen_v2.sh`
  - Output: no output; exit code 0.
- `python3 -m py_compile docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py docker/scripts/dependency_agent_v1.py`
  - Output: no output; exit code 0.
- Local tiny tensor smoke with stub `folder_paths`:
  - Imported `docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py`.
  - Verified `NODE_CLASS_MAPPINGS` contains existing v1 classes and the three new v2 classes.
  - Verified exact tensor identity pass-through at `strength=0.0` for all three v2 nodes.
  - Ran nonzero checks for adaptive exposure, YCbCr color transfer, RGB color transfer, and temporal smoothing.
  - Asserted shape preservation and output clamp range `[0, 1]`.
  - Asserted temporal smoothing preserves the first frame when `preserve_first_frame=true`.
  - Output: `furgen color v2 smoke ok: FCSConcatVideos, FurgenAdaptiveExposureMatch, FurgenColorTransferMatch, FurgenExposureAdjust, FurgenReferenceColorMatch, FurgenTemporalToneSmooth`
- `git diff --check`
  - Output: no output; exit code 0.

## Risk And Caveat

- These nodes are deterministic, dependency-free torch operations and do not use Image-Filters/OpenCV.
- The algorithms are intentionally conservative for dev testing; furgenai should tune the control defaults and workflow ordering against LTX paired extension metrics.
- `torch.quantile` is used for robust percentile exposure matching, so very old PyTorch builds would need validation.
- FurgenPub does not edit Firestore/backend bundle policy; FurgenContentServer owns bundle exposure.

## Readiness

READY_FOR_CONTENTSERVER=true

## Boundaries

- FurgenPub does not edit Firestore/backend policy.
- FurgenPub does not change `furgenai` workflow variants.
- FurgenPub does not deploy live/prod defaults.
- No sibling repos were modified.

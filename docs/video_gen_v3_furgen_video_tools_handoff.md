# video_gen_v3 FurgenVideoTools Handoff

Last updated: 2026-07-06

## Why

furgenai's Video Studio (combine + edits) submits `video_combine_v2` jobs
whose workflows can contain `FurgenExposureAdjust` (per-clip color
adjustment) alongside `VHS_LoadVideoPath`, `ImageBatchExtendWithOverlap`,
and `VHS_VideoCombine`. Per the ContentServer registry, `video_combine_v2`
is supported by both `video_gen_v2` and `video_gen_v3` server types.

`video_gen_v2` already installs FurgenVideoTools (bootstrap +
`video_gen_v2_furgen_color_nodes*` bundles). `video_gen_v3` did not, so a
color-adjusted combine scheduled onto a v3 worker would fail with
`missing_node_type`.

## What changed

- `docker/support/video_gen_v3.sh`
  - Added the `FURGENPUB_RAW_BASE_URL` default used by managed-node downloads.
  - Added `provisioning_install_furgen_video_tools_node()` (same
    local-copy/raw-download pattern as `video_gen_v2.sh`) and call it from
    `provisioning_start` as a soft-fail step, matching the script's style.
- `docker/scripts/dependency_agent_v1.py` (`dm-agent-py/0.10.85`)
  - `_process_install_node_bundles_item` now accepts `video_gen_v3` in both
    places that were gated to `("video_gen_v2", "video_gen_v2_salad")`:
    default verify-class expansion and the legacy bundle install dispatch.
    The `video_gen_v2_*` bundle installers are server-type agnostic, so v3
    workers can now receive on-demand bundle installs (e.g.
    `video_gen_v2_furgen_color_nodes`) instead of erroring with
    `install_node_bundles is not supported on server_type=video_gen_v3`.

No node class contracts changed. `FurgenExposureAdjust` inputs
(`images`, `brightness_multiplier`, `contrast`, `gamma`, `saturation`)
already match what furgenai emits.

## Checks passed

```bash
bash -n docker/support/video_gen_v2.sh
bash -n docker/support/video_gen_v3.sh
python3 -m py_compile docker/scripts/dependency_agent_v1.py \
  docker/support/custom_nodes/FurgenVideoTools/furgen_video_tools.py
```

## Ask for FurgenContentServer

1. Add `video_gen_v2_furgen_color_nodes` (or the v2 bundle id in use) to
   `serverTypeNodeProfiles/video_gen_v3.allowedBundleIds` (and
   `bootstrapDefaultBundleIds` if v3 should install it at bootstrap), so
   class-type resolution for `FurgenExposureAdjust` succeeds on v3 workflow
   validation.
2. Deploy the dependency-agent release `dm-agent-py/0.10.85` once this is
   pushed (pinned raw URL + SHA256, per the usual release flow).
3. Probe: submit one `video_combine_v2` job containing a
   `FurgenExposureAdjust` node to a v3 worker and confirm
   `/object_info/FurgenExposureAdjust` is non-empty after bundle install.

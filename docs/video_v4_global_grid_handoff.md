# V4 global-grid timing

`FCSConcatVideosV4GlobalGrid` is an additive capability for hard-cut timelines,
including per-clip picture fades. It forces `timingMode: "global-grid"` and rejects
blend/crossfade transitions between clips. `FCSConcatVideosV4` without that field retains its
existing rendering and result shape. Callers requiring this contract must use
the new class name: an older worker must reject the job rather than render legacy
per-clip timing silently.

Video frames and 48 kHz audio samples are allocated from cumulative clip durations,
using exclusive end boundaries rounded upward on their respective output grids.
Source video sampling retains its phase on the cumulative output clock. Individual
clips are constrained to their allocated frame/sample counts before concatenation;
there is no accumulation of independently rounded video durations. The final video
may extend beyond nominal duration by less than one output frame. Source timestamps
and continuation metadata remain separate from this composition ledger.

The normal video and audio MP4 paths are unchanged. A global-grid render additionally
writes `<prefix>_00001-timing.json`, appends its path to the result file list, and
returns the same object as `ui.compositionTiming[0]`:

- `version: 1`, `mode: "global-grid"`, `frameRate`, `audioSampleRate: 48000`
- `nominalDurationSeconds`, `totalFrames`, `totalAudioSamples`
- `boundaries`: `clipIndex`, `startSeconds`, `endSeconds`, `startFrame`, `endFrame`,
  `startSample`, `endSample`; end values are exclusive.

## Validation

Run `python3 -m pytest docker/support/tests -q`. The real FFmpeg regression covers
24/30/60 fps, variable frame timestamps, multiple continuation trims, the actual
30.15 + 14.6 + (12 - 1/24) + (12 - 1/24) duration pattern, native 32 kHz audio
sentinels exported to AAC, and a 0.6-second down/0.4-second up picture-only dip.
Decoded frame colors identify source-frame order. Audio checks locate known markers
within 0.5 ms; these are timing tests, not perceptual listening.

## Scoped development rollout

Do not change the dependency-agent global FurgenPub pin or reload an active worker.
Its `install_node_bundles` handler can restart Comfy; that handler alone does not
establish an idle scheduling fence. Use an exact utility worker only after its job
queue is empty and existing maintenance/scheduler fencing prevents new assignments.
Install the managed FurgenVideoTools bundle from this change's immutable commit,
with `FCSConcatVideosV4GlobalGrid` among the explicit verification classes. Existing
class verification must remain intact. Check the worker's `/object_info` before
submitting a tiny development render and verify its timing artifact and decoded
frame/sample allocation. Keep production bundle policy unchanged.

FurgenContentServer owns the scoped bundle installation command and readiness fence;
KT owns workflow class selection and exposing the timing artifact. FoxyVoxy must
request the new timing mode, retain that choice in saved export/cache identity, and
store `compositionTiming` separately from source continuation boundary metadata.
Rollback routes new requests to the previous explicit legacy mode and restores the
prior bundle on an idle worker; it must not reinterpret an exact-timeline request.

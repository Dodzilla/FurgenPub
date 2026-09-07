#!/usr/bin/env bash
# CPU-only fast-all installation. Checkpoints stay under dependency management.
set -euo pipefail
root="${WORKSPACE:-/workspace}"
instance="${DM_INSTANCE_ID:-${CONTAINER_ID:-${VAST_CONTAINERLABEL#C.}}}"
[[ "$instance" =~ ^[0-9]+$ ]] || { echo 'Missing v7 worker identity' >&2; exit 1; }
export DM_INSTANCE_ID="$instance"
export SERVER_TYPE=asset_gen_v7_lite
base="${FURGENPUB_RAW_BASE_URL:?Pinned support base required}"
for file in asset_gen_v7_lite_tts_install.py breeze_tts2_checkpoint_manifest.json breeze_tts2_production_policy.json; do
    curl --fail --silent --show-error --location --retry 3 "$base/$file" -o "$root/$file"
done
version="$(/venv/main/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$root/breeze_tts2_production_policy.json")"
/venv/main/bin/python "$root/asset_gen_v7_lite_tts_install.py" \
    --instance "$instance" --version "$version" \
    --checkpoint "${DM_COMFYUI_DIR:-$root/ComfyUI}/models/breezetts2/BreezeBlue_Breeze-TTS-2" \
    --checkpoint-manifest "$root/breeze_tts2_checkpoint_manifest.json" \
    --production-policy "$root/breeze_tts2_production_policy.json" --allow-missing-checkpoint

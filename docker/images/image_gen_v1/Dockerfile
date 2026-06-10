# Baked image for image_gen_v1: ComfyUI pinned commit, all custom nodes, and python
# deps preinstalled so per-rental provisioning reduces to model downloads.
# Build via docker/images/image_gen_v1/cloudbuild.yaml (discovers COMFY_DIR in the base).
ARG BASE_IMAGE=vastai/comfy:v0.18.2-cuda-12.9-py312
FROM ${BASE_IMAGE}

# Location of the ComfyUI checkout inside the base image (vastai images sync this
# into /workspace at boot). Overridden by cloudbuild after discovery.
ARG COMFY_DIR=/opt/workspace-internal/ComfyUI
ENV COMFYUI_PIN_COMMIT=a0ae3f3bd46b9e58f43fccfe17077873bf16f905
# Signals the provisioning script to skip ComfyUI/node/pip work at boot.
ENV FURGEN_BAKED=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Pin ComfyUI and install its requirements.
RUN source /venv/main/bin/activate && \
    cd "${COMFY_DIR}" && \
    git config --global --add safe.directory "$(pwd)" && \
    git fetch --all --tags && \
    git checkout --force "${COMFYUI_PIN_COMMIT}" && \
    pip install --no-cache-dir -r requirements.txt && \
    grep -Fq "FluxKVCache" comfy_extras/nodes_flux.py

# Clone custom nodes at their pinned commits and install their requirements.
RUN source /venv/main/bin/activate && \
    cd "${COMFY_DIR}/custom_nodes" && \
    set -e && \
    clone_pin() { \
        local repo="$1" pin="$2" dir; \
        dir="${repo##*/}"; dir="${dir%.git}"; \
        if [[ ! -d "$dir" ]]; then git clone --recursive "$repo" "$dir"; fi; \
        ( cd "$dir" && git fetch --all --tags && git checkout --force "$pin" ); \
        if [[ -f "$dir/requirements.txt" ]]; then \
            pip install --no-cache-dir -r "$dir/requirements.txt" || \
            echo "WARN: requirements install failed for $dir"; \
        fi; \
    } && \
    clone_pin https://github.com/ltdrdata/ComfyUI-Impact-Pack 6a517ebe06fea2b74fc41b3bd089c0d7173eeced && \
    clone_pin https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes d78b780ae43fcf8c6b7c6505e6ffb4584281ceca && \
    clone_pin https://github.com/cubiq/ComfyUI_essentials 9d9f4bedfc9f0321c19faf71855e228c93bd0dc9 && \
    clone_pin https://github.com/WASasquatch/was-node-suite-comfyui ea935d1044ae5a26efa54ebeb18fe9020af49a45 && \
    clone_pin https://github.com/Fannovel16/comfyui_controlnet_aux 136f125c89aed92ced1b6fbb491e13719b72fcc0 && \
    clone_pin https://github.com/PozzettiAndrea/ComfyUI-SAM3 978bb763cfadcad41363eba016e57686b414c27b && \
    clone_pin https://github.com/ltdrdata/ComfyUI-Manager d8e3f531c7348dedd39602cc85a438caf09589e5 && \
    clone_pin https://github.com/Dodzilla/easy-comfy-nodes-async d4c651a65e885a05ce5ce09468a2597ab1f7925c && \
    clone_pin https://github.com/Dodzilla/ComfyUI-ComfyCouple 6c815b13e6269b7ade1dd3a49ef67de71a0014eb && \
    clone_pin https://github.com/scottmudge/ComfyUI-NAG c6f27116a8259f5b501d498a09e51c82fa72e35f

# flash-attn: wheel-only. When no wheel matches this torch/CUDA combo we skip it
# (ComfyUI falls back to PyTorch attention) instead of compiling for an hour.
RUN source /venv/main/bin/activate && \
    (pip install --no-cache-dir --only-binary=:all: flash-attn && echo "flash-attn wheel installed") || \
    echo "WARN: no flash-attn wheel for this torch/CUDA; continuing without it"

# aria2 for multi-connection model downloads by the dependency agent.
RUN apt-get update -qq && apt-get install -y -qq aria2 && rm -rf /var/lib/apt/lists/*

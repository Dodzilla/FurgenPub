# Runtime packages and native compilation belong in the image build, never scale-out.
# No workload models, credentials, or dependency-agent instance state enter this image.
FROM vastai/comfy@sha256:f50deca9dc9f2f4bdee3ce4ce94642a17ee612e71343f9a9a285913104b60352 AS runtime-deps
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG SUPPORT_COMMIT=5efd569a73998563767768f568e451cd85b7c05b
ENV FURGEN_CUDA_VERSION=13.0
RUN mkdir -p /opt/furgen/v7/support && \
    for name in asset_gen_v7_lite_cuda130.sh asset_gen_v7_lite_comfy_kitchen.sh asset_gen_v5_lite.sh; do \
      curl -fsSL --retry 3 "https://raw.githubusercontent.com/Dodzilla/FurgenPub/${SUPPORT_COMMIT}/docker/support/${name}" -o "/opt/furgen/v7/support/${name}"; \
    done && \
    FURGEN_COMFY_PYTHON=/venv/main/bin/python bash /opt/furgen/v7/support/asset_gen_v7_lite_cuda130.sh && \
    DM_COMFYUI_DIR=/opt/workspace-internal/ComfyUI bash /opt/furgen/v7/support/asset_gen_v7_lite_comfy_kitchen.sh bootstrap-framework && \
    DM_COMFYUI_DIR=/opt/workspace-internal/ComfyUI bash /opt/furgen/v7/support/asset_gen_v7_lite_comfy_kitchen.sh install-core
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates cmake curl git ninja-build pkg-config \
    cuda-nvcc-13-0 libcublas-dev-13-0 vmtouch aria2 ffmpeg sox libgl1 libglib2.0-0 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN DM_AGENT_DISABLE=true SERVER_TYPE=asset_gen_v7_lite \
    WORKSPACE=/opt/workspace-internal DM_COMFYUI_DIR=/opt/workspace-internal/ComfyUI \
    HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
    FURGENPUB_RAW_BASE_URL="https://raw.githubusercontent.com/Dodzilla/FurgenPub/${SUPPORT_COMMIT}/docker/support" \
    bash /opt/furgen/v7/support/asset_gen_v5_lite.sh install-bundles \
      asset_gen_v5_runtime_helpers asset_gen_v5_flux_image
FROM runtime-deps AS native
RUN ln -s libcuda.so /usr/local/cuda-13.0/lib64/stubs/libcuda.so.1 && \
    git init /opt/furgen/v7/llama.cpp && \
    git -C /opt/furgen/v7/llama.cpp fetch --depth 1 https://github.com/ggml-org/llama.cpp.git a94d563ed801d1da1b8c2432946de07d0231bb3d && \
    git -C /opt/furgen/v7/llama.cpp checkout --detach FETCH_HEAD && \
    cmake -S /opt/furgen/v7/llama.cpp -B /opt/furgen/v7/llama-build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_CUDA_COMPILER=/usr/local/cuda-13.0/bin/nvcc \
      -DCMAKE_CUDA_ARCHITECTURES=120 -DGGML_CUDA=ON -DLLAMA_BUILD_SERVER=ON \
      -DCMAKE_EXE_LINKER_FLAGS="-Wl,-rpath-link,/usr/local/cuda-13.0/lib64/stubs" && \
    cmake --build /opt/furgen/v7/llama-build --target llama-server -j 2 && \
    rm /usr/local/cuda-13.0/lib64/stubs/libcuda.so.1 && \
    touch /opt/furgen/v7/llama-build/furgen-commit-a94d563ed801d1da1b8c2432946de07d0231bb3d-cuda-13.0 && \
    rm -rf /opt/furgen/v7/llama.cpp /opt/furgen/v7/llama-build/ggml /opt/furgen/v7/llama-build/src
RUN /venv/main/bin/python -c 'import importlib.metadata as m; expected={"torch":"2.10.0+cu130","torchvision":"0.25.0+cu130","torchaudio":"2.10.0+cu130","transformers":"5.3.0","comfy-kitchen":"0.2.31","comfy-aimdo":"0.4.13"}; assert all(m.version(k)==v for k,v in expected.items()), expected' && \
    /venv/main/bin/python -m pip freeze --all > /opt/furgen/v7/environment.lock.txt && \
    printf '%s\n' "${SUPPORT_COMMIT}" > /opt/furgen/v7/support-commit && \
    env-hash > /.env_hash

FROM native AS production
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG SUPPORT_COMMIT=aca51ba7e45c2d20d84ce40485d707a76451a803
RUN /venv/main/bin/python -m pip uninstall -y omnivoice && \
    rm -rf /opt/workspace-internal/ComfyUI/custom_nodes/ComfyUI-OmniVoice-TTS && \
    git init /opt/workspace-internal/ComfyUI/custom_nodes/ComfyUI-Breeze-TTS-2 && \
    git -C /opt/workspace-internal/ComfyUI/custom_nodes/ComfyUI-Breeze-TTS-2 fetch --depth 1 \
      https://github.com/Dodzilla/ComfyUI-Breeze-TTS-2 4bf4c0962bcef7227abd147cb14e10bbd59b3211 && \
    git -C /opt/workspace-internal/ComfyUI/custom_nodes/ComfyUI-Breeze-TTS-2 checkout --detach FETCH_HEAD && \
    /venv/main/bin/python -m pip install --no-deps --no-cache-dir \
      -r /opt/workspace-internal/ComfyUI/custom_nodes/ComfyUI-Breeze-TTS-2/requirements.txt && \
    curl -fsSL --retry 3 "https://raw.githubusercontent.com/Dodzilla/FurgenPub/${SUPPORT_COMMIT}/docker/support/asset_gen_v7_lite_prebuilt.py" \
      -o /opt/furgen/v7/asset_gen_v7_lite_prebuilt.py && \
    /venv/main/bin/python /opt/furgen/v7/asset_gen_v7_lite_prebuilt.py snapshot --comfy /opt/workspace-internal/ComfyUI && \
    /venv/main/bin/python /opt/furgen/v7/asset_gen_v7_lite_prebuilt.py verify --comfy /opt/workspace-internal/ComfyUI && \
    printf '%s\n' "${SUPPORT_COMMIT}" > /opt/furgen/v7/support-commit
ENV FURGEN_REQUIRE_PREBUILT_V7=true
RUN env-hash > /.env_hash

FROM scratch
COPY --from=production / /
SHELL ["/bin/bash","-o","pipefail","-c"]
ENV PATH="/opt/instance-tools/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV IMAGE_TYPE="vast"
ENV DATA_DIRECTORY="/workspace"
ENV WORKSPACE="/workspace"
ENV PIP_BREAK_SYSTEM_PACKAGES="1"
ENV DEBIAN_FRONTEND="noninteractive"
ENV PYTHONUNBUFFERED="1"
ENV TCLLIBPATH="/usr/lib/tcltk/default"
ENV NVIDIA_DRIVER_CAPABILITIES="all"
ENV UV_CACHE_DIR="/.uv/cache"
ENV UV_NO_CACHE="1"
ENV UV_LINK_MODE="copy"
ENV UV_PYTHON_BIN_DIR="/.uv/python_bin"
ENV UV_PYTHON_INSTALL_DIR="/.uv/python_install"
ENV PYTHON_VERSION="3.12"
ENV PYTORCH_VERSION="2.10.0"
ENV PYTORCH_BACKEND="cu128"
ENV COMFYUI_ARGS="--disable-auto-launch --enable-cors-header --port 18188"
ENV FURGEN_CUDA_VERSION="13.0"
ENV FURGEN_REQUIRE_PREBUILT_V7="true"
RUN /venv/main/bin/python /opt/furgen/v7/asset_gen_v7_lite_prebuilt.py verify --comfy /opt/workspace-internal/ComfyUI
ENTRYPOINT ["/opt/instance-tools/bin/entrypoint.sh"]

LABEL org.opencontainers.image.source="https://github.com/Dodzilla/FurgenPub"
RUN sha256sum /opt/furgen/v7/manifest.json

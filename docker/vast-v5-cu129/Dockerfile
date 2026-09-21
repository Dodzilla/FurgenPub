# Software only: model weights remain on their direct origins or verified caches.
FROM vastai/comfy:v0.30.0-cuda-12.9-py312
USER root

ARG COMFY_PIN=40c4fcdf513a4523e39d54a9d391908af8df8171
ARG FURGENPUB_COMMIT
ENV FURGENPUB_COMMIT=${FURGENPUB_COMMIT}

COPY docker/vast-v5-cu129 /opt/furgen/v5
COPY docker/support/custom_nodes/FurgenH3VideoTools /opt/furgen/v5/FurgenH3VideoTools
COPY docker/support/video_gen_v3.sh /opt/furgen/v5/video_gen_v3.sh
COPY docker/scripts/dependency_agent_v1.py /opt/furgen/v5/dependency_agent_v1.py

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends aria2 ffmpeg; \
    git clone --filter=blob:none https://github.com/comfyanonymous/ComfyUI.git /opt/furgen/v5/ComfyUI; \
    git -C /opt/furgen/v5/ComfyUI checkout "$COMFY_PIN"; \
    /venv/main/bin/python -m pip install --no-cache-dir -r /opt/furgen/v5/ComfyUI/requirements.txt; \
    /venv/main/bin/python /opt/furgen/v5/install_nodes.py; \
    /venv/main/bin/python -m pip install --no-cache-dir --upgrade --force-reinstall --index-url https://download.pytorch.org/whl/cu129 \
      torch==2.10.0+cu129 torchvision==0.25.0+cu129 torchaudio==2.10.0+cu129 torchcodec==0.10.0+cu129; \
    /venv/main/bin/python -m pip install --no-cache-dir --no-deps \
      'https://github.com/Comfy-Org/wheels/releases/download/sageattention-latest/sageattention-2.2.0%2Bcu129torch2.10-cp312-cp312-manylinux_2_34_x86_64.manylinux_2_35_x86_64.whl'; \
    /venv/main/bin/python -m pip install --no-cache-dir --extra-index-url https://pypi.nvidia.com nvidia-vfx==0.1.0.1; \
    /venv/main/bin/python -m pip check; \
    /venv/main/bin/python -c 'import color_matcher, cv2, imageio_ffmpeg, requests; from importlib.metadata import version; assert version("nvidia-vfx") == "0.1.0.1"'; \
    /venv/main/bin/python -c 'import torch; assert torch.__version__ == "2.10.0+cu129"; assert torch.version.cuda == "12.9"'; \
    /venv/main/bin/python /opt/furgen/v5/seal_manifest.py; \
    find /opt/furgen/v5/ComfyUI -type d -name .git -prune -exec rm -rf '{}' +; \
    find /opt/furgen/v5 -type d -name __pycache__ -prune -exec rm -rf '{}' +; \
    rm -rf /root/.cache/pip /root/.cache/huggingface /var/lib/apt/lists/* /opt/furgen/v5/FurgenH3VideoTools /opt/furgen/v5/video_gen_v3.sh

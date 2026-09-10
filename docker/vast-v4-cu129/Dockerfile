# Software only: model weights stay on direct origins or verified worker caches.
FROM vastai/comfy:v0.30.0-cuda-12.9-py312
USER root
ARG COMFY_PIN=e01fb4c56b7a88149d469b99cbbfe3223d715054
RUN apt-get update && apt-get install -y --no-install-recommends aria2 ffmpeg && rm -rf /var/lib/apt/lists/*
RUN git clone --filter=blob:none https://github.com/comfyanonymous/ComfyUI.git /opt/furgen/v4/ComfyUI && \
    git -C /opt/furgen/v4/ComfyUI checkout "$COMFY_PIN" && \
    /venv/main/bin/python -m pip install --no-cache-dir -r /opt/furgen/v4/ComfyUI/requirements.txt && \
    /venv/main/bin/python -m pip install --no-cache-dir --upgrade --force-reinstall --index-url https://download.pytorch.org/whl/cu129 \
      torch==2.10.0+cu129 torchvision==0.25.0+cu129 torchaudio==2.10.0+cu129 torchcodec==0.10.0+cu129 && \
    /venv/main/bin/python -m pip install --no-cache-dir --extra-index-url https://pypi.nvidia.com nvidia-vfx==0.1.0.1 && \
    /venv/main/bin/python -m pip check
RUN /venv/main/bin/python -c 'import torch,json,pathlib; assert torch.__version__=="2.10.0+cu129"; assert torch.version.cuda=="12.9"; pathlib.Path("/opt/furgen/v4/manifest.json").write_text(json.dumps({"schema":1,"comfyCommit":"e01fb4c56b7a88149d469b99cbbfe3223d715054","torchVersion":torch.__version__,"cuda":"12.9","modelsIncluded":False}))'
COPY bundles.json install_nodes.py seal_manifest.py /opt/furgen/v4/
RUN /venv/main/bin/python /opt/furgen/v4/install_nodes.py && \
    /venv/main/bin/python -m pip install --no-cache-dir --upgrade --index-url https://download.pytorch.org/whl/cu129 \
      torch==2.10.0+cu129 torchvision==0.25.0+cu129 torchaudio==2.10.0+cu129 torchcodec==0.10.0+cu129 && \
    /venv/main/bin/python -m pip check && \
    /venv/main/bin/python /opt/furgen/v4/seal_manifest.py

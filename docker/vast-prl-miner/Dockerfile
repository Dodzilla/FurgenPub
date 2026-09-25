FROM nvidia/cuda:12.9.1-base-ubuntu24.04

# The PRL worker downloads its Python agent and SRBMiner at boot. Keep only
# the libraries and tools required for Vast SSH startup and that bootstrap.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 ca-certificates curl git \
      openssh-client openssh-server rsync && \
    mkdir -p /run/sshd /workspace && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

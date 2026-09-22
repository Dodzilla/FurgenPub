"""Import every node module against the baked core, without a GPU.

Runs at image build time so an API break surfaces here rather than on a rented
worker. comfy.options must enable parsing before comfy.cli_args is imported:
otherwise cli_args parses an empty argv, --cpu is ignored, and model_management
initialises CUDA on the GPU-less builder.
"""
import sys

sys.argv = ["main.py", "--cpu"]

import comfy.options  # noqa: E402

comfy.options.enable_args_parsing()

from comfy.cli_args import args  # noqa: E402

assert args.cpu, "ComfyUI did not honour --cpu; node verification would need a GPU"

import asyncio  # noqa: E402

import nodes  # noqa: E402

asyncio.run(nodes.init_extra_nodes(init_custom_nodes=True))

REQUIRED = (
    # Qwen-Image 2.1
    "UnetLoaderGGUF", "TextEncodeQwenImage21", "QwenImage21Cache",
    # Shared core
    "CLIPLoader", "VAELoader", "VAEDecode", "KSampler", "EmptyLatentImage",
    # Existing v7 workloads must keep importing against the new core
    "EmptyFlux2LatentImage", "Flux2Scheduler", "FluxKVCache", "CFGGuider",
    "SamplerCustomAdvanced", "RandomNoise", "KSamplerSelect",
    "Image Save", "CR Upscale Image", "EZLoadImgFromUrlNode",
    "BreezeTTS2VoiceClone", "BreezeTTS2VoiceDesign",
)

missing = [name for name in REQUIRED if name not in nodes.NODE_CLASS_MAPPINGS]
assert not missing, f"missing node classes: {missing}"

clip_types = nodes.NODE_CLASS_MAPPINGS["CLIPLoader"].INPUT_TYPES()["required"]["type"][0]
for required_type in ("qwen_image", "flux2"):
    assert required_type in clip_types, f"CLIPLoader lost the {required_type} encoder type"

print(f"node classes present: {len(nodes.NODE_CLASS_MAPPINGS)}")

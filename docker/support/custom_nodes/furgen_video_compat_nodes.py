import io

import numpy as np
import requests
import torch
from PIL import Image, ImageOps


class AnyType(str):
    def __ne__(self, other):
        return False


ANY_TYPE = AnyType("*")


class ImpactExecutionOrderController:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "signal": (ANY_TYPE,),
                "value": (ANY_TYPE,),
            }
        }

    RETURN_TYPES = (ANY_TYPE, ANY_TYPE)
    RETURN_NAMES = ("signal", "value")
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, signal, value):
        return signal, value


class EZLoadImgFromUrlNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"url": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("IMAGE", "MASK")
    RETURN_NAMES = ("image", "mask")
    FUNCTION = "load"
    CATEGORY = "Furgen/compat"

    def load(self, url):
        response = requests.get(url, timeout=45)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content))
        image = ImageOps.exif_transpose(image).convert("RGBA")

        rgba = np.array(image).astype(np.float32) / 255.0
        rgb = torch.from_numpy(rgba[:, :, :3])[None,]
        mask = torch.from_numpy(1.0 - rgba[:, :, 3])[None,]
        return rgb, mask


class CM_FloatToInt:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"a": ("FLOAT", {"default": 0.0})}}

    RETURN_TYPES = ("INT",)
    RETURN_NAMES = ("INT",)
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, a=0.0):
        return (int(round(float(a))),)


class LatentMotionSharpener:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "latent": ("LATENT",),
                "base_sharpen": ("FLOAT", {"default": 0.04, "min": 0.0, "max": 5.0, "step": 0.01}),
                "motion_sharpen": ("FLOAT", {"default": 0.2, "min": 0.0, "max": 5.0, "step": 0.01}),
                "motion_thresh": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 1.0, "step": 0.01}),
                "temporal_smooth_mask": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("LATENT",)
    RETURN_NAMES = ("latent",)
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, latent, base_sharpen=0.04, motion_sharpen=0.2, motion_thresh=0.0, temporal_smooth_mask=False):
        return (latent,)


class LatentTemporalInpainter:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "latent": ("LATENT",),
                "anchor_sigma": ("FLOAT", {"default": 0.1, "min": 0.0, "max": 1.0, "step": 0.01}),
                "ghost_sigma": ("FLOAT", {"default": 0.35, "min": 0.0, "max": 1.0, "step": 0.01}),
                "score_gamma": ("FLOAT", {"default": 2.0, "min": 0.0, "max": 10.0, "step": 0.1}),
                "anchor_blend": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0, "step": 0.01}),
                "seed": ("INT", {"default": 0, "min": 0, "max": 0xffffffffffffffff}),
                "debug_scores": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("LATENT",)
    RETURN_NAMES = ("latent",)
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, latent, anchor_sigma=0.1, ghost_sigma=0.35, score_gamma=2.0, anchor_blend=0.4, seed=0, debug_scores=False):
        return (latent,)


class LTXVImgToVideoConditionOnly:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "vae": (ANY_TYPE,),
                "image": ("IMAGE",),
                "latent": ("LATENT",),
                "strength": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 1.0, "step": 0.01}),
                "bypass": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("LATENT",)
    RETURN_NAMES = ("latent",)
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, vae, image, latent, strength=1.0, bypass=False):
        return (latent,)


class LTXAddVideoICLoRAGuide:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "positive": ("CONDITIONING",),
                "negative": ("CONDITIONING",),
                "vae": (ANY_TYPE,),
                "latent": ("LATENT",),
                "image": ("IMAGE",),
                "frame_idx": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "strength": ("FLOAT", {"default": 0.65, "min": 0.0, "max": 2.0, "step": 0.01}),
                "latent_downscale_factor": (ANY_TYPE,),
                "crop": (["center", "disabled"],),
                "use_tiled_encode": ("BOOLEAN", {"default": False}),
                "tile_size": ("INT", {"default": 256, "min": 64, "max": 2048}),
                "tile_overlap": ("INT", {"default": 64, "min": 0, "max": 1024}),
            }
        }

    RETURN_TYPES = ("CONDITIONING", "CONDITIONING", "LATENT")
    RETURN_NAMES = ("positive", "negative", "latent")
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(
        self,
        positive,
        negative,
        vae,
        latent,
        image,
        frame_idx=0,
        strength=0.65,
        latent_downscale_factor=None,
        crop="center",
        use_tiled_encode=False,
        tile_size=256,
        tile_overlap=64,
    ):
        return positive, negative, latent


class RIFEInterpolation:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "model_name": ("STRING", {"default": "flownet.pkl"}),
                "source_fps": ("FLOAT", {"default": 24.0}),
                "target_fps": ("INT", {"default": 60, "min": 1, "max": 240}),
                "scale": ("FLOAT", {"default": 1.0}),
                "batch_size": ("INT", {"default": 8, "min": 1, "max": 64}),
                "use_fp16": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "execute"
    CATEGORY = "Furgen/compat"

    def execute(self, images, model_name="flownet.pkl", source_fps=24.0, target_fps=60, scale=1.0, batch_size=8, use_fp16=True):
        return (images,)


NODE_CLASS_MAPPINGS = {
    "CM_FloatToInt": CM_FloatToInt,
    "ImpactExecutionOrderController": ImpactExecutionOrderController,
    "EZLoadImgFromUrlNode": EZLoadImgFromUrlNode,
    "LatentMotionSharpener": LatentMotionSharpener,
    "LatentTemporalInpainter": LatentTemporalInpainter,
    "LTXVImgToVideoConditionOnly": LTXVImgToVideoConditionOnly,
    "LTXAddVideoICLoRAGuide": LTXAddVideoICLoRAGuide,
    "RIFEInterpolation": RIFEInterpolation,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "CM_FloatToInt": "Float To Int",
    "ImpactExecutionOrderController": "Execution Order Controller",
    "EZLoadImgFromUrlNode": "Load Img From URL (EZ)",
    "LatentMotionSharpener": "Latent Motion Sharpener",
    "LatentTemporalInpainter": "Latent Temporal Inpainter",
    "LTXVImgToVideoConditionOnly": "LTXV Img To Video Condition Only",
    "LTXAddVideoICLoRAGuide": "LTX Add Video ICLoRA Guide",
    "RIFEInterpolation": "RIFE Frame Interpolation",
}

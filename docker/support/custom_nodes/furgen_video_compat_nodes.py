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


NODE_CLASS_MAPPINGS = {
    "CM_FloatToInt": CM_FloatToInt,
    "ImpactExecutionOrderController": ImpactExecutionOrderController,
    "EZLoadImgFromUrlNode": EZLoadImgFromUrlNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "CM_FloatToInt": "Float To Int",
    "ImpactExecutionOrderController": "Execution Order Controller",
    "EZLoadImgFromUrlNode": "Load Img From URL (EZ)",
}

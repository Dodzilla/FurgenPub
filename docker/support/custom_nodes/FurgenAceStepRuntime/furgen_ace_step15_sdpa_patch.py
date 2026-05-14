import logging


class FurgenAceStep15SDPAPatch:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"model": ("MODEL",)}}

    RETURN_TYPES = ("MODEL",)
    FUNCTION = "patch"
    CATEGORY = "Furgen/runtime"
    DESCRIPTION = "Patch ACE-Step 1.5 model attention to ComfyUI PyTorch SDPA on RTX 5090 hosts."

    def patch(self, model):
        from comfy.ldm.modules import attention
        import comfy.ldm.ace.ace_step15 as ace_step15

        model_clone = model.clone()
        transformer_options = model_clone.model_options.setdefault("transformer_options", {})
        pytorch_attention = attention.attention_pytorch
        target = getattr(pytorch_attention, "__wrapped__", pytorch_attention)

        def attention_override_sdpa(func, *args, **kwargs):
            return target(*args, **kwargs)

        transformer_options["optimized_attention_override"] = attention_override_sdpa
        attention.optimized_attention = pytorch_attention
        attention.optimized_attention_masked = pytorch_attention
        ace_step15.optimized_attention = pytorch_attention
        logging.info("FurgenAceStep15SDPAPatch: patched ACE-Step attention to PyTorch SDPA")
        return (model_clone,)


NODE_CLASS_MAPPINGS = {"FurgenAceStep15SDPAPatch": FurgenAceStep15SDPAPatch}
NODE_DISPLAY_NAME_MAPPINGS = {"FurgenAceStep15SDPAPatch": "Furgen ACE-Step 1.5 SDPA Patch"}

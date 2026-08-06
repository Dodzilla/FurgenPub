"""FurgenVideoToolsV3 — side-by-side install of FCSConcatVideosV3.

Installed into its own custom_nodes directory so it never collides with the
pinned FurgenVideoTools package. Only FCSConcatVideosV3 is registered here;
every other class in the module is left unregistered so the pinned package
stays the single owner of its class types.
"""
from .furgen_video_tools import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]

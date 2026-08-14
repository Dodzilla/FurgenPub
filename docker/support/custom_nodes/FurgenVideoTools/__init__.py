import sys
from pathlib import Path


# ComfyUI 0.30 loads custom nodes with a filesystem-derived module name. That
# name is not a valid Python package, so sibling-relative imports fail even
# though both modules are present. Import the managed siblings from their
# explicit directory instead.
_NODE_DIR = str(Path(__file__).resolve().parent)
if _NODE_DIR not in sys.path:
    sys.path.insert(0, _NODE_DIR)

from furgen_sageattention_policy import SAGEATTENTION_POLICY_STATUS
from furgen_video_tools import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "SAGEATTENTION_POLICY_STATUS",
]

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "furgen_timing_artifacts", Path(__file__).parent / "docker/support/custom_nodes/FurgenTimingArtifacts/__init__.py")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
NODE_CLASS_MAPPINGS = _module.NODE_CLASS_MAPPINGS

__all__ = ["NODE_CLASS_MAPPINGS"]

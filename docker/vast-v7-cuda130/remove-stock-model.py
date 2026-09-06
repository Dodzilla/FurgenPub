"""Drop the stock SD1.5 example; V7 obtains workload models through its agent."""
from pathlib import Path

model = Path('/opt/model_store/v1-5-pruned-emaonly-fp16.safetensors')
link = Path('/opt/workspace-internal/ComfyUI/models/checkpoints') / model.name
if link.is_symlink():
    assert link.resolve() == model, 'Unexpected stock model link target'
    link.unlink()
elif link.exists():
    raise RuntimeError('Unexpected regular file at stock model link')
assert model.is_file(), 'Stock model changed; review the base image'
print('Removed stock SD1.5 model bytes:', model.stat().st_size)
model.unlink()

"""Seal installed node code and package versions after the final runtime pins."""
import hashlib
import importlib.metadata
import json
from pathlib import Path

root = Path('/opt/furgen/v4')
manifest = json.loads((root / 'manifest.json').read_text())
bundles = json.loads((root / 'bundles.json').read_text())
sealed = {}
for bundle_id, spec in bundles.items():
    dirs = set()
    for step in spec['steps'] + spec.get('compatibility', {}).get('steps', []):
        if step['type'] == 'git_custom_node':
            dirs.add(step['directoryName'])
        elif step['type'] == 'furgen_support_custom_node':
            dirs.add(step['packageName'])
    files = {}
    for directory in dirs:
        base = root / 'ComfyUI' / 'custom_nodes' / directory
        for path in sorted(base.rglob('*')):
            if not path.is_file() or path.suffix not in {'.py', '.json', '.txt', '.toml', '.yaml', '.yml'} or '.git' in path.parts or '__pycache__' in path.parts:
                continue
            files[str(path.relative_to(root / 'ComfyUI'))] = hashlib.sha256(path.read_bytes()).hexdigest()
    assert files
    sealed[bundle_id] = {'spec': spec, 'files': files}
manifest.update(nodeBundles=sealed, packageVersions={d.metadata['Name'].lower().replace('_', '-'): d.version for d in importlib.metadata.distributions()})
(root / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True) + '\n')

#!/usr/bin/env python3
"""Verify baked runtime contents before skipping any installation work."""
import argparse
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path
import subprocess

BUNDLES = ['asset_gen_v5_runtime_helpers', 'asset_gen_v5_flux_image', 'asset_gen_v7_lite_breeze_tts2']
CORE = 'e01fb4c56b7a88149d469b99cbbfe3223d715054'
PACKAGES = {'torch': '2.10.0+cu130', 'torchvision': '0.25.0+cu130', 'torchaudio': '2.10.0+cu130',
            'transformers': '5.3.0', 'comfy-kitchen': '0.2.31', 'comfy-aimdo': '0.4.13'}


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def versions():
    return {d.metadata['Name'].lower().replace('_', '-'): d.version for d in metadata.distributions()}


def head(path):
    return subprocess.check_output(['git', '-c', f'safe.directory={path}', '-C', str(path), 'rev-parse', 'HEAD'], text=True).strip()


def snapshot(root, runtime):
    assert head(root) == CORE, 'Comfy core mismatch'
    installed = versions()
    for name, version in PACKAGES.items():
        assert installed.get(name) == version, f'Framework mismatch: {name}'
    nodes = {p.name: head(p) for p in (root / 'custom_nodes').iterdir() if (p / '.git').exists()}
    required_nodes = {'easy-comfy-nodes-async', 'ComfyUI_essentials', 'ComfyUI-NAG', 'ComfyUI_Comfyroll_CustomNodes', 'was-node-suite-comfyui', 'ComfyUI-Breeze-TTS-2'}
    assert required_nodes <= nodes.keys(), 'Missing baked custom nodes'
    binaries = {str(p.relative_to(runtime)): sha(p) for p in (runtime / 'llama-build/bin').iterdir() if p.is_file()}
    assert 'llama-build/bin/llama-server' in binaries, 'Missing llama-server'
    return {'schemaVersion': 1, 'cuda': '13.0', 'core': CORE, 'packages': installed,
            'bundles': BUNDLES, 'nodes': nodes, 'binaries': binaries}


def verify(manifest, root, runtime, bundles):
    assert manifest['schemaVersion'] == 1 and manifest['cuda'] == '13.0', 'Unsupported prebuilt runtime'
    assert set(bundles) <= set(manifest['bundles']), 'Requested bundle is not baked'
    installed = versions()
    for name, version in manifest['packages'].items():
        assert installed.get(name) == version, f'Package drift: {name}'
    assert head(root) == manifest['core'] == CORE, 'Comfy core drift'
    for name, revision in manifest['nodes'].items():
        assert head(root / 'custom_nodes' / name) == revision, f'Node drift: {name}'
    for name, digest in manifest['binaries'].items():
        path = runtime / name
        assert path.resolve().is_relative_to(runtime.resolve()), 'Invalid binary path'
        assert sha(path) == digest, f'Native binary drift: {name}'


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['snapshot', 'verify'])
    p.add_argument('--comfy', required=True)
    p.add_argument('--runtime', default='/opt/furgen/v7')
    p.add_argument('bundles', nargs='*')
    args = p.parse_intermixed_args()
    root, runtime = Path(args.comfy), Path(args.runtime)
    manifest_path = runtime / 'manifest.json'
    if args.command == 'snapshot':
        manifest_path.write_text(json.dumps(snapshot(root, runtime), sort_keys=True) + '\n')
    else:
        verify(json.loads(manifest_path.read_text()), root, runtime, args.bundles)
    print('Verified baked v7 runtime: CUDA 13.0, pinned packages, nodes and native binary')

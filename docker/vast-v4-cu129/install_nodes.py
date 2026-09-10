"""Build-time only. Install the public, immutable v4 node contract; no models."""
import json
import pathlib
import re
import subprocess
import urllib.request

ROOT = pathlib.Path('/opt/furgen/v4')
COMFY = ROOT / 'ComfyUI'
PYTHON = '/venv/main/bin/python'
bundles = json.loads((ROOT / 'bundles.json').read_text())
seen = set()

def run(*args):
    subprocess.run(args, check=True)

for bundle in bundles.values():
    steps = bundle['steps'] + bundle.get('compatibility', {}).get('steps', [])
    for step in steps:
        key = json.dumps(step, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        kind = step['type']
        if kind == 'git_custom_node':
            assert re.fullmatch('[0-9a-f]{40}', step['ref'])
            target = COMFY / 'custom_nodes' / step['directoryName']
            if not target.exists():
                run('git', 'clone', '--filter=blob:none', step['repository'], str(target))
            run('git', '-C', str(target), 'checkout', step['ref'])
            run('git', '-C', str(target), 'submodule', 'update', '--init', '--recursive')
            requirements = target / 'requirements.txt'
            if step.get('installRequirements', True) and requirements.exists():
                run(PYTHON, '-m', 'pip', 'install', '--no-cache-dir', *step.get('pipArgs', []), '-r', str(requirements))
        elif kind == 'pip_install':
            indexes = [v for url in step.get('extraIndexUrls', []) for v in ['--extra-index-url', url]]
            run(PYTHON, '-m', 'pip', 'install', '--no-cache-dir', *indexes, *step['packages'])
        elif kind == 'furgen_support_custom_node':
            base = step['furgenPubRawBaseUrl']
            assert re.search('/[0-9a-f]{40}/docker/support$', base)
            target = COMFY / 'custom_nodes' / step['packageName']
            target.mkdir(parents=True, exist_ok=True)
            for name in ['__init__.py', 'furgen_video_tools.py']:
                urllib.request.urlretrieve(base + '/custom_nodes/' + step['packageName'] + '/' + name, target / name)
        elif kind != 'python_import_check':
            raise ValueError('Unsupported build step: ' + kind)

# Extract only the agent bootstrap functions; the full provisioning script is
# never sourced on image boot. This immutable source supplies the watchdog's
# liveness-only and credential-persistence behavior already used in production.
support = urllib.request.urlopen('https://raw.githubusercontent.com/Dodzilla/FurgenPub/2f85546548af3a57f9e7c5164d6a14aaafc09a73/docker/support/video_gen_v3.sh').read().decode()
start = support.index('function dependency_manager_is_disabled()')
end = support.index('# Start the dependency manager agent (best-effort;')
bootstrap = ROOT / 'agent_bootstrap.sh'
bootstrap.write_text('#!/bin/bash\nset -e\n' + support[start:end] + '\ndependency_manager_start_agent\n')
run('bash', '-n', str(bootstrap))

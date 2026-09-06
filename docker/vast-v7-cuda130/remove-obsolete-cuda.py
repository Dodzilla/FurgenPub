"""Remove obsolete cu12 wheel files without deleting shared cu13-owned files."""
import base64
import argparse
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path


def clean():
    distributions = list(metadata.distributions())
    obsolete = [d for d in distributions if d.metadata['Name'].lower().startswith('nvidia-')
                and d.metadata['Name'].lower().endswith('-cu12')]
    retained = [d for d in distributions if d not in obsolete]
    retained_paths = {Path(d.locate_file(f)).absolute() for d in retained for f in d.files or []}
    removed_bytes = 0
    removed_names = []
    for distribution in obsolete:
        name = distribution.metadata['Name']
        removed_names.append(name.lower().replace('_', '-'))
        empty_candidates = set()
        for entry in distribution.files or []:
            path = Path(distribution.locate_file(entry)).absolute()
            if path in retained_paths or not path.is_file():
                continue
            removed_bytes += path.stat().st_size
            path.unlink()
            empty_candidates.add(path.parent)
        for directory in sorted(empty_candidates, key=lambda p: len(p.parts), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                pass
        print('Removed obsolete distribution:', name)
    # Both generations use some identical paths. Verify the remaining generation
    # against its own wheel RECORD rather than assuming shared files survived.
    for distribution in retained:
        if not distribution.metadata['Name'].lower().startswith('nvidia-'):
            continue
        for entry in distribution.files or []:
            if entry.hash is None:
                continue
            path = Path(distribution.locate_file(entry))
            digest = hashlib.new(entry.hash.mode)
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b''):
                    digest.update(chunk)
            actual = base64.urlsafe_b64encode(digest.digest()).rstrip(b'=').decode()
            assert actual == entry.hash.value, f'Retained CUDA file differs: {path}'
    print('Obsolete wheel bytes removed:', removed_bytes)
    return removed_names


def update_manifest(path, removed_names):
    manifest = json.loads(path.read_text())
    for name in removed_names:
        assert name.startswith('nvidia-') and name.endswith('-cu12'), 'Unexpected manifest removal'
        del manifest['packages'][name]
    # Preserve every remaining version, node revision and native binary hash.
    path.write_text(json.dumps(manifest, sort_keys=True) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, type=Path)
    args = parser.parse_args()
    update_manifest(args.manifest, clean())

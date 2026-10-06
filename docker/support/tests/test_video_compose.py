import json
import subprocess

import numpy as np
from PIL import Image
from test_video_compat_nodes import _load_furgen_h3_video_tools


def ff(*args):
    return subprocess.run(['ffmpeg', '-v', 'error', '-y', *map(str, args)], capture_output=True, check=True).stdout


def test_exact_layers_hold_frame_and_stop_audio(tmp_path):
    module = _load_furgen_h3_video_tools()
    base, video, image, output = [tmp_path / name for name in ['base.mp4', 'excerpt.mp4', 'alpha.png', 'output.mp4']]
    ff('-f', 'lavfi', '-i', 'color=blue:s=160x90:r=30:d=3', '-f', 'lavfi', '-i', 'sine=frequency=400:duration=3', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', base)
    ff('-f', 'lavfi', '-i', 'color=red:s=32x32:r=30:d=0.5', '-f', 'lavfi', '-i', 'sine=frequency=2000:duration=0.5', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', video)
    rgba = np.zeros((20, 40, 4), dtype=np.uint8)
    rgba[:, :20] = [0, 255, 0, 255]
    Image.fromarray(rgba).save(image)
    manifest = {'width': 160, 'height': 90, 'frameRate': 30, 'durationSeconds': 3, 'layers': [
        {'id': 'base', 'kind': 'video', 'sourceUrl': 'base', 'trimEndSeconds': 3},
        {'id': 'image', 'kind': 'image', 'sourceUrl': 'image', 'zIndex': 1, 'startOffsetSeconds': .5, 'endOffsetSeconds': 1.5,
         'placement': {'x': 0, 'y': 0, 'width': .5, 'height': .5, 'objectFit': 'contain'}},
        {'id': 'video', 'kind': 'video', 'sourceUrl': 'video', 'zIndex': 2, 'startOffsetSeconds': 1, 'endOffsetSeconds': 2.5,
         'trimStartSeconds': 0, 'trimEndSeconds': .5, 'placement': {'x': .5, 'y': .5, 'width': .5, 'height': .5, 'objectFit': 'cover'}}],
        'audioTracks': [{'clips': [{'sourceAudioUrl': 'base'}]}, {'clips': [{'sourceAudioUrl': 'video', 'startSeconds': 1, 'endSeconds': 2.5, 'trimEndSeconds': .5}]}]}
    module.FCSComposeVideos.render(manifest, {'base': str(base), 'image': str(image), 'video': str(video)}, str(output))
    frames = np.frombuffer(ff('-i', output, '-an', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'), np.uint8).reshape(-1, 90, 160, 3)
    assert len(frames) == 90
    assert frames[5, 20, 10, 2] > 240
    assert frames[30, 20, 10, 1] > 235  # exact green half of transparent PNG
    assert frames[30, 20, 70, 2] > 235  # transparent half preserves base
    assert frames[50, 20, 10, 2] > 235  # hidden after end
    assert frames[65, 70, 120, 0] > 235  # video holds red final frame
    assert frames[80, 70, 120, 2] > 235  # ends at exclusive visibility boundary
    audio = np.frombuffer(ff('-i', output, '-vn', '-ac', 1, '-ar', 48000, '-f', 'f32le', '-'), np.float32)
    def power(start, frequency):
        window = audio[int(start*48000):int((start+.2)*48000)]
        return abs(np.fft.rfft(window)[round(frequency*.2)])
    assert power(1.1, 2000) > 20
    assert power(2.0, 2000) < 1
    assert power(2.0, 400) > 20  # base dialogue/audio continues beneath held video
    info = json.loads(subprocess.run(['ffprobe', '-v', 'error', '-show_format', '-of', 'json', str(output)], capture_output=True, check=True).stdout)
    assert abs(float(info['format']['duration']) - 3) < .05


def test_layer_order_opacity_and_consumed_tail(tmp_path):
    module = _load_furgen_h3_video_tools()
    video, output = tmp_path / 'moving.mp4', tmp_path / 'held.mp4'
    ff('-f', 'lavfi', '-i', 'color=red:s=32x32:r=24:d=0.5', '-f', 'lavfi', '-i', 'color=green:s=32x32:r=24:d=0.5', '-filter_complex', '[0:v][1:v]concat=n=2:v=1:a=0', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', video)
    image = tmp_path / 'red.png'
    Image.new('RGBA', (64, 32), (255, 0, 0, 255)).save(image)
    layers = [
        {'id': 'held', 'kind': 'video', 'sourceUrl': 'v', 'trimEndSeconds': 1, 'holdOnly': True, 'zIndex': 1},
        {'id': 'half-red', 'kind': 'image', 'sourceUrl': 'i', 'zIndex': 1, 'opacity': .5,
         'placement': {'x': 0, 'y': 0, 'width': .5, 'height': 1, 'objectFit': 'cover'}}]
    module.FCSComposeVideos.render({'width': 160, 'height': 90, 'durationSeconds': 1, 'frameRate': 30, 'layers': layers}, {'v': str(video), 'i': str(image)}, str(output))
    frames = np.frombuffer(ff('-i', output, '-an', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'), np.uint8).reshape(-1, 90, 160, 3)
    assert np.all(frames[:, 45, 100, 1] > 115)  # last source frame, even with 24→30 fps
    assert np.all(frames[:, 45, 100, 0] < 10)
    assert np.all((frames[:, 45, 50, 0] > 115) & (frames[:, 45, 50, 0] < 140))
    assert np.all((frames[:, 45, 50, 1] > 50) & (frames[:, 45, 50, 1] < 75))  # stable tie draws red above green


def test_public_fetch_rejects_private_and_redirected_private_hosts(tmp_path, monkeypatch):
    import socket
    import pytest
    module = _load_furgen_h3_video_tools()
    monkeypatch.setattr(socket, 'getaddrinfo', lambda host, *a, **k: [(2, 1, 6, '', ('127.0.0.1' if host == 'private.test' else '8.8.8.8', 443))])
    with pytest.raises(ValueError, match='private'):
        module.FCSComposeVideos._fetch('https://private.test/video', tmp_path / 'private')
    def redirect(cmd, **kwargs):
        header = cmd[cmd.index('--dump-header') + 1]
        target = cmd[cmd.index('--output') + 1]
        from pathlib import Path
        Path(header).write_text('HTTP/1.1 302 Found\nLocation: https://private.test/video\n\n')
        Path(target).write_bytes(b'')
    monkeypatch.setattr(module.subprocess, 'run', redirect)
    with pytest.raises(ValueError, match='private'):
        module.FCSComposeVideos._fetch('https://public.test/video', tmp_path / 'redirect')


def test_sealed_v4_and_h3_packages_ship_identical_compositors():
    import ast
    from pathlib import Path
    root = Path(__file__).parents[1] / 'custom_nodes'
    classes = []
    for package, filename in [('FurgenVideoTools', 'furgen_video_tools.py'), ('FurgenH3VideoTools', 'furgen_h3_video_tools.py')]:
        tree = ast.parse((root / package / filename).read_text())
        classes.append(ast.dump(next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'FCSComposeVideos')))
    assert classes[0] == classes[1]

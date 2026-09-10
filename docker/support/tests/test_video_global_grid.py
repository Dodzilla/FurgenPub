import json
import math
from pathlib import Path
import subprocess
import wave

import numpy as np
import pytest

from test_video_compat_nodes import _load_furgen_video_tools


def ff(*args, data=None):
    return subprocess.run(['ffmpeg', '-v', 'error', '-y', *map(str, args)], input=data,
                          capture_output=True, check=True).stdout


def pixels(path):
    return np.frombuffer(ff('-i', path, '-an', '-pix_fmt', 'rgb24', '-fps_mode', 'passthrough',
                            '-f', 'rawvideo', '-'), np.uint8).reshape(-1, 24, 32, 3).mean((1, 2))


def source(tmp_path, rate, audio):
    path = tmp_path / f'source-{rate}.mp4'
    n = np.arange(rate * 31)
    colors = np.array([n // 128 * 35, n % 128 * 2, n * 17 % 251]).T.astype(np.uint8)
    frames = np.broadcast_to(colors[:, None, None, :], (len(n), 24, 32, 3)).copy()
    ff('-f', 'rawvideo', '-pixel_format', 'rgb24', '-video_size', '32x24', '-framerate', rate,
       '-i', '-', '-i', audio, '-c:v', 'libx264', '-crf', 0, '-pix_fmt', 'yuv420p',
       '-video_track_timescale', 122880, '-c:a', 'alac', path, data=frames.tobytes())
    return path


def audio_source(tmp_path):
    t = np.arange(32000 * 31) / 32000
    signal = np.zeros(len(t))
    for center in (.1, .7, 3.1, 11.8, 14.5, 30.0):
        u = t - center
        signal += .5 * np.exp(-(u / .006) ** 2) * np.sin(2 * np.pi * (1100 * u + 9000 * u * u))
    path = tmp_path / 'markers.wav'
    with wave.open(str(path), 'wb') as w:
        w.setparams((1, 2, 32000, 0, 'NONE', 'NONE'))
        w.writeframes((signal * 32767).astype('<i2').tobytes())
    return path


@pytest.mark.parametrize('spec', [
    [(24, 0, 3.2)] + [(24, 1/24, .8)] * 3,
    [(30, 0, 3.2)] + [(60, 1/60, .8)] * 3,
    [(60, 0, 14.6)] + [(24, 1/24, 12)] * 3,
    [(30, 0, 30.15), (60, 0, 14.6), (24, 1/24, 12), (24, 1/24, 12)],
])
def test_global_grid_preserves_cumulative_frame_and_audio_clock(tmp_path, monkeypatch, spec):
    module = _load_furgen_video_tools()
    monkeypatch.setattr(module.folder_paths, 'get_output_directory', lambda: str(tmp_path))
    audio = audio_source(tmp_path)
    sources = {rate: source(tmp_path, rate, audio) for rate in {item[0] for item in spec}}
    segments = [(sources[rate], rate, start, end) for rate, start, end in spec]
    clips = [{'sourceVideoUrl': str(p), 'trimStartSeconds': start, 'trimEndSeconds': end}
             for p, _, start, end in segments]
    result = module.FCSConcatVideosV4GlobalGrid().concat_videos_v4(
        json.dumps({'clips': clips}), 32, 24, 60,
        'equalPower', 'grid', 'yuv420p', 0, True)
    out = result['result'][0][1][1]
    actual, expected, timeline, marker_times = pixels(out), [], 0, []
    receipt = result['ui']['compositionTiming'][0]
    assert json.loads(Path(result['result'][0][1][2]).read_text()) == receipt
    history_file = result['ui']['files'][0]
    assert history_file['type'] == 'output'
    assert json.loads((tmp_path / history_file['subfolder'] / history_file['filename']).read_text()) == receipt
    assert receipt['totalFrames'] == len(actual)
    for path, rate, start, end in segments:
        palette = pixels(path)
        duration = end - start
        first, last = math.ceil(timeline * 60 - 1e-7), math.ceil((timeline + duration) * 60 - 1e-7)
        first_source = round(start * rate)
        last_source = math.ceil(end * rate - 1e-7) - 1
        for frame in range(first, last):
            index = first_source + math.floor((frame / 60 - timeline) * rate + 1e-7)
            expected.append(palette[min(index, last_source)])
        marker_times.append(timeline + .1 - start)
        timeline += duration
    assert len(actual) == math.ceil(timeline * 60 - 1e-7)
    assert receipt['totalAudioSamples'] == math.ceil(timeline * 48000 - 1e-7)
    assert receipt['nominalDurationSeconds'] == pytest.approx(timeline)
    assert np.max(np.abs(actual - expected)) < 2
    if spec[0][2] == 30.15:
        clips[0]['framing'] = {'fadeOutSeconds': .6}
        clips[1]['framing'] = {'fadeInSeconds': .4}
        result = module.FCSConcatVideosV4GlobalGrid().concat_videos_v4(
            json.dumps({'clips': clips}), 32, 24, 60,
            'equalPower', 'faded-grid', 'yuv420p', 0, True)
        out = result['result'][0][1][1]
        faded = pixels(out)
        assert len(faded) == len(actual)
        assert faded[1808].mean() < actual[1808].mean() * .1
        assert faded[1809].max() < 2
        marker_times.append(30.0)
    pcm = np.frombuffer(ff('-i', out, '-vn', '-ac', 1, '-ar', 48000, '-f', 'f32le', '-'), '<f4')
    for expected_time in marker_times:
        lo, hi = int((expected_time - .02) * 48000), int((expected_time + .02) * 48000)
        energy = np.convolve(pcm[lo:hi].astype(float) ** 2, np.ones(96) / 96, mode='same')
        measured = (lo + energy.argmax()) / 48000
        assert abs(measured - expected_time) < .0005


def test_global_grid_rejects_overlapping_transitions():
    module = _load_furgen_video_tools()
    with pytest.raises(ValueError, match='hard cuts'):
        module._v4_global_grid([{'_output_duration': 1, 'transitionAfter': {'type': 'crossfade'}},
                                {'_output_duration': 1}], 60)


def test_global_grid_preserves_vfr_source_order(tmp_path, monkeypatch):
    module = _load_furgen_video_tools()
    monkeypatch.setattr(module.folder_paths, 'get_output_directory', lambda: str(tmp_path))
    base = source(tmp_path, 30, audio_source(tmp_path))
    path = tmp_path / 'vfr.mkv'
    ff('-i', base, '-vf', 'trim=end_frame=120,settb=1/1000,'
       r'setpts=floor(N/3)*100+if(eq(mod(N\,3)\,1)\,17\,if(eq(mod(N\,3)\,2)\,50\,0))',
       '-fps_mode', 'passthrough', '-enc_time_base', '1:1000', '-c:v', 'ffv1', '-c:a', 'pcm_s16le', path)
    clips = [{'sourceVideoUrl': str(path), 'trimStartSeconds': start, 'trimEndSeconds': end}
             for start, end in [(0, 3.2)] + [(.017, .8)] * 3]
    result = module.FCSConcatVideosV4GlobalGrid().concat_videos_v4(
        json.dumps({'clips': clips}), 32, 24, 60, 'equalPower', 'vfr', 'yuv420p', 0, True)
    actual, palette = pixels(result['result'][0][1][1]), pixels(path)
    pts = np.array([n // 3 * .1 + (0, .017, .05)[n % 3] for n in range(120)])
    expected, timeline = [], 0
    for clip in clips:
        start, end = clip['trimStartSeconds'], clip['trimEndSeconds']
        valid = np.flatnonzero((pts >= start - 1e-9) & (pts < end - 1e-9))
        for n in range(math.ceil(timeline*60-1e-7), math.ceil((timeline+end-start)*60-1e-7)):
            local = n/60-timeline+pts[valid[0]]
            selected = valid[pts[valid] <= local+1e-9][-1]
            expected.append(palette[selected])
        timeline += end-start
    assert len(actual) == math.ceil(timeline*60-1e-7)
    assert np.max(np.abs(actual-expected)) < 2
    assert result['ui']['compositionTiming'][0]['totalFrames'] == len(actual)


def test_global_grid_capability_forces_timing_mode(monkeypatch):
    module = _load_furgen_video_tools()
    seen = []
    monkeypatch.setattr(module.FCSConcatVideosV4, 'concat_videos_v4',
                        lambda self, manifest, *args: seen.append(json.loads(manifest)))
    module.NODE_CLASS_MAPPINGS['FCSConcatVideosV4GlobalGrid']().concat_videos_v4(
        '{"timingMode":"legacy","clips":[]}', 32, 24, 60, 'equalPower', 'grid', 'yuv420p', 0, True)
    assert seen == [{'timingMode': 'global-grid', 'clips': []}]


def test_global_grid_ignores_unused_final_transition():
    module = _load_furgen_video_tools()
    receipt = module._v4_global_grid(
        [{'_output_duration': 1, 'transitionAfter': {'type': 'crossfade'}}], 60)
    assert receipt['totalFrames'] == 60

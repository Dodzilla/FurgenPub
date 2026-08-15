import json
import hashlib
import math
import os
import re
import subprocess
from pathlib import Path

import folder_paths
import numpy as np
import torch
import torch.nn.functional as F
from PIL import Image


FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")
FCS_SEAM_REPAIR_NONE = "none"
FCS_SEAM_REPAIR_BLEND2_AFTER_TRIM = "blend2_after_trim"
FCS_SEAM_REPAIR_BLEND2_AFTER_CONCAT = "blend2_after_concat"


RGB_LUMA_WEIGHTS = (0.2126, 0.7152, 0.0722)
V2_FRAME_CHUNK_SIZE = 2
V2_STAT_SAMPLE_PIXELS = 65536


def _is_url(value: str) -> bool:
    return "://" in value


def _resolve_video_entry(value: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError("empty video entry")
    if _is_url(candidate) or os.path.isabs(candidate):
        return candidate
    return folder_paths.get_annotated_filepath(candidate)


def _parse_video_entries(video_entries: str) -> list[str]:
    entries = []
    for raw_line in (video_entries or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(_resolve_video_entry(line))
    if len(entries) < 1:
        raise ValueError("at least one video entry is required")
    return entries


def _probe_video(path: str) -> dict:
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(proc.stdout or "{}")
    streams = payload.get("streams", [])
    format_info = payload.get("format", {})
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    if video_stream is None:
        raise ValueError(f"no video stream found for {path}")
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
    duration = 0.0
    for source in (video_stream, audio_stream, format_info):
        value = source.get("duration") if isinstance(source, dict) else None
        if value not in (None, ""):
            try:
                duration = max(duration, float(value))
            except Exception:
                pass
    if duration <= 0:
        duration = 0.001
    return {
        "path": path,
        "width": int(video_stream.get("width") or 0),
        "height": int(video_stream.get("height") or 0),
        "duration": duration,
        "has_audio": audio_stream is not None,
    }


def _fraction(value) -> float:
    if value in (None, "", "0/0"):
        return 0.0
    text = str(value)
    if "/" in text:
        numerator, denominator = text.split("/", 1)
        denominator_value = float(denominator)
        return float(numerator) / denominator_value if denominator_value else 0.0
    return float(text)


def _probe_video_details(path: str) -> dict:
    cmd = [
        FFPROBE_BIN,
        "-v", "error",
        "-print_format", "json",
        "-count_frames",
        "-show_streams",
        "-show_format",
        path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(proc.stdout or "{}")
    streams = payload.get("streams") or []
    format_info = payload.get("format") or {}
    video = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    if not video:
        raise ValueError(f"no video stream found for {path}")
    audio = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)
    duration = max(
        [
            float(value)
            for value in (video.get("duration"), audio and audio.get("duration"), format_info.get("duration"))
            if value not in (None, "")
        ] or [0.001]
    )
    frame_rate = _fraction(video.get("avg_frame_rate") or video.get("r_frame_rate"))
    frame_count = int(video.get("nb_read_frames") or video.get("nb_frames") or round(duration * frame_rate))
    bitrate = int(format_info.get("bit_rate") or video.get("bit_rate") or 0)
    return {
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "duration_seconds": duration,
        "frame_rate": frame_rate,
        "frame_count": frame_count,
        "bitrate": bitrate,
        "has_audio": audio is not None,
        "audio_sample_rate": int(audio.get("sample_rate") or 0) if audio else None,
        "audio_channels": int(audio.get("channels") or 0) if audio else None,
    }


def _parse_seam_repair_weights(value: str) -> list[float]:
    weights = []
    for raw in str(value or "").replace(";", ",").split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            weights.append(max(0.0, min(1.0, float(item))))
        except Exception:
            continue
    return weights or [0.35, 0.15]


def _read_exact(pipe, size: int) -> bytes:
    chunks = []
    remaining = int(size)
    while remaining > 0:
        chunk = pipe.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _blend_rgb24_frame(source: bytes, frame: bytes, source_weight: float) -> bytes:
    if not source or len(source) != len(frame):
        return frame
    source_weight = max(0.0, min(1.0, float(source_weight)))
    frame_weight = 1.0 - source_weight
    out = bytearray(len(frame))
    src_view = memoryview(source)
    frame_view = memoryview(frame)
    for idx in range(len(frame)):
        out[idx] = min(255, max(0, int(src_view[idx] * source_weight + frame_view[idx] * frame_weight + 0.5)))
    return bytes(out)


class FCSConcatVideos:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "video_entries": (
                    "STRING",
                    {
                        "default": "clip_a.mp4\nclip_b.mp4",
                        "multiline": True,
                    },
                ),
                "frame_rate": (
                    "FLOAT",
                    {"default": 60.0, "min": 1.0, "max": 120.0, "step": 1.0},
                ),
                "overlap_frames": (
                    "INT",
                    {"default": 1, "min": 0, "max": 120, "step": 1},
                ),
                "filename_prefix": (
                    "STRING",
                    {"default": "video_concat"},
                ),
                "pix_fmt": (["yuv420p"],),
                "crf": (
                    "INT",
                    {"default": 17, "min": 0, "max": 51, "step": 1},
                ),
                "save_output": (
                    "BOOLEAN",
                    {"default": True},
                ),
            },
            "optional": {
                "seam_repair_mode": (
                    [
                        FCS_SEAM_REPAIR_NONE,
                        FCS_SEAM_REPAIR_BLEND2_AFTER_TRIM,
                        FCS_SEAM_REPAIR_BLEND2_AFTER_CONCAT,
                    ],
                    {"default": FCS_SEAM_REPAIR_NONE},
                ),
                "seam_repair_source_weights": (
                    "STRING",
                    {"default": "0.35,0.15"},
                ),
            },
        }

    RETURN_TYPES = ("VHS_FILENAMES",)
    RETURN_NAMES = ("Filenames",)
    OUTPUT_NODE = True
    CATEGORY = "Furgen"
    FUNCTION = "concat_videos"

    def concat_videos(
        self,
        video_entries,
        frame_rate,
        overlap_frames,
        filename_prefix,
        pix_fmt,
        crf,
        save_output,
        seam_repair_mode=FCS_SEAM_REPAIR_NONE,
        seam_repair_source_weights="0.35,0.15",
    ):
        entries = _parse_video_entries(video_entries)
        probes = [_probe_video(entry) for entry in entries]
        base_width = probes[0]["width"] or 1920
        base_height = probes[0]["height"] or 1088
        overlap_frames = max(0, int(overlap_frames or 0))
        frame_rate = float(frame_rate or 60.0)
        overlap_seconds = float(overlap_frames) / frame_rate if overlap_frames > 0 else 0.0
        seam_repair_mode = str(seam_repair_mode or FCS_SEAM_REPAIR_NONE).strip()
        if seam_repair_mode == FCS_SEAM_REPAIR_BLEND2_AFTER_CONCAT:
            seam_repair_mode = FCS_SEAM_REPAIR_BLEND2_AFTER_TRIM
        seam_repair_weights = _parse_seam_repair_weights(seam_repair_source_weights)

        if overlap_seconds > 0:
            for idx, probe in enumerate(probes):
                if idx == 0:
                    continue
                if probe["duration"] <= overlap_seconds:
                    raise ValueError(
                        f"clip {idx + 1} is too short for overlap trim: duration={probe['duration']:.3f}s overlap={overlap_seconds:.3f}s"
                    )

        output_dir = (
            folder_paths.get_output_directory()
            if save_output
            else folder_paths.get_temp_directory()
        )
        full_output_folder, filename, _, subfolder, _ = folder_paths.get_save_image_path(
            filename_prefix,
            output_dir,
        )
        existing = sorted(Path(full_output_folder).glob(f"{filename}_*.mp4"))
        counter = 1
        if existing:
            suffixes = []
            for item in existing:
                stem = item.stem
                parts = stem.split("_")
                if parts:
                    tail = parts[-1].replace("-audio", "")
                    if tail.isdigit():
                        suffixes.append(int(tail))
            if suffixes:
                counter = max(suffixes) + 1

        base_file = f"{filename}_{counter:05}.mp4"
        audio_file = f"{filename}_{counter:05}-audio.mp4"
        base_path = os.path.join(full_output_folder, base_file)
        audio_path = os.path.join(full_output_folder, audio_file)

        if seam_repair_mode == FCS_SEAM_REPAIR_BLEND2_AFTER_TRIM and len(probes) > 1:
            self._concat_videos_with_seam_repair(
                probes=probes,
                frame_rate=frame_rate,
                overlap_frames=overlap_frames,
                overlap_seconds=overlap_seconds,
                base_width=base_width,
                base_height=base_height,
                pix_fmt=pix_fmt,
                crf=crf,
                base_path=base_path,
                audio_path=audio_path,
                source_weights=seam_repair_weights,
            )
        else:
            self._concat_videos_ffmpeg_filtergraph(
                probes=probes,
                frame_rate=frame_rate,
                overlap_frames=overlap_frames,
                overlap_seconds=overlap_seconds,
                base_width=base_width,
                base_height=base_height,
                pix_fmt=pix_fmt,
                crf=crf,
                base_path=base_path,
                audio_path=audio_path,
            )

        preview = {
            "filename": audio_file,
            "subfolder": subfolder,
            "type": "output" if save_output else "temp",
            "format": "video/h264-mp4",
            "frame_rate": frame_rate,
            "fullpath": audio_path,
        }
        return {
            "ui": {"gifs": [preview]},
            "result": ((save_output, [base_path, audio_path]),),
        }

    def _concat_videos_ffmpeg_filtergraph(
        self,
        *,
        probes,
        frame_rate,
        overlap_frames,
        overlap_seconds,
        base_width,
        base_height,
        pix_fmt,
        crf,
        base_path,
        audio_path,
    ):
        ffmpeg_inputs = []
        filter_parts = []
        concat_inputs = []
        for idx, probe in enumerate(probes):
            clip_trim_seconds = overlap_seconds if idx > 0 else 0.0
            clip_trim_frames = overlap_frames if idx > 0 else 0
            ffmpeg_inputs.extend(["-i", probe["path"]])
            video_filters = [
                f"fps={frame_rate}",
            ]
            if clip_trim_frames > 0:
                video_filters.extend(
                    [
                        f"select='gte(n,{clip_trim_frames})'",
                        f"setpts=N/{float(frame_rate):.6f}/TB",
                    ]
                )
            video_filters.extend(
                [
                    f"scale={base_width}:{base_height}:flags=lanczos:force_original_aspect_ratio=decrease",
                    f"pad={base_width}:{base_height}:(ow-iw)/2:(oh-ih)/2:black",
                    f"format={pix_fmt}",
                    "setsar=1",
                ]
            )
            filter_parts.append(f"[{idx}:v]{','.join(video_filters)}[v{idx}]")
            if probe["has_audio"]:
                audio_filters = [
                    "aresample=48000",
                    "aformat=sample_fmts=fltp:channel_layouts=stereo",
                ]
                if clip_trim_seconds > 0:
                    audio_filters.extend(
                        [
                            f"atrim=start={clip_trim_seconds:.6f}",
                            "asetpts=PTS-STARTPTS",
                        ]
                    )
                filter_parts.append(f"[{idx}:a]{','.join(audio_filters)}[a{idx}]")
            else:
                silent_duration = max(0.001, probe["duration"] - clip_trim_seconds)
                filter_parts.append(
                    f"anullsrc=channel_layout=stereo:sample_rate=48000:d={silent_duration:.6f}[a{idx}]"
                )
            concat_inputs.extend([f"[v{idx}]", f"[a{idx}]"])

        filter_parts.append("".join(concat_inputs) + f"concat=n={len(probes)}:v=1:a=1[v][a]")
        cmd = [
            FFMPEG_BIN,
            "-y",
            "-v",
            "error",
            *ffmpeg_inputs,
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            str(crf),
            "-pix_fmt",
            pix_fmt,
            "-r",
            str(frame_rate),
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            audio_path,
        ]
        subprocess.run(cmd, check=True)

        subprocess.run(
            [FFMPEG_BIN, "-y", "-v", "error", "-i", audio_path, "-an", "-c:v", "copy", base_path],
            check=True,
        )

    def _concat_videos_with_seam_repair(
        self,
        *,
        probes,
        frame_rate,
        overlap_frames,
        overlap_seconds,
        base_width,
        base_height,
        pix_fmt,
        crf,
        base_path,
        audio_path,
        source_weights,
    ):
        frame_size = int(base_width) * int(base_height) * 3
        if frame_size <= 0:
            raise ValueError("invalid concat frame size for seam repair")

        encode_cmd = [
            FFMPEG_BIN,
            "-y",
            "-v",
            "error",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "rgb24",
            "-s",
            f"{base_width}x{base_height}",
            "-r",
            f"{float(frame_rate):.6f}",
            "-i",
            "-",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            str(crf),
            "-pix_fmt",
            pix_fmt,
            "-r",
            f"{float(frame_rate):.6f}",
            "-movflags",
            "+faststart",
            base_path,
        ]
        encoder = subprocess.Popen(encode_cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        assert encoder.stdin is not None
        previous_last_frame = None
        try:
            for idx, probe in enumerate(probes):
                clip_trim_frames = overlap_frames if idx > 0 else 0
                video_filters = [f"fps={float(frame_rate):.6f}"]
                if clip_trim_frames > 0:
                    video_filters.extend(
                        [
                            f"select='gte(n,{clip_trim_frames})'",
                            f"setpts=N/{float(frame_rate):.6f}/TB",
                        ]
                    )
                video_filters.extend(
                    [
                        f"scale={base_width}:{base_height}:flags=lanczos:force_original_aspect_ratio=decrease",
                        f"pad={base_width}:{base_height}:(ow-iw)/2:(oh-ih)/2:black",
                        "format=rgb24",
                        "setsar=1",
                    ]
                )
                decode_cmd = [
                    FFMPEG_BIN,
                    "-v",
                    "error",
                    "-i",
                    probe["path"],
                    "-vf",
                    ",".join(video_filters),
                    "-f",
                    "rawvideo",
                    "-pix_fmt",
                    "rgb24",
                    "-",
                ]
                decoder = subprocess.Popen(decode_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                assert decoder.stdout is not None
                frame_index = 0
                current_last_frame = None
                while True:
                    frame = _read_exact(decoder.stdout, frame_size)
                    if not frame:
                        break
                    if len(frame) != frame_size:
                        raise ValueError("short raw frame while concatenating videos")
                    if previous_last_frame is not None and frame_index < len(source_weights):
                        frame = _blend_rgb24_frame(previous_last_frame, frame, source_weights[frame_index])
                    encoder.stdin.write(frame)
                    current_last_frame = frame
                    frame_index += 1
                decoder_rc = decoder.wait()
                stderr = decoder.stderr.read().decode("utf-8", errors="replace") if decoder.stderr else ""
                if decoder_rc != 0:
                    raise RuntimeError(f"ffmpeg decode failed for clip {idx + 1}: {stderr[-2000:]}")
                if current_last_frame is None:
                    raise ValueError(f"clip {idx + 1} produced no frames after trim")
                previous_last_frame = current_last_frame
        finally:
            try:
                encoder.stdin.close()
            except Exception:
                pass

        encoder_rc = encoder.wait()
        encoder_stderr = encoder.stderr.read().decode("utf-8", errors="replace") if encoder.stderr else ""
        if encoder_rc != 0:
            raise RuntimeError(f"ffmpeg seam-repair video encode failed: {encoder_stderr[-2000:]}")

        audio_tmp = f"{os.path.splitext(audio_path)[0]}-track.m4a"
        try:
            self._write_concat_audio_track(
                probes=probes,
                overlap_seconds=overlap_seconds,
                output_path=audio_tmp,
            )
            subprocess.run(
                [
                    FFMPEG_BIN,
                    "-y",
                    "-v",
                    "error",
                    "-i",
                    base_path,
                    "-i",
                    audio_tmp,
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "copy",
                    "-shortest",
                    "-movflags",
                    "+faststart",
                    audio_path,
                ],
                check=True,
            )
        finally:
            try:
                if os.path.exists(audio_tmp):
                    os.remove(audio_tmp)
            except Exception:
                pass

    def _write_concat_audio_track(self, *, probes, overlap_seconds, output_path):
        ffmpeg_inputs = []
        filter_parts = []
        concat_inputs = []
        for idx, probe in enumerate(probes):
            clip_trim_seconds = overlap_seconds if idx > 0 else 0.0
            ffmpeg_inputs.extend(["-i", probe["path"]])
            if probe["has_audio"]:
                audio_filters = [
                    "aresample=48000",
                    "aformat=sample_fmts=fltp:channel_layouts=stereo",
                ]
                if clip_trim_seconds > 0:
                    audio_filters.extend(
                        [
                            f"atrim=start={clip_trim_seconds:.6f}",
                            "asetpts=PTS-STARTPTS",
                        ]
                    )
                filter_parts.append(f"[{idx}:a]{','.join(audio_filters)}[a{idx}]")
            else:
                silent_duration = max(0.001, probe["duration"] - clip_trim_seconds)
                filter_parts.append(
                    f"anullsrc=channel_layout=stereo:sample_rate=48000:d={silent_duration:.6f}[a{idx}]"
                )
            concat_inputs.append(f"[a{idx}]")

        filter_parts.append("".join(concat_inputs) + f"concat=n={len(probes)}:v=0:a=1[a]")
        cmd = [
            FFMPEG_BIN,
            "-y",
            "-v",
            "error",
            *ffmpeg_inputs,
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[a]",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            output_path,
        ]
        subprocess.run(cmd, check=True)


def _is_neutral(value: float, neutral: float) -> bool:
    return float(value) == float(neutral)


def _image_rgb(images: torch.Tensor) -> torch.Tensor:
    if images.ndim != 4 or images.shape[-1] < 1:
        raise ValueError("IMAGE tensor must have shape [batch, height, width, channels]")
    return images[..., : min(3, images.shape[-1])]


def _luma(rgb: torch.Tensor) -> torch.Tensor:
    if rgb.shape[-1] == 1:
        return rgb
    weights = torch.tensor(RGB_LUMA_WEIGHTS[: rgb.shape[-1]], dtype=rgb.dtype, device=rgb.device)
    weights = weights / weights.sum()
    return (rgb * weights).sum(dim=-1, keepdim=True)


def _blend_and_restore_channels(
    images: torch.Tensor,
    corrected_rgb: torch.Tensor,
    strength: float,
) -> torch.Tensor:
    blended_rgb = images[..., : corrected_rgb.shape[-1]].lerp(corrected_rgb, float(strength))
    if images.shape[-1] == corrected_rgb.shape[-1]:
        out = blended_rgb
    else:
        out = torch.cat((blended_rgb, images[..., corrected_rgb.shape[-1] :]), dim=-1)
    return out.clamp(0.0, 1.0)


def _restore_channels(images: torch.Tensor, corrected_rgb: torch.Tensor) -> torch.Tensor:
    if images.shape[-1] == corrected_rgb.shape[-1]:
        out = corrected_rgb
    else:
        out = torch.cat((corrected_rgb, images[..., corrected_rgb.shape[-1] :]), dim=-1)
    return out.clamp(0.0, 1.0)


def _broadcast_reference_rgb(images: torch.Tensor, reference: torch.Tensor) -> torch.Tensor:
    rgb = _image_rgb(images)
    ref_rgb = _image_rgb(reference).to(device=images.device, dtype=images.dtype)
    if ref_rgb.shape[0] == 1 and rgb.shape[0] != 1:
        return ref_rgb.expand(rgb.shape[0], -1, -1, -1)
    if ref_rgb.shape[0] != rgb.shape[0]:
        return ref_rgb[:1].expand(rgb.shape[0], -1, -1, -1)
    return ref_rgb


def _eps_for(tensor: torch.Tensor) -> float:
    return torch.finfo(tensor.dtype).eps if tensor.dtype.is_floating_point else 1e-6


def _node_tensor_summary(tensor: object) -> str:
    if not isinstance(tensor, torch.Tensor):
        return "non_tensor"
    return f"shape={tuple(tensor.shape)} dtype={tensor.dtype} device={tensor.device}"


def _node_runtime_error(class_name: str, images: object, phase: str, exc: Exception) -> RuntimeError:
    return RuntimeError(
        f"{class_name} failed phase={phase} input={_node_tensor_summary(images)} error={type(exc).__name__}: {exc}"
    )


def _first_reference_rgb(reference: torch.Tensor, images: torch.Tensor) -> torch.Tensor:
    return _image_rgb(reference[:1]).to(device=images.device, dtype=images.dtype)


def _chunked_frames(images: torch.Tensor, chunk_size=None):
    for start, end in _chunked_frame_ranges(images, chunk_size=chunk_size):
        yield images[start:end]


def _chunked_frame_ranges(images: torch.Tensor, chunk_size=None):
    batch = int(images.shape[0])
    if chunk_size is None:
        chunk_size = V2_FRAME_CHUNK_SIZE
    step = max(1, int(chunk_size))
    for start in range(0, batch, step):
        yield start, min(batch, start + step)


def _sample_pixels_channel_last(values: torch.Tensor, max_pixels=None) -> torch.Tensor:
    flat = values.reshape(-1, values.shape[-1])
    if max_pixels is None:
        max_pixels = V2_STAT_SAMPLE_PIXELS
    limit = max(1, int(max_pixels))
    if flat.shape[0] <= limit:
        return flat
    index = torch.linspace(0, flat.shape[0] - 1, steps=limit, device=flat.device).long()
    return flat.index_select(0, index)


def _mean_std_stats_single(values: torch.Tensor, max_pixels=None) -> tuple[torch.Tensor, torch.Tensor]:
    sample = _sample_pixels_channel_last(values, max_pixels=max_pixels)
    mean = sample.mean(dim=0).view(1, 1, 1, values.shape[-1])
    std = sample.std(dim=0, unbiased=False).view(1, 1, 1, values.shape[-1])
    return mean, std


def _mean_std_stats_frames(values: torch.Tensor, max_pixels=None) -> tuple[torch.Tensor, torch.Tensor]:
    means = []
    stds = []
    for index in range(values.shape[0]):
        mean, std = _mean_std_stats_single(values[index : index + 1], max_pixels=max_pixels)
        means.append(mean)
        stds.append(std)
    return torch.cat(means, dim=0), torch.cat(stds, dim=0)


def _mean_std_transfer_with_stats(
    source: torch.Tensor,
    ref_mean: torch.Tensor,
    ref_std: torch.Tensor,
    mean_strengths: torch.Tensor,
    std_strength: float,
    std_min: float,
    std_max: float,
) -> torch.Tensor:
    eps = _eps_for(source)
    src_mean, src_std = _mean_std_stats_frames(source)
    src_std = src_std.clamp_min(eps)
    ratio = (ref_std / src_std).clamp(float(std_min), float(std_max))
    ratio = torch.ones_like(ratio).lerp(ratio, max(0.0, min(1.0, float(std_strength))))
    return (source - src_mean) * ratio + src_mean + (ref_mean - src_mean) * mean_strengths


def _gaussian_kernel1d(radius: float, *, dtype: torch.dtype, device: torch.device, max_pad: int) -> torch.Tensor:
    pad = min(max_pad, max(1, int(round(float(radius) * 2.0))))
    coords = torch.arange(-pad, pad + 1, dtype=dtype, device=device)
    sigma = max(0.25, float(radius))
    kernel = torch.exp(-(coords * coords) / (2.0 * sigma * sigma))
    return kernel / kernel.sum().clamp_min(torch.finfo(dtype).eps if dtype.is_floating_point else 1e-6)


def _gaussian_blur_channel_last(rgb: torch.Tensor, radius: float) -> torch.Tensor:
    if rgb.ndim != 4 or rgb.shape[1] < 3 or rgb.shape[2] < 3:
        return rgb
    max_pad = max(1, min(int(rgb.shape[1]) - 1, int(rgb.shape[2]) - 1))
    kernel = _gaussian_kernel1d(radius, dtype=rgb.dtype, device=rgb.device, max_pad=max_pad)
    pad = int((kernel.numel() - 1) // 2)
    if pad < 1:
        return rgb
    nchw = rgb.permute(0, 3, 1, 2).contiguous()
    channels = int(nchw.shape[1])
    kernel_x = kernel.view(1, 1, 1, -1).expand(channels, 1, 1, -1)
    kernel_y = kernel.view(1, 1, -1, 1).expand(channels, 1, -1, 1)
    blurred = F.conv2d(F.pad(nchw, (pad, pad, 0, 0), mode="reflect"), kernel_x, groups=channels)
    blurred = F.conv2d(F.pad(blurred, (0, 0, pad, pad), mode="reflect"), kernel_y, groups=channels)
    return blurred.permute(0, 2, 3, 1)


def _threshold_detail(detail: torch.Tensor, threshold: float) -> torch.Tensor:
    threshold = max(0.0, float(threshold))
    if threshold <= 0.0:
        return detail
    mag = detail.abs()
    return detail * ((mag - threshold).clamp_min(0.0) / mag.clamp_min(_eps_for(detail)))


def _robust_luma_mean_single(
    luma: torch.Tensor,
    black_percentile: float,
    white_percentile: float,
    max_pixels=None,
) -> torch.Tensor:
    sample = _sample_pixels_channel_last(luma, max_pixels=max_pixels).reshape(-1)
    lo_p = max(0.0, min(1.0, float(black_percentile)))
    hi_p = max(0.0, min(1.0, float(white_percentile)))
    if sample.numel() < 2 or hi_p <= lo_p:
        return sample.mean().view(1, 1, 1, 1)
    lo = torch.quantile(sample, lo_p)
    hi = torch.quantile(sample, hi_p)
    return sample.clamp(lo, hi).mean().view(1, 1, 1, 1)


def _robust_luma_mean_frames(
    luma: torch.Tensor,
    black_percentile: float,
    white_percentile: float,
    max_pixels=None,
) -> torch.Tensor:
    means = [
        _robust_luma_mean_single(luma[index : index + 1], black_percentile, white_percentile, max_pixels=max_pixels)
        for index in range(luma.shape[0])
    ]
    return torch.cat(means, dim=0)


def _robust_luma_mean(luma: torch.Tensor, black_percentile: float, white_percentile: float) -> torch.Tensor:
    flat = luma.reshape(luma.shape[0], -1)
    lo_p = max(0.0, min(1.0, float(black_percentile)))
    hi_p = max(0.0, min(1.0, float(white_percentile)))
    if hi_p <= lo_p:
        return flat.mean(dim=1).view(-1, 1, 1, 1)
    lo = torch.quantile(flat, lo_p, dim=1, keepdim=True)
    hi = torch.quantile(flat, hi_p, dim=1, keepdim=True)
    return flat.clamp(lo, hi).mean(dim=1).view(-1, 1, 1, 1)


def _apply_highlight_protection(
    original_rgb: torch.Tensor,
    corrected_rgb: torch.Tensor,
    preserve_highlights: float,
) -> torch.Tensor:
    preserve = max(0.0, min(1.0, float(preserve_highlights)))
    if preserve <= 0.0:
        return corrected_rgb
    highlight = ((_luma(original_rgb).clamp(0.0, 1.0) - 0.70) / 0.30).clamp(0.0, 1.0)
    return corrected_rgb.lerp(original_rgb, highlight * preserve)


def _rgb_to_ycbcr(rgb: torch.Tensor) -> torch.Tensor:
    y = _luma(rgb)
    cb = (rgb[..., 2:3] - y) / 1.8556
    cr = (rgb[..., 0:1] - y) / 1.5748
    return torch.cat((y, cb, cr), dim=-1)


def _ycbcr_to_rgb(ycbcr: torch.Tensor) -> torch.Tensor:
    y = ycbcr[..., 0:1]
    cb = ycbcr[..., 1:2]
    cr = ycbcr[..., 2:3]
    r = y + 1.5748 * cr
    b = y + 1.8556 * cb
    g = (y - 0.2126 * r - 0.0722 * b) / 0.7152
    return torch.cat((r, g, b), dim=-1)


def _mean_std_transfer(
    source: torch.Tensor,
    reference: torch.Tensor,
    mean_strengths: torch.Tensor,
    std_strength: float,
    std_min: float,
    std_max: float,
) -> torch.Tensor:
    eps = _eps_for(source)
    src_mean = source.mean(dim=(1, 2), keepdim=True)
    ref_mean = reference.mean(dim=(1, 2), keepdim=True)
    src_std = source.std(dim=(1, 2), keepdim=True, unbiased=False).clamp_min(eps)
    ref_std = reference.std(dim=(1, 2), keepdim=True, unbiased=False)
    ratio = (ref_std / src_std).clamp(float(std_min), float(std_max))
    ratio = torch.ones_like(ratio).lerp(ratio, max(0.0, min(1.0, float(std_strength))))
    return (source - src_mean) * ratio + src_mean + (ref_mean - src_mean) * mean_strengths


def _parse_video_entries_with_options(video_entries: str) -> list[dict]:
    """Parse entries of the form 'url' or 'url|start=1.5|end=4.0'.

    start/end are seconds within the source clip. Unknown options are
    ignored so the format can grow without breaking older senders.
    """
    parsed: list[dict] = []
    for raw_line in (video_entries or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [part.strip() for part in line.split("|")]
        entry = {
            "path": _resolve_video_entry(parts[0]),
            "start": 0.0,
            "end": None,
            # V3 options. Neutral defaults, so a V2-style line behaves the same.
            "speed": 1.0,
            "brightness": 1.0,
            "contrast": 1.0,
            "saturation": 1.0,
            # Crossfade seconds into the NEXT entry (0 = straight concat).
            "xfade": 0.0,
        }
        for option in parts[1:]:
            key, sep, value = option.partition("=")
            if not sep:
                continue
            key = key.strip().lower()
            try:
                number = float(value.strip())
            except ValueError:
                continue
            if not math.isfinite(number):
                continue
            if key == "start" and number > 0:
                entry["start"] = number
            elif key in ("end", "stop") and number > 0:
                entry["end"] = number
            elif key == "speed" and number > 0:
                entry["speed"] = number
            elif key in ("brightness", "contrast", "saturation") and number > 0:
                entry[key] = number
            elif key in ("xfade", "crossfade") and number > 0:
                entry["xfade"] = number
        if entry["end"] is not None and entry["end"] <= entry["start"]:
            raise ValueError(f"video entry has an empty trim range: {line}")
        parsed.append(entry)
    if len(parsed) < 1:
        raise ValueError("at least one video entry is required")
    return parsed


class FCSConcatVideosV2(FCSConcatVideos):
    """Streaming concat with optional per-entry trims.

    video_entries lines accept 'url|start=S|end=E'; trims run inside the
    ffmpeg filtergraph so each clip's source audio follows its cut. The
    overlap_frames input still drops duplicated leading frames of clips 2+
    (applied after the entry's own start trim). Seam-repair inputs are
    accepted for signature compatibility but ignored.
    """

    def concat_videos(
        self,
        video_entries,
        frame_rate,
        overlap_frames,
        filename_prefix,
        pix_fmt,
        crf,
        save_output,
        seam_repair_mode=FCS_SEAM_REPAIR_NONE,
        seam_repair_source_weights="0.35,0.15",
    ):
        entries = _parse_video_entries_with_options(video_entries)
        probes = [_probe_video(entry["path"]) for entry in entries]
        base_width = probes[0]["width"] or 1920
        base_height = probes[0]["height"] or 1088
        overlap_frames = max(0, int(overlap_frames or 0))
        frame_rate = float(frame_rate or 60.0)
        overlap_seconds = float(overlap_frames) / frame_rate if overlap_frames > 0 else 0.0

        for idx, (entry, probe) in enumerate(zip(entries, probes)):
            start = float(entry["start"]) + (overlap_seconds if idx > 0 else 0.0)
            end = entry["end"] if entry["end"] is not None else probe["duration"]
            if end is not None and probe["duration"]:
                end = min(float(end), float(probe["duration"]))
            if end is not None and end - start < 1.0 / frame_rate:
                raise ValueError(
                    f"clip {idx + 1} trim leaves no frames: start={start:.3f}s end={end:.3f}s"
                )
            entry["_effective_start"] = start
            entry["_effective_end"] = end

        output_dir = (
            folder_paths.get_output_directory()
            if save_output
            else folder_paths.get_temp_directory()
        )
        full_output_folder, filename, _, subfolder, _ = folder_paths.get_save_image_path(
            filename_prefix,
            output_dir,
        )
        existing = sorted(Path(full_output_folder).glob(f"{filename}_*.mp4"))
        counter = 1
        if existing:
            suffixes = []
            for item in existing:
                stem = item.stem
                parts = stem.split("_")
                if parts:
                    tail = parts[-1].replace("-audio", "")
                    if tail.isdigit():
                        suffixes.append(int(tail))
            if suffixes:
                counter = max(suffixes) + 1

        base_file = f"{filename}_{counter:05}.mp4"
        audio_file = f"{filename}_{counter:05}-audio.mp4"
        base_path = os.path.join(full_output_folder, base_file)
        audio_path = os.path.join(full_output_folder, audio_file)

        self._concat_trimmed_videos_ffmpeg_filtergraph(
            entries=entries,
            probes=probes,
            frame_rate=frame_rate,
            base_width=base_width,
            base_height=base_height,
            pix_fmt=pix_fmt,
            crf=crf,
            base_path=base_path,
            audio_path=audio_path,
        )

        preview = {
            "filename": audio_file,
            "subfolder": subfolder,
            "type": "output" if save_output else "temp",
            "format": "video/h264-mp4",
            "frame_rate": frame_rate,
            "fullpath": audio_path,
        }
        return {
            "ui": {"gifs": [preview]},
            "result": ((save_output, [base_path, audio_path]),),
        }

    def _concat_trimmed_videos_ffmpeg_filtergraph(
        self,
        *,
        entries,
        probes,
        frame_rate,
        base_width,
        base_height,
        pix_fmt,
        crf,
        base_path,
        audio_path,
    ):
        ffmpeg_inputs = []
        filter_parts = []
        concat_inputs = []
        for idx, (entry, probe) in enumerate(zip(entries, probes)):
            start = float(entry.get("_effective_start") or 0.0)
            end = entry.get("_effective_end")
            ffmpeg_inputs.extend(["-i", probe["path"]])

            trim_args = []
            if start > 0:
                trim_args.append(f"start={start:.6f}")
            if end is not None:
                trim_args.append(f"end={float(end):.6f}")
            video_filters = []
            if trim_args:
                video_filters.extend([f"trim={':'.join(trim_args)}", "setpts=PTS-STARTPTS"])
            video_filters.extend(
                [
                    f"fps={frame_rate}",
                    f"scale={base_width}:{base_height}:flags=lanczos:force_original_aspect_ratio=decrease",
                    f"pad={base_width}:{base_height}:(ow-iw)/2:(oh-ih)/2:black",
                    f"format={pix_fmt}",
                    "setsar=1",
                ]
            )
            filter_parts.append(f"[{idx}:v]{','.join(video_filters)}[v{idx}]")

            if probe["has_audio"]:
                audio_filters = []
                if trim_args:
                    audio_filters.extend([f"atrim={':'.join(trim_args)}", "asetpts=PTS-STARTPTS"])
                audio_filters.extend(
                    [
                        "aresample=48000",
                        "aformat=sample_fmts=fltp:channel_layouts=stereo",
                    ]
                )
                filter_parts.append(f"[{idx}:a]{','.join(audio_filters)}[a{idx}]")
            else:
                clip_end = float(end) if end is not None else float(probe["duration"] or 0.0)
                silent_duration = max(0.001, clip_end - start)
                filter_parts.append(
                    f"anullsrc=channel_layout=stereo:sample_rate=48000:d={silent_duration:.6f}[a{idx}]"
                )
            concat_inputs.extend([f"[v{idx}]", f"[a{idx}]"])

        filter_parts.append("".join(concat_inputs) + f"concat=n={len(probes)}:v=1:a=1[v][a]")
        cmd = [
            FFMPEG_BIN,
            "-y",
            "-v",
            "error",
            *ffmpeg_inputs,
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            str(crf),
            "-pix_fmt",
            pix_fmt,
            "-r",
            str(frame_rate),
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            audio_path,
        ]
        subprocess.run(cmd, check=True)

        subprocess.run(
            [FFMPEG_BIN, "-y", "-v", "error", "-i", audio_path, "-an", "-c:v", "copy", base_path],
            check=True,
        )


class FurgenExposureAdjust:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "brightness_multiplier": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 4.0, "step": 0.01},
                ),
                "contrast": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 4.0, "step": 0.01},
                ),
                "gamma": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.05, "max": 4.0, "step": 0.01},
                ),
                "saturation": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 4.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "adjust"
    CATEGORY = "Furgen/image"

    def adjust(self, images, brightness_multiplier, contrast, gamma, saturation):
        if (
            _is_neutral(brightness_multiplier, 1.0)
            and _is_neutral(contrast, 1.0)
            and _is_neutral(gamma, 1.0)
            and _is_neutral(saturation, 1.0)
        ):
            return (images,)

        rgb = _image_rgb(images)
        adjusted = rgb
        if not _is_neutral(saturation, 1.0) and rgb.shape[-1] > 1:
            luma = _luma(adjusted)
            adjusted = luma + (adjusted - luma) * float(saturation)
        if not _is_neutral(contrast, 1.0):
            adjusted = (adjusted - 0.5) * float(contrast) + 0.5
        if not _is_neutral(brightness_multiplier, 1.0):
            adjusted = adjusted * float(brightness_multiplier)
        if not _is_neutral(gamma, 1.0):
            adjusted = adjusted.clamp(0.0, 1.0).pow(1.0 / float(gamma))

        if images.shape[-1] == adjusted.shape[-1]:
            out = adjusted
        else:
            out = torch.cat((adjusted, images[..., adjusted.shape[-1] :]), dim=-1)
        return (out.clamp(0.0, 1.0),)


class FurgenGetImageRangeFromBatch:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "start_index": ("INT", {"default": 0, "min": -1000000, "max": 1000000, "step": 1}),
                "num_frames": ("INT", {"default": 1, "min": 1, "max": 1000000, "step": 1}),
            },
            "optional": {
                "masks": ("MASK",),
            },
        }

    RETURN_TYPES = ("IMAGE", "MASK")
    RETURN_NAMES = ("images", "masks")
    FUNCTION = "slice"
    CATEGORY = "Furgen/video"

    @staticmethod
    def _slice_tensor(batch, start_index, num_frames):
        if batch is None:
            return None
        total = int(batch.shape[0])
        count = max(1, int(num_frames or 1))
        start = int(start_index or 0)
        if start < 0:
            start = max(0, total - count)
        start = max(0, min(start, max(0, total - 1)))
        end = max(start + 1, min(total, start + count))
        return batch[start:end]

    def slice(self, images, start_index, num_frames, masks=None):
        sliced_images = self._slice_tensor(images, start_index, num_frames)
        if sliced_images is None:
            raise ValueError("images batch is required")
        sliced_masks = self._slice_tensor(masks, start_index, num_frames) if masks is not None else None
        return (sliced_images, sliced_masks)


class FurgenPrependImageToBatch:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "first_image": ("IMAGE",),
                "images": ("IMAGE",),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "prepend"
    CATEGORY = "Furgen/video"

    @staticmethod
    def _as_batch(image, name: str):
        if image is None or not hasattr(image, "shape"):
            raise ValueError(f"{name} image batch is required")
        if len(image.shape) == 3:
            image = image.unsqueeze(0)
        if len(image.shape) != 4:
            raise ValueError(f"{name} must be an IMAGE tensor")
        return image

    @staticmethod
    def _match_like(frame, images):
        target_h = int(images.shape[1])
        target_w = int(images.shape[2])
        target_c = int(images.shape[3])
        frame = frame.to(device=images.device, dtype=images.dtype)
        if int(frame.shape[1]) != target_h or int(frame.shape[2]) != target_w:
            nchw = frame.movedim(-1, 1)
            frame = F.interpolate(nchw, size=(target_h, target_w), mode="bilinear", align_corners=False).movedim(1, -1)
        if int(frame.shape[3]) > target_c:
            frame = frame[..., :target_c]
        elif int(frame.shape[3]) < target_c:
            pad = images[:1, ..., int(frame.shape[3]) : target_c]
            if int(pad.shape[3]) != target_c - int(frame.shape[3]):
                pad = torch.zeros(
                    (1, int(frame.shape[1]), int(frame.shape[2]), target_c - int(frame.shape[3])),
                    device=frame.device,
                    dtype=frame.dtype,
                )
            frame = torch.cat((frame, pad.to(device=frame.device, dtype=frame.dtype)), dim=-1)
        return frame.clamp(0.0, 1.0)

    def prepend(self, first_image, images):
        images = self._as_batch(images, "images")
        first = self._as_batch(first_image, "first_image")[:1]
        first = self._match_like(first, images)
        return (torch.cat((first, images), dim=0),)


class FurgenSeamScaleStabilize:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "reference_image": ("IMAGE",),
                "images": ("IMAGE",),
                "full_strength_frames": ("INT", {"default": 4, "min": 0, "max": 240, "step": 1}),
                "fade_out_frames": ("INT", {"default": 16, "min": 0, "max": 240, "step": 1}),
                "strength": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 1.0, "step": 0.01}),
                "max_scale_delta": ("FLOAT", {"default": 0.12, "min": 0.0, "max": 1.0, "step": 0.01}),
                "min_inliers": ("INT", {"default": 20, "min": 0, "max": 10000, "step": 1}),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "stabilize"
    CATEGORY = "Furgen/video"

    @staticmethod
    def _as_batch(image, name: str):
        if image is None or not hasattr(image, "shape"):
            raise ValueError(f"{name} image batch is required")
        if len(image.shape) == 3:
            image = image.unsqueeze(0)
        if len(image.shape) != 4:
            raise ValueError(f"{name} must be an IMAGE tensor")
        return image

    @staticmethod
    def _match_reference(reference, images):
        reference = reference[:1].to(device=images.device, dtype=images.dtype)
        if int(reference.shape[1]) != int(images.shape[1]) or int(reference.shape[2]) != int(images.shape[2]):
            reference = F.interpolate(
                reference.movedim(-1, 1),
                size=(int(images.shape[1]), int(images.shape[2])),
                mode="bilinear",
                align_corners=False,
            ).movedim(1, -1)
        if int(reference.shape[3]) > int(images.shape[3]):
            reference = reference[..., : int(images.shape[3])]
        elif int(reference.shape[3]) < int(images.shape[3]):
            pad = torch.zeros(
                (
                    1,
                    int(reference.shape[1]),
                    int(reference.shape[2]),
                    int(images.shape[3]) - int(reference.shape[3]),
                ),
                device=reference.device,
                dtype=reference.dtype,
            )
            reference = torch.cat((reference, pad), dim=-1)
        return reference.clamp(0.0, 1.0)

    @staticmethod
    def _to_u8(frame):
        return (frame.detach().float().cpu().clamp(0.0, 1.0).numpy() * 255.0 + 0.5).astype(np.uint8)

    @staticmethod
    def _to_gray(cv2, frame):
        if frame.ndim == 2 or int(frame.shape[-1]) == 1:
            return frame[..., 0] if frame.ndim == 3 else frame
        if int(frame.shape[-1]) >= 3:
            return cv2.cvtColor(frame[..., :3], cv2.COLOR_RGB2GRAY)
        return frame

    @staticmethod
    def _estimate_reference_to_current_affine(cv2, reference_u8, current_u8, min_inliers):
        ref_gray = FurgenSeamScaleStabilize._to_gray(cv2, reference_u8)
        cur_gray = FurgenSeamScaleStabilize._to_gray(cv2, current_u8)
        orb = cv2.ORB_create(nfeatures=1800, fastThreshold=5)
        ref_kp, ref_desc = orb.detectAndCompute(ref_gray, None)
        cur_kp, cur_desc = orb.detectAndCompute(cur_gray, None)
        if ref_desc is None or cur_desc is None or len(ref_kp) < 8 or len(cur_kp) < 8:
            return None, 0
        matcher = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
        matches = matcher.match(ref_desc, cur_desc)
        if len(matches) < max(8, int(min_inliers)):
            return None, 0
        matches = sorted(matches, key=lambda match: match.distance)[: min(len(matches), 300)]
        ref_pts = np.float32([ref_kp[match.queryIdx].pt for match in matches]).reshape(-1, 1, 2)
        cur_pts = np.float32([cur_kp[match.trainIdx].pt for match in matches]).reshape(-1, 1, 2)
        affine, inliers = cv2.estimateAffinePartial2D(
            ref_pts,
            cur_pts,
            method=cv2.RANSAC,
            ransacReprojThreshold=3.0,
            maxIters=2000,
            confidence=0.995,
        )
        inlier_count = int(inliers.sum()) if inliers is not None else 0
        if affine is None or inlier_count < int(min_inliers):
            return None, inlier_count
        return affine.astype(np.float32), inlier_count

    @staticmethod
    def _affine_scale(affine):
        sx = float(np.linalg.norm(affine[:, 0]))
        sy = float(np.linalg.norm(affine[:, 1]))
        return (sx + sy) * 0.5

    @staticmethod
    def _frame_strength(index, full_strength_frames, fade_out_frames, strength):
        if index == 0:
            return 0.0
        full = max(0, int(full_strength_frames))
        fade = max(0, int(fade_out_frames))
        if index <= full:
            return float(strength)
        if fade <= 0 or index > full + fade:
            return 0.0
        return float(strength) * (1.0 - ((float(index - full) - 0.5) / float(fade)))

    def stabilize(
        self,
        reference_image,
        images,
        full_strength_frames,
        fade_out_frames,
        strength,
        max_scale_delta,
        min_inliers,
    ):
        try:
            import cv2
        except Exception as exc:
            raise RuntimeError("FurgenSeamScaleStabilize requires cv2/opencv-python") from exc

        images = self._as_batch(images, "images")
        reference = self._match_reference(self._as_batch(reference_image, "reference_image"), images)
        total = int(images.shape[0])
        limit = min(total, max(1, int(full_strength_frames) + int(fade_out_frames) + 1))
        if total <= 1 or float(strength) <= 0.0 or limit <= 1:
            return (images,)

        reference_u8 = self._to_u8(reference[0])
        output = images.clone()
        identity = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], dtype=np.float32)
        h = int(images.shape[1])
        w = int(images.shape[2])
        for index in range(1, limit):
            frame_strength = self._frame_strength(index, full_strength_frames, fade_out_frames, strength)
            if frame_strength <= 0.0:
                continue
            current_u8 = self._to_u8(images[index])
            affine, _inliers = self._estimate_reference_to_current_affine(cv2, reference_u8, current_u8, min_inliers)
            if affine is None:
                continue
            scale = self._affine_scale(affine)
            if not np.isfinite(scale) or abs(scale - 1.0) > float(max_scale_delta):
                continue
            inverse = cv2.invertAffineTransform(affine).astype(np.float32)
            correction = identity + (inverse - identity) * float(frame_strength)
            corrected = cv2.warpAffine(
                current_u8,
                correction,
                (w, h),
                flags=cv2.INTER_LANCZOS4,
                borderMode=cv2.BORDER_REPLICATE,
            )
            if corrected.ndim == 2:
                corrected = corrected[..., None]
            corrected_tensor = torch.from_numpy(corrected.astype(np.float32) / 255.0).to(
                device=images.device,
                dtype=images.dtype,
            )
            if int(corrected_tensor.shape[-1]) == int(images.shape[-1]):
                output[index] = corrected_tensor
            else:
                merged = output[index].clone()
                channels = min(int(merged.shape[-1]), int(corrected_tensor.shape[-1]))
                merged[..., :channels] = corrected_tensor[..., :channels]
                output[index] = merged
        return (output.clamp(0.0, 1.0),)


class FurgenTrimAudioDuration:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "audio": ("AUDIO",),
                "start_index": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 86400.0, "step": 0.001}),
                "duration": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 86400.0, "step": 0.001}),
            }
        }

    RETURN_TYPES = ("AUDIO",)
    RETURN_NAMES = ("audio",)
    FUNCTION = "trim"
    CATEGORY = "Furgen/audio"

    def trim(self, audio, start_index, duration):
        if not isinstance(audio, dict):
            return (audio,)
        waveform = audio.get("waveform")
        sample_rate = int(audio.get("sample_rate") or 0)
        if waveform is None or sample_rate <= 0 or not hasattr(waveform, "shape"):
            return (audio,)

        start_sample = max(0, int(round(float(start_index or 0.0) * sample_rate)))
        duration_seconds = float(duration or 0.0)
        end_sample = None
        if duration_seconds > 0:
            end_sample = start_sample + max(1, int(round(duration_seconds * sample_rate)))

        try:
            total_samples = int(waveform.shape[-1])
            start_sample = min(start_sample, total_samples)
            if end_sample is None:
                trimmed_waveform = waveform[..., start_sample:]
            else:
                trimmed_waveform = waveform[..., start_sample:min(end_sample, total_samples)]
        except Exception:
            return (audio,)

        next_audio = dict(audio)
        next_audio["waveform"] = trimmed_waveform
        return (next_audio,)


class FurgenReferenceColorMatch:
    MODES = ("luma_mean_std", "rgb_mean_std", "rgb_mean_only")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "reference": ("IMAGE",),
                "mode": (list(cls.MODES), {"default": "luma_mean_std"}),
                "strength": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "match"
    CATEGORY = "Furgen/image"

    def match(self, images, reference, mode, strength):
        if _is_neutral(strength, 0.0):
            return (images,)

        rgb = _image_rgb(images)
        ref_rgb = _image_rgb(reference).to(device=images.device, dtype=images.dtype)
        if ref_rgb.shape[0] == 1 and rgb.shape[0] != 1:
            ref_rgb = ref_rgb.expand(rgb.shape[0], -1, -1, -1)
        elif ref_rgb.shape[0] != rgb.shape[0]:
            ref_rgb = ref_rgb[:1].expand(rgb.shape[0], -1, -1, -1)

        eps = torch.finfo(rgb.dtype).eps if rgb.dtype.is_floating_point else 1e-6
        if mode == "rgb_mean_std":
            src_mean = rgb.mean(dim=(1, 2), keepdim=True)
            ref_mean = ref_rgb.mean(dim=(1, 2), keepdim=True)
            src_std = rgb.std(dim=(1, 2), keepdim=True, unbiased=False).clamp_min(eps)
            ref_std = ref_rgb.std(dim=(1, 2), keepdim=True, unbiased=False)
            corrected = (rgb - src_mean) / src_std * ref_std + ref_mean
        elif mode == "rgb_mean_only":
            src_mean = rgb.mean(dim=(1, 2), keepdim=True)
            ref_mean = ref_rgb.mean(dim=(1, 2), keepdim=True)
            corrected = rgb + (ref_mean - src_mean)
        elif mode == "luma_mean_std":
            src_luma = _luma(rgb)
            ref_luma = _luma(ref_rgb)
            src_mean = src_luma.mean(dim=(1, 2), keepdim=True)
            ref_mean = ref_luma.mean(dim=(1, 2), keepdim=True)
            src_std = src_luma.std(dim=(1, 2), keepdim=True, unbiased=False).clamp_min(eps)
            ref_std = ref_luma.std(dim=(1, 2), keepdim=True, unbiased=False)
            corrected_luma = (src_luma - src_mean) / src_std * ref_std + ref_mean
            corrected = rgb + (corrected_luma - src_luma)
        else:
            raise ValueError(f"unsupported color match mode: {mode}")

        return (_blend_and_restore_channels(images, corrected, strength),)


class FurgenAdaptiveExposureMatch:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "reference": ("IMAGE",),
                "strength": (
                    "FLOAT",
                    {"default": 0.60, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "gain_min": (
                    "FLOAT",
                    {"default": 0.85, "min": 0.10, "max": 4.0, "step": 0.01},
                ),
                "gain_max": (
                    "FLOAT",
                    {"default": 1.18, "min": 0.10, "max": 4.0, "step": 0.01},
                ),
                "black_percentile": (
                    "FLOAT",
                    {"default": 0.02, "min": 0.0, "max": 0.49, "step": 0.01},
                ),
                "white_percentile": (
                    "FLOAT",
                    {"default": 0.98, "min": 0.51, "max": 1.0, "step": 0.01},
                ),
                "preserve_highlights": (
                    "FLOAT",
                    {"default": 0.75, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "match"
    CATEGORY = "Furgen/image"

    def match(
        self,
        images,
        reference,
        strength,
        gain_min,
        gain_max,
        black_percentile,
        white_percentile,
        preserve_highlights,
    ):
        if _is_neutral(strength, 0.0):
            return (images,)

        phase = "validate"
        try:
            with torch.no_grad():
                _image_rgb(images)
                phase = "reference_stats"
                ref_rgb = _first_reference_rgb(reference, images)
                ref_mean = _robust_luma_mean_single(
                    _luma(ref_rgb),
                    black_percentile,
                    white_percentile,
                )
                lo = min(float(gain_min), float(gain_max))
                hi = max(float(gain_min), float(gain_max))
                output = torch.empty_like(images)

                for start, end in _chunked_frame_ranges(images):
                    phase = "frame_chunk"
                    chunk = images[start:end]
                    rgb = _image_rgb(chunk)
                    src_mean = _robust_luma_mean_frames(
                        _luma(rgb),
                        black_percentile,
                        white_percentile,
                    ).clamp_min(_eps_for(rgb))
                    gain = (ref_mean / src_mean).clamp(lo, hi)
                    corrected = _apply_highlight_protection(rgb, rgb * gain, preserve_highlights)
                    output[start:end] = _blend_and_restore_channels(chunk, corrected, strength)

                return (output,)
        except Exception as exc:
            raise _node_runtime_error(self.__class__.__name__, images, phase, exc) from exc


class FurgenColorTransferMatch:
    MODES = ("rgb_mean_std", "ycbcr_mean_std")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "reference": ("IMAGE",),
                "mode": (list(cls.MODES), {"default": "ycbcr_mean_std"}),
                "strength": (
                    "FLOAT",
                    {"default": 0.35, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "luma_strength": (
                    "FLOAT",
                    {"default": 0.25, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "chroma_strength": (
                    "FLOAT",
                    {"default": 0.35, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "std_strength": (
                    "FLOAT",
                    {"default": 0.30, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "std_min": (
                    "FLOAT",
                    {"default": 0.50, "min": 0.05, "max": 4.0, "step": 0.01},
                ),
                "std_max": (
                    "FLOAT",
                    {"default": 1.50, "min": 0.05, "max": 4.0, "step": 0.01},
                ),
                "preserve_highlights": (
                    "FLOAT",
                    {"default": 0.75, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "match"
    CATEGORY = "Furgen/image"

    def match(
        self,
        images,
        reference,
        mode,
        strength,
        luma_strength,
        chroma_strength,
        std_strength,
        std_min,
        std_max,
        preserve_highlights,
    ):
        if _is_neutral(strength, 0.0) or (
            _is_neutral(luma_strength, 0.0)
            and _is_neutral(chroma_strength, 0.0)
            and _is_neutral(std_strength, 0.0)
        ):
            return (images,)

        phase = "validate"
        try:
            with torch.no_grad():
                rgb_channels = _image_rgb(images).shape[-1]
                ref_rgb = _first_reference_rgb(reference, images)
                lo = min(float(std_min), float(std_max))
                hi = max(float(std_min), float(std_max))
                output = torch.empty_like(images)

                if mode == "rgb_mean_std":
                    phase = "reference_stats_rgb"
                    ref_mean, ref_std = _mean_std_stats_single(ref_rgb)
                    mean_strength = max(0.0, min(1.0, max(float(luma_strength), float(chroma_strength))))
                    mean_strengths = torch.full(
                        (1, 1, 1, rgb_channels),
                        mean_strength,
                        dtype=images.dtype,
                        device=images.device,
                    )
                    for start, end in _chunked_frame_ranges(images):
                        phase = "frame_chunk_rgb"
                        chunk = images[start:end]
                        rgb = _image_rgb(chunk)
                        corrected = _mean_std_transfer_with_stats(
                            rgb,
                            ref_mean,
                            ref_std,
                            mean_strengths,
                            std_strength,
                            lo,
                            hi,
                        )
                        corrected = _apply_highlight_protection(rgb, corrected, preserve_highlights)
                        output[start:end] = _blend_and_restore_channels(chunk, corrected, strength)
                elif mode == "ycbcr_mean_std":
                    phase = "reference_stats_ycbcr"
                    ref_ycbcr = _rgb_to_ycbcr(ref_rgb)
                    ref_mean, ref_std = _mean_std_stats_single(ref_ycbcr)
                    mean_strengths = torch.tensor(
                        [float(luma_strength), float(chroma_strength), float(chroma_strength)],
                        dtype=images.dtype,
                        device=images.device,
                    ).view(1, 1, 1, 3).clamp(0.0, 1.0)
                    for start, end in _chunked_frame_ranges(images):
                        phase = "frame_chunk_ycbcr"
                        chunk = images[start:end]
                        rgb = _image_rgb(chunk)
                        ycbcr = _rgb_to_ycbcr(rgb)
                        corrected = _ycbcr_to_rgb(
                            _mean_std_transfer_with_stats(
                                ycbcr,
                                ref_mean,
                                ref_std,
                                mean_strengths,
                                std_strength,
                                lo,
                                hi,
                            )
                        )
                        corrected = _apply_highlight_protection(rgb, corrected, preserve_highlights)
                        output[start:end] = _blend_and_restore_channels(chunk, corrected, strength)
                else:
                    raise ValueError(f"unsupported color transfer mode: {mode}")

                return (output,)
        except Exception as exc:
            raise _node_runtime_error(self.__class__.__name__, images, phase, exc) from exc


class FurgenTemporalToneSmooth:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "strength": (
                    "FLOAT",
                    {"default": 0.50, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "luma_smoothing": (
                    "FLOAT",
                    {"default": 0.65, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "chroma_smoothing": (
                    "FLOAT",
                    {"default": 0.35, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "max_frame_gain_delta": (
                    "FLOAT",
                    {"default": 0.035, "min": 0.0, "max": 0.50, "step": 0.001},
                ),
                "preserve_first_frame": (
                    "BOOLEAN",
                    {"default": True},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "smooth"
    CATEGORY = "Furgen/image"

    def smooth(
        self,
        images,
        strength,
        luma_smoothing,
        chroma_smoothing,
        max_frame_gain_delta,
        preserve_first_frame,
    ):
        if _is_neutral(strength, 0.0) or images.shape[0] <= 1:
            return (images,)

        phase = "validate"
        try:
            with torch.no_grad():
                _image_rgb(images)
                luma_keep = max(0.0, min(1.0, float(luma_smoothing)))
                chroma_keep = max(0.0, min(1.0, float(chroma_smoothing)))
                max_delta = max(0.0, float(max_frame_gain_delta))
                global_strength = max(0.0, min(1.0, float(strength)))

                smooth_y = None
                smooth_chroma = None
                previous_gain = None
                previous_chroma_offset = None
                output = torch.empty_like(images)

                for index in range(images.shape[0]):
                    phase = "frame"
                    image_frame = images[index : index + 1]
                    rgb = _image_rgb(image_frame)
                    ycbcr = _rgb_to_ycbcr(rgb)
                    current_mean = ycbcr.mean(dim=(1, 2), keepdim=True)
                    current_y = current_mean[..., 0:1]
                    current_chroma = current_mean[..., 1:3]

                    if smooth_y is None:
                        smooth_y = current_y
                        smooth_chroma = current_chroma
                        previous_gain = torch.ones_like(smooth_y)
                        previous_chroma_offset = torch.zeros_like(smooth_chroma)
                    else:
                        smooth_y = smooth_y * luma_keep + current_y * (1.0 - luma_keep)
                        smooth_chroma = smooth_chroma * chroma_keep + current_chroma * (1.0 - chroma_keep)

                    raw_gain = (smooth_y / current_y.clamp_min(_eps_for(rgb))).clamp(0.25, 4.0)
                    gain_delta = (raw_gain - previous_gain).clamp(-max_delta, max_delta)
                    limited_gain = previous_gain + gain_delta
                    raw_chroma_offset = smooth_chroma - current_chroma
                    chroma_delta = (raw_chroma_offset - previous_chroma_offset).clamp(-max_delta, max_delta)
                    limited_chroma_offset = previous_chroma_offset + chroma_delta

                    if index == 0 and bool(preserve_first_frame):
                        output[index : index + 1] = image_frame
                    else:
                        adjusted = ycbcr.clone()
                        adjusted[..., 0:1] = ycbcr[..., 0:1] * (1.0 + (limited_gain - 1.0) * global_strength)
                        adjusted[..., 1:3] = ycbcr[..., 1:3] + limited_chroma_offset * global_strength
                        output[index : index + 1] = _restore_channels(image_frame, _ycbcr_to_rgb(adjusted))
                    previous_gain = limited_gain
                    previous_chroma_offset = limited_chroma_offset

                return (output,)
        except Exception as exc:
            raise _node_runtime_error(self.__class__.__name__, images, phase, exc) from exc


class FurgenTemporalUnsharpMask:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "amount": (
                    "FLOAT",
                    {"default": 0.0, "min": 0.0, "max": 2.0, "step": 0.01},
                ),
                "radius": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.25, "max": 5.0, "step": 0.25},
                ),
                "threshold": (
                    "FLOAT",
                    {"default": 0.01, "min": 0.0, "max": 0.50, "step": 0.005},
                ),
                "luma_only": (
                    "BOOLEAN",
                    {"default": True},
                ),
                "temporal_blend": (
                    "FLOAT",
                    {"default": 0.35, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "sharpen"
    CATEGORY = "Furgen/image"

    def sharpen(self, images, amount, radius, threshold, luma_only, temporal_blend):
        if _is_neutral(amount, 0.0):
            return (images,)

        phase = "validate"
        try:
            with torch.no_grad():
                rgb = _image_rgb(images)
                correction = torch.empty_like(rgb)
                amount_f = max(0.0, float(amount))
                radius_f = max(0.25, float(radius))
                threshold_f = max(0.0, float(threshold))

                for start, end in _chunked_frame_ranges(images):
                    phase = "frame_chunk"
                    chunk_rgb = rgb[start:end]
                    blurred = _gaussian_blur_channel_last(chunk_rgb, radius_f)
                    detail = chunk_rgb - blurred
                    if bool(luma_only):
                        detail = _threshold_detail(_luma(detail), threshold_f).expand_as(chunk_rgb)
                    else:
                        detail = _threshold_detail(detail, threshold_f)
                    correction[start:end] = detail * amount_f

                blend = max(0.0, min(1.0, float(temporal_blend)))
                if blend > 0.0 and correction.shape[0] > 1:
                    phase = "temporal_blend"
                    smooth = correction.clone()
                    smooth[0:1] = (correction[0:1] + correction[1:2]) * 0.5
                    smooth[-1:] = (correction[-2:-1] + correction[-1:]) * 0.5
                    if correction.shape[0] > 2:
                        smooth[1:-1] = (correction[:-2] + correction[1:-1] + correction[2:]) / 3.0
                    correction = correction.lerp(smooth, blend)

                return (_restore_channels(images, rgb + correction),)
        except Exception as exc:
            raise _node_runtime_error(self.__class__.__name__, images, phase, exc) from exc


class FurgenLatentGuideTemporalMask:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "latent": ("LATENT",),
                "mode": (["hard_cut", "linear_fade", "cosine_fade"],),
                "active_latent_frames": (
                    "INT",
                    {"default": 1, "min": 0, "max": 128, "step": 1},
                ),
                "fade_latent_frames": (
                    "INT",
                    {"default": 0, "min": 0, "max": 128, "step": 1},
                ),
                "start_strength": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "end_strength": (
                    "FLOAT",
                    {"default": 0.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("LATENT",)
    RETURN_NAMES = ("latent",)
    FUNCTION = "apply"
    CATEGORY = "Furgen/latent"

    def apply(self, latent, mode, active_latent_frames, fade_latent_frames, start_strength, end_strength):
        phase = "validate"
        try:
            if not isinstance(latent, dict):
                raise ValueError("latent must be a LATENT dict")
            samples = latent.get("samples")
            if not isinstance(samples, torch.Tensor):
                raise ValueError("latent.samples must be a tensor")
            if samples.ndim != 5:
                raise ValueError(f"expected latent.samples shape [B,C,T,H,W], got {tuple(samples.shape)}")

            with torch.no_grad():
                phase = "schedule"
                batch, _channels, frames, height, width = samples.shape
                active = max(0, int(active_latent_frames))
                fade = max(0, int(fade_latent_frames))
                start = max(0.0, min(1.0, float(start_strength)))
                end = max(0.0, min(1.0, float(end_strength)))
                mode = str(mode or "hard_cut")

                strengths = torch.full(
                    (int(frames),),
                    end,
                    dtype=samples.dtype,
                    device=samples.device,
                )
                if active > 0:
                    strengths[: min(active, int(frames))] = start
                if fade > 0 and active < int(frames):
                    fade_count = min(fade, int(frames) - active)
                    positions = torch.arange(1, fade_count + 1, dtype=samples.dtype, device=samples.device) / float(fade)
                    if mode == "cosine_fade":
                        positions = (1.0 - torch.cos(positions * torch.pi)) * 0.5
                    elif mode != "linear_fade":
                        positions = torch.ones_like(positions)
                    strengths[active : active + fade_count] = start + (end - start) * positions

                phase = "mask"
                mask_values = (1.0 - strengths).clamp(0.0, 1.0).view(1, 1, int(frames), 1, 1)
                noise_mask = mask_values.expand(int(batch), 1, int(frames), int(height), int(width)).contiguous()
                out = dict(latent)
                out["noise_mask"] = noise_mask
                return (out,)
        except Exception as exc:
            shape = None
            try:
                shape = tuple(latent.get("samples").shape) if isinstance(latent, dict) else None
            except Exception:
                shape = None
            raise RuntimeError(f"FurgenLatentGuideTemporalMask failed during {phase}; latent_shape={shape}: {exc}") from exc


def _temporal_strengths(frames, mode, active_latent_frames, fade_latent_frames, start_strength, end_strength, *, dtype, device):
    frame_count = max(0, int(frames))
    active = max(0, int(active_latent_frames))
    fade = max(0, int(fade_latent_frames))
    start = max(0.0, min(1.0, float(start_strength)))
    end = max(0.0, min(1.0, float(end_strength)))
    mode = str(mode or "hard_cut")

    strengths = torch.full((frame_count,), end, dtype=dtype, device=device)
    if frame_count == 0:
        return strengths
    if active > 0:
        strengths[: min(active, frame_count)] = start
    if fade > 0 and active < frame_count:
        fade_count = min(fade, frame_count - active)
        positions = torch.arange(1, fade_count + 1, dtype=dtype, device=device) / float(fade)
        if mode == "cosine_fade":
            positions = (1.0 - torch.cos(positions * torch.pi)) * 0.5
        elif mode != "linear_fade":
            positions = torch.ones_like(positions)
        strengths[active : active + fade_count] = start + (end - start) * positions
    return strengths


def _temporal_noise_mask(samples, base_mask, mode, active_latent_frames, fade_latent_frames, start_strength, end_strength):
    if samples.ndim != 5:
        raise ValueError(f"expected latent samples shape [B,C,T,H,W], got {tuple(samples.shape)}")
    batch, _channels, frames, _height, _width = samples.shape
    strengths = _temporal_strengths(
        int(frames),
        mode,
        active_latent_frames,
        fade_latent_frames,
        start_strength,
        end_strength,
        dtype=samples.dtype,
        device=samples.device,
    ).view(1, 1, int(frames), 1, 1)
    if base_mask is None:
        base = torch.ones((int(batch), 1, int(frames), 1, 1), dtype=samples.dtype, device=samples.device)
    else:
        base = base_mask.to(device=samples.device, dtype=samples.dtype)
        if base.ndim != 5:
            raise ValueError(f"expected guide noise_mask shape [B,1,T,H,W], got {tuple(base.shape)}")
        if int(base.shape[2]) != int(frames):
            raise ValueError(f"guide noise_mask temporal length {base.shape[2]} != guide latent length {frames}")
    return (base - strengths).contiguous()


def _dilate_latent_for_ltxv(latent, horizontal_scale, vertical_scale):
    horizontal_scale = max(1, int(horizontal_scale))
    vertical_scale = max(1, int(vertical_scale))
    if horizontal_scale == 1 and vertical_scale == 1:
        return latent

    samples = latent["samples"]
    mask = latent.get("noise_mask", None)
    dilated_shape = samples.shape[:3] + (
        samples.shape[3] * vertical_scale,
        samples.shape[4] * horizontal_scale,
    )
    dilated_samples = torch.zeros(
        dilated_shape,
        device=samples.device,
        dtype=samples.dtype,
        requires_grad=False,
    )
    dilated_samples[..., ::vertical_scale, ::horizontal_scale] = samples

    dilated_mask = torch.full(
        (dilated_samples.shape[0], 1, dilated_samples.shape[2], dilated_samples.shape[3], dilated_samples.shape[4]),
        -1.0,
        device=samples.device,
        dtype=samples.dtype,
        requires_grad=False,
    )
    if mask is None:
        dilated_mask[..., ::vertical_scale, ::horizontal_scale] = 1.0
    else:
        dilated_mask[..., ::vertical_scale, ::horizontal_scale] = mask.to(device=samples.device, dtype=samples.dtype)
    return {"samples": dilated_samples, "noise_mask": dilated_mask}


def _append_ltxv_guide_attention_entry(conditioning, pre_filter_count, latent_shape):
    import node_helpers

    existing = []
    for item in conditioning:
        if isinstance(item, (list, tuple)) and len(item) > 1 and isinstance(item[1], dict):
            entries = item[1].get("guide_attention_entries")
            if entries is not None:
                existing = list(entries)
                break
    next_entries = [
        *existing,
        {
            "pre_filter_count": int(pre_filter_count),
            "strength": 1.0,
            "pixel_mask": None,
            "latent_shape": list(latent_shape),
        },
    ]
    return node_helpers.conditioning_set_values(conditioning, {"guide_attention_entries": next_entries})


class FurgenLTXVAddLatentGuideTemporal:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "vae": ("VAE",),
                "positive": ("CONDITIONING",),
                "negative": ("CONDITIONING",),
                "latent": ("LATENT",),
                "guiding_latent": ("LATENT",),
                "latent_idx": (
                    "INT",
                    {"default": 0, "min": -9999, "max": 9999, "step": 1},
                ),
                "mode": (["hard_cut", "linear_fade", "cosine_fade"],),
                "active_latent_frames": (
                    "INT",
                    {"default": 1, "min": 0, "max": 128, "step": 1},
                ),
                "fade_latent_frames": (
                    "INT",
                    {"default": 0, "min": 0, "max": 128, "step": 1},
                ),
                "start_strength": (
                    "FLOAT",
                    {"default": 1.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "end_strength": (
                    "FLOAT",
                    {"default": 0.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
            }
        }

    RETURN_TYPES = ("CONDITIONING", "CONDITIONING", "LATENT")
    RETURN_NAMES = ("positive", "negative", "latent")
    FUNCTION = "generate"
    CATEGORY = "Furgen/latent"

    def generate(
        self,
        vae,
        positive,
        negative,
        latent,
        guiding_latent,
        latent_idx,
        mode,
        active_latent_frames,
        fade_latent_frames,
        start_strength,
        end_strength,
    ):
        phase = "validate"
        try:
            import comfy_extras.nodes_lt as nodes_lt

            if not isinstance(latent, dict) or not isinstance(guiding_latent, dict):
                raise ValueError("latent and guiding_latent must be LATENT dicts")
            latent_samples = latent.get("samples")
            guide_samples = guiding_latent.get("samples")
            if not isinstance(latent_samples, torch.Tensor) or not isinstance(guide_samples, torch.Tensor):
                raise ValueError("latent.samples and guiding_latent.samples must be tensors")
            if latent_samples.ndim != 5 or guide_samples.ndim != 5:
                raise ValueError(
                    f"expected 5D latents, got latent={tuple(latent_samples.shape)} guide={tuple(guide_samples.shape)}"
                )
            if latent_samples.shape[4] % guide_samples.shape[4] != 0 or latent_samples.shape[3] % guide_samples.shape[3] != 0:
                raise ValueError("latent and guiding_latent spatial sizes must have an integer ratio")

            phase = "dilate"
            guide_orig_shape = list(guide_samples.shape[2:])
            dilated_guide = _dilate_latent_for_ltxv(
                guiding_latent,
                horizontal_scale=latent_samples.shape[4] // guide_samples.shape[4],
                vertical_scale=latent_samples.shape[3] // guide_samples.shape[3],
            )
            guide = dilated_guide["samples"]
            temporal_guide_mask = _temporal_noise_mask(
                guide,
                dilated_guide.get("noise_mask"),
                mode,
                active_latent_frames,
                fade_latent_frames,
                start_strength,
                end_strength,
            )

            phase = "append"
            scale_factors = vae.downscale_index_formula
            if int(latent_idx) <= 0:
                frame_idx = int(latent_idx) * scale_factors[0]
            else:
                frame_idx = 1 + (int(latent_idx) - 1) * scale_factors[0]
            noise_mask = nodes_lt.get_noise_mask(latent)
            positive, negative, latent_samples, noise_mask = nodes_lt.LTXVAddGuide.append_keyframe(
                positive=positive,
                negative=negative,
                frame_idx=frame_idx,
                latent_image=latent_samples,
                noise_mask=noise_mask,
                guiding_latent=guide,
                strength=0.0,
                scale_factors=scale_factors,
                guide_mask=temporal_guide_mask,
            )

            phase = "attention_entry"
            pre_filter_count = guide.shape[2] * guide.shape[3] * guide.shape[4]
            positive = _append_ltxv_guide_attention_entry(positive, pre_filter_count, guide_orig_shape)
            negative = _append_ltxv_guide_attention_entry(negative, pre_filter_count, guide_orig_shape)
            return (positive, negative, {"samples": latent_samples, "noise_mask": noise_mask})
        except Exception as exc:
            raise RuntimeError(f"FurgenLTXVAddLatentGuideTemporal failed during {phase}: {exc}") from exc


class FurgenLTXGuideAttentionAdjust:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "positive": ("CONDITIONING",),
                "negative": ("CONDITIONING",),
                "mode": (["set_last", "scale_last", "drop_last", "set_all", "scale_all"],),
                "strength": (
                    "FLOAT",
                    {"default": 0.0, "min": 0.0, "max": 1.0, "step": 0.01},
                ),
                "entry_count": (
                    "INT",
                    {"default": 1, "min": 1, "max": 16, "step": 1},
                ),
            }
        }

    RETURN_TYPES = ("CONDITIONING", "CONDITIONING")
    RETURN_NAMES = ("positive", "negative")
    FUNCTION = "adjust"
    CATEGORY = "Furgen/conditioning"

    @staticmethod
    def _copy_conditioning(conditioning, mode, strength, entry_count):
        mode = str(mode or "set_last")
        strength = max(0.0, min(1.0, float(strength)))
        entry_count = max(1, int(entry_count))
        out = []
        for item in conditioning:
            if not isinstance(item, (list, tuple)) or len(item) < 2 or not isinstance(item[1], dict):
                out.append(item)
                continue
            meta = dict(item[1])
            entries = meta.get("guide_attention_entries")
            if isinstance(entries, list) and entries:
                copied = []
                for entry in entries:
                    copied.append(dict(entry) if isinstance(entry, dict) else entry)
                if mode == "drop_last":
                    copied = copied[: max(0, len(copied) - entry_count)]
                else:
                    start = 0 if mode.endswith("_all") else max(0, len(copied) - entry_count)
                    for idx in range(start, len(copied)):
                        entry = copied[idx]
                        if not isinstance(entry, dict):
                            continue
                        current = float(entry.get("strength", 1.0))
                        entry["strength"] = current * strength if mode.startswith("scale_") else strength
                meta["guide_attention_entries"] = copied
            out.append([item[0], meta])
        return out

    def adjust(self, positive, negative, mode, strength, entry_count):
        return (
            self._copy_conditioning(positive, mode, strength, entry_count),
            self._copy_conditioning(negative, mode, strength, entry_count),
        )


def _finite_summary(tensor: torch.Tensor) -> str:
    finite = torch.isfinite(tensor)
    bad_count = int((~finite).sum().item())
    total = int(tensor.numel())
    summary = f"shape={tuple(tensor.shape)} dtype={tensor.dtype} device={tensor.device} bad={bad_count}/{total}"
    if finite.any():
        values = tensor[finite]
        summary += f" finite_min={float(values.min().item()):.6g} finite_max={float(values.max().item()):.6g}"
    return summary


def _nested_tensor_leaves(value):
    if isinstance(value, torch.Tensor):
        return [("", value)]
    tensors = getattr(value, "tensors", None)
    if not isinstance(tensors, (list, tuple)):
        return []
    leaves = []
    for index, tensor in enumerate(tensors):
        for suffix, leaf in _nested_tensor_leaves(tensor):
            leaves.append((f"[{index}]{suffix}", leaf))
    return leaves


class FurgenAssertFiniteImages:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "label": ("STRING", {"default": "images"}),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("images",)
    FUNCTION = "check"
    CATEGORY = "Furgen/debug"

    def check(self, images, label):
        if not isinstance(images, torch.Tensor):
            raise ValueError(f"FurgenAssertFiniteImages {label}: images must be a tensor")
        if not torch.isfinite(images).all():
            raise ValueError(f"FurgenAssertFiniteImages {label}: non-finite IMAGE tensor {_finite_summary(images)}")
        return (images,)


class FurgenAssertFiniteLatent:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "latent": ("LATENT",),
                "label": ("STRING", {"default": "latent"}),
                "check_noise_mask": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("LATENT",)
    RETURN_NAMES = ("latent",)
    FUNCTION = "check"
    CATEGORY = "Furgen/debug"

    def check(self, latent, label, check_noise_mask):
        if not isinstance(latent, dict):
            raise ValueError(f"FurgenAssertFiniteLatent {label}: latent must be a LATENT dict")
        samples = latent.get("samples")
        sample_leaves = _nested_tensor_leaves(samples)
        if not sample_leaves:
            raise ValueError(f"FurgenAssertFiniteLatent {label}: latent.samples must contain tensors")
        for suffix, sample_tensor in sample_leaves:
            if not torch.isfinite(sample_tensor).all():
                raise ValueError(
                    f"FurgenAssertFiniteLatent {label}: non-finite latent.samples{suffix} "
                    f"{_finite_summary(sample_tensor)}"
                )
        mask = latent.get("noise_mask")
        if check_noise_mask:
            for suffix, mask_tensor in _nested_tensor_leaves(mask):
                if not torch.isfinite(mask_tensor).all():
                    raise ValueError(
                        f"FurgenAssertFiniteLatent {label}: non-finite latent.noise_mask{suffix} "
                        f"{_finite_summary(mask_tensor)}"
                    )
        return (latent,)


def _atempo_chain(speed: float) -> list[str]:
    """atempo filters realising `speed`.

    A single atempo is only well-conditioned within [0.5, 2.0] on older
    ffmpeg builds, so decompose anything outside that into a product of
    in-range factors (3x -> 1.5 * 2.0).
    """
    filters: list[str] = []
    remaining = float(speed)
    if abs(remaining - 1.0) <= 1e-6:
        return filters
    while remaining > 2.0 + 1e-9:
        filters.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5 - 1e-9:
        filters.append("atempo=0.5")
        remaining /= 0.5
    if abs(remaining - 1.0) > 1e-6:
        filters.append(f"atempo={remaining:.6f}")
    return filters


def _color_filters(brightness: float, contrast: float, saturation: float) -> list[str]:
    """Video filters matching the client's CSS preview.

    The Studio previews grades with `filter: brightness(b) contrast(c)
    saturate(s)`, applied in that order. CSS brightness is a MULTIPLIER,
    while ffmpeg's eq=brightness is an additive offset — so multiply the
    channels with colorchannelmixer instead, and let eq handle contrast and
    saturation, which are multiplicative in both.
    """
    filters: list[str] = []
    if abs(brightness - 1.0) > 1e-4:
        b = max(0.0, float(brightness))
        filters.append(f"colorchannelmixer=rr={b:.6f}:gg={b:.6f}:bb={b:.6f}")
    eq_parts = []
    if abs(contrast - 1.0) > 1e-4:
        eq_parts.append(f"contrast={float(contrast):.6f}")
    if abs(saturation - 1.0) > 1e-4:
        eq_parts.append(f"saturation={float(saturation):.6f}")
    if eq_parts:
        filters.append("eq=" + ":".join(eq_parts))
    return filters


class FCSConcatVideosV3(FCSConcatVideosV2):
    """Concat with per-entry trims, speed, colour grading and crossfades.

    Everything runs in one ffmpeg filtergraph, so each clip's audio is
    trimmed, tempo-shifted and crossfaded alongside its picture instead of
    being dropped. Entry lines extend V2's syntax:

        url|start=S|end=E|speed=X|brightness=B|contrast=C|saturation=T|xfade=D

    `xfade=D` crossfades D seconds into the NEXT entry; omit it (or 0) for a
    straight concat. Speed scales BOTH streams (setpts + atempo), so audio
    stays in sync rather than drifting.
    """

    def concat_videos(
        self,
        video_entries,
        frame_rate,
        overlap_frames,
        filename_prefix,
        pix_fmt,
        crf,
        save_output,
        seam_repair_mode=FCS_SEAM_REPAIR_NONE,
        seam_repair_source_weights="0.35,0.15",
    ):
        entries = _parse_video_entries_with_options(video_entries)
        probes = [_probe_video(entry["path"]) for entry in entries]
        base_width = probes[0]["width"] or 1920
        base_height = probes[0]["height"] or 1088
        overlap_frames = max(0, int(overlap_frames or 0))
        frame_rate = float(frame_rate or 60.0)
        overlap_seconds = float(overlap_frames) / frame_rate if overlap_frames > 0 else 0.0

        for idx, (entry, probe) in enumerate(zip(entries, probes)):
            start = float(entry["start"]) + (overlap_seconds if idx > 0 else 0.0)
            end = entry["end"] if entry["end"] is not None else probe["duration"]
            if end is not None and probe["duration"]:
                end = min(float(end), float(probe["duration"]))
            if end is not None and end - start < 1.0 / frame_rate:
                raise ValueError(
                    f"clip {idx + 1} trim leaves no frames: start={start:.3f}s end={end:.3f}s"
                )
            entry["_effective_start"] = start
            entry["_effective_end"] = end
            speed = float(entry.get("speed") or 1.0)
            source_span = (float(end) - start) if end is not None else 0.0
            # Duration this clip contributes to the timeline once sped up.
            entry["_output_duration"] = (source_span / speed) if speed > 0 else source_span

        # A crossfade can never be longer than either clip it joins.
        for idx in range(len(entries) - 1):
            fade = float(entries[idx].get("xfade") or 0.0)
            if fade <= 0:
                continue
            budget = min(entries[idx]["_output_duration"], entries[idx + 1]["_output_duration"])
            entries[idx]["xfade"] = max(0.0, min(fade, max(0.0, budget - 1.0 / frame_rate)))

        output_dir = (
            folder_paths.get_output_directory()
            if save_output
            else folder_paths.get_temp_directory()
        )
        full_output_folder, filename, _, subfolder, _ = folder_paths.get_save_image_path(
            filename_prefix,
            output_dir,
        )
        existing = sorted(Path(full_output_folder).glob(f"{filename}_*.mp4"))
        counter = 1
        if existing:
            suffixes = []
            for item in existing:
                stem = item.stem
                parts = stem.split("_")
                if parts:
                    tail = parts[-1].replace("-audio", "")
                    if tail.isdigit():
                        suffixes.append(int(tail))
            if suffixes:
                counter = max(suffixes) + 1

        base_file = f"{filename}_{counter:05}.mp4"
        audio_file = f"{filename}_{counter:05}-audio.mp4"
        base_path = os.path.join(full_output_folder, base_file)
        audio_path = os.path.join(full_output_folder, audio_file)

        self._render_edited_videos_ffmpeg_filtergraph(
            entries=entries,
            probes=probes,
            frame_rate=frame_rate,
            base_width=base_width,
            base_height=base_height,
            pix_fmt=pix_fmt,
            crf=crf,
            base_path=base_path,
            audio_path=audio_path,
        )

        preview = {
            "filename": audio_file,
            "subfolder": subfolder,
            "type": "output" if save_output else "temp",
            "format": "video/h264-mp4",
            "frame_rate": frame_rate,
            "fullpath": audio_path,
        }
        return {
            "ui": {"gifs": [preview]},
            "result": ((save_output, [base_path, audio_path]),),
        }

    def _render_edited_videos_ffmpeg_filtergraph(
        self,
        *,
        entries,
        probes,
        frame_rate,
        base_width,
        base_height,
        pix_fmt,
        crf,
        base_path,
        audio_path,
    ):
        ffmpeg_inputs = []
        filter_parts = []
        for idx, (entry, probe) in enumerate(zip(entries, probes)):
            start = float(entry.get("_effective_start") or 0.0)
            end = entry.get("_effective_end")
            speed = float(entry.get("speed") or 1.0)
            ffmpeg_inputs.extend(["-i", probe["path"]])

            trim_args = []
            if start > 0:
                trim_args.append(f"start={start:.6f}")
            if end is not None:
                trim_args.append(f"end={float(end):.6f}")

            video_filters = []
            if trim_args:
                video_filters.append(f"trim={':'.join(trim_args)}")
            # Re-baselining timestamps and applying speed in one setpts keeps
            # the expression exact; a separate pass would round twice.
            video_filters.append(
                "setpts=PTS-STARTPTS" if abs(speed - 1.0) <= 1e-6
                else f"setpts=(PTS-STARTPTS)/{speed:.6f}"
            )
            video_filters.extend(_color_filters(
                float(entry.get("brightness") or 1.0),
                float(entry.get("contrast") or 1.0),
                float(entry.get("saturation") or 1.0),
            ))
            video_filters.extend(
                [
                    f"fps={frame_rate}",
                    f"scale={base_width}:{base_height}:flags=lanczos:force_original_aspect_ratio=decrease",
                    f"pad={base_width}:{base_height}:(ow-iw)/2:(oh-ih)/2:black",
                    f"format={pix_fmt}",
                    "setsar=1",
                ]
            )
            filter_parts.append(f"[{idx}:v]{','.join(video_filters)}[v{idx}]")

            if probe["has_audio"]:
                audio_filters = []
                if trim_args:
                    audio_filters.extend([f"atrim={':'.join(trim_args)}", "asetpts=PTS-STARTPTS"])
                else:
                    audio_filters.append("asetpts=PTS-STARTPTS")
                audio_filters.extend(_atempo_chain(speed))
                audio_filters.extend(
                    [
                        "aresample=48000",
                        "aformat=sample_fmts=fltp:channel_layouts=stereo",
                    ]
                )
                filter_parts.append(f"[{idx}:a]{','.join(audio_filters)}[a{idx}]")
            else:
                silent_duration = max(0.001, float(entry.get("_output_duration") or 0.001))
                filter_parts.append(
                    f"anullsrc=channel_layout=stereo:sample_rate=48000:d={silent_duration:.6f}[a{idx}]"
                )

        has_crossfade = any(float(e.get("xfade") or 0.0) > 0 for e in entries[:-1])
        if not has_crossfade:
            concat_inputs = []
            for idx in range(len(probes)):
                concat_inputs.extend([f"[v{idx}]", f"[a{idx}]"])
            filter_parts.append(
                "".join(concat_inputs) + f"concat=n={len(probes)}:v=1:a=1[v][a]"
            )
        else:
            # xfade/acrossfade are pairwise, so fold the clips left to right.
            # Each fade overlaps the streams, so the running duration grows by
            # the next clip's length MINUS the fade, and the next offset is
            # measured against that accumulated timeline.
            cur_v, cur_a = "[v0]", "[a0]"
            acc = float(entries[0].get("_output_duration") or 0.0)
            for idx in range(1, len(entries)):
                fade = float(entries[idx - 1].get("xfade") or 0.0)
                nxt_v, nxt_a = f"[v{idx}]", f"[a{idx}]"
                out_v, out_a = f"[vx{idx}]", f"[ax{idx}]"
                nxt_duration = float(entries[idx].get("_output_duration") or 0.0)
                if fade > 0:
                    offset = max(0.0, acc - fade)
                    filter_parts.append(
                        f"{cur_v}{nxt_v}xfade=transition=fade:duration={fade:.6f}:offset={offset:.6f}{out_v}"
                    )
                    filter_parts.append(
                        f"{cur_a}{nxt_a}acrossfade=d={fade:.6f}:c1=tri:c2=tri{out_a}"
                    )
                    acc = acc + nxt_duration - fade
                else:
                    filter_parts.append(f"{cur_v}{nxt_v}concat=n=2:v=1:a=0{out_v}")
                    filter_parts.append(f"{cur_a}{nxt_a}concat=n=2:v=0:a=1{out_a}")
                    acc = acc + nxt_duration
                cur_v, cur_a = out_v, out_a
            filter_parts.append(f"{cur_v}null[v]")
            filter_parts.append(f"{cur_a}anull[a]")

        cmd = [
            FFMPEG_BIN,
            "-y",
            "-v",
            "error",
            *ffmpeg_inputs,
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            str(crf),
            "-pix_fmt",
            pix_fmt,
            "-r",
            str(frame_rate),
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            audio_path,
        ]
        subprocess.run(cmd, check=True)

        subprocess.run(
            [FFMPEG_BIN, "-y", "-v", "error", "-i", audio_path, "-an", "-c:v", "copy", base_path],
            check=True,
        )


def _output_bundle(filename_prefix, extension_names, save_output):
    output_dir = folder_paths.get_output_directory() if save_output else folder_paths.get_temp_directory()
    full_output_folder, filename, _, subfolder, _ = folder_paths.get_save_image_path(filename_prefix, output_dir)
    # API job prefixes are already unique. Stable names make an idempotent
    # retry overwrite-safe and keep the gateway's output map exact.
    stem = f"{filename}_00001"
    return full_output_folder, subfolder, stem, {
        name: os.path.join(full_output_folder, f"{stem}{suffix}")
        for name, suffix in extension_names.items()
    }


def _storyboard_geometry(duration, source_width, source_height):
    frame_count = min(240, max(1, int(math.ceil(max(0.001, float(duration)) / 0.5))))
    columns = min(12, frame_count)
    rows = int(math.ceil(frame_count / columns))
    frame_width = 160
    frame_height = max(2, round(frame_width * source_height / max(1, source_width) / 2) * 2)
    return frame_count, columns, rows, frame_width, frame_height


class FCSAnalyzeVideo:
    """Create deterministic proxy, storyboard, waveform, and metadata artifacts."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "source_video_url": ("STRING", {"default": "https://example.com/video.mp4"}),
                "source_fingerprint": ("STRING", {"default": ""}),
                "filename_prefix": ("STRING", {"default": "video_analysis"}),
                "save_output": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("VHS_FILENAMES",)
    RETURN_NAMES = ("Filenames",)
    OUTPUT_NODE = True
    CATEGORY = "Furgen"
    FUNCTION = "analyze_video"

    def analyze_video(self, source_video_url, source_fingerprint, filename_prefix, save_output):
        source = _resolve_video_entry(source_video_url)
        details = _probe_video_details(source)
        folder, subfolder, stem, paths = _output_bundle(
            filename_prefix,
            {"proxy": "-proxy.mp4", "storyboard": "-storyboard.webp", "analysis": "-analysis.json"},
            save_output,
        )
        del folder
        duration = max(0.001, float(details["duration_seconds"]))
        subprocess.run(
            [
                FFMPEG_BIN, "-y", "-v", "error", "-i", source,
                "-vf", "fps=30,scale='min(1280,iw)':'min(720,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2",
                "-map", "0:v:0", "-map", "0:a?",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "24", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", paths["proxy"],
            ],
            check=True,
        )
        proxy_details = _probe_video_details(paths["proxy"])
        frame_count, columns, rows, storyboard_width, frame_height = _storyboard_geometry(
            duration, details["width"], details["height"]
        )
        storyboard_png = paths["storyboard"] + ".png"
        subprocess.run(
            [
                FFMPEG_BIN, "-y", "-v", "error", "-i", source,
                "-vf", f"fps={frame_count / duration:.9f},scale={storyboard_width}:{frame_height},tile={columns}x{rows}:nb_frames={frame_count}:padding=0:margin=0",
                "-frames:v", "1", "-c:v", "png", storyboard_png,
            ],
            check=True,
        )
        with Image.open(storyboard_png) as storyboard_image:
            storyboard_image.save(paths["storyboard"], format="WEBP", quality=82, method=4)
        os.unlink(storyboard_png)

        peaks, rms = [], []
        loudness = {}
        if details["has_audio"]:
            pcm = subprocess.run(
                [FFMPEG_BIN, "-v", "error", "-i", source, "-vn", "-ac", "1", "-ar", "48000", "-f", "s16le", "-"],
                capture_output=True,
                check=True,
            ).stdout
            samples = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768.0
            block = 2400
            for start in range(0, samples.size, block):
                segment = samples[start:start + block]
                if not segment.size:
                    continue
                peaks.append(round(float(np.max(np.abs(segment))), 6))
                rms.append(round(float(np.sqrt(np.mean(np.square(segment)))), 6))
            loudness_proc = subprocess.run(
                [
                    FFMPEG_BIN, "-v", "info", "-i", source, "-vn",
                    "-af", "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json", "-f", "null", "-",
                ],
                capture_output=True,
                text=True,
            )
            matches = re.findall(r"\{[^{}]*\}", loudness_proc.stderr or "", re.DOTALL)
            if matches:
                try:
                    measured = json.loads(matches[-1])
                    loudness = {
                        "integratedLoudnessLufs": float(measured["input_i"]),
                        "truePeakDbfs": float(measured["input_tp"]),
                    }
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    loudness = {}

        canonical_fingerprint = hashlib.sha256(
            f"{source}|{details['width']}x{details['height']}|{details['duration_seconds']:.6f}|{details['frame_count']}".encode()
        ).hexdigest()
        advisory = str(source_fingerprint or "").strip()
        cache_key = hashlib.sha256(f"video-analysis-v1|{canonical_fingerprint}|{advisory}".encode()).hexdigest()
        base_names = {key: os.path.basename(value) for key, value in paths.items()}
        cues = [
            {
                "index": index,
                "timeSeconds": round(min(duration, duration * (index + 0.5) / frame_count), 6),
                "x": (index % columns) * storyboard_width,
                "y": (index // columns) * frame_height,
                "width": storyboard_width,
                "height": frame_height,
            }
            for index in range(frame_count)
        ]
        manifest = {
            "version": 1,
            "cacheKey": cache_key,
            "sourceFingerprint": advisory or None,
            "canonicalSourceFingerprint": canonical_fingerprint,
            "source": {
                "width": details["width"], "height": details["height"],
                "frameRate": details["frame_rate"], "frameCount": details["frame_count"],
                "durationSeconds": details["duration_seconds"], "bitrate": details["bitrate"],
                "hasAudio": details["has_audio"], "audioSampleRate": details["audio_sample_rate"],
                "audioChannels": details["audio_channels"],
            },
            "proxy": {
                "filename": base_names["proxy"], "width": proxy_details["width"],
                "height": proxy_details["height"], "frameRate": 30,
                "durationSeconds": proxy_details["duration_seconds"],
            },
            "storyboard": {
                "filename": base_names["storyboard"], "columns": columns, "rows": rows,
                "frameWidth": storyboard_width, "frameHeight": frame_height, "cues": cues,
            },
            "waveform": {"pointsPerSecond": 20, "sampleRate": 48000, "channels": 1, "peaks": peaks, "rms": rms, **loudness},
            "cors": {"allowOrigin": "*"},
        }
        with open(paths["analysis"], "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, separators=(",", ":"), allow_nan=False)
        previews = [
            {
                "filename": base_names["proxy"], "subfolder": subfolder,
                "type": "output" if save_output else "temp", "format": "video/h264-mp4",
                "frame_rate": 30, "fullpath": paths["proxy"],
            }
        ]
        return {"ui": {"gifs": previews}, "result": ((save_output, list(paths.values())),)}


def _v4_clip_duration(entry, probe):
    start = max(0.0, float(entry.get("trimStartSeconds") or 0.0))
    end = min(float(probe["duration"]), float(entry.get("trimEndSeconds") or probe["duration"]))
    speed = max(0.001, float(entry.get("speed") or 1.0))
    return start, end, (end - start) / speed


class FCSConcatVideosV4(FCSConcatVideosV3):
    """Precision single-track compositor with framing and full audio control."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "edit_manifest": ("STRING", {"default": "{}", "multiline": True}),
                "output_width": ("INT", {"default": 1920, "min": 2, "max": 7680, "step": 2}),
                "output_height": ("INT", {"default": 1080, "min": 2, "max": 7680, "step": 2}),
                "frame_rate": ("FLOAT", {"default": 60.0, "min": 1.0, "max": 120.0, "step": 1.0}),
                "audio_crossfade_curve": (["linear", "equalPower"], {"default": "equalPower"}),
                "filename_prefix": ("STRING", {"default": "video_precision_edit"}),
                "pix_fmt": (["yuv420p"],),
                "crf": ("INT", {"default": 15, "min": 0, "max": 51, "step": 1}),
                "save_output": ("BOOLEAN", {"default": True}),
            }
        }

    FUNCTION = "concat_videos_v4"

    def concat_videos_v4(self, edit_manifest, output_width, output_height, frame_rate,
                         audio_crossfade_curve, filename_prefix, pix_fmt, crf, save_output):
        manifest = json.loads(edit_manifest)
        entries = manifest.get("clips") or []
        if not entries:
            raise ValueError("edit_manifest.clips must contain at least one clip")
        for entry in entries:
            entry["path"] = _resolve_video_entry(entry.get("sourceVideoUrl"))
        probes = [_probe_video(entry["path"]) for entry in entries]
        for entry, probe in zip(entries, probes):
            start, end, duration = _v4_clip_duration(entry, probe)
            if duration < 1.0 / float(frame_rate):
                raise ValueError("clip trim leaves no output frames")
            entry["_effective_start"], entry["_effective_end"], entry["_output_duration"] = start, end, duration
        for index, entry in enumerate(entries[:-1]):
            transition = entry.get("transitionAfter") or {}
            fade = 0.0
            if transition.get("type") == "blend":
                fade = 19.0 / float(frame_rate)
            elif transition.get("type") == "crossfade":
                fade = float(transition.get("durationSeconds") or 0.0)
            budget = min(entry["_output_duration"], entries[index + 1]["_output_duration"])
            entry["_xfade"] = min(fade, max(0.0, budget - 1.0 / float(frame_rate)))

        _, subfolder, stem, paths = _output_bundle(
            filename_prefix, {"video": ".mp4", "audio": "-audio.mp4"}, save_output
        )
        self._render_precision_filtergraph(
            entries=entries, probes=probes, soundtrack=manifest.get("soundtrack"),
            output_width=int(output_width), output_height=int(output_height), frame_rate=float(frame_rate),
            audio_curve=audio_crossfade_curve, pix_fmt=pix_fmt, crf=crf,
            base_path=paths["video"], audio_path=paths["audio"],
        )
        preview = {
            "filename": os.path.basename(paths["audio"]), "subfolder": subfolder,
            "type": "output" if save_output else "temp", "format": "video/h264-mp4",
            "frame_rate": frame_rate, "fullpath": paths["audio"],
        }
        return {"ui": {"gifs": [preview]}, "result": ((save_output, [paths["video"], paths["audio"]]),)}

    def _render_precision_filtergraph(self, *, entries, probes, soundtrack, output_width,
                                      output_height, frame_rate, audio_curve, pix_fmt, crf,
                                      base_path, audio_path):
        ffmpeg_inputs, filters = [], []
        for index, (entry, probe) in enumerate(zip(entries, probes)):
            ffmpeg_inputs.extend(["-i", probe["path"]])
            start, end = entry["_effective_start"], entry["_effective_end"]
            duration, speed = entry["_output_duration"], float(entry.get("speed") or 1.0)
            trim = f"trim=start={start:.6f}:end={end:.6f}"
            base_filters = [trim, f"setpts=(PTS-STARTPTS)/{speed:.6f}"]
            adjustments = entry.get("adjustments") or {}
            base_filters.extend(_color_filters(
                float(adjustments.get("brightness") or 1.0),
                float(adjustments.get("contrast") or 1.0),
                float(adjustments.get("saturation") or 1.0),
            ))
            base_filters.append(f"fps={frame_rate}")
            framing = entry.get("framing") or {}
            mode = framing.get("mode") or "fit"
            pan_x = max(0.0, min(1.0, float(framing.get("panX", 0.5))))
            pan_y = max(0.0, min(1.0, float(framing.get("panY", 0.5))))
            zoom = max(1.0, min(3.0, float(framing.get("zoom", 1.0))))
            if mode == "fit" and framing.get("fitBackground", "black") == "blur":
                filters.append(f"[{index}:v]{','.join(base_filters)},split=2[v{index}bg0][v{index}fg0]")
                filters.append(
                    f"[v{index}bg0]scale={output_width}:{output_height}:force_original_aspect_ratio=increase,"
                    f"crop={output_width}:{output_height},gblur=sigma=30[v{index}bg]"
                )
                filters.append(
                    f"[v{index}fg0]scale={output_width}:{output_height}:force_original_aspect_ratio=decrease[v{index}fg]"
                )
                filters.append(
                    f"[v{index}bg][v{index}fg]overlay=(W-w)/2:(H-h)/2,format={pix_fmt},setsar=1[v{index}]"
                )
            elif mode == "fit":
                base_filters.extend([
                    f"scale={output_width}:{output_height}:force_original_aspect_ratio=decrease",
                    f"pad={output_width}:{output_height}:(ow-iw)/2:(oh-ih)/2:black",
                    f"format={pix_fmt}", "setsar=1",
                ])
                filters.append(f"[{index}:v]{','.join(base_filters)}[v{index}]")
            else:
                scale_width = max(output_width, int(round(output_width * zoom / 2.0) * 2))
                scale_height = max(output_height, int(round(output_height * zoom / 2.0) * 2))
                base_filters.extend([
                    f"scale={scale_width}:{scale_height}:force_original_aspect_ratio=increase",
                    f"crop={output_width}:{output_height}:(iw-ow)*{pan_x:.6f}:(ih-oh)*{pan_y:.6f}",
                    f"format={pix_fmt}", "setsar=1",
                ])
                filters.append(f"[{index}:v]{','.join(base_filters)}[v{index}]")

            audio = entry.get("audio") or {}
            if probe["has_audio"]:
                audio_filters = [f"atrim=start={start:.6f}:end={end:.6f}", "asetpts=PTS-STARTPTS"]
                audio_filters.extend(_atempo_chain(speed))
                gain = 0.0 if audio.get("muted") else max(0.0, min(2.0, float(audio.get("gain", 1.0))))
                audio_filters.append(f"volume={gain:.6f}")
                fade_in = min(duration, max(0.0, float(audio.get("fadeInSeconds") or 0.0)))
                fade_out = min(duration, max(0.0, float(audio.get("fadeOutSeconds") or 0.0)))
                if fade_in:
                    audio_filters.append(f"afade=t=in:st=0:d={fade_in:.6f}")
                if fade_out:
                    audio_filters.append(f"afade=t=out:st={max(0.0, duration - fade_out):.6f}:d={fade_out:.6f}")
                audio_filters.extend(["aresample=48000", "aformat=sample_fmts=fltp:channel_layouts=stereo"])
                filters.append(f"[{index}:a]{','.join(audio_filters)}[a{index}]")
            else:
                filters.append(f"anullsrc=channel_layout=stereo:sample_rate=48000:d={duration:.6f}[a{index}]")

        cur_v, cur_a = "[v0]", "[a0]"
        timeline_duration = float(entries[0]["_output_duration"])
        curve = "tri" if audio_curve == "linear" else "qsin"
        for index in range(1, len(entries)):
            fade = float(entries[index - 1].get("_xfade") or 0.0)
            out_v, out_a = f"[vx{index}]", f"[ax{index}]"
            if fade:
                filters.append(
                    f"{cur_v}[v{index}]xfade=transition=fade:duration={fade:.6f}:"
                    f"offset={max(0.0, timeline_duration - fade):.6f}{out_v}"
                )
                filters.append(f"{cur_a}[a{index}]acrossfade=d={fade:.6f}:c1={curve}:c2={curve}{out_a}")
                timeline_duration += float(entries[index]["_output_duration"]) - fade
            else:
                filters.append(f"{cur_v}[v{index}]concat=n=2:v=1:a=0{out_v}")
                filters.append(f"{cur_a}[a{index}]concat=n=2:v=0:a=1{out_a}")
                timeline_duration += float(entries[index]["_output_duration"])
            cur_v, cur_a = out_v, out_a
        filters.append(f"{cur_v}null[v]")

        final_audio = cur_a
        if soundtrack:
            music_index = len(entries)
            ffmpeg_inputs.extend(["-i", _resolve_video_entry(soundtrack.get("sourceAudioUrl"))])
            music_start = max(0.0, float(soundtrack.get("trimStartSeconds") or 0.0))
            music_end = soundtrack.get("trimEndSeconds")
            music_filters = [f"atrim=start={music_start:.6f}" + (f":end={float(music_end):.6f}" if music_end else ""), "asetpts=PTS-STARTPTS", "aresample=48000"]
            if soundtrack.get("loop"):
                selected_duration = max(0.001, float(music_end or (music_start + timeline_duration)) - music_start)
                music_filters.append(f"aloop=loop=-1:size={max(1, int(round(selected_duration * 48000)))}")
            gain = max(0.0, min(2.0, float(soundtrack.get("gain", 1.0))))
            music_filters.append(f"volume={gain:.6f}")
            offset = max(0.0, float(soundtrack.get("timelineOffsetSeconds") or 0.0))
            fade_in = max(0.0, float(soundtrack.get("fadeInSeconds") or 0.0))
            fade_out = max(0.0, float(soundtrack.get("fadeOutSeconds") or 0.0))
            available = max(0.0, timeline_duration - offset)
            if fade_in:
                music_filters.append(f"afade=t=in:st=0:d={min(fade_in, available):.6f}")
            if fade_out:
                music_filters.append(f"afade=t=out:st={max(0.0, available - fade_out):.6f}:d={min(fade_out, available):.6f}")
            if offset:
                delay = int(round(offset * 1000))
                music_filters.append(f"adelay={delay}|{delay}")
            music_filters.extend([f"apad=whole_dur={timeline_duration:.6f}", f"atrim=end={timeline_duration:.6f}", "aformat=sample_fmts=fltp:channel_layouts=stereo"])
            filters.append(f"[{music_index}:a]{','.join(music_filters)}[music]")
            ducking = soundtrack.get("ducking") or {}
            music_label = "[music]"
            main_label = cur_a
            if ducking.get("enabled") and float(ducking.get("depthDb") or 0.0) > 0:
                depth = max(0.0, min(24.0, float(ducking.get("depthDb") or 0.0)))
                ratio = 1.0 + depth
                filters.append(f"{cur_a}asplit=2[mainmix][sidechain]")
                filters.append(f"[music][sidechain]sidechaincompress=threshold=0.02:ratio={ratio:.6f}:attack=20:release=250[ducked]")
                music_label = "[ducked]"
                main_label = "[mainmix]"
            filters.append(f"{main_label}{music_label}amix=inputs=2:duration=first:normalize=0[a]")
            final_audio = "[a]"
        else:
            filters.append(f"{cur_a}anull[a]")
            final_audio = "[a]"

        command = [
            FFMPEG_BIN, "-y", "-v", "error", *ffmpeg_inputs, "-filter_complex", ";".join(filters),
            "-map", "[v]", "-map", final_audio, "-c:v", "libx264", "-preset", "medium",
            "-crf", str(crf), "-pix_fmt", pix_fmt, "-r", str(frame_rate),
            "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", audio_path,
        ]
        subprocess.run(command, check=True)
        subprocess.run([FFMPEG_BIN, "-y", "-v", "error", "-i", audio_path, "-an", "-c:v", "copy", base_path], check=True)


NODE_CLASS_MAPPINGS = {
    "FCSConcatVideos": FCSConcatVideos,
    "FCSConcatVideosV2": FCSConcatVideosV2,
    "FCSConcatVideosV3": FCSConcatVideosV3,
    "FCSConcatVideosV4": FCSConcatVideosV4,
    "FCSAnalyzeVideo": FCSAnalyzeVideo,
    "FurgenExposureAdjust": FurgenExposureAdjust,
    "FurgenGetImageRangeFromBatch": FurgenGetImageRangeFromBatch,
    "FurgenPrependImageToBatch": FurgenPrependImageToBatch,
    "FurgenSeamScaleStabilize": FurgenSeamScaleStabilize,
    "FurgenTrimAudioDuration": FurgenTrimAudioDuration,
    "FurgenReferenceColorMatch": FurgenReferenceColorMatch,
    "FurgenAdaptiveExposureMatch": FurgenAdaptiveExposureMatch,
    "FurgenColorTransferMatch": FurgenColorTransferMatch,
    "FurgenTemporalToneSmooth": FurgenTemporalToneSmooth,
    "FurgenTemporalUnsharpMask": FurgenTemporalUnsharpMask,
    "FurgenLatentGuideTemporalMask": FurgenLatentGuideTemporalMask,
    "FurgenLTXVAddLatentGuideTemporal": FurgenLTXVAddLatentGuideTemporal,
    "FurgenLTXGuideAttentionAdjust": FurgenLTXGuideAttentionAdjust,
    "FurgenAssertFiniteImages": FurgenAssertFiniteImages,
    "FurgenAssertFiniteLatent": FurgenAssertFiniteLatent,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FCSConcatVideos": "Furgen Concat Videos",
    "FCSConcatVideosV2": "Furgen Concat Videos V2 (trims)",
    "FCSConcatVideosV3": "Furgen Concat Videos V3 (trims, speed, colour, crossfade)",
    "FCSConcatVideosV4": "Furgen Concat Videos V4 (precision editor)",
    "FCSAnalyzeVideo": "Furgen Analyze Video",
    "FurgenExposureAdjust": "Furgen Exposure Adjust",
    "FurgenGetImageRangeFromBatch": "Furgen Get Image Range From Batch",
    "FurgenPrependImageToBatch": "Furgen Prepend Image To Batch",
    "FurgenSeamScaleStabilize": "Furgen Seam Scale Stabilize",
    "FurgenTrimAudioDuration": "Furgen Trim Audio Duration",
    "FurgenReferenceColorMatch": "Furgen Reference Color Match",
    "FurgenAdaptiveExposureMatch": "Furgen Adaptive Exposure Match",
    "FurgenColorTransferMatch": "Furgen Color Transfer Match",
    "FurgenTemporalToneSmooth": "Furgen Temporal Tone Smooth",
    "FurgenTemporalUnsharpMask": "Furgen Temporal Unsharp Mask",
    "FurgenLatentGuideTemporalMask": "Furgen Latent Guide Temporal Mask",
    "FurgenLTXVAddLatentGuideTemporal": "Furgen LTXV Add Latent Guide Temporal",
    "FurgenLTXGuideAttentionAdjust": "Furgen LTX Guide Attention Adjust",
    "FurgenAssertFiniteImages": "Furgen Assert Finite Images",
    "FurgenAssertFiniteLatent": "Furgen Assert Finite Latent",
}

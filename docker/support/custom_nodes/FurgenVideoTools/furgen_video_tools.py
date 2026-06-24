import json
import os
import subprocess
from pathlib import Path

import folder_paths
import torch


FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")


RGB_LUMA_WEIGHTS = (0.2126, 0.7152, 0.0722)


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
            }
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
    ):
        entries = _parse_video_entries(video_entries)
        probes = [_probe_video(entry) for entry in entries]
        base_width = probes[0]["width"] or 1920
        base_height = probes[0]["height"] or 1088
        overlap_frames = max(0, int(overlap_frames or 0))
        overlap_seconds = float(overlap_frames) / float(frame_rate or 60.0) if overlap_frames > 0 else 0.0

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

        ffmpeg_inputs = []
        filter_parts = []
        concat_inputs = []
        for idx, probe in enumerate(probes):
            clip_trim_seconds = overlap_seconds if idx > 0 else 0.0
            ffmpeg_inputs.extend(["-i", probe["path"]])
            video_filters = [
                f"fps={frame_rate}",
                f"scale={base_width}:{base_height}:flags=lanczos:force_original_aspect_ratio=decrease",
                f"pad={base_width}:{base_height}:(ow-iw)/2:(oh-ih)/2:black",
                f"format={pix_fmt}",
                "setsar=1",
            ]
            if clip_trim_seconds > 0:
                video_filters.extend(
                    [
                        f"trim=start={clip_trim_seconds:.6f}",
                        "setpts=PTS-STARTPTS",
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


NODE_CLASS_MAPPINGS = {
    "FCSConcatVideos": FCSConcatVideos,
    "FurgenExposureAdjust": FurgenExposureAdjust,
    "FurgenReferenceColorMatch": FurgenReferenceColorMatch,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FCSConcatVideos": "Furgen Concat Videos",
    "FurgenExposureAdjust": "Furgen Exposure Adjust",
    "FurgenReferenceColorMatch": "Furgen Reference Color Match",
}

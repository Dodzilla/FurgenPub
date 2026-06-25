import json
import os
import subprocess
from pathlib import Path

import folder_paths
import torch


FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")


RGB_LUMA_WEIGHTS = (0.2126, 0.7152, 0.0722)
V2_FRAME_CHUNK_SIZE = 4
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


def _chunked_frames(images: torch.Tensor, chunk_size: int = V2_FRAME_CHUNK_SIZE):
    batch = int(images.shape[0])
    step = max(1, int(chunk_size))
    for start in range(0, batch, step):
        yield images[start : start + step]


def _sample_pixels_channel_last(values: torch.Tensor, max_pixels: int = V2_STAT_SAMPLE_PIXELS) -> torch.Tensor:
    flat = values.reshape(-1, values.shape[-1])
    limit = max(1, int(max_pixels))
    if flat.shape[0] <= limit:
        return flat
    index = torch.linspace(0, flat.shape[0] - 1, steps=limit, device=flat.device).long()
    return flat.index_select(0, index)


def _mean_std_stats_single(values: torch.Tensor, max_pixels: int = V2_STAT_SAMPLE_PIXELS) -> tuple[torch.Tensor, torch.Tensor]:
    sample = _sample_pixels_channel_last(values, max_pixels=max_pixels)
    mean = sample.mean(dim=0).view(1, 1, 1, values.shape[-1])
    std = sample.std(dim=0, unbiased=False).view(1, 1, 1, values.shape[-1])
    return mean, std


def _mean_std_stats_frames(values: torch.Tensor, max_pixels: int = V2_STAT_SAMPLE_PIXELS) -> tuple[torch.Tensor, torch.Tensor]:
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


def _robust_luma_mean_single(
    luma: torch.Tensor,
    black_percentile: float,
    white_percentile: float,
    max_pixels: int = V2_STAT_SAMPLE_PIXELS,
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
    max_pixels: int = V2_STAT_SAMPLE_PIXELS,
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
                out_chunks = []

                for chunk in _chunked_frames(images):
                    phase = "frame_chunk"
                    rgb = _image_rgb(chunk)
                    src_mean = _robust_luma_mean_frames(
                        _luma(rgb),
                        black_percentile,
                        white_percentile,
                    ).clamp_min(_eps_for(rgb))
                    gain = (ref_mean / src_mean).clamp(lo, hi)
                    corrected = _apply_highlight_protection(rgb, rgb * gain, preserve_highlights)
                    out_chunks.append(_blend_and_restore_channels(chunk, corrected, strength))

                phase = "concat"
                return (torch.cat(out_chunks, dim=0),)
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
                out_chunks = []

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
                    for chunk in _chunked_frames(images):
                        phase = "frame_chunk_rgb"
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
                        out_chunks.append(_blend_and_restore_channels(chunk, corrected, strength))
                elif mode == "ycbcr_mean_std":
                    phase = "reference_stats_ycbcr"
                    ref_ycbcr = _rgb_to_ycbcr(ref_rgb)
                    ref_mean, ref_std = _mean_std_stats_single(ref_ycbcr)
                    mean_strengths = torch.tensor(
                        [float(luma_strength), float(chroma_strength), float(chroma_strength)],
                        dtype=images.dtype,
                        device=images.device,
                    ).view(1, 1, 1, 3).clamp(0.0, 1.0)
                    for chunk in _chunked_frames(images):
                        phase = "frame_chunk_ycbcr"
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
                        out_chunks.append(_blend_and_restore_channels(chunk, corrected, strength))
                else:
                    raise ValueError(f"unsupported color transfer mode: {mode}")

                phase = "concat"
                return (torch.cat(out_chunks, dim=0),)
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
                corrected_frames = []

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
                        corrected_frames.append(image_frame)
                    else:
                        adjusted = ycbcr.clone()
                        adjusted[..., 0:1] = ycbcr[..., 0:1] * (1.0 + (limited_gain - 1.0) * global_strength)
                        adjusted[..., 1:3] = ycbcr[..., 1:3] + limited_chroma_offset * global_strength
                        corrected_frames.append(_restore_channels(image_frame, _ycbcr_to_rgb(adjusted)))
                    previous_gain = limited_gain
                    previous_chroma_offset = limited_chroma_offset

                phase = "concat"
                return (torch.cat(corrected_frames, dim=0),)
        except Exception as exc:
            raise _node_runtime_error(self.__class__.__name__, images, phase, exc) from exc


NODE_CLASS_MAPPINGS = {
    "FCSConcatVideos": FCSConcatVideos,
    "FurgenExposureAdjust": FurgenExposureAdjust,
    "FurgenReferenceColorMatch": FurgenReferenceColorMatch,
    "FurgenAdaptiveExposureMatch": FurgenAdaptiveExposureMatch,
    "FurgenColorTransferMatch": FurgenColorTransferMatch,
    "FurgenTemporalToneSmooth": FurgenTemporalToneSmooth,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FCSConcatVideos": "Furgen Concat Videos",
    "FurgenExposureAdjust": "Furgen Exposure Adjust",
    "FurgenReferenceColorMatch": "Furgen Reference Color Match",
    "FurgenAdaptiveExposureMatch": "Furgen Adaptive Exposure Match",
    "FurgenColorTransferMatch": "Furgen Color Transfer Match",
    "FurgenTemporalToneSmooth": "Furgen Temporal Tone Smooth",
}

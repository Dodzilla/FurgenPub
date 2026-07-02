import json
import os
import subprocess
from pathlib import Path

import folder_paths
import numpy as np
import torch
import torch.nn.functional as F


FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")


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
            clip_trim_frames = overlap_frames if idx > 0 else 0
            ffmpeg_inputs.extend(["-i", probe["path"]])
            video_filters = [
                f"fps={frame_rate}",
            ]
            if clip_trim_frames > 0:
                video_filters.extend(
                    [
                        f"select='gte(n,{clip_trim_frames})'",
                        f"setpts=N/{float(frame_rate or 60.0):.6f}/TB",
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
        if not isinstance(samples, torch.Tensor):
            raise ValueError(f"FurgenAssertFiniteLatent {label}: latent.samples must be a tensor")
        if not torch.isfinite(samples).all():
            raise ValueError(f"FurgenAssertFiniteLatent {label}: non-finite latent.samples {_finite_summary(samples)}")
        mask = latent.get("noise_mask")
        if check_noise_mask and isinstance(mask, torch.Tensor) and not torch.isfinite(mask).all():
            raise ValueError(f"FurgenAssertFiniteLatent {label}: non-finite latent.noise_mask {_finite_summary(mask)}")
        return (latent,)


NODE_CLASS_MAPPINGS = {
    "FCSConcatVideos": FCSConcatVideos,
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

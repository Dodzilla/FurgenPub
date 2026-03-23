import json
import os
import subprocess
from pathlib import Path

import folder_paths


FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")


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


NODE_CLASS_MAPPINGS = {
    "FCSConcatVideos": FCSConcatVideos,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FCSConcatVideos": "Furgen Concat Videos",
}

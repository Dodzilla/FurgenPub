import json
import os
import shutil
import subprocess
import time
import urllib.request
import zipfile
from pathlib import Path


def _comfy_root() -> Path:
    return Path(os.environ.get("DM_COMFYUI_DIR") or os.environ.get("COMFYUI_DIR") or "/workspace/ComfyUI")


def _factory_root() -> Path:
    return Path(os.environ.get("FCS_LORA_FACTORY_DIR") or "/data/lora_factory")


def _safe_name(value: str, fallback: str = "lora_run") -> str:
    value = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(value or "").strip())
    return value.strip("._") or fallback


def _split_lines(value: str):
    return [line.strip() for line in str(value or "").splitlines() if line.strip()]


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=120) as response:
        with dest.open("wb") as fh:
            shutil.copyfileobj(response, fh)


class FCSLoraDatasetPrepare:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "character_id": ("STRING", {"default": "kmt_aurin"}),
                "character_display_name": ("STRING", {"default": "Aurin"}),
                "dataset_version": ("STRING", {"default": "v001"}),
                "trigger": ("STRING", {"default": "kmtAurin"}),
                "class_name": ("STRING", {"default": "anthro fox character"}),
                "image_urls": ("STRING", {"multiline": True, "default": ""}),
                "caption_lines": ("STRING", {"multiline": True, "default": ""}),
                "dataset_zip_url": ("STRING", {"default": ""}),
                "mask_urls": ("STRING", {"multiline": True, "default": ""}),
                "target_resolution": ("INT", {"default": 1024, "min": 256, "max": 2048, "step": 64}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("dataset_manifest",)
    FUNCTION = "prepare"
    CATEGORY = "Furgen/LoRA"

    def prepare(
        self,
        character_id,
        character_display_name,
        dataset_version,
        trigger,
        class_name,
        image_urls,
        caption_lines,
        dataset_zip_url,
        mask_urls,
        target_resolution,
    ):
        character_id = _safe_name(character_id, "character")
        dataset_version = _safe_name(dataset_version, "v001")
        dataset_dir = _factory_root() / "datasets" / "characters" / f"{character_id}_{dataset_version}"
        images_dir = dataset_dir / "images"
        masks_dir = dataset_dir / "masks"
        images_dir.mkdir(parents=True, exist_ok=True)
        masks_dir.mkdir(parents=True, exist_ok=True)

        if str(dataset_zip_url or "").strip():
            archive_path = dataset_dir / "source.zip"
            _download(str(dataset_zip_url).strip(), archive_path)
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(dataset_dir / "source")
            source_images = [
                p for p in (dataset_dir / "source").rglob("*")
                if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")
            ]
            for idx, src in enumerate(sorted(source_images), start=1):
                shutil.copy2(src, images_dir / f"{idx:04d}{src.suffix.lower()}")

        urls = _split_lines(image_urls)
        captions = _split_lines(caption_lines)
        for idx, url in enumerate(urls, start=1):
            suffix = Path(url.split("?", 1)[0]).suffix.lower()
            if suffix not in (".png", ".jpg", ".jpeg", ".webp"):
                suffix = ".png"
            target = images_dir / f"{idx:04d}{suffix}"
            if not target.exists():
                _download(url, target)
            caption = captions[idx - 1] if idx - 1 < len(captions) else f"{trigger}, {class_name}"
            (images_dir / f"{idx:04d}.txt").write_text(caption, encoding="utf-8")

        for idx, url in enumerate(_split_lines(mask_urls), start=1):
            suffix = Path(url.split("?", 1)[0]).suffix.lower()
            if suffix not in (".png", ".jpg", ".jpeg", ".webp"):
                suffix = ".png"
            target = masks_dir / f"{idx:04d}{suffix}"
            if not target.exists():
                _download(url, target)

        image_files = sorted(p for p in images_dir.iterdir() if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")) if images_dir.exists() else []
        manifest = {
            "character_id": character_id,
            "character_display_name": character_display_name,
            "dataset_version": dataset_version,
            "trigger": trigger,
            "class_name": class_name,
            "base_model": "FLUX.2 Klein Base 9B",
            "image_count": len(image_files),
            "caption_style": "trigger_first_short_descriptive",
            "has_masks": any(masks_dir.iterdir()) if masks_dir.exists() else False,
            "target_resolution": int(target_resolution),
            "dataset_dir": str(dataset_dir),
            "images_dir": str(images_dir),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        manifest_path = dataset_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return (str(manifest_path),)


class FCSFluxKleinLoraTrain:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dataset_manifest": ("STRING", {"default": ""}),
                "backend": (["musubi", "ai-toolkit"], {"default": "musubi"}),
                "base_model": ("STRING", {"default": "FLUX.2 Klein Base 9B"}),
                "output_name": ("STRING", {"default": "character_flux2_klein9b"}),
                "rank": ("INT", {"default": 32, "min": 1, "max": 256}),
                "alpha": ("INT", {"default": 32, "min": 1, "max": 256}),
                "learning_rate": ("FLOAT", {"default": 0.00005, "min": 0.0, "max": 0.01, "step": 0.00001}),
                "steps": ("INT", {"default": 3000, "min": 0, "max": 200000}),
                "save_every_steps": ("INT", {"default": 250, "min": 1, "max": 10000}),
                "batch_size": ("INT", {"default": 1, "min": 1, "max": 16}),
                "gradient_checkpointing": ("BOOLEAN", {"default": True}),
                "fp8_base": ("BOOLEAN", {"default": True}),
                "fp8_text_encoder": ("BOOLEAN", {"default": True}),
                "sample_prompts": ("STRING", {"multiline": True, "default": ""}),
                "dry_run": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("report_json_path", "artifact_zip_path")
    FUNCTION = "train"
    CATEGORY = "Furgen/LoRA"

    def train(
        self,
        dataset_manifest,
        backend,
        base_model,
        output_name,
        rank,
        alpha,
        learning_rate,
        steps,
        save_every_steps,
        batch_size,
        gradient_checkpointing,
        fp8_base,
        fp8_text_encoder,
        sample_prompts,
        dry_run,
    ):
        manifest_path = Path(dataset_manifest)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        run_name = _safe_name(output_name, f"{manifest.get('character_id', 'character')}_flux2_klein9b")
        run_dir = _factory_root() / "training_runs" / run_name
        output_dir = run_dir / "output"
        run_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        config = {
            "backend": backend,
            "base_model": base_model,
            "dataset_manifest": str(manifest_path),
            "output_dir": str(output_dir),
            "output_name": run_name,
            "network": {"type": "lora", "rank": int(rank), "alpha": int(alpha)},
            "train": {
                "resolution": int(manifest.get("target_resolution") or 1024),
                "learning_rate": float(learning_rate),
                "steps": int(steps),
                "save_every_steps": int(save_every_steps),
                "batch_size": int(batch_size),
                "gradient_checkpointing": bool(gradient_checkpointing),
                "precision": "bf16",
                "optimizer": "adamw8bit",
                "fp8_base": bool(fp8_base),
                "fp8_text_encoder": bool(fp8_text_encoder),
            },
            "sample_prompts": _split_lines(sample_prompts),
        }
        config_path = run_dir / "train_config.json"
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

        command = None
        log_path = run_dir / "train.log"
        status = "dry_run" if dry_run or int(steps) == 0 else "completed"
        error = None

        try:
            if not dry_run and int(steps) > 0:
                command = os.environ.get("FCS_LORA_TRAIN_COMMAND", "").strip()
                if command:
                    subprocess.run(
                        command.format(config=str(config_path), run_dir=str(run_dir), output_dir=str(output_dir)),
                        shell=True,
                        check=True,
                        cwd=str(run_dir),
                        stdout=log_path.open("a", encoding="utf-8"),
                        stderr=subprocess.STDOUT,
                    )
                else:
                    raise RuntimeError("FCS_LORA_TRAIN_COMMAND is not configured; run with dry_run=true or set the backend command.")
            else:
                (output_dir / f"{run_name}.dry_run.txt").write_text("LoRA training dry run completed.\n", encoding="utf-8")
        except Exception as exc:
            status = "failed"
            error = str(exc)
            log_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")

        report = {
            "status": status,
            "error": error,
            "run_name": run_name,
            "run_dir": str(run_dir),
            "config_path": str(config_path),
            "dataset_manifest": manifest,
            "backend_command": command,
            "expected_lora_path": str(output_dir / f"{run_name}.safetensors"),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        report_path = run_dir / "report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        artifact_zip = run_dir / f"{run_name}_artifact.zip"
        with zipfile.ZipFile(artifact_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(run_dir.rglob("*")):
                if path == artifact_zip or path.is_dir():
                    continue
                zf.write(path, path.relative_to(run_dir))

        if status == "failed":
            raise RuntimeError(error or "LoRA training failed")

        return (str(report_path), str(artifact_zip))


class FCSSaveArtifact:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "artifact_path": ("STRING", {"default": ""}),
                "filename_prefix": ("STRING", {"default": "lora_gen_v1/artifact"}),
                "extension": ("STRING", {"default": "zip"}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("filename",)
    FUNCTION = "save"
    CATEGORY = "Furgen/LoRA"
    OUTPUT_NODE = True

    def save(self, artifact_path, filename_prefix, extension):
        source = Path(artifact_path)
        if not source.exists():
            raise FileNotFoundError(f"Artifact does not exist: {source}")

        ext = _safe_name(extension, source.suffix.lstrip(".") or "dat").lstrip(".")
        output_root = _comfy_root() / "output"
        rel = Path(str(filename_prefix).strip().lstrip("/"))
        if rel.suffix:
            rel = rel.with_suffix("")
        dest = output_root / rel.with_suffix(f".{ext}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
        return (str(dest.relative_to(output_root)).replace(os.sep, "/"),)


NODE_CLASS_MAPPINGS = {
    "FCSLoraDatasetPrepare": FCSLoraDatasetPrepare,
    "FCSFluxKleinLoraTrain": FCSFluxKleinLoraTrain,
    "FCSSaveArtifact": FCSSaveArtifact,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FCSLoraDatasetPrepare": "Prepare LoRA Dataset (FCS)",
    "FCSFluxKleinLoraTrain": "Train FLUX.2 Klein LoRA (FCS)",
    "FCSSaveArtifact": "Save Artifact (FCS)",
}

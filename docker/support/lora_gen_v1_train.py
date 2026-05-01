#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def quote(value: object) -> str:
    return shlex.quote(str(value))


def find_existing(paths, label: str) -> Path:
    for path in paths:
        if path is None:
            continue
        if path.is_file():
            return path
    fail(f"Missing {label}. Checked: {', '.join(str(path) for path in paths)}")


def env_path(name: str):
    value = os.environ.get(name, "").strip()
    return Path(value) if value else None


def write_dataset_toml(path: Path, manifest: dict, train: dict) -> None:
    images_dir = Path(manifest.get("images_dir") or "")
    if not images_dir.exists():
        fail(f"Dataset image directory does not exist: {images_dir}")
    images = sorted(p for p in images_dir.iterdir() if p.suffix.lower() in IMAGE_SUFFIXES)
    if not images:
        fail(f"Dataset image directory has no images: {images_dir}")

    resolution = int(train.get("resolution") or manifest.get("target_resolution") or 1024)
    batch_size = int(train.get("batch_size") or 1)
    cache_dir = path.parent / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    path.write_text(
        "\n".join(
            [
                "[general]",
                f"resolution = [{resolution}, {resolution}]",
                'caption_extension = ".txt"',
                f"batch_size = {batch_size}",
                "enable_bucket = true",
                "bucket_no_upscale = false",
                "",
                "[[datasets]]",
                f"image_directory = {json.dumps(str(images_dir))}",
                f"cache_directory = {json.dumps(str(cache_dir))}",
                "num_repeats = 1",
                "",
            ]
        ),
        encoding="utf-8",
    )


def build_commands(config_path: Path) -> list[list[str]]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    manifest_path = Path(config.get("dataset_manifest") or "")
    if not manifest_path.exists():
        fail(f"Dataset manifest does not exist: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    workspace = Path(os.environ.get("WORKSPACE") or "/workspace")
    comfy_dir = Path(os.environ.get("DM_COMFYUI_DIR") or os.environ.get("COMFYUI_DIR") or workspace / "ComfyUI")
    musubi_dir = Path(os.environ.get("LORA_GEN_V1_MUSUBI_DIR") or workspace / "training_backends" / "musubi-tuner")
    if not musubi_dir.exists():
        fail(f"Musubi Tuner directory does not exist: {musubi_dir}")

    train_script = find_existing(
        [musubi_dir / "src" / "musubi_tuner" / "flux_2_train_network.py"],
        "Musubi FLUX.2 train script",
    )
    cache_latents_script = find_existing(
        [musubi_dir / "src" / "musubi_tuner" / "flux_2_cache_latents.py"],
        "Musubi FLUX.2 latent cache script",
    )
    cache_text_script = find_existing(
        [musubi_dir / "src" / "musubi_tuner" / "flux_2_cache_text_encoder_outputs.py"],
        "Musubi FLUX.2 text encoder cache script",
    )
    dit = find_existing(
        [
            env_path("LORA_GEN_V1_DIT_PATH"),
            comfy_dir / "models" / "diffusion_models" / "flux-2-klein-9b-kv.safetensors",
        ],
        "FLUX.2 Klein 9B KV DiT checkpoint for training",
    )
    vae = find_existing(
        [
            env_path("LORA_GEN_V1_VAE_PATH"),
            comfy_dir / "models" / "vae" / "ae.safetensors",
            comfy_dir / "models" / "vae" / "flux2-vae.safetensors",
        ],
        "FLUX.2 VAE checkpoint",
    )
    text_encoder = find_existing(
        [
            env_path("LORA_GEN_V1_TEXT_ENCODER_PATH"),
            comfy_dir / "models" / "text_encoders" / "flux2-klein-9b-kv" / "model-00001-of-00004.safetensors",
            comfy_dir / "models" / "text_encoders" / "model-00001-of-00004.safetensors",
            comfy_dir / "models" / "text_encoders" / "qwen_3_8b.safetensors",
        ],
        "Qwen3 8B text encoder checkpoint",
    )

    train = config.get("train") or {}
    network = config.get("network") or {}
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_toml = config_path.parent / "musubi_dataset.toml"
    write_dataset_toml(dataset_toml, manifest, train)

    model_version = os.environ.get("LORA_GEN_V1_MODEL_VERSION") or "klein-9b"
    steps = int(train.get("steps") or 0)
    save_every_steps = max(1, int(train.get("save_every_steps") or steps or 1))
    rank = int(network.get("rank") or 32)
    alpha = int(network.get("alpha") or rank)
    learning_rate = float(train.get("learning_rate") or 5e-5)

    latent_cache_command = [
        sys.executable,
        str(cache_latents_script),
        "--dataset_config",
        str(dataset_toml),
        "--vae",
        str(vae),
        "--model_version",
        model_version,
        "--vae_dtype",
        "bfloat16",
    ]
    text_cache_command = [
        sys.executable,
        str(cache_text_script),
        "--dataset_config",
        str(dataset_toml),
        "--text_encoder",
        str(text_encoder),
        "--batch_size",
        str(int(os.environ.get("LORA_GEN_V1_TEXT_CACHE_BATCH_SIZE", "8"))),
        "--model_version",
        model_version,
    ]
    if train.get("fp8_text_encoder", True):
        text_cache_command.append("--fp8_text_encoder")

    train_command = [
        "accelerate",
        "launch",
        "--num_cpu_threads_per_process",
        "1",
        "--mixed_precision",
        "bf16",
        str(train_script),
        "--model_version",
        model_version,
        "--dit",
        str(dit),
        "--vae",
        str(vae),
        "--text_encoder",
        str(text_encoder),
        "--dataset_config",
        str(dataset_toml),
        "--sdpa",
        "--mixed_precision",
        "bf16",
        "--timestep_sampling",
        "flux2_shift",
        "--weighting_scheme",
        "none",
        "--optimizer_type",
        str(train.get("optimizer") or "adamw8bit"),
        "--learning_rate",
        str(learning_rate),
        "--max_data_loader_n_workers",
        "2",
        "--network_module",
        "networks.lora_flux_2",
        "--network_dim",
        str(rank),
        "--network_alpha",
        str(alpha),
        "--max_train_steps",
        str(steps),
        "--save_every_n_steps",
        str(save_every_steps),
        "--seed",
        str(int(config.get("seed") or 42)),
        "--output_dir",
        str(output_dir),
        "--output_name",
        str(config["output_name"]),
    ]

    if train.get("gradient_checkpointing", True):
        train_command.append("--gradient_checkpointing")
    if train.get("fp8_base", True):
        train_command.extend(["--fp8_base", "--fp8_scaled"])
    if train.get("fp8_text_encoder", True):
        train_command.append("--fp8_text_encoder")
    blocks_to_swap = os.environ.get("LORA_GEN_V1_BLOCKS_TO_SWAP", "").strip()
    if blocks_to_swap:
        train_command.extend(["--blocks_to_swap", blocks_to_swap])

    return [latent_cache_command, text_cache_command, train_command]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FLUX.2 Klein LoRA training for FurgenContentServer.")
    parser.add_argument("--config", required=True, help="Path to FCSFluxKleinLoraTrain train_config.json.")
    args = parser.parse_args()

    commands = build_commands(Path(args.config))
    for command in commands:
        print("Running LoRA command:")
        print(" ".join(quote(part) for part in command), flush=True)
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()

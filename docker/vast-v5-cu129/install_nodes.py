"""Build-time installer for the immutable v5 H3/general node contract."""
import json
import os
import pathlib
import re
import shutil
import subprocess


ROOT = pathlib.Path("/opt/furgen/v5")
COMFY = ROOT / "ComfyUI"
PYTHON = "/venv/main/bin/python"
commit = os.environ["FURGENPUB_COMMIT"]
if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
    raise ValueError("FURGENPUB_COMMIT must be an immutable lowercase SHA")
template = (ROOT / "bundles.template.json").read_text()
bundles = json.loads(template.replace("__FURGENPUB_COMMIT__", commit))
(ROOT / "bundles.json").write_text(json.dumps(bundles, indent=2, sort_keys=True) + "\n")
seen = set()


def run(*args):
    subprocess.run(args, check=True)


for bundle in bundles.values():
    for step in bundle["steps"] + bundle.get("compatibility", {}).get("steps", []):
        key = json.dumps(step, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        kind = step["type"]
        if kind == "git_custom_node":
            if re.fullmatch(r"[0-9a-f]{40}", step["ref"]) is None:
                raise ValueError(f"Unpinned node repository: {step['repository']}")
            target = COMFY / "custom_nodes" / step["directoryName"]
            run("git", "clone", "--filter=blob:none", step["repository"], str(target))
            run("git", "-C", str(target), "checkout", step["ref"])
            run("git", "-C", str(target), "submodule", "update", "--init", "--recursive")
            requirements = target / "requirements.txt"
            if step.get("installRequirements", True) and requirements.exists():
                run(PYTHON, "-m", "pip", "install", "--no-cache-dir", *step.get("pipArgs", []), "-r", str(requirements))
        elif kind == "furgen_support_custom_node":
            source = ROOT / step["packageName"]
            target = COMFY / "custom_nodes" / step["packageName"]
            shutil.copytree(source, target)
        elif kind in {"pip_install", "python_import_check"}:
            # Runtime pins/import checks are applied once in the final Docker layer.
            continue
        else:
            raise ValueError(f"Unsupported build step: {kind}")

# Reuse the production singleton watchdog/bootstrap implementation, but seed
# its first agent binary directly from this immutable image.
support = (ROOT / "video_gen_v3.sh").read_text()
start = support.index("function dependency_manager_is_disabled()")
end = support.index("# Start the dependency manager agent (best-effort;")
bootstrap = ROOT / "agent_bootstrap.sh"
bootstrap.write_text("#!/bin/bash\nset -e\n" + support[start:end] + "\ndependency_manager_start_agent\n")
bootstrap.chmod(0o755)
run("bash", "-n", str(bootstrap))

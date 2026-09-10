import json
from pathlib import Path

class FCSExposeCompositionTiming:
    RETURN_TYPES = ()
    FUNCTION = "expose"
    OUTPUT_NODE = True
    CATEGORY = "video/precision"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"filenames": ("VHS_FILENAMES",)}}

    def expose(self, filenames):
        import folder_paths

        saved, paths = filenames
        root = Path(folder_paths.get_output_directory() if saved else folder_paths.get_temp_directory()).resolve()
        matches = [Path(path).resolve() for path in paths if str(path).endswith("-timing.json")]
        if len(matches) != 1:
            raise ValueError("Expected exactly one executed composition timing artifact")
        path = matches[0]
        relative = path.relative_to(root)
        timing = json.loads(path.read_text(encoding="utf-8"))
        if timing.get("version") != 1 or timing.get("mode") != "global-grid":
            raise ValueError("Unsupported composition timing artifact")
        return {"ui": {"files": [{"filename": relative.name,
                "subfolder": str(relative.parent) if relative.parent != Path(".") else "",
                "type": "output" if saved else "temp"}],
                "compositionTiming": [timing]}, "result": ()}


NODE_CLASS_MAPPINGS = {"FCSExposeCompositionTiming": FCSExposeCompositionTiming}

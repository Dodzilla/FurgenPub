import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


def load_node(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "folder_paths", types.SimpleNamespace(
        get_output_directory=lambda: str(tmp_path), get_temp_directory=lambda: str(tmp_path / "temp")))
    root = Path(__file__).parents[3]
    spec = importlib.util.spec_from_file_location("timing_artifact_package", root / "__init__.py",
                                               submodule_search_locations=[str(root)])
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module.NODE_CLASS_MAPPINGS["FCSExposeCompositionTiming"]()


@pytest.mark.parametrize("saved", [True, False])
def test_timing_artifact_is_discoverable_in_comfy_history(tmp_path, monkeypatch, saved):
    node = load_node(tmp_path, monkeypatch)
    root = tmp_path if saved else tmp_path / "temp"
    path = root / "nested" / "export-timing.json"
    path.parent.mkdir(parents=True)
    ledger = {"version": 1, "mode": "global-grid", "totalFrames": 310}
    path.write_text(json.dumps(ledger))
    result = node.expose((saved, [str(root / "export.mp4"), str(path)]))
    ref = result["ui"]["files"][0]
    assert ref["type"] == ("output" if saved else "temp")
    assert json.loads((root / ref["subfolder"] / ref["filename"]).read_text()) == ledger
    assert result["ui"]["compositionTiming"] == [ledger]
    assert result["result"] == ()


@pytest.mark.parametrize("case", ["missing", "duplicate", "outside", "invalid"])
def test_timing_artifact_requires_one_valid_local_receipt(tmp_path, monkeypatch, case):
    node = load_node(tmp_path, monkeypatch)
    path = tmp_path / "export-timing.json"
    path.write_text(json.dumps({"version": 1, "mode": "legacy" if case == "invalid" else "global-grid"}))
    paths = {"missing": [], "duplicate": [str(path)] * 2,
             "outside": [str(tmp_path.parent / "outside-timing.json")], "invalid": [str(path)]}[case]
    with pytest.raises(ValueError):
        node.expose((True, paths))

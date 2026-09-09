import ast
from pathlib import Path
import pytest

# This transformation operates on metadata; no GPU/model imports are needed.
source = Path(__file__).parents[1] / "custom_nodes/FurgenVideoTools/furgen_video_tools.py"
tree = ast.parse(source.read_text())
node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "FurgenReferenceLatentPolicy")
namespace = {}
exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
Policy = namespace["FurgenReferenceLatentPolicy"]

@pytest.mark.parametrize("policy,kinds", [("encoder_only", []), ("images_only", ["image"]), ("full", ["image", "video_audio"])])
def test_keeps_encoder_tokens_guides_and_original_metadata(policy, kinds):
    tokens, keyframes, masks = object(), object(), object()
    refs = [{"kind": "image", "latent": object()}, {"kind": "video_audio", "latent": object()}]
    metadata = {"minimax_refs": refs, "minimax_keyframes": keyframes, "modality_tags": masks}
    output = Policy().apply([[tokens, metadata]], policy)[0]
    assert output[0][0] is tokens
    assert output[0][1]["minimax_keyframes"] is keyframes
    assert output[0][1]["modality_tags"] is masks
    assert [r["kind"] for r in output[0][1].get("minimax_refs", [])] == kinds
    assert metadata["minimax_refs"] is refs and len(refs) == 2

@pytest.mark.parametrize("conditioning,policy", [([[object(), {}]], "encoder_only"), ([[object(), {"minimax_refs": "bad"}]], "full"), ([], "unknown")])
def test_unknown_contracts_fail_loudly(conditioning, policy):
    with pytest.raises(ValueError):
        Policy().apply(conditioning, policy)

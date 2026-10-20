"""Bake the existing antelopev2 model pack into the Modal vision image."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

MODEL_NAME = "antelopev2"
MODEL_URL = f"https://github.com/deepinsight/insightface/releases/download/v0.7/{MODEL_NAME}.zip"
# Official v0.7 archive, verified from the production worker's downloaded pack.
MODEL_SHA256 = "8e182f14fc6e80b3bfa375b33eb6cff7ee05d8ef7633e738d1c89021dcf0c5c5"
MODEL_FILES = frozenset(
    {"genderage.onnx", "2d106det.onnx", "1k3d68.onnx", "glintr100.onnx", "scrfd_10g_bnkps.onnx"}
)


def install_model_archive(archive: Path, root: Path) -> Path:
    with archive.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != MODEL_SHA256:
        raise ValueError(f"InsightFace model archive SHA256 mismatch: {digest}")
    target = root / "models" / MODEL_NAME
    with zipfile.ZipFile(archive) as bundle:
        members = {name: f"{MODEL_NAME}/{name}" for name in MODEL_FILES}
        missing = set(members.values()) - set(bundle.namelist())
        if missing:
            raise ValueError(f"InsightFace model archive missing files: {sorted(missing)}")
        target.mkdir(parents=True, exist_ok=True)
        # The release ZIP has an extra antelopev2/ directory. FaceAnalysis scans
        # only direct *.onnx children, so extract the named files at that level.
        for name, member in members.items():
            with bundle.open(member) as source, (target / name).open("wb") as destination:
                shutil.copyfileobj(source, destination)
    return target


def main() -> None:
    root = Path.home() / ".insightface"
    with tempfile.TemporaryDirectory(prefix="trr-insightface-") as temporary:
        archive = Path(temporary) / "antelopev2.zip"
        with urllib.request.urlopen(MODEL_URL, timeout=120) as source, archive.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        target = install_model_archive(archive, root)

    from insightface.app import FaceAnalysis

    model = FaceAnalysis(name=MODEL_NAME, root=str(root), providers=["CPUExecutionProvider"])
    if not {"detection", "recognition"}.issubset(model.models):
        raise RuntimeError("InsightFace model pack requires detection and recognition")
    model.prepare(ctx_id=-1, det_size=(640, 640))
    print(f"Validated InsightFace {MODEL_NAME}: {len(MODEL_FILES)} models in {target}")


if __name__ == "__main__":
    main()

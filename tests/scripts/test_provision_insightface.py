import hashlib
import zipfile

import pytest

from scripts.modal import provision_insightface as provision


def _archive(tmp_path, monkeypatch, *, missing=None):
    archive = tmp_path / "models.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for name in provision.MODEL_FILES:
            if name == missing:
                continue
            bundle.writestr(f"antelopev2/{name}", b"model-fixture")
    monkeypatch.setattr(provision, "MODEL_SHA256", hashlib.sha256(archive.read_bytes()).hexdigest())
    return archive


def test_nested_release_pack_installs_directly_in_profile_directory(tmp_path, monkeypatch):
    archive = _archive(tmp_path, monkeypatch)
    target = provision.install_model_archive(archive, tmp_path / "root")
    assert {path.name for path in target.glob("*.onnx")} == provision.MODEL_FILES
    assert not (target / "antelopev2").exists()


def test_bad_digest_does_not_create_model_directory(tmp_path, monkeypatch):
    archive = _archive(tmp_path, monkeypatch)
    monkeypatch.setattr(provision, "MODEL_SHA256", "invalid")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        provision.install_model_archive(archive, tmp_path / "root")
    assert not (tmp_path / "root").exists()


def test_missing_detection_model_fails_before_install(tmp_path, monkeypatch):
    archive = _archive(tmp_path, monkeypatch, missing="scrfd_10g_bnkps.onnx")
    with pytest.raises(ValueError, match="missing files"):
        provision.install_model_archive(archive, tmp_path / "root")
    assert not (tmp_path / "root").exists()

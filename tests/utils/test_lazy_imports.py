from __future__ import annotations

from types import SimpleNamespace

from trr_backend.utils import lazy_imports


def test_lazy_module_imports_only_on_first_attribute_access(monkeypatch) -> None:
    calls: list[str] = []
    module = SimpleNamespace(value="loaded", other="cached")

    def fake_import_module(module_name: str):
        calls.append(module_name)
        return module

    monkeypatch.setattr(lazy_imports.importlib, "import_module", fake_import_module)

    lazy_module = lazy_imports.LazyModule("scripts.sync.sync_shows")

    assert calls == []
    assert lazy_module.value == "loaded"
    assert calls == ["scripts.sync.sync_shows"]
    assert lazy_module.other == "cached"
    assert calls == ["scripts.sync.sync_shows"]

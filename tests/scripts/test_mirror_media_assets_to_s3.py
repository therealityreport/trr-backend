from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import scripts.media.mirror_media_assets_to_s3 as mod


def test_main_dry_run_with_assets_does_not_require_storage_configuration(
    monkeypatch,
    capsys,
) -> None:
    args = SimpleNamespace(
        source="all",
        status="pending",
        limit=10,
        dry_run=True,
        verbose=False,
        batch_size=10,
        max_retries=3,
        retry_backoff_hours=1.0,
        concurrency=1,
    )
    process_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(mod, "_parse_args", lambda _argv: args)
    monkeypatch.setattr(mod, "load_env_and_db", lambda: object())
    monkeypatch.setattr(
        mod,
        "load_object_storage_config",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dry-run must not load storage configuration")),
    )
    monkeypatch.setattr(
        mod,
        "fetch_assets_for_mirroring",
        lambda *_args, **_kwargs: [{"id": "asset-1", "source_url": "https://image.tmdb.org/image.jpg"}],
    )

    def fake_process_batch(*_args: Any, **kwargs: Any) -> mod.MirrorSummary:
        process_calls.append(kwargs)
        return mod.MirrorSummary(total=1, hosted=1)

    monkeypatch.setattr(mod, "process_batch", fake_process_batch)

    assert mod.main([]) == 0
    assert process_calls == [
        {
            "s3_client": None,
            "bucket": "",
            "cdn_base_url": "",
            "max_retries": 3,
            "backoff_hours": 1.0,
            "concurrency": 1,
            "dry_run": True,
            "verbose": False,
        }
    ]
    assert "DRY RUN: No changes will be made." in capsys.readouterr().out

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.socials.backfill_social_media_mirror_jobs as mod


def test_main_fails_fast_when_s3_preflight_fails(monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            weeks=8,
            platforms="instagram,tiktok,youtube,twitter",
            source_scope="bravo",
            limit_per_platform=5000,
            failed_only=False,
        ),
    )

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: AWS_S3_BUCKET")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_preflight)

    with pytest.raises(SystemExit, match="Social media mirror S3 preflight failed"):
        mod.main()

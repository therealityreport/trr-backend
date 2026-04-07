from __future__ import annotations

from types import SimpleNamespace

from scripts.socials import remediate_instagram_backfill_strategy_drift as cli


def test_main_applies_workspace_runtime_env_before_loading_social_repo(
    monkeypatch,
    capsys,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        cli,
        "apply_workspace_runtime_env",
        lambda *, repo_root, environ=None: calls.append("env") or {},
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            account="bravotv",
            source_scope="bravo",
            run_id=None,
            limit=25,
            execute=False,
            requeue_canary=False,
            include_gap_analysis=False,
            cancelled_by=None,
            initiated_by=None,
            pretty=False,
        ),
    )

    def _social_repo():
        calls.append("repo")
        return SimpleNamespace(
            SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE="backfill",
            CATALOG_FULL_HISTORY_FRONTIER_STRATEGY="newest_first_frontier",
            _normalize_social_account_profile_handle=lambda handle: handle.lower(),
            pg=SimpleNamespace(fetch_all=lambda *args, **kwargs: []),
        )

    monkeypatch.setattr(cli, "_social_repo", _social_repo)

    assert cli.main() == 0
    assert calls[0] == "env"
    assert calls[1] == "repo"
    assert '"candidate_job_count": 0' in capsys.readouterr().out

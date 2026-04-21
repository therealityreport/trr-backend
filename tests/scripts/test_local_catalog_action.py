from __future__ import annotations

from types import SimpleNamespace

from scripts.socials import local_catalog_action as cli


def test_parse_args_defaults() -> None:
    args = cli.parse_args(["--platform", "twitter", "--account", "bravotv", "--action", "backfill"])

    assert args.platform == "twitter"
    assert args.account == "bravotv"
    assert args.source_scope == "bravo"
    assert args.action == "backfill"


def test_main_dispatches_backfill(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="twitter",
            account="bravotv",
            source_scope="bravo",
            action="backfill",
        ),
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(start_social_account_catalog_backfill=lambda *args, **kwargs: {"run_id": "run-backfill-1"}),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: {
                "run_id": run_id,
                "status": "completed",
            }
        ),
    )

    assert cli.main() == 0
    assert "run-backfill-1" in capsys.readouterr().out


def test_main_dispatches_sync_newer(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="threads",
            account="bravotv",
            source_scope="bravo",
            action="sync_newer",
        ),
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(sync_newer_social_account_catalog=lambda *args, **kwargs: {"run_id": "run-sync-newer-1"}),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: {
                "run_id": run_id,
                "status": "completed",
            }
        ),
    )

    assert cli.main() == 0
    assert "run-sync-newer-1" in capsys.readouterr().out


def test_main_fill_missing_posts_dispatches_sync_newer(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="tiktok",
            account="bravotv",
            source_scope="bravo",
            action="fill_missing_posts",
        ),
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            get_social_account_catalog_gap_analysis=lambda *args, **kwargs: {"recommended_action": "sync_newer"},
            sync_newer_social_account_catalog=lambda *args, **kwargs: {"run_id": "run-head-gap-1"},
        ),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: {
                "run_id": run_id,
                "status": "completed",
            }
        ),
    )

    assert cli.main() == 0
    assert "run-head-gap-1" in capsys.readouterr().out


def test_main_fill_missing_posts_dispatches_backfill_for_tail_gap(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="instagram",
            account="bravotv",
            source_scope="bravo",
            action="fill_missing_posts",
        ),
    )

    captured: dict[str, object] = {}

    def _start_backfill(*args, **kwargs):
        captured.update(kwargs)
        return {"run_id": "run-tail-gap-1"}

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            get_social_account_catalog_gap_analysis=lambda *args, **kwargs: {"recommended_action": "backfill_posts"},
            start_social_account_catalog_backfill=_start_backfill,
        ),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: {
                "run_id": run_id,
                "status": "completed",
            }
        ),
    )

    assert cli.main() == 0
    assert captured["catalog_action_scope"] == "full_history"
    assert "run-tail-gap-1" in capsys.readouterr().out


def test_main_fill_missing_posts_dispatches_bounded_window_backfill(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="facebook",
            account="bravotv",
            source_scope="bravo",
            action="fill_missing_posts",
        ),
    )

    captured: dict[str, object] = {}

    def _start_backfill(*args, **kwargs):
        captured.update(kwargs)
        return {"run_id": "run-window-gap-1"}

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            get_social_account_catalog_gap_analysis=lambda *args, **kwargs: {
                "recommended_action": "bounded_window_backfill",
                "repair_window_start": "2026-04-01T00:00:00Z",
                "repair_window_end": "2026-04-03T23:59:59Z",
            },
            start_social_account_catalog_backfill=_start_backfill,
        ),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: {
                "run_id": run_id,
                "status": "completed",
            }
        ),
    )

    assert cli.main() == 0
    assert captured["catalog_action_scope"] == "bounded_window"
    assert captured["date_start"] == "2026-04-01T00:00:00Z"
    assert captured["date_end"] == "2026-04-03T23:59:59Z"
    assert "run-window-gap-1" in capsys.readouterr().out


def test_main_fill_missing_posts_exits_nonzero_for_complete_state(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="youtube",
            account="bravo",
            source_scope="bravo",
            action="fill_missing_posts",
        ),
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(get_social_account_catalog_gap_analysis=lambda *args, **kwargs: {"recommended_action": "none"}),
    )

    assert cli.main() == 1
    assert "No missing posts to fill right now." in capsys.readouterr().err

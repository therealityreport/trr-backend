from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.socials import local_catalog_action as cli


@pytest.fixture(autouse=True)
def _clear_local_catalog_pool_env(monkeypatch):
    for key in cli.LOCAL_CATALOG_DB_POOL_DEFAULTS:
        monkeypatch.delenv(key, raising=False)


def test_apply_local_catalog_db_pool_defaults_preserves_explicit_overrides(monkeypatch) -> None:
    monkeypatch.setenv("TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN", "8")

    applied = cli.apply_local_catalog_db_pool_defaults()

    assert applied == {
        "TRR_DB_POOL_MAXCONN": "4",
        "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN": "4",
        "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN": "4",
    }
    assert cli.os.environ["TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN"] == "8"


def test_parse_args_defaults() -> None:
    args = cli.parse_args(["--platform", "twitter", "--account", "bravotv", "--action", "backfill"])

    assert args.platform == "twitter"
    assert args.account == "bravotv"
    assert args.source_scope == "network"
    assert args.action == "backfill"
    assert args.selected_tasks == []
    assert args.dry_run is False
    assert args.date_start is None
    assert args.date_end is None
    assert args.confirm_bravotv_instagram_backfill == ""


def test_parse_args_accepts_legacy_bravo_source_scope() -> None:
    args = cli.parse_args(
        [
            "--platform",
            "twitter",
            "--account",
            "bravotv",
            "--source-scope",
            "bravo",
            "--action",
            "backfill",
        ]
    )

    assert args.source_scope == "bravo"


def test_parse_args_accepts_selected_tasks() -> None:
    args = cli.parse_args(
        [
            "--platform",
            "instagram",
            "--account",
            "bravotv",
            "--action",
            "backfill",
            "--selected-task",
            "post_details",
            "--selected-task",
            "comments",
            "--selected-task",
            "media",
        ]
    )

    assert args.selected_tasks == ["post_details", "comments", "media"]


def test_parse_args_accepts_dry_run_and_bravotv_confirmation() -> None:
    args = cli.parse_args(
        [
            "--platform",
            "instagram",
            "--account",
            "bravotv",
            "--action",
            "backfill",
            "--dry-run",
            "--confirm-bravotv-instagram-backfill",
            cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION,
        ]
    )

    assert args.dry_run is True
    assert args.confirm_bravotv_instagram_backfill == cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION


def test_parse_args_accepts_comment_anchor_source_ids() -> None:
    args = cli.parse_args(
        [
            "--platform",
            "twitter",
            "--account",
            "TheTraitorsUS",
            "--action",
            "backfill",
            "--selected-task",
            "comments",
            "--comment-anchor-source-id",
            "2015151653134172554",
            "--comment-anchor-source-id",
            "2014150059768652195",
        ]
    )

    assert args.comment_anchor_source_ids == ["2015151653134172554", "2014150059768652195"]


def test_parse_args_requires_complete_date_window() -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(
            [
                "--platform",
                "twitter",
                "--account",
                "bravotv",
                "--action",
                "backfill",
                "--date-start",
                "2026-01-01T00:00:00Z",
            ]
        )


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


def test_main_dispatches_selected_task_backfill_through_launch_orchestrator(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="instagram",
            account="bravotv",
            source_scope="bravo",
            action="backfill",
            selected_tasks=["post_details", "comments", "media"],
            confirm_bravotv_instagram_backfill=cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION,
        ),
    )

    captured: dict[str, object] = {}

    def _launch(*args, **kwargs):
        captured.update(kwargs)
        return {
            "run_id": "catalog-run-1",
            "catalog_run_id": "catalog-run-1",
            "comments_run_id": "comments-run-1",
            "status": "queued",
        }

    executed: list[tuple[str, str]] = []

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(launch_social_account_catalog_backfill=_launch),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: executed.append(
                (run_id, kwargs["worker_id"])
            )
        ),
    )

    assert cli.main() == 0
    assert captured["selected_tasks"] == ["post_details", "comments", "media"]
    assert captured["allow_local_dev_inline_bypass"] is True
    assert executed == [
        ("catalog-run-1", "local-script:catalog:instagram:1"),
        ("comments-run-1", "local-script:catalog:instagram:2"),
    ]
    captured_output = capsys.readouterr()
    assert "catalog-run-1" in captured_output.out
    assert "local_catalog_db_pool_defaults" in captured_output.err


def test_main_dry_run_prints_plan_without_loading_runtime(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no env")))
    monkeypatch.setattr(
        cli,
        "apply_workspace_runtime_env",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("no runtime env")),
    )

    assert (
        cli.main(
            [
                "--platform",
                "instagram",
                "--account",
                "bravotv",
                "--source-scope",
                "network",
                "--action",
                "backfill",
                "--selected-task",
                "post_details",
                "--selected-task",
                "comments",
                "--selected-task",
                "media",
                "--date-start",
                "2026-01-01T00:00:00Z",
                "--date-end",
                "2026-12-31T23:59:59Z",
                "--dry-run",
            ]
        )
        == 0
    )

    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "dry_run"
    assert payload["would_launch"] is False
    assert payload["confirmation_required"] is True
    assert payload["required_confirmation"] == cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION
    assert payload["selected_tasks"] == ["post_details", "comments", "media"]
    assert payload["date_start"] == "2026-01-01T00:00:00Z"
    assert payload["date_end"] == "2026-12-31T23:59:59Z"
    assert payload["catalog_action_scope"] == "bounded_window"


def test_main_blocks_bravotv_instagram_backfill_without_confirmation(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no env")))
    monkeypatch.setattr(
        cli,
        "apply_workspace_runtime_env",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("no runtime env")),
    )

    assert (
        cli.main(
            [
                "--platform",
                "instagram",
                "--account",
                "bravotv",
                "--action",
                "backfill",
                "--selected-task",
                "post_details",
                "--selected-task",
                "comments",
                "--selected-task",
                "media",
            ]
        )
        == 2
    )

    payload = __import__("json").loads(capsys.readouterr().err)
    assert payload["reason"] == "bravotv_instagram_backfill_confirmation_required"
    assert payload["required_confirmation"] == cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION


def test_main_cancels_started_run_on_keyboard_interrupt(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="instagram",
            account="bravotv",
            source_scope="network",
            action="backfill",
            selected_tasks=["post_details"],
            comment_anchor_source_ids=[],
            confirm_bravotv_instagram_backfill=cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION,
        ),
    )

    cancelled: list[dict[str, object]] = []

    def _cancel(**kwargs):
        cancelled.append(kwargs)
        return {"run_id": kwargs["run_id"], "status": "cancelled"}

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            launch_social_account_catalog_backfill=lambda *args, **kwargs: {
                "platform": "instagram",
                "catalog_run_id": "catalog-run-1",
                "status": "queued",
            },
            cancel_social_account_catalog_run=_cancel,
        ),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt()
            )
        ),
    )

    assert cli.main() == 130
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "interrupted"
    assert payload["interrupted_cleanup"]["cancelled_run_ids"] == ["catalog-run-1"]
    assert cancelled[0]["platform"] == "instagram"
    assert cancelled[0]["account_handle"] == "bravotv"
    assert cancelled[0]["cancelled_by"] == "local-script:local_catalog_action.py:interrupted"


def test_main_prints_blocked_launch_payload_without_run_id(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="instagram",
            account="bravotv",
            source_scope="network",
            action="backfill",
            selected_tasks=["post_details"],
            comment_anchor_source_ids=[],
            confirm_bravotv_instagram_backfill=cli.BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION,
        ),
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            launch_social_account_catalog_backfill=lambda *args, **kwargs: {
                "status": "blocked_auth",
                "auth_repair_reason": "checkpoint_required",
            }
        ),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(execute_run_with_inline_worker_registration=lambda *_args, **_kwargs: None),
    )

    assert cli.main() == 1
    out = capsys.readouterr().out
    assert '"executed_run_ids": []' in out
    assert '"status": "blocked_auth"' in out
    assert '"auth_repair_reason": "checkpoint_required"' in out


def test_main_dispatches_targeted_twitter_comment_backfill(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="twitter",
            account="TheTraitorsUS",
            source_scope="network",
            action="backfill",
            selected_tasks=["comments"],
            comment_anchor_source_ids=["2015151653134172554"],
        ),
    )

    captured: dict[str, object] = {}

    def _launch(*args, **kwargs):
        captured.update(kwargs)
        return {
            "run_id": "catalog-run-twitter-comments",
            "platform": "twitter",
            "status": "queued",
        }

    execute_calls: list[dict[str, object]] = []

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(launch_social_account_catalog_backfill=_launch),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.socials.control_plane",
        SimpleNamespace(
            execute_run_with_inline_worker_registration=lambda run_id, **kwargs: (
                execute_calls.append({"run_id": run_id, **kwargs})
                or {
                    "run_id": run_id,
                    "status": "completed",
                }
            )
        ),
    )

    assert cli.main() == 0
    assert captured["selected_tasks"] == ["comments"]
    assert captured["comment_anchor_source_ids"] == {"twitter": ["2015151653134172554"]}
    assert execute_calls[0]["platform"] == "twitter"
    assert execute_calls[0]["supported_platforms"] == ["twitter"]
    assert "catalog-run-twitter-comments" in capsys.readouterr().out


def test_main_dispatches_bounded_selected_task_backfill(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "apply_workspace_runtime_env", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="twitter",
            account="bravotv",
            source_scope="network",
            action="backfill",
            selected_tasks=["post_details"],
            comment_anchor_source_ids=[],
            date_start="2026-01-01T00:00:00Z",
            date_end="2026-12-31T23:59:59Z",
            confirm_bravotv_instagram_backfill="",
        ),
    )

    captured: dict[str, object] = {}

    def _launch(*args, **kwargs):
        captured.update(kwargs)
        return {"run_id": "twitter-window-run-1", "catalog_run_id": "twitter-window-run-1"}

    monkeypatch.setitem(
        __import__("sys").modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(launch_social_account_catalog_backfill=_launch),
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
    assert captured["date_start"] == "2026-01-01T00:00:00Z"
    assert captured["date_end"] == "2026-12-31T23:59:59Z"
    assert captured["selected_tasks"] == ["post_details"]
    assert "twitter-window-run-1" in capsys.readouterr().out


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

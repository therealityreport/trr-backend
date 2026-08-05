from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import trr_backend.socials.instagram.media_mirror as media_mirror
import trr_backend.socials.social_season_analytics_impl as legacy_core
from trr_backend.socials.control_plane import dispatch as control_plane_dispatch

MEDIA_MIRROR_ROOM_NAMES = (
    "_instagram_post_source_urls",
    "_instagram_post_needs_media_mirror",
    "_update_instagram_post_media_mirror_fields",
    "_update_instagram_post_source_media_fields",
    "_enqueue_instagram_media_mirror_job",
    "_run_instagram_media_mirror_stage",
    "requeue_instagram_media_mirror_jobs",
)


@pytest.fixture(autouse=True)
def _configured_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    originals = {name: getattr(legacy_core, name) for name in MEDIA_MIRROR_ROOM_NAMES}
    monkeypatch.setattr(media_mirror, "_LEGACY_NAMESPACE", legacy_core.__dict__)
    monkeypatch.setattr(media_mirror, "_LEGACY_ORIGINALS", originals)


def test_media_mirror_import_does_not_load_legacy_social_modules() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "module = importlib.import_module('trr_backend.socials.instagram.media_mirror')",
            "assert callable(module.requeue_instagram_media_mirror_jobs)",
            "loaded = set(sys.modules) - before",
            "forbidden = {",
            "    'trr_backend.socials.social_season_analytics_impl',",
            "    'trr_backend.repositories.social_season_analytics',",
            "}",
            "assert not (loaded & forbidden), sorted(loaded & forbidden)",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_late_legacy_import_configures_preloaded_media_mirror_leaf() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "leaf = importlib.import_module('trr_backend.socials.instagram.media_mirror')",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "legacy = importlib.import_module(legacy_name)",
            "assert leaf._LEGACY_NAMESPACE is legacy.__dict__",
            "assert leaf._legacy_value('FIELD_UNSET') is legacy.FIELD_UNSET",
            "assert leaf._instagram_post_source_urls({'media_urls': ['a']}) == ('a', ['a'])",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("function_name", MEDIA_MIRROR_ROOM_NAMES)
def test_all_legacy_media_mirror_wrappers_delegate_to_leaf_room(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def replacement(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return function_name

    monkeypatch.setitem(media_mirror._LOCAL_ROOM_FUNCTIONS, function_name, replacement)

    assert getattr(legacy_core, function_name)("arg", marker="value") == function_name
    assert calls == [(("arg",), {"marker": "value"})]


@pytest.mark.parametrize("function_name", MEDIA_MIRROR_ROOM_NAMES)
def test_room_callable_avoids_original_wrapper_recursion_and_honors_live_patches(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_impl = lambda: "local"  # noqa: E731
    replacement = lambda: "patched"  # noqa: E731

    assert media_mirror._room_callable(function_name, local_impl) is local_impl

    monkeypatch.setattr(legacy_core, function_name, replacement)

    assert media_mirror._room_callable(function_name, local_impl) is replacement


def test_public_owner_is_canonical_leaf_not_legacy_wrapper() -> None:
    canonical_owner = control_plane_dispatch.requeue_instagram_media_mirror_jobs

    assert canonical_owner is media_mirror.requeue_instagram_media_mirror_jobs
    assert canonical_owner is not legacy_core.requeue_instagram_media_mirror_jobs


def test_source_url_helpers_honor_live_legacy_patches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Any]] = []

    def as_text_list(value: Any) -> list[str]:
        calls.append(("as_text_list", value))
        return [" A ", "a", "B"]

    def normalize_unique_terms(values: list[str]) -> list[str]:
        calls.append(("normalize_unique_terms", values))
        return ["A", "B"]

    monkeypatch.setattr(legacy_core, "_as_text_list", as_text_list)
    monkeypatch.setattr(legacy_core, "_normalize_unique_terms", normalize_unique_terms)

    assert media_mirror._instagram_post_source_urls({"media_urls": "raw"}) == ("A", ["A", "B"])
    assert calls == [
        ("as_text_list", "raw"),
        ("normalize_unique_terms", [" A ", "a", "B"]),
    ]


def test_needs_mirror_keeps_instagram_platform_and_conn(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = object()
    post_row = {"id": "post-1"}
    calls: list[tuple[str, dict[str, Any], Any]] = []

    def needs_mirror(platform: str, row: dict[str, Any], *, conn: Any | None = None) -> bool:
        calls.append((platform, row, conn))
        return True

    monkeypatch.setattr(legacy_core, "_platform_post_needs_media_mirror", needs_mirror)

    assert media_mirror._instagram_post_needs_media_mirror(post_row, conn=conn) is True
    assert calls == [("instagram", post_row, conn)]


def test_generic_update_translates_omissions_to_live_provider_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = object()
    provider_unset = object()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(legacy_core, "FIELD_UNSET", provider_unset)
    monkeypatch.setattr(
        legacy_core,
        "_update_platform_post_media_mirror_fields",
        lambda **kwargs: calls.append(kwargs),
    )

    media_mirror._update_instagram_post_media_mirror_fields(
        post_id="post-1",
        hosted_thumbnail_url=None,
        hosted_media_urls=provider_unset,
        conn=conn,
    )

    assert calls == [
        {
            "platform": "instagram",
            "post_id": "post-1",
            "hosted_thumbnail_url": None,
            "hosted_media_urls": provider_unset,
            "media_mirror_status": provider_unset,
            "media_mirror_error": provider_unset,
            "media_mirror_attempt_count": provider_unset,
            "media_mirror_last_attempt_at": provider_unset,
            "media_mirror_last_job_id": provider_unset,
            "conn": conn,
        }
    ]


def test_source_field_update_treats_omission_and_provider_sentinel_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = object()
    provider_unset = object()
    column_calls: list[tuple[str, Any]] = []
    cursor_calls: list[Any] = []
    fetch_calls: list[tuple[Any, str, list[Any]]] = []
    cursor = object()

    class CursorContext:
        def __enter__(self) -> Any:
            return cursor

        def __exit__(self, *_args: Any) -> None:
            return None

    class FakePg:
        @staticmethod
        def db_cursor(*, conn: Any | None = None) -> CursorContext:
            cursor_calls.append(conn)
            return CursorContext()

        @staticmethod
        def fetch_one_with_cursor(cur: Any, sql: str, params: list[Any]) -> None:
            fetch_calls.append((cur, sql, params))

    def has_column(column: str, *, conn: Any | None = None) -> bool:
        column_calls.append((column, conn))
        return True

    monkeypatch.setattr(legacy_core, "FIELD_UNSET", provider_unset)
    monkeypatch.setattr(legacy_core, "pg", FakePg)
    monkeypatch.setattr(legacy_core, "_instagram_posts_has_column", has_column)

    media_mirror._update_instagram_post_source_media_fields(post_id="post-1", conn=conn)
    media_mirror._update_instagram_post_source_media_fields(
        post_id="post-1",
        thumbnail_url=provider_unset,
        media_urls=provider_unset,
        conn=conn,
    )

    assert column_calls == []
    assert cursor_calls == []
    assert fetch_calls == []

    media_mirror._update_instagram_post_source_media_fields(
        post_id="post-1",
        thumbnail_url=None,
        media_urls=["media-1"],
        conn=conn,
    )

    assert column_calls == [("thumbnail_url", conn), ("media_urls", conn)]
    assert cursor_calls == [conn]
    assert fetch_calls[0][0] is cursor
    assert "update social.instagram_posts" in fetch_calls[0][1]
    assert fetch_calls[0][2] == [None, '["media-1"]', "post-1"]


def test_enqueue_keeps_instagram_platform_conn_and_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    context = object()
    conn = object()
    post_row = {"id": "post-1"}
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def enqueue(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return "job-1"

    monkeypatch.setattr(legacy_core, "_enqueue_platform_media_mirror_job", enqueue)

    assert (
        media_mirror._enqueue_instagram_media_mirror_job(
            context,
            run_id="run-1",
            source_scope="network",
            account="bravotv",
            post_row=post_row,
            week_index=3,
            parent_job_id="parent-1",
            conn=conn,
        )
        == "job-1"
    )
    assert calls == [
        (
            (context,),
            {
                "platform": "instagram",
                "run_id": "run-1",
                "source_scope": "network",
                "account": "bravotv",
                "post_row": post_row,
                "week_index": 3,
                "parent_job_id": "parent-1",
                "conn": conn,
            },
        )
    ]


def test_stage_keeps_instagram_platform_and_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    context = object()
    config = {"post_id": "post-1"}
    calls: list[dict[str, Any]] = []
    result = (1, 2, {"status": "complete"})

    def run_stage(**kwargs: Any) -> tuple[int, int, dict[str, Any]]:
        calls.append(kwargs)
        return result

    monkeypatch.setattr(legacy_core, "_run_platform_media_mirror_stage", run_stage)

    assert (
        media_mirror._run_instagram_media_mirror_stage(
            context=context,
            job_id="job-1",
            config=config,
        )
        == result
    )
    assert calls == [
        {
            "context": context,
            "platform": "instagram",
            "job_id": "job-1",
            "config": config,
        }
    ]


def test_requeue_keeps_instagram_platform_pagination_and_window(monkeypatch: pytest.MonkeyPatch) -> None:
    date_start = datetime(2026, 1, 1, tzinfo=UTC)
    date_end = datetime(2026, 2, 1, tzinfo=UTC)
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    result = {"queued_jobs": 4}

    def requeue(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return result

    monkeypatch.setattr(legacy_core, "requeue_media_mirror_jobs", requeue)

    assert (
        media_mirror.requeue_instagram_media_mirror_jobs(
            "season-1",
            source_scope="cast",
            limit=321,
            failed_only=True,
            date_start=date_start,
            date_end=date_end,
        )
        is result
    )
    assert calls == [
        (
            ("season-1",),
            {
                "platform": "instagram",
                "source_scope": "cast",
                "limit": 321,
                "failed_only": True,
                "date_start": date_start,
                "date_end": date_end,
            },
        )
    ]

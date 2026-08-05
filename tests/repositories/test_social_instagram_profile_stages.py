from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import trr_backend.socials.instagram.profile_stages as profile_stages
import trr_backend.socials.social_season_analytics_impl as legacy_core

PROFILE_STAGE_ROOM_NAMES = (
    "_instagram_profile_scraper",
    "_run_instagram_profile_snapshot_stage",
    "_instagram_following_rows_from_payload",
    "_fetch_instagram_following_rows",
    "_run_instagram_profile_following_stage",
    "_instagram_profile_tables_ready",
    "_normalize_instagram_profile_source_scope",
    "_instagram_profile_fetch_one",
    "_instagram_profile_fetch_all",
    "_instagram_profile_execute_one",
    "_instagram_profile_execute",
    "_instagram_profile_parse_about_timestamp",
    "_instagram_profile_domain",
    "_instagram_profile_normalized_url",
    "_instagram_profile_merge_rows",
    "_instagram_profile_existing_row",
    "_sync_instagram_profile_external_links",
    "persist_instagram_profile_snapshot",
    "_instagram_profile_row_for_username",
    "persist_instagram_profile_relationships",
    "_instagram_profile_response",
    "get_instagram_profile_detail",
    "get_instagram_profile_relationships",
)
PROFILE_STAGE_PROVIDER_NAMES = (
    "SharedStageRuntimeError",
    "_adapt_payload_json_values",
    "_coerce_dt",
    "_column_exists",
    "_iso",
    "_load_instagram_cookies",
    "_load_shared_account_source_row",
    "_metadata_dict",
    "_normalize_account_handle",
    "_normalize_non_negative_int",
    "_normalize_social_account_profile_handle",
    "_now_utc",
    "_pg_upsert",
    "_touch_shared_account_source",
    "pg",
)


@pytest.fixture(autouse=True)
def _configured_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    originals = {name: getattr(legacy_core, name) for name in PROFILE_STAGE_ROOM_NAMES}
    monkeypatch.setattr(profile_stages, "_LEGACY_NAMESPACE", legacy_core.__dict__)
    monkeypatch.setattr(profile_stages, "_LEGACY_ORIGINALS", originals)


def test_profile_stages_import_does_not_load_legacy_social_modules() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "leaf = importlib.import_module('trr_backend.socials.instagram.profile_stages')",
            "assert callable(leaf.get_instagram_profile_detail)",
            "loaded = set(sys.modules) - before",
            "forbidden = {'trr_backend.socials.social_season_analytics_impl',",
            "             'trr_backend.repositories.social_season_analytics'}",
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


def test_late_legacy_import_configures_preloaded_profile_leaf() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "leaf = importlib.import_module('trr_backend.socials.instagram.profile_stages')",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "legacy = importlib.import_module(legacy_name)",
            "assert leaf._LEGACY_NAMESPACE is legacy.__dict__",
            "assert leaf._legacy_value('SharedStageRuntimeError') is legacy.SharedStageRuntimeError",
            "legacy._load_instagram_cookies = lambda: {'sessionid': 'late'}",
            "assert leaf._load_instagram_cookies() == {'sessionid': 'late'}",
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


def test_fresh_socialblade_sidecar_completes_without_legacy_provider() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "service = importlib.import_module('trr_backend.socials.socialblade.service')",
            "legacy = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy not in sys.modules",
            "leaf = importlib.import_module('trr_backend.socials.instagram.profile_stages')",
            "assert leaf._LEGACY_NAMESPACE is None",
            "class RequestClient:",
            "    def get_json(self, *_args, **_kwargs):",
            "        return {'users': [{'pk': 'friend-1', 'username': 'friend'}], 'has_more': False}",
            "class Scraper:",
            "    WEB_X_ASBD_ID = '359341'",
            "    _request_client = RequestClient()",
            "    _get = None",
            "    def fetch_profile_page_content_graphql(self, *_args, **_kwargs):",
            "        return {'data': {'user': {'id': 'owner-1', 'follows_count': 1}}}",
            "    def _get_profile_page_context_cache_entry(self, *_args): return {}",
            "    def _request_cookies(self): return {}",
            "    def _get_headers(self, *_args): return {}",
            "leaf._instagram_profile_scraper = lambda *_args, **_kwargs: Scraper()",
            "leaf.persist_instagram_profile_snapshot = lambda *_args, **_kwargs: {'id': 'profile-row'}",
            (
                "leaf.persist_instagram_profile_relationships = lambda rows, **_kwargs: "
                "{'rows_upserted': len(rows), 'rows_missing': 0, 'snapshot_id': 'snapshot-1', "
                "'source_is_complete': True, 'mismatches': []}"
            ),
            "leaf._touch_shared_account_source = lambda **_kwargs: None",
            "service.socialblade_instagram_following_scrape_enabled = lambda: True",
            (
                "result = service.attach_instagram_following_scrape("
                "{'username': 'bravotv'}, handle='bravotv', source='modal')"
            ),
            "assert result['instagram_following_scrape']['status'] == 'completed', result",
            "assert result['instagram_following_scrape']['relationships_fetched'] == 1",
            "assert legacy not in sys.modules",
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


def test_exact_room_and_provider_surfaces() -> None:
    assert tuple(profile_stages.__all__) == PROFILE_STAGE_ROOM_NAMES
    assert profile_stages._LOCAL_ROOM_NAMES == set(PROFILE_STAGE_ROOM_NAMES)
    assert set(profile_stages._LOCAL_ROOM_FUNCTIONS) == set(PROFILE_STAGE_ROOM_NAMES)
    assert len(PROFILE_STAGE_PROVIDER_NAMES) == 15
    assert all(name in legacy_core.__dict__ for name in PROFILE_STAGE_PROVIDER_NAMES)


@pytest.mark.parametrize("function_name", PROFILE_STAGE_ROOM_NAMES)
def test_all_legacy_profile_wrappers_delegate_to_leaf_room(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def replacement(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return function_name

    monkeypatch.setitem(profile_stages._LOCAL_ROOM_FUNCTIONS, function_name, replacement)

    assert getattr(legacy_core, function_name)("arg", marker="value") == function_name
    assert calls == [(("arg",), {"marker": "value"})]


@pytest.mark.parametrize("function_name", PROFILE_STAGE_ROOM_NAMES)
def test_room_callable_avoids_original_wrapper_recursion_and_honors_live_patches(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_impl = lambda: "local"  # noqa: E731
    replacement = lambda: "patched"  # noqa: E731

    assert profile_stages._room_callable(function_name, local_impl) is local_impl

    monkeypatch.setattr(legacy_core, function_name, replacement)

    assert profile_stages._room_callable(function_name, local_impl) is replacement


@pytest.mark.parametrize("function_name", PROFILE_STAGE_ROOM_NAMES)
def test_room_honors_live_canonical_leaf_patches(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replacement = lambda: "canonical-patched"  # noqa: E731

    monkeypatch.setattr(profile_stages, function_name, replacement)

    assert profile_stages._room(function_name) is replacement


def test_profile_scraper_uses_live_cookie_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.socials.instagram as instagram_package

    created: list[dict[str, Any]] = []

    class FakeScraper:
        def __init__(self, **kwargs: Any) -> None:
            created.append(kwargs)

    monkeypatch.setattr(legacy_core, "_load_instagram_cookies", lambda: {"sessionid": "patched"})
    monkeypatch.setattr(instagram_package, "InstagramScraper", FakeScraper)

    scraper = profile_stages._instagram_profile_scraper(
        {"browser_account_id": "browser-account"},
        account_handle="bravotv",
    )

    assert isinstance(scraper, FakeScraper)
    assert created == [{"cookies": {"sessionid": "patched"}, "browser_account_id": "browser-account"}]


def test_snapshot_stage_preserves_exception_identity_and_live_room_patches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MarkerStageError(RuntimeError):
        def __init__(self, message: str, **metadata: Any) -> None:
            super().__init__(message)
            self.metadata = metadata

    scraper = SimpleNamespace(fetch_profile_info=lambda *_args, **_kwargs: {})
    monkeypatch.setattr(legacy_core, "SharedStageRuntimeError", MarkerStageError)
    monkeypatch.setattr(profile_stages, "_instagram_profile_scraper", lambda *_args, **_kwargs: scraper)

    with pytest.raises(MarkerStageError) as exc_info:
        profile_stages._run_instagram_profile_snapshot_stage(
            run_id="run-1",
            source_scope="network",
            account_handle="bravotv",
            config={},
            job_id="job-1",
        )

    assert exc_info.value.metadata == {
        "error_code": "instagram_profile_snapshot_empty",
        "retryable": True,
    }


def test_snapshot_stage_preserves_persistence_response_touch_and_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"username": "BravoTV"}
    touch_calls: list[dict[str, Any]] = []
    scraper = SimpleNamespace(fetch_profile_info=lambda *_args, **_kwargs: payload)
    monkeypatch.setattr(profile_stages, "_instagram_profile_scraper", lambda *_a, **_k: scraper)
    monkeypatch.setattr(
        profile_stages,
        "persist_instagram_profile_snapshot",
        lambda *_a, **_k: {"id": "row-1", "profile_id": "profile-1"},
    )
    monkeypatch.setattr(
        profile_stages,
        "_instagram_profile_response",
        lambda *_a, **_k: {"username": "bravotv"},
    )
    monkeypatch.setattr(legacy_core, "_touch_shared_account_source", lambda **kwargs: touch_calls.append(kwargs))

    result = profile_stages._run_instagram_profile_snapshot_stage(
        run_id="run-1",
        source_scope="network",
        account_handle="bravotv",
        config={},
        job_id="job-1",
    )

    assert result == (
        0,
        0,
        {
            "stage": "instagram_profile_snapshot",
            "platform": "instagram",
            "account": "bravotv",
            "profile_id": "profile-1",
            "profile_row_id": "row-1",
            "profile_snapshot": {"username": "bravotv"},
            "activity": {"phase": "instagram_profile_snapshot_end"},
        },
    )
    assert touch_calls[0]["metadata_updates"] == {"profile_snapshot": {"username": "bravotv"}}


def test_pg_fetch_and_execute_use_live_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Any]] = []

    class FakePg:
        @staticmethod
        def fetch_one(sql: str, params: list[Any]) -> dict[str, Any]:
            calls.append(("fetch_one", (sql, params)))
            return {"id": "row-1"}

        @staticmethod
        def execute(sql: str, params: list[Any], *, conn: Any | None = None) -> None:
            calls.append(("execute", (sql, params, conn)))

    monkeypatch.setattr(legacy_core, "pg", FakePg)

    assert profile_stages._instagram_profile_fetch_one("select 1", [1]) == {"id": "row-1"}
    profile_stages._instagram_profile_execute("update row", [2], conn="conn")

    assert calls == [
        ("fetch_one", ("select 1", [1])),
        ("execute", ("update row", [2], "conn")),
    ]


def test_external_link_sync_uses_live_normalization_persistence_and_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 7, 22, tzinfo=UTC)
    upserts: list[tuple[str, dict[str, Any], Any, Any]] = []
    monkeypatch.setattr(legacy_core, "_metadata_dict", lambda value: dict(value or {}))
    monkeypatch.setattr(legacy_core, "_now_utc", lambda: observed_at)
    monkeypatch.setattr(
        legacy_core,
        "_pg_upsert",
        lambda table, payload, *, conflict_col, conn=None: upserts.append((table, payload, conflict_col, conn)),
    )
    monkeypatch.setattr(profile_stages, "_instagram_profile_normalized_url", lambda _v: "url")
    monkeypatch.setattr(profile_stages, "_instagram_profile_domain", lambda _v: "domain")

    rows = profile_stages._sync_instagram_profile_external_links(
        profile_row_id="row-1",
        instagram_profile_id="profile-1",
        username="BravoTV",
        normalized_username="bravotv",
        external_links=[{"title": "Site", "url": "https://example.com/"}],
        job_id="job-1",
        run_id="run-1",
        conn="conn",
    )

    assert rows[0]["normalized_url"] == "url"
    assert rows[0]["normalized_domain"] == "domain"
    assert rows[0]["last_seen_at"] is observed_at
    assert upserts == [("instagram_profile_external_links", rows[0], ["profile_id", "link_index", "url"], "conn")]


def test_relationship_sync_marks_absent_rows_only_for_complete_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.instagram.profile_relationship_normalizer as normalizer

    missing_calls: list[dict[str, Any]] = []
    result = SimpleNamespace(relationships=[], mismatches=[], page_info={})
    monkeypatch.setattr(normalizer, "normalize_instagram_profile_relationships", lambda *_a, **_k: result)
    monkeypatch.setattr(legacy_core, "_normalize_social_account_profile_handle", lambda value: str(value).lower())
    monkeypatch.setattr(legacy_core, "_now_utc", lambda: datetime(2026, 7, 22, tzinfo=UTC))
    monkeypatch.setattr(profile_stages, "_instagram_profile_tables_ready", lambda **_k: True)
    monkeypatch.setattr(
        profile_stages,
        "_instagram_profile_row_for_username",
        lambda *_a, **_k: {"id": "owner-1", "profile_id": "profile-1"},
    )
    monkeypatch.setattr(profile_stages, "_instagram_following_snapshot_is_complete", lambda _meta: True)
    monkeypatch.setattr(
        profile_stages,
        "_active_instagram_profile_relationship_rows",
        lambda **_k: [{"id": "old-1", "related_user_id": "user-1"}],
    )
    monkeypatch.setattr(
        profile_stages,
        "_mark_instagram_profile_relationship_missing",
        lambda **kwargs: missing_calls.append(kwargs) or {"id": "old-1"},
    )
    monkeypatch.setattr(profile_stages, "_instagram_profile_snapshot_tables_ready", lambda **_k: False)

    synced = profile_stages.persist_instagram_profile_relationships(
        [],
        owner_username="BravoTV",
        snapshot_metadata={"has_more": False, "rows_fetched": 0},
        job_id="job-1",
        run_id="run-1",
    )

    assert synced["rows_missing"] == 1
    assert synced["source_is_complete"] is True
    assert missing_calls[0]["relationship_row_id"] == "old-1"


def test_relationship_response_preserves_normalization_pagination_and_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 7, 22, tzinfo=UTC)
    fetch_calls: list[tuple[str, list[Any]]] = []
    monkeypatch.setattr(legacy_core, "_normalize_social_account_profile_handle", lambda _value: "bravotv")
    monkeypatch.setattr(legacy_core, "_normalize_non_negative_int", lambda value: max(0, int(value or 0)))
    monkeypatch.setattr(legacy_core, "_coerce_dt", lambda value: value)
    monkeypatch.setattr(legacy_core, "_iso", lambda value: value.isoformat() if value else None)
    monkeypatch.setattr(
        profile_stages,
        "_instagram_profile_row_for_username",
        lambda *_a, **_k: {"id": "owner-1", "profile_id": "profile-1", "username": "BravoTV"},
    )

    def fetch_one(sql: str, params: list[Any], **_kwargs: Any) -> dict[str, Any]:
        fetch_calls.append((sql, params))
        return {"total": 51}

    def fetch_all(sql: str, params: list[Any], **_kwargs: Any) -> list[dict[str, Any]]:
        fetch_calls.append((sql, params))
        return [{"id": "rel-1", "related_username": "friend", "last_seen_at": observed_at}]

    monkeypatch.setattr(profile_stages, "_instagram_profile_fetch_one", fetch_one)
    monkeypatch.setattr(profile_stages, "_instagram_profile_fetch_all", fetch_all)

    response = profile_stages.get_instagram_profile_relationships("@BravoTV", page=2, page_size=25)

    assert response["owner"]["username"] == "BravoTV"
    assert response["items"][0]["last_seen_at"] == "2026-07-22T00:00:00+00:00"
    assert response["pagination"] == {"page": 2, "page_size": 25, "total": 51, "total_pages": 3}
    assert fetch_calls[1][1] == ["owner-1", 25, 25]

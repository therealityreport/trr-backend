"""DB-backed integration coverage for social account catalog backfill launches."""

from __future__ import annotations

import os
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from dotenv import load_dotenv

import trr_backend.repositories.social_season_analytics as social_repo

load_dotenv()

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_DB_TESTS", "").lower() not in ("1", "true", "yes"),
    reason="RUN_DB_TESTS not enabled - set RUN_DB_TESTS=1 to run integration tests",
)


@pytest.fixture(scope="module")
def db_client():
    from trr_backend.db import create_supabase_admin_client

    return create_supabase_admin_client()


def _seed_instagram_shared_catalog_rows(db_client: Any, *, handle: str, shortcodes: list[str]) -> None:
    base_posted_at = datetime(2026, 4, 1, tzinfo=UTC)
    rows = [
        {
            "source_id": shortcode,
            "source_account": handle,
            "posted_at": (base_posted_at + timedelta(days=index)).isoformat(),
            "caption": f"Catalog seed {shortcode}",
            "media_urls": [f"https://example.com/{shortcode}.jpg"],
            "hashtags": [],
            "mentions": [],
            "collaborators": [],
            "profile_tags": [],
            "raw_data": {"shortcode": shortcode},
            "assignment_status": "unassigned",
        }
        for index, shortcode in enumerate(shortcodes)
    ]
    response = db_client.schema("social").table("instagram_account_catalog_posts").insert(rows).execute()
    assert not (hasattr(response, "error") and response.error), f"Failed to seed catalog rows: {response.error}"


def test_launch_instagram_backfill_bootstraps_when_catalog_rows_exist_but_materialized_posts_are_missing(
    monkeypatch: pytest.MonkeyPatch,
    db_client: Any,
) -> None:
    handle = f"igcov_{uuid.uuid4().hex[:8]}"
    catalog_calls: list[dict[str, Any]] = []
    comments_calls: list[dict[str, Any]] = []
    merged_updates: list[dict[str, Any]] = []

    try:
        _seed_instagram_shared_catalog_rows(db_client, handle=handle, shortcodes=["SC1", "SC2", "SC3"])
        monkeypatch.setattr(
            social_repo,
            "start_social_account_catalog_backfill",
            lambda platform, account_handle, **kwargs: (
                catalog_calls.append({"platform": platform, "account_handle": account_handle, **kwargs})
                or {"run_id": "catalog-run-1", "status": "queued"}
            ),
        )
        monkeypatch.setattr(
            social_repo,
            "start_social_account_comments_scrape",
            lambda *_args, **_kwargs: comments_calls.append({}) or {"run_id": "comments-run-1", "status": "queued"},
        )
        monkeypatch.setattr(
            social_repo,
            "_load_catalog_run_row_by_id",
            lambda _run_id: {
                "config": {
                    "required_runtime_version": {"execution_backend": "modal", "modal_image": "im-latest"},
                    "created_by_runtime_version": {"execution_backend": "local", "commit_sha": "abc123"},
                }
            },
        )
        monkeypatch.setattr(
            social_repo,
            "_merge_catalog_run_config_with_conn",
            lambda **kwargs: (
                merged_updates.append(kwargs["metadata_updates"]) or {"config": kwargs["metadata_updates"]}
            ),
        )
        monkeypatch.setattr(social_repo.pg, "db_connection", lambda **_kwargs: nullcontext(object()))

        payload = social_repo.launch_social_account_catalog_backfill(
            "instagram",
            handle,
            source_scope="bravo",
            selected_tasks=["post_details", "comments", "media"],
        )

        assert payload["catalog_bootstrap_required"] is True
        assert payload["comments_deferred_until_catalog_complete"] is True
        assert payload["effective_selected_tasks"] == ["post_details", "comments", "media"]
        assert len(catalog_calls) == 1
        assert catalog_calls[0]["social_account_post_details_only"] is False
        assert comments_calls == []
        assert merged_updates[-1]["deferred_comments_followup"]["state"] == "pending"
    finally:
        (
            db_client.schema("social")
            .table("instagram_account_catalog_posts")
            .delete()
            .eq("source_account", handle)
            .execute()
        )
        db_client.schema("social").table("instagram_posts").delete().eq("username", handle).execute()

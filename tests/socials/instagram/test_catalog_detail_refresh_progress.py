from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from trr_backend.socials.instagram import catalog_ingest as catalog
from trr_backend.socials.pipelines.account_catalog import progress


@pytest.fixture(scope="module", autouse=True)
def _configure_catalog_detail_providers() -> None:
    """Publish the existing legacy providers so the unit fakes patch the exact provider-owned runtime bindings."""
    from trr_backend.socials.control_plane_bootstrap import register_social_control_plane_providers

    register_social_control_plane_providers()


def _patch_catalog_and_core(monkeypatch: pytest.MonkeyPatch, name: str, value: Any) -> None:
    monkeypatch.setattr(catalog, name, value)
    if hasattr(catalog._core, name):
        monkeypatch.setattr(catalog._core, name, value)


@contextmanager
def _fake_account_execution(*_args: Any, **_kwargs: Any):
    yield "bravotv"


@contextmanager
def _fake_db_connection(**_kwargs: Any):
    yield object()


class _FakeDetailScraper:
    def fetch_post_info(self, shortcode: str, *, delay: float) -> dict[str, Any]:
        assert shortcode == "SHORT1"
        assert delay >= 0
        return {"items": [{"code": shortcode}]}

    def _parse_post_node(self, node: dict[str, Any], _config: Any) -> SimpleNamespace:
        post = SimpleNamespace(
            shortcode=node["code"],
            pk="media-1",
            username="bravotv",
            caption="detail caption",
            post_type="reel",
            media_urls=["https://example.test/detail.mp4"],
            thumbnail_url="https://example.test/detail.jpg",
            likes=12,
            comments=7,
            video_views_observed=44,
            taken_at=1_700_000_000,
            hashtags=[],
            mentions=[],
            collaborators=[],
            profile_tags=[],
        )
        post.to_dict = lambda: {"code": node["code"]}
        return post


def test_detail_refresh_stamps_metadata_on_successful_fetched_post(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
    captured_posts: list[Any] = []
    progress_updates: list[dict[str, Any]] = []

    def _fake_upsert_instagram_post(*_args: Any, posts: list[Any], **_kwargs: Any) -> list[dict[str, Any]]:
        captured_posts.extend(posts)
        return [
            {
                "id": "post-1",
                "shortcode": post.shortcode,
                "metadata_source": getattr(post, "metadata_source", None),
                "metadata_scraped_at": getattr(post, "metadata_scraped_at", None),
            }
            for post in posts
        ]

    _patch_catalog_and_core(monkeypatch, "_now_utc", lambda: fixed_now)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_account_execution", _fake_account_execution)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    _patch_catalog_and_core(monkeypatch, "_build_shared_instagram_scraper", lambda **_kwargs: _FakeDetailScraper())
    _patch_catalog_and_core(
        monkeypatch,
        "_shared_account_expected_total_posts_from_config",
        lambda *_a, **_k: 17_518,
    )
    _patch_catalog_and_core(monkeypatch, "_refresh_instagram_post_metrics_only", lambda **_kwargs: None)
    _patch_catalog_and_core(monkeypatch, "_batch_upsert_instagram_posts", _fake_upsert_instagram_post)
    _patch_catalog_and_core(
        monkeypatch,
        "_load_existing_social_account_posts",
        lambda *_args, **_kwargs: [
            {
                "id": "post-1",
                "shortcode": "SHORT1",
                "likes": 1,
                "comments_count": 1,
                "views": 1,
                "metadata_scraped_at": None,
            }
        ],
    )
    monkeypatch.setattr(catalog.pg, "db_connection", _fake_db_connection)

    rows, meta = catalog._scrape_shared_instagram_post_details_refresh(
        run_id="run-1",
        account_handle="bravotv",
        config={
            "selected_tasks": ["post_details"],
            "date_start": "2026-06-01T00:00:00+00:00",
            "date_end": "2026-08-01T00:00:00+00:00",
            "details_refresh_force_detail_fetch": True,
            "details_refresh_skip_media_followups": True,
            "details_refresh_write_batch_size": 1,
        },
        job_id="job-1",
        progress_cb=progress_updates.append,
    )

    assert meta["details_refreshed_posts"] == 1
    assert rows[0]["metadata_scraped_at"] == fixed_now
    assert rows[0]["metadata_source"] == "api_permalink"
    assert captured_posts[0].metadata_scraped_at == fixed_now
    assert captured_posts[0].metadata_source == "api_permalink"
    assert captured_posts[0].metadata_error is None
    assert meta["expected_total_posts"] == 17_518
    assert meta["total_posts"] == 1
    assert meta["completion_target_posts"] == 1
    assert meta["completion_target_source"] == "bounded_catalog"
    assert {update["total_posts"] for update in progress_updates} == {1}
    assert [update["phase"] for update in progress_updates] == [
        "details_refresh_fetch",
        "details_refresh_update",
    ]


def test_detail_refresh_progress_uses_terminal_job_rows_over_stale_nonterminal_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(progress, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    run_config = {
        "selected_tasks": ["post_details"],
        "effective_selected_tasks": ["post_details"],
        "pagination_state": {},
        "stage_graph": {
            "detail_refresh": {
                "status": "queued",
                "selected": True,
                "blocker_reasons": [],
            },
        },
        "target_readiness": {"detail_gap_count": 687},
    }
    job_rows = [
        {
            "stage": "shared_account_posts",
            "status": "completed",
            "metadata": {"fetcher_runtime": {"auth_state": "anonymous"}},
        }
    ]

    stage_payload = progress._catalog_progress_stage_graph_payload(
        run_config=run_config,
        stages_payload={},
        job_rows=job_rows,
    )
    runtime_payload = progress._catalog_posts_runtime_additive_payload(
        platform="instagram",
        account_handle="bravotv",
        run_id="run-1",
        run_config=run_config,
        job_rows=job_rows,
    )

    assert stage_payload["stage_graph"]["detail_refresh"]["status"] == "completed"
    assert runtime_payload["details_progress"]["status"] == "completed"


@pytest.mark.parametrize("bounded_total", [360, 0])
def test_bounded_detail_refresh_shard_progress_and_completion_use_enumerated_rows(
    monkeypatch: pytest.MonkeyPatch,
    bounded_total: int,
) -> None:
    progress_updates: list[dict[str, Any]] = []
    rows = [
        {
            "id": f"post-{index}",
            "shortcode": f"SHORT{index}",
            "likes": 1,
            "comments_count": 1,
            "views": 1,
        }
        for index in range(bounded_total)
    ]

    _patch_catalog_and_core(monkeypatch, "_shared_instagram_account_execution", _fake_account_execution)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    _patch_catalog_and_core(monkeypatch, "_build_shared_instagram_scraper", lambda **_kwargs: SimpleNamespace())
    _patch_catalog_and_core(monkeypatch, "_load_existing_social_account_posts", lambda *_args, **_kwargs: rows)
    _patch_catalog_and_core(
        monkeypatch,
        "_shared_account_expected_total_posts_from_config",
        lambda *_args, **_kwargs: 17_518,
    )

    refreshed_rows, meta = catalog._scrape_shared_instagram_post_details_refresh(
        run_id="run-bounded",
        account_handle="bravotv",
        config={
            "selected_tasks": ["post_details"],
            "date_start": "2026-06-01T00:00:00+00:00",
            "date_end": "2026-08-01T00:00:00+00:00",
            "details_refresh_dry_run": True,
            "details_refresh_skip_media_followups": True,
            "details_refresh_shard_index": 1,
            "details_refresh_shard_count": 6,
        },
        job_id="job-bounded",
        progress_cb=progress_updates.append,
    )

    enumerated_total = bounded_total // 6
    assert len(refreshed_rows) == enumerated_total
    assert meta["details_refresh_rows_seen"] == enumerated_total
    assert meta["total_posts"] == enumerated_total
    assert meta["completion_target_posts"] == enumerated_total
    assert meta["completion_target_source"] == "bounded_catalog"
    if progress_updates:
        assert progress_updates[-1]["posts_checked"] == enumerated_total
        assert progress_updates[-1]["total_posts"] == enumerated_total
    else:
        assert enumerated_total == 0


@pytest.mark.live
@pytest.mark.parametrize(
    "scenario",
    [
        "failed",
        "partial",
        "null_likes",
        "invalid_comments",
        "object_likes",
        "object_comments",
        "edge_metrics",
        "zero_no_caption",
        "legacy_cached",
        "incomplete_cached",
        "rollback",
        "ambiguous",
        "resume",
        "cached",
        "cancelled",
        "provider",
        "unavailable",
        "no_work",
        "gallery",
        "known_edit",
        "attempt_cap",
        "bulk",
        "bulk_stale",
        "bulk_cancel",
        "bulk_lease",
    ],
)
def test_durable_detail_checkpoint_against_isolated_postgres(monkeypatch: pytest.MonkeyPatch, scenario: str) -> None:
    """E3/E4: actual repository SQL and detail/sidecar/canonical writes.

    Opt-in Unix-socket database, provisioned with the relevant migrations.
    External transport and unrelated run-counter/heartbeat consumers are faked.
    No production or generic DATABASE_URL value is accepted by this probe.
    """
    import json
    import os
    import uuid

    import psycopg2
    from psycopg2.extensions import parse_dsn

    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as core
    from trr_backend.socials.control_plane import instagram_detail_targets as targets
    from trr_backend.socials.instagram import InstagramScraper

    dsn = os.getenv("TRR_DETAIL_TEST_DSN")
    if not dsn:
        pytest.skip("TRR_DETAIL_TEST_DSN must explicitly select the isolated Postgres probe")
    binding = parse_dsn(dsn)
    assert binding.get("host", "").startswith("/tmp/trr-detail-check.")
    assert binding.get("user") == "trr_detail_test" and binding.get("dbname") == "postgres"
    with psycopg2.connect(dsn) as identity_conn, identity_conn.cursor() as cur:
        cur.execute("show data_directory")
        data_directory_row = cur.fetchone()
        assert data_directory_row is not None
        assert data_directory_row[0] == binding["host"] + "/data"
        if scenario == "bulk":
            cur.execute(
                "select current_setting('shared_preload_libraries'), "
                "exists(select 1 from pg_available_extensions where name='pg_stat_statements')"
            )
            extension_row = cur.fetchone()
            assert extension_row is not None
            preload, extension_available = extension_row
            if not extension_available:
                pytest.skip("bulk SQL-call comparison requires the pg_stat_statements extension on isolated Postgres")
            if not any(
                library.strip().strip('"').rsplit("/", 1)[-1] == "pg_stat_statements" for library in preload.split(",")
            ):
                pytest.skip(
                    "bulk SQL-call comparison requires shared_preload_libraries=pg_stat_statements "
                    "on the guarded isolated Postgres; other durable behavior cases run independently"
                )

    ambiguous_raised = False
    active_connections = 0

    @contextmanager
    def connection(**kwargs):
        nonlocal ambiguous_raised, active_connections
        existing = pg._transaction_connection.get()
        if existing is not None:
            yield existing
            return
        conn = psycopg2.connect(dsn)
        active_connections += 1
        try:
            yield conn
            conn.commit()
            if (
                scenario == "ambiguous"
                and kwargs.get("label") == "instagram_detail_checkpoint"
                and not ambiguous_raised
            ):
                ambiguous_raised = True
                raise psycopg2.OperationalError("injected lost commit acknowledgement")
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
            active_connections -= 1

    def one(sql, params=None, **kwargs) -> dict[str, Any]:
        with pg.db_cursor(conn=kwargs.get("conn")) as cur:
            row = pg.fetch_one_with_cursor(cur, sql, params or [])
            assert row is not None
            return row

    monkeypatch.setattr(pg, "db_connection", connection)
    monkeypatch.setattr(pg, "fetch_one", one)
    catalog._sync_core_overrides()
    _patch_catalog_and_core(
        monkeypatch, "_shared_instagram_frontier_auth_validation", lambda _config: (True, "validated")
    )
    _patch_catalog_and_core(monkeypatch, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    cancel_checks = 0

    def check_cancelled(**_kwargs):
        nonlocal cancel_checks
        cancel_checks += 1
        if (scenario == "cancelled" and cancel_checks == 2) or (scenario == "bulk_cancel" and len(requests) >= 2):
            raise core.SharedStageRuntimeError(
                "controlled cancellation",
                error_code=core.SHARED_ACCOUNT_STAGE_CANCELLED_ERROR_CODE,
            )

    _patch_catalog_and_core(monkeypatch, "_raise_if_shared_account_stage_cancelled", check_cancelled)
    monkeypatch.setattr(core, "_claimed_media_runtime_versions", lambda: ("{}", "{}"))
    for name in (
        "_increment_run_counters_on_job_finish",
        "_clear_worker_heartbeat_for_job",
        "_invalidate_queue_status_cache",
        "_finalize_run_status",
    ):
        monkeypatch.setattr(core, name, lambda *_args, **_kwargs: None)
    requests: list[str] = []

    class Scraper(InstagramScraper):
        def __init__(self):
            from requests import Session

            self.session = cast(Any, Session())
            self.session.get = self.get

        def _request_cookies(self):
            return {"sessionid": account, "ds_user_id": account}

        def _get_headers(self, referer=None):
            return {}

        def _shortcode_to_media_id(self, shortcode):
            return shortcode

        def get(self, url, **kwargs):
            assert active_connections == 0, "A database connection must not span outbound transport"
            assert kwargs["allow_redirects"] is False
            payload = self.fetch_post_info(url.split("/")[-3])
            return SimpleNamespace(
                status_code=200,
                headers={"content-type": "application/json"},
                is_redirect=False,
                text="",
                json=lambda: payload,
            )

        def fetch_post_info(self, shortcode, delay=2.0):
            requests.append(shortcode)
            if scenario == "bulk_lease" and len(requests) > 1:
                assert (
                    one(
                        "select state from social.instagram_detail_run_targets where run_id=%s::uuid and source_id=%s",
                        [run_id, requests[-2]],
                    )["state"]
                    == "committed"
                )
            if scenario == "bulk_stale" and len(requests) == 2:
                one(
                    """update social.instagram_detail_run_targets set lease_generation=lease_generation+1
                    where run_id=%s::uuid and source_id=%s returning source_id""",
                    [run_id, requests[0]],
                )
            if scenario in {"failed", "attempt_cap"}:
                return None
            node = {
                "code": shortcode,
                "pk": "1234567890",
                "taken_at": 1788220800,
                "media_type": 1,
                "like_count": 10,
                "comment_count": 2,
                "caption": {"text": "controlled probe"},
                "image_versions2": {
                    "candidates": [{"url": "https://example.test/probe.jpg", "width": 100, "height": 100}]
                },
                "user": {"username": "controlled"},
            }
            if scenario == "partial":
                del node["comment_count"]
            if scenario == "null_likes":
                node["like_count"] = None
            if scenario == "invalid_comments":
                node["comment_count"] = "invalid"
            if scenario == "object_likes":
                node["like_count"] = {"count": 17}
            if scenario == "object_comments":
                node["comment_count"] = {"count": 17}
            if scenario == "edge_metrics":
                del node["like_count"], node["comment_count"]
                node["edge_liked_by"] = {"count": 17}
                node["edge_media_to_comment"] = {"count": 0}
            if scenario == "zero_no_caption":
                node["like_count"] = node["comment_count"] = 0
                del node["caption"]
            return {"items": [node]}

    _patch_catalog_and_core(monkeypatch, "_build_shared_instagram_scraper", lambda **_kwargs: Scraper())
    if scenario == "rollback":
        original_sync = core._sync_instagram_canonical_post_media_assets

        def fail_after_observation(**kwargs):
            original_sync(**kwargs)
            raise RuntimeError("injected failure after canonical observation")

        monkeypatch.setattr(core, "_sync_instagram_canonical_post_media_assets", fail_after_observation)
    run_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
    account, code = "probe" + uuid.uuid4().hex[:10], "DETAIL" + uuid.uuid4().hex[:10]
    one("insert into social.scrape_runs(id) values (%s::uuid) returning id", [run_id])
    codes = (
        []
        if scenario == "no_work"
        else [code + str(index) for index in range(6)]
        if scenario.startswith("bulk")
        else [code, code + "B"]
        if scenario in {"resume", "ambiguous", "rollback"}
        else [code]
    )
    for source in codes:
        one(
            "insert into social.instagram_account_catalog_posts(source_id,source_account,posted_at) "
            "values (%s,%s,now()) returning id",
            [source, account],
        )
    old_success = datetime(2020, 1, 1, tzinfo=UTC)
    if scenario == "bulk":
        one(
            "insert into social.instagram_account_catalog_posts(source_id,source_account,posted_at) "
            "values (%s,%s,now()) returning id",
            [codes[0] + " ", account],
        )
    if scenario in {
        "failed",
        "partial",
        "null_likes",
        "invalid_comments",
        "object_likes",
        "object_comments",
        "rollback",
        "cached",
        "legacy_cached",
        "incomplete_cached",
        "known_edit",
    }:
        successful_at = (
            datetime.now(UTC)
            if scenario in {"cached", "legacy_cached", "incomplete_cached", "known_edit"}
            else old_success
        )
        raw_data: dict[str, Any] = {"view_metrics": {"observed_at": successful_at.isoformat()}}
        if scenario in {"cached", "incomplete_cached"}:
            raw_data["detail_snapshot"] = {
                "version": 1,
                "coverage_version": 2,
                "source": "instagram_response",
                "observed_at": successful_at.isoformat(),
                "caption_present": True,
                "likes_present": True,
                "comments_present": True,
                "media_complete": True,
                "media_count": 2 if scenario == "incomplete_cached" else 1,
            }
        one(
            """insert into social.instagram_posts
            (shortcode, likes, comments_count, views, media_urls, metadata_scraped_at, scraped_at, raw_data)
            values (%s, 99, 77, 66, '["https://example.test/old.jpg"]', %s, %s, %s::jsonb) returning id""",
            [
                code,
                successful_at,
                successful_at,
                json.dumps(raw_data),
            ],
        )
    if scenario in {"gallery", "known_edit"}:
        from trr_backend.socials.instagram import ScrapeConfig

        gallery_payload = Scraper().fetch_post_info(code)
        assert gallery_payload is not None
        parsed = Scraper()._parse_post_node(gallery_payload["items"][0], ScrapeConfig(username=account, hashtags=[]))
        one(
            """update social.instagram_account_catalog_posts set raw_data = %s::jsonb
            where source_id = %s returning id""",
            [json.dumps(parsed.to_dict()), code],
        )
        requests.clear()
    with connection() as conn:
        manifest = targets.freeze_manifest(
            run_id,
            account,
            {"selected_tasks": ["post_details"], "details_refresh_force_detail_fetch": scenario == "bulk"},
            conn=conn,
        )
    if scenario == "gallery":
        monkeypatch.setenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_STALE_METADATA_DAYS", "1")
        with connection() as conn:
            assert (
                targets.freeze_manifest(
                    run_id,
                    account,
                    {"details_refresh_stale_metadata_days": 365, "selected_tasks": ["media"]},
                    conn=conn,
                )
                == manifest
            )
    config = {
        "detail_contract_version": 1,
        "detail_manifest": manifest,
        "stage": "shared_account_posts",
        "account": account,
        "details_refresh_skip_media_followups": True,
        "details_chunk_targets": 50 if scenario.startswith("bulk") or scenario in {"ambiguous", "rollback"} else 1,
    }
    one(
        "insert into social.scrape_jobs(id,run_id,config) values (%s::uuid,%s::uuid,%s::jsonb) returning id",
        [job_id, run_id, json.dumps(config)],
    )
    reservation = targets.reserve_dispatch(job_id)
    assert reservation is not None
    if scenario == "provider":
        assert targets.bind_dispatch(job_id, reservation["token"], {"dispatched": True, "call_id": "probe-" + job_id})
    claimed = core._claim_job_by_id(job_id=job_id, worker_id="probe-worker", dispatch_token=reservation["token"])
    assert claimed is not None
    if scenario == "bulk_lease":
        claim_target = targets.claim_target
        monkeypatch.setattr(
            targets,
            "claim_target",
            lambda *args: {**target, "lease_expires_at": datetime.now(UTC) + timedelta(seconds=55)}
            if (target := claim_target(*args)) is not None
            else None,
        )
    if scenario == "provider":
        from trr_backend.socials.control_plane import dispatch_runtime

        monkeypatch.setattr(
            core, "_load_current_job_row", lambda job: one("select * from social.scrape_jobs where id=%s::uuid", [job])
        )
        duplicate_claim = dispatch_runtime.claim_and_process_social_job(
            job_id=job_id,
            worker_id="duplicate",
            dispatch_token=reservation["token"],
        )
        assert duplicate_claim is not None
        assert duplicate_claim["claimed"] is False
        for status in ("unknown", "completed"):
            assert not targets.reconcile_provider_failure(core, claimed, {"status": status})
            assert one("select status from social.scrape_jobs where id=%s::uuid", [job_id])["status"] == "running"
        assert targets.reconcile_provider_failure(core, claimed, {"status": "timeout"})
        row = one("select status,detail_completion_receipt from social.scrape_jobs where id=%s::uuid", [job_id])
        assert row == {"status": "failed", "detail_completion_receipt": None}
        resumed = targets.resume_manifest(run_id=run_id, account_handle=account, manifest_identity=manifest["identity"])
        assert resumed["resumed"]
        assert not targets.bind_dispatch(job_id, reservation["token"], {"dispatched": True, "call_id": "late"})
        assert not targets.reconcile_provider_failure(core, claimed, {"status": "timeout"})
        return
    if scenario == "unavailable":
        target = targets.claim_target(run_id, account, job_id, reservation["token"], "probe-worker")
        assert target is not None
        with connection() as conn:
            with pytest.raises(ValueError, match="explicit healthy authenticated"):
                targets.checkpoint(target, state="source_unavailable", conn=conn)
            targets.checkpoint(
                target,
                state="source_unavailable",
                conn=conn,
                unavailable_evidence={
                    "authenticated": True,
                    "transport_healthy": True,
                    "source_id": code,
                    "reason": "deleted",
                    "verified_at": datetime.now(UTC).isoformat(),
                },
            )
        receipt = targets.finish_chunk(core, claimed, "probe-worker")
        assert receipt["completion_kind"] == "completed_with_unavailable"
        assert receipt["outcomes"]["source_unavailable"] == 1 and receipt["outcomes"]["committed"] == 0
        return
    if scenario == "cancelled":
        with pytest.raises(core.SharedStageRuntimeError):
            catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
        targets.finish_chunk(core, claimed, "probe-worker", cancelled=True)
        assert not requests
        assert targets.snapshot(run_id)["attempted"] == 0
        assert targets.snapshot(run_id)["unresolved"] == 1
        assert one("select status from social.scrape_jobs where id=%s::uuid", [job_id])["status"] == "cancelled"
        return
    if scenario in {"rollback", "bulk_stale", "bulk_cancel"}:
        expected_error = (
            targets.LostDetailOwnershipError
            if scenario == "bulk_stale"
            else core.SharedStageRuntimeError
            if scenario == "bulk_cancel"
            else RuntimeError
        )
        with pytest.raises(expected_error):
            catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
        result = targets.snapshot(run_id)
        assert result["committed"] == 0 and result["unresolved"] == len(codes)
        assert (
            one("select count(*) as n from social.social_post_observations where detail_run_id=%s::uuid", [run_id])["n"]
            == 0
        )
        assert one("select count(*) as n from social.instagram_posts where shortcode=any(%s)", [codes])["n"] == int(
            scenario == "rollback"
        )
        if scenario == "rollback":
            assert one("select likes from social.instagram_posts where shortcode=%s", [code])["likes"] == 99
        return
    if scenario == "bulk":
        with connection() as conn, conn.cursor() as cur:
            cur.execute("create extension if not exists pg_stat_statements")
        # Warm cached schema checks equally; count server-executed SQL, including
        # preservation reads, request accounting and cursor execute_values pages.
        core._instagram_canonical_post_tables_ready(conn=None)
        one("select pg_stat_statements_reset()")
    result = catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
    if scenario == "bulk":
        bulk_calls = one(
            "select sum(calls)::int as n from pg_stat_statements "
            "where dbid=(select oid from pg_database where datname=current_database())"
        )["n"]
        one(
            """update social.instagram_detail_run_targets set state='pending', attempt_count=0, request_count=0
            where run_id=%s::uuid returning source_id""",
            [run_id],
        )
        claimed["config"]["details_refresh_write_batch_size"] = 1
        one("select pg_stat_statements_reset()")
        repeated = catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
        single_calls = one(
            "select sum(calls)::int as n from pg_stat_statements "
            "where dbid=(select oid from pg_database where datname=current_database())"
        )["n"]
        assert repeated["committed"] == result["committed"] == len(codes)
        assert bulk_calls < single_calls, (bulk_calls, single_calls)
        assert one("select count(*) as n from social.social_post_observations where detail_run_id=%s::uuid", [run_id])[
            "n"
        ] == len(codes)
        assert one("select count(*) as n from social.instagram_posts where shortcode=any(%s)", [codes])["n"] == len(
            codes
        )
        assert one(
            """select count(*) as n from social.social_post_media_assets a
            join social.social_posts p on p.id=a.post_id and p.platform=a.platform
            where p.platform='instagram' and p.source_id=any(%s)""",
            [codes],
        )["n"] == len(codes)
        print(f"Executing durable worker SQL calls: batch={bulk_calls}, single={single_calls}; {len(codes)} targets")
    if scenario == "attempt_cap":
        for _ in range(2):
            one(
                """update social.instagram_detail_run_targets set next_attempt_at=now()-interval '1 second'
                where run_id=%s::uuid returning source_id""",
                [run_id],
            )
            result = catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
        assert len(requests) == 3 and result["attempted"] == 3 and result["committed"] == 0
        assert (
            one("select state from social.instagram_detail_run_targets where run_id=%s::uuid", [run_id])["state"]
            == "failed"
        )
        assert catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")["attempted"] == 3
        assert len(requests) == 3
        return
    if scenario == "resume":
        assert result["committed"] == 1 and result["unresolved"] == 1
        one(
            """update social.scrape_jobs set detail_dispatch_expires_at=now()-interval '1 second',
              heartbeat_at=now()-interval '2 minutes' where id=%s::uuid returning id""",
            [job_id],
        )
        assert targets.recover_expired(core, run_id=run_id)
        one("update social.scrape_jobs set available_at=now() where id=%s::uuid returning id", [job_id])
        second = targets.reserve_dispatch(job_id)
        assert second is not None
        claimed = core._claim_job_by_id(job_id=job_id, worker_id="probe-worker", dispatch_token=second["token"])
        assert claimed is not None
        result = catalog.run_durable_instagram_details(claimed, worker_id="probe-worker")
        assert requests == codes
    receipt = targets.finish_chunk(core, claimed, "probe-worker")
    current = one(
        "select status,items_found,detail_completion_receipt from social.scrape_jobs where id=%s::uuid", [job_id]
    )
    observation_count = one(
        "select count(*) as n from social.social_post_observations where detail_run_id=%s::uuid",
        [run_id],
    )["n"]
    if scenario in {
        "failed",
        "partial",
        "null_likes",
        "invalid_comments",
        "object_likes",
        "object_comments",
        "rollback",
    }:
        assert result["committed"] == 0 and result["unresolved"] == 1
        assert current["status"] == "retrying" and current["items_found"] == 0
        detail = one(
            "select likes,metadata_scraped_at,scraped_at from social.instagram_posts where shortcode=%s", [code]
        )
        assert detail == {"likes": 99, "metadata_scraped_at": old_success, "scraped_at": old_success}
        assert observation_count == 0
        assert len(requests) == 1
    elif scenario in {"cached", "gallery"}:
        assert result["cached_satisfied"] == 1 and result["committed"] == 0
        assert not requests and observation_count == (1 if scenario == "gallery" else 0)
        assert current["items_found"] == 0 and current["status"] == "completed"
    else:
        assert result["committed"] == len(codes) and result["unresolved"] == 0
        assert observation_count == len(codes) and len(requests) == len(codes) * (2 if scenario == "bulk" else 1)
        assert current["status"] == "completed" and current["items_found"] == len(codes)
    assert receipt["claimed"] is True
    assert current["detail_completion_receipt"]["dispatch_generation"] == claimed["detail_dispatch_generation"]
    assert result["total"] == sum(
        result[key] for key in ("committed", "cached_satisfied", "source_unavailable", "unresolved")
    )


@pytest.mark.live
@pytest.mark.parametrize(
    "scenario",
    [
        "budget",
        "probe",
        "invalid_probe",
        "semantic_breaker",
        "auth",
        "rate_limit",
        "retry",
        "breaker",
        "cancel",
        "deadline",
    ],
)
def test_detail_transport_shared_limits_against_isolated_postgres(monkeypatch, scenario):
    """E5: real atomic lane reservations; controlled outbound responses only."""
    import os
    import time
    import uuid
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import psycopg2
    import requests
    from psycopg2.extensions import parse_dsn

    from trr_backend.db import pg
    from trr_backend.socials.instagram.detail_transport import DetailDeferred, DetailTransport
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure

    dsn = os.getenv("TRR_DETAIL_TEST_DSN")
    if not dsn:
        pytest.skip("requires explicit isolated Postgres")
    binding = parse_dsn(dsn)
    assert binding.get("host", "").startswith("/tmp/trr-detail-check.")
    assert binding.get("user") == "trr_detail_test" and binding.get("dbname") == "postgres"
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("show data_directory")
        data_directory_row = cur.fetchone()
        assert data_directory_row is not None
        assert data_directory_row[0] == binding["host"] + "/data"

    @contextmanager
    def connection(**_kwargs):
        with psycopg2.connect(dsn) as conn:
            yield conn

    monkeypatch.setattr(pg, "db_connection", connection)
    identity = uuid.uuid4().hex
    sent, attempts = [], []
    entered, release = Event(), Event()
    session = cast(Any, requests.Session())

    def send(url, **kwargs):
        assert kwargs["allow_redirects"] is False
        assert sum(kwargs["timeout"]) < 30
        sent.append(url)
        if scenario == "retry":
            raise requests.Timeout("controlled")
        if scenario == "probe":
            entered.set()
            assert release.wait(2)
        status = 401 if scenario == "auth" else 429 if scenario == "rate_limit" else 200
        payload = {} if scenario == "breaker" else {"items": [{"code": "CONTROLLED"}]}
        return SimpleNamespace(
            status_code=status, headers={"Retry-After": "120"}, text="", is_redirect=False, json=lambda: payload
        )

    session.get = send
    scraper = SimpleNamespace(
        session=session,
        _request_cookies=lambda: {"sessionid": identity, "ds_user_id": identity},
        _get_headers=lambda _: {},
        POST_INFO_URL="https://www.instagram.com/api/v1/media/{media_id}/info/",
        _shortcode_to_media_id=lambda code: code,
    )

    def cancelled():
        if scenario == "cancel":
            raise RuntimeError("cancelled")

    def transport():
        return DetailTransport(
            scraper=scraper,
            check_cancelled=cancelled,
            on_request=attempts.append,
            deadline=time.monotonic() + (-1 if scenario == "deadline" else 30),
        )

    first = transport()
    if scenario in {"semantic_breaker", "invalid_probe"}:

        def invalid(_payload):
            raise ValueError("partial_required_fields")

        if scenario == "invalid_probe":
            with connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """insert into social.instagram_detail_request_lanes(identity_key,cooldown_until)
                    values (%s, now() - interval '1 second')""",
                    [first.identity_key],
                )
        count = 1 if scenario == "invalid_probe" else 3
        for _ in range(count):
            with pytest.raises(InstagramRequestFailure, match="partial_required_fields"):
                transport().fetch("CONTROLLED", validate=invalid)
        with pytest.raises(DetailDeferred, match="identity_cooldown"):
            transport().fetch("CONTROLLED")
        assert len(sent) == len(attempts) == count
        return
    if scenario in {"cancel", "deadline"}:
        with pytest.raises((RuntimeError, DetailDeferred)):
            first.fetch("CONTROLLED")
        assert sent == attempts == []
        return
    if scenario == "budget":
        for _ in range(180):
            transport().fetch("CONTROLLED")
        for _ in range(3):
            with pytest.raises(DetailDeferred, match="identity_budget"):
                transport().fetch("CONTROLLED")
        assert len(sent) == len(attempts) == 180
    elif scenario == "probe":
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_detail_request_lanes(identity_key,cooldown_until)
                values (%s, now() - interval '1 second')""",
                [first.identity_key],
            )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(first.fetch, "CONTROLLED")
            assert entered.wait(2)
            for _ in range(3):
                with pytest.raises(DetailDeferred, match="identity_probe_pending"):
                    transport().fetch("CONTROLLED")
            assert len(sent) == len(attempts) == 1
            release.set()
            assert future.result()["items"]
        assert transport().fetch("CONTROLLED")["items"]
        assert len(sent) == 2
    elif scenario == "retry":
        with pytest.raises(InstagramRequestFailure, match="transport_timeout"):
            first.fetch("CONTROLLED", allow_public_fallback=True)
        assert len(sent) == 2 and attempts == [True, False]
    else:
        count = 3 if scenario == "breaker" else 1
        last_error: InstagramRequestFailure | None = None
        for _ in range(count):
            with pytest.raises(InstagramRequestFailure):
                transport().fetch("CONTROLLED")
        for _ in range(3):
            with pytest.raises(DetailDeferred) as exc:
                transport().fetch("CONTROLLED", allow_public_fallback=True)
            last_error = exc.value
        assert len(sent) == len(attempts) == count
        if scenario == "rate_limit":
            assert last_error is not None
            next_attempt_at = last_error.next_attempt_at
            assert next_attempt_at is not None
            assert 115 <= (next_attempt_at - datetime.now(UTC)).total_seconds() <= 120


@pytest.mark.parametrize(
    "condition,expected",
    [
        ("recent", False),
        ("missing_timestamp", True),
        ("expired", True),
        ("carousel", True),
        ("known_edit", True),
        ("force", True),
        ("policy", True),
    ],
)
def test_detail_field_policy_requires_successful_fresh_complete_metadata(condition, expected):
    from datetime import timedelta

    now = datetime.now(UTC)
    row = {
        "metadata_scraped_at": now,
        "likes": 1,
        "comments_count": 0,
        "views": 2,
        "caption": "",
        "location": None,
        "media_urls": ["https://example.test/one.jpg"],
    }
    if condition == "missing_timestamp":
        row.pop("metadata_scraped_at")
        row["scraped_at"] = now
    if condition == "expired":
        row["metadata_scraped_at"] = now - timedelta(days=31)
    if condition == "carousel":
        row["carousel_media_count"] = 2
    if condition == "known_edit":
        row["known_edit"] = True
    if condition == "policy":
        row["detail_field_policy_version"] = 2
    result = catalog.classify_instagram_detail_refresh_need(
        row,
        selected_tasks=["post_details"],
        require_fresh_metadata=True,
        now_utc=now,
        force_network_detail_fetch=condition == "force",
    )
    assert result["fetch_needed"] is expected

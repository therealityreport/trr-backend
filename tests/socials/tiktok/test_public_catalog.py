from types import SimpleNamespace

import pytest

from trr_backend.socials.tiktok.shared_catalog import public_listing as module


class StageError(RuntimeError):
    def __init__(self, message, **kwargs):
        super().__init__(message)
        self.metadata = kwargs


def setup(monkeypatch, *, fail=False, expected=1, partial=False):
    events = []

    def fetch(sql, params):
        if sql.startswith("update"):
            events.append("checkpoint")
        return {"id": "job", "status": "running", "config": {}}

    monkeypatch.setattr(module.pg, "fetch_one", fetch)

    def upsert(**kwargs):
        events.append("post")
        if fail:
            raise RuntimeError("write failed")
        return {"source_id": kwargs["post"].video_id}

    core = SimpleNamespace(
        SharedStageRuntimeError=StageError,
        _shared_account_expected_total_posts_from_config=lambda *a, **k: expected,
        _coerce_dt=lambda value: None,
        _shared_tiktok_catalog_effective_tasks=lambda config: {"post_details", "media"},
        _upsert_shared_catalog_post=upsert,
    )

    def collect(**kwargs):
        kwargs["on_page"](
            [SimpleNamespace(video_id="123", create_time=1780000000)], {"pages_scanned": 1, "checkpoint_ids": ["123"]}
        )
        return [], {"partial": partial, "native_exhausted": not partial}

    monkeypatch.setattr(module, "scrape_public_profile_posts", collect)

    def hydrate(**kwargs):
        assert kwargs["config"]["tiktok_comments_in_posts_stage"] is False
        assert "comments" not in kwargs["config"]["selected_tasks"]
        events.append("hydrate")
        return {"id": "post"}

    args = {
        "core": core,
        "hydrate": hydrate,
        "run_id": "run",
        "account_handle": "bravotv",
        "config": {"selected_tasks": ["post_details", "comments", "media"]},
        "job_id": "job",
    }
    return args, events


def test_posts_and_materialization_precede_checkpoint(monkeypatch):
    args, events = setup(monkeypatch)
    rows, metadata = module.scrape_catalog(**args)
    assert events == ["post", "hydrate", "checkpoint", "checkpoint"]
    assert rows == [{"source_id": "123"}]
    assert metadata["persist_counters"]["comments_upserted"] == 0


def test_failed_persistence_does_not_advance_checkpoint(monkeypatch):
    args, events = setup(monkeypatch, fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        module.scrape_catalog(**args)
    assert events == ["post"]


def test_materialization_failure_does_not_advance_checkpoint(monkeypatch):
    args, events = setup(monkeypatch)
    args["hydrate"] = lambda **kwargs: None
    with pytest.raises(RuntimeError, match="materialization failed"):
        module.scrape_catalog(**args)
    assert events == ["post"]


@pytest.mark.parametrize("expected,partial", [(2, False), (1, True)])
def test_missing_history_keeps_checkpoint_but_fails_completion(monkeypatch, expected, partial):
    args, events = setup(monkeypatch, expected=expected, partial=partial)
    with pytest.raises(StageError) as error:
        module.scrape_catalog(**args)
    assert error.value.metadata["error_code"] == "catalog_incomplete"
    assert events[-1] == "checkpoint"


def test_cancelled_job_never_persists(monkeypatch):
    args, events = setup(monkeypatch)
    monkeypatch.setattr(module.pg, "fetch_one", lambda *args: {"status": "cancelled", "config": {}})
    with pytest.raises(StageError):
        module.scrape_catalog(**args)
    assert events == []


def test_checkpoint_is_claim_fenced_and_heartbeats_before_persistence(monkeypatch):
    args, events = setup(monkeypatch)
    queries = []

    def fetch(sql, params):
        queries.append((sql, params))
        return {"id": "job", "status": "running", "config": {}}

    monkeypatch.setattr(module.pg, "fetch_one", fetch)
    args["config"]["claim_token"] = "retained-claim"
    args["progress_cb"] = lambda value: events.append(value["phase"])
    module.scrape_catalog(**args)
    assert events.index("fetch_tiktok_public_listing") < events.index("post")
    writes = [(sql, params) for sql, params in queries if sql.startswith("update")]
    assert len(writes) == 2
    assert all("claim_token" in sql and "retained-claim" in params for sql, params in writes)
    assert not any("status in ('failed','cancelled')" in sql for sql, _ in queries)


def test_replaced_claim_stops_before_persistence(monkeypatch):
    args, events = setup(monkeypatch)
    monkeypatch.setattr(module.pg, "fetch_one", lambda sql, params: None if "select status" in sql else {})
    with pytest.raises(StageError):
        module.scrape_catalog(**args)
    assert events == []

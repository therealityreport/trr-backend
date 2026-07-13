"""Focused CAS and idempotency coverage for Instagram comments rebalancing."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import trr_backend.socials.pipelines.comments.instagram as comments_pipeline


def test_session_advisory_lock_connection_uses_requested_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()
    calls: list[tuple[str, str]] = []

    @contextmanager
    def db_connection(*, label: str, pool_name: str):
        calls.append((label, pool_name))
        yield connection

    monkeypatch.setattr(comments_pipeline.pg, "db_connection", db_connection)

    with comments_pipeline._session_advisory_lock_connection(
        label="comments-lock",
        pool_name="session_control",
    ) as (lock_conn, discard_state):
        assert lock_conn is connection
        assert discard_state == {"discarded": False, "preserve_outcome": False}

    assert calls == [("comments-lock", "session_control")]


def test_session_advisory_lock_discard_closes_connection_and_preserves_completed_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = Connection()

    @contextmanager
    def db_connection(*, label: str, pool_name: str):  # noqa: ARG001
        yield connection
        raise RuntimeError("closed connection cannot commit")

    monkeypatch.setattr(comments_pipeline.pg, "db_connection", db_connection)

    with comments_pipeline._session_advisory_lock_connection(
        label="comments-lock",
        pool_name="session_control",
    ) as (lock_conn, discard_state):
        comments_pipeline._discard_session_advisory_lock_connection(
            lock_conn,
            discard_state=discard_state,
            preserve_outcome=True,
        )

    assert connection.closed is True
    assert discard_state == {"discarded": True, "preserve_outcome": True}


@pytest.fixture
def failed_rebalance_conn(monkeypatch: pytest.MonkeyPatch) -> object:
    conn = object()

    @contextmanager
    def db_connection(**_kwargs: Any):
        yield conn

    monkeypatch.setattr(comments_pipeline.pg, "db_connection", db_connection)
    monkeypatch.setattr(
        comments_pipeline._core,
        "_increment_run_counters_on_job_create_batch",
        lambda **_kwargs: None,
    )
    return conn


def _slow_source_row(*, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "run_id": "run-1",
        "job_id": "slow-job-1",
        "source_scope": "bravo",
        "initiated_by": "test",
        "started_at": datetime.now(UTC) - timedelta(hours=2),
        "config": {
            "account": "bravotv",
            "source_scope": "bravo",
            "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            "comments_shard_count": 4,
            "target_source_ids": [f"P{index}" for index in range(1, 13)],
        },
        "metadata": metadata or {"stage_counters": {"posts": 0}},
    }


def test_slow_rebalance_does_not_create_children_after_cas_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    update_calls: list[str] = []
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_REBALANCE_ENABLED", "1")
    monkeypatch.setattr(comments_pipeline.pg, "fetch_all", lambda *_args, **_kwargs: [_slow_source_row()])

    def cas_miss(query: str, *_args: Any, **_kwargs: Any) -> None:
        update_calls.append(query)
        return None

    monkeypatch.setattr(comments_pipeline.pg, "fetch_one", cas_miss)
    monkeypatch.setattr(
        comments_pipeline,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("a CAS miss must not create retry children"),
    )

    payload = comments_pipeline.rebalance_slow_instagram_comments_shards(
        run_id="run-1",
        slow_elapsed_seconds=60,
        slow_posts_per_minute=0.25,
        min_remaining_targets=3,
        max_retry_shard_size=3,
        dispatch_immediately=False,
    )

    assert payload["created_job_ids"] == []
    assert payload["rebalanced_source_job_ids"] == []
    assert payload["skipped_sources"] == [{"job_id": "slow-job-1", "reason": "source_status_changed"}]
    assert "and status = 'running'" in update_calls[0]


@pytest.mark.parametrize("remote_status", ["pending", "unknown"])
def test_slow_rebalance_skips_active_remote_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    remote_status: str,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SLOW_SHARD_REBALANCE_ENABLED", "1")
    monkeypatch.setattr(
        comments_pipeline.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            _slow_source_row(
                metadata={
                    "dispatch": {
                        "remote_invocation_id": "fc-active",
                        "remote_invocation_status": remote_status,
                    },
                    "stage_counters": {"posts": 0},
                }
            )
        ],
    )
    monkeypatch.setattr(
        comments_pipeline.pg,
        "fetch_one",
        lambda *_args, **_kwargs: pytest.fail("active remote dispatch must not be cancelled"),
    )
    monkeypatch.setattr(
        comments_pipeline,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("active remote dispatch must not be split"),
    )

    payload = comments_pipeline.rebalance_slow_instagram_comments_shards(
        run_id="run-1",
        slow_elapsed_seconds=60,
        slow_posts_per_minute=0.25,
        min_remaining_targets=3,
        max_retry_shard_size=3,
        dispatch_immediately=False,
    )

    assert payload["created_job_ids"] == []
    assert payload["skipped_sources"] == [
        {
            "job_id": "slow-job-1",
            "reason": "remote_invocation_active",
            "remote_invocation_status": remote_status,
        }
    ]


def test_failed_rebalance_does_not_create_children_after_claim_cas_miss(
    monkeypatch: pytest.MonkeyPatch,
    failed_rebalance_conn: object,
) -> None:
    source_row = {
        "job_id": "failed-job",
        "run_id": "run-1",
        "status": "failed",
        "source_scope": "bravo",
        "initiated_by": "test",
        "config": {
            "account": "bravotv",
            "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        },
        "metadata": {"retry_rebalance": {"remaining_target_source_ids": ["A", "B", "C"]}},
    }
    calls = 0

    def fetch_one(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any] | None:
        nonlocal calls
        assert _kwargs["conn"] is failed_rebalance_conn
        calls += 1
        if query.lstrip().lower().startswith("select"):
            return source_row
        return None

    monkeypatch.setattr(comments_pipeline.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(
        comments_pipeline._core,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("a failed-source CAS miss must not create retry children"),
    )

    payload = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=2,
    )

    assert calls == 2
    assert payload == {
        "created_job_ids": [],
        "failed_job_id": "failed-job",
        "reason": "source_status_changed",
    }


def test_failed_rebalance_skips_unknown_remote_dispatch_before_claim(
    monkeypatch: pytest.MonkeyPatch,
    failed_rebalance_conn: object,
) -> None:
    source_row = {
        "job_id": "failed-job",
        "run_id": "run-1",
        "status": "failed",
        "source_scope": "bravo",
        "initiated_by": "test",
        "config": {
            "account": "bravotv",
            "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        },
        "metadata": {
            "dispatch": {
                "remote_invocation_id": "fc-unknown",
                "remote_invocation_status": "unknown",
            },
            "retry_rebalance": {"remaining_target_source_ids": ["A", "B", "C"]},
        },
    }
    monkeypatch.setattr(comments_pipeline.pg, "fetch_one", lambda *_args, **_kwargs: source_row)
    monkeypatch.setattr(
        comments_pipeline._core,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("unknown remote dispatch must not create retry children"),
    )

    payload = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=2,
    )

    assert payload == {
        "created_job_ids": [],
        "failed_job_id": "failed-job",
        "reason": "remote_invocation_active",
        "remote_invocation_status": "unknown",
    }


def test_failed_rebalance_claim_is_idempotent_across_repeated_calls(
    monkeypatch: pytest.MonkeyPatch,
    failed_rebalance_conn: object,
) -> None:
    source_state: dict[str, Any] = {
        "status": "failed",
        "metadata": {
            "dispatch": {
                "remote_invocation_id": "fc-completed",
                "remote_invocation_status": "completed",
            },
            "retry_rebalance": {"remaining_target_source_ids": ["A", "B", "C"]},
        },
    }
    created_jobs: list[dict[str, Any]] = []
    claim_queries: list[str] = []
    source_row = {
        "job_id": "failed-job",
        "run_id": "run-1",
        "status": "failed",
        "platform": "instagram",
        "source_scope": "bravo",
        "initiated_by": "test",
        "config": {
            "account": "bravotv",
            "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            "comments_shard_count": 4,
        },
        "metadata": {"retry_rebalance": {"remaining_target_source_ids": ["A", "B", "C"]}},
    }

    def fetch_one(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any] | None:
        assert _kwargs["conn"] is failed_rebalance_conn
        if query.lstrip().lower().startswith("select"):
            if source_state["status"] == "failed":
                return {**source_row, "status": "failed", "metadata": source_state["metadata"]}
            return {
                **source_row,
                "status": "cancelled",
                "metadata": source_state["metadata"],
            }
        claim_queries.append(query)
        if source_state["status"] != "failed" or source_state["metadata"].get("comments_retry_rebalance_claimed_at"):
            return None
        source_state["status"] = "cancelled"
        source_state["metadata"] = {
            **source_state["metadata"],
            "comments_retry_rebalance_claimed_at": "claimed-at",
            "comments_retry_rebalance_group_id": "retry-group",
            "comments_retry_rebalance_shard_count": 2,
        }
        return {"id": "failed-job"}

    monkeypatch.setattr(comments_pipeline.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(
        comments_pipeline.pg,
        "fetch_all",
        lambda *_args, **kwargs: [
            {
                "job_id": f"retry-{index}",
                "retry_index": job["config"]["comments_retry_rebalance_index"],
            }
            for index, job in enumerate(created_jobs, start=1)
            if kwargs["conn"] is failed_rebalance_conn
        ],
    )
    monkeypatch.setattr(
        comments_pipeline._core,
        "_create_job",
        lambda *_args, **kwargs: (
            created_jobs.append(dict(kwargs))
            or ("retry-" + str(len(created_jobs)) if kwargs["conn"] is failed_rebalance_conn else "")
        ),
    )

    first = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=2,
    )
    second = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=2,
    )

    assert first["created_job_ids"] == ["retry-1", "retry-2"]
    assert second == {
        "created_job_ids": [],
        "failed_job_id": "failed-job",
        "retry_group_id": "retry-group",
        "reason": "already_rebalanced",
    }
    assert len(created_jobs) == 2
    assert source_state["status"] == "cancelled"
    assert "metadata->>'comments_retry_rebalance_claimed_at' is null" in claim_queries[0]


@pytest.mark.parametrize("existing_indexes", [[], [1]])
def test_failed_rebalance_resumes_claimed_zero_or_partial_children_without_duplicates(
    monkeypatch: pytest.MonkeyPatch,
    failed_rebalance_conn: object,
    existing_indexes: list[int],
) -> None:
    child_indexes = set(existing_indexes)
    created_indexes: list[int] = []
    source_row = {
        "job_id": "failed-job",
        "run_id": "run-1",
        "status": "cancelled",
        "source_scope": "bravo",
        "initiated_by": "test",
        "config": {
            "account": "bravotv",
            "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        },
        "metadata": {
            "retry_rebalance": {"remaining_target_source_ids": ["A", "B", "C", "D", "E"]},
            "comments_retry_rebalance_claimed_at": "claimed-at",
            "comments_retry_rebalance_group_id": "retry-group",
            "comments_retry_rebalance_shard_count": 3,
        },
    }

    def fetch_one(query: str, *_args: Any, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["conn"] is failed_rebalance_conn
        assert query.lstrip().lower().startswith("select")
        return source_row

    def fetch_all(_query: str, *_args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["conn"] is failed_rebalance_conn
        return [{"job_id": f"retry-{index}", "retry_index": str(index)} for index in sorted(child_indexes)]

    def create_job(*_args: Any, **kwargs: Any) -> str:
        assert kwargs["conn"] is failed_rebalance_conn
        assert kwargs["track_run_counters"] is False
        index = int(kwargs["config"]["comments_retry_rebalance_index"])
        assert index not in child_indexes
        child_indexes.add(index)
        created_indexes.append(index)
        return f"retry-{index}"

    monkeypatch.setattr(comments_pipeline.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(comments_pipeline.pg, "fetch_all", fetch_all)
    monkeypatch.setattr(comments_pipeline._core, "_create_job", create_job)

    first = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=1,
    )
    second = comments_pipeline.rebalance_failed_instagram_comments_shard(
        failed_job_id="failed-job",
        max_retry_shard_size=1,
    )

    assert first["created_job_ids"] == [f"retry-{index}" for index in range(1, 4) if index not in existing_indexes]
    assert first["resumed_from_claimed_source"] is True
    assert second == {
        "created_job_ids": [],
        "failed_job_id": "failed-job",
        "retry_group_id": "retry-group",
        "reason": "already_rebalanced",
    }
    assert child_indexes == {1, 2, 3}
    assert created_indexes == [index for index in range(1, 4) if index not in existing_indexes]


@pytest.mark.parametrize("remote_status", ["running", "unknown"])
def test_waiting_rebalance_keeps_active_remote_dispatch_recoverable(
    monkeypatch: pytest.MonkeyPatch,
    remote_status: str,
) -> None:
    targets = [f"P{index}" for index in range(1, 26)]
    monkeypatch.setattr(
        comments_pipeline.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "run_id": "run-1",
                "job_id": "queued-dispatched-1",
                "status": "queued",
                "priority": 120,
                "source_scope": "bravo",
                "initiated_by": "test",
                "config": {
                    "account": "bravotv",
                    "source_scope": "bravo",
                    "stage": comments_pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                    "target_source_ids": targets,
                },
                "metadata": {
                    "dispatch": {
                        "remote_invocation_id": "fc-active",
                        "remote_invocation_status": remote_status,
                    }
                },
            }
        ],
    )
    monkeypatch.setattr(
        comments_pipeline.pg,
        "fetch_one",
        lambda *_args, **_kwargs: pytest.fail("active remote dispatch must not be cancelled"),
    )
    monkeypatch.setattr(
        comments_pipeline,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("active remote dispatch must not be split"),
    )

    payload = comments_pipeline.rebalance_waiting_instagram_comments_shards(
        run_id="run-1",
        max_waiting_shard_size=10,
        dispatch_immediately=False,
    )

    assert payload["created_job_ids"] == []
    assert payload["skipped_sources"] == [
        {
            "job_id": "queued-dispatched-1",
            "reason": "remote_invocation_active",
            "remote_invocation_status": remote_status,
        }
    ]

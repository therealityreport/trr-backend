from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field

from scripts.socials import reconcile_stale_social_run as subject


@dataclass
class FakePg:
    rows: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    writes: list[tuple[str, list[object]]] = field(default_factory=list)
    remaining_count_reads: int = 0
    summary_reads: int = 0
    lock_entries: int = 0
    current_run_status: str = "queued"
    lock_conn: object = field(default_factory=object)

    @contextmanager
    def advisory_session_lock(self, lock_key: int, *, label: str, pool_name: str = "default"):
        del lock_key, label, pool_name
        self.lock_entries += 1
        yield self.lock_conn

    def fetch_one(self, query: str, params: list[object], **kwargs):
        if kwargs:
            assert kwargs.get("conn") is self.lock_conn
        normalized = " ".join(query.lower().split())
        if "stage_counts" in normalized and "from social.scrape_jobs" in normalized:
            self.summary_reads += 1
            rows = self.fetch_all(query, params)
            counter_statuses = set(params[1])
            stage_counts: dict[str, dict[str, int]] = {}
            for row in rows:
                status = str(row.get("status") or "")
                stage = str(row.get("stage") or row.get("job_type") or "unknown")
                bucket = stage_counts.setdefault(stage, {"total": 0, "completed": 0, "failed": 0, "active": 0})
                bucket["total"] += 1
                if status == "completed":
                    bucket["completed"] += 1
                if status == "failed":
                    bucket["failed"] += 1
                if status in counter_statuses:
                    bucket["active"] += 1
            return {
                "active_jobs": sum(1 for row in rows if row.get("status") in counter_statuses),
                "completed_jobs": sum(1 for row in rows if row.get("status") == "completed"),
                "failed_jobs": sum(1 for row in rows if row.get("status") == "failed"),
                "running_jobs": sum(1 for row in rows if row.get("status") == "running"),
                "cancelling_jobs": sum(1 for row in rows if row.get("status") == "cancelling"),
                "queued_jobs": sum(1 for row in rows if row.get("status") in {"queued", "pending", "retrying"}),
                "stage_counts": stage_counts,
            }
        if "count(*) filter (where status = any(%s))::int as open_jobs" in normalized:
            self.remaining_count_reads += 1
            rows = self.fetch_all(query, params)
            open_statuses = set(params[0])
            return {
                "open_jobs": sum(1 for row in rows if row.get("status") in open_statuses),
                "failed_jobs": sum(1 for row in rows if row.get("status") == "failed"),
            }
        if "select status" in normalized and "from social.scrape_runs" in normalized:
            return {"status": self.current_run_status}
        if "from social.scrape_runs" in normalized:
            return {
                "id": "80cf0056-7659-4203-b5f9-0758ee9d98c0",
                "status": "queued",
                "total_jobs": 2,
                "active_jobs": 2,
            }
        return None

    def fetch_all(self, query: str, params: list[object], **kwargs):
        if kwargs:
            assert kwargs.get("conn") is self.lock_conn
        normalized = " ".join(query.lower().split())
        if "from social.scrape_jobs" in normalized:
            return [
                {
                    "id": "retry-job",
                    "status": "retrying",
                    "job_type": "shared_account_posts",
                    "stage": "shared_account_posts",
                    "last_error_code": "shared_stage_failed",
                },
                {
                    "id": "queued-job",
                    "status": "queued",
                    "job_type": "shared_account_posts",
                    "stage": "shared_account_posts",
                    "last_error_code": None,
                },
            ]
        return []

    def execute(self, query: str, params: list[object], **kwargs) -> None:
        if kwargs:
            assert kwargs.get("conn") is self.lock_conn
        self.writes.append((" ".join(query.lower().split()), list(params)))


def test_plan_run_identifies_duplicate_active_jobs_without_writing(monkeypatch):
    fake_pg = FakePg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.run_id == "80cf0056-7659-4203-b5f9-0758ee9d98c0"
    assert result.duplicate_open_job_ids == ["queued-job"]
    assert result.retry_job_ids == ["retry-job"]
    assert fake_pg.writes == []


def test_execute_cleanup_cancels_duplicates_and_recomputes_run(monkeypatch):
    fake_pg = FakePg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == ["queued-job"]
    assert fake_pg.lock_entries == 1
    assert fake_pg.summary_reads == 1
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" in joined_sql
    assert "status = 'cancelled'" in joined_sql
    assert "completed_at = coalesce(completed_at, now())" in joined_sql
    assert "heartbeat_at = now()" in joined_sql
    assert "worker_id = null" in joined_sql
    assert "claimed_at = null" in joined_sql
    assert "jsonb_typeof(metadata) = 'object'" in joined_sql
    assert "update social.scrape_runs" in joined_sql
    run_update_params = fake_pg.writes[-1][1]
    summary_payload = json.loads(run_update_params[-2])
    assert summary_payload["active_jobs"] == 2
    assert summary_payload["completed_jobs"] == 0
    assert summary_payload["failed_jobs"] == 0
    assert summary_payload["stage_counts"] == {
        "shared_account_posts": {"total": 2, "completed": 0, "failed": 0, "active": 2}
    }


def test_partitioned_same_type_jobs_are_not_duplicates(monkeypatch):
    @dataclass
    class PartitionedPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "shard-0-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"partition_id": "shard-0"},
                        "metadata": {},
                    },
                    {
                        "id": "shard-1-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"partition_id": "shard-1"},
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = PartitionedPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_same_type_jobs_with_distinct_config_values_are_not_duplicates(monkeypatch):
    @dataclass
    class CursorPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "cursor-a-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"cursor": "a"},
                        "metadata": {},
                    },
                    {
                        "id": "cursor-b-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"cursor": "b"},
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = CursorPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_same_type_jobs_with_distinct_metadata_values_are_not_duplicates(monkeypatch):
    @dataclass
    class MetadataPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "metadata-a-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {},
                        "metadata": {"cursor": "a"},
                    },
                    {
                        "id": "metadata-b-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {},
                        "metadata": {"cursor": "b"},
                    },
                ]
            return []

    fake_pg = MetadataPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_same_type_jobs_with_equivalent_payloads_are_duplicates(monkeypatch):
    @dataclass
    class EquivalentPayloadPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "first-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"cursor": "a", "nested": {"page": 1}},
                        "metadata": {"source": "backfill"},
                    },
                    {
                        "id": "second-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"nested": {"page": 1}, "cursor": "a"},
                        "metadata": {"source": "backfill"},
                    },
                ]
            return []

    fake_pg = EquivalentPayloadPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == ["second-job"]


def test_same_type_jobs_with_distinct_non_dict_config_values_are_not_duplicates(monkeypatch):
    @dataclass
    class NonDictConfigPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "list-x-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": ["x"],
                        "metadata": {},
                    },
                    {
                        "id": "list-y-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": ["y"],
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = NonDictConfigPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_same_type_jobs_with_distinct_non_dict_metadata_values_are_not_duplicates(monkeypatch):
    @dataclass
    class NonDictMetadataPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "metadata-alpha-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {},
                        "metadata": "alpha",
                    },
                    {
                        "id": "metadata-beta-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {},
                        "metadata": "beta",
                    },
                ]
            return []

    fake_pg = NonDictMetadataPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_same_type_jobs_with_equivalent_non_dict_payloads_are_duplicates(monkeypatch):
    @dataclass
    class EquivalentNonDictPayloadPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "first-list-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": ["x"],
                        "metadata": "alpha",
                    },
                    {
                        "id": "second-list-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": ["x"],
                        "metadata": "alpha",
                    },
                ]
            return []

    fake_pg = EquivalentNonDictPayloadPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == ["second-list-job"]


def test_dict_config_and_json_string_config_are_not_duplicates(monkeypatch):
    @dataclass
    class DictVsStringPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "dict-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"a": 1},
                        "metadata": {},
                    },
                    {
                        "id": "string-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": '{"a":1}',
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = DictVsStringPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []


def test_none_config_and_zero_config_are_not_duplicates(monkeypatch):
    @dataclass
    class NoneVsZeroPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "none-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": None,
                        "metadata": {},
                    },
                    {
                        "id": "zero-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": 0,
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = NoneVsZeroPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []


def test_int_config_and_string_config_are_not_duplicates(monkeypatch):
    @dataclass
    class IntVsStringPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [
                    {
                        "id": "int-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": 1,
                        "metadata": {},
                    },
                    {
                        "id": "string-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": "1",
                        "metadata": {},
                    },
                ]
            return []

    fake_pg = IntVsStringPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []


def test_execute_cleanup_builds_duplicate_plan_from_locked_current_jobs(monkeypatch):
    @dataclass
    class ChangingJobsPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" not in normalized:
                return []
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
                return [
                    {
                        "id": "current-a-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"cursor": "a"},
                        "metadata": {},
                    },
                    {
                        "id": "current-b-job",
                        "status": "queued",
                        "job_type": "shared_account_posts",
                        "config": {"cursor": "b"},
                        "metadata": {},
                    },
                ]
            return [
                {
                    "id": "stale-first-job",
                    "status": "queued",
                    "job_type": "shared_account_posts",
                    "config": {"cursor": "stale"},
                    "metadata": {},
                },
                {
                    "id": "stale-duplicate-job",
                    "status": "queued",
                    "job_type": "shared_account_posts",
                    "config": {"cursor": "stale"},
                    "metadata": {},
                },
            ]

    fake_pg = ChangingJobsPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    dry_run = subject.plan_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")
    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert dry_run.duplicate_open_job_ids == ["stale-duplicate-job"]
    assert result.duplicate_open_job_ids == []
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" not in joined_sql


def test_execute_cleanup_marks_active_run_terminal_when_no_open_jobs_remain(monkeypatch):
    @dataclass
    class StaleActivePg(FakePg):
        open_fetch_count: int = 0

        def fetch_one(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "count(*) filter (where status = any(%s))::int as open_jobs" in normalized:
                self.remaining_count_reads += 1
                return {"open_jobs": 0}
            if "from social.scrape_runs" in normalized:
                return {
                    "id": "80cf0056-7659-4203-b5f9-0758ee9d98c0",
                    "status": "running",
                    "total_jobs": 2,
                    "active_jobs": 2,
                }
            return None

    fake_pg = StaleActivePg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == ["queued-job"]
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[4] == "completed"
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "status = %s" in joined_sql
    assert "completed_at = case" in joined_sql


def test_execute_cleanup_terminalizes_cancelling_run_when_no_open_jobs_remain(monkeypatch):
    @dataclass
    class StaleCancellingPg(FakePg):
        def fetch_one(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "count(*) filter (where status = any(%s))::int as open_jobs" in normalized:
                self.remaining_count_reads += 1
                return {"open_jobs": 0, "failed_jobs": 0}
            if "from social.scrape_runs" in normalized:
                return {
                    "id": "80cf0056-7659-4203-b5f9-0758ee9d98c0",
                    "status": "cancelling",
                    "total_jobs": 2,
                    "active_jobs": 2,
                }
            return None

    fake_pg = StaleCancellingPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.run_status == "cancelling"
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[4] == "completed"
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "status = %s" in joined_sql


def test_execute_cleanup_reconciles_completed_run_with_active_queue_to_queued(monkeypatch):
    @dataclass
    class CompletedWithQueuedPg(FakePg):
        current_run_status: str = "completed"

        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [{"id": "queued-job", "status": "queued", "job_type": "posts", "stage": "posts"}]
            return []

    fake_pg = CompletedWithQueuedPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.run_status == "queued"
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[4] == "queued"
    summary_payload = json.loads(run_update_params[-2])
    assert summary_payload["stale_run_reconciler"]["next_run_status"] == "queued"
    assert summary_payload["stale_run_reconciler"]["terminalized"] is False


def test_execute_cleanup_reconciles_running_jobs_to_running(monkeypatch):
    @dataclass
    class RunningJobPg(FakePg):
        current_run_status: str = "completed"

        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [{"id": "running-job", "status": "running", "job_type": "posts", "stage": "posts"}]
            return []

    fake_pg = RunningJobPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.run_status == "queued"
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[4] == "running"
    summary_payload = json.loads(run_update_params[-2])
    assert summary_payload["running_jobs"] == 1
    assert summary_payload["stale_run_reconciler"]["next_run_status"] == "running"


def test_execute_cleanup_cancelling_job_sets_cancelling_status_but_zero_active_counters(monkeypatch):
    @dataclass
    class CancellingJobPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                return [{"id": "cancelling-job", "status": "cancelling", "job_type": "posts", "stage": "posts"}]
            return []

    fake_pg = CancellingJobPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.run_status == "queued"
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[0] == 0
    assert run_update_params[4] == "cancelling"
    assert json.loads(run_update_params[3]) == {
        "posts": {"total": 1, "completed": 0, "failed": 0, "active": 0}
    }
    summary_payload = json.loads(run_update_params[-2])
    assert summary_payload["active_jobs"] == 0
    assert summary_payload["cancelling_jobs"] == 1
    assert summary_payload["stage_counts"]["posts"]["active"] == 0
    assert summary_payload["stale_run_reconciler"]["next_run_status"] == "cancelling"


def test_execute_cleanup_summary_counters_reflect_job_statuses(monkeypatch):
    @dataclass
    class MixedStatusPg(FakePg):
        def fetch_all(self, query: str, params: list[object], **kwargs):
            if kwargs:
                assert kwargs.get("conn") is self.lock_conn
            normalized = " ".join(query.lower().split())
            if "from social.scrape_jobs" in normalized:
                if "order by created_at asc, id asc" in normalized:
                    return [
                        {"id": "queued-job", "status": "queued", "job_type": "comments", "stage": "comments"},
                    ]
                return [
                    {"id": "completed-job", "status": "completed", "job_type": "posts", "stage": "posts"},
                    {"id": "failed-job", "status": "failed", "job_type": "posts", "stage": "posts"},
                    {"id": "queued-job", "status": "queued", "job_type": "comments", "stage": "comments"},
                ]
            return []

    fake_pg = MixedStatusPg()
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.execute_run_cleanup("80cf0056-7659-4203-b5f9-0758ee9d98c0")

    assert result.duplicate_open_job_ids == []
    run_update_params = fake_pg.writes[-1][1]
    assert run_update_params[0] == 1
    assert run_update_params[1] == 1
    assert run_update_params[2] == 1
    assert json.loads(run_update_params[3]) == {
        "posts": {"total": 2, "completed": 1, "failed": 1, "active": 0},
        "comments": {"total": 1, "completed": 0, "failed": 0, "active": 1},
    }
    summary_payload = json.loads(run_update_params[-2])
    assert summary_payload["active_jobs"] == 1
    assert summary_payload["completed_jobs"] == 1
    assert summary_payload["failed_jobs"] == 1
    assert summary_payload["stage_counts"]["posts"]["failed"] == 1

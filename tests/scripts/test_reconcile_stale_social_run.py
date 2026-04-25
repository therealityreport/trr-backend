from __future__ import annotations

from dataclasses import dataclass, field

from scripts.socials import reconcile_stale_social_run as subject


@dataclass
class FakePg:
    rows: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    writes: list[tuple[str, list[object]]] = field(default_factory=list)

    def fetch_one(self, query: str, params: list[object]):
        normalized = " ".join(query.lower().split())
        if "count(*)::int as open_jobs" in normalized:
            return {"open_jobs": len(self.fetch_all(query, params))}
        if "from social.scrape_runs" in normalized:
            return {
                "id": "80cf0056-7659-4203-b5f9-0758ee9d98c0",
                "status": "queued",
                "total_jobs": 2,
                "active_jobs": 2,
            }
        return None

    def fetch_all(self, query: str, params: list[object]):
        normalized = " ".join(query.lower().split())
        if "from social.scrape_jobs" in normalized:
            return [
                {
                    "id": "retry-job",
                    "status": "retrying",
                    "job_type": "shared_account_posts",
                    "last_error_code": "shared_stage_failed",
                },
                {
                    "id": "queued-job",
                    "status": "queued",
                    "job_type": "shared_account_posts",
                    "last_error_code": None,
                },
            ]
        return []

    def execute(self, query: str, params: list[object]) -> None:
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
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "update social.scrape_jobs" in joined_sql
    assert "status = 'cancelled'" in joined_sql
    assert "update social.scrape_runs" in joined_sql


def test_partitioned_same_type_jobs_are_not_duplicates(monkeypatch):
    @dataclass
    class PartitionedPg(FakePg):
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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
        def fetch_all(self, query: str, params: list[object]):
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


def test_execute_cleanup_marks_active_run_terminal_when_no_open_jobs_remain(monkeypatch):
    @dataclass
    class StaleActivePg(FakePg):
        open_fetch_count: int = 0

        def fetch_one(self, query: str, params: list[object]):
            normalized = " ".join(query.lower().split())
            if "count(*)::int as open_jobs" in normalized:
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
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "status = case" in joined_sql
    assert "then 'completed'" in joined_sql
    assert "completed_at = case" in joined_sql


def test_execute_cleanup_terminalizes_cancelling_run_when_no_open_jobs_remain(monkeypatch):
    @dataclass
    class StaleCancellingPg(FakePg):
        def fetch_one(self, query: str, params: list[object]):
            normalized = " ".join(query.lower().split())
            if "count(*) filter (where status = any(%s))::int as open_jobs" in normalized:
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
    assert "cancelling" in run_update_params[2]
    joined_sql = "\n".join(sql for sql, _params in fake_pg.writes)
    assert "then 'completed'" in joined_sql

from __future__ import annotations

from dataclasses import dataclass, field

from scripts.socials import reconcile_stale_social_run as subject


@dataclass
class FakePg:
    rows: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    writes: list[tuple[str, list[object]]] = field(default_factory=list)

    def fetch_one(self, query: str, params: list[object]):
        normalized = " ".join(query.lower().split())
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

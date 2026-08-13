from __future__ import annotations

import hashlib
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PREDECESSOR = REPO_ROOT / "supabase/migrations/20260629140000_instagram_comments_public_proxy_budget_ledger.sql"
MIGRATION = REPO_ROOT / "supabase/migrations/20260806133000_reconcile_instagram_comments_public_proxy_budget_ledger.sql"


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8").lower()).strip()


def test_published_predecessor_is_unchanged_and_forward_version_is_unique() -> None:
    assert hashlib.sha256(PREDECESSOR.read_bytes()).hexdigest() == (
        "74bee84772ff559b54541e4eaa5d5fe1c73af39f2ae6be28c788a8aae14030d7"
    )
    assert MIGRATION.name.startswith("20260806133000_")


def test_reconciliation_is_transactional_idempotent_and_fail_closed() -> None:
    sql = _sql()

    assert sql.startswith("-- reconciles the published 20260629140000 predecessor")
    assert "begin;" in sql
    assert "set local lock_timeout = '5s';" in sql
    assert "set local statement_timeout = '60s';" in sql
    assert sql.endswith("commit;")
    assert "refuses an unrecognized hybrid schema" in sql
    assert "if canonical_state then return;" in sql
    assert "if not predecessor_state then" in sql
    assert "social.scrape_jobs" in sql
    assert "social.scrape_runs" in sql
    assert "dependency.deptype = 'a'" in sql
    assert "sequence_relation.relkind = 's'" in sql


def test_reconciliation_regresses_diagnosed_delimiter_and_primary_key_vector_defects() -> None:
    raw_sql = MIGRATION.read_text(encoding="utf-8")

    assert raw_sql.count("= 'NaN'::numeric") == 3
    assert "''NaN''::numeric" not in raw_sql
    assert "conkey = ARRAY[1]::smallint[]" in raw_sql
    assert "conkey::text = '1'" not in raw_sql
    assert raw_sql.count("a.attidentity::text") == 2
    assert re.search(r"a\.attidentity(?!::text)", raw_sql) is None


def test_reconciliation_accepts_only_complete_predecessor_or_target_signatures() -> None:
    sql = _sql()

    # The predecessor guard must distinguish the three old secondary-index
    # definitions from a hybrid that merely reuses their names and key slots.
    for digest in (
        "01fda201fef66a33601f31a80d1b1199",
        "06d54d9c65408de1db63f3d083dbe889",
        "8b4121bcc00298b71942747bc4572031",
        "3d157544840ec584feb47cdb1da1bbff",
    ):
        assert digest in sql

    # Both known states require complete catalog identities, not only object
    # names. The target's three secondary indexes must also be valid/ready.
    assert sql.count("md5(pg_catalog.pg_get_indexdef(i.indexrelid))") == 8
    assert "not condeferrable" in sql
    assert "not condeferred" in sql
    assert "and canonical_sequence" in sql
    assert "and not canonical_sequence" in sql


def test_reconciliation_is_catalog_only_and_rejects_unsafe_existing_rows() -> None:
    sql = _sql()

    for forbidden in (
        "insert into social.instagram_comments_public_proxy_budget_ledger",
        "update social.instagram_comments_public_proxy_budget_ledger",
        "delete from social.instagram_comments_public_proxy_budget_ledger",
        "truncate social.instagram_comments_public_proxy_budget_ledger",
        "drop table social.instagram_comments_public_proxy_budget_ledger",
        "drop column",
    ):
        assert forbidden not in sql

    assert "cannot be converted without changing data" in sql
    assert "without required foreign-key targets" in sql
    assert "trunc(usd_per_gb, 6)" in sql
    assert "trunc(estimated_usd, 6)" in sql
    assert "trunc(budget_usd, 6)" in sql


def test_reconciliation_covers_the_accepted_proxy_target_contract() -> None:
    sql = _sql()

    for fragment in (
        "alter column id drop identity",
        "create sequence social.instagram_comments_public_proxy_budget_ledger_id_seq",
        "alter column usd_per_gb type numeric(12,6)",
        "alter column estimated_usd type numeric(12,6)",
        "alter column budget_usd type numeric(12,6)",
        "instagram_comments_public_proxy_budg_proxy_cdn_bytes_leak_check",
        "instagram_comments_public_proxy_budget__proxy_bytes_total_check",
        "instagram_comments_public_proxy_budget_ledg_request_count_check",
        "instagram_comments_public_proxy_budget_ledger_job_id_fkey",
        "instagram_comments_public_proxy_budget_ledger_run_id_fkey",
        "idx_ig_comments_public_proxy_budget_account_recorded",
        "idx_ig_comments_public_proxy_budget_job",
        "idx_ig_comments_public_proxy_budget_run_recorded",
        "instagram_comments_public_proxy_budget_ledger_service_role_all",
    ):
        assert fragment in sql

    assert sql.count("drop index social.ig_comments_proxy_budget_ledger_") == 3
    assert sql.count("create index idx_ig_comments_public_proxy_budget_") == 3

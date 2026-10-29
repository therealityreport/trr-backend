from datetime import UTC, datetime

import pytest

from trr_backend.ingestion.domain import RunIntent
from trr_backend.ingestion.recovery_config import certification_dates, recovery_value


def test_configuration_accepts_generic_and_legacy_but_rejects_conflicts():
    assert recovery_value("RECOVERY_RUN_ID", environ={"TRR_INSTAGRAM_RECOVERY_RUN_ID": "run"}) == "run"
    assert recovery_value("RECOVERY_RUN_ID", environ={"TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID": "run"}) == "run"
    with pytest.raises(ValueError, match="Conflicting"):
        recovery_value(
            "RECOVERY_RUN_ID",
            environ={"TRR_INSTAGRAM_RECOVERY_RUN_ID": "new", "TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID": "old"},
        )


@pytest.mark.parametrize(
    "start,end", [("2024-02-28", "2024-03-02"), ("2025-12-25", "2026-01-04"), ("2026-10-02", "2026-10-03")]
)
def test_certification_uses_exact_frozen_range(start, end):
    intent = RunIntent(
        account="other_account",
        scope="bounded_window",
        date_start=datetime.fromisoformat(start).replace(tzinfo=UTC),
        date_end=datetime.fromisoformat(end).replace(tzinfo=UTC),
    )
    a, b = certification_dates(intent.payload(), intent.digest, intent.account)
    assert a.startswith(start) and b.startswith(end)
    with pytest.raises(ValueError, match="binding"):
        certification_dates(intent.payload(), "0" * 64, intent.account)


def test_full_history_is_not_mislabeled_as_a_date_range():
    intent = RunIntent(account="other_account")
    with pytest.raises(ValueError, match="explicit date range"):
        certification_dates(intent.payload(), intent.digest, intent.account)


def test_overrides_clear_inherited_certification_and_replace_both_aliases():
    from trr_backend.ingestion.recovery_config import recovery_overrides

    env = {"TRR_INSTAGRAM_CERTIFICATION_OUTPUT": "old", "TRR_INSTAGRAM_JANUARY_CERTIFICATION_OUTPUT": "old"}
    env.update(recovery_overrides({"CERTIFICATION_OUTPUT": "", "RECOVERY_RUN_ID": "new"}))
    assert recovery_value("CERTIFICATION_OUTPUT", environ=env) == ""
    assert recovery_value("RECOVERY_RUN_ID", environ=env) == "new"


def test_prior_window_requires_exact_evidence_before_a_new_grant(tmp_path):
    import json

    from scripts.ingestion.recover_january_window import validate_predecessor

    latest = {
        "run_id": "run",
        "account": "account",
        "generation": 7,
        "window_id": "window",
        "expected_intent_hash": "hash",
        "source_revision": "revision",
    }
    validate_predecessor(None, None)
    with pytest.raises(RuntimeError, match="exact predecessor"):
        validate_predecessor(latest, None)
    (tmp_path / "window.json").write_text(json.dumps(latest | {"generation": 5}))
    with pytest.raises(RuntimeError, match="latest durable"):
        validate_predecessor(latest, tmp_path)
    (tmp_path / "window.json").write_text(json.dumps(latest))
    validate_predecessor(latest, tmp_path)

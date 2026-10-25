"""One-shot media repair acceptance against private PostgreSQL and files only."""

import hashlib
import json
import plistlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_local_execution as local
from trr_backend.ingestion import media_repair_budget as repair
from trr_backend.ingestion.domain import (
    Conflict,
    Failure,
    LostOwnership,
    RunIntent,
    SourceProfile,
    Stage,
    fingerprint,
    stable_json,
)
from trr_backend.ingestion.persistence import Store

base_database = local.base_database
database = local.database
clean = local.clean


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def qualify(tmp_path, monkeypatch, request):
    root = tmp_path / "ig-fixture-reviewed-repair"
    runtime = root / "trr_backend/ingestion/fixture.py"
    runtime.parent.mkdir(parents=True, exist_ok=True)
    runtime.write_text("# isolated reviewed runtime fixture\n")
    freeze = tmp_path / "freeze.json"
    freeze.write_text(json.dumps({"files": [{"path": "trr_backend/ingestion/fixture.py", "sha256": digest(runtime)}]}))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "source_identity": root.name,
                "release_path": str(root),
                "files": {"trr_backend/ingestion/fixture.py": digest(runtime)},
            }
        )
    )
    review = tmp_path / "review.json"
    review.write_text(json.dumps({"accepted": True, "source_identity": root.name, "freeze_sha256": digest(freeze)}))
    installed_binding = tmp_path / "installed-binding.json"
    installed_binding.write_text(
        json.dumps(
            {
                "release_path": str(root),
                "source_identity": root.name,
                "successor_run_id": request["run_id"],
                "successor_intent_hash": request["intent_hash"],
                "expected_generation": request["generation"],
            }
        )
    )
    plist = tmp_path / "worker.plist"
    plist.write_bytes(
        plistlib.dumps(
            {
                "WorkingDirectory": str(root),
                "ProgramArguments": [
                    "/bin/zsh",
                    "-lc",
                    "export TRR_INSTAGRAM_JANUARY_BINDING=" + str(installed_binding),
                ],
            }
        )
    )
    monkeypatch.setattr(repair, "WORKER_PLIST", plist)
    proof = tmp_path / "eligibility.json"
    proof.write_text(
        json.dumps({"schema": "fixture-saved-owner-plan-v1", "target": request["target_key"], "eligible": True})
    )
    qualified = {
        "repair_revision": root.name,
        "installed_release": str(root.resolve()),
        "manifest_sha256": digest(manifest),
        "review_sha256": digest(review),
        "freeze_sha256": digest(freeze),
        "verified_runtime_files": 1,
        "installed_binding_sha256": digest(installed_binding),
        "installed_run_id": request["run_id"],
        "installed_intent_hash": request["intent_hash"],
        "installed_generation": request["generation"],
        "running_process_verification": "separate parent qualification required",
    }
    approval = tmp_path / "diagnostic.json"
    approval.write_text(
        json.dumps(
            {
                "version": 1,
                "approved": True,
                "qualification_sha256": fingerprint(qualified),
                **{
                    key: request[key]
                    for key in ("run_id", "account", "generation", "intent_hash", "target_key", "failed_revision")
                },
                "repair_revision": root.name,
                "checkpoint_sha256": request["expected_checkpoint_hash"],
                "owner_generation": request["expected_owner_generation"],
                "attempts": 3,
                "max_attempts": 3,
                "failure_code": request["expected_failure_code"],
                "eligibility_proof": {
                    "path": str(proof),
                    "sha256": digest(proof),
                    "schema_identity": "fixture-saved-owner-plan-v1",
                    "parent_reviewed_eligible": True,
                },
            }
        )
    )
    return repair.RepairQualification(
        root.name,
        str(manifest),
        digest(manifest),
        str(review),
        digest(review),
        str(freeze),
        str(approval),
        digest(approval),
    )


def row(tx, sql, params=()):
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or None)
        return [dict(r) for r in cur.fetchall()]


def target(tx, request):
    return row(
        tx,
        "select * from social.instagram_ingestion_targets where run_id=%s and stage='media' and target_key=%s",
        (request["run_id"], request["target_key"]),
    )[0]


def setup(database, tmp_path, monkeypatch, *, failure_code="avatar_refresh_asset_unresolved"):
    _, _, tx = database
    store = Store(tx, execution_placement="local")
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        selected_tasks=(Stage.MEDIA,),
        source_profile=SourceProfile.for_mode("public"),
    )
    run = store.start(
        intent, idempotency_key=str(uuid4()), engine="dbos", workflow_version="fixture", initiated_by="fixture"
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s", (run["run_id"],)
        )
    key = "DUDfixture"
    owned = {"run_id": run["run_id"], "account": "bravotv", "generation": 1, "target_key": key}
    store.discover(
        **{k: v for k, v in owned.items() if k != "target_key"},
        stage=Stage.MEDIA,
        target_keys=[key],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="one",
        seal=True,
    )
    checkpoint = {"public_recovery": {"no_improvement_passes": 2}, "retained": "unchanged"}
    for _ in range(3):
        token = str(store.reserve(**owned, stage=Stage.MEDIA)["owner_token"])
        store.acquire(**owned, stage=Stage.MEDIA, owner_token=token)
        store.checkpoint_failure(
            **owned,
            stage=Stage.MEDIA,
            owner_token=token,
            checkpoint=checkpoint,
            requests=1,
            failure=Failure(failure_code),
        )
    saved = row(
        tx, "select * from social.instagram_ingestion_targets where run_id=%s and stage='media'", (run["run_id"],)
    )[0]
    assert all(saved[name] is None for name in ("owner_token", "owner_started_at", "owner_expires_at"))
    request = {
        **owned,
        "intent_hash": intent.digest,
        "expected_checkpoint_hash": fingerprint(saved["checkpoint"]),
        "expected_owner_generation": saved["owner_generation"],
        "expected_attempts": 3,
        "expected_max_attempts": 3,
        "expected_failure_code": failure_code,
        "failed_revision": "fixture-broken-revision",
        "reason": "Reviewed exact saved-owner repair",
        "initiated_by": "fixture-parent",
    }
    request["qualification"] = qualify(tmp_path, monkeypatch, request)
    return store, tx, request


def test_plan_is_read_only_and_apply_changes_only_budget_with_durable_receipt(database, tmp_path, monkeypatch):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    before = target(tx, request)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set next_attempt_at=%s where run_id=%s and stage='media'",
            (datetime.now(UTC) + timedelta(hours=1), request["run_id"]),
        )
    before = target(tx, request)
    journal_before = row(tx, "select count(*) n from social.instagram_ingestion_commands")[0]["n"]
    plan = repair.extend_media_repair_budget(store, **request)
    assert not plan["applied"] and target(tx, request) == before
    assert row(tx, "select count(*) n from social.instagram_ingestion_commands")[0]["n"] == journal_before
    result = repair.extend_media_repair_budget(store, **request, apply=True)
    after = target(tx, request)
    assert after == {**before, "max_attempts": 4}
    assert result["before"]["max_attempts"] == 3 and result["after"]["max_attempts"] == 4
    assert result["qualification"]["eligibility_proof_schema_identity"] == "fixture-saved-owner-plan-v1"
    assert repair.extend_media_repair_budget(store, **request, apply=True) == result
    assert (
        store.reserve(request["run_id"], "bravotv", generation=1, stage=Stage.MEDIA, target_key=request["target_key"])
        is None
    )
    with pytest.raises(Conflict):
        repair.extend_media_repair_budget(store, **{**request, "reason": "new caller text"}, apply=True)


@pytest.mark.parametrize(
    "change",
    [
        "checkpoint",
        "owner_generation",
        "attempts",
        "max_attempts",
        "blocked",
        "owner_token",
        "owner_started_at",
        "owner_expires_at",
        "failure",
        "settled",
    ],
)
def test_target_state_changes_are_fenced(database, tmp_path, monkeypatch, change):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    changes = {
        "checkpoint": ("checkpoint", "{}"),
        "owner_generation": ("owner_generation", 88),
        "attempts": ("attempts", 2),
        "max_attempts": ("max_attempts", 4),
        "blocked": ("blocked_reason", "source_block"),
        "owner_token": ("owner_token", str(uuid4())),
        "owner_started_at": ("owner_started_at", datetime.now(UTC)),
        "owner_expires_at": ("owner_expires_at", datetime.now(UTC) - timedelta(hours=1)),
        "failure": ("checkpoint", stable_json({"last_error_code": "other_error"})),
        "settled": ("outcome", "cached_satisfied"),
    }
    column, value = changes[change]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            f"update social.instagram_ingestion_targets set {column}=%s where run_id=%s and stage='media'",
            (value, request["run_id"]),
        )
    before = target(tx, request)
    with pytest.raises(Conflict):
        repair.extend_media_repair_budget(store, **request, apply=True)
    assert target(tx, request) == before


@pytest.mark.parametrize("state", ["reserved", "sending", "accepted"])
def test_dispatch_must_be_reaped_and_reconciled_before_grant(database, tmp_path, monkeypatch, state):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    token = str(uuid4())
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_dispatches
            (owner_token,run_id,generation,stage,target_key,state) values(%s,%s,1,'media',%s,%s)""",
            (token, request["run_id"], request["target_key"], state),
        )
    with pytest.raises(Conflict, match="reaped and reconciled"):
        repair.extend_media_repair_budget(store, **request, apply=True)
    # Model the existing coordinator's post-reap expired transition, never an
    # expiry performed by this operator. Owner release was real checkpoint_failure.
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_dispatches set state='expired' where owner_token=%s", (token,))
    assert repair.extend_media_repair_budget(store, **request, apply=True)["applied"]


@pytest.mark.parametrize(
    "column,value",
    [
        ("generation", 2),
        ("state", "blocked"),
        ("state", "cancelled"),
        ("execution_hold_reason", "held"),
        ("execution_placement", "modal"),
        ("intent_hash", "0" * 64),
    ],
)
def test_run_fences(database, tmp_path, monkeypatch, column, value):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            f"update social.instagram_ingestion_runs set {column}=%s"
            + (",state='cancelled'" if column == "execution_hold_reason" else "")
            + " where run_id=%s",
            (value, request["run_id"]),
        )
    with pytest.raises((Conflict, LostOwnership)):
        repair.extend_media_repair_budget(store, **request, apply=True)
    assert target(tx, request)["max_attempts"] == 3


def test_atomic_journal_failure_rolls_back_grant(database, tmp_path, monkeypatch):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    with tx() as conn, conn.cursor() as cur:
        cur.execute("""create function public.reject_media_repair_journal() returns trigger language plpgsql as $$
        begin if new.idempotency_key like 'media-repair:%' then
        raise exception 'synthetic fixture journal failure'; end if; return new; end$$;
        create trigger reject_media_repair_journal before insert on social.instagram_ingestion_commands
        for each row execute function public.reject_media_repair_journal()""")
    try:
        with pytest.raises(Exception, match="synthetic fixture journal failure"):
            repair.extend_media_repair_budget(store, **request, apply=True)
        assert target(tx, request)["max_attempts"] == 3
    finally:
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """drop trigger reject_media_repair_journal on social.instagram_ingestion_commands;
                drop function public.reject_media_repair_journal()"""
            )


def test_concurrent_identical_apply_grants_once(database, tmp_path, monkeypatch):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: repair.extend_media_repair_budget(store, **request, apply=True), range(2)))
    assert results[0] == results[1] and target(tx, request)["max_attempts"] == 4
    assert (
        len(
            row(
                tx, "select result from social.instagram_ingestion_commands where idempotency_key like 'media-repair:%'"
            )
        )
        == 1
    )


@pytest.mark.parametrize("failure", ["rate_limited", "public_access_backoff", "profile_pic_not_available"])
def test_nonrepair_failure_cannot_be_authorized(database, tmp_path, monkeypatch, failure):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    with pytest.raises(Conflict, match="approved technical"):
        repair.extend_media_repair_budget(store, **{**request, "expected_failure_code": failure}, apply=True)
    assert target(tx, request)["max_attempts"] == 3


@pytest.mark.parametrize(
    "part",
    [
        "runtime",
        "manifest",
        "review",
        "freeze",
        "diagnostic",
        "eligibility",
        "installed",
        "diagnostic_binding",
        "installed_binding",
    ],
)
def test_installed_review_diagnostic_and_eligibility_qualification_cannot_be_claimed(
    database, tmp_path, monkeypatch, part
):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    q = request["qualification"]
    if part == "runtime":
        (tmp_path / q.repair_revision / "trr_backend/ingestion/fixture.py").write_text("# changed\n")
    elif part == "eligibility":
        (tmp_path / "eligibility.json").write_text("{}")
    elif part == "installed_binding":
        (tmp_path / "installed-binding.json").write_text("{}")
    elif part == "installed":
        repair.WORKER_PLIST.write_bytes(plistlib.dumps({"WorkingDirectory": str(tmp_path / "wrong-release")}))
    elif part == "diagnostic_binding":
        path = Path(q.diagnostic_path)
        value = json.loads(path.read_text())
        value["target_key"] = "wrong"
        path.write_text(json.dumps(value))
        request["qualification"] = replace(q, diagnostic_sha256=digest(path))
    else:
        Path(getattr(q, part + "_path")).write_text("{}")
    with pytest.raises(Conflict):
        repair.extend_media_repair_budget(store, **request, apply=True)
    assert target(tx, request)["max_attempts"] == 3


@pytest.mark.parametrize("attempts,maximum", [(2, 3), (3, 4), (4, 4), (True, 3), (3, True)])
def test_fixed_attempt_cap_and_integer_boundary(database, tmp_path, monkeypatch, attempts, maximum):
    store, tx, request = setup(database, tmp_path, monkeypatch)
    with pytest.raises((Conflict, ValueError)):
        repair.extend_media_repair_budget(
            store, **{**request, "expected_attempts": attempts, "expected_max_attempts": maximum}, apply=True
        )
    assert target(tx, request)["max_attempts"] == 3


@pytest.mark.parametrize("inside_installed", [False, True])
@pytest.mark.parametrize(
    "path_case", ["missing", "empty", "whitespace", "null", "integer", "boolean", "object", "array"]
)
def test_binding_release_path_semantics_survive_repinned_hashes_and_operator_cwd(
    tmp_path, monkeypatch, inside_installed, path_case
):
    """A parent byte pin cannot make a malformed path semantically valid."""
    request = {
        "run_id": str(uuid4()),
        "account": "bravotv",
        "generation": 1,
        "intent_hash": "a" * 64,
        "target_key": "DUDfixture",
        "failed_revision": "fixture-old",
        "expected_checkpoint_hash": "b" * 64,
        "expected_owner_generation": 3,
        "expected_failure_code": "avatar_refresh_asset_unresolved",
    }
    q = qualify(tmp_path, monkeypatch, request)
    verified = repair.qualify_installed_repair(q)
    material = {
        key: value
        for key, value in verified.items()
        if key
        not in {
            "qualification_sha256",
            "diagnostic_sha256",
            "eligibility_proof_sha256",
            "eligibility_proof_schema_identity",
            "diagnostic_binding",
        }
    }
    path = tmp_path / "installed-binding.json"
    binding = json.loads(path.read_text())
    values = {"empty": "", "whitespace": "  ", "null": None, "integer": 7, "boolean": True, "object": {}, "array": []}
    if path_case == "missing":
        del binding["release_path"]
    else:
        binding["release_path"] = values[path_case]
    path.write_text(json.dumps(binding))
    # Keep every other field valid and update both integrity pins, so only
    # semantic path validation can reject the reviewer's reproduced boundary.
    material["installed_binding_sha256"] = digest(path)
    diagnostic = Path(q.diagnostic_path)
    approval = json.loads(diagnostic.read_text())
    approval["qualification_sha256"] = fingerprint(material)
    diagnostic.write_text(json.dumps(approval))
    q = replace(q, diagnostic_sha256=digest(diagnostic))
    monkeypatch.chdir(tmp_path / q.repair_revision if inside_installed else tmp_path)
    with pytest.raises(Conflict, match="nonempty release path string"):
        repair.qualify_installed_repair(q)


def test_reviewed_cached_alias_limit_grant_requires_exact_pinned_diagnostic(database, tmp_path, monkeypatch):
    code = "cached_avatar_alias_inventory_limit"
    store, tx, request = setup(database, tmp_path, monkeypatch, failure_code=code)
    before = target(tx, request)
    q = request["qualification"]
    approval_path = Path(q.diagnostic_path)
    original = approval_path.read_bytes()
    approval = json.loads(original)
    approval["failure_code"] = "avatar_refresh_asset_unresolved"
    approval_path.write_text(json.dumps(approval))
    # Even a parent byte pin cannot authorize a different diagnostic binding.
    wrong = replace(q, diagnostic_sha256=digest(approval_path))
    with pytest.raises(Conflict, match="diagnostic does not match"):
        repair.extend_media_repair_budget(store, **{**request, "qualification": wrong}, apply=True)
    assert target(tx, request) == before
    approval_path.write_bytes(original)
    accepted = repair.extend_media_repair_budget(store, **request, apply=True)
    assert target(tx, request) == {**before, "max_attempts": 4}
    assert accepted["before"]["last_error_code"] == code
    assert accepted["qualification"]["diagnostic_binding"]["failure_code"] == code

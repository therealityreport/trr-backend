import copy
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trr_backend.ingestion import count_witness as witness
from trr_backend.ingestion.domain import Conflict


@pytest.fixture
def binding(monkeypatch):
    intent = SimpleNamespace(
        digest=witness.INTENT_HASH, account="bravotv", effective_source_profile=SimpleNamespace(access_mode="public")
    )
    monkeypatch.setattr(witness.RunIntent, "from_payload", lambda _: intent)
    return {
        "database_now": datetime(2026, 10, 25, tzinfo=UTC),
        "run": {
            "run_id": witness.RUN_ID,
            "generation": 1,
            "intent_hash": witness.INTENT_HASH,
            "intent": {},
            "account": "bravotv",
            "execution_placement": "local",
        },
        "post": {
            "id": "canonical-post",
            "shortcode": "ABC",
            "media_id": "123",
            "username": "bravotv",
            "posted_at": "2026-01-10T00:00:00+00:00",
            "comments_count": 80,
            "scraped_at": "old-clock",
        },
        "targets": [
            {
                "run_id": witness.RUN_ID,
                "stage": "comments",
                "target_key": "ABC",
                "owner_token": "retained-terminal-receipt-token",
                "owner_expires_at": None,
                "outcome": "committed",
                "checkpoint_sha256": "checkpoint-before",
            }
        ],
        "receipts": [
            {"run_id": witness.RUN_ID, "target_key": "ABC", "receipt_id": "old-receipt", "row_sha256": "receipt-before"}
        ],
        "pages": [],
    }


class FakeGate:
    def __init__(self, **kwargs):
        assert kwargs["cookies"] == {} and kwargs["public_only"] and kwargs["network_lane"] == "local"
        self.check = kwargs["check_cancelled"]
        self.request_count = 0

    def before_request(self):
        self.check()
        self.request_count += 1

    def finish_success(self):
        pass

    def finish_failure(self):
        pass


class FakeSource:
    sends = 1
    after = None
    payload_extra = {}
    instances = []

    def __init__(self, **kwargs):
        self.gate = kwargs["gate"]
        self.instances.append(self)

    def __call__(self, *, check_cancelled, on_request, source_observer):
        for _ in range(self.sends):
            self.gate.before_request()
            on_request()
        payload = {
            "id": "123",
            "shortcode": "ABC",
            "comment_count": 0,
            "owner": {"username": "bravotv"},
            "sessionid": "excluded",
        }
        payload.update(self.payload_extra)
        source_observer("details", payload)
        if type(self).after:
            type(self).after()
        return payload

    def finish_success(self):
        self.gate.finish_success()

    def finish_failure(self, error):
        self.gate.finish_failure()


@pytest.fixture
def execute(binding, tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    monkeypatch.setattr(FakeSource, "sends", 1)
    monkeypatch.setattr(FakeSource, "after", None)
    monkeypatch.setattr(FakeSource, "payload_extra", {})
    monkeypatch.setattr(FakeSource, "instances", [])

    def call(**kwargs):
        defaults = {
            "transaction": lambda: None,
            "source_revision": {"git_head": "revision"},
            "apply": True,
            "artifact_directory": tmp_path,
            "binding_reader": lambda *_: copy.deepcopy(binding),
            "gate_factory": FakeGate,
            "source_factory": FakeSource,
        }
        defaults.update(kwargs)
        return witness.refresh_witnesses(witness.WitnessRequest(("ABC",)), **defaults)

    return call


def test_auxiliary_zero_count_preserves_all_canonical_state(binding, execute, tmp_path):
    before = copy.deepcopy(binding)
    result = execute()
    artifact = json.loads(next(tmp_path.glob("*.json")).read_text())
    assert result["status"] == "observed" and artifact["native_count"] == 0
    assert artifact["certification"] is False
    assert "sessionid" not in artifact["native_evidence"]
    assert artifact["source_revision"]["git_head"] == "revision"
    assert next(tmp_path.glob("*.json")).stat().st_mode & 0o777 == 0o600
    assert binding == before


@pytest.mark.parametrize(
    "payload",
    [
        {"id": "123", "shortcode": "ABC"},
        {"id": "123", "shortcode": "ABC", "comment_count": -1},
        {"id": "123", "shortcode": "ABC", "comment_count": True},
        {"id": "123", "shortcode": "ABC", "comment_count": "0"},
        {"id": "999", "shortcode": "ABC", "comment_count": 0},
        {"id": "123", "shortcode": "OTHER", "comment_count": 0},
        {"id": "123", "shortcode": "ABC", "comment_count": 1, "edge_media_to_comment": {"count": 2}},
    ],
)
def test_requires_native_identity_and_explicit_count(binding, payload):
    with pytest.raises((Conflict, ValueError)):
        witness.native_count(payload, binding["post"])


def test_supported_native_envelope(binding):
    payload = {
        "graphql": {"shortcode_media": {"id": "123", "shortcode": "ABC", "edge_media_to_parent_comment": {"count": 8}}}
    }
    assert witness.native_count(payload, binding["post"]) == 8


def test_preview_never_constructs_source(execute, tmp_path):
    assert execute(apply=False)["status"] == "preview"
    assert not FakeSource.instances and not list(tmp_path.iterdir())


def test_fourth_send_rejected_before_dispatch(execute, monkeypatch, tmp_path):
    monkeypatch.setattr(FakeSource, "sends", 4)
    with pytest.raises(Conflict, match="send limit"):
        execute()
    assert FakeSource.instances[0].gate.request_count == 3
    assert not list(tmp_path.iterdir())


def test_exactly_three_sends_allowed(execute, monkeypatch):
    monkeypatch.setattr(FakeSource, "sends", 3)
    assert execute()["results"][0]["sends"] == 3


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("run", "generation", 2),
        ("post", "comments_count", 99),
        ("run", "execution_placement", "modal"),
    ],
)
def test_post_request_drift_rejects_artifact(binding, execute, monkeypatch, tmp_path, section, key, value):
    monkeypatch.setattr(FakeSource, "after", lambda: binding[section].__setitem__(key, value))
    with pytest.raises(Conflict):
        execute()
    assert not list(tmp_path.iterdir())


def test_checkpoint_and_receipt_drift_rejected(binding, execute, monkeypatch):
    def change():
        binding["targets"][0]["checkpoint_sha256"] = "checkpoint-after"
        binding["receipts"].append({"run_id": witness.RUN_ID, "target_key": "ABC", "row_sha256": "new"})

    monkeypatch.setattr(FakeSource, "after", change)
    with pytest.raises(Conflict, match="binding drift"):
        execute()


def test_active_owner_rejected(binding, execute):
    binding["targets"][0].update(owner_token="owner", outcome="unresolved")
    with pytest.raises(Conflict, match="owner"):
        execute()
    assert not FakeSource.instances


def test_total_deadline_rejected(execute):
    ticks = iter([0, 91])
    with pytest.raises(Conflict, match="deadline"):
        execute(clock=lambda: next(ticks))


@pytest.mark.parametrize(
    "codes,sends,seconds",
    [
        ((), 3, 90),
        (("A",) * 2, 3, 90),
        (tuple("ABCDEF"), 3, 90),
        (("A",), 4, 90),
        (("A",), 3, 91),
        (("A",), 3, float("nan")),
    ],
)
def test_request_bounds(codes, sends, seconds):
    with pytest.raises(ValueError):
        witness.WitnessRequest(codes, sends, seconds)


def test_read_snapshot_and_database_deadlines_are_read_only(binding, execute):
    from contextlib import contextmanager

    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            statements.append((sql.strip().lower(), params))

        def fetchone(self):
            return copy.deepcopy(binding)

    class Connection:
        def cursor(self, **kwargs):
            return Cursor()

    @contextmanager
    def transaction():
        yield Connection()

    assert execute(transaction=transaction, binding_reader=witness.read_binding)["status"] == "observed"
    assert any(sql.startswith("select") for sql, _ in statements)
    assert any(sql.startswith("set local statement_timeout") for sql, _ in statements)
    assert all(sql.startswith(("select", "set local")) for sql, _ in statements)


@pytest.mark.parametrize("change", ["checkpoint", "receipt", "page", "duplicate_page"])
def test_compact_binding_detects_every_row_change(binding, execute, monkeypatch, change):
    binding["pages"] = [
        {"run_id": witness.RUN_ID, "target_key": "ABC", "page_id": "page-one", "row_sha256": "page-before"}
    ]

    def mutate():
        if change == "checkpoint":
            binding["targets"][0]["checkpoint_sha256"] = "checkpoint-after"
        elif change == "receipt":
            binding["receipts"][0]["row_sha256"] = "receipt-after"
        elif change == "page":
            binding["pages"][0]["row_sha256"] = "page-after"
        else:
            binding["pages"].append(copy.deepcopy(binding["pages"][0]))

    monkeypatch.setattr(FakeSource, "after", mutate)
    with pytest.raises(Conflict, match="binding drift"):
        execute()


@pytest.mark.parametrize("outcome", ["committed", "cached_satisfied", "source_unavailable"])
def test_terminal_receipt_token_without_lease_is_idle(binding, execute, outcome):
    binding["targets"][0].update(outcome=outcome, owner_token="receipt-fence", owner_expires_at=None)
    assert execute()["status"] == "observed"


@pytest.mark.parametrize(
    "outcome,token", [("committed", "receipt-fence"), ("unresolved", "owner"), ("cached_satisfied", None)]
)
def test_unexpired_owner_lease_rejected(binding, execute, outcome, token):
    binding["targets"][0].update(outcome=outcome, owner_token=token, owner_expires_at="2026-10-26T00:00:00+00:00")
    with pytest.raises(Conflict, match="active or uncertain owner"):
        execute()
    assert not FakeSource.instances


def test_expired_unresolved_lease_is_idle(binding, execute):
    binding["targets"][0].update(
        outcome="unresolved", owner_token="expired-owner", owner_expires_at="2026-10-24T00:00:00+00:00"
    )
    assert execute()["status"] == "observed"


@pytest.mark.parametrize("disabled", [True, False, None])
def test_comments_disabled_retains_explicit_status_with_native_count(execute, tmp_path, monkeypatch, disabled):
    if disabled is not None:
        monkeypatch.setattr(FakeSource, "payload_extra", {"comments_disabled": disabled})
    execute()
    artifact = json.loads(next(tmp_path.glob("*.json")).read_text())
    assert artifact["comments_disabled"] is disabled
    assert artifact["native_count"] == 0


def test_comments_disabled_does_not_manufacture_missing_count(binding):
    with pytest.raises(ValueError, match="missing"):
        witness.native_count({"id": "123", "shortcode": "ABC", "comments_disabled": True}, binding["post"])


@pytest.mark.parametrize("stalled", [False, True])
def test_cli_supervision_bounds_stalled_child_and_reaps_owned_group(monkeypatch, tmp_path, stalled):
    import signal
    import subprocess

    from scripts.ingestion import refresh_count_witnesses as cli

    successful = tmp_path / "successful-private-artifact.json"
    successful.write_text("retained")
    waits, signals, starts = [], [], []

    class Child:
        pid = 7654321

        def wait(self, *, timeout):
            waits.append(timeout)
            if stalled and len(waits) < 3:
                raise subprocess.TimeoutExpired("owned-child", timeout)
            return 0

    def launch(command, **kwargs):
        starts.append((command, kwargs))
        return Child()

    monkeypatch.setattr(cli.subprocess, "Popen", launch)
    monkeypatch.setattr(cli.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    prior_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    result = cli.supervise(["--shortcode", "ABC"], seconds=90, started=10, clock=lambda: 10)
    assert {sig: signal.getsignal(sig) for sig in prior_handlers} == prior_handlers
    assert starts[0][1]["start_new_session"] is True
    assert result == (124 if stalled else 0)
    assert waits == ([90, 1, 2] if stalled else [90, 1, 2])
    assert signals == [(7654321, signal.SIGTERM), (7654321, signal.SIGKILL)]
    assert successful.read_text() == "retained"


@pytest.mark.parametrize("signum", [15, 2, 9])
def test_actual_supervisor_signal_and_parent_loss(tmp_path, signum):
    import os
    import subprocess
    import sys
    import time

    from scripts.ingestion import refresh_count_witnesses as cli

    ready = tmp_path / "child-ready"
    artifact = tmp_path / "successful-private.json"
    artifact.write_text("preserved")
    fake_work = (
        "def child_main(argv):\n"
        "    from pathlib import Path\n"
        f"    Path({str(ready)!r}).write_text(str(os.getpid()))\n"
        "    time.sleep(30)\n"
        "    return 0"
    )
    guarded = cli._GUARDED_CHILD_CODE.replace(
        "from scripts.ingestion.refresh_count_witnesses import child_main", fake_work
    )
    script = (
        "import time; from scripts.ingestion import refresh_count_witnesses as cli; "
        f"cli._GUARDED_CHILD_CODE={guarded!r}; "
        "raise SystemExit(cli.supervise([], seconds=10, started=time.monotonic()))"
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=cli.Path(cli.__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    child_pid = None

    def running(pid):
        result = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
        return result.returncode == 0 and result.stdout.strip() and not result.stdout.strip().startswith("Z")

    try:
        end = time.monotonic() + 4
        while not ready.exists() and time.monotonic() < end:
            time.sleep(0.01)
        assert ready.exists(), parent.poll()
        child_pid = int(ready.read_text())
        start = time.monotonic()
        os.kill(parent.pid, signum)
        output, errors = parent.communicate(timeout=4)
        assert parent.returncode == (-9 if signum == 9 else 128 + signum), errors.decode()
        if signum != 9:
            assert b'"child_reaped": true' in output
            assert time.monotonic() - start < 2.5
        end = time.monotonic() + 2
        while running(child_pid) and time.monotonic() < end:
            time.sleep(0.01)
        assert not running(child_pid)
        assert artifact.read_text() == "preserved"
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=2)
        if child_pid and running(child_pid):
            os.killpg(child_pid, 9)


@pytest.mark.parametrize("stall", [False, True])
def test_actual_guarded_child_success_and_independent_deadline(stall):
    import os
    import subprocess
    import sys
    import time

    from scripts.ingestion import refresh_count_witnesses as cli

    work = "def child_main(argv):\n    time.sleep(30)\n    return 0" if stall else "def child_main(argv): return 0"
    guarded = cli._GUARDED_CHILD_CODE.replace("from scripts.ingestion.refresh_count_witnesses import child_main", work)
    child = subprocess.Popen(
        [sys.executable, "-c", guarded, str(os.getpid()), str(time.monotonic() + 0.3)], start_new_session=True
    )
    try:
        assert child.wait(timeout=2) == (124 if stall else 0)
    finally:
        if child.poll() is None:
            os.killpg(child.pid, 9)
            child.wait(timeout=2)


@pytest.mark.parametrize("failure", [RuntimeError("wait failed"), SystemExit(7)])
def test_supervisor_wait_failure_always_cleans_and_restores(monkeypatch, failure):
    import signal

    from scripts.ingestion import refresh_count_witnesses as cli

    waits, signals = [], []

    class Child:
        pid = 7654321

        def wait(self, *, timeout):
            waits.append(timeout)
            if len(waits) == 1:
                raise failure
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: Child())
    monkeypatch.setattr(cli.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    prior_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    with pytest.raises(type(failure)):
        cli.supervise([], seconds=90, started=10, clock=lambda: 10)
    assert waits == [90, 1, 2]
    assert signals == [(7654321, signal.SIGTERM), (7654321, signal.SIGKILL)]
    assert {sig: signal.getsignal(sig) for sig in prior_handlers} == prior_handlers

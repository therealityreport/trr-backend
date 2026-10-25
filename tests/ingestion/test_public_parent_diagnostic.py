"""Owned diagnostic integration with maintained parsing and fake HTTP only."""

import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from trr_backend.ingestion import public_parent_diagnostic as diagnostic
from trr_backend.ingestion.comment_transport import PublicRelayCommentSource
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.domain import Failure, LostOwnership
from trr_backend.ingestion.identity_policy import IdentityDeferred, RequestPermit
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.comments_scrapling import fetcher as maintained


def native_page():
    return {
        "data": {
            "xdt_api__v1__media__media_id__comments__connection": {
                "edges": [{"node": {"pk": "p", "text": "hello", "user": {"username": "fixture"}}}],
                "count": 17,
                "page_info": {"has_next_page": True, "end_cursor": "private-native-cursor"},
            }
        }
    }


@pytest.fixture
def plan(tmp_path, monkeypatch):
    # Controlled wall time makes the frozen run's boundary deterministic.
    wall = [datetime(2026, 10, 25, 22, 55, tzinfo=UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return wall[0]

    monkeypatch.setattr(diagnostic, "datetime", Clock)
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    value = dict(diagnostic.FROZEN, deadline_at="2026-10-25T23:00:00Z", output_dir=str(output))
    path = tmp_path / "plan.json"

    def save():
        if path.exists():
            path.chmod(0o600)
        raw = json.dumps(value).encode()
        path.write_bytes(raw)
        path.chmod(0o400)
        monkeypatch.setenv(diagnostic.PLAN_ENV, str(path))
        monkeypatch.setenv(diagnostic.SHA_ENV, hashlib.sha256(raw).hexdigest())

    save()
    return SimpleNamespace(value=value, path=path, output=output, save=save, wall=wall)


class Gate:
    public_only = True
    network_lane = "local"

    def __init__(self):
        self.deadline = time.monotonic() + 10
        self.request_count = 0
        self.successes, self.failures = [], []

    def before_request(self):
        self.request_count += 1
        return RequestPermit("public:local", 0, self.request_count, None, "owned-test")

    def check_cancelled(self):
        pass

    def record_success(self, *, permit):
        self.successes.append(permit.request_number)

    def record_failure(self, code, *, permit):
        self.failures.append((code, permit.request_number))

    def finish_failure(self, code=None, **kwargs):
        self.failures.append((code, kwargs))
        return Failure(code) if code else None


def owned(gate, factory):
    charges, ordinary = [], []

    def before():
        charges.append("owned-request")

    before.run_id = diagnostic.RUN_ID
    before.run_generation = 1
    before.deadline = time.monotonic() + 10
    before.owner_generation = 9
    source = _GatedSource(PublicRelayCommentSource(fetcher_factory=factory), gate, failure_code="comment_fetch_failed")

    def run(checkpoint=None, shortcode=diagnostic.SHORTCODE):
        return source(
            shortcode=shortcode,
            checkpoint=checkpoint or {},
            pre_source_request=before,
            source_observer=lambda *args: ordinary.append(args),
        )

    return run, before, charges, ordinary


@pytest.fixture
def fake_transport(monkeypatch):
    sends, options, ordinary_checkpoints, closed = [], [], [], []
    response_specs = []
    bootstrap_specs = []
    response_hook, close_hook = [None], [None]

    class Client:
        cookies = {}

        def __init__(self, **kwargs):
            assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
            assert kwargs["proxy"] is None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, **kwargs):
            sends.append(("bootstrap", url))
            spec = bootstrap_specs.pop(0) if bootstrap_specs else {"text": "private-bootstrap-lsd"}
            return httpx.Response(request=httpx.Request("GET", url), **{"status_code": 200, **spec})

        async def post(self, url, *, data, headers):
            sends.append(("graphql", data["doc_id"], json.loads(data["variables"])))
            spec = response_specs.pop(0) if response_specs else {"json": native_page()}
            response = httpx.Response(request=httpx.Request("POST", url), **{"status_code": 200, **spec})
            if response_hook[0]:
                response_hook[0]()
            return response

    monkeypatch.setattr(maintained.httpx, "AsyncClient", Client)
    monkeypatch.setattr(maintained, "_extract_logged_out_graphql_context", lambda *a, **kw: {"lsd": "NEVER-RETAIN"})

    async def no_wait(*args, **kwargs):
        return True

    monkeypatch.setattr(maintained, "_sleep_before_deadline", no_wait)

    class Fetcher(maintained.InstagramCommentsScraplingFetcher):
        def __init__(self, **kwargs):
            options.append(kwargs)
            super().__init__(**kwargs)

        async def _pace_api_requests(self, **kwargs):
            return True

        async def fetch_comments_for_shortcode(self, shortcode, **kwargs):
            ordinary_checkpoints.append(kwargs)
            return SimpleNamespace(
                comments=[],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                reported_comment_count=None,
                top_level_checkpoint={},
                reply_checkpoints=[],
            )

        async def aclose(self):
            closed.append(True)
            if close_hook[0]:
                close_hook[0]()

    return SimpleNamespace(
        factory=Fetcher,
        sends=sends,
        options=options,
        specs=response_specs,
        bootstrap_specs=bootstrap_specs,
        response_hook=response_hook,
        close_hook=close_hook,
        ordinary=ordinary_checkpoints,
        closed=closed,
    )


def test_two_maintained_root_queries_share_four_permits_and_preserve_checkpoint(plan, fake_transport):
    gate = Gate()
    run, before, charges, ordinary = owned(gate, fake_transport.factory)
    checkpoint = {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "lanes": {
                "__top__": {
                    "relay_doc_id": diagnostic.DOC_IDS[0],
                    "relay_friendly_name": "PolarisPostCommentsPaginationQuery",
                    "sort_order": "recent",
                    "page_size": 12,
                    "cursors": ["saved-after"],
                    "cursor_param": "after",
                    "exhausted": False,
                    "started_from_root": True,
                }
            },
        },
    }
    original = json.loads(json.dumps(checkpoint))
    run(checkpoint)
    assert checkpoint == original
    assert len(fake_transport.sends) == len(charges) == gate.request_count == 4
    assert gate.successes == [1, 2, 3, 4] and gate.failures == []
    assert ordinary == []  # Diagnostic never enters canonical observation.
    assert len(fake_transport.ordinary) == 1  # Ordinary target still executes.
    assert fake_transport.ordinary[0]["public_relay_resume"]["top"]["after"] == "saved-after"
    assert len(fake_transport.closed) == 3
    for index, doc in enumerate(diagnostic.DOC_IDS):
        kind, selected, variables = fake_transport.sends[index * 2 + 1]
        assert kind == "graphql" and selected == doc
        assert variables["first"] == 12 and variables["sort_order"] == "recent" and "after" not in variables
        assert variables["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] is False
    report = json.loads((plan.output / "parent-root-completed.json").read_bytes())
    assert report["status"] == "completed" and report["send_count"] == 4
    assert [page["edge_count"] for page in report["pages"]] == [1, 1]
    for doc_id in diagnostic.DOC_IDS:
        private_page = plan.output / f"parent-root-{doc_id}.json"
        assert json.loads(private_page.read_bytes())["doc_id"] == doc_id
        assert json.loads(private_page.read_bytes())["native_comment_ids"] == ["p"]
        assert private_page.stat().st_mode & 0o777 == 0o400
    serialized = json.dumps(report)
    assert "NEVER-RETAIN" not in serialized and "private-native-cursor" not in serialized and '"text"' not in serialized
    assert (plan.output / "parent-root-completed.json").stat().st_mode & 0o777 == 0o400
    run(checkpoint)
    assert len(fake_transport.sends) == 4 and len(fake_transport.ordinary) == 2


@pytest.mark.parametrize(
    "key,value",
    [
        ("generation", True),
        ("doc_ids", ["unsupported"]),
        ("request_cursor", "saved"),
        ("fetch_replies", True),
        ("page_size", 13),
        ("max_sends", 5),
        ("sort_order", "popular"),
        ("max_pages_per_doc", 2),
        ("full_history_enabled", True),
        ("public_only", False),
        ("execution_placement", "modal"),
        ("intent_hash", "wrong"),
        ("deadline_at", "2026-10-25T23:08:15.797481Z"),
        ("deadline_at", "2026-10-25T22:00:00Z"),
    ],
)
def test_invalid_plan_fails_before_marker_or_send(plan, fake_transport, key, value):
    plan.value[key] = value
    plan.save()
    run, *_ = owned(Gate(), fake_transport.factory)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)):
        run()
    assert fake_transport.sends == [] and list(plan.output.iterdir()) == []


@pytest.mark.parametrize("binding", ["run", "generation", "local", "public", "deadline", "hash", "private"])
def test_wrong_owner_or_plan_binding_fails_closed(plan, fake_transport, monkeypatch, binding):
    gate = Gate()
    run, before, *_ = owned(gate, fake_transport.factory)
    if binding == "run":
        before.run_id = "another-run"
    elif binding == "generation":
        before.run_generation = 2
    elif binding == "local":
        gate.network_lane = "modal"
    elif binding == "public":
        gate.public_only = False
    elif binding == "deadline":
        before.deadline = time.monotonic() - 1
    elif binding == "hash":
        monkeypatch.setenv(diagnostic.SHA_ENV, "0" * 64)
    else:
        plan.path.chmod(0o600)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)):
        run()
    assert fake_transport.sends == [] and list(plan.output.iterdir()) == []


def test_started_without_complete_never_resends(plan, fake_transport):
    (plan.output / "parent-root-started.json").write_text("uncertain")
    run, *_ = owned(Gate(), fake_transport.factory)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)) as error:
        run()
    assert error.value.error_code == "source_evidence_missing" and fake_transport.sends == []


def test_default_off_and_other_target_do_not_read_plan(plan, fake_transport, monkeypatch):
    run, *_ = owned(Gate(), fake_transport.factory)
    monkeypatch.setenv(diagnostic.PLAN_ENV, "/does/not/exist")
    run(shortcode="another-shortcode")
    monkeypatch.delenv(diagnostic.PLAN_ENV)
    monkeypatch.delenv(diagnostic.SHA_ENV)
    run()
    assert fake_transport.sends == [] and len(fake_transport.ordinary) == 2


@pytest.mark.parametrize("spec", [{"text": "not-json"}, {"json": {"data": {}}}])
def test_malformed_or_decode_retry_stops_before_third_send(plan, fake_transport, spec):
    fake_transport.specs.append(spec)
    gate = Gate()
    run, *_ = owned(gate, fake_transport.factory)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)):
        run()
    assert len(fake_transport.sends) == 2 and gate.successes == []
    assert fake_transport.ordinary == []
    report = json.loads((plan.output / "parent-root-completed.json").read_bytes())
    assert report["status"] == "failed" and report["send_count"] == 2
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)):
        run()
    assert len(fake_transport.sends) == 2


@pytest.mark.parametrize(
    "status,code,delay",
    [
        (429, "rate_limited", 43),
        (503, "request_failed", 43),
        (500, "request_failed", None),
        (403, "public_access_backoff", 43),
    ],
)
def test_source_wait_uses_exact_permit_and_existing_failure_policy(plan, fake_transport, status, code, delay):
    headers = {"retry-after": str(delay)} if delay is not None else {}
    fake_transport.specs.append({"status_code": status, "headers": headers, "text": "rejected"})
    gate = Gate()
    run, *_ = owned(gate, fake_transport.factory)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)) as error:
        run()
    assert error.value.failure.code == code
    actual, kwargs = gate.failures[0]
    assert actual == code and kwargs["permit"].request_number == 2 and kwargs["retry_after_seconds"] == delay
    assert len(gate.failures) == 1 and gate.successes == [] and len(fake_transport.sends) == 2


def test_owner_fence_failure_before_wire_keeps_uncertain_marker(plan, fake_transport):
    gate = Gate()
    run, before, *_ = owned(gate, fake_transport.factory)

    def fenced():
        raise LostOwnership("fixture")

    # Retain metadata while substituting only the actual owner check.
    fenced.__dict__.update(before.__dict__)
    source = _GatedSource(
        PublicRelayCommentSource(fetcher_factory=fake_transport.factory), gate, failure_code="comment_fetch_failed"
    )
    with pytest.raises(LostOwnership):
        source(
            shortcode=diagnostic.SHORTCODE,
            checkpoint={},
            pre_source_request=fenced,
            source_observer=lambda *args: pytest.fail("diagnostic reached canonical observer"),
        )
    assert fake_transport.sends == []
    assert (plan.output / "parent-root-started.json").exists()


@pytest.mark.parametrize("operation", ["graphql_replies", "comments_api", "browser_warmup", "graphql_comments"])
def test_wrong_first_operation_is_rejected_without_permit(plan, operation):
    gate = Gate()

    class Fetcher:
        def __init__(self, **kwargs):
            self.before = kwargs["before_source_request"]

        async def _fetch_public_relay_coauthor_comments_for_status_only(self, *args, **kwargs):
            self.before(operation)
            pytest.fail("unsupported operation was admitted")

        async def aclose(self):
            pass

    run, _, charges, _ = owned(gate, Fetcher)
    with pytest.raises((SourceFailure, diagnostic.ParentDiagnosticError)):
        run()
    assert gate.request_count == 0 and charges == []


@pytest.mark.parametrize(
    "headers,code,delay",
    [
        ({"set-cookie": "sessionid=fixture-private-cookie; Path=/"}, "public_access_backoff", None),
        ({"retry-after": "71"}, "request_failed", 71),
    ],
)
def test_bootstrap_cookie_or_server_wait_stops_before_graphql(plan, fake_transport, headers, code, delay):
    fake_transport.bootstrap_specs.append({"status_code": 503 if delay else 200, "text": "private", "headers": headers})
    gate = Gate()
    run, *_ = owned(gate, fake_transport.factory)
    with pytest.raises(SourceFailure) as error:
        run()
    assert error.value.failure.code == code and len(fake_transport.sends) == 1
    actual, kwargs = gate.failures[0]
    assert actual == code and kwargs["permit"].request_number == 1 and kwargs["retry_after_seconds"] == delay
    assert len(gate.failures) == 1 and gate.successes == []
    report = (plan.output / "parent-root-completed.json").read_text()
    assert "cookie" not in report and "private" not in report


@pytest.mark.parametrize("malformation", ["identity", "page_info", "after", "reply"])
def test_invalid_native_parent_shape_never_acknowledges(plan, fake_transport, malformation):
    payload = native_page()
    connection = payload["data"]["xdt_api__v1__media__media_id__comments__connection"]
    if malformation == "identity":
        connection["edges"][0]["node"].pop("pk")
    elif malformation == "page_info":
        connection.pop("page_info")
    elif malformation == "after":
        connection["page_info"]["end_cursor"] = None
    else:
        payload["data"] = {
            "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": connection
        }
    fake_transport.specs.append({"json": payload})
    gate = Gate()
    run, *_ = owned(gate, fake_transport.factory)
    with pytest.raises(SourceFailure):
        run()
    assert gate.successes == [] and len(fake_transport.sends) == 2 and fake_transport.ordinary == []


@pytest.mark.parametrize(
    "error",
    [
        LostOwnership("owner fenced after bootstrap"),
        IdentityDeferred("identity_budget", None),
        SourceFailure(Failure("target_deadline")),
        asyncio.CancelledError("cancelled after bootstrap"),
    ],
)
def test_admission_failure_after_bootstrap_preserves_original_without_source_health_failure(
    plan,
    fake_transport,
    error,
):
    gate = Gate()
    admit = gate.before_request

    def denied():
        if gate.request_count:
            raise error
        return admit()

    gate.before_request = denied
    run, _, charges, ordinary = owned(gate, fake_transport.factory)

    def close_failure():
        raise RuntimeError("cleanup must not mask original control failure")

    fake_transport.close_hook[0] = close_failure
    with pytest.raises(type(error)) as actual:
        run()
    assert actual.value is error
    assert gate.request_count == len(charges) == len(fake_transport.sends) == 1
    assert gate.successes == [] and gate.failures == [(None, {})] and ordinary == []
    assert (plan.output / "parent-root-started.json").exists()
    report = json.loads((plan.output / "parent-root-completed.json").read_bytes())
    assert report["status"] == "failed" and report["send_count"] == 1
    assert list(plan.output.glob("parent-root-2*.json")) == []


@pytest.mark.parametrize("phase", ["response", "completion"])
@pytest.mark.parametrize("boundary", ["owner", "wall", "monotonic"])
def test_late_owner_or_deadline_fence_prevents_page_or_completion_without_another_request(
    plan,
    fake_transport,
    monkeypatch,
    phase,
    boundary,
):
    gate = Gate()
    run, before, charges, ordinary = owned(gate, fake_transport.factory)
    monotonic = [time.monotonic()]
    monkeypatch.setattr(diagnostic, "time", SimpleNamespace(monotonic=lambda: monotonic[0]))
    owner_error = LostOwnership("late target fence")

    def lost():
        raise owner_error

    def expire():
        if phase == "completion" and len(fake_transport.sends) != 4:
            return
        if boundary == "owner":
            gate.check_cancelled = lost
            # The guarded callback is captured when the source invocation starts.
            alive[0] = False
        elif boundary == "wall":
            plan.wall[0] = datetime(2026, 10, 25, 23, 0, tzinfo=UTC)
        else:
            monotonic[0] = before.deadline + 1

    alive = [True]

    def owner_fence():
        if not alive[0]:
            raise owner_error

    gate.check_cancelled = owner_fence
    (fake_transport.response_hook if phase == "response" else fake_transport.close_hook)[0] = expire
    with pytest.raises((LostOwnership, IdentityDeferred)) as actual:
        run()
    if boundary == "owner":
        assert actual.value is owner_error
    else:
        assert actual.value.failure.code == "target_deadline"
    sends = 2 if phase == "response" else 4
    assert gate.request_count == len(charges) == len(fake_transport.sends) == sends
    assert gate.failures == [(None, {})] and ordinary == []
    assert gate.successes == ([] if phase == "response" else [1, 2, 3, 4])
    report = json.loads((plan.output / "parent-root-completed.json").read_bytes())
    assert report["status"] == "failed" and len(report["pages"]) == (0 if phase == "response" else 2)
    assert (plan.output / "parent-root-started.json").exists()


def test_deadline_crossed_during_private_write_is_rechecked_before_success_ack(plan, fake_transport, monkeypatch):
    gate = Gate()
    run, _, charges, _ = owned(gate, fake_transport.factory)
    exclusive = diagnostic._exclusive

    def delayed_write(path, value):
        exclusive(path, value)
        if path.name == f"parent-root-{diagnostic.DOC_IDS[0]}.json":
            plan.wall[0] = datetime(2026, 10, 25, 23, 0, tzinfo=UTC)

    monkeypatch.setattr(diagnostic, "_exclusive", delayed_write)
    with pytest.raises(IdentityDeferred) as actual:
        run()
    assert actual.value.failure.code == "target_deadline"
    assert gate.request_count == len(charges) == len(fake_transport.sends) == 2
    assert gate.successes == [] and gate.failures == [(None, {})]
    assert (plan.output / f"parent-root-{diagnostic.DOC_IDS[0]}.json").exists()


def test_equal_count_documents_keep_separate_native_id_membership(plan, fake_transport):
    first, second = native_page(), native_page()
    second["data"]["xdt_api__v1__media__media_id__comments__connection"]["edges"][0]["node"]["pk"] = "different-parent"
    fake_transport.specs.extend([{"json": first}, {"json": second}])
    run, *_ = owned(Gate(), fake_transport.factory)
    run()
    documents = [json.loads((plan.output / f"parent-root-{doc}.json").read_bytes()) for doc in diagnostic.DOC_IDS]
    assert documents[0]["edge_count"] == documents[1]["edge_count"] == 1
    assert documents[0]["native_count"] == documents[1]["native_count"] == 17
    assert documents[0]["native_comment_ids"] == ["p"]
    assert documents[1]["native_comment_ids"] == ["different-parent"]
    assert documents[0]["payload_sha256"] != documents[1]["payload_sha256"]
    for document in documents:
        assert datetime.fromisoformat(document["captured_at"]).utcoffset().total_seconds() == 0
        assert '"text"' not in json.dumps(document) and "username" not in json.dumps(document)

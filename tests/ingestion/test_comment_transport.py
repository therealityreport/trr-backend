"""Injected async fetcher only: no browser, source, credentials, or service access."""

import asyncio
import copy
from types import SimpleNamespace

import pytest

from trr_backend.ingestion.comment_transport import ScraplingCommentSource, UnsupportedCommentOperationError
from trr_backend.ingestion.comments import CommentSourceError
from trr_backend.ingestion.domain import LostOwnership


class FakeFetcher:
    def __init__(self, script=None, result=None, **kwargs):
        self.options = kwargs
        self.script = script
        self.result = result or result_for()
        self.calls = []
        self.closed = False
        self.warmed = False
        self.swallowed = False

    async def warmup(self):
        self.options["before_source_request"]("browser_warmup")
        self.warmed = True

    async def fetch_comments_for_shortcode(self, shortcode, **kwargs):
        self.calls.append((shortcode, kwargs))
        if self.script:
            try:
                self.script(self)
            except Exception:
                # Mirror the legacy broad recovery boundary; durable callback
                # errors must cross this without being classified as transport.
                self.swallowed = True
                return result_for(fetch_failed=True)
        return self.result

    async def aclose(self):
        self.closed = True


def result_for(**overrides):
    fields = {
        "fetch_failed": False,
        "auth_failed": False,
        "retryable": False,
        "reported_comment_count": 2,
        "top_level_checkpoint": None,
        "reply_checkpoints": [],
        "diagnostic_metadata": {},
        "public_block_signal": "none",
        "fetch_reason": None,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def source_for(script=None, result=None, **options):
    created = []

    def factory(**kwargs):
        fetcher = FakeFetcher(script, result, **kwargs)
        created.append(fetcher)
        return fetcher

    return ScraplingCommentSource(
        cookies=[{"name": "fixture", "value": "fake"}],
        raw_cookies={"fixture": "fake"},
        browser_account_id="fixture-identity",
        fetcher_factory=factory,
        **options,
    ), created


def run(source, *, checkpoint=None, observer=lambda *args: None, before=lambda: None):
    return source(shortcode="post1", checkpoint=checkpoint or {}, source_observer=observer, pre_source_request=before)


def test_explicit_credentials_hooks_and_native_payload_order():
    timeline = []
    native = {"parent_comment_id": "parent1", "comments": [{"pk": "reply1"}]}

    def script(fetcher):
        fetcher.options["before_source_request"]("comments_api")
        timeline.append("dispatch")
        fetcher.options["source_payload_observer"]("comments_api", native)
        timeline.append("legacy-parse")

    source, created = source_for(script, expected_count=2)
    summary = run(
        source,
        observer=lambda kind, payload: timeline.append((kind, payload)),
        before=lambda: timeline.append("durable-count"),
    )
    assert timeline == ["durable-count", "dispatch", ("comments_api", native), "legacy-parse"]
    fetcher = created[0]
    assert fetcher.options["consume_credentials_only"] is True
    assert fetcher.options["cookies"] == [{"name": "fixture", "value": "fake"}]
    assert fetcher.options["raw_cookies"] == {"fixture": "fake"}
    assert fetcher.options["browser_account_id"] == "fixture-identity"
    assert fetcher.closed and not fetcher.warmed
    assert summary.full_observation and not summary.fetch_failed
    assert fetcher.calls[0][1]["load_strategy"] == "instagram_comments_endpoint_cursor"
    assert fetcher.calls[0][1]["fetch_replies"] is True
    assert fetcher.calls[0][1]["max_comments"] == 0


@pytest.mark.parametrize("site", ["request", "evidence"])
@pytest.mark.parametrize("error_type", [LostOwnership, RuntimeError, ValueError])
def test_durable_guard_escapes_legacy_recovery_as_original_error(site, error_type):
    error = error_type("fixture durable failure")

    def fail(*args):
        raise error

    def script(fetcher):
        if site == "request":
            fetcher.options["before_source_request"]("comments_api")
        else:
            fetcher.options["source_payload_observer"]("comments_api", {"comments": []})
        pytest.fail("callback failure must stop source parsing/dispatch")

    source, created = source_for(script)
    with pytest.raises(error_type) as caught:
        run(
            source,
            observer=fail if site == "evidence" else lambda *args: None,
            before=fail if site == "request" else lambda: None,
        )
    assert caught.value is error
    assert created[0].closed and not created[0].swallowed


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_process_control_exceptions_are_not_swallowed(error_type):
    error = error_type()

    def before():
        raise error

    def script(fetcher):
        fetcher.options["before_source_request"]("comments_api")

    source, created = source_for(script)
    with pytest.raises(error_type) as caught:
        run(source, before=before)
    assert caught.value is error
    assert created[0].closed and not created[0].swallowed


def test_rendered_and_unknown_paths_block_before_dispatch_or_count():
    for operation in ("rendered_comments", "unknown_transport"):
        calls = []

        def script(fetcher, operation=operation):
            fetcher.options["before_source_request"](operation)
            pytest.fail("unsupported browser operation must not dispatch")

        source, created = source_for(script)
        with pytest.raises(UnsupportedCommentOperationError):
            run(source, before=lambda calls=calls: calls.append("count"))
        assert calls == []
        assert created[0].closed and not created[0].swallowed


def test_allowed_browser_and_api_operations_count_explicit_boundaries_only():
    calls = []

    def script(fetcher):
        for operation in (
            "comments_api",
            "graphql_comments",
            "graphql_replies",
            "relay_bootstrap",
            "browser_api",
            "browser_api_evaluate",
        ):
            fetcher.options["before_source_request"](operation)

    source, created = source_for(script, warmup=True)
    run(source, before=lambda: calls.append(1))
    assert len(calls) == 7
    assert created[0].warmed


def test_resume_uses_exact_top_and_reply_directions_without_mutating_checkpoint():
    checkpoint = {
        "comment_pages": {
            "expected": 8,
            "observed": {"parent1": {}},
            "lanes": {"__top__": {"cursors": ["tail1"], "exhausted": False}},
        },
        "top_level_checkpoint": {
            "next_top_level_cursor": "tail1",
            "next_top_level_cursor_param": "max_id",
            "observation_started_from_root": True,
        },
        "reply_checkpoints": [
            {"parent_comment_id": "parent1", "next_reply_cursor": "reply-tail", "next_reply_cursor_param": "min_id"}
        ],
    }
    original = copy.deepcopy(checkpoint)
    source, created = source_for(result=result_for(reported_comment_count=8))
    summary = run(source, checkpoint=checkpoint)
    kwargs = created[0].calls[0][1]
    assert kwargs["top_level_cursor"] == "tail1" and kwargs["top_level_cursor_param"] == "max_id"
    assert kwargs["reply_resume_cursors"] == {"parent1": "reply-tail"}
    assert kwargs["reply_resume_cursor_params"] == {"parent1": "min_id"}
    assert kwargs["expected_comment_count"] == 8
    assert checkpoint == original and summary.full_observation


def test_unproven_root_resume_never_certifies_full_observation():
    checkpoint = {"top_level_checkpoint": {"next_top_level_cursor": "tail", "next_top_level_cursor_param": "min_id"}}
    source, created = source_for(expected_count=2)
    assert not run(source, checkpoint=checkpoint).full_observation
    assert created[0].calls[0][1]["top_level_cursor"] == "tail"


def test_missing_cursor_direction_restarts_root_instead_of_guessing():
    checkpoint = {
        "comment_pages": {"expected": 2, "lanes": {"__top__": {"cursors": ["opaque-tail"], "exhausted": False}}}
    }
    source, created = source_for()
    run(source, checkpoint=checkpoint)
    assert created[0].calls[0][1]["top_level_cursor"] is None
    assert created[0].calls[0][1]["top_level_cursor_param"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"fetch_failed": True},
        {"auth_failed": True},
        {"retryable": True},
        {"fetch_reason": "repeated_cursor"},
        {"fetch_reason": "http_429"},
        {"public_block_signal": "http_429"},
        {"diagnostic_metadata": {"session_invalidated": True}},
        {"top_level_checkpoint": {"next_top_level_cursor": "tail", "next_top_level_cursor_param": "max_id"}},
        {"reply_checkpoints": [{"parent_comment_id": "parent1", "next_reply_cursor": "reply-tail"}]},
    ],
)
def test_failures_and_pending_cursors_cannot_appear_complete(changes):
    source, _ = source_for(result=result_for(**changes))
    summary = run(source)
    assert (
        summary.fetch_failed
        or summary.auth_failed
        or summary.retryable
        or summary.top_level_checkpoint
        or summary.reply_checkpoints
    )


def test_unknown_expected_count_stays_unknown_and_unresolved():
    source, created = source_for(result=result_for(reported_comment_count=None))
    summary = run(source)
    assert summary.expected_count is None and summary.retryable
    assert created[0].calls[0][1]["expected_count_unknown"] is True
    source, _ = source_for(expected_count=2, result=result_for(diagnostic_metadata={"expected_count_unknown": True}))
    summary = run(source)
    assert summary.expected_count is None and summary.retryable


def test_checkpoint_projection_omits_session_diagnostics_and_keeps_root_provenance():
    source, _ = source_for(
        result=result_for(
            top_level_checkpoint={
                "next_top_level_cursor": "tail",
                "next_top_level_cursor_param": "max_id",
                "diagnostic_metadata": {"sessionid": "excluded"},
                "error_message": "excluded",
                "headers": {"cookie": "excluded"},
            }
        )
    )
    assert run(source).top_level_checkpoint == {
        "next_top_level_cursor": "tail",
        "next_top_level_cursor_param": "max_id",
        "observation_started_from_root": True,
    }


def test_capped_source_does_not_claim_full_observation():
    source, _ = source_for(max_comments=100)
    assert not run(source).full_observation


def test_active_async_loop_requires_worker_thread_without_creating_fetcher():
    source, created = source_for()

    async def call():
        with pytest.raises(RuntimeError, match="event loop"):
            run(source)

    asyncio.run(call())
    assert not created


def test_malformed_fetch_result_is_rejected_and_resources_close():
    source, created = source_for(result=SimpleNamespace())
    with pytest.raises(CommentSourceError, match="result contract"):
        run(source)
    assert created[0].closed


def test_durable_page_cursors_override_stale_summary_after_crash():
    checkpoint = {
        "comment_pages": {
            "expected": 2,
            "lanes": {
                "__top__": {
                    "cursors": ["durable-tail"],
                    "cursor_param": "max_id",
                    "exhausted": False,
                    "started_from_root": True,
                },
                "parent1": {"cursors": ["durable-reply"], "cursor_param": "min_id", "exhausted": False},
            },
        },
        "top_level_checkpoint": {"next_top_level_cursor": "stale-tail", "next_top_level_cursor_param": "min_id"},
        "reply_checkpoints": [
            {"parent_comment_id": "parent1", "next_reply_cursor": "stale-reply", "next_reply_cursor_param": "max_id"}
        ],
    }
    source, created = source_for()
    summary = run(source, checkpoint=checkpoint)
    kwargs = created[0].calls[0][1]
    assert kwargs["top_level_cursor"] == "durable-tail" and kwargs["top_level_cursor_param"] == "max_id"
    assert kwargs["reply_resume_cursors"] == {"parent1": "durable-reply"}
    assert kwargs["reply_resume_cursor_params"] == {"parent1": "min_id"}
    assert summary.full_observation


def test_unmappable_graphql_after_restarts_with_explicit_root_provenance():
    checkpoint = {
        "comment_pages": {
            "expected": 2,
            "lanes": {
                "__top__": {
                    "cursors": ["graphql-tail"],
                    "cursor_param": "after",
                    "exhausted": False,
                    "started_from_root": True,
                },
            },
        }
    }
    captures = []

    def script(fetcher):
        fetcher.options["source_payload_observer"]("comments_api", {"comments": [], "has_more_comments": False})
        fetcher.options["source_payload_observer"]("comments_api", {"comments": [], "has_more_comments": False})

    source, created = source_for(script)
    run(source, checkpoint=checkpoint, observer=lambda kind, payload: captures.append(payload))
    assert created[0].calls[0][1]["top_level_cursor"] is None
    assert captures[0]["cursor_payload"]["source"] == "root"
    assert "cursor_payload" not in captures[1]


def test_exact_supported_rest_opaque_cursor_param_is_not_substituted():
    for param in ("cached_comments_cursor", "tao_cursor", "bifilter_token"):
        checkpoint = {
            "comment_pages": {
                "expected": 2,
                "lanes": {
                    "__top__": {
                        "cursors": ["opaque"],
                        "cursor_param": param,
                        "exhausted": False,
                        "started_from_root": True,
                    },
                },
            }
        }
        source, created = source_for()
        run(source, checkpoint=checkpoint)
        assert created[0].calls[0][1]["top_level_cursor_param"] == param


@pytest.mark.parametrize("status,code,retry_after", [(429, "rate_limited", 37.0), (403, "source_auth_failed", None)])
def test_native_comment_rejection_escapes_recovery_before_retry(status, code, retry_after):
    from trr_backend.ingestion.domain import Failure
    from trr_backend.ingestion.post_transport import SourceFailure
    from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher

    events = []
    response = SimpleNamespace(status_code=status, text="", headers={"retry-after": "37"})

    class Fetcher(InstagramCommentsScraplingFetcher):
        def __init__(self, **kwargs):
            self._before_source_request = kwargs["before_source_request"]
            self._source_failure_observer = kwargs["source_failure_observer"]
            self._request_count = 0
            self._http_client = self
            self._http_client_lock = asyncio.Lock()
            self._max_transient_retries = 10

        async def _pace_api_requests(self, **kwargs):
            return True

        async def get(self, *args, **kwargs):
            events.append("sent")
            return response

        async def _fetch_api(self, url, **kwargs):
            return await self._request_shared_http_client("GET", url)

        async def fetch_comments_for_shortcode(self, *args, **kwargs):
            try:
                await self._fetch_json_response("https://fixture.invalid", referer="fixture")
            except Exception:
                events.append("swallowed")
            pytest.fail("rejection returned instead of aborting")

        async def aclose(self):
            events.append("closed")

    def rejected(actual, seconds):
        events.append((actual, seconds))
        raise SourceFailure(Failure(actual))

    source = ScraplingCommentSource(cookies=[], raw_cookies={}, browser_account_id=None, fetcher_factory=Fetcher)
    with pytest.raises(SourceFailure) as caught:
        source(
            shortcode="post",
            checkpoint={},
            pre_source_request=lambda: events.append("permit"),
            source_observer=lambda *args: None,
            source_failure_observer=rejected,
        )
    assert caught.value.failure.code == code
    assert events == ["permit", "sent", (code, retry_after), "closed"]

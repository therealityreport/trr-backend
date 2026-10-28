"""One immutable, private root-query diagnostic inside an owned public activity.

This observer never calls CommentActivity.observe and never supplies a retained
checkpoint. A started marker is deliberately permanent, including on failure.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import stat
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from trr_backend.ingestion.comments import comment_page_failure
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch
from trr_backend.socials.instagram.public_response import parse_public_retry_after

PLAN_ENV = "TRR_INSTAGRAM_PARENT_ROOT_DIAGNOSTIC_PLAN"
SHA_ENV = PLAN_ENV + "_SHA256"
RUN_ID = "2aef72ed-110c-4af9-b2b3-4a45da170a32"
INTENT_HASH = "10746e1197ea8b5f7272a90c662a2d1ebfd6e36a8a01cda4be1c73be428b9fc4"
SHORTCODE = "DTiwjjpAYzG"
DOC_IDS = ("25516980651312394", "26113520058347588")
RUN_DEADLINE = datetime.fromisoformat("2026-10-25T23:08:15.797480+00:00")
FROZEN = {
    "version": 1,
    "run_id": RUN_ID,
    "generation": 1,
    "intent_hash": INTENT_HASH,
    "shortcode": SHORTCODE,
    "account": "bravotv",
    "execution_placement": "local",
    "public_only": True,
    "full_history_enabled": False,
    "doc_ids": list(DOC_IDS),
    "sort_order": "recent",
    "page_size": 12,
    "request_cursor": None,
    "max_pages_per_doc": 1,
    "fetch_replies": False,
    "max_sends": 4,
}


class ParentDiagnosticError(RuntimeError):
    """Bounded failure requiring reconciliation; never an ordinary completion."""

    error_code = "source_evidence_missing"


class _StopPage(BaseException):
    """Exit legacy retry/fallback handlers after precisely one native page."""


class _AbortDiagnostic(BaseException):
    def __init__(self, error, *, control=False):
        self.error = error
        self.control = control


def _control_failure(error):
    from trr_backend.ingestion.domain import LostOwnership
    from trr_backend.ingestion.identity_policy import IdentityDeferred

    if error is None:
        return False
    if isinstance(error, _AbortDiagnostic):
        return error.control or _control_failure(error.error)
    return not isinstance(error, Exception) or isinstance(error, (LostOwnership, IdentityDeferred))


def _private(path: Path, *, directory=False, immutable=False):
    info = path.lstat()
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type or info.st_uid != os.getuid() or info.st_mode & 0o077 or (immutable and info.st_mode & 0o222):
        raise ParentDiagnosticError("diagnostic_private_path_required")


def _exclusive(path, value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _plan(before):
    raw_path, expected = os.environ.get(PLAN_ENV), os.environ.get(SHA_ENV)
    if not raw_path or not expected or len(expected) != 64:
        raise ParentDiagnosticError("diagnostic_plan_binding_required")
    path = Path(raw_path)
    if not path.is_absolute():
        raise ParentDiagnosticError("diagnostic_absolute_plan_required")
    _private(path, immutable=True)
    raw = path.read_bytes()
    if len(raw) > 16_384 or hashlib.sha256(raw).hexdigest() != expected:
        raise ParentDiagnosticError("diagnostic_plan_hash_mismatch")
    try:
        value = json.loads(raw)
        if type(value) is not dict or set(value) != set(FROZEN) | {"deadline_at", "output_dir"}:
            raise ValueError
        if any(type(value[key]) is not type(fixed) or value[key] != fixed for key, fixed in FROZEN.items()):
            raise ValueError
        deadline = datetime.fromisoformat(value["deadline_at"].replace("Z", "+00:00"))
        if deadline.utcoffset() is None or deadline > RUN_DEADLINE:
            raise ValueError
        output = Path(value["output_dir"])
        if not output.is_absolute():
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ParentDiagnosticError("diagnostic_plan_invalid") from None
    if (
        getattr(before, "run_id", None) != RUN_ID
        or type(getattr(before, "run_generation", None)) is not int
        or before.run_generation != 1
        or getattr(before, "diagnostic_public_only", None) is not True
        or getattr(before, "diagnostic_network_lane", None) != "local"
        or not callable(getattr(before, "diagnostic_success", None))
        or not callable(getattr(before, "diagnostic_failure", None))
        or not callable(getattr(before, "diagnostic_check_owner", None))
        or not callable(getattr(before, "diagnostic_cancel", None))
        or not isinstance(getattr(before, "deadline", None), (int, float))
        or not math.isfinite(before.deadline)
        or before.deadline <= time.monotonic()
    ):
        raise ParentDiagnosticError("diagnostic_owned_local_public_activity_required")
    _private(output, directory=True)
    return expected, deadline, output


def _page_metadata(kind, payload, doc_id):
    cursor = payload.get("cursor_payload") or {}
    if kind != "graphql_comments" or cursor != {
        "adapter_family": "instagram_public_relay",
        "adapter_version": 1,
        "source": "root",
        "request_cursor": None,
        "relay_doc_id": doc_id,
        "relay_friendly_name": "PolarisPostCommentsPaginationQuery",
        "sort_order": "recent",
        "page_size": 12,
    }:
        raise ParentDiagnosticError("diagnostic_root_provenance_invalid")
    data = payload.get("data")
    connection = data.get("xdt_api__v1__media__media_id__comments__connection") if isinstance(data, Mapping) else None
    if not isinstance(connection, Mapping) or not isinstance(connection.get("edges"), list):
        raise ParentDiagnosticError("diagnostic_parent_connection_missing")
    page_info = connection.get("page_info")
    if not isinstance(page_info, Mapping) or type(page_info.get("has_next_page")) is not bool:
        raise ParentDiagnosticError("diagnostic_native_page_info_missing")
    end = page_info.get("end_cursor")
    if connection["page_info"]["has_next_page"] and (not isinstance(end, str) or not end):
        raise ParentDiagnosticError("diagnostic_native_cursor_missing")
    if any(not isinstance(edge, Mapping) or not isinstance(edge.get("node"), Mapping) for edge in connection["edges"]):
        raise ParentDiagnosticError("diagnostic_native_edges_invalid")
    from trr_backend.socials.instagram.comments_scrapling.fetcher import _extract_graphql_connection_comments

    parsed, _ = _extract_graphql_connection_comments(
        payload, shortcode=SHORTCODE, post_url=f"https://www.instagram.com/p/{SHORTCODE}/"
    )
    if len(parsed) != len(connection["edges"]):
        raise ParentDiagnosticError("diagnostic_native_parent_identity_missing")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    count = connection.get("count")
    return {
        "doc_id": doc_id,
        "root": True,
        "sort_order": "recent",
        "page_size": 12,
        "edge_count": len(connection["edges"]),
        "native_count": count if type(count) is int and count >= 0 else None,
        "native_comment_ids": [comment.comment_id for comment in parsed],
        "has_next_page": page_info["has_next_page"],
        "has_end_cursor": bool(end),
        "payload_sha256": hashlib.sha256(encoded).hexdigest(),
        "payload_bytes": len(encoded),
    }


def run_parent_diagnostic(*, shortcode, before, factory, timeout_ms, source_failure_observer):
    """Default off; enabled only for the frozen target and owned public run."""
    if not os.environ.get(PLAN_ENV) and not os.environ.get(SHA_ENV):
        return
    if shortcode != SHORTCODE:
        return
    digest, deadline, output = _plan(before)
    started, completed = output / "parent-root-started.json", output / "parent-root-completed.json"
    if completed.exists():
        _private(completed, immutable=True)
        report = json.loads(completed.read_bytes())
        if report.get("plan_sha256") != digest or report.get("status") != "completed":
            raise ParentDiagnosticError("diagnostic_failed_or_unbound_report")
        return
    if datetime.now(UTC) >= deadline:
        raise ParentDiagnosticError("diagnostic_plan_expired")
    if not callable(source_failure_observer):
        raise ParentDiagnosticError("diagnostic_source_failure_callback_required")
    try:
        _exclusive(started, {"plan_sha256": digest, "started_at": datetime.now(UTC).isoformat(), "pid": os.getpid()})
    except FileExistsError:
        raise ParentDiagnosticError("diagnostic_uncertain_started_no_resend") from None
    report = {"plan_sha256": digest, "status": "started", "send_count": 0, "pages": []}

    def check_live():
        from trr_backend.ingestion.identity_policy import IdentityDeferred

        def check_deadlines():
            if datetime.now(UTC) >= deadline or time.monotonic() >= before.deadline:
                raise IdentityDeferred("target_deadline", None)

        check_deadlines()
        before.diagnostic_check_owner()  # No request charge or additional permit.
        check_deadlines()  # The ownership check itself can block.

    async def execute_doc(doc_id):
        permits = []
        acknowledged = False
        failure_handled = False
        operations = []

        def guard(callback, *args, control=False):
            try:
                return callback(*args)
            except Exception as error:
                raise _AbortDiagnostic(error, control=control) from None

        def before_send(kind):
            if (
                operations != ["relay_bootstrap", "graphql_comments"][: len(operations)]
                or len(operations) >= 2
                or kind != ("relay_bootstrap", "graphql_comments")[len(operations)]
                or report["send_count"] >= 4
            ):
                raise _AbortDiagnostic(
                    ParentDiagnosticError("diagnostic_send_budget_or_operation_denied"), control=True
                )
            guard(check_live, control=True)
            permit = guard(before, control=True)
            if permit is None:
                raise _AbortDiagnostic(ParentDiagnosticError("diagnostic_request_permit_required"))
            permits.append(permit)
            operations.append(kind)
            report["send_count"] += 1
            return permit

        def failure(code, delay):
            nonlocal failure_handled
            from trr_backend.ingestion.reply_context import request_permit

            token = request_permit.set(permits[-1] if permits else None)
            try:
                failure_handled = True
                if source_failure_observer is None:
                    raise ParentDiagnosticError("diagnostic_source_failure_callback_required")
                guard(source_failure_observer, "public_access_backoff" if code == "source_auth_failed" else code, delay)
                raise ParentDiagnosticError("diagnostic_source_failure_not_raised")
            finally:
                request_permit.reset(token)

        def observe(kind, payload):
            nonlocal acknowledged
            failed = comment_page_failure(payload)
            if failed:
                delays = [x for x in (failed[1], parse_public_retry_after(payload.get("retry_after"))) if x is not None]
                guard(failure, failed[0], max(delays) if delays else None)
            page = guard(_page_metadata, kind, payload, doc_id)
            if operations != ["relay_bootstrap", "graphql_comments"]:
                raise _AbortDiagnostic(ParentDiagnosticError("diagnostic_unadmitted_native_page"))
            guard(check_live, control=True)
            page["captured_at"] = datetime.now(UTC).isoformat()
            guard(_exclusive, output / f"parent-root-{doc_id}.json", {"plan_sha256": digest, **page})
            report["pages"].append(page)
            for permit in permits:
                guard(check_live, control=True)
                guard(before.diagnostic_success, permit)
            acknowledged = True
            raise _StopPage

        fetcher = factory(
            cookies=[],
            raw_cookies={},
            browser_account_id=None,
            proxy_config=None,
            public_only=True,
            timeout_ms=timeout_ms,
            source_payload_observer=observe,
            before_source_request=before_send,
            source_failure_observer=failure,
        )
        # The maintained observer reports rate/auth rejection, but currently
        # leaves 5xx to legacy retry code. Extend only this diagnostic instance
        # so the admitted server failure and its actual wait reach the gate.
        observe_response = getattr(fetcher, "_observe_source_failure", None)
        if observe_response is not None:

            def diagnostic_response(response):
                status = getattr(response, "status_code", None)
                if type(status) is int and status >= 500:
                    failure("request_failed", parse_public_retry_after(response.headers.get("retry-after")))
                observe_response(response)

            fetcher._observe_source_failure = diagnostic_response
        primary_error = None
        try:
            try:
                await fetcher._fetch_public_relay_coauthor_comments_for_status_only(
                    SHORTCODE,
                    f"https://www.instagram.com/p/{SHORTCODE}/",
                    allow_authenticated=False,
                    use_proxy=False,
                    max_comments=0,
                    deadline=min(before.deadline, time.monotonic() + (deadline - datetime.now(UTC)).total_seconds()),
                    durable_resume={
                        "top": {"relay_doc_id": doc_id, "sort_order": "recent", "page_size": 12},
                        "replies": {},
                    },
                    persisted_comments=[],
                    strict_native_exhaustion=True,
                    fetch_replies=False,
                )
            except _StopPage:
                pass
            except CredentialIdentityMismatch:
                guard(failure, "source_auth_failed", getattr(fetcher, "_public_retry_after_seconds", None))
            if not acknowledged:
                raise ParentDiagnosticError("diagnostic_no_native_first_page")
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                if permits and not acknowledged and not failure_handled and not _control_failure(primary_error):
                    before.diagnostic_failure(permits[-1], "source_evidence_missing")
            finally:
                try:
                    await fetcher.aclose()
                except Exception:
                    if primary_error is None:
                        raise

    async def execute():
        for doc_id in DOC_IDS:
            await execute_doc(doc_id)

    try:
        asyncio.run(execute())
        check_live()
        report["status"] = "completed"
    except BaseException as error:
        if _control_failure(error):
            try:
                before.diagnostic_cancel()
            except Exception as cleanup_error:
                error.add_note("diagnostic_probe_cleanup_failed:" + type(cleanup_error).__name__)
        report["status"] = "failed"
        try:
            _exclusive(completed, report)
        except Exception as report_error:
            error.add_note("diagnostic_report_write_failed:" + type(report_error).__name__)
        if isinstance(error, _AbortDiagnostic):
            raise error.error from None
        raise
    _exclusive(completed, report)

"""Explicit production bindings, lazy until a configured operation needs them."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from urllib.parse import urlsplit

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.command_service import CommandService, ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, RunIntent, SourceProfile
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.validation_gate import ValidationStore as Store

WORKFLOW_VERSION = "instagram-ingestion-v2"


def enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


_MODAL_TARGET = {
    "MODAL_PROFILE": "admin-56995",
    "MODAL_WORKSPACE": "admin-56995",
    "MODAL_ENVIRONMENT": "main",
    "TRR_MODAL_APP_NAME": "trr-backend-jobs",
}
_CANARY_START = datetime(2026, 1, 1, tzinfo=UTC)
_CANARY_END = datetime(2027, 1, 1, tzinfo=UTC)


def _modal_target():
    # The current target record is explicit. Do not fill in another account or
    # select provider CLI defaults when a configured value is absent/mismatched.
    if any(os.getenv(name, "").strip() != expected for name, expected in _MODAL_TARGET.items()):
        raise ServiceUnavailableError("Explicit recorded TRR Modal target configuration is required")
    return _MODAL_TARGET.copy()


@dataclass(frozen=True)
class CanaryPolicy:
    accounts: frozenset[str]
    max_runs: int
    workflow_version: str = WORKFLOW_VERSION

    @classmethod
    def configured(cls):
        try:
            accounts = frozenset(
                RunIntent(account=value).account
                for value in os.getenv("TRR_INSTAGRAM_INGESTION_CANARY_ACCOUNTS", "").split(",")
                if value.strip()
            )
            maximum = int(os.environ["TRR_INSTAGRAM_INGESTION_CANARY_MAX_RUNS"])
            start = datetime.fromisoformat(
                os.environ["TRR_INSTAGRAM_INGESTION_CANARY_DATE_START"].replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(os.environ["TRR_INSTAGRAM_INGESTION_CANARY_DATE_END"].replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError):
            raise ServiceUnavailableError("Explicit bounded ingestion canary configuration is required") from None
        if accounts != frozenset({"bravotv"}) or not 1 <= maximum <= 10 or start != _CANARY_START or end != _CANARY_END:
            raise ServiceUnavailableError("Canary configuration differs from the approved bravotv calendar-2026 target")
        return cls(accounts, maximum)

    supported_actions = ("backfill",)

    def allows_account(self, account):
        return account in self.accounts

    def validate_scope(self, account, *, action, scope, date_start, date_end):
        if account not in self.accounts:
            raise Conflict("Account is outside the configured ingestion canary")
        if action != "backfill" or scope != "bounded_window" or date_start != _CANARY_START or date_end != _CANARY_END:
            raise Conflict("Ingestion canary requires the approved calendar-2026 bounded backfill")

    def counts(self, cur, *, including_current, account=None):
        cur.execute(
            """select count(*) filter(where account=any(%s)) as total,
            count(*) filter(where state in ('queued','running','blocked','failed','cancelled')
                and not (state='cancelled' and execution_hold_reason is not null)) as active
            from social.instagram_ingestion_runs where workflow_version=any(%s)""",
            (
                sorted(self.accounts),
                ["instagram-ingestion-v1", "instagram-ingestion-v2"]
                if self.workflow_version == "instagram-ingestion-v2"
                else [self.workflow_version],
            ),
        )
        row = cur.fetchone()
        # A paused/cancelled run can resume; it occupies the single canary run
        # slot until completed. All created runs consume the total launch budget.
        total, active = (row["total"], row["active"]) if isinstance(row, Mapping) else row
        if total > (self.max_runs if including_current else self.max_runs - 1):
            raise Conflict("Ingestion canary total run budget is exhausted; resume the existing run")
        if active > (1 if including_current else 0):
            raise Conflict("Only one ingestion canary run may be active or resumable")


# Each launch action resolves to exactly these intent scopes (command_service.launch).
# Capacity sees the raw payload, whose backfill_scope is meaningless for sync actions.
_ACCOUNT_ACTION_SCOPES = {
    "backfill": frozenset({"full_history", "bounded_window"}),
    "sync_newer": frozenset({"head_gap"}),
    "sync_recent": frozenset({"recent_window"}),
    "resume_tail": frozenset({"full_history"}),
}
_PAYLOAD_DEFAULT_SCOPES = frozenset({"full_history", "bounded_window"})
_ACTIVE_RUN_STATES = ("queued", "running", "blocked")
_LEGACY_TERMINAL_JOB_STATES = ("completed", "cancelled", "failed")


def v2_owns_instagram_launches() -> bool:
    """While true, the legacy catalog routes must not start Instagram work."""
    mode = os.getenv("TRR_INSTAGRAM_INGESTION_POLICY", "canary").strip().lower()
    return enabled("TRR_INSTAGRAM_INGESTION_ENABLED") and mode == "accounts"


def _active_shared_instagram_accounts():
    from trr_backend.db.pg import fetch_all

    rows = fetch_all(
        """select distinct lower(ltrim(account_handle, '@')) as account
        from social.shared_account_sources where platform = 'instagram' and is_active"""
    )
    return frozenset(str(row["account"]) for row in rows if row.get("account"))


@dataclass(frozen=True)
class AccountRunPolicy:
    """Manual launches for any active shared Instagram account, one active run each.

    Cancelled and failed runs stay resumable but do not block a new launch; a
    stale cancelled run must never make the launch button permanently unusable.
    """

    max_runs_per_day: int
    workflow_version: str = WORKFLOW_VERSION
    account_source: Callable[[], frozenset[str]] = _active_shared_instagram_accounts
    supported_actions = ("backfill", "sync_recent", "sync_newer", "resume_tail")

    @classmethod
    def configured(cls):
        try:
            maximum = int(os.getenv("TRR_INSTAGRAM_INGESTION_MAX_RUNS_PER_DAY", "12"))
        except ValueError:
            raise ServiceUnavailableError("Invalid daily Instagram ingestion run limit") from None
        if not 1 <= maximum <= 48:
            raise ServiceUnavailableError("Daily Instagram ingestion run limit must be 1 to 48")
        return cls(maximum)

    def allows_account(self, account):
        try:
            return account in self.account_source()
        except Exception:  # noqa: BLE001 - an unreadable allowlist admits nobody
            return False

    def validate_scope(self, account, *, action, scope, date_start, date_end):
        if not self.allows_account(account):
            raise Conflict("Account is not an active shared Instagram source")
        allowed = _ACCOUNT_ACTION_SCOPES.get(action)
        if allowed is None:
            raise Conflict("Unsupported Instagram ingestion action")
        if scope not in allowed and not (action != "backfill" and scope in _PAYLOAD_DEFAULT_SCOPES):
            raise Conflict("Instagram ingestion scope does not match the requested action")

    def counts(self, cur, *, including_current, account=None):
        if account is None:
            raise Conflict("Account run limits require an explicit account")
        cur.execute(
            """select count(*) filter(where state = any(%s)) as active,
            count(*) filter(where created_at > now() - interval '24 hours') as recent,
            (select count(*) from social.scrape_jobs job
                where job.platform = 'instagram' and job.status <> all(%s)
                and lower(ltrim(job.config->>'account', '@')) = %s) as legacy_active
            from social.instagram_ingestion_runs where account = %s""",
            (list(_ACTIVE_RUN_STATES), list(_LEGACY_TERMINAL_JOB_STATES), account, account),
        )
        row = cur.fetchone()
        active, recent, legacy_active = (
            (row["active"], row["recent"], row["legacy_active"]) if isinstance(row, Mapping) else row
        )
        if legacy_active:
            raise Conflict("An older-system Instagram job is still active for this account")
        if active > (1 if including_current else 0):
            raise Conflict("This Instagram account already has an active ingestion run")
        if recent > (self.max_runs_per_day if including_current else self.max_runs_per_day - 1):
            raise Conflict("Daily Instagram ingestion run limit reached for this account")


# The Mac worker holds its own session advisory lock: the same namespace as the
# Modal coordinator (RuntimeConfig.lock_namespace) with a distinct key, so the
# two never block each other. Placement fences keep their runs apart.
LOCAL_WORKER_LOCK_NAMESPACE = 48112
LOCAL_WORKER_LOCK_KEY = 20261017


def local_worker_online(read_connection) -> bool:
    """True while some session holds the Mac worker lease (no new table needed)."""
    with read_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """select exists(select 1 from pg_locks where locktype='advisory' and granted
            and classid=%s::oid and objid=%s::oid and objsubid=2)""",
            (LOCAL_WORKER_LOCK_NAMESPACE, LOCAL_WORKER_LOCK_KEY),
        )
        row = cur.fetchone()
    return bool(row["exists"] if isinstance(row, Mapping) else row[0])


def configured_run_policy():
    mode = os.getenv("TRR_INSTAGRAM_INGESTION_POLICY", "canary").strip().lower()
    if mode == "accounts":
        return AccountRunPolicy.configured()
    if mode != "canary":
        raise ServiceUnavailableError("Unknown Instagram ingestion run policy")
    return CanaryPolicy.configured()


class CanaryStore(Store):
    def __init__(self, policy: CanaryPolicy | None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policy = policy

    def start(self, intent, **kwargs):
        policy = self.policy
        if policy is None:
            raise ServiceUnavailableError("Configured ingestion canary is required")
        policy.validate_scope(
            intent.account,
            action=intent.action,
            scope=intent.scope,
            date_start=intent.date_start,
            date_end=intent.date_end,
        )
        if kwargs.get("engine") != "dbos" or kwargs.get("workflow_version") != policy.workflow_version:
            raise Conflict("Canary workflow version is not the pinned compatible version")
        initialize = kwargs.pop("initialize", None)

        def guarded(conn, run_id):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("instagram-ingestion-canary:" + policy.workflow_version,),
                )
                policy.counts(cur, including_current=True, account=intent.account)
            if initialize:
                initialize(conn, run_id)

        return super().start(intent, initialize=guarded, **kwargs)


class CanaryCapacity:
    def __init__(self, policy, admission, read_connection):
        self.policy, self.admission, self.read_connection = policy, admission, read_connection

    def __call__(self, account, payload):
        _modal_target()
        if self.policy is None:
            raise ServiceUnavailableError("Configured ingestion canary is required")
        self.policy.validate_scope(
            account,
            action=payload.action,
            scope=payload.backfill_scope,
            date_start=payload.date_start,
            date_end=payload.date_end,
        )
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self.policy.counts(cur, including_current=False, account=account)
        self.admission.validate_launch(account, payload)


class CanaryCommandService(CommandService):
    def capabilities(self, handle):
        capabilities = super().capabilities(handle)
        policy = self.store.policy
        if policy is None or not policy.allows_account(capabilities.account):
            return capabilities.model_copy(
                update={
                    "enabled": False,
                    "supported_actions": [],
                    "disabled_reason": (
                        "Account is not an active shared Instagram source."
                        if isinstance(policy, AccountRunPolicy)
                        else "Account is outside the configured bounded ingestion canary."
                    ),
                }
            )
        if capabilities.enabled:
            return capabilities.model_copy(
                update={
                    "supported_actions": [
                        action for action in capabilities.supported_actions if action in policy.supported_actions
                    ]
                }
            )
        return capabilities


def runtime_config():
    _modal_target()
    from trr_backend.ingestion.workflows import RuntimeConfig

    dsn = os.getenv("TRR_INSTAGRAM_DBOS_DATABASE_URL", "")
    role = os.getenv("TRR_INSTAGRAM_DBOS_RUNTIME_ROLE", "")
    parsed = urlsplit(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not role:
        raise ValueError("Explicit DBOS session connection and restricted role are required")
    if parsed.port == 6543:
        raise ValueError("DBOS lease cannot use transaction pooling")
    return RuntimeConfig(system_database_url=dsn, workflow_version=WORKFLOW_VERSION, runtime_role=role)


def frontier(account: str, source_scope: str):
    # Catalog identities are global per account; source_scope classifies runs,
    # rather than partitioning the account's existing native post timeline.
    with Queries().read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select max(posted_at) as newest from social.instagram_account_catalog_posts
            where lower(source_account)=%s""",
            (account,),
        )
        row = cur.fetchone()
        return row["newest"] if row else None


def _configured_source_profile():
    from trr_backend.socials.instagram.comments_scrapling.public_mode import (
        _AUTHENTICATED_MODE_ALIASES,
        _PUBLIC_MODE_ALIASES,
    )

    mode = os.getenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "public_first").strip().lower()
    if mode in _PUBLIC_MODE_ALIASES:
        return SourceProfile.for_mode("public")
    if mode in _AUTHENTICATED_MODE_ALIASES:
        comments_transport = os.getenv("TRR_INSTAGRAM_INGESTION_AUTH_COMMENTS_TRANSPORT", "rest")
        if comments_transport == "www_relay":
            return SourceProfile.for_authenticated_relay()
        if comments_transport != "rest":
            raise ServiceUnavailableError("Unsupported authenticated Instagram comments transport")
        return SourceProfile.for_mode("authenticated")
    raise ServiceUnavailableError("Unsupported Instagram source mode")


def _source_profile(context):
    return RunIntent.from_payload(context["run"]["intent"]).effective_source_profile


def _worker_store(context):
    placement = context.get("run", {}).get("execution_placement", "modal")
    return Store() if placement == "modal" else Store(execution_placement=placement)


def _network_lane(context):
    placement = context.get("run", {}).get("execution_placement", "modal")
    return placement if placement in {"modal", "local"} else "modal"


def _public_source_parts(context, owned):
    from trr_backend.ingestion.identity_policy import SourceRequestGate
    from trr_backend.ingestion.reply_context import source_deadline

    store = _worker_store(context)
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = source_deadline(time.monotonic() + max(0, min(590, remaining)))

    def check():
        store.check_owner(**owned)

    return {
        "gate": SourceRequestGate(
            cookies={},
            transaction=store.transaction,
            check_cancelled=check,
            check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
            deadline=deadline,
            public_only=True,
            network_lane=_network_lane(context),
        ),
        "deadline": deadline,
        "check_cancelled": check,
    }


def build_api():
    # Additive schema deploy precedes installation. Default process startup has
    # no DBOS SDK, background runner, database call, or credential access.
    if not enabled("TRR_INSTAGRAM_INGESTION_SCHEMA_READY"):
        return None
    from trr_backend.ingestion.admission import Admission
    from trr_backend.ingestion.replay_commands import ReplayCommands

    try:
        policy = configured_run_policy()
        _modal_target()
        source_profile = _configured_source_profile()
    except ServiceUnavailableError:
        policy, source_profile = None, None
    store = CanaryStore(policy)
    queries = Queries()
    return CanaryCommandService(
        store,
        queries,
        engine="dbos",
        workflow_version=WORKFLOW_VERSION,
        enabled=enabled("TRR_INSTAGRAM_INGESTION_ENABLED") and policy is not None,
        capacity=CanaryCapacity(policy, Admission(), queries.read_connection),
        frontier=frontier,
        replay_command=ReplayCommands(store),
        replay_apply_enabled=enabled("TRR_INSTAGRAM_INGESTION_REPLAY_APPLY_ENABLED"),
        listing_max_attempts=int(os.getenv("TRR_SOCIAL_SHARED_POSTS_FRONTIER_MAX_ATTEMPTS", "12")),
        comments_max_attempts=int(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_MAX_ATTEMPTS", "12")),
        source_profile=source_profile,
        local_worker_status=lambda: local_worker_online(queries.read_connection),
    )


def _credentials(account):
    from trr_backend.ingestion.credential_snapshot import CredentialSnapshot

    # The target account never selects the authenticated browser identity.
    return CredentialSnapshot.from_environ().source_kwargs()


class _GatedSource:
    """Shared request pressure spans every request until Worker finalizes target."""

    def __init__(self, source, gate, *, failure_code):
        self.source, self.gate, self.failure_code = source, gate, failure_code
        self.finished = False
        self.branch_failure_recorded = False

    def __call__(self, **callbacks):
        before = callbacks["pre_source_request"]
        observe = callbacks["source_observer"]

        from trr_backend.ingestion.reply_context import request_permit

        terminal_permit = None
        terminal_code = None

        def record_terminal_failure(permit, code=None):
            nonlocal terminal_permit, terminal_code
            if code not in (None, "target_deadline"):
                raise ValueError("Unsupported terminal reply failure category")
            terminal_permit, terminal_code = permit, code

        def guarded_request():
            counted = getattr(self.gate, "before_counted_request", None)
            accounting = getattr(before, "in_transaction", None)
            if callable(counted) and callable(accounting):
                return counted(accounting)
            permit = self.gate.before_request()
            before()
            return permit

        for name in ("run_id", "run_generation", "owner_generation", "observation_clock", "deadline"):
            if hasattr(before, name):
                setattr(guarded_request, name, getattr(before, name))
        guarded_request.record_terminal_failure = record_terminal_failure
        guarded_request.mark_sent = getattr(self.gate, "mark_sent", lambda _permit: None)
        guarded_request.release_unsent = getattr(self.gate, "release_unsent", lambda _permit: False)
        # The optional private diagnostic shares this activity's exact permits;
        # it never enters the canonical comment observer.
        guarded_request.diagnostic_public_only = getattr(self.gate, "public_only", False)
        guarded_request.diagnostic_network_lane = getattr(self.gate, "network_lane", None)
        guarded_request.diagnostic_success = lambda permit: self.gate.record_success(permit=permit)
        guarded_request.diagnostic_failure = lambda permit, code: self.gate.record_failure(code, permit=permit)
        guarded_request.diagnostic_check_owner = getattr(self.gate, "check_cancelled", None)

        def diagnostic_cancel():
            # A cancellation/admission failure is not source health evidence.
            # Release only this activity's probe and preserve the original error.
            try:
                return self.gate.finish_failure()
            finally:
                self.finished = True

        guarded_request.diagnostic_cancel = diagnostic_cancel
        deadlines = [
            value
            for value in (getattr(before, "deadline", None), getattr(self.gate, "deadline", None))
            if value is not None
        ]
        if deadlines:
            guarded_request.deadline = min(deadlines)

        def guarded_observer(*args, **kwargs):
            try:
                observe(*args, **kwargs)  # Retain/parse before acknowledging healthy content.
            except Exception as error:
                permit = request_permit.get()
                if permit is not None:
                    error._instagram_request_permit = permit
                raise
            if not self.gate.request_count:
                from trr_backend.ingestion.providers import InvalidSourceError

                raise InvalidSourceError("Source response has no admitted source request")
            payload = next((arg for arg in args if isinstance(arg, Mapping)), kwargs.get("payload", {}))
            from trr_backend.ingestion.comments import native_child_rejection

            if (
                not payload.get("errors")
                and payload.get("status") in (None, "ok")
                and not native_child_rejection(payload)
            ):
                permit = request_permit.get()
                self.gate.record_success(permit=permit) if permit is not None else self.gate.record_success()

        def rejected_response(code, retry_after_seconds):
            from trr_backend.ingestion.post_transport import SourceFailure

            permit = request_permit.get()
            failure = self.gate.finish_failure(
                code, retry_after_seconds=retry_after_seconds, **({"permit": permit} if permit is not None else {})
            )
            self.finished = True
            raise SourceFailure(failure)

        def record_branch_failure():
            permit = request_permit.get()
            if permit is None:
                raise ValueError("identity_request_permit_required")
            # Reconcile this exact admitted failed response, without closing
            # healthy peer branches or releasing another request's lease.
            self.gate.record_failure("source_incomplete", permit=permit)
            self.branch_failure_recorded = True

        rejected_response.record_branch_failure = record_branch_failure

        try:
            result = self.source(
                **dict(
                    callbacks,
                    pre_source_request=guarded_request,
                    source_observer=guarded_observer,
                    source_failure_observer=rejected_response,
                )
            )
            if getattr(result, "auth_failed", False) or getattr(result, "fetch_failed", False):
                from trr_backend.ingestion.domain import Failure
                from trr_backend.ingestion.post_transport import SourceFailure

                failure = Failure(
                    terminal_code
                    or ("source_auth_failed" if getattr(result, "auth_failed", False) else self.failure_code)
                )
                error = SourceFailure(failure)
                if terminal_permit is not None:
                    error._instagram_request_permit = terminal_permit
                raise error
            return result
        except Exception as error:
            failure = self.finish_failure(error)
            if failure is not None:
                from trr_backend.ingestion.post_transport import SourceFailure

                raise SourceFailure(failure) from None
            raise

    def finish_success(self):
        if self.finished:
            return None
        if self.gate.request_count:
            result = self.gate.finish_success()
        else:
            result = self.gate.finish_failure()
        self.finished = True
        return result

    def finish_failure(self, error=None):
        if self.finished:
            return None
        import psycopg2

        from trr_backend.ingestion.canonical import CanonicalWriteError
        from trr_backend.ingestion.comments import CommentSourceError, CursorStalledError
        from trr_backend.ingestion.domain import LostOwnership
        from trr_backend.ingestion.identity_policy import IdentityDeferred
        from trr_backend.ingestion.providers import InvalidSourceError

        supplied = getattr(error, "failure", None)
        if getattr(supplied, "code", None) == "target_deadline":
            # A normal reservation time cap is neither source rejection nor a
            # health failure. Keep the typed deadline after releasing our probe.
            self.gate.finish_failure()
            self.finished = True
            return None
        if (
            (error is None and self.branch_failure_recorded)
            or not self.gate.request_count
            or isinstance(
                error, (LostOwnership, IdentityDeferred, CursorStalledError, psycopg2.Error, CanonicalWriteError)
            )
        ):
            result = self.gate.finish_failure()
        else:
            supplied = getattr(error, "failure", None)
            code = getattr(supplied, "code", None) or getattr(error, "error_code", None)
            if code is None:
                code = (
                    "source_incomplete"
                    if error is None
                    else (
                        "parse_error"
                        if isinstance(error, (InvalidSourceError, CommentSourceError))
                        else "request_failed"
                    )
                )
            from trr_backend.ingestion.reply_context import request_permit

            permit = getattr(error, "_instagram_request_permit", None) or request_permit.get()
            result = self.gate.finish_failure(code, **({"permit": permit} if permit is not None else {}))
        self.finished = True
        return result


def _gate_source(source, context, owned, credentials, *, failure_code, public_only=False):
    from trr_backend.ingestion.identity_policy import SourceRequestGate
    from trr_backend.ingestion.reply_context import source_deadline

    store = _worker_store(context)
    expires = context["target"]["owner_expires_at"]
    remaining = (expires - datetime.now(UTC)).total_seconds() - 1
    deadline = time.monotonic() + max(0, min(590, remaining))
    if public_only:
        deadline = source_deadline(deadline)
    gate = SourceRequestGate(
        cookies=credentials["raw_cookies"],
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=deadline,
        public_only=public_only,
        network_lane=_network_lane(context) if public_only else None,
    )
    return _GatedSource(source, gate, failure_code=failure_code)


def _listing(context, owned):
    from trr_backend.ingestion.post_transport import ScraplingListingSource

    if _source_profile(context).access_mode == "public":
        from trr_backend.ingestion.domain import SourceProfile

        if _source_profile(context) == SourceProfile.for_public_ordered():
            from trr_backend.ingestion.ordered_listing_transport import OrderedListingSource

            return OrderedListingSource(
                account=owned["account"],
                profile=context["target"]["checkpoint"].get("ordered_profile"),
                **_public_source_parts(context, owned),
            )
        from trr_backend.ingestion.public_transport import PublicListingSource

        return PublicListingSource(account=owned["account"], **_public_source_parts(context, owned))
    credentials = _credentials(owned["account"])
    source = ScraplingListingSource(account=owned["account"], **credentials)
    return _gate_source(source, context, owned, credentials, failure_code="listing_fetch_failed")


def _details(context, owned):
    from trr_backend.ingestion.media_refresh import ExplicitDetailClient
    from trr_backend.ingestion.post_transport import DetailSource

    if _source_profile(context).access_mode == "public":
        from trr_backend.ingestion.public_transport import PublicDetailSource

        return PublicDetailSource(shortcode=owned["target_key"], **_public_source_parts(context, owned))
    credentials = _credentials(owned["account"])
    store = _worker_store(context)
    return DetailSource(
        shortcode=owned["target_key"],
        cookies=credentials["raw_cookies"],
        browser_account_id=credentials["browser_account_id"],
        transaction=store.transaction,
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        scraper_factory=ExplicitDetailClient,
    )


def _comments(context, owned):
    from trr_backend.ingestion.comment_transport import ScraplingCommentSource

    if _source_profile(context).access_mode == "public":
        from trr_backend.ingestion.comment_transport import PublicRelayCommentSource

        source = PublicRelayCommentSource(expected_count=context["post"]["comments_count"])
        return _gate_source(
            source, context, owned, {"raw_cookies": {}}, failure_code="comment_fetch_failed", public_only=True
        )
    credentials = _credentials(owned["account"])
    if _source_profile(context).comments_family == "instagram_authenticated_www_relay":
        from trr_backend.ingestion.authenticated_comment_transport import AuthenticatedRelayCommentSource

        source = AuthenticatedRelayCommentSource(**credentials, expected_count=context["post"]["comments_count"])
        return _gate_source(source, context, owned, credentials, failure_code="comment_fetch_failed")
    source = ScraplingCommentSource(**credentials, expected_count=context["post"]["comments_count"])
    return _gate_source(source, context, owned, credentials, failure_code="comment_fetch_failed")


def _profile_provider(context, owned):
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource
    from trr_backend.ingestion.identity_policy import SourceRequestGate
    from trr_backend.ingestion.reply_context import source_deadline

    if _source_profile(context).access_mode != "public":
        raise Conflict("Commenter profiles require an explicitly public run")
    store = _worker_store(context)
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = source_deadline(time.monotonic() + max(0, min(590, remaining)))
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=deadline,
        network_lane=_network_lane(context),
    )
    user_id = context["target"]["checkpoint"].get("user_id")
    if user_id:
        from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource

        return PublicGraphQLProfileSource(user_id=user_id, deadline=deadline), gate
    return AvatarProfileSource(cookies={}, deadline=deadline, public_only=True), gate


def _media_storage(context, owned):
    from botocore.config import Config

    from trr_backend.ingestion.media import MediaStorage
    from trr_backend.ingestion.media_refresh import MediaRefresh
    from trr_backend.ingestion.media_storage_io import BoundedS3Client
    from trr_backend.media.s3_mirror import build_public_object_url, get_hosted_media_storage_config

    config = get_hosted_media_storage_config()
    public_only = _source_profile(context).access_mode == "public"
    options = {
        "region_name": config.region,
        "config": Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"total_max_attempts": 1},
            signature_version="s3v4",
            s3={"addressing_style": "path"} if config.endpoint_url else {},
        ),
    }
    if config.endpoint_url:
        options["endpoint_url"] = config.endpoint_url
    if config.access_key_id and config.secret_access_key:
        options.update(aws_access_key_id=config.access_key_id, aws_secret_access_key=config.secret_access_key)
        if config.session_token:
            options["aws_session_token"] = config.session_token
    refresh_store = _worker_store(context)
    return MediaStorage(
        client=BoundedS3Client(bucket=config.bucket, profile_name=config.profile_name, client_options=options),
        bucket=config.bucket,
        build_hosted_url=build_public_object_url,
        prefix=config.prefix,
        refresh=MediaRefresh(
            store=refresh_store,
            owned=owned,
            owner_expires_at=context["target"]["owner_expires_at"],
            credential_provider=lambda: _credentials(owned["account"]),
            public_only=public_only,
            network_lane=_network_lane(context) if public_only else None,
            **(
                {
                    "public_avatar_source_factory": lambda username, deadline: _public_avatar_source(
                        refresh_store, owned, username, deadline
                    )
                }
                if public_only
                else {}
            ),
        ),
    )


def _public_avatar_lookup_hint(store, owned, username):
    """Bounded indexed owner lookup, used only to qualify a fresh public query.

    Native ``id`` and ``pk`` may use different namespaces. Preserve the saved
    object and follow the maintained query's id-first hint; fresh response aliases
    still pass the strict parser without dropping or rewriting any field.
    """
    from trr_backend.ingestion.avatar_resolution import AvatarResolutionError, normalize_handle
    from trr_backend.ingestion.domain import Stage
    from trr_backend.ingestion.profiles import ProfileError, native_id

    username = normalize_handle(username)
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        store.check_owner(**{**owned, "stage": Stage.MEDIA}, connection=conn)
        cur.execute(
            """select raw_data->'user' as native_user, raw_data->'owner' as native_owner,
            raw_data->'owner_detail' as owner_detail, owner_username, owner_user_id
            from social.instagram_posts where username=%s limit 2""",
            (username,),
        )
        candidates = list(cur.fetchall())
    hints = set()
    for row in candidates:
        bindings = [
            (row.get("owner_username"), row.get("owner_user_id")),
        ]
        for key in ("native_user", "native_owner", "owner_detail"):
            user = row.get(key)
            if isinstance(user, dict):
                # Preserve each native branch's id-first namespace choice.
                bindings.append((user.get("username"), user.get("id") or user.get("pk") or user.get("user_id")))
        for owner_handle, owner_id in bindings:
            try:
                if normalize_handle(owner_handle) != username:
                    continue
                hints.add(native_id(owner_id))
            except (AvatarResolutionError, ProfileError):
                continue
    return next(iter(hints)) if len(hints) == 1 else None


def _public_avatar_source(store, owned, username, deadline):
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource

    hint = _public_avatar_lookup_hint(store, owned, username)
    if hint is not None:
        from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource

        return PublicGraphQLProfileSource(user_id=hint, deadline=deadline)
    return AvatarProfileSource(cookies={}, deadline=deadline, public_only=True)


def _bind_avatar_downloads(snapshot, observations):
    """Ephemeral delivery inputs cannot change a sealed role or image version."""
    from trr_backend.ingestion.media import avatar_delivery_equivalent, source_reference

    if not observations:
        return snapshot

    def bind(item):
        if item.cached_hosted_url:
            return item
        candidates = {
            (
                source_reference(observation.source_url),
                getattr(observation, "validated_native_id", None),
            ): observation.source_url
            for observation in observations
            if observation.username == item.username
            and observation.source_url
            and not observation.unavailable
            and avatar_delivery_equivalent(
                item.source_url,
                observation.source_url,
                native_id=getattr(observation, "validated_native_id", None),
            )
        }
        # Distinct matching CDN identities are ambiguous; never choose by order.
        if len(candidates) != 1:
            return item
        (_, native_id), delivery_url = next(iter(candidates.items()))
        return replace(
            item,
            download_url=delivery_url,
            download_provenance=(item.username, native_id) if native_id is not None else None,
        )

    result = replace(snapshot, avatars=tuple(bind(item) for item in snapshot.avatars))
    result.validate()
    return result


def _load_media(loader, store, context, owned, **callbacks):
    from trr_backend.ingestion.avatar_resolution import AvatarResolutionActivity
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource
    from trr_backend.ingestion.cached_avatar_alias import bridge_cached_avatars
    from trr_backend.ingestion.identity_policy import SourceRequestGate
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing
    from trr_backend.ingestion.reply_context import source_deadline
    from trr_backend.ingestion.saved_owner_asset import recover as recover_saved_owner_asset

    binding = {key: owned[key] for key in ("run_id", "account", "generation", "target_key")}
    try:
        snapshot = loader.load(**binding, **callbacks)
    except AvatarSourcesMissing as missing:
        # The read-only inventory transaction has closed. Profile discovery is
        # explicit source work under the already acquired media owner.
        handles = missing.handles
    else:
        snapshot = bridge_cached_avatars(
            snapshot,
            loader=loader,
            store=store,
            owned=owned,
            storage_factory=lambda: _media_storage(context, owned),
            callbacks=callbacks,
        )
        return recover_saved_owner_asset(
            snapshot,
            loader=loader,
            store=store,
            owned=owned,
            storage_factory=lambda: _media_storage(context, owned),
            callbacks=callbacks,
        )
    public_only = _source_profile(context).access_mode == "public"
    credentials = {"raw_cookies": {}} if public_only else _credentials(owned["account"])
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = time.monotonic() + max(0, min(590, remaining))
    if public_only:
        deadline = source_deadline(deadline)
    gate = SourceRequestGate(
        cookies=credentials["raw_cookies"],
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=deadline,
        public_only=public_only,
        network_lane=_network_lane(context) if public_only else None,
    )
    observations = ()
    if public_only:
        observations = AvatarResolutionActivity(store, gate=gate).execute(
            **binding,
            owner_token=owned["owner_token"],
            handles=handles,
            source_factory=lambda username: _public_avatar_source(store, owned, username, deadline),
        )
    else:
        source = AvatarProfileSource(cookies=credentials["raw_cookies"], deadline=deadline)
        try:
            AvatarResolutionActivity(store, gate=gate).execute(
                **binding,
                owner_token=owned["owner_token"],
                handles=handles,
                fetch=source,
            )
        finally:
            source.close()
    return _bind_avatar_downloads(loader.load(**binding, **callbacks), observations)


def _avatar_skip_ttl_hours():
    try:
        return max(1, int(os.getenv("SOCIAL_AVATAR_SKIP_TTL_HOURS", "168")))
    except (TypeError, ValueError):
        return 168


def build_worker(*, execution_placement="modal"):
    from trr_backend.ingestion.freshness import DetailCache
    from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader
    from trr_backend.ingestion.worker import Worker

    store = Store() if execution_placement == "modal" else Store(execution_placement=execution_placement)
    loader = MediaSnapshotLoader(
        store.transaction,
        include_comment_avatars=enabled("SOCIAL_INSTAGRAM_MEDIA_MIRROR_COMMENT_AVATARS"),
        unsupported_ttl_hours=_avatar_skip_ttl_hours(),
    )
    return Worker(
        store,
        listing_source=_listing,
        detail_source=_details,
        comment_source=_comments,
        profile_source=_profile_provider,
        media_storage=_media_storage,
        detail_cache=lambda context, owned: DetailCache(Queries().read_connection).lookup(
            owned["run_id"], owned["account"], target_key=owned["target_key"]
        ),
        media_loader=lambda context, owned, **callbacks: _load_media(loader, store, context, owned, **callbacks),
    )


def _dispatch(reservation):
    # Check persisted placement/generation before importing or addressing Modal.
    # The worker repeats this fence, closing the preflight-to-spawn race.
    from trr_backend.ingestion.domain import Stage

    store = Store()
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        run = store._execution_run(cur, reservation["run_id"], reservation["account"])
        if run["generation"] != reservation["generation"] or run["state"] not in {"queued", "running"}:
            from trr_backend.ingestion.domain import LostOwnership

            raise LostOwnership("Dispatch generation is no longer active")
        Stage(reservation["stage"])
    target = _modal_target()
    from trr_backend.ingestion.runtime import activity_reservation
    from trr_backend.modal_dispatch import (
        get_trr_modal_function_handle,
        modal_social_comments_job_function_name,
        modal_social_media_job_function_name,
        modal_social_posts_job_function_name,
    )

    names = {
        "listing": modal_social_posts_job_function_name(),
        "post_details": modal_social_posts_job_function_name(),
        "comments": modal_social_comments_job_function_name(),
        "media": modal_social_media_job_function_name(),
    }
    call = get_trr_modal_function_handle(
        names[reservation["stage"]], app_name=target["TRR_MODAL_APP_NAME"], environment_name=target["MODAL_ENVIRONMENT"]
    ).spawn(ingestion_reservation=activity_reservation(reservation))
    return call.object_id


def _reconcile(reservation):
    try:
        _modal_target()
        identity = reservation.get("remote_identity")
        if not isinstance(identity, str) or not identity or len(identity) > 128:
            return "unknown"
        import modal

        nodes = list(modal.FunctionCall.from_id(identity).get_call_graph())
        inspected = 0
        while nodes and inspected < 1000:
            node = nodes.pop()
            inspected += 1
            if getattr(node, "function_call_id", None) == identity:
                return "accepted"  # Positive existence, including terminal calls.
            nodes.extend(getattr(node, "children", None) or [])
    except Exception:
        pass
    # Missing/retained-out/ambiguous lookup is not proof of non-submission.
    return "unknown"


def build_scheduler():
    from trr_backend.ingestion.admission import Admission
    from trr_backend.ingestion.validation_gate import ValidationWorkflowStore as WorkflowStore
    from trr_backend.ingestion.workflows import Scheduler

    return Scheduler(WorkflowStore(), admit=Admission().admit, dispatch=_dispatch, reconcile=_reconcile)

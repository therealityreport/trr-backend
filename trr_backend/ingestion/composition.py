"""Explicit production bindings, lazy until a configured operation needs them."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.command_service import CommandService, ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, RunIntent, SourceProfile
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.queries import Queries

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

    def validate_scope(self, account, *, action, scope, date_start, date_end):
        if account not in self.accounts:
            raise Conflict("Account is outside the configured ingestion canary")
        if action != "backfill" or scope != "bounded_window" or date_start != _CANARY_START or date_end != _CANARY_END:
            raise Conflict("Ingestion canary requires the approved calendar-2026 bounded backfill")

    def counts(self, cur, *, including_current):
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
                policy.counts(cur, including_current=True)
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
            self.policy.counts(cur, including_current=False)
        self.admission.validate_launch(account, payload)


class CanaryCommandService(CommandService):
    def capabilities(self, handle):
        capabilities = super().capabilities(handle)
        if self.store.policy is None or capabilities.account not in self.store.policy.accounts:
            return capabilities.model_copy(
                update={
                    "enabled": False,
                    "supported_actions": [],
                    "disabled_reason": "Account is outside the configured bounded ingestion canary.",
                }
            )
        if capabilities.enabled:
            return capabilities.model_copy(update={"supported_actions": ["backfill"]})
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
        return SourceProfile.for_mode("authenticated")
    raise ServiceUnavailableError("Unsupported Instagram source mode")


def _source_profile(context):
    return RunIntent.from_payload(context["run"]["intent"]).effective_source_profile


def _worker_store(context):
    placement = context.get("run", {}).get("execution_placement", "modal")
    return Store() if placement == "modal" else Store(execution_placement=placement)


def _public_source_parts(context, owned):
    from trr_backend.ingestion.identity_policy import SourceRequestGate

    store = _worker_store(context)
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = time.monotonic() + max(0, min(590, remaining))

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
        policy = CanaryPolicy.configured()
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

    def __call__(self, **callbacks):
        before = callbacks["pre_source_request"]
        observe = callbacks["source_observer"]

        def guarded_request():
            self.gate.before_request()
            before()

        def guarded_observer(*args, **kwargs):
            observe(*args, **kwargs)  # Retain/parse before acknowledging healthy content.
            if not self.gate.request_count:
                from trr_backend.ingestion.providers import InvalidSourceError

                raise InvalidSourceError("Source response has no admitted source request")
            payload = next((arg for arg in args if isinstance(arg, Mapping)), kwargs.get("payload", {}))
            if not payload.get("errors") and payload.get("status") in (None, "ok"):
                self.gate.record_success()

        def rejected_response(code, retry_after_seconds):
            from trr_backend.ingestion.post_transport import SourceFailure

            failure = self.gate.finish_failure(code, retry_after_seconds=retry_after_seconds)
            self.finished = True
            raise SourceFailure(failure)

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

                failure = Failure("source_auth_failed" if getattr(result, "auth_failed", False) else self.failure_code)
                raise SourceFailure(failure)
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
        from trr_backend.ingestion.comments import CommentSourceError
        from trr_backend.ingestion.domain import LostOwnership
        from trr_backend.ingestion.identity_policy import IdentityDeferred
        from trr_backend.ingestion.providers import InvalidSourceError

        if not self.gate.request_count or isinstance(
            error, (LostOwnership, IdentityDeferred, psycopg2.Error, CanonicalWriteError)
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
            result = self.gate.finish_failure(code)
        self.finished = True
        return result


def _gate_source(source, context, owned, credentials, *, failure_code, public_only=False):
    from trr_backend.ingestion.identity_policy import SourceRequestGate

    store = _worker_store(context)
    expires = context["target"]["owner_expires_at"]
    remaining = (expires - datetime.now(UTC)).total_seconds() - 1
    gate = SourceRequestGate(
        cookies=credentials["raw_cookies"],
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=time.monotonic() + max(0, min(590, remaining)),
        public_only=public_only,
    )
    return _GatedSource(source, gate, failure_code=failure_code)


def _listing(context, owned):
    from trr_backend.ingestion.post_transport import ScraplingListingSource

    if _source_profile(context).access_mode == "public":
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
    source = ScraplingCommentSource(**credentials, expected_count=context["post"]["comments_count"])
    return _gate_source(source, context, owned, credentials, failure_code="comment_fetch_failed")


def _profile_provider(context, owned):
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource
    from trr_backend.ingestion.identity_policy import SourceRequestGate

    if _source_profile(context).access_mode != "public":
        raise Conflict("Commenter profiles require an explicitly public run")
    store = _worker_store(context)
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = time.monotonic() + max(0, min(590, remaining))
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=deadline,
    )
    user_id = context["target"]["checkpoint"].get("user_id")
    if user_id:
        from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource

        return PublicGraphQLProfileSource(user_id=user_id, deadline=deadline), gate
    return AvatarProfileSource(cookies={}, deadline=deadline, public_only=True), gate


def _media_storage(context, owned):
    import boto3
    from botocore.config import Config

    from trr_backend.ingestion.media import MediaStorage
    from trr_backend.ingestion.media_refresh import MediaRefresh
    from trr_backend.media.s3_mirror import build_public_object_url, get_hosted_media_storage_config

    config = get_hosted_media_storage_config()
    session = boto3.Session(profile_name=config.profile_name) if config.profile_name else boto3.Session()
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
    return MediaStorage(
        client=session.client("s3", **options),
        bucket=config.bucket,
        build_hosted_url=build_public_object_url,
        prefix=config.prefix,
        refresh=MediaRefresh(
            store=_worker_store(context),
            owned=owned,
            owner_expires_at=context["target"]["owner_expires_at"],
            credential_provider=lambda: _credentials(owned["account"]),
            public_only=_source_profile(context).access_mode == "public",
        ),
    )


def _load_media(loader, store, context, owned, **callbacks):
    from trr_backend.ingestion.avatar_resolution import AvatarResolutionActivity
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource
    from trr_backend.ingestion.identity_policy import SourceRequestGate
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing

    binding = {key: owned[key] for key in ("run_id", "account", "generation", "target_key")}
    try:
        return loader.load(**binding, **callbacks)
    except AvatarSourcesMissing as missing:
        # The read-only inventory transaction has closed. Profile discovery is
        # explicit source work under the already acquired media owner.
        handles = missing.handles
    public_only = _source_profile(context).access_mode == "public"
    credentials = {"raw_cookies": {}} if public_only else _credentials(owned["account"])
    remaining = (context["target"]["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
    deadline = time.monotonic() + max(0, min(590, remaining))
    gate = SourceRequestGate(
        cookies=credentials["raw_cookies"],
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
        deadline=deadline,
        public_only=public_only,
    )
    source = AvatarProfileSource(
        cookies=credentials["raw_cookies"], deadline=deadline, **({"public_only": True} if public_only else {})
    )
    try:
        AvatarResolutionActivity(store, gate=gate).execute(
            **binding,
            owner_token=owned["owner_token"],
            handles=handles,
            fetch=source,
        )
    finally:
        source.close()
    return loader.load(**binding, **callbacks)


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
    from trr_backend.ingestion.workflow_store import WorkflowStore
    from trr_backend.ingestion.workflows import Scheduler

    return Scheduler(WorkflowStore(), admit=Admission().admit, dispatch=_dispatch, reconcile=_reconcile)

"""Cross-process account auth cooldown for the social posts-backfill lane.

Partition runners execute in SEPARATE Modal containers, so an in-process
cooldown (a dict, an asyncio lock, a file lock scoped to one container) cannot be
shared across them. This module is the Postgres-backed source of truth, mirroring
the cross-process advisory-lock pattern in
``posts_scrapling/fetcher.py::_try_advisory_lock_pace`` by using the
``social_control`` pool.

Lifecycle:
    * ``record_auth_block`` — called at the posts-lane auth-block handler when a
      page fetch classifies a hard 401/403 (or a checkpoint/challenge). UPSERTs
      the row, increments ``consecutive_auth_failures``, and sets
      ``cooldown_until = now() + escalating backoff`` via
      ``exponential_backoff_delay`` keyed on the (post-increment) failure count.
    * ``get_active_cooldown`` — called before each page request / at job start and
      in the dispatch guard. Returns the row only when ``cooldown_until`` is in
      the future; otherwise ``None``.
    * ``clear_cooldown`` — called on a clean page fetch. Resets the counter and
      clears ``cooldown_until`` so the account is immediately eligible again.
      Checkpoint blockers are *non-clearing* (see ``CHECKPOINT_ERROR_CODES``):
      ``clear_cooldown`` is a no-op while a checkpoint blocker is active so a
      single lucky public-data fetch cannot mask a session that still needs
      operator re-auth.

All public functions are defensive: any database failure is swallowed and treated
as "no cooldown" so the reliability layer can never itself wedge the pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from trr_backend.socials._retry import exponential_backoff_delay
from trr_backend.socials._scrapling_http_utils import resolve_positive_float_env

logger = logging.getLogger(__name__)

_COOLDOWN_POOL_NAME = "social_control"
_COOLDOWN_TABLE = "social.account_auth_cooldown"
_IDENTITY_COOLDOWN_HANDLE_PREFIX = "__identity__"

# Escalation curve for cooldown_until = now() + exponential_backoff_delay(n).
# Defaults: 1st block ~60s, 2nd ~120s, 3rd ~240s, ... capped at 1h. Jitter is
# inherited from exponential_backoff_delay so container cohorts do not retry in
# lockstep. Overridable via env for ops tuning without a redeploy of constants.
_COOLDOWN_BASE_DELAY_DEFAULT = 60.0
_COOLDOWN_MAX_DELAY_DEFAULT = 3600.0

# Error codes that represent a non-clearing blocker: the session is challenged /
# checkpointed / forced to log in. These must NOT auto-rotate-retry (a fresh IP
# will not solve a checkpoint) and must NOT be cleared by an incidental success.
CHECKPOINT_ERROR_CODES: frozenset[str] = frozenset(
    {
        "instagram_graphql_checkpoint_required",
        "checkpoint_required",
        "checkpoint",
        "challenge_required",
        "challenge",
        "redirect_to_checkpoint",
        "login_required",
        "redirect_to_login",
        "feedback_required",
        "facebook_checkpoint_required",
        "facebook_two_step_verification",
        "facebook_redirect_to_login",
        "facebook_login_required",
        "threads_login_prompt",
        "threads_redirect_to_login",
        "threads_posts_auth_failed",
        "threads_posts_auth_cooldown_active",
    }
)

_BLOCKER_KIND_AUTH = "auth"
_BLOCKER_KIND_CHECKPOINT = "checkpoint"


@dataclass(slots=True, frozen=True)
class AccountAuthCooldown:
    """Redaction-safe snapshot of an active cooldown row."""

    platform: str
    account_handle: str
    cooldown_until: datetime
    consecutive_auth_failures: int
    last_error_code: str | None
    blocker_kind: str

    @property
    def is_checkpoint(self) -> bool:
        return self.blocker_kind == _BLOCKER_KIND_CHECKPOINT

    def to_metadata(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "account_handle": self.account_handle,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "consecutive_auth_failures": self.consecutive_auth_failures,
            "last_error_code": self.last_error_code,
            "blocker_kind": self.blocker_kind,
        }


def _normalize_platform(platform: str | None) -> str:
    return str(platform or "").strip().lower() or "instagram"


def _normalize_handle(account_handle: str | None) -> str:
    return str(account_handle or "").strip().lower().lstrip("@")


def _identity_cooldown_handle(platform: str | None = "instagram") -> str:
    return f"{_IDENTITY_COOLDOWN_HANDLE_PREFIX}:{_normalize_platform(platform)}"


def classify_blocker_kind(error_code: str | None) -> str:
    normalized = str(error_code or "").strip().lower()
    return _BLOCKER_KIND_CHECKPOINT if normalized in CHECKPOINT_ERROR_CODES else _BLOCKER_KIND_AUTH


def is_checkpoint_error_code(error_code: str | None) -> bool:
    return classify_blocker_kind(error_code) == _BLOCKER_KIND_CHECKPOINT


def cooldown_backoff_seconds(consecutive_auth_failures: int) -> float:
    """Escalating backoff seconds for the given (1-indexed) failure count."""
    base_delay = resolve_positive_float_env(
        "SOCIAL_INSTAGRAM_AUTH_COOLDOWN_BASE_SECONDS",
        _COOLDOWN_BASE_DELAY_DEFAULT,
        minimum=1.0,
        maximum=3600.0,
    )
    max_delay = resolve_positive_float_env(
        "SOCIAL_INSTAGRAM_AUTH_COOLDOWN_MAX_SECONDS",
        _COOLDOWN_MAX_DELAY_DEFAULT,
        minimum=base_delay,
        maximum=86_400.0,
    )
    return exponential_backoff_delay(
        max(1, int(consecutive_auth_failures)),
        base_delay=base_delay,
        max_delay=max_delay,
    )


def _row_to_cooldown(row: dict[str, Any] | None) -> AccountAuthCooldown | None:
    if not row:
        return None
    cooldown_until = row.get("cooldown_until")
    if not isinstance(cooldown_until, datetime):
        return None
    return AccountAuthCooldown(
        platform=str(row.get("platform") or ""),
        account_handle=str(row.get("account_handle") or ""),
        cooldown_until=cooldown_until,
        consecutive_auth_failures=int(row.get("consecutive_auth_failures") or 0),
        last_error_code=str(row.get("last_error_code") or "").strip() or None,
        blocker_kind=str(row.get("blocker_kind") or _BLOCKER_KIND_AUTH).strip() or _BLOCKER_KIND_AUTH,
    )


def record_auth_block(
    platform: str,
    account_handle: str,
    error_code: str | None,
) -> AccountAuthCooldown | None:
    """Record an auth block and set/extend an escalating cooldown.

    UPSERT semantics: increments ``consecutive_auth_failures`` and sets
    ``cooldown_until = now() + cooldown_backoff_seconds(new_count)``. Returns the
    persisted cooldown snapshot, or ``None`` if the row could not be written
    (table missing in this environment, DB unavailable, etc.).
    """
    normalized_platform = _normalize_platform(platform)
    normalized_handle = _normalize_handle(account_handle)
    if not normalized_handle:
        return None
    blocker_kind = classify_blocker_kind(error_code)
    normalized_error_code = str(error_code or "").strip() or None

    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        logger.debug("auth_cooldown record skipped: pg import failed: %s", exc)
        return None

    try:
        with pg.db_connection(label="instagram-auth-cooldown-record", pool_name=_COOLDOWN_POOL_NAME) as conn:
            # Step 1: atomic UPSERT to bump the counter and record the error/kind.
            # checkpoint is "sticky" — once a checkpoint blocker is recorded it is
            # not downgraded to a plain auth blocker by a subsequent 401/403.
            upsert_rows = pg.execute_returning(
                f"""
                insert into {_COOLDOWN_TABLE} (
                    platform,
                    account_handle,
                    consecutive_auth_failures,
                    last_error_code,
                    blocker_kind,
                    updated_at
                )
                values (%s, %s, 1, %s, %s, now())
                on conflict (platform, account_handle) do update set
                    consecutive_auth_failures = {_COOLDOWN_TABLE}.consecutive_auth_failures + 1,
                    last_error_code = excluded.last_error_code,
                    blocker_kind = case
                        when {_COOLDOWN_TABLE}.blocker_kind = %s then %s
                        else excluded.blocker_kind
                    end,
                    updated_at = now()
                returning consecutive_auth_failures
                """,
                [
                    normalized_platform,
                    normalized_handle,
                    normalized_error_code,
                    blocker_kind,
                    _BLOCKER_KIND_CHECKPOINT,
                    _BLOCKER_KIND_CHECKPOINT,
                ],
                conn=conn,
            )
            new_count = int((upsert_rows[0] if upsert_rows else {}).get("consecutive_auth_failures") or 1)

            # Step 2: set cooldown_until using the Python-computed escalating
            # backoff (reuses exponential_backoff_delay for a single source of
            # truth). Same transaction → the counter and the deadline can never
            # disagree across containers.
            delay_seconds = cooldown_backoff_seconds(new_count)
            updated_rows = pg.execute_returning(
                f"""
                update {_COOLDOWN_TABLE}
                set cooldown_until = now() + make_interval(secs => %s),
                    updated_at = now()
                where platform = %s and account_handle = %s
                returning platform, account_handle, cooldown_until,
                          consecutive_auth_failures, last_error_code, blocker_kind
                """,
                [float(delay_seconds), normalized_platform, normalized_handle],
                conn=conn,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "auth_cooldown record failed platform=%s account=%s error=%s",
            normalized_platform,
            normalized_handle,
            exc,
        )
        return None

    cooldown = _row_to_cooldown(updated_rows[0] if updated_rows else None)
    if cooldown is not None:
        logger.warning(
            "instagram_auth_cooldown recorded",
            extra={
                "event": "auth_cooldown_recorded",
                "platform": cooldown.platform,
                "account": cooldown.account_handle,
                "consecutive_auth_failures": cooldown.consecutive_auth_failures,
                "blocker_kind": cooldown.blocker_kind,
                "last_error_code": cooldown.last_error_code,
                "cooldown_until": cooldown.cooldown_until.isoformat(),
            },
        )
    return cooldown


def record_identity_auth_block(
    platform: str | None = "instagram",
    error_code: str | None = None,
) -> AccountAuthCooldown | None:
    """Record a platform identity-level auth block via the reserved handle."""
    normalized_platform = _normalize_platform(platform)
    return record_auth_block(
        normalized_platform,
        _identity_cooldown_handle(normalized_platform),
        error_code,
    )


def get_active_cooldown(
    platform: str,
    account_handle: str,
) -> AccountAuthCooldown | None:
    """Return the cooldown row only when it is currently active (future deadline).

    Returns ``None`` when there is no row, the row has no deadline, the deadline
    is in the past, or the DB is unavailable (fail-open).
    """
    normalized_platform = _normalize_platform(platform)
    normalized_handle = _normalize_handle(account_handle)
    if not normalized_handle:
        return None

    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        logger.debug("auth_cooldown read skipped: pg import failed: %s", exc)
        return None

    try:
        row = pg.fetch_one(
            f"""
            select platform, account_handle, cooldown_until,
                   consecutive_auth_failures, last_error_code, blocker_kind
            from {_COOLDOWN_TABLE}
            where platform = %s
              and account_handle = %s
              and cooldown_until is not null
              and cooldown_until > now()
            """,
            [normalized_platform, normalized_handle],
            pool_name=_COOLDOWN_POOL_NAME,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "auth_cooldown read failed platform=%s account=%s error=%s",
            normalized_platform,
            normalized_handle,
            exc,
        )
        return None

    return _row_to_cooldown(row)


def get_active_identity_cooldown(
    platform: str | None = "instagram",
) -> AccountAuthCooldown | None:
    """Return an active identity-level cooldown for the platform, if any."""
    normalized_platform = _normalize_platform(platform)
    return get_active_cooldown(
        normalized_platform,
        _identity_cooldown_handle(normalized_platform),
    )


def clear_cooldown(
    platform: str,
    account_handle: str,
    *,
    force: bool = False,
) -> bool:
    """Clear an account's cooldown after a clean fetch.

    Resets ``consecutive_auth_failures`` to 0 and ``cooldown_until`` to NULL.
    A *checkpoint* blocker is non-clearing: unless ``force=True`` (operator
    intervention), this is a no-op while ``blocker_kind = 'checkpoint'`` so an
    incidental success cannot mask a session that still needs re-auth.

    Returns True when a row was actually cleared.
    """
    normalized_platform = _normalize_platform(platform)
    normalized_handle = _normalize_handle(account_handle)
    if not normalized_handle:
        return False

    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        logger.debug("auth_cooldown clear skipped: pg import failed: %s", exc)
        return False

    guard_clause = "" if force else f"and {_COOLDOWN_TABLE}.blocker_kind <> %s"
    params: list[Any] = [normalized_platform, normalized_handle]
    if not force:
        params.append(_BLOCKER_KIND_CHECKPOINT)

    try:
        with pg.db_connection(label="instagram-auth-cooldown-clear", pool_name=_COOLDOWN_POOL_NAME) as conn:
            rows = pg.execute_returning(
                f"""
                update {_COOLDOWN_TABLE}
                set consecutive_auth_failures = 0,
                    cooldown_until = null,
                    last_error_code = null,
                    blocker_kind = %s,
                    updated_at = now()
                where platform = %s
                  and account_handle = %s
                  and (cooldown_until is not null or consecutive_auth_failures > 0)
                  {guard_clause}
                returning platform
                """,
                [_BLOCKER_KIND_AUTH, *params],
                conn=conn,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "auth_cooldown clear failed platform=%s account=%s error=%s",
            normalized_platform,
            normalized_handle,
            exc,
        )
        return False

    cleared = bool(rows)
    if cleared:
        logger.info(
            "instagram_auth_cooldown cleared",
            extra={
                "event": "auth_cooldown_cleared",
                "platform": normalized_platform,
                "account": normalized_handle,
                "forced": bool(force),
            },
        )
    return cleared


def clear_identity_cooldown(
    platform: str | None = "instagram",
    *,
    force: bool = False,
) -> bool:
    """Clear a platform identity-level cooldown via the reserved handle."""
    normalized_platform = _normalize_platform(platform)
    return clear_cooldown(
        normalized_platform,
        _identity_cooldown_handle(normalized_platform),
        force=force,
    )

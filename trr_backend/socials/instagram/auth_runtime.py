# ruff: noqa: F821, UP037
"""Instagram auth runtime, cookie health, and repair helpers."""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _default_instagram_cookie_file_path() -> Path:
    return Path(__file__).resolve().parents[2] / "scripts" / "socials" / "instagram" / "instagram_cookies.json"


def _instagram_cookie_file_candidates() -> list[Path]:
    raw_candidates = [
        (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip(),
        (os.getenv("INSTAGRAM_COOKIES_FILE") or "").strip(),
        str(_default_instagram_cookie_file_path()),
    ]
    return [Path(raw_path).expanduser() for raw_path in raw_candidates if str(raw_path or "").strip()]


def _instagram_cookie_refresh_target_path() -> Path:
    candidates = _instagram_cookie_file_candidates()
    return candidates[0] if candidates else _default_instagram_cookie_file_path()


def _instagram_auth_credentials() -> tuple[str | None, str | None]:
    username = (
        (os.getenv("SOCIAL_AUTH_INSTAGRAM_USERNAME") or "").strip()
        or (os.getenv("INSTAGRAM_USERNAME") or "").strip()
        or None
    )
    password = (
        (os.getenv("SOCIAL_AUTH_INSTAGRAM_PASSWORD") or "").strip()
        or (os.getenv("INSTAGRAM_PASSWORD") or "").strip()
        or None
    )
    return username, password


def _instagram_cookie_auto_refresh_enabled() -> bool:
    raw = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH") or "").strip().lower()
    if raw:
        return raw not in {"0", "false", "off", "no"}
    username, password = _instagram_auth_credentials()
    return bool(username and password)


def _instagram_cookie_validation_username() -> str:
    return (os.getenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME") or "").strip() or "bravotv"


def _load_instagram_cookies_from_sources() -> dict[str, str]:
    """
    Resolve Instagram auth cookies for season ingest.

    Resolution order:
    1) SOCIAL_INSTAGRAM_COOKIES_JSON / INSTAGRAM_COOKIES_JSON (inline JSON object)
    2) SOCIAL_INSTAGRAM_COOKIES_FILE / INSTAGRAM_COOKIES_FILE (path to JSON file)
    3) scripts/socials/instagram/instagram_cookies.json (repo-local default)
    """
    from trr_backend.socials.instagram import load_cookies_from_file

    candidates: list[dict[str, str]] = []

    raw_json = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip() or (
        os.getenv("INSTAGRAM_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.warning("Invalid Instagram cookies JSON from env; falling back to file-based cookies")
        else:
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    candidates.append(cookies)
            elif parsed is not None:
                logger.warning("Instagram cookies JSON env value is not an object; falling back to file-based cookies")

    for path in _instagram_cookie_file_candidates():
        if not path.is_file():
            continue
        try:
            cookies = load_cookies_from_file(str(path))
        except Exception as exc:
            logger.warning("Failed to load Instagram cookies from %s: %s", path, exc)
            continue
        if cookies:
            candidates.append(cookies)

    return _select_preferred_cookie_candidate(
        candidates,
        required_cookie_names_any=("sessionid",),
        required_cookie_names_all=("csrftoken", "ds_user_id"),
    )


def _instagram_cookie_fingerprint(cookies: Mapping[str, Any]) -> str:
    payload = json.dumps(sorted((str(key), str(value)) for key, value in cookies.items()), separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _instagram_cookie_structure_detail(cookies: Mapping[str, Any]) -> dict[str, Any]:
    sessionid = bool(str(cookies.get("sessionid") or "").strip())
    csrftoken = bool(str(cookies.get("csrftoken") or "").strip())
    ds_user_id = bool(str(cookies.get("ds_user_id") or "").strip())
    missing_fields = [
        field_name
        for field_name, present in (
            ("sessionid", sessionid),
            ("csrftoken", csrftoken),
            ("ds_user_id", ds_user_id),
        )
        if not present
    ]
    return {
        "phase": "structural",
        "has_sessionid": sessionid,
        "has_csrftoken": csrftoken,
        "has_ds_user_id": ds_user_id,
        "missing_fields": missing_fields,
    }


def _instagram_cookie_schema_result(cookies: Mapping[str, Any]) -> dict[str, Any]:
    detail = _instagram_cookie_structure_detail(cookies)
    missing_fields = list(detail.get("missing_fields") or [])
    if not missing_fields:
        return {
            "valid": True,
            "reason": None,
            "detail": None,
        }
    detail["message"] = "Missing required Instagram cookie fields: " + ", ".join(missing_fields)
    return {
        "valid": False,
        "reason": "cookie_schema_invalid",
        "detail": detail,
    }


def _instagram_cookie_validation_detail(
    *,
    phase: str,
    message: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "phase": str(phase or "").strip() or "graphql",
    }
    normalized_message = str(message or "").strip()
    if normalized_message:
        payload["message"] = normalized_message[:240]
    for key, value in extra.items():
        if value is None or value == "":
            continue
        payload[key] = value
    return payload


def _inspect_instagram_cookie_health(cookies: dict[str, str]) -> dict[str, Any]:
    global _instagram_cookie_validation_cache

    schema_result = _instagram_cookie_schema_result(cookies)
    if not schema_result["valid"]:
        return schema_result

    fingerprint = _instagram_cookie_fingerprint(cookies)
    ttl_seconds = _resolve_positive_int_env(
        "SOCIAL_INSTAGRAM_COOKIE_VALIDATION_TTL_SECONDS",
        SOCIAL_INSTAGRAM_COOKIE_VALIDATION_TTL_SECONDS_DEFAULT,
        minimum=30,
    )
    now = time_module.monotonic()
    cached = _instagram_cookie_validation_cache
    if cached and cached[1] == fingerprint and (now - cached[0]) < ttl_seconds:
        return dict(cached[2])

    from trr_backend.socials.instagram import InstagramScraper

    validation_username = _instagram_cookie_validation_username()
    result: dict[str, Any]
    try:
        scraper = InstagramScraper(
            cookies=cookies,
            browser_account_id=validation_username,
        )
        payload = scraper.fetch_posts_graphql(
            validation_username,
            delay=0.0,
            request_timeout=(10, 20),
            allow_browser_fallback=False,
        )
        connection = (payload or {}).get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
        if connection.get("edges"):
            result = {
                "valid": True,
                "reason": None,
                "detail": None,
            }
        else:
            retrieval_meta = dict(scraper.last_retrieval_meta or {})
            error_code = str(retrieval_meta.get("error_code") or "").strip().lower()
            error_message = str(retrieval_meta.get("error_message") or "").strip().lower()
            transport = str(retrieval_meta.get("retrieval_transport") or retrieval_meta.get("transport") or "").strip()
            if error_code == "instagram_graphql_checkpoint_required" or error_message == "checkpoint_required":
                result = {
                    "valid": False,
                    "reason": "checkpoint_required",
                    "detail": _instagram_cookie_validation_detail(
                        phase="graphql",
                        message="Instagram GraphQL validation reported checkpoint_required.",
                        error_code=error_code or None,
                        transport=transport or None,
                    ),
                }
            elif int(retrieval_meta.get("error_status_code") or 0) in (401, 429) and "wait" in error_message:
                result = {
                    "valid": True,
                    "reason": "rate_limited_soft_pass",
                    "detail": _instagram_cookie_validation_detail(
                        phase="graphql",
                        message="Instagram rate-limited the validation probe; cookies assumed valid.",
                        error_code=error_code or None,
                        status_code=retrieval_meta.get("error_status_code"),
                        transport=transport or None,
                    ),
                }
            elif error_code or retrieval_meta.get("error_class"):
                result = {
                    "valid": False,
                    "reason": "request_error",
                    "detail": _instagram_cookie_validation_detail(
                        phase="graphql",
                        message=(
                            str(retrieval_meta.get("error_message") or "").strip()
                            or "Instagram GraphQL validation request failed."
                        ),
                        error_code=error_code or None,
                        error_class=str(retrieval_meta.get("error_class") or "").strip() or None,
                        status_code=retrieval_meta.get("error_status_code"),
                        transport=transport or None,
                    ),
                }
            else:
                result = {
                    "valid": False,
                    "reason": "graphql_validation_failed",
                    "detail": _instagram_cookie_validation_detail(
                        phase="graphql",
                        message="Instagram GraphQL validation returned no connection edges.",
                        transport=transport or None,
                    ),
                }
    except Exception as exc:  # noqa: BLE001
        exception_type = type(exc).__name__
        result = {
            "valid": False,
            "reason": f"unexpected_exception:{exception_type.lower()}",
            "detail": _instagram_cookie_validation_detail(
                phase="graphql",
                message=str(exc),
                exception_class=exception_type,
            ),
        }
        logger.debug("Instagram cookie validation raised %s", result["reason"], exc_info=True)

    _instagram_cookie_validation_cache = (now, fingerprint, dict(result))
    return dict(result)


def _validate_instagram_cookie_health(cookies: dict[str, str]) -> tuple[bool, str | None]:
    _sync_core_overrides()
    inspect_health = _room_callable("_inspect_instagram_cookie_health", _inspect_instagram_cookie_health)
    result = inspect_health(cookies)
    return bool(result.get("valid")), str(result.get("reason") or "").strip() or None


def _refresh_instagram_cookies(stale_reason: str | None = None) -> dict[str, str]:
    global _instagram_cookie_validation_cache, _instagram_cookie_runtime_override

    username, password = _instagram_auth_credentials()
    if not username or not password:
        return {}

    from trr_backend.socials.instagram.cookie_refresh import refresh_instagram_cookies

    try:
        refreshed = refresh_instagram_cookies(
            username=username,
            password=password,
            cookie_file=_instagram_cookie_refresh_target_path(),
            headless=(os.getenv("SOCIAL_INSTAGRAM_COOKIE_REFRESH_HEADLESS") or "true").strip().lower()
            not in {"0", "false", "off", "no"},
            timeout_seconds=_resolve_positive_int_env(
                "SOCIAL_INSTAGRAM_COOKIE_REFRESH_TIMEOUT_SECONDS",
                SOCIAL_INSTAGRAM_COOKIE_REFRESH_TIMEOUT_SECONDS_DEFAULT,
                minimum=30,
            ),
            validation_username=_instagram_cookie_validation_username(),
            validator=_validate_instagram_cookie_health,
            validation_mode=(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION") or "graphql_profile"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Instagram cookie refresh failed%s: %s",
            f" ({stale_reason})" if stale_reason else "",
            exc,
        )
        return {}

    if refreshed:
        _instagram_cookie_runtime_override = dict(refreshed)
        _instagram_cookie_validation_cache = None
        try:
            from trr_backend.socials.instagram import set_instagram_runtime_override

            set_instagram_runtime_override(refreshed)
        except Exception:  # noqa: BLE001
            logger.debug("Failed syncing Instagram auth resolver runtime override", exc_info=True)
    return refreshed


def _ensure_instagram_cookies_fresh(cookies: dict[str, str]) -> dict[str, str]:
    global _instagram_cookie_runtime_override

    if _instagram_cookie_runtime_override:
        return dict(_instagram_cookie_runtime_override)
    if not _instagram_cookie_auto_refresh_enabled():
        return cookies

    is_valid, validation_reason = _validate_instagram_cookie_health(cookies)
    if is_valid:
        return cookies

    with _instagram_cookie_refresh_lock:
        latest = _instagram_cookie_runtime_override or _load_instagram_cookies_from_sources()
        latest_valid, _ = _validate_instagram_cookie_health(dict(latest))
        if latest_valid:
            _instagram_cookie_runtime_override = dict(latest)
            return dict(latest)

        refreshed = _refresh_instagram_cookies(validation_reason)
        if refreshed:
            refreshed_valid, refreshed_reason = _validate_instagram_cookie_health(refreshed)
            if refreshed_valid:
                return refreshed
            logger.warning(
                "Instagram cookies refreshed but validation still failed (%s)",
                refreshed_reason or "unknown_reason",
            )
        return cookies


def _load_instagram_cookies_legacy() -> dict[str, str]:
    cookies = _load_instagram_cookies_from_sources()
    return _ensure_instagram_cookies_fresh(cookies)


def _build_legacy_instagram_auth_session(
    *,
    cookies: dict[str, str],
    browser_account_id: str | None,
    shadow_session: Any | None,
) -> Any:
    from trr_backend.socials.instagram import InstagramAuthSession

    shadow_cookies = dict(getattr(shadow_session, "cookies", {}) or {})
    parity_match = bool(cookies) and bool(shadow_cookies) and cookies == shadow_cookies
    validation_reason = getattr(shadow_session, "validation_reason", None) if parity_match else "legacy_shadow_mismatch"
    validation_category = (
        str(getattr(shadow_session, "validation_category", "") or "").strip() or "legacy_loader"
        if parity_match
        else "shadow_mode"
    )
    stale_ok = bool(getattr(shadow_session, "stale_ok", False)) if parity_match else False
    session_account_id = (
        str(
            getattr(shadow_session, "session_account_id", "")
            or browser_account_id
            or _instagram_cookie_validation_username()
        ).strip()
        or None
    )
    caller_context = str(getattr(shadow_session, "caller_context", "") or "legacy_loader").strip() or "legacy_loader"
    cookie_file_path = getattr(shadow_session, "cookie_file_path", None)
    storage_state_path = getattr(shadow_session, "storage_state_path", None)
    metadata = dict(getattr(shadow_session, "metadata", {}) or {})
    metadata.update(
        {
            "shadow_mode": True,
            "shadow_parity_match": parity_match,
        }
    )
    return InstagramAuthSession(
        cookies=dict(cookies),
        source="legacy_loader",
        validated=bool(parity_match and getattr(shadow_session, "validated", False)),
        validation_reason=validation_reason,
        validation_category=validation_category,
        stale_ok=stale_ok,
        browser_account_id=browser_account_id,
        session_account_id=session_account_id,
        caller_context=caller_context,
        cookie_file_path=cookie_file_path,
        storage_state_path=storage_state_path,
        refreshed=False,
        refresh_method=None,
        repaired_from_browser_session=bool(
            parity_match and getattr(shadow_session, "repaired_from_browser_session", False)
        ),
        resolver_version=int(getattr(shadow_session, "resolver_version", 2) or 2),
        metadata=metadata,
    )


def _load_instagram_cookies() -> dict[str, str]:
    from trr_backend.socials.instagram import (
        auth_session_log_payload,
        clear_instagram_auth_runtime_state,
        resolve_instagram_auth_session,
        set_current_instagram_auth_session,
        set_instagram_runtime_override,
    )

    legacy_cookies = _load_instagram_cookies_legacy()
    set_instagram_runtime_override(_instagram_cookie_runtime_override)

    shadow_session: Any | None = None
    try:
        shadow_session = resolve_instagram_auth_session(
            browser_account_id=_instagram_cookie_validation_username(),
            caller_context="legacy_loader",
            require_validation=_env_truthy("INSTAGRAM_AUTH_RESOLVER_V2"),
        )
    except Exception:  # noqa: BLE001
        if _env_truthy("INSTAGRAM_AUTH_RESOLVER_V2"):
            raise
        logger.debug("Instagram auth resolver shadow evaluation failed", exc_info=True)

    if _env_truthy("INSTAGRAM_AUTH_RESOLVER_V2"):
        if shadow_session is not None:
            set_current_instagram_auth_session(shadow_session)
            return dict(shadow_session.cookies)
        clear_instagram_auth_runtime_state()
        return {}

    legacy_session = _build_legacy_instagram_auth_session(
        cookies=legacy_cookies,
        browser_account_id=_instagram_cookie_validation_username(),
        shadow_session=shadow_session,
    )
    set_current_instagram_auth_session(legacy_session)

    if shadow_session is not None:
        legacy_fingerprint = hashlib.sha256(
            json.dumps(
                sorted((str(key), str(value)) for key, value in legacy_cookies.items()),
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:12]
        shadow_fingerprint = str((shadow_session.metadata or {}).get("fingerprint") or "")
        logger.debug(
            "Instagram auth resolver shadow parity",
            extra={
                "instagram_auth_shadow_parity": legacy_cookies == dict(shadow_session.cookies),
                "instagram_auth_shadow_legacy_fingerprint": legacy_fingerprint or None,
                "instagram_auth_shadow_resolver_fingerprint": shadow_fingerprint or None,
                **auth_session_log_payload(legacy_session),
            },
        )
    return legacy_cookies


def get_instagram_auth_repair_signal(*, failure_lookback_hours: int = 24) -> dict[str, Any]:
    _sync_core_overrides()
    load_cookies = _room_callable("_load_instagram_cookies_from_sources", _load_instagram_cookies_from_sources)
    inspect_health = _room_callable("_inspect_instagram_cookie_health", _inspect_instagram_cookie_health)
    cookies = load_cookies()
    cookie_validation = inspect_health(cookies)
    reason_codes: list[str] = []
    if not bool(cookie_validation.get("valid")):
        reason_codes.append("instagram_cookie_invalid")

    latest_failure: dict[str, Any] | None = None
    error_reason_codes = {
        "instagram_graphql_cursor_unauthorized": "recent_instagram_graphql_unauthorized",
        "instagram_graphql_checkpoint_required": "recent_instagram_graphql_checkpoint_required",
        INSTAGRAM_LOCAL_EXECUTOR_BLOCKED_ERROR_CODE: "recent_instagram_local_executor_blocked",
    }
    if _relation_exists("social.scrape_jobs"):
        cutoff = _now_utc() - timedelta(hours=max(1, int(failure_lookback_hours or 24)))
        try:
            failure_row = pg.fetch_one(
                """
                select
                  j.id::text as job_id,
                  j.run_id::text as run_id,
                  j.worker_id,
                  j.last_error_code,
                  coalesce(j.updated_at, j.completed_at, j.started_at, j.created_at) as updated_at
                from social.scrape_jobs as j
                where lower(coalesce(j.last_error_code, '')) = any(%s)
                  and coalesce(j.updated_at, j.completed_at, j.started_at, j.created_at) >= %s
                order by coalesce(j.updated_at, j.completed_at, j.started_at, j.created_at) desc
                limit 1
                """,
                [list(error_reason_codes.keys()), cutoff],
            )
        except Exception:  # noqa: BLE001
            logger.warning("Failed loading recent Instagram auth repair failures", exc_info=True)
            failure_row = None
        if failure_row:
            normalized_error_code = str(failure_row.get("last_error_code") or "").strip().lower()
            mapped_reason_code = error_reason_codes.get(normalized_error_code)
            if mapped_reason_code and mapped_reason_code not in reason_codes:
                reason_codes.append(mapped_reason_code)
            updated_at = failure_row.get("updated_at")
            latest_failure = {
                "job_id": str(failure_row.get("job_id") or "").strip() or None,
                "run_id": str(failure_row.get("run_id") or "").strip() or None,
                "worker_id": str(failure_row.get("worker_id") or "").strip() or None,
                "last_error_code": normalized_error_code or None,
                "updated_at": _iso(_coerce_dt(updated_at)) or str(updated_at or "").strip() or None,
            }

    return {
        "needs_repair": bool(reason_codes),
        "reason_codes": reason_codes,
        "cookie_validation": cookie_validation,
        "latest_failure": latest_failure,
    }


_LOCAL_ROOM_NAMES = {
    "_default_instagram_cookie_file_path",
    "_instagram_cookie_file_candidates",
    "_instagram_cookie_refresh_target_path",
    "_instagram_auth_credentials",
    "_instagram_cookie_auto_refresh_enabled",
    "_instagram_cookie_validation_username",
    "_load_instagram_cookies_from_sources",
    "_instagram_cookie_fingerprint",
    "_instagram_cookie_structure_detail",
    "_instagram_cookie_schema_result",
    "_instagram_cookie_validation_detail",
    "_inspect_instagram_cookie_health",
    "_validate_instagram_cookie_health",
    "_refresh_instagram_cookies",
    "_ensure_instagram_cookies_fresh",
    "_load_instagram_cookies_legacy",
    "_build_legacy_instagram_auth_session",
    "_load_instagram_cookies",
    "get_instagram_auth_repair_signal",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "_default_instagram_cookie_file_path",
    "_instagram_cookie_file_candidates",
    "_instagram_cookie_refresh_target_path",
    "_instagram_auth_credentials",
    "_instagram_cookie_auto_refresh_enabled",
    "_instagram_cookie_validation_username",
    "_load_instagram_cookies_from_sources",
    "_instagram_cookie_fingerprint",
    "_instagram_cookie_structure_detail",
    "_instagram_cookie_schema_result",
    "_instagram_cookie_validation_detail",
    "_inspect_instagram_cookie_health",
    "_validate_instagram_cookie_health",
    "_refresh_instagram_cookies",
    "_ensure_instagram_cookies_fresh",
    "_load_instagram_cookies_legacy",
    "_build_legacy_instagram_auth_session",
    "_load_instagram_cookies",
    "get_instagram_auth_repair_signal",
]

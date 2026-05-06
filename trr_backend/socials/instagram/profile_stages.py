# ruff: noqa: F821, UP037
"""Instagram profile snapshot and relationship stage room."""

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


def _instagram_profile_scraper(config: Mapping[str, Any], *, account_handle: str) -> Any:
    from trr_backend.socials.instagram import InstagramScraper

    cookies = _load_instagram_cookies()
    return InstagramScraper(
        cookies=cookies,
        browser_account_id=str(config.get("browser_account_id") or account_handle),
    )

def _run_instagram_profile_snapshot_stage(
    *,
    run_id: str,
    source_scope: str,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
) -> tuple[int, int, dict[str, Any]]:
    scraper = _instagram_profile_scraper(config, account_handle=account_handle)
    delay_seconds = float(config.get("delay_seconds") or 0)
    payload = scraper.fetch_profile_info(
        account_handle,
        delay=delay_seconds,
        request_timeout=(10, 30),
    )
    if not isinstance(payload, dict) or not payload:
        raise SharedStageRuntimeError(
            f"Instagram profile snapshot failed for @{account_handle}: empty profile payload",
            error_code="instagram_profile_snapshot_empty",
            retryable=True,
        )
    row = persist_instagram_profile_snapshot(
        payload,
        source_scope=source_scope,
        source_account=account_handle,
        job_id=job_id,
        run_id=run_id,
    )
    profile_payload = _instagram_profile_response(row, []) if row else {}
    _touch_shared_account_source(
        source_scope=source_scope,
        platform="instagram",
        account_handle=account_handle,
        run_id=run_id or None,
        job_id=job_id,
        last_scrape_status="completed",
        metadata_updates={"profile_snapshot": profile_payload},
    )
    return (
        0,
        0,
        {
            "stage": INSTAGRAM_PROFILE_SNAPSHOT_STAGE,
            "platform": "instagram",
            "account": account_handle,
            "profile_id": row.get("profile_id") if row else None,
            "profile_row_id": str((row or {}).get("id") or "").strip() or None,
            "profile_snapshot": profile_payload,
            "activity": {"phase": "instagram_profile_snapshot_end"},
        },
    )

def _instagram_following_rows_from_payload(
    payload: Mapping[str, Any],
    *,
    owner_username: str,
    source_cursor: str | None,
    source_page_ordinal: int,
    starting_rank: int,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    raw_rows = payload.get("users")
    if not isinstance(raw_rows, list):
        raw_rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    rows: list[dict[str, Any]] = []
    for index, raw_row in enumerate(raw_rows):
        if not isinstance(raw_row, Mapping):
            continue
        row = dict(raw_row)
        row.setdefault("username_scrape", owner_username)
        row.setdefault("type", "Following")
        row.setdefault("source_rank", starting_rank + index)
        row.setdefault("source_cursor", source_cursor)
        row.setdefault("source_page_ordinal", source_page_ordinal)
        rows.append(row)
    next_cursor = str(payload.get("next_max_id") or payload.get("next_cursor") or "").strip() or None
    has_more = bool(payload.get("has_more") or payload.get("big_list") or next_cursor)
    return rows, next_cursor, has_more

def _fetch_instagram_following_rows(
    *,
    account_handle: str,
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scraper = _instagram_profile_scraper(config, account_handle=account_handle)
    delay_seconds = float(config.get("delay_seconds") or 0)
    profile_payload = scraper.fetch_profile_info(
        account_handle,
        delay=delay_seconds,
        request_timeout=(10, 30),
    )
    user = _metadata_dict(_metadata_dict(profile_payload or {}).get("data")).get("user")
    user = user if isinstance(user, Mapping) else {}
    user_id = str(user.get("id") or user.get("pk") or "").strip()
    if not user_id:
        raise SharedStageRuntimeError(
            f"Instagram following scrape failed for @{account_handle}: missing profile id",
            error_code="instagram_following_missing_profile_id",
            retryable=True,
            runtime_metadata={"profile_payload": _metadata_dict(profile_payload or {})},
        )
    max_pages = max(1, min(_normalize_non_negative_int(config.get("max_pages")) or 1, 25))
    page_size = max(1, min(_normalize_non_negative_int(config.get("page_size")) or 50, 200))
    max_relationships = max(1, min(_normalize_non_negative_int(config.get("max_relationships")) or page_size, 5000))
    cursor = str(config.get("cursor") or config.get("max_id") or "").strip() or None
    rows: list[dict[str, Any]] = []
    page_count = 0
    has_more = False
    while page_count < max_pages and len(rows) < max_relationships:
        params: dict[str, Any] = {"count": min(page_size, max_relationships - len(rows))}
        if cursor:
            params["max_id"] = cursor
        url = f"https://www.instagram.com/api/v1/friendships/{user_id}/following/"
        payload = scraper._request_client.get_json(  # noqa: SLF001
            url,
            query_type="profile_following",
            headers=scraper._get_headers(f"https://www.instagram.com/{account_handle}/"),  # noqa: SLF001
            cookies=scraper._request_cookies(),  # noqa: SLF001
            params=params,
            timeout=(10, 30),
            sender=scraper._get,  # noqa: SLF001
        )
        page_rows, next_cursor, has_more = _instagram_following_rows_from_payload(
            payload,
            owner_username=account_handle,
            source_cursor=cursor,
            source_page_ordinal=page_count,
            starting_rank=len(rows),
        )
        rows.extend(page_rows)
        page_count += 1
        if not next_cursor or not has_more:
            cursor = next_cursor
            break
        cursor = next_cursor
    return rows[:max_relationships], {
        "profile_payload": _metadata_dict(profile_payload or {}),
        "profile_id": user_id,
        "pages_fetched": page_count,
        "next_cursor": cursor,
        "has_more": has_more,
        "max_pages": max_pages,
        "max_relationships": max_relationships,
    }

def _run_instagram_profile_following_stage(
    *,
    run_id: str,
    source_scope: str,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
) -> tuple[int, int, dict[str, Any]]:
    rows, fetch_meta = _fetch_instagram_following_rows(account_handle=account_handle, config=config)
    if fetch_meta.get("profile_payload"):
        persist_instagram_profile_snapshot(
            fetch_meta["profile_payload"],
            source_scope=source_scope,
            source_account=account_handle,
            job_id=job_id,
            run_id=run_id,
        )
    result = persist_instagram_profile_relationships(
        rows,
        owner_username=account_handle,
        source_scope=source_scope,
        intended_relationship_type="following",
        source_cursor=str(fetch_meta.get("next_cursor") or "") or None,
        job_id=job_id,
        run_id=run_id,
    )
    metadata = {
        "stage": INSTAGRAM_PROFILE_FOLLOWING_STAGE,
        "platform": "instagram",
        "account": account_handle,
        "relationship_type": "following",
        "relationships_fetched": len(rows),
        "relationships_upserted": result.get("rows_upserted"),
        "relationship_mismatches": result.get("mismatches") or [],
        "retrieval_meta": fetch_meta,
        "activity": {"phase": "instagram_profile_following_end"},
    }
    _touch_shared_account_source(
        source_scope=source_scope,
        platform="instagram",
        account_handle=account_handle,
        run_id=run_id or None,
        job_id=job_id,
        last_scrape_status="completed",
        metadata_updates={"profile_following": metadata},
    )
    return 0, 0, metadata

def _instagram_profile_tables_ready(*, conn: Any | None = None) -> bool:
    try:
        return all(
            [
                _column_exists("social", "instagram_profiles", "normalized_username", conn=conn),
                _column_exists("social", "instagram_profile_external_links", "profile_id", conn=conn),
                _column_exists("social", "instagram_profile_relationships", "owner_profile_id", conn=conn),
            ]
        )
    except Exception:
        logger.debug("[instagram] Profile queryable tables are not ready", exc_info=True)
        return False

def _normalize_instagram_profile_source_scope(source_scope: Any) -> str:
    try:
        return normalize_source_scope(source_scope)
    except ValueError as exc:
        raise ValueError(f"Unsupported Instagram profile source_scope: {source_scope}") from exc

def _instagram_profile_fetch_one(
    sql: str,
    params: Sequence[Any],
    *,
    conn: Any | None = None,
    label: str = "instagram_profile_fetch_one",
) -> dict[str, Any] | None:
    if conn is None:
        return pg.fetch_one(sql, list(params))
    with pg.db_cursor(conn=conn, label=label) as cur:
        return pg.fetch_one_with_cursor(cur, sql, list(params))

def _instagram_profile_fetch_all(
    sql: str,
    params: Sequence[Any],
    *,
    conn: Any | None = None,
    label: str = "instagram_profile_fetch_all",
) -> list[dict[str, Any]]:
    if conn is None:
        return pg.fetch_all(sql, list(params))
    with pg.db_cursor(conn=conn, label=label) as cur:
        return pg.fetch_all_with_cursor(cur, sql, list(params))

def _instagram_profile_execute_one(
    sql: str,
    params: Sequence[Any],
    *,
    conn: Any | None = None,
    label: str = "instagram_profile_execute_one",
) -> dict[str, Any] | None:
    return _instagram_profile_fetch_one(sql, params, conn=conn, label=label)

def _instagram_profile_execute(
    sql: str,
    params: Sequence[Any],
    *,
    conn: Any | None = None,
) -> None:
    pg.execute(sql, list(params), conn=conn)

def _instagram_profile_parse_about_timestamp(about_raw: Mapping[str, Any], *keys: str) -> datetime | None:
    for key in keys:
        raw_value = about_raw.get(key)
        if raw_value in (None, ""):
            continue
        try:
            return datetime.fromtimestamp(int(raw_value), tz=UTC)
        except (TypeError, ValueError, OSError):
            parsed = _coerce_dt(raw_value)
            if parsed is not None:
                return parsed
    return None

def _instagram_profile_domain(url: Any) -> str | None:
    parsed = urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None

def _instagram_profile_normalized_url(url: Any) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", parsed.query, ""))

def _instagram_profile_merge_rows(*, keep_id: str, discard_id: str, conn: Any | None) -> None:
    if keep_id == discard_id:
        return
    statements: list[tuple[str, list[Any]]] = [
        (
            """
            delete from social.instagram_profile_external_links losing
            using social.instagram_profile_external_links kept
            where losing.profile_id = %s::uuid
              and kept.profile_id = %s::uuid
              and kept.link_index = losing.link_index
              and kept.url = losing.url
            """,
            [discard_id, keep_id],
        ),
        (
            """
            update social.instagram_profile_external_links
            set profile_id = %s::uuid,
                updated_at = now()
            where profile_id = %s::uuid
            """,
            [keep_id, discard_id],
        ),
        (
            """
            delete from social.instagram_profile_relationships losing
            using social.instagram_profile_relationships kept
            where losing.owner_profile_id = %s::uuid
              and kept.owner_profile_id = %s::uuid
              and kept.relationship_type = losing.relationship_type
              and (
                (
                  losing.related_user_id is not null
                  and kept.related_user_id = losing.related_user_id
                )
                or (
                  losing.related_user_id is null
                  and kept.related_user_id is null
                  and kept.related_normalized_username = losing.related_normalized_username
                )
              )
            """,
            [discard_id, keep_id],
        ),
        (
            """
            update social.instagram_profile_relationships
            set owner_profile_id = %s::uuid,
                updated_at = now()
            where owner_profile_id = %s::uuid
            """,
            [keep_id, discard_id],
        ),
        (
            "delete from social.instagram_profiles where id = %s::uuid",
            [discard_id],
        ),
    ]
    for sql, params in statements:
        _instagram_profile_execute(sql, params, conn=conn)

def _instagram_profile_existing_row(
    *,
    profile_id: str | None,
    source_scope: str,
    normalized_username: str,
    conn: Any | None,
) -> dict[str, Any] | None:
    by_profile_id: dict[str, Any] | None = None
    by_username: dict[str, Any] | None = None
    if profile_id:
        by_profile_id = _instagram_profile_fetch_one(
            """
            select *
            from social.instagram_profiles
            where profile_id = %s
            limit 1
            """,
            [profile_id],
            conn=conn,
            label="instagram_profile_existing_by_id",
        )
    if normalized_username:
        by_username = _instagram_profile_fetch_one(
            """
            select *
            from social.instagram_profiles
            where source_scope = %s
              and normalized_username = %s
            order by (profile_id is not null) desc, last_seen_at desc, updated_at desc
            limit 1
            """,
            [source_scope, normalized_username],
            conn=conn,
            label="instagram_profile_existing_by_username",
        )
    if by_profile_id and by_username and by_profile_id.get("id") != by_username.get("id"):
        _instagram_profile_merge_rows(
            keep_id=str(by_profile_id["id"]),
            discard_id=str(by_username["id"]),
            conn=conn,
        )
        return by_profile_id
    return by_profile_id or by_username

def _sync_instagram_profile_external_links(
    *,
    profile_row_id: str,
    instagram_profile_id: str | None,
    username: str,
    normalized_username: str,
    external_links: Sequence[Any],
    job_id: str | None,
    run_id: str | None,
    conn: Any | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, link in enumerate(external_links):
        raw_link = link.to_dict() if hasattr(link, "to_dict") else _metadata_dict(link)
        url = str(raw_link.get("url") or "").strip()
        if not url:
            continue
        payload = {
            "profile_id": profile_row_id,
            "instagram_profile_id": instagram_profile_id,
            "username": username,
            "normalized_username": normalized_username,
            "link_index": index,
            "title": str(raw_link.get("title") or "").strip() or None,
            "url": url,
            "shim_url": str(raw_link.get("shim_url") or raw_link.get("lynx_url") or "").strip() or None,
            "normalized_url": _instagram_profile_normalized_url(url),
            "normalized_domain": _instagram_profile_domain(url),
            "link_type": str(raw_link.get("link_type") or "").strip() or None,
            "raw_data": raw_link,
            "last_seen_at": _now_utc(),
            "last_scrape_job_id": job_id,
            "last_scrape_run_id": run_id,
        }
        _pg_upsert(
            "instagram_profile_external_links",
            payload,
            conflict_col=["profile_id", "link_index", "url"],
            conn=conn,
        )
        rows.append(payload)
    return rows

def persist_instagram_profile_snapshot(
    profile_payload: Mapping[str, Any],
    *,
    source_scope: str = "network",
    source_account: str | None = None,
    shared_account_source_id: str | None = None,
    job_id: str | None = None,
    run_id: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    if not _instagram_profile_tables_ready(conn=conn):
        raise RuntimeError("Instagram profile queryable tables are not available.")

    from trr_backend.socials.instagram.profile_normalizer import normalize_instagram_profile

    normalized_scope = _normalize_instagram_profile_source_scope(source_scope)
    profile = normalize_instagram_profile(dict(profile_payload or {}))
    normalized_username = _normalize_account_handle(profile.username)
    if not normalized_username:
        raise ValueError("Instagram profile payload is missing a valid username.")
    username = profile.username or normalized_username
    profile_id = str(profile.profile_id or profile.pk or "").strip() or None
    about_raw = dict(profile.about_raw or {})
    source_account = _normalize_account_handle(source_account or username) or username

    if shared_account_source_id is None:
        try:
            source_row = _load_shared_account_source_row(
                source_scope=normalized_scope,
                platform="instagram",
                account_handle=source_account,
            )
            shared_account_source_id = str((source_row or {}).get("id") or "").strip() or None
        except Exception:
            shared_account_source_id = None

    existing = _instagram_profile_existing_row(
        profile_id=profile_id,
        source_scope=normalized_scope,
        normalized_username=normalized_username,
        conn=conn,
    )
    now = _now_utc()
    payload = {
        "shared_account_source_id": shared_account_source_id,
        "source_scope": normalized_scope,
        "source_account": source_account,
        "profile_id": profile_id,
        "input_url": profile.input_url,
        "username": username,
        "normalized_username": normalized_username,
        "url": profile.url or f"https://www.instagram.com/{normalized_username}",
        "full_name": profile.full_name,
        "biography": profile.biography,
        "country": profile.country,
        "date_joined": profile.date_joined,
        "date_joined_at": _instagram_profile_parse_about_timestamp(
            about_raw,
            "date_joined_as_timestamp",
            "joined_date_as_timestamp",
        ),
        "date_verified": profile.date_verified,
        "date_verified_at": _instagram_profile_parse_about_timestamp(
            about_raw,
            "date_verified_as_timestamp",
            "verified_date_as_timestamp",
        ),
        "former_usernames_count": profile.former_usernames_count,
        "followers_count": profile.followers_count,
        "follows_count": profile.follows_count,
        "posts_count": profile.posts_count,
        "highlight_reel_count": profile.highlight_reel_count,
        "igtv_video_count": profile.igtv_video_count,
        "is_business_account": profile.is_business_account,
        "joined_recently": profile.joined_recently,
        "has_channel": profile.has_channel,
        "business_category_name": profile.business_category_name or profile.category_name,
        "is_private": profile.is_private,
        "is_verified": profile.is_verified,
        "external_url": profile.external_url,
        "external_url_shimmed": profile.external_url_shimmed,
        "profile_pic_url": profile.profile_pic_url,
        "profile_pic_url_hd": profile.profile_pic_url_hd,
        "about_raw": about_raw,
        "raw_data": dict(profile.raw_data or profile_payload or {}),
        "last_seen_at": now,
        "last_scraped_at": now,
        "last_scrape_job_id": job_id,
        "last_scrape_run_id": run_id,
        "updated_at": now,
    }
    if existing:
        set_columns = [column for column in payload if column != "id"]
        adapted = _adapt_payload_json_values(payload)
        sql = f"""
            update social.instagram_profiles
            set {", ".join(f"{column} = %s" for column in set_columns)}
            where id = %s::uuid
            returning *
        """
        row = _instagram_profile_execute_one(
            sql,
            [adapted[column] for column in set_columns] + [existing["id"]],
            conn=conn,
            label="instagram_profile_update",
        )
    else:
        insert_payload = {"first_seen_at": now, "created_at": now, **payload}
        adapted = _adapt_payload_json_values(insert_payload)
        columns = list(adapted)
        row = _instagram_profile_execute_one(
            f"""
            insert into social.instagram_profiles ({", ".join(columns)})
            values ({", ".join(["%s"] * len(columns))})
            returning *
            """,
            list(adapted.values()),
            conn=conn,
            label="instagram_profile_insert",
        )

    row_id = str((row or {}).get("id") or "").strip()
    if row_id:
        _sync_instagram_profile_external_links(
            profile_row_id=row_id,
            instagram_profile_id=profile_id,
            username=username,
            normalized_username=normalized_username,
            external_links=profile.external_links,
            job_id=job_id,
            run_id=run_id,
            conn=conn,
        )
    return dict(row or {})

def _instagram_profile_row_for_username(
    account_handle: str,
    *,
    source_scope: str = "network",
    conn: Any | None = None,
) -> dict[str, Any] | None:
    normalized_scope = _normalize_instagram_profile_source_scope(source_scope)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if not _instagram_profile_tables_ready(conn=conn):
        raise RuntimeError("Instagram profile queryable tables are not available.")
    return _instagram_profile_fetch_one(
        """
        select *
        from social.instagram_profiles
        where source_scope = %s
          and normalized_username = %s
        order by (profile_id is not null) desc, last_scraped_at desc nulls last, updated_at desc
        limit 1
        """,
        [normalized_scope, normalized_account],
        conn=conn,
        label="instagram_profile_row_for_username",
    )

def persist_instagram_profile_relationships(
    relationship_payloads: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    owner_username: str,
    source_scope: str = "network",
    intended_relationship_type: str = "following",
    source_cursor: str | None = None,
    source_page_ordinal: int | None = None,
    job_id: str | None = None,
    run_id: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    if not _instagram_profile_tables_ready(conn=conn):
        raise RuntimeError("Instagram profile queryable tables are not available.")

    from trr_backend.socials.instagram.profile_relationship_normalizer import (
        normalize_instagram_profile_relationships,
    )

    normalized_owner = _normalize_social_account_profile_handle(owner_username)
    owner_row = _instagram_profile_row_for_username(normalized_owner, source_scope=source_scope, conn=conn)
    if not owner_row:
        owner_row = persist_instagram_profile_snapshot(
            {"username": normalized_owner, "url": f"https://www.instagram.com/{normalized_owner}"},
            source_scope=source_scope,
            source_account=normalized_owner,
            job_id=job_id,
            run_id=run_id,
            conn=conn,
        )
    owner_profile_id = str((owner_row or {}).get("id") or "").strip()
    if not owner_profile_id:
        raise RuntimeError("Unable to materialize owner Instagram profile for relationship sync.")

    result = normalize_instagram_profile_relationships(
        relationship_payloads,
        owner_username=normalized_owner,
        intended_relationship_type=intended_relationship_type,
        source_cursor=source_cursor,
        source_page_ordinal=source_page_ordinal,
    )
    rows_upserted = 0
    now = _now_utc()
    for relationship in result.relationships:
        related_normalized_username = _normalize_account_handle(relationship.related_username)
        if not related_normalized_username:
            continue
        payload = {
            "owner_profile_id": owner_profile_id,
            "owner_instagram_profile_id": owner_row.get("profile_id"),
            "owner_username": normalized_owner,
            "owner_normalized_username": normalized_owner,
            "relationship_type": "following",
            "related_user_id": relationship.related_user_id,
            "related_username": relationship.related_username,
            "related_normalized_username": related_normalized_username,
            "related_full_name": relationship.related_full_name,
            "related_is_private": relationship.related_is_private,
            "related_is_verified": relationship.related_is_verified,
            "related_profile_pic_url": relationship.related_profile_pic_url,
            "raw_data": dict(relationship.raw_data or {}),
            "source_page_ordinal": relationship.source_page_ordinal,
            "source_cursor": relationship.source_cursor,
            "source_page_size": len(result.relationships),
            "source_rank": relationship.source_rank,
            "last_seen_at": now,
            "missing_at": None,
            "is_missing": False,
            "last_scrape_job_id": job_id,
            "last_scrape_run_id": run_id,
            "updated_at": now,
        }
        existing = _instagram_profile_fetch_one(
            """
            select id
            from social.instagram_profile_relationships
            where owner_profile_id = %s::uuid
              and relationship_type = 'following'
              and (
                (%s is not null and related_user_id = %s)
                or (
                  %s is null
                  and related_user_id is null
                  and related_normalized_username = %s
                )
              )
            limit 1
            """,
            [
                owner_profile_id,
                relationship.related_user_id,
                relationship.related_user_id,
                relationship.related_user_id,
                related_normalized_username,
            ],
            conn=conn,
            label="instagram_profile_relationship_existing",
        )
        if existing:
            adapted = _adapt_payload_json_values(payload)
            columns = list(adapted)
            _instagram_profile_execute_one(
                f"""
                update social.instagram_profile_relationships
                set {", ".join(f"{column} = %s" for column in columns)}
                where id = %s::uuid
                returning id::text
                """,
                list(adapted.values()) + [existing["id"]],
                conn=conn,
                label="instagram_profile_relationship_update",
            )
        else:
            insert_payload = {"first_seen_at": now, "created_at": now, **payload}
            adapted = _adapt_payload_json_values(insert_payload)
            columns = list(adapted)
            _instagram_profile_execute_one(
                f"""
                insert into social.instagram_profile_relationships ({", ".join(columns)})
                values ({", ".join(["%s"] * len(columns))})
                returning id::text
                """,
                list(adapted.values()),
                conn=conn,
                label="instagram_profile_relationship_insert",
            )
        rows_upserted += 1
    return {
        "owner_username": normalized_owner,
        "relationship_type": "following",
        "rows_upserted": rows_upserted,
        "mismatches": [asdict(mismatch) for mismatch in result.mismatches],
        "page_info": result.page_info,
    }

def _instagram_profile_response(row: Mapping[str, Any], links: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    username = str(row.get("username") or row.get("normalized_username") or "").strip()
    profile_id = str(row.get("profile_id") or "").strip() or None
    return {
        "id": profile_id,
        "row_id": str(row.get("id") or "").strip() or None,
        "username": username,
        "url": row.get("url") or (f"https://www.instagram.com/{username}" if username else None),
        "full_name": row.get("full_name"),
        "biography": row.get("biography"),
        "about": {
            "country": row.get("country"),
            "date_joined": row.get("date_joined"),
            "date_joined_at": _iso(_coerce_dt(row.get("date_joined_at"))),
            "date_verified": row.get("date_verified"),
            "date_verified_at": _iso(_coerce_dt(row.get("date_verified_at"))),
            "former_usernames_count": row.get("former_usernames_count"),
        },
        "counts": {
            "followers": _normalize_non_negative_int(row.get("followers_count")),
            "following": _normalize_non_negative_int(row.get("follows_count")),
            "posts": _normalize_non_negative_int(row.get("posts_count")),
            "highlight_reels": _normalize_non_negative_int(row.get("highlight_reel_count")),
            "igtv_videos": _normalize_non_negative_int(row.get("igtv_video_count")),
        },
        "flags": {
            "is_business_account": row.get("is_business_account"),
            "joined_recently": row.get("joined_recently"),
            "has_channel": row.get("has_channel"),
            "is_private": row.get("is_private"),
            "is_verified": row.get("is_verified"),
        },
        "business_category_name": row.get("business_category_name"),
        "external_url": row.get("external_url"),
        "external_url_shimmed": row.get("external_url_shimmed"),
        "external_links": [
            {
                "title": link.get("title"),
                "url": link.get("url"),
                "shim_url": link.get("shim_url"),
                "normalized_domain": link.get("normalized_domain"),
                "link_type": link.get("link_type"),
            }
            for link in links
        ],
        "profile_pic_url": row.get("profile_pic_url"),
        "profile_pic_url_hd": row.get("profile_pic_url_hd"),
        "hosted_profile_pic_url": row.get("hosted_profile_pic_url"),
        "hosted_profile_pic_url_hd": row.get("hosted_profile_pic_url_hd"),
        "source_scope": row.get("source_scope"),
        "source_account": row.get("source_account"),
        "last_scraped_at": _iso(_coerce_dt(row.get("last_scraped_at"))),
        "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
    }

def get_instagram_profile_detail(
    account_handle: str,
    *,
    source_scope: str = "network",
    conn: Any | None = None,
) -> dict[str, Any]:
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    row = _instagram_profile_row_for_username(normalized_account, source_scope=source_scope, conn=conn)
    if not row:
        raise LookupError("Instagram profile not found.")
    links = _instagram_profile_fetch_all(
        """
        select
          id::text as id,
          title,
          url,
          shim_url,
          normalized_domain,
          link_type,
          link_index,
          last_seen_at
        from social.instagram_profile_external_links
        where profile_id = %s::uuid
        order by link_index asc, url asc
        """,
        [row["id"]],
        conn=conn,
        label="instagram_profile_detail_links",
    )
    return {"profile": _instagram_profile_response(row, links)}

def get_instagram_profile_relationships(
    account_handle: str,
    *,
    source_scope: str = "network",
    relationship_type: str = "following",
    page: int = 1,
    page_size: int = _SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE,
    conn: Any | None = None,
) -> dict[str, Any]:
    normalized_relationship_type = str(relationship_type or "following").strip().lower()
    if normalized_relationship_type != "following":
        raise ValueError("Instagram profile relationships currently support type=following only.")
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    safe_page = max(1, int(page))
    safe_page_size = max(1, min(int(page_size), _SOCIAL_ACCOUNT_PROFILE_MAX_PAGE_SIZE))
    owner_row = _instagram_profile_row_for_username(normalized_account, source_scope=source_scope, conn=conn)
    if not owner_row:
        raise LookupError("Instagram profile not found.")
    total_row = (
        _instagram_profile_fetch_one(
            """
        select count(*)::int as total
        from social.instagram_profile_relationships
        where owner_profile_id = %s::uuid
          and relationship_type = 'following'
          and coalesce(is_missing, false) = false
        """,
            [owner_row["id"]],
            conn=conn,
            label="instagram_profile_relationship_total",
        )
        or {}
    )
    rows = _instagram_profile_fetch_all(
        """
        select
          id::text as id,
          relationship_type,
          related_user_id,
          related_username,
          related_normalized_username,
          related_full_name,
          related_is_private,
          related_is_verified,
          related_profile_pic_url,
          hosted_related_profile_pic_url,
          source_rank,
          source_page_ordinal,
          source_cursor,
          last_seen_at
        from social.instagram_profile_relationships
        where owner_profile_id = %s::uuid
          and relationship_type = 'following'
          and coalesce(is_missing, false) = false
        order by source_rank asc nulls last, related_normalized_username asc, id asc
        limit %s
        offset %s
        """,
        [owner_row["id"], safe_page_size, (safe_page - 1) * safe_page_size],
        conn=conn,
        label="instagram_profile_relationship_rows",
    )
    total = _normalize_non_negative_int(total_row.get("total"))
    return {
        "owner": {
            "id": owner_row.get("profile_id"),
            "username": owner_row.get("username") or normalized_account,
            "row_id": str(owner_row.get("id") or "").strip(),
        },
        "relationship_type": "following",
        "items": [
            {
                "id": row.get("id"),
                "relationship_type": row.get("relationship_type"),
                "user": {
                    "id": row.get("related_user_id"),
                    "username": row.get("related_username"),
                    "normalized_username": row.get("related_normalized_username"),
                    "full_name": row.get("related_full_name"),
                    "is_private": row.get("related_is_private"),
                    "is_verified": row.get("related_is_verified"),
                    "profile_pic_url": row.get("related_profile_pic_url"),
                    "hosted_profile_pic_url": row.get("hosted_related_profile_pic_url"),
                },
                "source_rank": row.get("source_rank"),
                "source_page_ordinal": row.get("source_page_ordinal"),
                "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
            }
            for row in rows
        ],
        "pagination": {
            "page": safe_page,
            "page_size": safe_page_size,
            "total": total,
            "total_pages": max(1, (total + safe_page_size - 1) // safe_page_size) if safe_page_size else 1,
        },
    }


_LOCAL_ROOM_NAMES = {
    '_instagram_profile_scraper',
    '_run_instagram_profile_snapshot_stage',
    '_instagram_following_rows_from_payload',
    '_fetch_instagram_following_rows',
    '_run_instagram_profile_following_stage',
    '_instagram_profile_tables_ready',
    '_normalize_instagram_profile_source_scope',
    '_instagram_profile_fetch_one',
    '_instagram_profile_fetch_all',
    '_instagram_profile_execute_one',
    '_instagram_profile_execute',
    '_instagram_profile_parse_about_timestamp',
    '_instagram_profile_domain',
    '_instagram_profile_normalized_url',
    '_instagram_profile_merge_rows',
    '_instagram_profile_existing_row',
    '_sync_instagram_profile_external_links',
    'persist_instagram_profile_snapshot',
    '_instagram_profile_row_for_username',
    'persist_instagram_profile_relationships',
    '_instagram_profile_response',
    'get_instagram_profile_detail',
    'get_instagram_profile_relationships',
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    '_instagram_profile_scraper',
    '_run_instagram_profile_snapshot_stage',
    '_instagram_following_rows_from_payload',
    '_fetch_instagram_following_rows',
    '_run_instagram_profile_following_stage',
    '_instagram_profile_tables_ready',
    '_normalize_instagram_profile_source_scope',
    '_instagram_profile_fetch_one',
    '_instagram_profile_fetch_all',
    '_instagram_profile_execute_one',
    '_instagram_profile_execute',
    '_instagram_profile_parse_about_timestamp',
    '_instagram_profile_domain',
    '_instagram_profile_normalized_url',
    '_instagram_profile_merge_rows',
    '_instagram_profile_existing_row',
    '_sync_instagram_profile_external_links',
    'persist_instagram_profile_snapshot',
    '_instagram_profile_row_for_username',
    'persist_instagram_profile_relationships',
    '_instagram_profile_response',
    'get_instagram_profile_detail',
    'get_instagram_profile_relationships',
]

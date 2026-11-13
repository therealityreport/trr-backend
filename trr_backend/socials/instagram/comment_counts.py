"""Additive observed counts and canonical inventory, independent of collection progress.

Legacy counters and completeness certificates deliberately do not participate in
this contract. An absent observation is not a measured zero.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel

from trr_backend.db import pg

logger = logging.getLogger(__name__)
OBSERVATION_KEY = "comment_count_observations"
SOURCE_FIELDS = ("post_reported_count", "facebook_count_observed_on_instagram", "threads_count_observed_on_instagram")
INVENTORY_FIELDS: tuple[str, ...] = (
    "instagram_saved_count",
    "instagram_saved_top_level_count",
    "instagram_saved_reply_count",
    "instagram_saved_unclassified_count",
    "instagram_marked_missing_count",
)


class CommentCountObservation(BaseModel):
    value: int | None
    status: Literal["observed", "unknown"]
    source: str | None
    source_field: str | None
    source_surface: str | None
    observed_at: str | None
    evidence_reference: str | None
    observed_in_latest_response: bool | None
    unknown_reason: str | None


class InstagramCommentCounts(BaseModel):
    version: Literal[1] = 1
    post_reported_count: int | None
    instagram_saved_count: int | None
    instagram_saved_top_level_count: int | None
    instagram_saved_reply_count: int | None
    instagram_saved_unclassified_count: int | None
    instagram_marked_missing_count: int | None
    facebook_count_observed_on_instagram: int | None
    threads_count_observed_on_instagram: int | None
    post_minus_saved_difference: int | None
    inventory_status: Literal["available", "unavailable"]
    inventory_observed_at: str | None
    observations: dict[str, CommentCountObservation]
    observation_conflicts: list[dict[str, Any]]


class InstagramCommentCountsSummary(BaseModel):
    version: Literal[1] = 1
    posts_count: int
    post_reported_known_subtotal: int
    post_reported_known_posts_count: int
    post_reported_unknown_posts_count: int
    posts_with_reported_comments_count: int
    posts_reporting_zero_comments_count: int
    reported_count_unknown_posts_count: int
    posts_with_reported_comments_but_no_saved_records_count: int | None
    facebook_known_subtotal: int
    facebook_known_posts_count: int
    facebook_unknown_posts_count: int
    threads_known_subtotal: int
    threads_known_posts_count: int
    threads_unknown_posts_count: int
    instagram_saved_count: int | None
    instagram_saved_top_level_count: int | None
    instagram_saved_reply_count: int | None
    instagram_saved_unclassified_count: int | None
    instagram_marked_missing_count: int | None
    post_minus_saved_difference_known_subtotal: int | None
    inventory_status: Literal["available", "unavailable"]
    inventory_observed_at: str | None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if 0 <= value <= 9223372036854775807 else None
    if isinstance(value, str) and len(value.strip()) <= 19 and value.strip().isascii() and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed <= 9223372036854775807 else None
    return None


def _time(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if not isinstance(value, str):
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value


def _unknown(reason: str = "not_observed") -> dict[str, Any]:
    return {
        "value": None,
        "status": "unknown",
        "source": None,
        "source_field": None,
        "source_surface": None,
        "observed_at": None,
        "evidence_reference": None,
        "observed_in_latest_response": None,
        "unknown_reason": reason,
    }


def _observation(value: int, field: str, raw: Mapping[str, Any], *, observed_at: Any = None) -> dict[str, Any]:
    snapshot = _mapping(raw.get("detail_snapshot"))
    return {
        "value": value,
        "status": "observed",
        "source": "instagram_response",
        "source_field": field,
        "source_surface": "instagram_post_details" if field != "fb_comment_count" else "instagram_media_response",
        "observed_at": _time(observed_at or snapshot.get("observed_at")),
        "evidence_reference": f"retained_payload:{field}",
        "observed_in_latest_response": None,
        "unknown_reason": None,
    }


def _validated_observation(value: Any, metric: str) -> dict[str, Any]:
    candidate = _mapping(value)
    fields = {
        "post_reported_count": {"comment_count", "commentsCount", "edge_media_to_comment.count"},
        "facebook_count_observed_on_instagram": {"fb_comment_count"},
    }
    field = candidate.get("source_field")
    if (
        count(candidate.get("value")) is None
        or not isinstance(candidate.get("value"), int)
        or candidate.get("status") != "observed"
        or not isinstance(field, str)
        or field not in fields.get(metric, set())
        or candidate.get("source") != "instagram_response"
        or candidate.get("source_surface")
        != ("instagram_post_details" if metric == "post_reported_count" else "instagram_media_response")
        or candidate.get("evidence_reference") != f"retained_payload:{field}"
    ):
        return _unknown("invalid_or_absent_retained_observation")
    return {
        **_unknown(),
        **{key: candidate.get(key) for key in _unknown()},
        "value": count(candidate["value"]),
        "unknown_reason": None,
        "observed_at": _time(candidate.get("observed_at")),
        "observed_in_latest_response": candidate.get("observed_in_latest_response")
        if isinstance(candidate.get("observed_in_latest_response"), bool)
        else None,
    }


def _conflicts(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = []
    for item in raw.get("comment_count_observation_conflicts") or []:
        metric = _mapping(item).get("metric")
        if metric not in SOURCE_FIELDS:
            continue
        observation = _validated_observation(item, metric)
        if observation["value"] is not None:
            result.append({"metric": metric, **observation})
    return result


def observations_from_raw(
    raw: Mapping[str, Any], *, observed_at: Any = None, source_surface: str | None = None
) -> dict[str, Any]:
    """Read retained observations; parse post totals only on a proven detail surface.

    Serialized DTO defaults and generic parser presence markers do not prove
    that a field came from an individual post-details response.
    """
    stored = _mapping(raw.get(OBSERVATION_KEY))
    result = {key: _validated_observation(stored.get(key), key) for key in SOURCE_FIELDS}
    candidates = [
        ("comment_count", raw.get("comment_count")),
        ("commentsCount", raw.get("commentsCount")),
        ("edge_media_to_comment.count", _mapping(raw.get("edge_media_to_comment")).get("count")),
    ]
    for field, value in candidates:
        parsed = count(value)
        if (
            source_surface == "instagram_post_details"
            and parsed is not None
            and result["post_reported_count"]["value"] is None
        ):
            result["post_reported_count"] = _observation(parsed, field, raw, observed_at=observed_at)
            break
    facebook = count(raw.get("fb_comment_count"))
    native_facebook_observed = facebook is not None
    crosspost = _mapping(raw.get("facebook_crosspost"))
    # This retained object is produced exclusively from fb_comment_count.
    if facebook is None and crosspost.get("source"):
        facebook = count(crosspost.get("comments_count"))
    if facebook is not None and result["facebook_count_observed_on_instagram"]["value"] is None:
        result["facebook_count_observed_on_instagram"] = _observation(
            facebook,
            "fb_comment_count",
            raw,
            observed_at=observed_at if native_facebook_observed else crosspost.get("observed_at") or observed_at,
        )
    # No supported Threads extraction has been demonstrated. Do not infer one.
    result["threads_count_observed_on_instagram"] = _unknown("no_supported_source_evidence")
    return result


def merge_observations(incoming: Mapping[str, Any], previous: Mapping[str, Any], *, observed_at: Any) -> dict[str, Any]:
    """Preserve omitted observations, their timestamps, and conflicting evidence."""
    prior = observations_from_raw(previous)
    fresh = observations_from_raw(incoming, observed_at=observed_at)
    conflicts = _conflicts(previous)
    conflicts.extend(item for item in _conflicts(incoming) if item not in conflicts)
    for field, value in (
        ("comment_count", incoming.get("comment_count")),
        ("commentsCount", incoming.get("commentsCount")),
        ("edge_media_to_comment.count", _mapping(incoming.get("edge_media_to_comment")).get("count")),
    ):
        parsed = count(value)
        if (
            fresh["post_reported_count"]["value"] is not None
            and parsed is not None
            and parsed != fresh["post_reported_count"]["value"]
        ):
            conflict = {
                "metric": "post_reported_count",
                **_observation(parsed, field, incoming, observed_at=observed_at),
            }
            if conflict not in conflicts:
                conflicts.append(conflict)
    merged: dict[str, Any] = {}
    for key in SOURCE_FIELDS:
        old, new = prior[key], fresh[key]
        if count(new.get("value")) is None:
            merged[key] = {**old, "observed_in_latest_response": False if old.get("value") is not None else None}
        else:
            if old.get("value") is not None and old.get("value") != new["value"]:
                conflict = {"metric": key, **old}
                if conflict not in conflicts:
                    conflicts.append(conflict)
            merged[key] = {**new, "observed_in_latest_response": True}
    return {OBSERVATION_KEY: merged, "comment_count_observation_conflicts": conflicts}


def reconcile_prior_observations(*payloads: Mapping[str, Any]) -> dict[str, Any]:
    """Keep the newest timed prior evidence per metric, retaining other values."""
    selected = observations_from_raw({})
    conflicts: list[dict[str, Any]] = []

    def instant(observation: Mapping[str, Any]) -> float:
        value = observation.get("observed_at")
        if not isinstance(value, str):
            return float("-inf")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.timestamp() if parsed.utcoffset() is not None else float("-inf")
        except ValueError:
            return float("-inf")

    for payload in payloads:
        conflicts.extend(item for item in _conflicts(payload) if item not in conflicts)
        for metric, candidate in observations_from_raw(payload).items():
            if candidate["value"] is None:
                continue
            previous = selected[metric]
            if previous["value"] is None or instant(candidate) > instant(previous):
                selected[metric] = candidate
                displaced = previous
            else:
                displaced = candidate
            if displaced["value"] is not None and displaced["value"] != selected[metric]["value"]:
                conflict = {"metric": metric, **displaced}
                if conflict not in conflicts:
                    conflicts.append(conflict)
    return {OBSERVATION_KEY: selected, "comment_count_observation_conflicts": conflicts}


def build_comment_counts(row: Mapping[str, Any], inventory: Mapping[str, Any] | None = None) -> dict[str, Any]:
    raw = _mapping(row.get("raw_data"))
    observations = observations_from_raw(raw)
    reported = count(observations["post_reported_count"].get("value"))
    saved = count(inventory.get("instagram_saved_count")) if inventory is not None else None
    return {
        "version": 1,
        **{key: count(observations[key].get("value")) for key in SOURCE_FIELDS},
        **{key: count(inventory.get(key)) if inventory is not None else None for key in INVENTORY_FIELDS},
        "post_minus_saved_difference": reported - saved if reported is not None and saved is not None else None,
        "inventory_status": "available" if inventory is not None else "unavailable",
        "inventory_observed_at": inventory.get("inventory_observed_at") if inventory is not None else None,
        "observations": observations,
        "observation_conflicts": _conflicts(raw),
    }


def reported_count_sql(raw_sql: str) -> str:
    """Only post-details observations qualify; historical catalog totals do not."""
    observed = f"({raw_sql} #> '{{{OBSERVATION_KEY},post_reported_count}}')"
    valid = (
        f"({observed} ->> 'status') = 'observed' and ({observed} ->> 'source') = 'instagram_response' "
        f"and jsonb_typeof({observed} -> 'value') = 'number' "
        f"and ({observed} ->> 'source_surface') = 'instagram_post_details' "
        f"and ({observed} ->> 'source_field') in "
        "('comment_count','commentsCount','edge_media_to_comment.count') "
        f"and ({observed} ->> 'evidence_reference') = 'retained_payload:' || ({observed} ->> 'source_field')"
    )
    value = f"({observed} ->> 'value')"
    return (
        f"case when {valid} and {value} ~ '^[0-9]{{1,19}}$' "
        f"then case when {value}::numeric <= 9223372036854775807 then {value}::bigint end end"
    )


def detail_observation_metadata(node: Mapping[str, Any], *, observed_at: Any) -> dict[str, Any]:
    """Build provenance only at a proven individual post-details boundary."""
    observations = observations_from_raw(node, observed_at=observed_at, source_surface="instagram_post_details")
    conflicts = []
    chosen = observations["post_reported_count"]["value"]
    for field, value in (
        ("comment_count", node.get("comment_count")),
        ("commentsCount", node.get("commentsCount")),
        ("edge_media_to_comment.count", _mapping(node.get("edge_media_to_comment")).get("count")),
    ):
        parsed = count(value)
        if chosen is not None and parsed is not None and parsed != chosen:
            conflicts.append(
                {"metric": "post_reported_count", **_observation(parsed, field, node, observed_at=observed_at)}
            )
    return {OBSERVATION_KEY: observations, "comment_count_observation_conflicts": conflicts}


def capture_detail_observations(post: Any, node: Mapping[str, Any], *, observed_at: Any) -> None:
    """Called only after fetching and extracting an individual post-details response."""
    metadata = detail_observation_metadata(node, observed_at=observed_at)
    post.comment_count_observations = metadata[OBSERVATION_KEY]
    post.comment_count_observation_conflicts = metadata["comment_count_observation_conflicts"]


def saved_count_sql(source_id_sql: str) -> str:
    # UNIQUE(shortcode) and UNIQUE(post_id, comment_id) preserve row identity.
    # Null IDs are separate canonical rows; no JSON materialization is needed.
    return f"""(select count(*)
        from social.instagram_posts cp join social.instagram_comments c on c.post_id = cp.id
        where cp.shortcode = {source_id_sql} and c.is_missing is not true
          and coalesce(c.source_snapshot_type, '') <> 'fb_crosspost')"""


def _query(sql: str, params: list[Any], *, conn: Any | None = None) -> list[dict[str, Any]]:
    if conn is None:
        return pg.fetch_all(sql, params)
    with pg.db_cursor(conn=conn, label="instagram-comment-counts") as cur:
        return pg.fetch_all_with_cursor(cur, sql, params)


def load_inventory(source_ids: Sequence[str], *, conn: Any | None = None) -> dict[str, dict[str, Any]] | None:
    """Count unique current records; Facebook rows are never Instagram inventory.

    Catalog and materialized identities resolve through shortcode. Missing rows
    stay a separate inventory count and are not included in saved active rows.
    """
    keys = list(dict.fromkeys(str(value) for value in source_ids if value))
    if not keys:
        return {}
    try:
        rows = _query(
            """
            select p.shortcode,
              count(*) filter (where c.is_missing is not true)::int as instagram_saved_count,
              count(*) filter (
                where c.is_missing is not true and not (
                  coalesce(c.is_reply, false) or c.parent_comment_id is not null
                  or nullif(c.parent_comment_external_id, '') is not null or coalesce(c.reply_depth, 0) > 0
                ) and c.is_reply is not null
              )::int as instagram_saved_top_level_count,
              count(*) filter (
                where c.is_missing is not true and (
                  coalesce(c.is_reply, false) or c.parent_comment_id is not null
                  or nullif(c.parent_comment_external_id, '') is not null or coalesce(c.reply_depth, 0) > 0
                )
              )::int as instagram_saved_reply_count,
              count(*) filter (
                where c.is_missing is not true and not (
                  coalesce(c.is_reply, false) or c.parent_comment_id is not null
                  or nullif(c.parent_comment_external_id, '') is not null or coalesce(c.reply_depth, 0) > 0
                ) and c.is_reply is null
              )::int as instagram_saved_unclassified_count,
              count(*) filter (where c.is_missing is true)::int as instagram_marked_missing_count
            from social.instagram_posts p
            join social.instagram_comments c on c.post_id = p.id
            where p.shortcode = any(%s)
              and coalesce(c.source_snapshot_type, '') <> 'fb_crosspost'
            group by p.shortcode
        """,
            [keys],
            conn=conn,
        )
        observed_at = datetime.now(UTC).isoformat()
        result = {key: dict.fromkeys(INVENTORY_FIELDS, 0) | {"inventory_observed_at": observed_at} for key in keys}
        result.update({str(row["shortcode"]): {**row, "inventory_observed_at": observed_at} for row in rows})
        return result
    except Exception as error:
        # No successful query means no count, including no invented zero.
        logger.warning(
            "Instagram canonical comment inventory unavailable error_class=%s sqlstate=%s",
            type(error).__name__,
            getattr(error, "pgcode", None),
        )
        return None


def load_post_rows(source_ids: Sequence[str], *, conn: Any | None = None) -> dict[str, dict[str, Any]]:
    keys = list(dict.fromkeys(str(value) for value in source_ids if value))
    if not keys:
        return {}
    rows = _query(
        """
        select distinct on (source_id) source_id, raw_data from (
          select p.shortcode as source_id, coalesce(s.raw_data, p.raw_data, '{}'::jsonb) as raw_data, 1 as priority
          from social.instagram_posts p
          left join social.instagram_post_payloads s on s.post_id = p.id
          where p.shortcode = any(%s)
          union all
          select p.source_id, coalesce(s.raw_data, p.raw_data, '{}'::jsonb), 2
          from social.instagram_account_catalog_posts p
          left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
          where p.source_id = any(%s)
        ) candidates order by source_id, priority
    """,
        [keys, keys],
        conn=conn,
    )
    return {str(row["source_id"]): row for row in rows}


def _source_count_projection(raw: str) -> str:
    """Extract evidence in one JSON pass; repeated operators re-read large TOAST values.

    PostgreSQL keyvalue() preserves the original keys and their scalar/object
    values. Media and embedded comment arrays are excluded from the result.
    https://www.postgresql.org/docs/current/functions-json.html
    """
    return f"""jsonb_path_query_array({raw}, '$.keyvalue() ? (
        @.key == "{OBSERVATION_KEY}" || @.key == "comment_count_observation_conflicts"
        || @.key == "fb_comment_count" || @.key == "facebook_crosspost"
    )')"""


def _projected_raw(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, list):
        return {}
    allowed = {OBSERVATION_KEY, "comment_count_observation_conflicts", "fb_comment_count", "facebook_crosspost"}
    return {
        item["key"]: item.get("value")
        for item in value
        if isinstance(item, Mapping) and isinstance(item.get("key"), str) and item["key"] in allowed
    }


def counts_for_posts(rows: Sequence[Mapping[str, Any]], *, conn: Any | None = None) -> dict[str, dict[str, Any]]:
    keys = list(dict.fromkeys(str(row.get("source_id") or row.get("shortcode") or "") for row in rows))
    inventory = load_inventory(keys, conn=conn)
    return {
        str(row.get("source_id") or row.get("shortcode") or ""): build_comment_counts(
            row,
            inventory.get(str(row.get("source_id") or row.get("shortcode") or "")) if inventory is not None else None,
        )
        for row in rows
    }


def _profile_account_match_sql() -> str:
    # Reuse the normal profile membership contract, including owners and raw
    # collaborator identities that predate the materialized membership rows.
    from trr_backend.socials.social_season_analytics_impl import _instagram_account_match_sql

    return _instagram_account_match_sql(alias="p")


def profile_post_keys(
    account: str,
    *,
    page: int,
    page_size: int,
    comment_filter: str | None,
    sort_by: str,
    sort_dir: str,
    search_where_sql: str = "",
    search_params: Sequence[Any] = (),
    conn: Any | None = None,
) -> tuple[list[str], int]:
    """Select bounded identities before fetching any full post payloads.

    Source-only filters/sorts do not read comments. A signed difference reads
    inventory only for posts with a known reported observation; unknown values
    remain null and sort last without scanning their comments.
    """
    observed_sql = reported_count_sql("d.raw_data")
    source_filter = {
        "reported_positive": "reported_count > 0",
        "reported_zero": "reported_count = 0",
        "reported_unknown": "reported_count is null",
        "count_difference": "reported_count is not null",
    }.get(comment_filter or "", "true")
    needs_saved = sort_by in {"instagram_saved_count", "count_difference", "saved_comments", "missing_comments"}
    needs_saved = needs_saved or comment_filter == "count_difference"
    all_saved = sort_by in {"instagram_saved_count", "saved_comments", "missing_comments"}
    saved_sql = saved_count_sql("e.source_id") if needs_saved else "null::bigint"
    if needs_saved and not all_saved:
        saved_sql = f"case when e.reported_count is not null then {saved_sql} end"
    sort_fields = {
        "post": "lower(source_id)",
        "created": "posted_at",
        "caption": "lower(coalesce(caption, title, ''))",
        "likes": "likes",
        "post_total": "legacy_reported",
        "post_reported_count": "reported_count",
        "instagram_saved_count": "saved_count",
        "saved_comments": "saved_count",
        "count_difference": "reported_count - saved_count",
        "missing_comments": "greatest(legacy_reported - saved_count, 0)",
    }
    if sort_by not in sort_fields or sort_dir not in {"asc", "desc"}:
        raise ValueError("Unsupported Instagram count page sort")
    order = f"{sort_fields[sort_by]} {sort_dir} nulls last, posted_at desc nulls last, source_id"
    final_filter = "reported_count <> saved_count" if comment_filter == "count_difference" else "true"
    rows = _query(
        f"""
        with candidates as materialized (
          select p.shortcode as source_id, p.shortcode, p.posted_at, p.caption,
            null::text as title, null::text as description, null::text as text, null::text as show_name,
            null::text as permalink, p.hashtags, p.mentions, p.collaborators, p.profile_tags,
            coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as legacy_reported,
            jsonb_build_object('{OBSERVATION_KEY}',
              coalesce(s.raw_data, p.raw_data) -> '{OBSERVATION_KEY}') as raw_data, 1 as priority
          from social.instagram_posts p
          left join social.instagram_post_payloads s on s.post_id = p.id
          where {_profile_account_match_sql()}
          union all
          select p.source_id, p.source_id, p.posted_at, p.caption, p.title, p.description, p.text,
            null::text, p.permalink, p.hashtags, p.mentions, p.collaborators, p.profile_tags,
            coalesce(p.likes, 0), coalesce(p.comments_count, 0),
            jsonb_build_object('{OBSERVATION_KEY}',
              coalesce(s.raw_data, p.raw_data) -> '{OBSERVATION_KEY}'), 2
          from social.instagram_account_catalog_posts p
          left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
          where {_profile_account_match_sql()}
          union all
          select p.source_id, p.source_id, p.posted_at, p.caption, p.title, p.description, p.text,
            null::text, p.permalink, p.hashtags, p.mentions, p.collaborators, p.profile_tags,
            coalesce(p.likes, 0), coalesce(p.comments_count, 0),
            jsonb_build_object('{OBSERVATION_KEY}',
              coalesce(s.raw_data, p.raw_data) -> '{OBSERVATION_KEY}'), 3
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p on p.id = m.catalog_post_id
          left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
          where m.collaborator_handle = %s and lower(p.source_account) <> %s
        ), deduped as materialized (
          select distinct on (source_id) * from candidates
          where nullif(source_id, '') is not null order by source_id, priority
        ), observed as materialized (
          select d.*, {observed_sql} as reported_count from deduped d
          where true {search_where_sql}
        ), eligible as materialized (
          select * from observed where {source_filter}
        ), scored as materialized (
          select e.*, {saved_sql} as saved_count from eligible e
        ), final_rows as materialized (
          select * from scored where {final_filter}
        ), page_rows as materialized (
          select source_id, row_number() over (order by {order}) as position
          from final_rows order by {order} limit %s offset %s
        )
        select source_id, position, (select count(*) from final_rows) as total from page_rows
        union all
        select null, null, (select count(*) from final_rows) where not exists (select 1 from page_rows)
        order by position nulls last
    """,
        [account] * 4 + list(search_params) + [page_size, (page - 1) * page_size],
        conn=conn,
    )
    return [str(row["source_id"]) for row in rows if row.get("source_id")], int(rows[0]["total"]) if rows else 0


def profile_summary(account: str, *, conn: Any | None = None) -> dict[str, Any]:
    """Deduplicate owner/catalog/collaborator rows before summing observations."""
    rows = _query(
        f"""
        select distinct on (source_id) source_id, raw_data from (
          select p.shortcode as source_id,
            {_source_count_projection("coalesce(s.raw_data, p.raw_data)")} as raw_data, 1 as priority
          from social.instagram_posts p
          left join social.instagram_post_payloads s on s.post_id = p.id
          where {_profile_account_match_sql()}
          union all
          select p.source_id, {_source_count_projection("coalesce(s.raw_data, p.raw_data)")}, 2
          from social.instagram_account_catalog_posts p
          left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
          where {_profile_account_match_sql()}
          union all
          select p.source_id, {_source_count_projection("coalesce(s.raw_data, p.raw_data)")}, 3
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts p on p.id = m.catalog_post_id
          left join social.instagram_account_catalog_post_payloads s on s.catalog_post_id = p.id
          where m.collaborator_handle = %s and lower(p.source_account) <> %s
        ) candidates where nullif(source_id, '') is not null order by source_id, priority
        """,
        [account] * 4,
        conn=conn,
    )
    rows = [{**row, "raw_data": _projected_raw(row.get("raw_data"))} for row in rows]
    return summarize_comment_counts(counts_for_posts(rows, conn=conn))


def summarize_comment_counts(contracts: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(contracts.values())
    available = all(row.get("inventory_status") == "available" for row in rows)
    result: dict[str, Any] = {
        "version": 1,
        "posts_count": len(rows),
        "inventory_status": "available" if available else "unavailable",
        "inventory_observed_at": next((row.get("inventory_observed_at") for row in rows), None) if available else None,
    }
    for prefix, key in zip(("post_reported", "facebook", "threads"), SOURCE_FIELDS, strict=True):
        known = [row[key] for row in rows if row.get(key) is not None]
        result.update(
            {
                f"{prefix}_known_subtotal": sum(known),
                f"{prefix}_known_posts_count": len(known),
                f"{prefix}_unknown_posts_count": len(rows) - len(known),
            }
        )
    result["posts_with_reported_comments_count"] = sum(
        row["post_reported_count"] is not None and row["post_reported_count"] > 0 for row in rows
    )
    result["posts_reporting_zero_comments_count"] = sum(row["post_reported_count"] == 0 for row in rows)
    result["reported_count_unknown_posts_count"] = result["post_reported_unknown_posts_count"]
    result["posts_with_reported_comments_but_no_saved_records_count"] = (
        sum(
            row["post_reported_count"] is not None
            and row["post_reported_count"] > 0
            and row["instagram_saved_count"] == 0
            for row in rows
        )
        if available
        else None
    )
    result.update({key: sum(row[key] for row in rows) if available else None for key in INVENTORY_FIELDS})
    differences = [
        row["post_minus_saved_difference"] for row in rows if row.get("post_minus_saved_difference") is not None
    ]
    result["post_minus_saved_difference_known_subtotal"] = sum(differences) if available else None
    return result


def contract_smoke() -> dict[str, Any]:
    """Pure deployed-module smoke: no database, network, collection or writes."""
    inventory = dict.fromkeys(INVENTORY_FIELDS, 0) | {
        "instagram_saved_count": 3,
        "inventory_observed_at": "2026-01-01T00:00:00Z",
    }
    unknown = build_comment_counts({})
    zero = build_comment_counts(
        {
            "raw_data": {
                OBSERVATION_KEY: observations_from_raw({"comment_count": 0}, source_surface="instagram_post_details")
            }
        },
        inventory,
    )
    InstagramCommentCounts.model_validate(unknown)
    InstagramCommentCounts.model_validate(zero)
    assert unknown["post_reported_count"] is None and unknown["instagram_saved_count"] is None
    assert zero["post_reported_count"] == 0 and zero["post_minus_saved_difference"] == -3
    return {"version": 1, "unknown": "passed", "zero": "passed", "signed_difference": "passed"}

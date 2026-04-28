#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


SUPPORTED_PLATFORMS = ("instagram", "tiktok", "twitter", "facebook", "threads", "youtube", "reddit")
API_ROLES = ("public", "anon", "authenticated")


@dataclass(frozen=True)
class CommentConfig:
    table: str
    fk_column: str
    target_table: str
    target_column: str


@dataclass(frozen=True)
class PlatformConfig:
    platform: str
    materialized_table: str
    materialized_source_expr: str
    materialized_account_expr: str | None
    catalog_table: str | None
    catalog_source_expr: str | None
    catalog_account_expr: str | None
    community_expr: str | None = None
    comment: CommentConfig | None = None


PLATFORM_CONFIGS: dict[str, PlatformConfig] = {
    "instagram": PlatformConfig(
        platform="instagram",
        materialized_table="instagram_posts",
        materialized_source_expr="t.shortcode",
        materialized_account_expr="coalesce(nullif(t.source_account, ''), nullif(t.username, ''))",
        catalog_table="instagram_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
        comment=CommentConfig("instagram_comments", "post_id", "instagram_posts", "id"),
    ),
    "tiktok": PlatformConfig(
        platform="tiktok",
        materialized_table="tiktok_posts",
        materialized_source_expr="t.video_id",
        materialized_account_expr="coalesce(nullif(t.source_account, ''), nullif(t.username, ''))",
        catalog_table="tiktok_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
        comment=CommentConfig("tiktok_comments", "post_id", "tiktok_posts", "id"),
    ),
    "twitter": PlatformConfig(
        platform="twitter",
        materialized_table="twitter_tweets",
        materialized_source_expr="t.tweet_id",
        materialized_account_expr="coalesce(nullif(t.source_account, ''), nullif(t.username, ''))",
        catalog_table="twitter_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
    ),
    "facebook": PlatformConfig(
        platform="facebook",
        materialized_table="facebook_posts",
        materialized_source_expr="t.post_id",
        materialized_account_expr="coalesce(nullif(t.source_account, ''), nullif(t.username, ''))",
        catalog_table="facebook_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
        comment=CommentConfig("facebook_comments", "post_id", "facebook_posts", "id"),
    ),
    "threads": PlatformConfig(
        platform="threads",
        materialized_table="meta_threads_posts",
        materialized_source_expr="t.post_id",
        materialized_account_expr="coalesce(nullif(t.source_account, ''), nullif(t.username, ''))",
        catalog_table="threads_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
        comment=CommentConfig("meta_threads_comments", "post_id", "meta_threads_posts", "id"),
    ),
    "youtube": PlatformConfig(
        platform="youtube",
        materialized_table="youtube_videos",
        materialized_source_expr="t.video_id",
        materialized_account_expr=(
            "coalesce(nullif(t.source_account, ''), nullif(t.channel_id, ''), nullif(t.channel_title, ''))"
        ),
        catalog_table="youtube_account_catalog_posts",
        catalog_source_expr="t.source_id",
        catalog_account_expr="t.source_account",
        comment=CommentConfig("youtube_comments", "video_id", "youtube_videos", "id"),
    ),
    "reddit": PlatformConfig(
        platform="reddit",
        materialized_table="reddit_posts",
        materialized_source_expr="t.reddit_post_id",
        materialized_account_expr=None,
        catalog_table=None,
        catalog_source_expr=None,
        catalog_account_expr=None,
        community_expr="t.subreddit",
        comment=CommentConfig("reddit_comments", "reddit_post_id", "reddit_posts", "reddit_post_id"),
    ),
}


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def _fetch_scalar(sql: str, params: list[Any] | None = None) -> int:
    row = pg.fetch_one(sql, params or [])
    if not row:
        return 0
    return int(next(iter(row.values())) or 0)


def _table_exists(table: str) -> bool:
    row = pg.fetch_one("select to_regclass(%s) is not null as exists", [f"social.{table}"])
    return bool(row and row.get("exists"))


def _filtered_where(
    config: PlatformConfig,
    *,
    account: str | None,
    community: str | None,
    account_expr: str | None,
    community_expr: str | None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if account and account_expr:
        clauses.append(f"ltrim(lower({account_expr}), '@') = ltrim(lower(%s), '@')")
        params.append(account)
    if community and community_expr:
        clauses.append(f"ltrim(lower({community_expr}), 'r/') = ltrim(lower(%s), 'r/')")
        params.append(community)
    if account and not account_expr and config.platform == "reddit":
        clauses.append("false")
    if community and not community_expr and config.platform != "reddit":
        clauses.append("false")
    return (" where " + " and ".join(clauses), params) if clauses else ("", params)


def _row_count(
    table: str,
    config: PlatformConfig,
    *,
    account: str | None,
    community: str | None,
    account_expr: str | None,
    community_expr: str | None,
) -> int | None:
    if not _table_exists(table):
        return None
    where_sql, params = _filtered_where(
        config,
        account=account,
        community=community,
        account_expr=account_expr,
        community_expr=community_expr,
    )
    return _fetch_scalar(f"select count(*)::int from social.{table} t{where_sql}", params)


def _duplicate_source_count(
    table: str,
    source_expr: str,
    config: PlatformConfig,
    *,
    account: str | None,
    community: str | None,
    account_expr: str | None,
    community_expr: str | None,
) -> int | None:
    if not _table_exists(table):
        return None
    where_sql, params = _filtered_where(
        config,
        account=account,
        community=community,
        account_expr=account_expr,
        community_expr=community_expr,
    )
    if where_sql:
        where_sql += f" and nullif(btrim(({source_expr})::text), '') is not null"
    else:
        where_sql = f" where nullif(btrim(({source_expr})::text), '') is not null"
    return _fetch_scalar(
        f"""
        select count(*)::int
        from (
          select ({source_expr})::text as source_id
          from social.{table} t
          {where_sql}
          group by 1
          having count(*) > 1
        ) duplicates
        """,
        params,
    )


def _legacy_without_shared(
    table: str,
    source_expr: str,
    config: PlatformConfig,
    *,
    account: str | None,
    community: str | None,
    account_expr: str | None,
    community_expr: str | None,
) -> int | None:
    if not (_table_exists(table) and _table_exists("social_post_legacy_refs")):
        return None
    where_sql, params = _filtered_where(
        config,
        account=account,
        community=community,
        account_expr=account_expr,
        community_expr=community_expr,
    )
    source_clause = f"nullif(btrim(({source_expr})::text), '') is not null"
    if where_sql:
        where_sql += f" and {source_clause}"
    else:
        where_sql = f" where {source_clause}"
    params = [*params, config.platform, table]
    return _fetch_scalar(
        f"""
        select count(*)::int
        from social.{table} t
        {where_sql}
          and not exists (
            select 1
            from social.social_post_legacy_refs refs
            where refs.platform = %s
              and refs.legacy_schema = 'social'
              and refs.legacy_table = %s
              and refs.legacy_source_id = ({source_expr})::text
          )
        """,
        params,
    )


def _shared_counts(config: PlatformConfig, *, account: str | None, community: str | None) -> dict[str, int | None]:
    if not _table_exists("social_posts"):
        return {"shared_rows": None, "shared_without_legacy": None}
    params: list[Any] = [config.platform]
    joins = ""
    clauses = ["p.platform = %s"]
    if account:
        joins += """
        join social.social_post_memberships account_membership
          on account_membership.platform = p.platform
         and account_membership.post_id = p.id
         and account_membership.membership_type in ('account', 'channel')
        """
        clauses.append("ltrim(lower(account_membership.membership_key_norm), '@') = ltrim(lower(%s), '@')")
        params.append(account)
    if community:
        joins += """
        join social.social_post_memberships community_membership
          on community_membership.platform = p.platform
         and community_membership.post_id = p.id
         and community_membership.membership_type = 'community'
        """
        clauses.append("ltrim(lower(community_membership.membership_key_norm), 'r/') = ltrim(lower(%s), 'r/')")
        params.append(community)
    where_sql = " and ".join(clauses)
    shared_rows = _fetch_scalar(
        f"select count(*)::int from social.social_posts p {joins} where {where_sql}",
        params,
    )
    if not _table_exists("social_post_legacy_refs"):
        return {"shared_rows": shared_rows, "shared_without_legacy": None}
    shared_without_legacy = _fetch_scalar(
        f"""
        select count(*)::int
        from social.social_posts p
        {joins}
        where {where_sql}
          and not exists (
            select 1
            from social.social_post_legacy_refs refs
            where refs.platform = p.platform
              and refs.post_id = p.id
          )
        """,
        params,
    )
    return {"shared_rows": shared_rows, "shared_without_legacy": shared_without_legacy}


def _missing_comment_targets(comment: CommentConfig | None) -> int | None:
    if comment is None:
        return None
    if not (_table_exists(comment.table) and _table_exists(comment.target_table)):
        return None
    return _fetch_scalar(
        f"""
        select count(*)::int
        from social.{comment.table} c
        left join social.{comment.target_table} target
          on target.{comment.target_column} = c.{comment.fk_column}
        where c.{comment.fk_column} is not null
          and target.{comment.target_column} is null
        """
    )


def _shared_schema_available() -> bool:
    return all(
        _table_exists(table)
        for table in (
            "social_posts",
            "social_post_observations",
            "social_post_legacy_refs",
            "social_post_memberships",
            "social_post_entities",
            "social_post_media_assets",
        )
    )


def _observation_exposure() -> dict[str, Any]:
    if not _table_exists("social_post_observations"):
        return {
            "table_exists": False,
            "public_grants": [],
            "public_policies": [],
            "is_publicly_exposed": False,
        }
    grants = pg.fetch_all(
        """
        select grantee, privilege_type
        from information_schema.role_table_grants
        where table_schema = 'social'
          and table_name = 'social_post_observations'
          and grantee = any(%s)
        order by grantee, privilege_type
        """,
        [list(API_ROLES)],
    )
    policies = pg.fetch_all(
        """
        select policyname, roles, cmd
        from pg_policies
        where schemaname = 'social'
          and tablename = 'social_post_observations'
          and roles && %s::name[]
        order by policyname
        """,
        [list(API_ROLES)],
    )
    return {
        "table_exists": True,
        "public_grants": [_json_safe(row) for row in grants],
        "public_policies": [_json_safe(row) for row in policies],
        "is_publicly_exposed": bool(grants or policies),
    }


def _platform_report(config: PlatformConfig, *, account: str | None, community: str | None) -> dict[str, Any]:
    materialized_rows = _row_count(
        config.materialized_table,
        config,
        account=account,
        community=community,
        account_expr=config.materialized_account_expr,
        community_expr=config.community_expr,
    )
    catalog_rows = (
        _row_count(
            config.catalog_table,
            config,
            account=account,
            community=community,
            account_expr=config.catalog_account_expr,
            community_expr=None,
        )
        if config.catalog_table
        else None
    )
    report = {
        "platform": config.platform,
        "legacy": {
            "materialized_table": config.materialized_table,
            "materialized_rows": materialized_rows,
            "materialized_duplicate_source_ids": _duplicate_source_count(
                config.materialized_table,
                config.materialized_source_expr,
                config,
                account=account,
                community=community,
                account_expr=config.materialized_account_expr,
                community_expr=config.community_expr,
            ),
            "materialized_without_shared_legacy_ref": _legacy_without_shared(
                config.materialized_table,
                config.materialized_source_expr,
                config,
                account=account,
                community=community,
                account_expr=config.materialized_account_expr,
                community_expr=config.community_expr,
            ),
            "catalog_table": config.catalog_table,
            "catalog_rows": catalog_rows,
            "catalog_duplicate_source_ids": (
                _duplicate_source_count(
                    config.catalog_table,
                    config.catalog_source_expr or "t.source_id",
                    config,
                    account=account,
                    community=community,
                    account_expr=config.catalog_account_expr,
                    community_expr=None,
                )
                if config.catalog_table
                else None
            ),
            "catalog_without_shared_legacy_ref": (
                _legacy_without_shared(
                    config.catalog_table,
                    config.catalog_source_expr or "t.source_id",
                    config,
                    account=account,
                    community=community,
                    account_expr=config.catalog_account_expr,
                    community_expr=None,
                )
                if config.catalog_table
                else None
            ),
            "comment_table": config.comment.table if config.comment else None,
            "comments_rows": _row_count(
                config.comment.table,
                config,
                account=None,
                community=None,
                account_expr=None,
                community_expr=None,
            )
            if config.comment
            else None,
            "comments_missing_legacy_target": _missing_comment_targets(config.comment),
        },
        "shared": _shared_counts(config, account=account, community=community),
    }
    return report


def build_report(*, platform: str, account: str | None, community: str | None) -> dict[str, Any]:
    selected = SUPPORTED_PLATFORMS if platform == "all" else (platform,)
    return {
        "filters": {
            "platform": platform,
            "account": account,
            "community": community,
        },
        "shared_schema_available": _shared_schema_available(),
        "observation_exposure": _observation_exposure(),
        "platforms": [
            _platform_report(PLATFORM_CONFIGS[item], account=account, community=community) for item in selected
        ],
    }


def _render_text(report: dict[str, Any]) -> str:
    lines = [
        "Social post schema parity",
        f"shared_schema_available: {report['shared_schema_available']}",
        f"observation_public_exposure: {report['observation_exposure']['is_publicly_exposed']}",
        "",
    ]
    for platform in report["platforms"]:
        legacy = platform["legacy"]
        shared = platform["shared"]
        materialized_line = (
            f"  materialized: {legacy['materialized_table']} rows={legacy['materialized_rows']} "
            f"duplicate_sources={legacy['materialized_duplicate_source_ids']} "
            f"missing_shared_refs={legacy['materialized_without_shared_legacy_ref']}"
        )
        catalog_line = (
            f"  catalog: {legacy['catalog_table']} rows={legacy['catalog_rows']} "
            f"duplicate_sources={legacy['catalog_duplicate_source_ids']} "
            f"missing_shared_refs={legacy['catalog_without_shared_legacy_ref']}"
        )
        comments_line = (
            f"  comments: {legacy['comment_table']} rows={legacy['comments_rows']} "
            f"missing_targets={legacy['comments_missing_legacy_target']}"
        )
        lines.extend(
            [
                platform["platform"],
                materialized_line,
                catalog_line,
                comments_line,
                f"  shared: rows={shared['shared_rows']} without_legacy={shared['shared_without_legacy']}",
                "",
            ]
        )
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only parity checks for shared social post schema rollout.")
    parser.add_argument("--platform", choices=("all", *SUPPORTED_PLATFORMS), default="all")
    parser.add_argument("--account")
    parser.add_argument("--community")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    report = build_report(platform=args.platform, account=args.account, community=args.community)
    if args.json:
        print(json.dumps(_json_safe(report), indent=2, sort_keys=True))
    else:
        print(_render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Preview or upsert Bravo show and cast hashtag aliases."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env  # noqa: E402
from trr_backend.db import pg  # noqa: E402
from trr_backend.socials import social_season_analytics_impl as social_repo  # noqa: E402

DEFAULT_ALIAS_SOURCE = "bravo_hashtag"
ENTITY_CHOICES = ("all", "shows", "cast")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_bravo_show_hashtag_aliases",
        description=(
            "Generate exact compact Bravo hashtag aliases such as RHOBH, RHOSLC, "
            "and SouthernCharm and optionally upsert them into core.show_alternative_names."
        ),
    )
    parser.add_argument("--network", default="Bravo", help="Show network filter, default: Bravo")
    parser.add_argument(
        "--entity",
        choices=ENTITY_CHOICES,
        default="all",
        help="Alias entity set to sync: all, shows, or cast. Default: all.",
    )
    parser.add_argument("--show-id", action="append", default=[], help="Limit to a show UUID. Repeatable.")
    parser.add_argument("--source", default=DEFAULT_ALIAS_SOURCE, help=f"Alias source label, default: {DEFAULT_ALIAS_SOURCE}")
    parser.add_argument("--apply", action="store_true", help="Upsert aliases. Without this flag the command only previews.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON result.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _compact_alias(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lstrip("#@").lower())


def _display_alias_from_words(value: Any) -> str:
    words = re.findall(r"[A-Za-z0-9]+", str(value or "").strip())
    return "".join(words)


def _alias_display(value: Any) -> str:
    raw = str(value or "").strip().lstrip("#@")
    if not raw:
        return ""
    if re.fullmatch(r"[A-Z0-9]{2,12}", raw):
        return raw
    compact = _compact_alias(raw)
    if compact.startswith("rho") and 4 <= len(compact) <= 8:
        return compact.upper()
    return _display_alias_from_words(raw)


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def _show_alias_candidates(row: Mapping[str, Any]) -> list[str]:
    show_name = str(row.get("name") or row.get("show_name") or "").strip()
    slug = str(row.get("slug") or row.get("show_slug") or "").strip()
    derived_hashtags, _derived_keywords = social_repo._derive_show_terms(show_name)  # noqa: SLF001
    raw_candidates = [
        show_name,
        slug,
        *derived_hashtags,
        *_json_list(row.get("alternative_names")),
    ]

    aliases: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        display = _alias_display(candidate)
        key = _compact_alias(display)
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        aliases.append(display)
    return aliases


def _cast_alias_candidates(row: Mapping[str, Any]) -> list[str]:
    raw_candidates = [
        row.get("full_name"),
        row.get("cast_member_name"),
        *_json_list(row.get("alternative_names")),
    ]

    aliases: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        display = _alias_display(candidate)
        key = _compact_alias(display)
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        aliases.append(display)
    return aliases


def _alias_rows_for_shows(show_rows: Sequence[Mapping[str, Any]], *, source: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for show in show_rows:
        show_id = str(show.get("id") or show.get("show_id") or "").strip()
        show_name = str(show.get("name") or show.get("show_name") or "").strip()
        if not show_id:
            continue
        for alias in _show_alias_candidates(show):
            rows.append(
                {
                    "show_id": show_id,
                    "show_name": show_name,
                    "name": alias,
                    "source": source,
                }
            )
    return rows


def _alias_rows_for_cast(cast_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for cast_member in cast_rows:
        person_id = str(cast_member.get("person_id") or "").strip()
        person_name = str(
            cast_member.get("full_name") or cast_member.get("cast_member_name") or cast_member.get("person_id") or ""
        ).strip()
        if not person_id:
            continue
        for alias in _cast_alias_candidates(cast_member):
            key = (person_id, _compact_alias(alias))
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "person_id": person_id,
                    "person_name": person_name,
                    "name": alias,
                }
            )
    return rows


def _fetch_show_rows(*, network: str, show_ids: Sequence[str]) -> list[dict[str, Any]]:
    normalized_show_ids = [str(show_id).strip() for show_id in show_ids if str(show_id).strip()]
    network_filter = str(network or "").strip()
    return pg.fetch_all(
        """
        select
          sh.id::text as id,
          sh.name,
          sh.slug,
          coalesce(to_jsonb(sh) -> 'alternative_names', '[]'::jsonb) as alternative_names,
          coalesce(to_jsonb(sh) -> 'networks', '[]'::jsonb) as networks
        from core.shows sh
        where (%s::uuid[] is null or sh.id = any(%s::uuid[]))
          and (
            nullif(%s, '') is null
            or exists (
              select 1
              from jsonb_array_elements_text(
                case
                  when jsonb_typeof(coalesce(to_jsonb(sh) -> 'networks', '[]'::jsonb)) = 'array'
                    then coalesce(to_jsonb(sh) -> 'networks', '[]'::jsonb)
                  when nullif(to_jsonb(sh) ->> 'networks', '') is not null
                    then jsonb_build_array(to_jsonb(sh) ->> 'networks')
                  else '[]'::jsonb
                end
              ) as n(name)
              where lower(n.name) = lower(%s)
            )
          )
        order by sh.name asc, sh.id asc
        """,
        [normalized_show_ids or None, normalized_show_ids or None, network_filter, network_filter],
    )


def _fetch_cast_rows(*, network: str, show_ids: Sequence[str]) -> list[dict[str, Any]]:
    normalized_show_ids = [str(show_id).strip() for show_id in show_ids if str(show_id).strip()]
    network_filter = str(network or "").strip()
    return pg.fetch_all(
        """
        with filtered_shows as (
          select sh.id
          from core.shows sh
          where (%s::uuid[] is null or sh.id = any(%s::uuid[]))
            and (
              nullif(%s, '') is null
              or exists (
                select 1
                from jsonb_array_elements_text(
                  case
                    when jsonb_typeof(coalesce(to_jsonb(sh) -> 'networks', '[]'::jsonb)) = 'array'
                      then coalesce(to_jsonb(sh) -> 'networks', '[]'::jsonb)
                    when nullif(to_jsonb(sh) ->> 'networks', '') is not null
                      then jsonb_build_array(to_jsonb(sh) ->> 'networks')
                    else '[]'::jsonb
                  end
                ) as n(name)
                where lower(n.name) = lower(%s)
              )
            )
        ),
        cast_people as (
          select distinct
            p.id::text as person_id,
            p.full_name,
            sc.cast_member_name,
            coalesce(to_jsonb(p) -> 'alternative_names', '[]'::jsonb) as alternative_names
          from core.show_cast sc
          join filtered_shows sh on sh.id = sc.show_id
          join core.people p on p.id = sc.person_id
          where sc.person_id is not null
            and nullif(coalesce(p.full_name, sc.cast_member_name), '') is not null
          union
          select distinct
            p.id::text as person_id,
            p.full_name,
            null::text as cast_member_name,
            coalesce(to_jsonb(p) -> 'alternative_names', '[]'::jsonb) as alternative_names
          from core.show_cast_role_assignments sra
          join filtered_shows sh on sh.id = sra.show_id
          join core.people p on p.id = sra.person_id
          where sra.person_id is not null
            and nullif(p.full_name, '') is not null
        )
        select person_id, full_name, cast_member_name, alternative_names
        from cast_people
        order by full_name asc, cast_member_name asc, person_id asc
        """,
        [normalized_show_ids or None, normalized_show_ids or None, network_filter, network_filter],
    )


def _existing_alias_keys(rows: Sequence[Mapping[str, str]], *, source: str) -> set[tuple[str, str]]:
    show_ids = sorted({str(row.get("show_id") or "").strip() for row in rows if row.get("show_id")})
    if not show_ids:
        return set()
    existing = pg.fetch_all(
        """
        select show_id::text as show_id, name
        from core.show_alternative_names
        where show_id = any(%s::uuid[])
          and source = %s
        """,
        [show_ids, source],
    )
    return {
        (str(row.get("show_id") or "").strip(), _compact_alias(row.get("name")))
        for row in existing
        if row.get("show_id") and row.get("name")
    }


def _existing_person_alias_keys(rows: Sequence[Mapping[str, str]]) -> set[tuple[str, str]]:
    person_ids = sorted({str(row.get("person_id") or "").strip() for row in rows if row.get("person_id")})
    if not person_ids:
        return set()
    existing = pg.fetch_all(
        """
        select id::text as person_id, coalesce(alternative_names, '[]'::jsonb) as alternative_names
        from core.people
        where id = any(%s::uuid[])
        """,
        [person_ids],
    )
    keys: set[tuple[str, str]] = set()
    for row in existing:
        person_id = str(row.get("person_id") or "").strip()
        for alias in _json_list(row.get("alternative_names")):
            key = _compact_alias(alias)
            if person_id and key:
                keys.add((person_id, key))
    return keys


def _upsert_alias_rows(rows: Sequence[Mapping[str, str]]) -> int:
    if not rows:
        return 0
    with pg.db_cursor(label="sync_bravo_show_hashtag_aliases") as cur:
        for row in rows:
            cur.execute(
                """
                insert into core.show_alternative_names (show_id, name, language, country, source)
                select %s::uuid, %s, null, null, %s
                where not exists (
                  select 1
                  from core.show_alternative_names existing
                  where existing.show_id = %s::uuid
                    and lower(existing.name) = lower(%s)
                    and existing.language is null
                    and existing.country is null
                    and existing.source = %s
                )
                """,
                [row["show_id"], row["name"], row["source"], row["show_id"], row["name"], row["source"]],
            )
    return len(rows)


def _upsert_person_alias_rows(rows: Sequence[Mapping[str, str]]) -> int:
    if not rows:
        return 0
    aliases_by_person: dict[str, list[str]] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        alias = str(row.get("name") or "").strip()
        if person_id and alias:
            aliases_by_person.setdefault(person_id, []).append(alias)

    applied = 0
    with pg.db_cursor(label="sync_bravo_cast_hashtag_aliases") as cur:
        for person_id, aliases in aliases_by_person.items():
            cur.execute(
                """
                select coalesce(alternative_names, '[]'::jsonb) as alternative_names
                from core.people
                where id = %s::uuid
                for update
                """,
                [person_id],
            )
            existing_row = cur.fetchone()
            if not existing_row:
                continue
            existing_aliases = _json_list(existing_row.get("alternative_names"))
            existing_keys = {_compact_alias(alias) for alias in existing_aliases if _compact_alias(alias)}
            merged_aliases = list(existing_aliases)
            for alias in aliases:
                key = _compact_alias(alias)
                if not key or key in existing_keys:
                    continue
                existing_keys.add(key)
                merged_aliases.append(alias)
                applied += 1
            if len(merged_aliases) == len(existing_aliases):
                continue
            cur.execute(
                """
                update core.people
                set alternative_names = %s::jsonb,
                    updated_at = now()
                where id = %s::uuid
                """,
                [json.dumps(merged_aliases), person_id],
            )
    return applied


def _summary(rows: Sequence[Mapping[str, str]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in rows:
        label = str(row.get("show_name") or row.get("show_id") or "").strip()
        grouped.setdefault(label, []).append(str(row.get("name") or "").strip())
    return {key: values for key, values in sorted(grouped.items())}


def _person_summary(rows: Sequence[Mapping[str, str]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in rows:
        label = str(row.get("person_name") or row.get("person_id") or "").strip()
        grouped.setdefault(label, []).append(str(row.get("name") or "").strip())
    return {key: values for key, values in sorted(grouped.items())}


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    source = str(args.source or DEFAULT_ALIAS_SOURCE).strip()
    include_shows = args.entity in {"all", "shows"}
    include_cast = args.entity in {"all", "cast"}

    show_rows = _fetch_show_rows(network=args.network, show_ids=args.show_id) if include_shows else []
    candidate_rows = _alias_rows_for_shows(show_rows, source=source)
    existing_keys = _existing_alias_keys(candidate_rows, source=source) if include_shows else set()
    new_show_rows = [
        row
        for row in candidate_rows
        if (str(row.get("show_id") or "").strip(), _compact_alias(row.get("name"))) not in existing_keys
    ]

    cast_rows = _fetch_cast_rows(network=args.network, show_ids=args.show_id) if include_cast else []
    cast_candidate_rows = _alias_rows_for_cast(cast_rows)
    existing_person_keys = _existing_person_alias_keys(cast_candidate_rows) if include_cast else set()
    new_cast_rows = [
        row
        for row in cast_candidate_rows
        if (str(row.get("person_id") or "").strip(), _compact_alias(row.get("name"))) not in existing_person_keys
    ]

    applied_show_count = _upsert_alias_rows(new_show_rows) if args.apply else 0
    applied_cast_count = _upsert_person_alias_rows(new_cast_rows) if args.apply else 0
    payload = {
        "apply": bool(args.apply),
        "entity": args.entity,
        "source": source,
        "network": str(args.network or "").strip(),
        "show_count": len(show_rows),
        "show_candidate_alias_count": len(candidate_rows),
        "show_new_alias_count": len(new_show_rows),
        "show_applied_alias_count": applied_show_count,
        "cast_person_count": len({str(row.get("person_id") or "") for row in cast_rows if row.get("person_id")}),
        "cast_candidate_alias_count": len(cast_candidate_rows),
        "cast_new_alias_count": len(new_cast_rows),
        "cast_applied_alias_count": applied_cast_count,
        "candidate_alias_count": len(candidate_rows) + len(cast_candidate_rows),
        "new_alias_count": len(new_show_rows) + len(new_cast_rows),
        "applied_alias_count": applied_show_count + applied_cast_count,
        "aliases_by_show": _summary(new_show_rows if not args.apply else candidate_rows),
        "aliases_by_person": _person_summary(new_cast_rows if not args.apply else cast_candidate_rows),
    }
    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

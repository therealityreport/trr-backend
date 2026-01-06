#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any

import scripts.sync_episodes as sync_episodes
import scripts.sync_seasons as sync_seasons
from trr_backend.repositories.shows import update_show

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_seasons_episodes",
        description="Sync seasons + episodes, then recompute show totals and most recent episode fields.",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def _build_common_args(args: argparse.Namespace) -> list[str]:
    argv: list[str] = []
    if args.all:
        argv.append("--all")
    for value in args.show_id or []:
        argv.extend(["--show-id", str(value)])
    for value in args.tmdb_show_id or []:
        argv.extend(["--tmdb-show-id", str(value)])
    for value in args.imdb_series_id or []:
        argv.extend(["--imdb-series-id", str(value)])
    if args.limit is not None:
        argv.extend(["--limit", str(int(args.limit))])
    if args.dry_run:
        argv.append("--dry-run")
    if args.verbose:
        argv.append("--verbose")
    if not args.incremental:
        argv.append("--no-incremental")
    if not args.resume:
        argv.append("--no-resume")
    if args.force:
        argv.append("--force")
    if args.since:
        argv.extend(["--since", str(args.since)])
    return argv


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _build_most_recent_episode_string(ep: dict[str, Any]) -> str | None:
    season = _as_int(ep.get("season_number"))
    episode = _as_int(ep.get("episode_number"))
    title = str(ep.get("title") or "").strip() or None
    air_date = str(ep.get("air_date") or "").strip() or None
    imdb_episode_id = str(ep.get("imdb_episode_id") or "").strip() or None

    parts: list[str] = []
    if season is not None and episode is not None:
        parts.append(f"S{season}.E{episode}")
    if title:
        parts.append(f"- {title}" if parts else title)
    if air_date:
        parts.append(f"({air_date})")

    summary = " ".join(parts).strip()
    if not summary:
        return None
    if imdb_episode_id:
        return f"{summary} [imdbEpisodeId={imdb_episode_id}]"
    return summary


def _fetch_season_numbers(db, show_id: str) -> list[int]:
    response = (
        db.schema("core")
        .table("seasons")
        .select("season_number")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing seasons: {response.error}")
    data = response.data or []
    return [int(row["season_number"]) for row in data if isinstance(row.get("season_number"), int)]


def _fetch_episode_rows(db, show_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("episodes")
        .select("season_number,episode_number,title,air_date,imdb_episode_id")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing episodes: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _pick_most_recent_episode(episodes: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not episodes:
        return None

    with_air = []
    for ep in episodes:
        air_date = ep.get("air_date")
        if isinstance(air_date, str) and air_date.strip():
            with_air.append(ep)

    def sort_key(ep: dict[str, Any]) -> tuple[str, int, int]:
        air_date = str(ep.get("air_date") or "")
        season = _as_int(ep.get("season_number")) or 0
        episode = _as_int(ep.get("episode_number")) or 0
        return (air_date, season, episode)

    if with_air:
        return max(with_air, key=sort_key)
    return max(episodes, key=lambda ep: (_as_int(ep.get("season_number")) or 0, _as_int(ep.get("episode_number")) or 0))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    common_args = _build_common_args(args)

    code = sync_seasons.main(list(common_args))
    if code != 0:
        return code

    code = sync_episodes.main(list(common_args))
    if code != 0:
        return code

    if args.dry_run:
        return 0

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        return 0

    updated = 0
    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        if not show_id:
            continue

        season_numbers = _fetch_season_numbers(db, show_id)
        episodes = _fetch_episode_rows(db, show_id)

        patch: dict[str, Any] = {}
        if season_numbers:
            patch["show_total_seasons"] = len(set(season_numbers))
        if episodes:
            patch["show_total_episodes"] = len(episodes)

        most_recent = _pick_most_recent_episode(episodes)
        if most_recent:
            patch["most_recent_episode"] = _build_most_recent_episode_string(most_recent)
            patch["most_recent_episode_season"] = _as_int(most_recent.get("season_number"))
            patch["most_recent_episode_number"] = _as_int(most_recent.get("episode_number"))
            patch["most_recent_episode_title"] = (
                str(most_recent.get("title") or "").strip() or None
            )
            patch["most_recent_episode_air_date"] = (
                str(most_recent.get("air_date") or "").strip() or None
            )
            patch["most_recent_episode_imdb_id"] = (
                str(most_recent.get("imdb_episode_id") or "").strip() or None
            )

        patch = {k: v for k, v in patch.items() if v is not None}
        if not patch:
            continue

        update_show(db, show_id, patch)
        updated += 1

    if args.verbose:
        print(f"UPDATED shows={updated}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

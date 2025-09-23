#!/usr/bin/env python3
"""Populate WWHLinfo using IMDb API for seasons 6+ with fallbacks."""

import os
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

HEADERS = [
    "EpisodeID",
    "Season",
    "Episode",
    "AirDate",
    "GuestNames",
    "IMDbCastIDs",
    "TMDbCastIDs",
    "REALITEASE",
]

IMDB_SERIES_ID = "tt2057880"
TMDB_SHOW_ID = 22980
START_SEASON = 6


def load_env():
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(env_path)


def imdb_session() -> requests.Session:
    session = requests.Session()
    api_key = os.getenv("IMDB_API_KEY")
    if api_key:
        session.headers["Authorization"] = f"Bearer {api_key}"
    session.headers["Accept"] = "application/json"
    session.headers["User-Agent"] = "WWHLInfoBot/1.0"
    return session


def tmdb_session() -> requests.Session:
    token = os.getenv("TMDB_BEARER") or os.getenv("TMDB_API_KEY")
    if not token:
        raise RuntimeError("TMDB_BEARER or TMDB_API_KEY must be set")
    session = requests.Session()
    if os.getenv("TMDB_BEARER"):
        session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })
    else:
        session.params = {"api_key": token}
    return session


def setup_sheets() -> Tuple[gspread.Worksheet, gspread.Worksheet]:
    key_path = os.path.join(os.path.dirname(__file__), "..", "..", "keys", "trr-backend-df2c438612e1.json")
    creds = Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open("Realitease2025Data")
    wwhl_ws = spreadsheet.worksheet("WWHLinfo")
    realitease_ws = spreadsheet.worksheet("RealiteaseInfo")
    return wwhl_ws, realitease_ws


def ensure_headers(ws: gspread.Worksheet) -> None:
    current = ws.row_values(1)
    if current != HEADERS:
        ws.update(range_name="A1:H1", values=[HEADERS])


def normalize_name(name: str) -> str:
    return " ".join(name.lower().replace('"', "").split())


def load_realitease_mappings(ws: gspread.Worksheet) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, Dict[str, str]]]:
    data = ws.get_all_values()
    if len(data) < 2:
        return {}, {}, {}

    headers = [h.strip().lower() for h in data[0]]
    name_idx = headers.index("castname") if "castname" in headers else None
    imdb_idx = headers.index("castimdbid") if "castimdbid" in headers else None
    tmdb_idx = headers.index("casttmdbid") if "casttmdbid" in headers else None

    tmdb_to_imdb = {}
    imdb_to_tmdb = {}
    name_lookup: Dict[str, Dict[str, str]] = {}

    for row in data[1:]:
        imdb_id = row[imdb_idx].strip() if imdb_idx is not None and len(row) > imdb_idx else ""
        tmdb_id = row[tmdb_idx].strip() if tmdb_idx is not None and len(row) > tmdb_idx else ""
        name = row[name_idx].strip() if name_idx is not None and len(row) > name_idx else ""

        if imdb_id and not imdb_id.startswith("nm"):
            imdb_id = f"nm{imdb_id}"

        if tmdb_id and imdb_id:
            tmdb_to_imdb[tmdb_id] = imdb_id
            imdb_to_tmdb[imdb_id] = tmdb_id

        if name:
            norm = normalize_name(name)
            if norm not in name_lookup:
                name_lookup[norm] = {}
            if imdb_id:
                name_lookup[norm]["imdb_id"] = imdb_id
            if tmdb_id:
                name_lookup[norm]["tmdb_id"] = tmdb_id

    return tmdb_to_imdb, imdb_to_tmdb, name_lookup


def load_existing_rows(ws: gspread.Worksheet) -> Dict[str, Tuple[int, List[str]]]:
    rows = ws.get_all_values()
    mapping = {}
    for idx, row in enumerate(rows[1:], start=2):
        if not row:
            continue
        episode_id = row[0].strip() if len(row) > 0 else ""
        if episode_id:
            mapping[episode_id] = (idx, row)
    return mapping


def extract_air_date(ep_data: dict) -> str:
    release = ep_data.get("releaseDate") or ep_data.get("earliestReleaseDate") or {}
    if isinstance(release, dict):
        year = release.get("year")
        month = release.get("month")
        day = release.get("day")
        if year and month and day:
            try:
                return datetime(year, month, day).strftime("%m/%d/%Y")
            except ValueError:
                pass
    if isinstance(release, str):
        try:
            return datetime.strptime(release, "%Y-%m-%d").strftime("%m/%d/%Y")
        except ValueError:
            return release
    return ""


def fetch_imdb_json(session: requests.Session, url: str, params=None) -> dict:
    time.sleep(0.25)
    resp = session.get(url, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"IMDb API error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def collect_episodes(
    session: requests.Session,
    seasons: List[int],
    verbose: bool = False,
) -> List[dict]:
    episodes = []

    for season in seasons:
        if verbose:
            print(f"📥  Downloading season {season} episodes from IMDb…")

        try:
            data = fetch_imdb_json(
                session,
                f"https://api.imdbapi.dev/titles/{IMDB_SERIES_ID}/episodes",
                params={"season": season, "pageSize": 50},
            )
        except RuntimeError as exc:
            print(f"   ⚠️  Season {season} fetch failed: {exc}")
            continue

        if not data.get("episodes"):
            if verbose:
                print(f"   ⚠️  No episodes returned for season {season}")
            continue

        def process_page(page_data: dict):
            for ep_data in page_data.get("episodes", []):
                episode_id = ep_data.get("id") or ep_data.get("title", {}).get("id")
                if not episode_id:
                    continue
                episode_id = episode_id.replace("/title/", "").replace("/", "")
                episode_number = ep_data.get("episodeNumber") or ep_data.get("episode")
                season_number = ep_data.get("seasonNumber") or season
                if not episode_number:
                    continue
                try:
                    episode_number = int(episode_number)
                except (ValueError, TypeError):
                    continue

                title_info = ep_data.get("titleText") or ep_data.get("episodeTitle") or {}
                title_text = title_info.get("text") if isinstance(title_info, dict) else title_info
                air_date = extract_air_date(ep_data)

                episodes.append({
                    "episode_id": episode_id,
                    "season": int(season_number),
                    "episode_number": episode_number,
                    "air_date": air_date,
                    "title": title_text or "",
                })

                if verbose:
                    summary = f"   • S{season}E{episode_number:02d} ({episode_id})"
                    if title_text:
                        summary += f" — {title_text}"
                    print(summary)

        process_page(data)
        next_token = data.get("nextPageToken")
        while next_token:
            data = fetch_imdb_json(
                session,
                f"https://api.imdbapi.dev/titles/{IMDB_SERIES_ID}/episodes",
                params={"season": season, "pageSize": 50, "pageToken": next_token},
            )
            process_page(data)
            next_token = data.get("nextPageToken")

    return episodes


def fetch_imdb_episode_guests(session: requests.Session, episode_id: str) -> List[Tuple[str, str, List[str]]]:
    guests = []
    page_token = None
    while True:
        params = {"categories": "self", "pageSize": 50}
        if page_token:
            params["pageToken"] = page_token
        data = fetch_imdb_json(session, f"https://api.imdbapi.dev/titles/{episode_id}/credits", params=params)
        credits = data.get("credits", [])
        for credit in credits:
            name_info = credit.get("name") or {}
            person_id = name_info.get("id")
            display_name = name_info.get("displayName") or name_info.get("name")
            characters = credit.get("characters", [])
            if not person_id or not display_name:
                continue
            # skip host
            if "host" in " ".join(characters).lower():
                continue
            guests.append((display_name, person_id, characters))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return guests


def fetch_tmdb_guests(session: requests.Session, season: int, episode_number: int) -> List[Tuple[str, str]]:
    try:
        credits = fetch_imdb_json(
            session,
            f"https://api.themoviedb.org/3/tv/{TMDB_SHOW_ID}/season/{season}/episode/{episode_number}/credits",
        )
    except RuntimeError:
        return []
    guests = []
    for guest in credits.get("guest_stars", []):
        tmdb_id = str(guest.get("id")) if guest.get("id") else ""
        name = guest.get("name") or guest.get("original_name") or ""
        if tmdb_id and name:
            guests.append((name, tmdb_id))
    return guests


def fallback_from_title(title: str, name_lookup: Dict[str, Dict[str, str]]) -> List[Tuple[str, Optional[str], Optional[str]]]:
    if not title:
        return []
    raw_parts = [part.strip() for part in title.replace("&", "/").split("/")] 
    guests = []
    for part in raw_parts:
        if not part:
            continue
        norm = normalize_name(part)
        info = name_lookup.get(norm, {})
        imdb_id = info.get("imdb_id")
        tmdb_id = info.get("tmdb_id")
        guests.append((part, imdb_id, tmdb_id))
    return guests


def merge_guest_lists(
    imdb_guests: List[Tuple[str, str, List[str]]],
    tmdb_guests: List[Tuple[str, str]],
    imdb_to_tmdb: Dict[str, str],
    tmdb_to_imdb: Dict[str, str],
) -> Tuple[List[str], List[str], List[str]]:
    names = []
    imdb_ids = []
    tmdb_ids = []

    seen_imdb = set()
    seen_tmdb = set()

    for name, imdb_id, _ in imdb_guests:
        names.append(name)
        imdb_ids.append(imdb_id)
        seen_imdb.add(imdb_id)
        tmdb_id = imdb_to_tmdb.get(imdb_id, "")
        if tmdb_id:
            tmdb_ids.append(tmdb_id)
            seen_tmdb.add(tmdb_id)

    for name, tmdb_id in tmdb_guests:
        if tmdb_id in seen_tmdb:
            continue
        imdb_id = tmdb_to_imdb.get(tmdb_id)
        names.append(name)
        if imdb_id:
            imdb_ids.append(imdb_id)
            seen_imdb.add(imdb_id)
        tmdb_ids.append(tmdb_id)
        seen_tmdb.add(tmdb_id)

    return names, imdb_ids, tmdb_ids


def write_rows(
    ws: gspread.Worksheet,
    episodes: List[dict],
    existing_rows: Dict[str, Tuple[int, List[str]]],
    dry_run: bool = False,
) -> Tuple[int, int]:
    updates = []
    appends = []

    for data in episodes:
        episode_id = data["episode_id"]
        guest_names = ", ".join(sorted(set(data["guest_names"])))
        imdb_ids = ", ".join(sorted(set(data["imdb_ids"])))
        tmdb_ids = ", ".join(sorted(set(data["tmdb_ids"])))

        row_values = [
            episode_id,
            str(data["season"]),
            str(data["episode_number"]),
            data["air_date"],
            guest_names,
            imdb_ids,
            tmdb_ids,
            "",
        ]

        if episode_id in existing_rows:
            row_num, existing = existing_rows[episode_id]
            if len(existing) >= 8 and existing[7]:
                row_values[7] = existing[7]
            updates.append({"range": f"A{row_num}:H{row_num}", "values": [row_values]})
        else:
            appends.append(row_values)

    if dry_run:
        print(f"🔍 DRY RUN: would update {len(updates)} rows and append {len(appends)} rows")
        return len(updates), len(appends)

    if updates:
        ws.batch_update(updates)
    if appends:
        ws.append_rows(appends, value_input_option="RAW")
    return len(updates), len(appends)


def main():
    parser = argparse.ArgumentParser(description="Populate WWHLinfo from IMDb API (seasons 6+)")
    parser.add_argument(
        "--season",
        dest="seasons",
        action="append",
        type=int,
        help="Season number to include (can be repeated)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument("--verbose", action="store_true", help="Print per-episode logging")
    args = parser.parse_args()

    load_env()
    imdb_sess = imdb_session()
    tmdb_sess = tmdb_session()
    wwhl_ws, realitease_ws = setup_sheets()
    ensure_headers(wwhl_ws)

    tmdb_to_imdb, imdb_to_tmdb, name_lookup = load_realitease_mappings(realitease_ws)
    existing_rows = load_existing_rows(wwhl_ws)

    if args.seasons:
        seasons = sorted({season for season in args.seasons if season >= START_SEASON})
    else:
        seasons = list(range(START_SEASON, START_SEASON + 20))

    print(f"📺 Processing seasons sequentially: {seasons}")

    total_updates = 0
    total_appends = 0

    for season in seasons:
        print(f"\n=== Season {season} ===")
        episode_records = collect_episodes(imdb_sess, [season], verbose=args.verbose)
        if not episode_records:
            print(f"   ⚠️  No episodes returned for season {season}")
            continue
        print(f"📄 Season {season}: retrieved {len(episode_records)} episodes from IMDb")

        enriched_rows = []
        for ep in episode_records:
            imdb_guests = []
            try:
                imdb_guests = fetch_imdb_episode_guests(imdb_sess, ep["episode_id"])
            except RuntimeError:
                imdb_guests = []

            tmdb_guests = []
            if not imdb_guests:
                tmdb_guests = fetch_tmdb_guests(tmdb_sess, ep["season"], ep["episode_number"])

            guest_names: List[str] = []
            imdb_ids: List[str] = []
            tmdb_ids: List[str] = []

            if imdb_guests or tmdb_guests:
                names, imdbs, tmdbs = merge_guest_lists(imdb_guests, tmdb_guests, imdb_to_tmdb, tmdb_to_imdb)
                guest_names = names
                imdb_ids = imdbs
                tmdb_ids = tmdbs
            else:
                fallback_guests = fallback_from_title(ep["title"], name_lookup)
                for name, imdb_id, tmdb_id in fallback_guests:
                    guest_names.append(name)
                    if imdb_id:
                        imdb_ids.append(imdb_id)
                    if tmdb_id:
                        tmdb_ids.append(tmdb_id)

            if args.verbose:
                if guest_names:
                    print(
                        f"   • S{ep['season']}E{ep['episode_number']:02d} ({ep['episode_id']}): "
                        f"{', '.join(guest_names)}"
                    )
                else:
                    print(
                        f"   • S{ep['season']}E{ep['episode_number']:02d} ({ep['episode_id']}): "
                        "no guests from APIs/title"
                    )

            enriched_rows.append({
                "episode_id": ep["episode_id"],
                "season": ep["season"],
                "episode_number": ep["episode_number"],
                "air_date": ep["air_date"],
                "guest_names": guest_names,
                "imdb_ids": imdb_ids,
                "tmdb_ids": tmdb_ids,
            })

        updates, appends = write_rows(
            wwhl_ws,
            enriched_rows,
            existing_rows,
            dry_run=args.dry_run,
        )
        total_updates += updates
        total_appends += appends
        print(
            f"✅ Season {season} summary — "
            f"{'Would update' if args.dry_run else 'Updated'} {updates} rows, "
            f"{'would append' if args.dry_run else 'appended'} {appends} rows"
        )

        if not args.dry_run and (updates or appends):
            existing_rows = load_existing_rows(wwhl_ws)

    print(
        f"\n📊 IMDb Step2 complete — "
        f"{'Would update' if args.dry_run else 'Updated'} {total_updates} rows total, "
        f"{'would append' if args.dry_run else 'appended'} {total_appends} rows"
    )


if __name__ == "__main__":
    main()

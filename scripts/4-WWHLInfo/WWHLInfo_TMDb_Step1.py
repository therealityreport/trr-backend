#!/usr/bin/env python3
"""Populate WWHLinfo using TMDb for early seasons."""

import os
import time
from datetime import datetime
from typing import Dict, List, Tuple
import argparse

import gspread
import requests
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

SHOW_TMDB_ID = 22980
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


def load_env():
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(env_path)


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


def load_realitease_mappings(ws: gspread.Worksheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    data = ws.get_all_values()
    if len(data) < 2:
        return {}, {}

    headers = [h.strip().lower() for h in data[0]]
    name_idx = headers.index("castname") if "castname" in headers else None
    imdb_idx = headers.index("castimdbid") if "castimdbid" in headers else None
    tmdb_idx = headers.index("casttmdbid") if "casttmdbid" in headers else None

    tmdb_to_imdb = {}
    imdb_to_tmdb = {}

    for row in data[1:]:
        tmdb_id = (row[tmdb_idx].strip() if tmdb_idx is not None and len(row) > tmdb_idx else "")
        imdb_id = (row[imdb_idx].strip() if imdb_idx is not None and len(row) > imdb_idx else "")
        if imdb_id and not imdb_id.startswith("nm"):
            imdb_id = f"nm{imdb_id}"
        if tmdb_id:
            if imdb_id:
                tmdb_to_imdb[tmdb_id] = imdb_id
            if imdb_id:
                imdb_to_tmdb[imdb_id] = tmdb_id
    return tmdb_to_imdb, imdb_to_tmdb


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


def format_date(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%m/%d/%Y")
    except ValueError:
        return iso_date


def fetch_tmdb_json(session: requests.Session, url: str, params=None) -> dict:
    time.sleep(0.25)
    resp = session.get(url, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"TMDb request failed: {resp.status_code} - {resp.text[:200]}")
    return resp.json()


def collect_episode_data(
    session: requests.Session,
    tmdb_to_imdb: Dict[str, str],
    seasons: List[int],
    verbose: bool = False,
) -> List[dict]:
    episodes = []
    for season in seasons:
        if verbose:
            print(f"📥  Downloading season {season} metadata from TMDb…")
        season_data = fetch_tmdb_json(session, f"https://api.themoviedb.org/3/tv/{SHOW_TMDB_ID}/season/{season}")
        for episode in season_data.get("episodes", []):
            ep_number = episode.get("episode_number")
            episode_tmdb_id = str(episode.get("id")) if episode.get("id") else ""
            air_date = format_date(episode.get("air_date", ""))

            external_ids = fetch_tmdb_json(
                session,
                f"https://api.themoviedb.org/3/tv/{SHOW_TMDB_ID}/season/{season}/episode/{ep_number}/external_ids",
            )
            imdb_episode_id = external_ids.get("imdb_id") or ""
            episode_id = imdb_episode_id or episode_tmdb_id

            credits = fetch_tmdb_json(
                session,
                f"https://api.themoviedb.org/3/tv/{SHOW_TMDB_ID}/season/{season}/episode/{ep_number}/credits",
            )
            guest_list = credits.get("guest_stars", [])

            guest_names = []
            imdb_ids = []
            tmdb_ids = []
            for guest in guest_list:
                tmdb_id = str(guest.get("id")) if guest.get("id") else ""
                name = guest.get("name") or guest.get("original_name") or ""
                if not name:
                    continue
                guest_names.append(name)
                if tmdb_id:
                    tmdb_ids.append(tmdb_id)
                    imdb_id = tmdb_to_imdb.get(tmdb_id)
                    if imdb_id:
                        imdb_ids.append(imdb_id)

            if verbose:
                if guest_names:
                    print(
                        f"   • S{season}E{ep_number:02d} ({episode_id}): "
                        f"{', '.join(guest_names)}"
                    )
                else:
                    print(
                        f"   • S{season}E{ep_number:02d} ({episode_id}): "
                        "no guest_stars returned by TMDb"
                    )

            episodes.append({
                "episode_id": episode_id,
                "season": season,
                "episode_number": ep_number,
                "air_date": air_date,
                "guest_names": guest_names,
                "tmdb_ids": tmdb_ids,
                "imdb_ids": imdb_ids,
                "fallback_tmdb_id": episode_tmdb_id,
            })
    return episodes


def write_rows(
    ws: gspread.Worksheet,
    episodes: List[dict],
    existing_rows: Dict[str, Tuple[int, List[str]]],
    dry_run: bool = False,
) -> Tuple[int, int]:
    updates = []
    appends = []

    for data in episodes:
        episode_id = data["episode_id"] or data["fallback_tmdb_id"]
        if not episode_id:
            continue

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
    parser = argparse.ArgumentParser(description="Populate WWHLinfo from TMDb (early seasons)")
    parser.add_argument(
        "--season",
        dest="seasons",
        action="append",
        type=int,
        help="Season number to include (can be repeated)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to Google Sheets",
    )
    args = parser.parse_args()

    load_env()
    session = tmdb_session()
    wwhl_ws, realitease_ws = setup_sheets()
    ensure_headers(wwhl_ws)

    tmdb_to_imdb, _ = load_realitease_mappings(realitease_ws)
    existing_rows = load_existing_rows(wwhl_ws)

    if args.seasons:
        seasons = sorted({season for season in args.seasons if season > 0})
    else:
        seasons = [1, 2, 3, 4, 5]

    print(f"📺 Processing seasons sequentially: {seasons}")

    total_updates = 0
    total_appends = 0

    for season in seasons:
        print(f"\n=== Season {season} ===")
        season_episodes = collect_episode_data(session, tmdb_to_imdb, [season], verbose=True)
        print(f"📄 Season {season}: retrieved {len(season_episodes)} episodes")
        updates, appends = write_rows(
            wwhl_ws,
            season_episodes,
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

        # refresh existing rows mapping so subsequent seasons see new episode IDs
        if not args.dry_run and (updates or appends):
            existing_rows = load_existing_rows(wwhl_ws)

    print(
        f"\n📊 TMDb Step1 complete — "
        f"{'Would update' if args.dry_run else 'Updated'} {total_updates} rows total, "
        f"{'would append' if args.dry_run else 'appended'} {total_appends} rows"
    )


if __name__ == "__main__":
    main()

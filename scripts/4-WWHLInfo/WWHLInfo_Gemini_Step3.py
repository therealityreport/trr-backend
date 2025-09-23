#!/usr/bin/env python3
"""Fill missing WWHLinfo data using the Gemini API."""

import argparse
import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple

import gspread
import google.generativeai as genai
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

import random
from collections import deque
import hashlib

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


class PerMinuteRateLimiter:
    """Enforces <= max_calls per rolling 60s window (per model)."""
    def __init__(self, max_calls_per_minute: int = 1000, safety_margin: float = 0.9):
        self.capacity = int(max_calls_per_minute * safety_margin)  # e.g., 900
        self.events = deque()  # timestamps of recent calls

    def acquire(self):
        now = time.time()
        window_start = now - 60.0
        # drop old timestamps
        while self.events and self.events[0] < window_start:
            self.events.popleft()
        if len(self.events) >= self.capacity:
            sleep_for = self.events[0] + 60.0 - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            # after sleeping, window advanced; call again
            return self.acquire()
        self.events.append(time.time())


class DiskCache:
    """Simple append-only JSONL cache keyed by prompt hash; survives reruns."""
    def __init__(self, path: str = ".cache/.gemini_cache.jsonl"):
        self.path = path
        self.mem = {}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        k, v = json.loads(line)
                        self.mem[k] = v
                    except Exception:
                        pass

    @staticmethod
    def _key(prompt: str) -> str:
        return hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    def get(self, prompt: str):
        return self.mem.get(self._key(prompt))

    def set(self, prompt: str, value):
        k = self._key(prompt)
        self.mem[k] = value
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps([k, value], ensure_ascii=False) + "\n")


# Globals wired up in main()
_RATE_LIMITER: Optional[PerMinuteRateLimiter] = None
_DISK_CACHE: Optional[DiskCache] = None

_GEMINI_CACHE: Dict[str, Optional[dict]] = {}


def load_env():
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(env_path)


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
    return re.sub(r"[^a-z0-9]", "", name.lower()) if name else ""


def load_realitease_mappings(ws: gspread.Worksheet) -> Dict[str, Dict[str, str]]:
    data = ws.get_all_values()
    if len(data) < 2:
        return {}

    headers = [h.strip().lower() for h in data[0]]
    name_idx = headers.index("castname") if "castname" in headers else None
    imdb_idx = headers.index("castimdbid") if "castimdbid" in headers else None
    tmdb_idx = headers.index("casttmdbid") if "casttmdbid" in headers else None

    lookup: Dict[str, Dict[str, str]] = {}
    print(f"📋 Loading cast ID mappings from RealiteaseInfo sheet...")

    for row in data[1:]:
        name = row[name_idx].strip() if name_idx is not None and len(row) > name_idx else ""
        imdb_id = row[imdb_idx].strip() if imdb_idx is not None and len(row) > imdb_idx else ""
        tmdb_id = row[tmdb_idx].strip() if tmdb_idx is not None and len(row) > tmdb_idx else ""
        
        # Ensure IMDb ID has proper format
        if imdb_id and not imdb_id.startswith("nm"):
            imdb_id = f"nm{imdb_id}"
            
        if name:
            norm = normalize_name(name)
            entry = lookup.setdefault(norm, {})
            if imdb_id:
                entry["imdb_id"] = imdb_id
            if tmdb_id:
                entry["tmdb_id"] = tmdb_id

    print(f"📊 Loaded {len(lookup)} cast member ID mappings")
    return lookup


def init_gemini(model: str) -> genai.GenerativeModel:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(model)


def parse_gemini_json(text: str) -> Optional[dict]:
    if "```" in text:
        blocks = re.findall(r"```(?:json)?\s*(.*?)```", text, flags=re.DOTALL)
        if blocks:
            text = blocks[0]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def call_gemini(model: genai.GenerativeModel, prompt: str, retry: int = 3) -> Optional[dict]:
    global _DISK_CACHE, _RATE_LIMITER, _GEMINI_CACHE
    # in-memory cache first
    if prompt in _GEMINI_CACHE:
        return _GEMINI_CACHE[prompt]
    # persistent cache next
    if _DISK_CACHE:
        cached = _DISK_CACHE.get(prompt)
        if cached is not None:
            _GEMINI_CACHE[prompt] = cached
            return cached

    last_error = None
    backoff = 1.0
    for attempt in range(retry):
        try:
            if _RATE_LIMITER:
                _RATE_LIMITER.acquire()  # enforce per-minute cap
            response = model.generate_content(prompt)
            if hasattr(response, "text"):
                payload = parse_gemini_json(response.text)
                if payload:
                    _GEMINI_CACHE[prompt] = payload
                    if _DISK_CACHE:
                        _DISK_CACHE.set(prompt, payload)
                    return payload
            last_error = ValueError("Gemini response did not contain valid JSON")
        except Exception as exc:
            # If quota/rate limited, try to honor suggested delay from message text
            msg = str(exc)
            last_error = exc
            # "Please retry in 31.702828628s." or retry_delay { seconds: 31 }
            m = re.search(r"Please retry in ([0-9.]+)s", msg)
            n = re.search(r"retry_delay\s*{\s*seconds:\s*(\d+)", msg)
            if m:
                sleep_for = float(m.group(1))
                time.sleep(sleep_for)
            elif n:
                sleep_for = float(n.group(1))
                time.sleep(sleep_for)
            else:
                # exponential backoff + jitter
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2

        # next attempt continues

    print(f"   ⚠️  Gemini request failed: {last_error}")
    _GEMINI_CACHE[prompt] = None
    if _DISK_CACHE:
        _DISK_CACHE.set(prompt, None)  # negative cache to avoid hammering
    return None


def build_prompt(row: List[str], missing_fields: List[str]) -> str:
    episode_id, season, episode, air_date, guest_names, imdb_ids, tmdb_ids, _ = (row + [""] * (8 - len(row)))[:8]
    
    # Build search terms that Gemini can actually use to find the episode
    search_terms = []
    if season and episode:
        search_terms.append(f"WWHL Season {season} Episode {episode}")
        search_terms.append(f"Watch What Happens Live Season {season} Episode {episode}")
    if air_date:
        search_terms.append(f"WWHL {air_date}")
        search_terms.append(f"Watch What Happens Live {air_date}")
    
    # If we have no useful search terms, create a generic one
    if not search_terms:
        if season:
            search_terms.append(f"WWHL Season {season}")
        else:
            search_terms.append("Watch What Happens Live episode")
    
    payload = {
        "search_terms": search_terms,
        "current_season": season,
        "current_episode": episode,
        "current_air_date": air_date,
        "current_guest_names": guest_names,
    }
    
    instructions = (
        f"You are researching a Watch What Happens Live episode. "
        f"Use these search terms to find the correct episode: {', '.join(search_terms)}. "
        f"Only fill the following missing fields: {', '.join(missing_fields)}. "
        f"If a field already has a value, keep it exactly as is. "
        f"Return ONLY JSON with the keys you filled (omit keys you did not change). "
        f"Use the format: "
        f'{{\"season\": \"<string>\", \"episode\": \"<string>\", \"air_date\": \"<MM/DD/YYYY or empty>\", '
        f'\"guest_names\": [\"Guest 1\", \"Guest 2\", ...]}}. '
        f"Provide empty strings or an empty list when you cannot determine a value with confidence. "
        f"Focus on finding the actual Watch What Happens Live episode that matches the search terms."
    )
    prompt = f"{instructions}\nCurrent data: {json.dumps(payload, ensure_ascii=False)}"
    return prompt


def merge_ids(names: List[str], name_lookup: Dict[str, Dict[str, str]]) -> Tuple[List[str], List[str]]:
    imdb_ids: List[str] = []
    tmdb_ids: List[str] = []
    for name in names:
        norm = normalize_name(name)
        info = name_lookup.get(norm, {})
        imdb_id = info.get("imdb_id", "")
        tmdb_id = info.get("tmdb_id", "")
        imdb_ids.append(imdb_id)
        tmdb_ids.append(tmdb_id)
    return imdb_ids, tmdb_ids


def process_rows(
    ws: gspread.Worksheet,
    name_lookup: Dict[str, Dict[str, str]],
    model: genai.GenerativeModel,
    start_row: int,
    end_row: Optional[int],
    limit: Optional[int],
    dry_run: bool,
    batch_size: int = 50,  # More frequent updates
) -> Tuple[int, int]:
    values = ws.get_all_values()
    updates = []
    rows_updated = 0
    gemini_calls = 0

    for idx, row in enumerate(values[1:], start=2):
        if idx < start_row:
            continue
        if end_row and idx > end_row:
            break
        if limit and gemini_calls >= limit:
            break

        padded = (row + [""] * (8 - len(row)))[:8]
        episode_id, season, episode, air_date, guest_names, imdb_ids, tmdb_ids, realitease = padded

        missing = []
        if not season:
            missing.append("season")
        if not episode:
            missing.append("episode")
        if not air_date:
            missing.append("air_date")
        if not guest_names:
            missing.append("guest_names")

        if not missing:
            continue

        # Show what search terms will be used
        search_terms = []
        if season and episode:
            search_terms.append(f"WWHL Season {season} Episode {episode}")
        elif air_date:
            search_terms.append(f"WWHL {air_date}")
        elif season:
            search_terms.append(f"WWHL Season {season}")
        else:
            search_terms.append("WWHL episode")

        print(f"\n🔎 Row {idx} (EpisodeID: {episode_id}) — missing {', '.join(missing)}")
        print(f"   🔍 Search terms: {', '.join(search_terms)}")
        prompt = build_prompt(padded, missing)
        
        # Check if this prompt is already cached
        was_cached = (prompt in _GEMINI_CACHE) or (_DISK_CACHE and _DISK_CACHE.get(prompt) is not None)
        
        payload = call_gemini(model, prompt)
        
        # Only increment gemini_calls if we made an actual API call (not cached)
        if not was_cached:
            gemini_calls += 1
        
        if not payload:
            print(f"   ❌ No valid response from Gemini for row {idx}")
            continue

        new_season = payload.get("season") or season
        new_episode = payload.get("episode") or episode
        new_air_date = payload.get("air_date") or air_date
        new_guest_names = payload.get("guest_names") or []

        if isinstance(new_season, (int, float)):
            new_season = str(int(new_season))
        if isinstance(new_episode, (int, float)):
            new_episode = str(int(new_episode))
        if isinstance(new_guest_names, str):
            new_guest_names = [name.strip() for name in new_guest_names.split(",") if name.strip()]

        # Log what information was found/updated
        updates_made = []
        if new_season and new_season != season:
            updates_made.append(f"Season: '{season}' → '{new_season}'")
        if new_episode and new_episode != episode:
            updates_made.append(f"Episode: '{episode}' → '{new_episode}'")
        if new_air_date and new_air_date != air_date:
            updates_made.append(f"AirDate: '{air_date}' → '{new_air_date}'")
        if new_guest_names and ", ".join(new_guest_names) != guest_names:
            updates_made.append(f"GuestNames: '{guest_names}' → '{', '.join(new_guest_names)}'")
        
        if updates_made:
            print(f"   ✅ Found info for row {idx}:")
            for update in updates_made:
                print(f"      • {update}")
        else:
            print(f"   ℹ️ No new information found for row {idx}")
            continue

        # Only look up IDs if we found new guest names
        new_imdb_ids = imdb_ids  # Keep existing IDs by default
        new_tmdb_ids = tmdb_ids  # Keep existing IDs by default
        
        if new_guest_names and ", ".join(new_guest_names) != guest_names:
            # We found new guest names, look up their IDs
            imdb_list, tmdb_list = merge_ids(new_guest_names, name_lookup)
            new_imdb_ids = ", ".join(filter(None, imdb_list)) if imdb_list else ""
            new_tmdb_ids = ", ".join(filter(None, tmdb_list)) if tmdb_list else ""
            
            # Log the ID lookups
            print(f"   🔍 Looking up IDs for guests: {new_guest_names}")
            for i, name in enumerate(new_guest_names):
                norm_name = normalize_name(name)
                found_info = name_lookup.get(norm_name, {})
                imdb_found = found_info.get("imdb_id", "")
                tmdb_found = found_info.get("tmdb_id", "")
                if imdb_found or tmdb_found:
                    print(f"      • {name}: IMDb={imdb_found}, TMDb={tmdb_found}")
                else:
                    print(f"      • {name}: No IDs found in RealiteaseInfo")

        row_values = [
            episode_id,
            new_season or season,
            new_episode or episode,
            new_air_date or air_date,
            ", ".join(new_guest_names) if new_guest_names else guest_names,
            new_imdb_ids,
            new_tmdb_ids,
            realitease,
        ]

        if row_values == padded:
            continue

        # Log the specific cell updates that will be made
        cell_updates = []
        if row_values[1] != padded[1]:  # Season column B
            cell_updates.append(f"B{idx} (Season): '{padded[1]}' → '{row_values[1]}'")
        if row_values[2] != padded[2]:  # Episode column C
            cell_updates.append(f"C{idx} (Episode): '{padded[2]}' → '{row_values[2]}'")
        if row_values[3] != padded[3]:  # AirDate column D
            cell_updates.append(f"D{idx} (AirDate): '{padded[3]}' → '{row_values[3]}'")
        if row_values[4] != padded[4]:  # GuestNames column E
            cell_updates.append(f"E{idx} (GuestNames): '{padded[4]}' → '{row_values[4]}'")
        if row_values[5] != padded[5]:  # IMDbCastIDs column F
            cell_updates.append(f"F{idx} (IMDbIDs): '{padded[5]}' → '{row_values[5]}'")
        if row_values[6] != padded[6]:  # TMDbCastIDs column G
            cell_updates.append(f"G{idx} (TMDbIDs): '{padded[6]}' → '{row_values[6]}'")
        
        if cell_updates:
            print(f"   📝 Will update cells:")
            for cell_update in cell_updates:
                print(f"      • {cell_update}")

        updates.append({"range": f"A{idx}:H{idx}", "values": [row_values]})
        rows_updated += 1
        
        # Write updates in smaller batches for progress and crash recovery
        if not dry_run and len(updates) >= batch_size:
            print(f"\n📤 Writing batch of {len(updates)} rows to Google Sheets (rows processed so far: {rows_updated})...")
            ws.batch_update(updates)
            print(f"   ✅ Batch written successfully")
            updates = []  # Clear the batch

    if dry_run:
        print(f"🔍 DRY RUN: would update {rows_updated} rows (Gemini called {gemini_calls} times)")
        return rows_updated, gemini_calls

    # Write any remaining updates
    if updates:
        print(f"   📤 Writing final batch of {len(updates)} rows to Google Sheets...")
        ws.batch_update(updates)
    
    print(f"✅ Completed writing {rows_updated} rows to Google Sheets")
    return rows_updated, gemini_calls


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill missing WWHLinfo data using Gemini")
    parser.add_argument("--start-row", type=int, default=2, help="Row number to start processing (default 2)")
    parser.add_argument("--end-row", type=int, help="Inclusive row to stop processing")
    parser.add_argument("--limit", type=int, default=500, help="Maximum rows (Gemini calls) to attempt")
    parser.add_argument("--model", default="gemini-2.5-flash", help="Gemini model name (paid tier)")
    parser.add_argument("--dry-run", action="store_true", help="Show the changes without writing")
    parser.add_argument("--rpm", type=int, default=1000, help="Max requests per minute for limiter (project+model).")
    parser.add_argument("--safety-margin", type=float, default=0.9, help="Limiter safety margin (0<sm≤1).")
    parser.add_argument("--cache-file", default=".cache/.gemini_cache.jsonl", help="Path to on-disk cache JSONL.")
    return parser.parse_args()


def main():
    args = parse_args()
    load_env()
    wwhl_ws, realitease_ws = setup_sheets()
    ensure_headers(wwhl_ws)
    name_lookup = load_realitease_mappings(realitease_ws)
    model = init_gemini(args.model)

    global _RATE_LIMITER, _DISK_CACHE
    _RATE_LIMITER = PerMinuteRateLimiter(max_calls_per_minute=args.rpm, safety_margin=args.safety_margin)
    _DISK_CACHE = DiskCache(path=args.cache_file)
    # Warn loudly if user accidentally uses free-tier model or free-tier quota is active.
    if "1.5" in args.model:
        print("⚠️  You are using a 1.5 model. If you expect paid Tier 1 limits (1000 RPM), use --model gemini-2.5-flash.")

    updated_rows, _ = process_rows(
        wwhl_ws,
        name_lookup,
        model,
        start_row=args.start_row,
        end_row=args.end_row,
        limit=args.limit,
        dry_run=args.dry_run,
        batch_size=50,  # Update every 50 rows
    )
    if args.dry_run:
        print(f"\n📊 Gemini Step3 dry run complete — {updated_rows} rows would be updated")
    else:
        print(f"\n📊 Gemini Step3 complete — updated {updated_rows} rows")


if __name__ == "__main__":
    main()

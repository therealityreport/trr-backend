#!/usr/bin/env python3
"""RealiteaseInfo Step 4: Backfill missing TMDb Cast IDs by name matching."""

# This file is the renamed version of RealiteaseInfo_BackfillTMDb.py
# Content mirrors the original with adjusted docstrings and imports.

import argparse
import os
import time
import unicodedata
import re
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from dotenv import load_dotenv

from RealiteaseInfo_Step3 import (
    REALITEASE_COLUMN_INDEX,
    REALITEASE_COLUMN_NUMBER,
    get_realitease_value,
    column_number_to_letter,
)


REQUEST_DELAY = 0.3  # Respect TMDb rate limits (~40 req/10s)
POST_BATCH_DELAY = 0.5
DEFAULT_BATCH_SIZE = 500
DEFAULT_SHOW_LIMIT = 5


def normalize_name(name: str) -> str:
    """Return a lowercased, accent-free version of ``name`` without punctuation."""
    if not name:
        return ''
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s'-]", ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_first_last(name: str) -> Tuple[str, str]:
    """Extract first and last name tokens for straightforward comparison."""
    normalized = normalize_name(name)
    if not normalized:
        return '', ''
    parts = normalized.split()
    if not parts:
        return '', ''
    first = parts[0]
    last = parts[-1]
    last_primary = last.split('-')[-1]
    return first, last_primary


def names_match(a: str, b: str) -> bool:
    """Return True when first and last names align (ignoring middle names)."""
    first_a, last_a = extract_first_last(a)
    first_b, last_b = extract_first_last(b)
    if not first_a or not first_b or not last_a or not last_b:
        return False
    if first_a != first_b:
        return False
    if last_a == last_b:
        return True
    hyphen_variants_a = set(last_a.split('-'))
    hyphen_variants_b = set(last_b.split('-'))
    return bool(hyphen_variants_a & hyphen_variants_b)


class RealiteaseTMDbCastBackfiller:
    """Step 4 task: Backfill missing TMDb cast IDs using TMDb show credits."""

    def __init__(self, batch_size: int, shows_limit: int, dry_run: bool = False):
        load_dotenv()

        self.tmdb_api_key = (os.getenv('TMDB_API_KEY') or '').strip()
        if not self.tmdb_api_key:
            raise RuntimeError('TMDB_API_KEY is not set. Add it to your environment or .env file.')

        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '../../keys/trr-backend-df2c438612e1.json')
        if not os.path.isabs(creds_path):
            creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), creds_path)

        self.gc = gspread.service_account(filename=creds_path)
        spreadsheet_name = os.getenv('SPREADSHEET_NAME', 'Realitease2025Data')
        workbook = self.gc.open(spreadsheet_name)
        self.worksheet = workbook.worksheet('RealiteaseInfo')

        self.batch_size = batch_size
        self.shows_limit = shows_limit
        self.dry_run = dry_run

        self.tmdb_session = requests.Session()
        self.tmdb_base_url = 'https://api.themoviedb.org/3'
        self.credits_cache: Dict[str, Dict[str, List[Dict[str, object]]]] = {}
        self.person_credit_cache: Dict[int, Dict[str, List[Dict[str, object]]]] = {}

        self.pending_updates: List[Dict[str, List[List[str]]]] = []
        self.stats = {
            'rows_considered': 0,
            'matched': 0,
            'skipped_existing': 0,
            'skipped_no_shows': 0,
            'not_found': 0,
            'api_errors': 0,
            'search_attempts': 0,
        }

        self.target_column_letter = column_number_to_letter(REALITEASE_COLUMN_NUMBER['cast_tmdb_id'])
        self.interval_start_time = time.time()
        self.interval_start_matched = 0

    def fetch_all_rows(self) -> List[List[str]]:
        print('📋 Downloading RealiteaseInfo sheet…')
        return self.worksheet.get_all_values()

    def search_tmdb_person(self, name: str) -> List[Dict[str, object]]:
        url = f"{self.tmdb_base_url}/search/person"
        params = {
            'api_key': self.tmdb_api_key,
            'query': name,
            'include_adult': 'false',
            'page': 1,
        }
        try:
            time.sleep(REQUEST_DELAY)
            response = self.tmdb_session.get(url, params=params, timeout=20)
            if response.status_code != 200:
                print(f"      ⚠️ TMDb search error for '{name}': HTTP {response.status_code}")
                self.stats['api_errors'] += 1
                return []
            data = response.json() or {}
            return data.get('results', [])[:10]
        except requests.RequestException as exc:
            print(f"      ⚠️ TMDb search request error for '{name}': {exc}")
            self.stats['api_errors'] += 1
            return []

    def get_person_tv_credits(self, person_id: int) -> Optional[Dict[str, List[Dict[str, object]]]]:
        if person_id in self.person_credit_cache:
            return self.person_credit_cache[person_id]
        url = f"{self.tmdb_base_url}/person/{person_id}/tv_credits"
        params = {'api_key': self.tmdb_api_key}
        try:
            time.sleep(REQUEST_DELAY)
            response = self.tmdb_session.get(url, params=params, timeout=20)
            if response.status_code != 200:
                print(f"      ⚠️ TMDb TV credits error for person {person_id}: HTTP {response.status_code}")
                self.stats['api_errors'] += 1
                return None
            data = response.json() or {}
            self.person_credit_cache[person_id] = data
            return data
        except requests.RequestException as exc:
            print(f"      ⚠️ TMDb TV credits request error for person {person_id}: {exc}")
            self.stats['api_errors'] += 1
            return None

    def person_on_any_show(self, person_id: int, show_ids: List[str]) -> bool:
        credits = self.get_person_tv_credits(person_id)
        if not credits:
            return False
        cast_entries = credits.get('cast') or []
        for entry in cast_entries:
            entry_id = entry.get('id')
            if entry_id is None:
                continue
            if str(entry_id) in show_ids:
                return True
        return False

    def find_tmdb_id_by_name(self, person_name: str, show_tmdb_ids: str) -> Optional[int]:
        show_ids = [sid.strip() for sid in show_tmdb_ids.split(',') if sid.strip().isdigit()]
        if not show_ids:
            return None
        self.stats['search_attempts'] += 1
        print(f"      🔍 Searching TMDb for '{person_name}'")
        candidates = self.search_tmdb_person(person_name)
        for candidate in candidates:
            person_id = candidate.get('id')
            candidate_name = candidate.get('name') or candidate.get('original_name') or ''
            if not person_id or not names_match(person_name, candidate_name):
                continue
            print(f"      👤 Candidate match '{candidate_name}' (ID {person_id}) – checking credits")
            if self.person_on_any_show(person_id, show_ids[: self.shows_limit]):
                print(f"      🎯 Candidate appears on one of the target shows")
                return int(person_id)
        return None

    def queue_update(self, row_number: int, tmdb_id: int) -> None:
        if self.dry_run:
            return
        update_range = f"RealiteaseInfo!{self.target_column_letter}{row_number}"
        self.pending_updates.append({'range': update_range, 'values': [[str(tmdb_id)]]})
        if len(self.pending_updates) >= self.batch_size:
            self.flush_updates()

    def flush_updates(self) -> None:
        if not self.pending_updates or self.dry_run:
            self.pending_updates.clear()
            return
        body = {'valueInputOption': 'RAW', 'data': self.pending_updates}
        try:
            self.worksheet.spreadsheet.values_batch_update(body)
            print(f"📤 Wrote {len(self.pending_updates)} TMDb IDs to sheet")
            time.sleep(POST_BATCH_DELAY)
        except Exception as exc:
            print(f"❌ Failed to batch update: {exc}")
        finally:
            self.pending_updates.clear()

    def process_rows(self, start_row: Optional[int], end_row: Optional[int], limit: Optional[int], reverse: bool = False) -> None:
        all_rows = self.fetch_all_rows()
        total_rows = len(all_rows)
        print(f"📊 Sheet contains {total_rows} rows (including header)")
        if start_row is None:
            start_row = total_rows if reverse else 2
        if end_row is None:
            end_row = 2 if reverse else total_rows
        indexed_rows: List[Tuple[int, List[str]]] = [(idx + 1, row) for idx, row in enumerate(all_rows)]
        if reverse:
            indexed_rows = list(reversed(indexed_rows))
        processed = 0
        for row_number, row in indexed_rows:
            if row_number == 1:
                continue
            if reverse:
                if row_number > start_row:
                    continue
                if end_row and row_number < end_row:
                    break
            else:
                if row_number < start_row:
                    continue
                if end_row and row_number > end_row:
                    break
            if limit and processed >= limit:
                break
            cast_name = get_realitease_value(row, 'cast_name')
            if not cast_name:
                continue
            cast_tmdb_id = get_realitease_value(row, 'cast_tmdb_id')
            show_tmdb_ids = get_realitease_value(row, 'show_tmdb_ids')
            self.stats['rows_considered'] += 1
            if cast_tmdb_id:
                self.stats['skipped_existing'] += 1
                continue
            if not show_tmdb_ids:
                self.stats['skipped_no_shows'] += 1
                continue
            processed += 1
            print(f"\n{'=' * 60}")
            print(f"🎭 Row {row_number}: {cast_name}")
            tmdb_id = self.find_tmdb_id_by_name(cast_name, show_tmdb_ids)
            if tmdb_id:
                print(f"   ✅ TMDb ID {tmdb_id} will be recorded")
                self.stats['matched'] += 1
                self.queue_update(row_number, tmdb_id)
                self.maybe_log_interval_progress()
            else:
                print("   ⚠️ No TMDb match found")
                self.stats['not_found'] += 1
        self.flush_updates()

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("📊 Step 4 Summary: TMDb Backfill")
        for key, value in self.stats.items():
            print(f"   • {key.replace('_', ' ').title()}: {value}")
        if self.dry_run:
            print("\nℹ️ Dry-run mode: no spreadsheet updates were written.")

    def maybe_log_interval_progress(self) -> None:
        now = time.time()
        if now - self.interval_start_time >= 300:
            interval_total = self.stats['matched'] - self.interval_start_matched
            print(f"⏱️ 5-minute progress: {interval_total} new TMDb IDs (total {self.stats['matched']})")
            self.interval_start_time = now
            self.interval_start_matched = self.stats['matched']


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Step 4: Backfill TMDb cast IDs in RealiteaseInfo by TMDb show cast lookups.',
    )
    parser.add_argument('--start-row', type=int, help='Row to start processing (default: top when forward, bottom when reverse)')
    parser.add_argument('--end-row', type=int, help='Row to stop processing (inclusive)')
    parser.add_argument('--limit', type=int, help='Maximum number of rows to attempt')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE, help='Number of updates per batch write')
    parser.add_argument('--shows-per-cast', type=int, default=DEFAULT_SHOW_LIMIT, help='Max TMDb shows to check per cast member')
    parser.add_argument('--dry-run', action='store_true', help='Run without writing updates to the sheet')
    parser.add_argument('--reverse', action='store_true', help='Process rows from bottom to top')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    backfiller = RealiteaseTMDbCastBackfiller(
        batch_size=args.batch_size,
        shows_limit=args.shows_per_cast,
        dry_run=args.dry_run,
    )
    backfiller.process_rows(
        start_row=args.start_row,
        end_row=args.end_row,
        limit=args.limit,
        reverse=args.reverse,
    )
    backfiller.print_summary()


if __name__ == '__main__':
    main()

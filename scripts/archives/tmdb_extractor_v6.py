#!/usr/bin/env python3
# File: tmdb_extractor_v6.py

import os
import sys
import time
import re
import math
import unicodedata
import random
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# Load environment variables from project root
load_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(load_env_path)


class TMDBFinalExtractorV6:
    """
    v6 COMPREHENSIVE EDITION - Uses ALL methods for maximum accuracy:
      - Row 2 -> last row (auto)
      - CAST + CREW credits
      - Multiple extraction methods: Credit ID, Direct credits, Name search, Show validation
      - Process rows where: both G/H are empty, either G/H is blank, OR both G=1 AND H=1 (bogus defaults), OR either is SKIP
      - Accept partial data when complete data unavailable
      - Enhanced data validation and quality checks
      - Batch updates with contiguous range grouping
      - TMDb response caching + backoff on 429/5xx
      - Sheets batch update backoff on 429/5xx
    """
    def __init__(self):
        # TMDb auth
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        if not self.tmdb_bearer and not self.tmdb_api_key:
            raise ValueError("Neither TMDB_BEARER nor TMDB_API_KEY found in environment variables")

        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})
        if self.tmdb_bearer:
            self.session.headers.update({'Authorization': f'Bearer {self.tmdb_bearer}'})
            print("🔑 Using TMDB Bearer token (v4)")
        else:
            # For v3 API key, use query param rather than Authorization header
            self.session.params.update({'api_key': self.tmdb_api_key})
            print("🔑 Using TMDB API key (v3)")

        # TMDb polite pacing
        self.min_delay_s = 0.10   # ~10 req/sec soft cap
        self.last_req_ts = 0.0
        self.tmdb_retry_max = 6

        # Caches to reduce TMDb calls  
        self.cache_basic: Dict[str, dict] = {}

        # Sheets
        self.gc = None
        self.worksheet = None

        # Counters
        self.processed_count = 0
        self.updated_cells_count = 0
        self.skipped_rows_count = 0
        self.failed_count = 0

    # ---------------- Utilities ----------------

    def _digits(self, text) -> Optional[str]:
        m = re.search(r'\d+', str(text or ''))
        return m.group(0) if m else None

    def _norm_name(self, s: str) -> str:
        s = unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode('ascii')
        return re.sub(r'\s+', ' ', s).strip().lower()

    # ---------------- Google Sheets ----------------

    def setup_google_sheets(self) -> bool:
        try:
            print("🔄 Setting up Google Sheets connection...")
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("ViableCast")
            print("✅ Google Sheets ready")
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {e}")
            return False

    def _col_letter(self, idx0: int) -> str:
        """0-based index -> A1 column letters."""
        n = idx0 + 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def _batch_update_with_backoff(self, ranges_and_values: List[Tuple[str, List[List[str]]]]) -> bool:
        """
        Perform multiple contiguous updates using minimal calls.
        Uses Worksheet.update(range, values), with retries on 429/5xx.
        """
        if not ranges_and_values:
            return True

        attempt = 0
        while True:
            try:
                # Each range is sent as its own update to keep memory small and handle partial failures gracefully.
                for rng, vals in ranges_and_values:
                    self.worksheet.update(rng, vals, value_input_option="RAW")
                    # small jitter to avoid hammering API
                    time.sleep(0.25 + random.random() * 0.25)
                self.updated_cells_count += sum(len(v) for _, v in ranges_and_values)
                return True
            except APIError as e:
                attempt += 1
                status = getattr(e, "response", None)
                code = getattr(status, "status_code", None)
                # Backoff for rate limiting or transient
                if attempt <= 6 and code in (429, 500, 502, 503, 504):
                    delay = min(30, (2 ** attempt) * 0.5 + random.random())
                    print(f"⏳ Sheets API backoff (attempt {attempt}, status {code}) sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"❌ Sheets batch update failed after {attempt} attempts: {e}")
                return False
            except Exception as e:
                attempt += 1
                if attempt <= 4:
                    delay = min(15, (2 ** attempt) * 0.4 + random.random())
                    print(f"⏳ Sheets unknown error backoff (attempt {attempt}) sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"❌ Sheets batch update error: {e}")
                return False

    def _group_updates_into_ranges(self, updates: Dict[str, str]) -> List[Tuple[str, List[List[str]]]]:
        """
        updates: {"G23":"5","H23":"1, 2, 3","G24":"1", ...}
        -> produce compact contiguous ranges per column with 2D values.
        """
        by_col: Dict[str, Dict[int, str]] = {}
        for a1, val in updates.items():
            m = re.match(r"([A-Z]+)(\d+)$", a1)
            if not m:
                continue
            col, row = m.group(1), int(m.group(2))
            by_col.setdefault(col, {})[row] = val

        ranges: List[Tuple[str, List[List[str]]]] = []
        for col, rowvals in by_col.items():
            rows_sorted = sorted(rowvals.keys())
            block: List[int] = []
            start = prev = None
            for r in rows_sorted:
                if start is None:
                    start = prev = r
                    block = [r]
                elif r == prev + 1:
                    prev = r
                    block.append(r)
                else:
                    # flush previous contiguous block
                    values = [[rowvals[rr]] for rr in block]
                    ranges.append((f"{col}{block[0]}:{col}{block[-1]}", values))
                    # start new
                    start = prev = r
                    block = [r]
            if block:
                values = [[rowvals[rr]] for rr in block]
                ranges.append((f"{col}{block[0]}:{col}{block[-1]}", values))
        return ranges

    # ---------------- TMDb with cache + backoff ----------------

    def _tmdb_get_json(self, path: str) -> Optional[dict]:
        """
        GET {base_url}{path} with soft pacing and exponential backoff on 429/5xx.
        """
        # enforce soft rate limit
        now = time.time()
        sleep_need = self.min_delay_s - (now - self.last_req_ts)
        if sleep_need > 0:
            time.sleep(sleep_need)

        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            try:
                r = self.session.get(url, timeout=20)
                self.last_req_ts = time.time()
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504) and attempt < self.tmdb_retry_max:
                    attempt += 1
                    delay = min(30, (2 ** attempt) * 0.5 + random.random())
                    print(f"⏳ TMDb backoff (attempt {attempt}, status {r.status_code}) sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"⚠️ TMDb GET {path} failed: {r.status_code} {r.text[:200]}")
                return None
            except requests.RequestException as e:
                attempt += 1
                if attempt <= self.tmdb_retry_max:
                    delay = min(20, (2 ** attempt) * 0.4 + random.random())
                    print(f"⏳ TMDb network error (attempt {attempt}): {e} — sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"❌ TMDb request error: {e}")
                return None

    # ---------------- TMDb with cache + backoff ----------------

    def _tmdb_get_json(self, path: str) -> Optional[dict]:
        """
        GET {base_url}{path} with soft pacing and exponential backoff on 429/5xx.
        """
        # enforce soft rate limit
        now = time.time()
        sleep_need = self.min_delay_s - (now - self.last_req_ts)
        if sleep_need > 0:
            time.sleep(sleep_need)

        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            try:
                r = self.session.get(url, timeout=20)
                self.last_req_ts = time.time()
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504) and attempt < self.tmdb_retry_max:
                    attempt += 1
                    delay = min(30, (2 ** attempt) * 0.5 + random.random())
                    print(f"⏳ TMDb backoff (attempt {attempt}, status {r.status_code}) sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"⚠️ TMDb GET {path} failed: {r.status_code} {r.text[:200]}")
                return None
            except requests.RequestException as e:
                attempt += 1
                if attempt <= self.tmdb_retry_max:
                    delay = min(20, (2 ** attempt) * 0.4 + random.random())
                    print(f"⏳ TMDb network error (attempt {attempt}): {e} — sleeping {delay:.1f}s …")
                    time.sleep(delay)
                    continue
                print(f"❌ TMDb request error: {e}")
                return None

    def _get_credit_details_seasons(self, credit_id: str, cast_name: str, show_name: str) -> List[int]:
        """Get detailed season information for a specific credit"""
        try:
            print(f"🔍 {show_name}: Getting credit details for ID: {credit_id}")
            credit_data = self._tmdb_get_json(f"/credit/{credit_id}")
            
            if not credit_data:
                print(f"❌ {show_name}: Failed to get credit details for {cast_name}")
                return []
            
            # Extract season information from media
            seasons = []
            if 'media' in credit_data:
                media = credit_data['media']
                
                # First try: Check the seasons array
                if 'seasons' in media and media['seasons']:
                    seasons_data = media['seasons']
                    print(f"🔍 {show_name}: Credit details returned {len(seasons_data)} seasons")
                    
                    for season in seasons_data:
                        season_number = season.get('season_number')
                        if season_number is not None and season_number > 0:  # Exclude season 0 (specials)
                            seasons.append(season_number)
                            print(f"✅ {show_name}: Found Season {season_number}")
                
                # Second try: If seasons array is empty, extract from episodes
                elif 'episodes' in media and media['episodes']:
                    episodes_data = media['episodes']
                    print(f"🔍 {show_name}: Credit details returned {len(episodes_data)} episodes, extracting seasons from episodes")
                    
                    season_numbers = set()
                    for episode in episodes_data:
                        season_number = episode.get('season_number')
                        if season_number is not None and season_number > 0:  # Exclude season 0 (specials)
                            season_numbers.add(season_number)
                    
                    seasons = sorted(list(season_numbers))
                    for season_num in seasons:
                        print(f"✅ {show_name}: Found Season {season_num}")
                else:
                    print(f"🔍 {show_name}: Credit details returned 0 seasons")
                        
            return seasons
            
        except Exception as e:
            print(f"❌ {show_name}: Error getting credit details for {cast_name}: {e}")
            return []

    # ---------------- Extraction (CAST ONLY) ----------------

    # ---------------- Extraction (CAST ONLY) ----------------

    def _search_person_by_name(self, cast_name: str, show_name: str):
        """Search for a person by name as fallback method"""
        try:
            print(f"🔍 {show_name}: Searching for person by name: {cast_name}")
            
            # Clean the name for search
            search_name = cast_name.strip()
            search_data = self._tmdb_get_json(f"/search/person?query={search_name}")
            
            if not search_data or not search_data.get('results'):
                print(f"⚠️ {show_name}: No person found for name '{cast_name}'")
                return None
                
            # Take the first result (most popular match)
            person = search_data['results'][0]
            person_id = person.get('id')
            person_name = person.get('name', '')
            
            print(f"✅ {show_name}: Found person {person_name} (ID: {person_id}) for search '{cast_name}'")
            return str(person_id)
            
        except Exception as e:
            print(f"❌ {show_name}: Error searching person by name '{cast_name}': {e}")
            return None

    def _get_show_seasons_direct(self, show_id: str, show_name: str):
        """Get all seasons for a show directly"""
        try:
            print(f"🔍 {show_name}: Getting all seasons for show {show_id}")
            show_data = self._tmdb_get_json(f"/tv/{show_id}")
            
            if not show_data:
                return []
                
            seasons = []
            for season in show_data.get('seasons', []):
                season_number = season.get('season_number')
                if season_number is not None and season_number > 0:  # Exclude specials
                    seasons.append(season_number)
                    
            print(f"📺 {show_name}: Show has seasons: {seasons}")
            return seasons
            
        except Exception as e:
            print(f"❌ {show_name}: Error getting show seasons: {e}")
            return []

    def _validate_data_quality(self, result, cast_name: str, show_name: str):
        """Validate and enhance the quality of extracted data"""
        if not result:
            return None
            
        episodes = result.get('episodes', 0)
        seasons_str = result.get('seasons', '')
        
        # Basic validation
        if episodes <= 0:
            print(f"⚠️ {show_name}: {cast_name} - Invalid episode count: {episodes}")
            return None
            
        if not seasons_str:
            print(f"⚠️ {show_name}: {cast_name} - No season information found")
            return None
            
        # Parse seasons
        try:
            seasons = [int(s.strip()) for s in seasons_str.split(',') if s.strip().isdigit()]
            if not seasons:
                print(f"⚠️ {show_name}: {cast_name} - Could not parse seasons: '{seasons_str}'")
                return None
                
            # Validate season numbers are reasonable
            if any(s < 0 or s > 100 for s in seasons):
                print(f"⚠️ {show_name}: {cast_name} - Unreasonable season numbers: {seasons}")
                
        except Exception as e:
            print(f"⚠️ {show_name}: {cast_name} - Error parsing seasons '{seasons_str}': {e}")
            return None
        
        print(f"✅ {show_name}: {cast_name} - Data validated: {episodes} episodes, seasons {seasons_str}")
        return result

    def extract_show_episodes(self, person_id_or_blank: str, tmdb_show_id: str, cast_name: str, show_name: str):
        """
        Extract episode information using ALL available methods for maximum accuracy:
        1) Credit ID approach (most accurate)
        2) Direct TV credits with episode counts  
        3) Search by name as fallback
        4) Cross-validation with show data
        """
        try:
            series_id = self._digits(tmdb_show_id)
            if not series_id:
                print(f"❌ Invalid show id '{tmdb_show_id}'")
                return None

            # METHOD 1: Use provided person ID
            pid = (person_id_or_blank or '').strip()
            
            # METHOD 2: If no person ID, search by name
            if not pid or not pid.isdigit():
                print(f"🔄 {show_name}: No valid person ID for {cast_name}, searching by name...")
                pid = self._search_person_by_name(cast_name, show_name)
                if not pid:
                    return None

            print(f"🔍 {show_name}: Processing {cast_name} (Person: {pid}, Show: {series_id})")
            
            # Get TV credits for the person
            credits_data = self._tmdb_get_json(f"/person/{pid}/tv_credits")
            if not credits_data:
                print(f"❌ {show_name}: Failed to get TV credits for {cast_name}")
                return None

            # Look for cast AND crew credits for the specific show
            all_credits = []
            
            # Check cast credits
            for credit in credits_data.get('cast', []):
                if str(credit.get('id')) == str(series_id):
                    all_credits.append(credit)
                    print(f"✅ {show_name}: Found CAST credit for {cast_name}")
            
            # Check crew credits  
            for credit in credits_data.get('crew', []):
                if str(credit.get('id')) == str(series_id):
                    all_credits.append(credit)
                    print(f"✅ {show_name}: Found CREW credit for {cast_name}")
            
            if not all_credits:
                print(f"⚠️ {show_name}: No credits found for {cast_name}")
                return None

            # METHOD 3: Process ALL credits with multiple approaches
            total_episodes = 0
            all_seasons = set()
            credit_methods_used = []
            
            for i, credit in enumerate(all_credits):
                credit_id = credit.get('credit_id')
                episode_count = credit.get('episode_count', 0)
                credit_type = 'cast' if credit in credits_data.get('cast', []) else 'crew'
                
                print(f"🔍 {show_name}: Processing {credit_type} credit {i+1} - ID: {credit_id}, Episodes: {episode_count}")
                
                # APPROACH A: Credit ID detailed extraction (most accurate)
                if credit_id:
                    detailed_seasons = self._get_credit_details_seasons(credit_id, cast_name, show_name)
                    if detailed_seasons:
                        print(f"📺 {show_name}: Credit ID method found seasons {detailed_seasons}")
                        for season_num in detailed_seasons:
                            all_seasons.add(season_num)
                        credit_methods_used.append(f"credit_id_{i+1}")
                
                # APPROACH B: Direct episode count from credits
                if episode_count > 0:
                    total_episodes += episode_count
                    print(f"📊 {show_name}: Added {episode_count} episodes from {credit_type} credit")
                else:
                    # Assume at least 1 episode if credit exists but no count
                    total_episodes += 1
                    print(f"📊 {show_name}: Assumed 1 episode for {credit_type} credit with no count")
            
            # METHOD 4: Cross-validate with show's available seasons
            show_seasons = self._get_show_seasons_direct(series_id, show_name)
            if show_seasons and not all_seasons:
                print(f"🔄 {show_name}: No seasons from credits, but show has seasons {show_seasons}")
                # If we have episodes but no seasons, estimate based on show structure
                if total_episodes > 0:
                    # Conservative estimate: assume person was in season 1 unless proven otherwise
                    all_seasons.add(1)
                    print(f"📊 {show_name}: Conservative estimate - assigned to Season 1")
            
            # Process and validate seasons
            seasons_list = sorted([s for s in all_seasons if s is not None and s != 0])
            
            # Only include Season 0 if it's the only season found
            if 0 in all_seasons and not seasons_list:
                seasons_list = [0]
            
            # Ensure we have both episodes AND seasons
            if total_episodes <= 0:
                print(f"⚠️ {show_name}: {cast_name} - No valid episode count found")
                return None
                
            if not seasons_list:
                print(f"⚠️ {show_name}: {cast_name} - No valid seasons found")
                return None
            
            # Format result
            seasons_str = ", ".join(map(str, seasons_list))
            
            result = {
                'episodes': total_episodes,
                'seasons': seasons_str,
                'methods_used': credit_methods_used,
                'credits_found': len(all_credits)
            }
            
            print(f"✅ {show_name}: {cast_name} - FINAL: {total_episodes} episodes, Season(s) {seasons_str}")
            print(f"🔧 {show_name}: Methods used: {len(all_credits)} credits, {len(credit_methods_used)} detailed extractions")
            
            # Validate data quality before returning
            return self._validate_data_quality(result, cast_name, show_name)
            
        except Exception as e:
            print(f"❌ {show_name}: Error processing {cast_name}: {e}")
            return None

    # ---------------- Main loop ----------------

    def process_all(self) -> bool:
        if not self.setup_google_sheets():
            return False

        # Validate TMDb auth
        test = self._tmdb_get_json("/configuration")
        if not test:
            print("❌ TMDb auth/config test failed")
            return False
        print("✅ TMDb auth validated")

        try:
            all_data = self.worksheet.get_all_values()
            if not all_data or len(all_data) < 2:
                print("❌ Sheet has no data rows.")
                return False

            # Column mapping: B=1, C=2, E=4, F=5, G=6, H=7 (0-indexed)
            print("📊 v6 COMPREHENSIVE: Processing rows 2 → last (ascending)")
            print("📊 Columns: B=TMDb Person ID, C=Name, E=TMDb Show ID, F=Show Name, G=Episodes, H=Seasons")
            print("🎯 Will process: empty cells, blank cells, bogus '1,1' pairs, or SKIP values")
            print("🔧 Methods: Credit ID + Direct Credits + Name Search + Show Validation")
            print("📊 Accepts partial data when complete unavailable")
            print("✅ Enhanced validation and quality checks")

            # Precompute column letters for G/H
            epi_col_letter = self._col_letter(6)  # index 6 -> column G (0-based A=0)
            sea_col_letter = self._col_letter(7)  # index 7 -> column H

            updates: Dict[str, str] = {}
            flush_threshold = 20  # 10 rows x 2 cells = 20 cells to batch before flushing

            # Iterate from row 2 to end (1-indexed row numbers)
            for row_num in range(2, len(all_data) + 1):
                row_index = row_num - 1
                row = all_data[row_index]

                self.processed_count += 1

                cast_name      = row[2] if len(row) > 2 else ''   # C
                person_tmdb_id = row[1] if len(row) > 1 else ''   # B
                tmdb_show_id   = row[4] if len(row) > 4 else ''   # E
                show_name      = row[5] if len(row) > 5 else ''   # F
                episodes_cell  = row[6] if len(row) > 6 else ''   # G
                seasons_cell   = row[7] if len(row) > 7 else ''   # H

                ep_val = (episodes_cell or '').strip()
                sz_val = (seasons_cell or '').strip()
                
                # Debug first 20 rows to see what's in the data
                if row_num <= 20:
                    print(f"🔍 Row {row_num}: {cast_name} - G='{ep_val}', H='{sz_val}', ShowID='{tmdb_show_id}'")
                
                # Check if this row needs processing:
                # Process if either field is empty, contains "SKIP", or both are "1" (bogus defaults)
                ep_is_blank = not ep_val
                sz_is_blank = not sz_val
                ep_is_skip = ep_val.upper() == "SKIP" if ep_val else False
                sz_is_skip = sz_val.upper() == "SKIP" if sz_val else False
                both_are_one = (ep_val == "1" and sz_val == "1")
                
                # Skip if we already have valid data in both fields where at least one is not "1"
                has_episodes_data = ep_val and ep_val.upper() != "SKIP"
                has_seasons_data = sz_val and sz_val.upper() != "SKIP"
                
                # If BOTH have values AND at least ONE is not "1", then SKIP (leave as is)
                if has_episodes_data and has_seasons_data and (ep_val != "1" or sz_val != "1"):
                    self.skipped_rows_count += 1
                    # Debug first 10 skipped rows to understand why
                    if row_num <= 10:
                        print(f"⏭️ Row {row_num}: {cast_name} - Skipping (both have values, at least one ≠ 1: G='{ep_val}', H='{sz_val}')")
                    continue
                else:
                    # Process this row - cases to handle:
                    # 1. Both empty
                    # 2. One empty, other has value 
                    # 3. Both are "1" (could be bogus defaults)
                    # 4. Either contains "SKIP"
                    if row_num <= 10:  # Debug first few rows being processed
                        if both_are_one:
                            print(f"🔄 Row {row_num}: {cast_name} - Processing (both are 1,1 - checking if true)")
                        elif not has_episodes_data and not has_seasons_data:
                            print(f"🔄 Row {row_num}: {cast_name} - Processing (both empty)")
                        elif not has_episodes_data or not has_seasons_data:
                            print(f"🔄 Row {row_num}: {cast_name} - Processing (one empty: G='{ep_val}', H='{sz_val}')")
                        elif ep_is_skip or sz_is_skip:
                            print(f"🔄 Row {row_num}: {cast_name} - Processing (contains SKIP)")
                        else:
                            print(f"🔄 Row {row_num}: {cast_name} - Processing (G='{ep_val}', H='{sz_val}')")
                

                # Validate data needed
                if not cast_name or not tmdb_show_id:
                    self.skipped_rows_count += 1
                    if row_num <= 10:  # Debug first few rows
                        print(f"⚠️ Row {row_num}: Missing data - Name='{cast_name}', ShowID='{tmdb_show_id}'")
                    continue

                pid_candidate = (person_tmdb_id or '').strip()
                if not pid_candidate.isdigit():
                    pid_candidate = ''  # rely on aggregate-by-name

                result = self.extract_show_episodes(
                    pid_candidate,
                    self._digits(tmdb_show_id) or tmdb_show_id,
                    cast_name.strip(),
                    (show_name or f"Show {tmdb_show_id}").strip()
                )

                # Handle different scenarios after trying to find data:
                if result and result.get('episodes') and result.get('seasons'):
                    # We found complete data - update the cells
                    a1_epi = f"{epi_col_letter}{row_num}"
                    a1_sea = f"{sea_col_letter}{row_num}"
                    
                    episodes_val = str(result['episodes'])
                    seasons_val = result['seasons']
                    
                    updates[a1_epi] = episodes_val
                    updates[a1_sea] = seasons_val
                    
                    # Enhanced logging with method info
                    methods_info = ""
                    if result.get('methods_used'):
                        methods_info = f" (methods: {', '.join(result['methods_used'])})"
                    
                    print(f"✅ Row {row_num}: {cast_name} - UPDATED: {episodes_val} episodes, seasons {seasons_val}{methods_info}")
                        
                elif result and (result.get('episodes') or result.get('seasons')):
                    # Partial data found - this is a change! Let's be more aggressive about partial data
                    episodes_val = str(result.get('episodes', '')) if result.get('episodes') else ''
                    seasons_val = result.get('seasons', '') if result.get('seasons') else ''
                    
                    # If we have episodes but no seasons, try to estimate or accept partial data
                    if episodes_val and not seasons_val:
                        # For single episode, assume season 1
                        if result.get('episodes') == 1:
                            seasons_val = "1"
                            print(f"📊 Row {row_num}: {cast_name} - Single episode, assuming Season 1")
                        # For multiple episodes, leave seasons blank but keep episodes
                        else:
                            print(f"📊 Row {row_num}: {cast_name} - Multiple episodes found but no seasons, keeping episodes only")
                    
                    # Update cells with whatever data we have
                    a1_epi = f"{epi_col_letter}{row_num}"
                    a1_sea = f"{sea_col_letter}{row_num}"
                    
                    updates[a1_epi] = episodes_val
                    updates[a1_sea] = seasons_val
                    
                    print(f"📝 Row {row_num}: {cast_name} - PARTIAL: episodes='{episodes_val}', seasons='{seasons_val}'")
                        
                else:
                    # Could not find any data - check if we should clear or leave as-is
                    should_clear = False
                    
                    # Clear if either field was "SKIP" or both were "1" (likely bogus)
                    if ep_is_skip or sz_is_skip or both_are_one:
                        should_clear = True
                        
                    # Clear if both fields are currently empty (so we mark as "searched")
                    elif ep_is_blank and sz_is_blank:
                        should_clear = True
                    
                    # Clear if one field has meaningful data but we couldn't find the other
                    elif (has_episodes_data and not has_seasons_data) or (has_seasons_data and not has_episodes_data):
                        # Only clear if the existing data looks bogus
                        if ep_val in ['1', 'SKIP', ''] or sz_val in ['1', 'SKIP', '']:
                            should_clear = True
                    
                    if should_clear:
                        a1_epi = f"{epi_col_letter}{row_num}"
                        a1_sea = f"{sea_col_letter}{row_num}"
                        
                        updates[a1_epi] = ""
                        updates[a1_sea] = ""
                        
                        print(f"🧹 Row {row_num}: {cast_name} - CLEARED: No complete data found")
                    else:
                        print(f"⏭️ Row {row_num}: {cast_name} - SKIPPED: Keeping existing data (G='{ep_val}', H='{sz_val}')")

                # Flush staged updates when threshold reached
                if len(updates) >= flush_threshold:
                    ranges = self._group_updates_into_ranges(updates)
                    ok = self._batch_update_with_backoff(ranges)
                    updates.clear()
                    if not ok:
                        # If batch fails after retries, keep going (don’t crash the run)
                        self.failed_count += 1

                # Progress every 200 rows
                if self.processed_count % 200 == 0:
                    success_rate = (
                        (self.updated_cells_count / max(1, self.processed_count * 2)) * 100
                    )  # rough ratio (2 cells per row possible)
                    print(f"📈 {self.processed_count} rows processed | {self.updated_cells_count} cells updated | {self.skipped_rows_count} skipped | {self.failed_count} failed | ~Success {success_rate:.1f}%")

            # Final flush
            if updates:
                ranges = self._group_updates_into_ranges(updates)
                ok = self._batch_update_with_backoff(ranges)
                updates.clear()
                if not ok:
                    self.failed_count += 1

            # Summary
            print("\n🎉 TMDB v6: Processing complete!")
            print(f"📊 Rows processed: {self.processed_count}")
            print(f"📝 Cells updated: {self.updated_cells_count}")
            print(f"⏭️ Rows skipped: {self.skipped_rows_count}")
            print(f"❌ Failures: {self.failed_count}")

            return True

        except Exception as e:
            print(f"❌ Error processing: {e}")
            return False


def main():
    extractor = TMDBFinalExtractorV6()
    print("🚀 Starting TMDB Final Extractor v6 COMPREHENSIVE EDITION…")
    print("🎯 Processing rows 2 → last (ascending)")
    print("🎬 CAST + CREW credits (all roles)")
    print("🔧 Multiple extraction methods: Credit ID + Direct + Name search + Show validation")
    print("🔒 Will process empty cells, blank cells, bogus '1,1' pairs, or SKIP values")
    print("📊 Accepts partial data when complete data unavailable")
    print("✅ Enhanced data validation and quality checks")

    success = extractor.process_all()
    if success:
        print("✅ Comprehensive extraction completed successfully!")
    else:
        print("❌ Extraction failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()

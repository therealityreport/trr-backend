#!/usr/bin/env python3

import os
import re
import time
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

# Constants
IMDB_BASE = "https://www.imdb.com"
DELAY_REQUEST = 1.0  # Delay between requests
DELAY_SEASON = 2.0   # Delay between seasons

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

@dataclass
class Episode:
    tconst: str
    season: int
    episode: int
    air_date: str = ""

def as_int(s: str) -> Optional[int]:
    """Convert string to int, return None if invalid."""
    if not s:
        return None
    try:
        return int(s.strip())
    except (ValueError, TypeError):
        return None

def tt_from_href(href: str) -> Optional[str]:
    """Extract tt ID from href like '/title/tt1234567/'."""
    if not href:
        return None
    m = re.search(r"/title/(tt\d+)", href)
    return m.group(1) if m else None

class WWHLIMDbFast:
    def __init__(self):
        # WWHL show identifiers
        self.imdb_series_id = "tt2057880"  # WWHL IMDb ID
        self.tmdb_show_id = "22980"       # WWHL TMDb ID
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Google Sheets
        self.gc = None
        self.wwhl_ws = None
        self.castinfo_ws = None
        self.realitease_ws = None
        
        # Mappings
        self.imdb_to_tmdb_person: Dict[str, str] = {}
        self.tmdb_to_imdb_person: Dict[str, str] = {}
        self.realitease_tmdb_people: Set[str] = set()
        self.realitease_imdb_people: Set[str] = set()
        
        log.info("🎬 WWHL Fast IMDb Fetcher initialized")

    def setup_google_sheets(self) -> bool:
        """Connect to Google Sheets."""
        try:
            # Load service account credentials
            key_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'keys', 'trr-backend-df2c438612e1.json')
            
            if not os.path.exists(key_file_path):
                log.error("❌ Service account key file not found")
                return False
            
            # Setup credentials and connect
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            self.gc = gspread.authorize(credentials)
            
            # Open spreadsheet
            spreadsheet_name = 'Realitease2025Data'
            self.spreadsheet = self.gc.open(spreadsheet_name)
            
            # Get worksheets
            self.wwhl_ws = self.spreadsheet.worksheet('WWHLinfo')
            self.castinfo_ws = self.spreadsheet.worksheet('CastInfo')
            self.realitease_ws = self.spreadsheet.worksheet('RealiteaseInfo')
            
            log.info("✅ Connected to Google Sheets")
            return True
            
        except Exception as e:
            log.error(f"❌ Failed Google Sheets setup: {e}")
            import traceback
            traceback.print_exc()
            return False

    def load_castinfo_mappings(self):
        """Build IMDb → TMDb person id mapping from CastInfo."""
        vals = self.castinfo_ws.get_all_values()
        if not vals or len(vals) < 2:
            log.warning("CastInfo is empty.")
            return

        headers = [h.strip() for h in vals[0]]
        
        # Find exact column matches
        h_tmdb = None
        h_imdb = None
        for i, h in enumerate(headers):
            if h == "TMDb CastID":
                h_tmdb = i
            elif h == "Cast IMDbID":
                h_imdb = i

        if h_tmdb is None or h_imdb is None:
            log.warning(f"CastInfo missing required columns. TMDb: {h_tmdb}, IMDb: {h_imdb}")
            return

        for row in vals[1:]:
            if len(row) <= max(h_tmdb, h_imdb):
                continue
            tmdb = row[h_tmdb].strip()
            imdb = row[h_imdb].strip()
            
            # Ensure IMDb ID has nm prefix
            if imdb and not imdb.startswith("nm"):
                imdb = f"nm{imdb}"
            if imdb and tmdb:
                self.imdb_to_tmdb_person[imdb] = tmdb
                self.tmdb_to_imdb_person[tmdb] = imdb

        log.info(f"🔗 IMDb→TMDb person mappings loaded: {len(self.imdb_to_tmdb_person)}")

    def load_realitease_membership(self):
        """Load TMDb IDs from RealiteaseInfo to identify reality TV cast."""
        vals = self.realitease_ws.get_all_values()
        if not vals or len(vals) < 2:
            log.warning("RealiteaseInfo is empty.")
            return

        headers = [h.strip().lower() for h in vals[0]]
        h_tmdb = next((i for i, h in enumerate(headers) if "tmdb" in h and "id" in h), None)

        if h_tmdb is None:
            log.warning("RealiteaseInfo missing TMDb ID column.")
            return

        for row in vals[1:]:
            if len(row) > h_tmdb:
                tmdb_id = row[h_tmdb].strip()
                if tmdb_id:
                    self.realitease_tmdb_people.add(tmdb_id)
                    # Also add corresponding IMDb ID if available
                    imdb_id = self.tmdb_to_imdb_person.get(tmdb_id)
                    if imdb_id:
                        self.realitease_imdb_people.add(imdb_id)

        log.info(f"👥 Realitease members (TMDb): {len(self.realitease_tmdb_people)} | (IMDb): {len(self.realitease_imdb_people)}")

    def _ensure_headers(self):
        """Ensure WWHLinfo has the correct headers."""
        headers = [
            "TMDbID", "EpisodeMarker", "Season", "Episode", "AirDate",
            "GuestNames", "GuestStarTMDbIDs", "GuestStarIMDbIDs", "Cast_Source"
        ]
        
        vals = self.wwhl_ws.get_all_values()
        if not vals or vals[0] != headers:
            self.wwhl_ws.update("A1:I1", [headers])
            log.info("📋 Updated WWHLinfo headers")

    def _read_existing(self) -> Tuple[Dict[str, int], List[List[str]]]:
        """Return map of EpisodeMarker → row_number (1-based) and all data for quick updating."""
        data = self.wwhl_ws.get_all_values()
        idx = {}
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 2:
                continue
            marker = row[1].strip()
            if marker:
                idx[marker] = i
        return idx, data

    def get_available_seasons(self) -> List[int]:
        """Use known season range for WWHL."""
        # WWHL has been running since 2009, with 22+ seasons as of 2024
        seasons = list(range(1, 23))  # Seasons 1-22
        log.info(f"🗂️ Using season range 1-22 for WWHL")
        return seasons

    def get_season_episodes(self, season: int) -> List[Episode]:
        """Get episodes for a season by scraping IMDb."""
        url = f"{IMDB_BASE}/title/{self.imdb_series_id}/episodes?season={season}"
        
        try:
            response = self.session.get(url, timeout=25)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            episodes = []
            
            # Look for episode links with href="/title/tt..." and text containing "S{season}.E{episode}"
            episode_links = soup.find_all("a", href=re.compile(r"/title/tt\d+"))
            
            for link in episode_links:
                href = link.get("href")
                if not href:
                    continue
                    
                tconst = tt_from_href(href)
                if not tconst:
                    continue
                
                # Extract episode number from the link text or nearby text
                epno = None
                link_text = link.get_text(" ", strip=True)
                
                # Look for "S{season}.E{episode}" pattern in the link text
                pattern = rf"S{season}\.E(\d+)"
                m = re.search(pattern, link_text)
                if m:
                    epno = as_int(m.group(1))
                
                # If not found in link text, check parent element
                if epno is None and link.parent:
                    parent_text = link.parent.get_text(" ", strip=True)
                    m = re.search(pattern, parent_text)
                    if m:
                        epno = as_int(m.group(1))
                
                if not epno:
                    continue
                    
                # Try to find air date - look for date patterns near the episode
                air_date = ""
                parent = link.parent
                if parent:
                    # Look for date patterns in the parent element
                    parent_text = parent.get_text(" ", strip=True)
                    # Look for patterns like "Thu, Jun 16, 2009" or "Mon, Dec 3, 2009"
                    date_match = re.search(r"[A-Za-z]{3},\s+[A-Za-z]{3}\s+\d{1,2},\s+\d{4}", parent_text)
                    if date_match:
                        air_date = self._normalize_date(date_match.group(0))

                if tconst and epno:
                    episodes.append(Episode(tconst=tconst, season=season, episode=epno, air_date=air_date))
            
            # Remove duplicates by episode number
            seen = set()
            unique_episodes = []
            for ep in episodes:
                if ep.episode not in seen:
                    seen.add(ep.episode)
                    unique_episodes.append(ep)
            
            log.info(f"📺 Season {season}: found {len(unique_episodes)} unique episodes")
            time.sleep(DELAY_REQUEST)
            return unique_episodes
            
        except Exception as e:
            log.warning(f"⚠️ Failed to scrape season {season}: {e}")
            time.sleep(DELAY_REQUEST)
            return []

    def _normalize_date(self, s: str) -> str:
        """Convert common IMDb airdate patterns to YYYY-MM-DD when possible; else ''."""
        s = s.replace("\xa0", " ").strip()
        # Month mapping
        mm = {
            "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
            "jul": "07", "aug": "08", "sep": "09", "sept": "09", "oct": "10", "nov": "11", "dec": "12",
        }
        
        # "Thu, Jun 16, 2009" format
        m = re.match(r"[A-Za-z]{3},\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})", s)
        if m:
            mon, d, y = m.groups()
            mon = mm.get(mon.lower().strip("."), "")
            if mon:
                return f"{y}-{mon}-{d.zfill(2)}"
        
        return ""

    @staticmethod
    def _set_list(lst: List[str], idx: int, val: str):
        while len(lst) <= idx:
            lst.append("")
        lst[idx] = val

    def process(self):
        """Main processing function - focuses on episode structure only."""
        if not self.setup_google_sheets():
            return False

        self._ensure_headers()
        self.load_castinfo_mappings()
        self.load_realitease_membership()

        # Build existing index with cached data
        marker_to_row, all_data = self._read_existing()
        log.info(f"📋 Existing episode rows: {len(marker_to_row)}")

        # Get seasons
        seasons = self.get_available_seasons()
        log.info(f"🗂️ Processing {len(seasons)} seasons")

        new_rows: List[List[str]] = []
        rows_to_update: List[Tuple[int, List[str]]] = []

        for season in seasons:
            episodes = self.get_season_episodes(season)
            
            for ep in episodes:
                marker = f"S{ep.season}E{ep.episode}"
                
                # For now, skip guest processing to speed things up
                # Focus on episode structure and air dates
                
                # Prepare row data
                row = [
                    self.tmdb_show_id or "",     # TMDbID (show)
                    marker,                       # EpisodeMarker
                    str(ep.season),              # Season
                    str(ep.episode),             # Episode
                    ep.air_date or "",           # AirDate
                    "",                          # GuestNames (empty for now)
                    "",                          # GuestStarTMDbIDs (empty for now)
                    "",                          # GuestStarIMDbIDs (empty for now)
                    "NONE",                      # Cast_Source (default for now)
                ]

                if marker in marker_to_row:
                    # Update existing row if it has missing data
                    row_num = marker_to_row[marker]
                    existing = all_data[row_num - 1] if row_num <= len(all_data) else []
                    changed = False

                    def get(col_idx):
                        return existing[col_idx] if len(existing) > col_idx else ""

                    merged = existing[:]
                    
                    # Check and update missing fields
                    if not get(4) and row[4]:  # AirDate
                        self._set_list(merged, 4, row[4])
                        changed = True
                    if not get(0) and row[0]:  # TMDbID
                        self._set_list(merged, 0, row[0])
                        changed = True

                    if changed:
                        while len(merged) < 9:
                            merged.append("")
                        rows_to_update.append((row_num, merged[:9]))
                        log.info(f"♻️ Update {marker}: filled missing data")
                else:
                    new_rows.append(row)
                    log.info(f"➕ Queue new episode {marker}")
            
            # Add delay between seasons
            time.sleep(DELAY_SEASON)

        # Apply updates
        if rows_to_update:
            for row_num, payload in rows_to_update:
                rng = f"A{row_num}:I{row_num}"
                self.wwhl_ws.update(rng, [payload])
                time.sleep(0.1)
            log.info(f"✅ Updated {len(rows_to_update)} existing rows")

        if new_rows:
            self.wwhl_ws.append_rows(new_rows, value_input_option="RAW", table_range="A1:I1")
            log.info(f"🎉 Appended {len(new_rows)} new rows")

        log.info("🏁 Fast IMDb processing complete")
        return True

def main():
    fetcher = WWHLIMDbFast()
    ok = fetcher.process()
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

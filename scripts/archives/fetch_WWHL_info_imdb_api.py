#!/usr/bin/env python3

import os
import re
import time
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

# Constants
IMDB_API_BASE = "https://imdbapi.dev/titles"
DELAY_REQUEST = 0.5  # Delay between API requests

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

@dataclass
class Episode:
    tconst: str
    season: int
    episode: int
    air_date: str = ""

@dataclass
class Guest:
    name: str
    imdb_id: str

class WWHLIMDbAPIFetcher:
    def __init__(self):
        # WWHL show identifiers
        self.imdb_series_id = "tt2057880"  # WWHL IMDb ID
        self.tmdb_show_id = "22980"       # WWHL TMDb ID
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
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
        
        log.info("🎬 IMDb API Episode Fetcher initialized")

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
            spreadsheet_name = 'The Real Reality Backend'
            self.spreadsheet = self.gc.open(spreadsheet_name)
            
            # Get worksheets
            self.wwhl_ws = self.spreadsheet.worksheet('WWHLinfo')
            self.castinfo_ws = self.spreadsheet.worksheet('CastInfo')
            self.realitease_ws = self.spreadsheet.worksheet('RealiteaseInfo')
            
            log.info("✅ Connected to Google Sheets")
            return True
            
        except Exception as e:
            log.error(f"❌ Failed Google Sheets setup: {e}")
            return False

    def load_castinfo_mappings(self):
        """Build IMDb → TMDb person id mapping from CastInfo."""
        vals = self.castinfo_ws.get_all_values()
        if not vals or len(vals) < 2:
            log.warning("CastInfo is empty.")
            return

        headers = [h.strip() for h in vals[0]]
        log.info(f"🔍 CastInfo headers: {headers}")
        
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

    def _read_existing(self) -> Dict[str, int]:
        """Return map of EpisodeMarker → row_number (1-based) for quick updating."""
        data = self.wwhl_ws.get_all_values()
        idx = {}
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 2:
                continue
            marker = row[1].strip()
            if marker:
                idx[marker] = i
        return idx

    def get_available_seasons(self) -> List[int]:
        """Get seasons using IMDb API."""
        url = f"{IMDB_API_BASE}/{self.imdb_series_id}/seasons"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            
            if "seasons" in data and isinstance(data["seasons"], list):
                seasons = []
                for season_info in data["seasons"]:
                    if isinstance(season_info, dict) and "season" in season_info:
                        try:
                            season_num = int(season_info["season"])
                            seasons.append(season_num)
                        except (ValueError, TypeError):
                            continue
                
                seasons.sort()
                log.info(f"🗂️ Found {len(seasons)} seasons via IMDb API: {seasons}")
                return seasons
            
        except Exception as e:
            log.warning(f"⚠️ IMDb API seasons request failed: {e}")
        
        # Fallback to hardcoded range
        seasons = list(range(1, 23))  # Seasons 1-22
        log.info(f"🗂️ Using fallback season range 1-22")
        return seasons

    def get_season_episodes(self, season: int) -> List[Episode]:
        """Get episodes for a season using IMDb API."""
        url = f"{IMDB_API_BASE}/{self.imdb_series_id}/episodes"
        params = {"season": season}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            episodes = []
            if "episodes" in data and isinstance(data["episodes"], list):
                for ep_data in data["episodes"]:
                    if not isinstance(ep_data, dict):
                        continue
                    
                    # Extract episode information
                    tconst = ep_data.get("id", "").replace("/title/", "").replace("/", "")
                    episode_num = ep_data.get("episodeNumber")
                    air_date = ep_data.get("plot", "")  # Sometimes air date is in plot field
                    
                    # Try to parse episode number
                    if isinstance(episode_num, (int, str)):
                        try:
                            episode_num = int(episode_num)
                        except (ValueError, TypeError):
                            continue
                    else:
                        continue
                    
                    if tconst and episode_num:
                        episodes.append(Episode(
                            tconst=tconst,
                            season=season,
                            episode=episode_num,
                            air_date=air_date or ""
                        ))
            
            log.info(f"📺 Season {season}: found {len(episodes)} episodes via API")
            time.sleep(DELAY_REQUEST)
            return episodes
            
        except Exception as e:
            log.warning(f"⚠️ IMDb API episodes request failed for season {season}: {e}")
            time.sleep(DELAY_REQUEST)
            return []

    def get_episode_cast(self, tconst: str) -> List[Guest]:
        """Get cast information for an episode using IMDb API."""
        # Note: The IMDb API shown in the docs doesn't seem to have a direct cast endpoint
        # This would need to be implemented based on the actual API capabilities
        # For now, return empty list
        return []

    def _resolve_guest_ids_and_source(self, guests: List[Guest]) -> Tuple[str, str, str]:
        """Build joined strings for GuestNames, GuestStarTMDbIDs, GuestStarIMDbIDs and compute Cast_Source."""
        guest_names = []
        imdb_ids = []
        tmdb_ids = []
        sources = set()

        for g in guests:
            guest_names.append(g.name)
            imdb_ids.append(g.imdb_id)

            tmdb = self.imdb_to_tmdb_person.get(g.imdb_id)
            if tmdb:
                tmdb_ids.append(tmdb)
            else:
                tmdb_ids.append("")

            # Source tagging
            if tmdb and tmdb in self.realitease_tmdb_people:
                sources.add("REALITEASE")
            elif g.imdb_id in self.realitease_imdb_people:
                sources.add("REALITEASE")
            elif tmdb:
                sources.add("CAST INFO")

        cast_source = "NONE"
        if "REALITEASE" in sources:
            cast_source = "REALITEASE"
        elif "CAST INFO" in sources:
            cast_source = "CAST INFO"

        return (
            ", ".join([n for n in guest_names if n]),
            ", ".join([t for t in tmdb_ids if t]),
            ", ".join([i for i in imdb_ids if i]),
        ), cast_source

    @staticmethod
    def _set_list(lst: List[str], idx: int, val: str):
        while len(lst) <= idx:
            lst.append("")
        lst[idx] = val

    def process(self):
        """Main processing function."""
        if not self.setup_google_sheets():
            return False

        self._ensure_headers()
        self.load_castinfo_mappings()
        self.load_realitease_membership()

        # Build existing index
        marker_to_row = self._read_existing()
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
                
                # Get guests (currently empty due to API limitations)
                guests = self.get_episode_cast(ep.tconst)
                (names_str, tmdbs_str, imdbs_str), cast_source = self._resolve_guest_ids_and_source(guests)

                # Prepare row data
                row = [
                    self.tmdb_show_id or "",     # TMDbID (show)
                    marker,                       # EpisodeMarker
                    str(ep.season),              # Season
                    str(ep.episode),             # Episode
                    ep.air_date or "",           # AirDate
                    names_str,                   # GuestNames
                    tmdbs_str,                   # GuestStarTMDbIDs
                    imdbs_str,                   # GuestStarIMDbIDs
                    cast_source,                 # Cast_Source
                ]

                if marker in marker_to_row:
                    # Update existing row if it has missing data
                    row_num = marker_to_row[marker]
                    existing = self.wwhl_ws.row_values(row_num)
                    changed = False

                    def get(col_idx):
                        return existing[col_idx] if len(existing) > col_idx else ""

                    merged = existing[:]
                    
                    # Check and update missing fields
                    if not get(4) and row[4]:  # AirDate
                        self._set_list(merged, 4, row[4])
                        changed = True
                    if not get(5) and row[5]:  # GuestNames
                        self._set_list(merged, 5, row[5])
                        changed = True
                    if not get(6) and row[6]:  # GuestStarTMDbIDs
                        self._set_list(merged, 6, row[6])
                        changed = True
                    if not get(7) and row[7]:  # GuestStarIMDbIDs
                        self._set_list(merged, 7, row[7])
                        changed = True
                    if not get(8) and row[8]:  # Cast_Source
                        self._set_list(merged, 8, row[8])
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

        log.info("🏁 IMDb API processing complete")
        return True

def main():
    fetcher = WWHLIMDbAPIFetcher()
    ok = fetcher.process()
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

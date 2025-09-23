#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WWHL episodes/guests fetcher (IMDb scraper version)

- Scrapes IMDb episodes & episode cast (guests) for "Watch What Happens Live with Andy Cohen".
- Updates existing rows with missing fields; appends new rows for missing episodes.
- Resolves IDs using your Google Sheets:
    • ShowInfo: IMDbSeriesID ↔ TMDb ShowID  (get both series IDs)
    • CastInfo: IMDb nm ↔ TMDb person id    (guest TMDb IDs)
    • RealiteaseInfo: membership check for Cast_Source

Worksheet schema (WWHLinfo):
    ['TMDbID','EpisodeMarker','Season','Episode','AirDate',
     'GuestNames','GuestStarTMDbIDs','GuestStarIMDbIDs','Cast_Source']

Requirements:
    pip install gspread google-auth requests beautifulsoup4 python-dotenv
"""

import os
import re
import time
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("WWHLIMDb")

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
IMDB_BASE = "https://www.imdb.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WWHLIMDbBot/1.0; +realitease)",
    "Accept-Language": "en-US,en;q=0.8",
}

# How aggressively to pause between network calls
DELAY_REQUEST = 0.7
DELAY_EPISODE = 0.2

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def nm_from_href(href: str) -> Optional[str]:
    # '/name/nm0004977/?ref_=ttfc_fc_cl_t1' -> nm0004977
    m = re.search(r"/name/(nm\d+)", href or "")
    return m.group(1) if m else None

def tt_from_href(href: str) -> Optional[str]:
    # '/title/tt1234567/?ref_=ttep_ep' -> tt1234567
    m = re.search(r"/title/(tt\d+)", href or "")
    return m.group(1) if m else None

def as_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------
@dataclass
class Episode:
    tconst: str
    season: int
    episode: int
    air_date: str  # yyyy-mm-dd or "" if not found

@dataclass
class Guest:
    name: str
    imdb_id: str  # nm...
    role: str     # character text
    tmdb_id: Optional[str] = None

# -----------------------------------------------------------------------------
# Main fetcher
# -----------------------------------------------------------------------------
class WWHLIMDbEpisodeFetcher:
    def __init__(self):
        load_dotenv()

        # Sheets/auth
        self.gc = None
        self.spreadsheet = None
        self.wwhl_ws = None
        self.castinfo_ws = None
        self.realitease_ws = None
        self.showinfo_ws = None

        # Show IDs
        self.imdb_series_id: Optional[str] = None  # tt...
        self.tmdb_show_id: Optional[str] = None    # numeric string

        # Mappings
        self.imdb_to_tmdb_person: Dict[str, str] = {}   # nm -> tmdb person id
        self.tmdb_to_imdb_person: Dict[str, str] = {}   # tmdb -> nm  (not essential, but handy)
        self.realitease_tmdb_people: Set[str] = set()
        self.realitease_imdb_people: Set[str] = set()

        # HTTP
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        log.info("🎬 IMDb Episode Fetcher initialized")

    # --------------------- Google Sheets bootstrap ---------------------------
    def setup_google_sheets(self) -> bool:
        try:
            key_file_path = os.path.join(
                os.path.dirname(__file__), "..", "..", "keys", "trr-backend-df2c438612e1.json"
            )
            creds = Credentials.from_service_account_file(
                key_file_path,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            self.gc = gspread.authorize(creds)
            self.spreadsheet = self.gc.open("Realitease2025Data")

            # Worksheets
            try:
                self.wwhl_ws = self.spreadsheet.worksheet("WWHLinfo")
            except gspread.WorksheetNotFound:
                self.wwhl_ws = self.spreadsheet.add_worksheet("WWHLinfo", rows=3000, cols=12)

            self.castinfo_ws = self.spreadsheet.worksheet("CastInfo")
            self.realitease_ws = self.spreadsheet.worksheet("RealiteaseInfo")
            self.showinfo_ws = self.spreadsheet.worksheet("ShowInfo")
            log.info("✅ Connected to Google Sheets")
            return True
        except Exception as e:
            log.error(f"❌ Failed Google Sheets setup: {e}")
            return False

    # ------------------------ Load mappings from sheets ----------------------
    def load_showinfo_ids(self):
        """Use hardcoded WWHL show information since it's not in ShowInfo sheet."""
        # Hardcode WWHL information
        self.imdb_series_id = "tt2057880"  # WWHL IMDb ID
        self.tmdb_show_id = "22980"       # WWHL TMDb ID
        log.info(f"🎯 Using hardcoded WWHL: IMDbSeriesID={self.imdb_series_id}, TMDbShowID={self.tmdb_show_id}")
        log.info(f"🎯 WWHL: IMDbSeriesID={self.imdb_series_id}  TMDbShowID={self.tmdb_show_id}")

    def load_castinfo_mappings(self):
        """Build IMDb nm → TMDb person id mapping (and reverse) from CastInfo."""
        vals = self.castinfo_ws.get_all_values()
        if not vals or len(vals) < 2:
            log.warning("CastInfo is empty.")
            return

        headers = [h.strip().lower() for h in vals[0]]
        log.info(f"🔍 CastInfo headers (lowercased): {headers}")
        h_tmdb = next((i for i, h in enumerate(headers) if "tmdb" in h and "cast" in h and "id" in h), None)
        h_imdb = next((i for i, h in enumerate(headers) if "cast" in h and "imdb" in h and "id" in h), None)
        log.info(f"🔍 Found TMDb col: {h_tmdb}, IMDb col: {h_imdb}")

        if h_tmdb is None or h_imdb is None:
            log.warning("CastInfo missing TMDb/IMDb person id columns.")
            return

        for row in vals[1:]:
            if len(row) <= max(h_tmdb, h_imdb):
                continue
            tmdb = row[h_tmdb].strip()
            imdb = row[h_imdb].strip()
            if imdb and not imdb.startswith("nm"):
                imdb = f"nm{imdb}"
            if imdb and tmdb:
                self.imdb_to_tmdb_person[imdb] = tmdb
                self.tmdb_to_imdb_person[tmdb] = imdb

        log.info(f"🔗 IMDb→TMDb person mappings loaded: {len(self.imdb_to_tmdb_person)}")

    def load_realitease_membership(self):
        """Load IMDb/TMDb person IDs present in RealiteaseInfo for Cast_Source tagging."""
        vals = self.realitease_ws.get_all_values()
        if not vals or len(vals) < 2:
            log.warning("RealiteaseInfo empty.")
            return

        headers = [h.strip().lower() for h in vals[0]]
        h_tmdb = next((i for i, h in enumerate(headers) if "tmdb" in h and "id" in h), None)
        h_imdb = next((i for i, h in enumerate(headers) if "imdb" in h and "id" in h), None)

        for row in vals[1:]:
            if h_tmdb is not None and len(row) > h_tmdb:
                v = row[h_tmdb].strip()
                if v:
                    self.realitease_tmdb_people.add(v)
            if h_imdb is not None and len(row) > h_imdb:
                v = row[h_imdb].strip()
                if v:
                    if not v.startswith("nm"):
                        v = f"nm{v}"
                    self.realitease_imdb_people.add(v)

        log.info(f"👥 Realitease members (TMDb): {len(self.realitease_tmdb_people)} | (IMDb): {len(self.realitease_imdb_people)}")

    # --------------------------- IMDb scraping -------------------------------
    def get_available_seasons(self) -> List[int]:
        """
        Return available seasons. Since IMDb pages don't reliably show all seasons,
        we'll use a known range for WWHL (seasons 1-22+ as of 2024).
        """
        # WWHL has been running since 2009, with 22+ seasons as of 2024
        seasons = list(range(1, 23))  # Seasons 1-22
        log.info(f"🗂️ Using hardcoded season range 1-22 for WWHL")
        return seasons

    def scrape_season(self, season: int) -> List[Episode]:
        """Scrape all episodes (tconst, ep number, airdate) for a given season."""
        url = f"{IMDB_BASE}/title/{self.imdb_series_id}/episodes?season={season}"
        r = self.session.get(url, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        eps: List[Episode] = []

        # Look for episode links in the current IMDb structure
        # Episodes appear as links with href="/title/tt..." and text containing "S{season}.E{episode}"
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
                eps.append(Episode(tconst=tconst, season=season, episode=epno, air_date=air_date))

        log.info(f"📺 Season {season}: found {len(eps)} episodes")
        return eps

    def _normalize_date(self, s: str) -> str:
        """Convert common IMDb airdate patterns to YYYY-MM-DD when possible; else ''."""
        s = s.replace("\xa0", " ").strip()
        # Try known formats
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
        
        # "15 Jul. 2010" or "15 Jul 2010"
        m = re.match(r"(\d{1,2})\s+([A-Za-z]{3,4})\.?\s+(\d{4})", s)
        if m:
            d, mon, y = m.groups()
            mon = mm.get(mon.lower().strip("."), "")
            if mon:
                return f"{y}-{mon}-{d.zfill(2)}"
        # "July 15, 2010"
        m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", s)
        if m:
            mon_name, d, y = m.groups()
            mon = mm.get(mon_name[:3].lower(), "")
            if mon:
                return f"{y}-{mon}-{d.zfill(2)}"
        # Year only
        m = re.match(r"(\d{4})", s)
        if m:
            return f"{m.group(1)}-01-01"
        return ""

    def scrape_episode_guests(self, tconst: str) -> List[Guest]:
        """
        Scrape episode 'full credits' page and extract guests.
        Heuristic: keep cast rows where character contains 'Guest' and not 'Host'
        (common for WWHL: 'Self - Guest').
        """
        url = f"{IMDB_BASE}/title/{tconst}/fullcredits"
        r = self.session.get(url, timeout=25)
        if r.status_code != 200:
            log.warning(f"⚠️ fullcredits not found for {tconst}: {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")

        guests: List[Guest] = []
        for tr in soup.select("table.cast_list tr"):
            a = tr.select_one("td.primary_photo a[href*='/name/nm']")
            name_link = tr.select_one("td:not(.primary_photo) a[href*='/name/nm']")
            char_td = tr.select_one("td.character")

            if not a or not name_link or not char_td:
                continue

            nm_id = nm_from_href(a.get("href"))
            name = name_link.get_text(" ", strip=True)
            role = char_td.get_text(" ", strip=True)

            if not nm_id or not name:
                continue

            rl = role.lower()
            # include common guest signals; exclude host
            is_guest = ("guest" in rl) and ("host" not in rl)
            if is_guest:
                guests.append(Guest(name=name, imdb_id=nm_id, role=role))

        # De-dup (prefer first occurrence)
        uniq: Dict[str, Guest] = {}
        for g in guests:
            uniq.setdefault(g.imdb_id, g)

        guests = list(uniq.values())
        log.debug(f"   👤 Guests for {tconst}: {[(g.name, g.role) for g in guests]}")
        return guests

    # --------------------------- Sheet I/O -----------------------------------
    def _ensure_headers(self):
        headers = [
            "TMDbID",
            "EpisodeMarker",
            "Season",
            "Episode",
            "AirDate",
            "GuestNames",
            "GuestStarTMDbIDs",
            "GuestStarIMDbIDs",
            "Cast_Source",
        ]
        vals = self.wwhl_ws.get_all_values()
        if not vals:
            self.wwhl_ws.append_row(headers)
            return
        if vals[0] != headers:
            # If header row exists but different, do not overwrite; assume compatible.
            pass

    def _read_existing(self) -> Dict[str, int]:
        """
        Return map of EpisodeMarker -> row_number (1-based) for quick updating.
        """
        data = self.wwhl_ws.get_all_values()
        idx = {}
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 2:
                continue
            marker = row[1].strip()
            if marker:
                idx[marker] = i
        return idx

    def _resolve_guest_ids_and_source(self, guests: List[Guest]) -> Tuple[str, str, str]:
        """
        Build joined strings for GuestNames, GuestStarTMDbIDs, GuestStarIMDbIDs
        and compute Cast_Source (REALITEASE > CAST INFO > NONE).
        """
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
                tmdb_ids.append("")  # unknown

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
            ", ".join(guest_names),
            ", ".join([t for t in tmdb_ids if t]),
            ", ".join([i for i in imdb_ids if i]),
        ), cast_source

    # --------------------------- Orchestration -------------------------------
    def process(self):
        if not self.setup_google_sheets():
            return False

        self._ensure_headers()
        self.load_showinfo_ids()
        self.load_castinfo_mappings()
        self.load_realitease_membership()

        # Build existing index
        marker_to_row = self._read_existing()
        log.info(f"📋 Existing episode rows: {len(marker_to_row)}")

        # Discover seasons on IMDb
        seasons = self.get_available_seasons()
        log.info(f"🗂️ Seasons on IMDb: {seasons}")

        new_rows: List[List[str]] = []
        rows_to_update: List[Tuple[int, List[str]]] = []

        for s in seasons:
            episodes = self.scrape_season(s)
            for ep in episodes:
                marker = f"S{ep.season}E{ep.episode}"

                # scrape guests
                guests = self.scrape_episode_guests(ep.tconst)
                (names_str, tmdbs_str, imdbs_str), cast_source = self._resolve_guest_ids_and_source(guests)

                # Prepare row in our schema
                row = [
                    self.tmdb_show_id or "",     # TMDbID (show)
                    marker,                       # EpisodeMarker
                    ep.season,                    # Season
                    ep.episode,                   # Episode
                    ep.air_date or "",            # AirDate
                    names_str,                    # GuestNames
                    tmdbs_str,                    # GuestStarTMDbIDs
                    imdbs_str,                    # GuestStarIMDbIDs
                    cast_source,                  # Cast_Source
                ]

                if marker in marker_to_row:
                    # Update only if missing fields in existing row
                    row_num = marker_to_row[marker]
                    existing = self.wwhl_ws.row_values(row_num)
                    changed = False

                    def get(col_idx):
                        return existing[col_idx] if len(existing) > col_idx else ""

                    # Columns: 0..8 per schema
                    merged = existing[:]
                    # AirDate
                    if not get(4) and row[4]:
                        self._set_list(merged, 4, row[4]); changed = True
                    # GuestNames
                    if not get(5) and row[5]:
                        self._set_list(merged, 5, row[5]); changed = True
                    # GuestStarTMDbIDs
                    if not get(6) and row[6]:
                        self._set_list(merged, 6, row[6]); changed = True
                    # GuestStarIMDbIDs
                    if not get(7) and row[7]:
                        self._set_list(merged, 7, row[7]); changed = True
                    # Cast_Source
                    if not get(8) and row[8]:
                        self._set_list(merged, 8, row[8]); changed = True

                    # Also ensure TMDbID (show) is set (col 0)
                    if not get(0) and row[0]:
                        self._set_list(merged, 0, row[0]); changed = True

                    if changed:
                        # ensure row has at least 9 columns
                        while len(merged) < 9:
                            merged.append("")
                        rows_to_update.append((row_num, merged[:9]))
                        log.info(f"♻️ Update {marker}: filled missing data")
                else:
                    new_rows.append(row)
                    log.info(f"➕ Queue new episode {marker} (guests: {len(guests)})")

                time.sleep(DELAY_EPISODE)

            time.sleep(DELAY_REQUEST)

        # Batch write updates, then appends
        if rows_to_update:
            # group by contiguous ranges is overkill; update row by row to keep it simple & robust
            for row_num, payload in rows_to_update:
                rng = f"A{row_num}:I{row_num}"
                self.wwhl_ws.update(rng, [payload])
                time.sleep(0.1)
            log.info(f"✅ Updated {len(rows_to_update)} existing rows")

        if new_rows:
            self.wwhl_ws.append_rows(new_rows, value_input_option="RAW", table_range="A1:I1")
            log.info(f"🎉 Appended {len(new_rows)} new rows")

        log.info("🏁 IMDb scraping complete")
        return True

    @staticmethod
    def _set_list(lst: List[str], idx: int, val: str):
        while len(lst) <= idx:
            lst.append("")
        lst[idx] = val


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def main():
    fetcher = WWHLIMDbEpisodeFetcher()
    ok = fetcher.process()
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
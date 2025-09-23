#!/usr/bin/env python3
"""
ShowInfo Step 1 - showinfo_step1.py

Builds and updates the ShowInfo sheet using TMDb and IMDb list sources
with safe append/backfill behavior. Preserves OVERRIDE flags.
"""
import os
import time
import re
import json
import requests
import gspread
from typing import Optional, Dict, List, Set
from pathlib import Path
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv, find_dotenv

# ===============================
# Environment / Config (robust .env find)
# ===============================
HERE = Path(__file__).resolve().parent

def _load_env():
    # Try common locations, then fallback
    candidates = [
        HERE / ".env",
        HERE.parent / ".env",
        Path(find_dotenv(filename=".env", raise_error_if_not_found=False or False)),
    ]
    for p in candidates:
        try:
            if p and str(p) != "" and Path(p).exists():
                load_dotenv(dotenv_path=str(p), override=True)
                break
        except Exception:
            pass
    # also allow default search
    load_dotenv(override=True)

_load_env()

GOOGLE_CREDS   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TMDB_API_KEY   = os.getenv("TMDB_API_KEY")
TMDB_BEARER    = os.getenv("TMDB_BEARER")
TMDB_LIST_ID   = os.getenv("TMDB_LIST_ID")  # e.g., "8301263"
THETVDB_API_KEY= os.getenv("THETVDB_API_KEY")

# IMDb list URL — set to your list
IMDB_LIST_URL  = os.getenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")

# --- TheTVDB v4 token helper (login if needed) ---
_TVDB_JWT = None

def tvdb_get_token() -> Optional[str]:
    """
    Returns a valid TheTVDB v4 JWT. If THETVDB_API_KEY already looks like a JWT (has two dots),
    use it as-is. Otherwise, log in with the API key to obtain a JWT and cache it.
    """
    global _TVDB_JWT
    # Reuse cached JWT if present and looks valid
    if _TVDB_JWT and _TVDB_JWT.count(".") == 2:
        return _TVDB_JWT

    # Some users store a JWT directly in THETVDB_API_KEY; detect by two dots in the string
    if THETVDB_API_KEY and THETVDB_API_KEY.count(".") == 2:
        _TVDB_JWT = THETVDB_API_KEY
        return _TVDB_JWT

    if not THETVDB_API_KEY:
        return None

    try:
        r = requests.post(
            "https://api4.thetvdb.com/v4/login",
            json={"apikey": THETVDB_API_KEY},
            timeout=15
        )
        if r.status_code == 200:
            _TVDB_JWT = (r.json().get("data") or {}).get("token")
            return _TVDB_JWT
        else:
            print(f"TheTVDB login failed: {r.status_code} {r.text}")
            return None
    except Exception as e:
        print(f"TheTVDB login error: {e}")
        return None

def require(name: str, value: Optional[str]):
    if not value:
        raise ValueError(f"Missing {name} in .env (or environment).")
    return value

# Soft-fallback for service account path
def _resolve_service_account_path(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None

    cleaned = raw.strip().strip("\"'")
    if not cleaned:
        return None

    path = Path(cleaned).expanduser()
    candidates = []
    if path.is_absolute():
        candidates.append(path)
    else:
        candidates.append((HERE / cleaned).resolve())
        candidates.append((HERE.parent / cleaned).resolve())
        candidates.append((HERE.parent.parent / cleaned).resolve())
        candidates.append((Path.cwd() / cleaned).resolve())
        candidates.append(path.resolve())

    seen: Set[str] = set()
    ordered: List[Path] = []
    for cand in candidates:
        key = str(cand)
        if key not in seen:
            ordered.append(cand)
            seen.add(key)

    for cand in ordered:
        try:
            if cand.exists():
                return str(cand)
        except Exception:
            continue

    return str(ordered[0]) if ordered else str(path)


if not GOOGLE_CREDS or not Path(_resolve_service_account_path(GOOGLE_CREDS) or "").exists():
    for base in (HERE, HERE.parent, HERE.parent.parent):
        keys_dir = base / "keys"
        if not keys_dir.exists():
            continue
        for p in keys_dir.iterdir():
            if p.suffix == ".json":
                GOOGLE_CREDS = str(p)
                break
        if GOOGLE_CREDS:
            break

GOOGLE_CREDS = _resolve_service_account_path(require("GOOGLE_APPLICATION_CREDENTIALS", GOOGLE_CREDS))
if not GOOGLE_CREDS or not Path(GOOGLE_CREDS).exists():
    raise ValueError("Unable to locate Google service account JSON. Set GOOGLE_APPLICATION_CREDENTIALS or place a key file in the project keys/ directory.")

SPREADSHEET_ID = require("SPREADSHEET_ID", SPREADSHEET_ID)
TMDB_API_KEY   = require("TMDB_API_KEY", TMDB_API_KEY)
TMDB_BEARER    = os.getenv("TMDB_BEARER", TMDB_BEARER)

# ===============================
# Target column layout (A..H)
# ===============================
NEW_HEADERS: List[str] = [
    "Show",                # A – Show name
    "Network",             # B
    "ShowTotalSeasons",    # C
    "ShowTotalEpisodes",   # D
    "IMDbSeriesID",        # E
    "TMDbSeriesID",        # F
    "Most Recent Episode", # G
    "OVERRIDE",            # H
]

COLS = {h:i for i,h in enumerate(NEW_HEADERS)}

# ===============================
# Google Sheets helpers
# ===============================
def sheets_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def open_sheet(gc):
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("ShowInfo")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="ShowInfo", rows=1000, cols=len(NEW_HEADERS))
        ws.update(values=[NEW_HEADERS], range_name="A1")
    return ws

def get_all_values_safe(ws) -> List[List[str]]:
    try:
        return ws.get_all_values()
    except Exception:
        return []

def header_index_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def normalize_sheet_structure(ws) -> None:
    """Ensure the sheet uses NEW_HEADERS and migrate legacy layouts safely."""
    data = get_all_values_safe(ws)
    if not data:
        ws.update(values=[NEW_HEADERS], range_name="A1")
        return

    current_headers = data[0]
    if current_headers == NEW_HEADERS:
        return

    print("Normalizing header structure…")
    cur_map = header_index_map(current_headers)

    def pick(row: List[str], *names: str) -> str:
        for name in names:
            idx = cur_map.get(name)
            if idx is None or idx >= len(row):
                continue
            value = row[idx].strip()
            if value:
                return value
        return ""

    remapped: List[List[str]] = [NEW_HEADERS]
    for row in data[1:]:
        if not any(cell.strip() for cell in row):
            remapped.append([""] * len(NEW_HEADERS))
            continue

        show_name = pick(row, "ShowName", "Show")
        tmdb_id = pick(row, "TMDbSeriesID", "TheMovieDB ID")
        if not tmdb_id and "ShowName" in cur_map:
            fallback_id = pick(row, "Show")
            if fallback_id and not fallback_id.strip().lower().startswith("imdb_"):
                tmdb_id = fallback_id

        new_row = [""] * len(NEW_HEADERS)
        new_row[COLS["Show"]] = show_name
        new_row[COLS["Network"]] = pick(row, "Network")
        new_row[COLS["ShowTotalSeasons"]] = pick(row, "ShowTotalSeasons")
        new_row[COLS["ShowTotalEpisodes"]] = pick(row, "ShowTotalEpisodes")
        new_row[COLS["IMDbSeriesID"]] = pick(row, "IMDbSeriesID")
        new_row[COLS["TMDbSeriesID"]] = tmdb_id
        new_row[COLS["Most Recent Episode"]] = pick(row, "Most Recent Episode")
        new_row[COLS["OVERRIDE"]] = pick(row, "OVERRIDE")

        remapped.append(new_row)

    ws.clear()
    try:
        ws.resize(rows=max(len(remapped), 1000), cols=len(NEW_HEADERS))
    except Exception:
        pass
    ws.update(values=remapped, range_name="A1", value_input_option="RAW")
    print("Header normalization complete.")


def read_existing_shows(ws) -> Dict[str, Dict]:
    """Read existing shows keyed by TMDb or IMDb identifier. Keep row_index for updates."""
    existing: Dict[str, Dict] = {}
    data = get_all_values_safe(ws)
    if not data:
        return existing

    headers = data[0]
    cmap = header_index_map(headers)

    def cell(row: List[str], header: str) -> str:
        idx = cmap.get(header)
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    for i, row in enumerate(data[1:], start=2):
        if not row or not any(cell_val.strip() for cell_val in row):
            continue

        show_name = cell(row, "Show")
        network = cell(row, "Network")
        seasons = cell(row, "ShowTotalSeasons")
        episodes = cell(row, "ShowTotalEpisodes")
        imdb_id = cell(row, "IMDbSeriesID")
        tmdb_id = cell(row, "TMDbSeriesID")
        most_recent = cell(row, "Most Recent Episode")
        override = cell(row, "OVERRIDE")

        key = ""
        if tmdb_id:
            key = tmdb_id
        elif imdb_id:
            imdb_norm = imdb_id if imdb_id.lower().startswith("tt") else f"tt{imdb_id}"
            key = f"imdb_{imdb_norm}"
        elif show_name:
            key = show_name
        else:
            continue

        existing[key] = {
            "row_index": i,
            "Show": show_name,
            "Network": network,
            "ShowTotalSeasons": seasons,
            "ShowTotalEpisodes": episodes,
            "IMDbSeriesID": imdb_id,
            "TMDbSeriesID": tmdb_id,
            "Most Recent Episode": most_recent,
            "OVERRIDE": override,
        }

    return existing

# ===============================
# IMDb Scraper
# ===============================
def fetch_imdb_list_shows() -> List[Dict]:
    """
    Scrape (name, imdb_id) from your IMDb list using structured data.
    
    Primary strategy: Extract from JSON-LD structured data (gets all items in one request).
    Fallback strategies: HTML parsing and pagination if needed.
    
    Returns:
        List of dicts with 'name' and 'imdb_id' keys.
    """
    base = (IMDB_LIST_URL or "").split("?")[0].rstrip("/") + "/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        print(f"Fetching IMDb list: {base}")
        resp = requests.get(base, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # Strategy 1: Extract from JSON-LD structured data (most reliable)
        json_scripts = soup.find_all("script", type="application/ld+json")
        shows = []
        
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                
                # Check if this is the list data with itemListElement
                if isinstance(data, dict) and "itemListElement" in data:
                    items = data["itemListElement"]
                    print(f"Found structured data with {len(items)} items")
                    
                    for item in items:
                        try:
                            tv_series = item.get("item", {})
                            if tv_series.get("@type") == "TVSeries":
                                name = tv_series.get("name", "")
                                url = tv_series.get("url", "")
                                
                                # Extract IMDb ID from URL
                                m = re.search(r"/title/(tt\d+)/", url)
                                if m and name:
                                    imdb_id = m.group(1)
                                    shows.append({"name": name, "imdb_id": imdb_id})
                        except Exception as e:
                            print(f"  Error parsing structured item: {e}")
                    
                    if shows:
                        print(f"Successfully extracted {len(shows)} shows from structured data")
                        return shows
                        
            except Exception as e:
                print(f"  Error parsing JSON-LD: {e}")
        
        # Strategy 2: Fallback to HTML parsing if structured data fails
        print("Structured data extraction failed, falling back to HTML parsing")
        items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
        seen_ids = set()
        
        for it in items:
            try:
                link = it.find("a", class_="ipc-title-link-wrapper")
                if not link:
                    continue
                href = link.get("href", "")
                title = link.find("h3")
                if not title:
                    continue
                m = re.search(r"/title/(tt\d+)/", href)
                if not m:
                    continue
                imdb_id = m.group(1)
                if imdb_id in seen_ids:
                    continue
                name = re.sub(r"^\d+\.\s*", "", title.get_text(strip=True))
                shows.append({"name": name, "imdb_id": imdb_id})
                seen_ids.add(imdb_id)
            except Exception as e:
                print(f"  HTML parse error: {e}")
        
        print(f"HTML parsing extracted {len(shows)} shows")
        
        # If we still don't have many shows, try pagination
        if len(shows) < 50:
            print("Attempting pagination fallback...")
            return fetch_imdb_with_pagination(base, headers, shows)
        
        return shows
        
    except Exception as e:
        print(f"IMDb list fetch error: {e}")
        return []

def fetch_imdb_with_pagination(base: str, headers: dict, initial_shows: List[Dict]) -> List[Dict]:
    """Fallback pagination logic for IMDb lists."""
    shows = initial_shows.copy()
    seen_ids = {show["imdb_id"] for show in shows}
    visited = set()
    page_url = base
    page_num = 1
    
    while page_url and page_url not in visited and page_num <= 10:  # Safety limit
        if page_num == 1 and initial_shows:
            # Skip first page since we already processed it
            page_num += 1
            # Try different pagination URL patterns
            for pattern in ["?page=2", "?start=25", "/?page=2"]:
                test_url = base.rstrip("/") + pattern
                try:
                    test_resp = requests.get(test_url, headers=headers, timeout=15)
                    if test_resp.status_code == 200:
                        test_soup = BeautifulSoup(test_resp.content, "html.parser")
                        test_items = test_soup.find_all("li", class_="ipc-metadata-list-summary-item")
                        if test_items:
                            page_url = test_url
                            break
                except Exception:
                    continue
            else:
                break
        
        visited.add(page_url)
        try:
            print(f"Fetching pagination page {page_num}: {page_url}")
            resp = requests.get(page_url, headers=headers, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            
            items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
            page_count = 0
            
            for it in items:
                try:
                    link = it.find("a", class_="ipc-title-link-wrapper")
                    if not link:
                        continue
                    href = link.get("href", "")
                    title = link.find("h3")
                    if not title:
                        continue
                    m = re.search(r"/title/(tt\d+)/", href)
                    if not m:
                        continue
                    imdb_id = m.group(1)
                    if imdb_id in seen_ids:
                        continue
                    name = re.sub(r"^\d+\.\s*", "", title.get_text(strip=True))
                    shows.append({"name": name, "imdb_id": imdb_id})
                    seen_ids.add(imdb_id)
                    page_count += 1
                except Exception as e:
                    print(f"  Pagination parse error: {e}")
            
            print(f"  Page {page_num}: {page_count} new shows (total: {len(shows)})")
            
            if page_count == 0:
                break
                
            # Try to find next page (simplified)
            page_num += 1
            next_url = base.rstrip("/") + f"?page={page_num}"
            page_url = next_url
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Pagination error on page {page_num}: {e}")
            break
    
    return shows

# ===============================
# TMDb helpers
# ===============================
V4_HEADERS = None if not TMDB_BEARER else {"Authorization": f"Bearer {TMDB_BEARER}", "accept": "application/json"}
V3_BASE = "https://api.themoviedb.org/3"

def fetch_list_items(list_id: str) -> List[dict]:
    """Fetch all items from a TMDb list with graceful v4→v3 fallback."""
    if V4_HEADERS:
        items, fallback_reason = _fetch_list_items_v4(list_id)
        if items is not None:
            return items
        print(f"TMDb v4 list fetch unavailable ({fallback_reason}). Falling back to v3 API key…")
    else:
        print("TMDb bearer token not provided; using TMDb v3 API key for list fetch.")

    return _fetch_list_items_v3(list_id)


def _is_tv_entry(item: dict) -> bool:
    media = (item.get("media_type") or "").lower()
    if media in ("", "tv", "tv_show", "tvshow", "tvseries"):
        return True
    media_name = (item.get("media_type_name") or "").lower()
    return media_name.startswith("tv") or "television" in media_name


def _fetch_list_items_v4(list_id: str) -> tuple[Optional[List[dict]], Optional[str]]:
    url = f"https://api.themoviedb.org/4/list/{list_id}"
    page = 1
    out: List[dict] = []

    while True:
        resp = requests.get(url, headers=V4_HEADERS, params={"page": page}, timeout=30)
        if resp.status_code == 401:
            return None, f"{resp.status_code} unauthorized"
        if resp.status_code != 200:
            return None, f"{resp.status_code} response"

        data = resp.json()
        out.extend([r for r in data.get("results", []) if _is_tv_entry(r)])
        total_pages = data.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.25)

    return out, None


def _fetch_list_items_v3(list_id: str) -> List[dict]:
    url = f"{V3_BASE}/list/{list_id}"
    page = 1
    out: List[dict] = []

    while True:
        resp = requests.get(url, params={"api_key": TMDB_API_KEY, "page": page}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"TMDb v3 list fetch failed p{page}: {resp.status_code} {resp.text}")

        data = resp.json()
        items = data.get("items") or data.get("results") or []
        out.extend([r for r in items if _is_tv_entry(r)])

        total_pages = data.get("total_pages")
        if total_pages is None or page >= total_pages or not items:
            break
        page += 1
        time.sleep(0.25)

    return out

def fetch_tv_details(tv_id: int) -> dict:
    """Fetch full TV details (including external_ids)."""
    params = {"api_key": TMDB_API_KEY}
    base_url = f"{V3_BASE}/tv/{tv_id}"
    resp = requests.get(base_url, params=params, timeout=30)
    if resp.status_code != 200:
        return {"number_of_seasons": None, "number_of_episodes": None, "networks": [], "external_ids": {}}
    data = resp.json()
    try:
        ext = requests.get(f"{base_url}/external_ids", params=params, timeout=30)
        data["external_ids"] = ext.json() if ext.status_code == 200 else {}
    except Exception:
        data["external_ids"] = {}
    return data

def search_tmdb_by_imdb_id(imdb_id: str) -> Optional[Dict]:
    """Match on TMDb via IMDb ID."""
    if not imdb_id.startswith("tt"):
        imdb_id = "tt" + imdb_id
    url = f"{V3_BASE}/find/{imdb_id}"
    params = {"api_key": TMDB_API_KEY, "external_source": "imdb_id"}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            tv = r.json().get("tv_results", [])
            if tv:
                return tv[0]
    except Exception as e:
        print("TMDb find-by-IMDb error:", e)
    return None

def search_tmdb_by_name(show_name: str) -> Optional[Dict]:
    """Fallback: TMDb search by name."""
    url = f"{V3_BASE}/search/tv"
    params = {"api_key": TMDB_API_KEY, "query": show_name}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            res = r.json().get("results", [])
            if res:
                return res[0]
    except Exception as e:
        print("TMDb search-by-name error:", e)
    return None

def get_most_recent_episode_date(tv_id: int) -> str:
    """Return YYYY-MM-DD of the last aired episode (best effort)."""
    try:
        params = {"api_key": TMDB_API_KEY}
        r = requests.get(f"{V3_BASE}/tv/{tv_id}", params=params, timeout=30)
        if r.status_code != 200:
            return ""
        data = r.json()
        if data.get("last_episode_to_air") and data["last_episode_to_air"].get("air_date"):
            return data["last_episode_to_air"]["air_date"]
        seasons = data.get("seasons", [])
        if not seasons:
            return ""
        latest = max(seasons, key=lambda s: s.get("season_number", 0))
        sn = latest.get("season_number", 1)
        rs = requests.get(f"{V3_BASE}/tv/{tv_id}/season/{sn}", params=params, timeout=30)
        if rs.status_code != 200:
            return ""
        eps = rs.json().get("episodes", [])
        if not eps:
            return ""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        aired = [ep for ep in eps if ep.get("air_date") and ep["air_date"] <= today]
        if not aired:
            return ""
        return max(aired, key=lambda e: e.get("air_date", "")).get("air_date", "")
    except Exception as e:
        print("Recent-episode fetch error:", e)
        return ""

# ===============================
# External IDs
# ===============================
def get_wikidata_id(show_name: str, imdb_id: Optional[str] = None) -> Optional[str]:
    """Lightweight Wikidata search by name."""
    try:
        r = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={"action":"wbsearchentities","format":"json","search":show_name,"language":"en","type":"item","limit":5},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            for item in data.get("search", []):
                desc = (item.get("description") or "").lower()
                if any(t in desc for t in ["television series","tv series","reality show","tv show"]):
                    return item.get("id")
        time.sleep(0.3)
    except Exception as e:
        print("Wikidata search error:", e)
    return None

def get_tvdb_id(show_name: str, imdb_id: Optional[str] = None) -> Optional[str]:
    """TheTVDB v4 search by name with proper JWT handling."""
    token = tvdb_get_token()
    if not token:
        return None
    try:
        headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
        r = requests.get(
            "https://api4.thetvdb.com/v4/search",
            headers=headers,
            params={"query": show_name, "type": "series"},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                return str(data[0].get("tvdb_id") or data[0].get("id", ""))
        elif r.status_code in (401, 403):
            # Refresh token and retry once
            token = tvdb_get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
                r2 = requests.get(
                    "https://api4.thetvdb.com/v4/search",
                    headers=headers,
                    params={"query": show_name, "type": "series"},
                    timeout=15
                )
                if r2.status_code == 200:
                    data = r2.json().get("data", [])
                    if data:
                        return str(data[0].get("tvdb_id") or data[0].get("id", ""))
        return None
    except Exception as e:
        print(f"TheTVDB search error '{show_name}': {e}")
        return None
    
# ===============================
# Utility
# ===============================
def col_letter(idx0: int) -> str:
    return chr(ord('A') + idx0)  # good up to K

# ===============================
# Main
# ===============================
def main():
    print("Starting Show Info Collection")
    gc = sheets_client()
    ws = open_sheet(gc)

    # 1) Ensure header layout (adds I/J/K if missing, preserves OVERRIDE by remap)
    normalize_sheet_structure(ws)

    # 2) Read current sheet
    existing = read_existing_shows(ws)
    print(f"Found {len(existing)} existing rows")

    # 3) Collect IDs from both lists
    collected: Dict[str, Dict] = {}

    # TMDb list
    if TMDB_LIST_ID:
        print(f"Fetching TMDb list {TMDB_LIST_ID}…")
        tmdb_items = fetch_list_items(TMDB_LIST_ID)
        tv_items = [it for it in tmdb_items if it.get("media_type") in (None, "tv")]
        print(f"  TMDb TV items: {len(tv_items)}")
        for it in tv_items:
            tv_id = str(it.get("id"))
            name  = it.get("name") or it.get("title") or "N/A"
            collected[tv_id] = {"name": name, "tmdb_id": tv_id, "imdb_id": None, "source": "tmdb_list"}

    # IMDb list
    print("Fetching IMDb list…")
    imdb_items = fetch_imdb_list_shows()
    print(f"  IMDb items: {len(imdb_items)}")

    for s in imdb_items:
        imdb_id = s["imdb_id"]
        name    = s["name"]

        tmdb_match = search_tmdb_by_imdb_id(imdb_id) or search_tmdb_by_name(name)
        if tmdb_match:
            tv_id = str(tmdb_match.get("id"))
            if tv_id in collected:
                collected[tv_id]["imdb_id"] = imdb_id
            else:
                collected[tv_id] = {"name": tmdb_match.get("name", name), "tmdb_id": tv_id, "imdb_id": imdb_id, "source":"imdb_list_matched"}
        else:
            imdb_key = f"imdb_{imdb_id}"
            if imdb_key not in collected:
                collected[imdb_key] = {"name": name, "tmdb_id": None, "imdb_id": imdb_id, "source":"imdb_only"}

    print(f"Total collected unique shows (TMDb + IMDb-only): {len(collected)}")

    # 4) Build rows & updates
    new_rows: List[List[str]] = []
    active_ids: Set[str] = set()
    updates: List[Dict] = []  # batch updates for existing rows (Most Recent Ep + backfills)

    for key, data in collected.items():
        name = (data.get("name") or "").strip()
        tmdb_raw = data.get("tmdb_id")
        tmdb_id = str(tmdb_raw).strip() if tmdb_raw is not None else ""
        imdb_id = (data.get("imdb_id") or "").strip()

        # Use the collection key as the show_id - this is already correctly formatted
        # (TMDb ID for matched shows, "imdb_" format only for unmatched IMDb-only shows)
        show_id = key
        active_ids.add(show_id)

        # Fetch the existing row (for short-circuit decisions)
        existing_row = existing.get(show_id)

        # Pull details if TMDb is known
        network_name = ""
        season_count = ""
        episode_count= ""
        most_recent  = ""

        if tmdb_id and tmdb_id.isdigit():
            try:
                det = fetch_tv_details(int(tmdb_id))
                season_count = det.get("number_of_seasons") or ""
                episode_count= det.get("number_of_episodes") or ""
                nets = det.get("networks") or []
                network_name = (nets[0]["name"] if nets else "") or ""
                if not imdb_id:
                    imdb_id = (det.get("external_ids") or {}).get("imdb_id", "") or imdb_id
                most_recent = get_most_recent_episode_date(int(tmdb_id)) or ""
            except Exception as e:
                print(f"  TMDb details error for {name} ({tmdb_id}):", e)

        # Construct final row
        row = [
            name,               # A Show (display name)
            network_name,       # B Network
            season_count,       # C ShowTotalSeasons
            episode_count,      # D ShowTotalEpisodes
            imdb_id,            # E IMDbSeriesID
            str(tmdb_id or ""),# F TMDbSeriesID
            most_recent,        # G Most Recent Episode
            "",                 # H OVERRIDE (blank for new rows)
        ]

        if show_id in existing:
            # Existing row: targeted updates only
            r = existing[show_id]["row_index"]

            # Always refresh Most Recent Episode (Column G) if we found a value
            if row[COLS["Most Recent Episode"]]:
                updates.append({
                    "range": f"{col_letter(COLS['Most Recent Episode'])}{r}:{col_letter(COLS['Most Recent Episode'])}{r}",
                    "values": [[row[COLS["Most Recent Episode"]]]]
                })

            # Backfill blanks (never overwrite)
            for col_name in ["Show","Network","ShowTotalSeasons","ShowTotalEpisodes","IMDbSeriesID","TMDbSeriesID"]:
                new_val = row[COLS[col_name]]
                if new_val and not (existing[show_id].get(col_name) or "").strip():
                    updates.append({
                        "range": f"{col_letter(COLS[col_name])}{r}:{col_letter(COLS[col_name])}{r}",
                        "values": [[new_val]]
                    })
        else:
            # New row → append at bottom with columns A–H populated best-effort
            new_rows.append(row)

        # Progress heartbeat every 10 items
        if len(active_ids) % 10 == 0:
            print(f"  Processed {len(active_ids)} / {len(collected)} …")

        time.sleep(0.05)

    # 5) Append new shows at bottom
    if new_rows:
        print(f"Appending {len(new_rows)} new show(s) at the bottom…")
        ws.append_rows(new_rows, value_input_option="RAW")
        print("New rows appended.")
    else:
        print("No new shows to add.")

    # 6) Apply targeted updates (existing rows: Most Recent Episode + backfills)
    if updates:
        print(f"Applying {len(updates)} cell updates…")
        ws.batch_update(updates, value_input_option="RAW")
        print("Cell updates applied.")

    # 7) Mark rows not present in either list as SKIP (OVERRIDE J)
    print("Marking rows not found in either list as SKIP…")
    # Reload after appends
    existing = read_existing_shows(ws)
    current_ids = set(existing.keys())
    to_skip = current_ids - active_ids - {""}
    if to_skip:
        j_col = NEW_HEADERS.index("OVERRIDE") + 1  # 1-based
        for sid in sorted(to_skip):
            r = existing[sid]["row_index"]
            cur = (existing[sid].get("OVERRIDE") or "").strip().upper()
            if cur != "SKIP":
                ws.update_cell(r, j_col, "SKIP")
                print(f"  Marked row {r} ({sid}) as SKIP")
            time.sleep(0.03)
        print("SKIP marking complete.")
    else:
        print("No rows require SKIP.")

    print("Done. Columns A–H refreshed as needed, removed shows → SKIP, existing data preserved.")

if __name__ == "__main__":
    main()

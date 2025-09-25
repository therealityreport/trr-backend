#!/usr/bin/env python3
"""
Enhanced CastInfo Updater - All Shows from ShowInfo
===================================================

Adds missing cast members to CastInfo (fills columns A–F only) using smart filtering rules 
based on ShowInfo column H:

1. Processes ALL shows from ShowInfo sheet (not just A-F letter shows)
2. New people: Need to meet show's minimum episodes (default 4, configurable per show via column H)
3. Existing people: Must ALSO meet show's minimum episodes
4. Special shows: Use column H threshold or Y=2 episodes
5. SKIP shows: Completely excluded from CastInfo (removes existing cast if present)
6. No episode data: Don't add
7. ShowName sync: Updates ShowName (column D) in CastInfo to match ShowInfo if different

NOTE: Only fills columns A-F. Columns G (TotalEpisodes) and H (TotalSeasons) are left 
blank for the v2UniversalSeasonExtractor script to fill later.
The "A-F" refers to filling CastInfo columns A-F, not filtering ShowInfo alphabetically.
"""

import gspread
import requests
import time
from collections import deque
from datetime import datetime, timedelta, date, timezone
from gspread.exceptions import APIError
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple, TypeVar

T = TypeVar("T")

class IMDbAPIClient:
    """Client for IMDbAPI.dev to get accurate episode counts."""
    MAX_RETRIES = 5
    INITIAL_BACKOFF = 2.0
    BACKOFF_MULTIPLIER = 2.0
    MAX_BACKOFF = 30.0
    
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        self.session.timeout = 30  # 30 second timeout
        self._cache = {}

    def _sleep_for_retry(self, attempt: int) -> float:
        wait = min(self.INITIAL_BACKOFF * (self.BACKOFF_MULTIPLIER ** attempt), self.MAX_BACKOFF)
        time.sleep(wait)
        return wait

    def _perform_get(self, url: str, params: Dict[str, str]) -> requests.Response:
        attempt = 0
        last_error: Optional[Exception] = None

        while attempt < self.MAX_RETRIES:
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    raise requests.exceptions.HTTPError(response=response)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt >= self.MAX_RETRIES - 1:
                    raise
                wait = self._sleep_for_retry(attempt)
                print(f"    ⏳ IMDb request timed out – retrying in {wait:.1f}s (attempt {attempt + 1}/{self.MAX_RETRIES})")
            except requests.exceptions.RequestException as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in {429, 500, 503} and attempt < self.MAX_RETRIES - 1:
                    wait = self._sleep_for_retry(attempt)
                    status_label = status or "unknown"
                    print(
                        f"    ⏳ IMDb request throttled (HTTP {status_label}) – retrying in {wait:.1f}s "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                else:
                    raise
            attempt += 1

        if last_error:
            raise last_error

        raise RuntimeError("IMDb API request retries exhausted")
        
    def get_all_cast_credits(self, title_id: str) -> Dict[str, dict]:
        """Get all cast credits for a title (with caching)."""
        if title_id in self._cache:
            return self._cache[title_id]
            
        cast_data = {}
        next_token = None
        max_pages = 100  # Safety valve to prevent runaway loops on malformed tokens
        page_count = 0
        seen_tokens = set()
        
        try:
            while page_count < max_pages:
                url = f"{self.base_url}/titles/{title_id}/credits"
                params = {"categories": "self"}
                if next_token:
                    params["pageToken"] = next_token
                    
                print(f"    🌐 API request page {page_count + 1} for {title_id}...")
                response = self._perform_get(url, params)
                data = response.json()
                
                credits = data.get("credits", [])
                print(f"    📄 Got {len(credits)} credits on page {page_count + 1}")
                
                for credit in credits:
                    name_data = credit.get("name", {})
                    name_id = name_data.get("id", "")
                    
                    if name_id and name_id.startswith("nm"):
                        cast_data[name_id] = {
                            "name": name_data.get("displayName", ""),
                            "episodes": credit.get("episodeCount", 0),
                            "characters": credit.get("characters", []),
                            "is_credited": credit.get("isCredited", True),
                            "attributes": credit.get("attributes", []),
                        }
                
                next_token = data.get("nextPageToken")
                page_count += 1
                
                if not next_token:
                    break

                if next_token in seen_tokens:
                    print("    ⚠️  Duplicate page token encountered – stopping pagination to avoid loop")
                    break

                seen_tokens.add(next_token)
                time.sleep(1.0)
                
        except requests.exceptions.Timeout:
            print(f"    ⏰ API request timed out for {title_id}")
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  API request failed for {title_id}: {e}")
        except Exception as e:
            print(f"    ⚠️  Failed to fetch cast for {title_id}: {e}")
        
        self._cache[title_id] = cast_data
        print(f"    ✅ Cached {len(cast_data)} cast members for {title_id}")
        return cast_data

class EnhancedCastInfoUpdater:
    """Enhanced updater that both updates existing entries and adds missing cast."""
    GOOGLE_SHEETS_MAX_RETRIES = 6
    GOOGLE_SHEETS_INITIAL_BACKOFF = 2.0
    GOOGLE_SHEETS_BACKOFF_MULTIPLIER = 1.8
    GOOGLE_SHEETS_MAX_BACKOFF = 60.0
    GOOGLE_SHEETS_POST_WRITE_DELAY = 0.2
    GOOGLE_SHEETS_MAX_WRITES_PER_MINUTE = 40
    GOOGLE_SHEETS_MIN_SLEEP_BETWEEN_WRITES = 0.9

    RECENT_EPISODE_WINDOW_DAYS = 3
    _RECENT_EPISODE_FORMATS: Tuple[str, ...] = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%b %d %Y",
        "%B %d %Y",
    )
    
    def __init__(self):
        # Connect to Google Sheets
        SERVICE_KEY_PATH = "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
        gc = gspread.service_account(filename=SERVICE_KEY_PATH)
        self.ss = gc.open("Realitease2025Data")
        self.imdb_api = IMDbAPIClient()
        self._castinfo_ws = None
        self._showinfo_ws = None
        self._castinfo_rows = None
        self._castinfo_by_show: Dict[str, Dict[str, dict]] = {}
        self._row_lookup: Dict[int, Tuple[str, str]] = {}
        self._write_history: Deque[Tuple[float, int]] = deque()
        self._write_history_tokens = 0
        self._last_write_time = 0.0

    def _with_gspread_retry(
        self,
        description: str,
        action: Callable[[], T],
        *,
        is_write: bool = False,
        tokens: int = 1,
    ) -> T:
        delay = self.GOOGLE_SHEETS_INITIAL_BACKOFF
        for attempt in range(self.GOOGLE_SHEETS_MAX_RETRIES):
            try:
                if is_write:
                    self._throttle_writes(tokens)
                result = action()
                if is_write:
                    self._record_write(tokens)
                return result
            except APIError as exc:
                is_rate = self._is_rate_limit_error(exc)
                if not is_rate or attempt == self.GOOGLE_SHEETS_MAX_RETRIES - 1:
                    print(f"    ❌ {description} failed: {self._summarize_api_error(exc)}")
                    raise
                print(
                    f"    ⏳ {description} throttled (attempt {attempt + 1}/{self.GOOGLE_SHEETS_MAX_RETRIES}) – "
                    f"sleeping {delay:.1f}s"
                )
                time.sleep(delay)
                delay = min(delay * self.GOOGLE_SHEETS_BACKOFF_MULTIPLIER, self.GOOGLE_SHEETS_MAX_BACKOFF)

        raise RuntimeError(f"Google Sheets retries exhausted for: {description}")

    def _trim_write_history(self, reference_time: float) -> None:
        window = 60.0
        while self._write_history and reference_time - self._write_history[0][0] >= window:
            _, count = self._write_history.popleft()
            self._write_history_tokens -= count

    def _throttle_writes(self, tokens: int) -> None:
        if tokens <= 0:
            return

        window_limit = self.GOOGLE_SHEETS_MAX_WRITES_PER_MINUTE
        min_gap = self.GOOGLE_SHEETS_MIN_SLEEP_BETWEEN_WRITES

        while True:
            now = time.time()
            self._trim_write_history(now)

            if self._last_write_time:
                gap = now - self._last_write_time
                if gap < min_gap:
                    sleep_time = min_gap - gap
                    print(f"    ⏳ Spacing Google Sheets writes – sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    continue

            if window_limit and self._write_history_tokens + tokens > window_limit and self._write_history:
                oldest_ts, _ = self._write_history[0]
                sleep_time = (oldest_ts + 60.0) - now + 0.05
                if sleep_time > 0:
                    print(f"    ⏱️ Near Google Sheets write quota – sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    continue

            break

    def _record_write(self, tokens: int) -> None:
        if tokens <= 0:
            return

        now = time.time()
        self._trim_write_history(now)
        self._write_history.append((now, tokens))
        self._write_history_tokens += tokens
        self._last_write_time = now

    @staticmethod
    def _is_rate_limit_error(error: APIError) -> bool:
        response = getattr(error, "response", None)
        status = getattr(response, "status_code", None)
        if status in {429, 500, 503}:
            return True
        if status == 403:
            reason = None
            try:
                payload = response.json()
                error_info = payload.get("error", {}) if isinstance(payload, dict) else {}
                reason = error_info.get("status") or ""
                errors = error_info.get("errors", []) or []
                if errors and not reason:
                    reason = errors[0].get("reason", "")
            except Exception:
                reason = None
            if reason and "rate" in reason.lower():
                return True
        message = str(error).lower()
        return any(token in message for token in ("ratelimit", "rate limit", "quota", "exceed", "user rate"))

    @staticmethod
    def _summarize_api_error(error: APIError) -> str:
        parts: List[str] = []
        response = getattr(error, "response", None)
        status = getattr(response, "status_code", None)
        if status:
            parts.append(f"status {status}")
        try:
            message = str(error)
            if message:
                parts.append(message)
        except Exception:
            pass
        if response is not None:
            try:
                payload = response.json()
                detail = payload.get("error", {}).get("message")
                if detail:
                    parts.append(detail)
            except Exception:
                text = getattr(response, "text", "")
                if text:
                    parts.append(text[:120])
        return " - ".join(parts) if parts else "unknown Google Sheets error"

    def _get_castinfo_ws(self) -> gspread.Worksheet:
        """Lazy-load the CastInfo worksheet."""
        if self._castinfo_ws is None:
            self._castinfo_ws = self.ss.worksheet("CastInfo")
        return self._castinfo_ws

    def _get_showinfo_ws(self) -> gspread.Worksheet:
        """Lazy-load the ShowInfo worksheet."""
        if self._showinfo_ws is None:
            self._showinfo_ws = self.ss.worksheet("ShowInfo")
        return self._showinfo_ws

    def _ensure_castinfo_cache(self, force_refresh: bool = False):
        """Fetch CastInfo sheet values and rebuild helper lookups when needed."""
        if self._castinfo_rows is None or force_refresh:
            ws = self._get_castinfo_ws()
            self._castinfo_rows = self._with_gspread_retry(
                "load CastInfo sheet",
                lambda: ws.get_all_values(),
            )
            self._rebuild_castinfo_lookup_from_rows()

    def _rebuild_castinfo_lookup_from_rows(self):
        """Recompute cached lookups based on the current CastInfo rows."""
        self._castinfo_by_show = {}
        self._row_lookup = {}

        if not self._castinfo_rows:
            return

        for row_idx, row in enumerate(self._castinfo_rows[1:], start=2):  # Skip header row
            if not row:
                continue

            cast_name = row[0].strip() if len(row) > 0 else ""
            cast_imdb_id = row[2].strip() if len(row) > 2 else ""
            show_name = row[3].strip() if len(row) > 3 else ""
            show_imdb_id = row[4].strip() if len(row) > 4 else ""
            current_episodes = row[6].strip() if len(row) > 6 else ""

            if not show_imdb_id:
                continue

            self._row_lookup[row_idx] = (show_imdb_id, cast_imdb_id or "")

            if not cast_imdb_id:
                continue

            self._castinfo_by_show.setdefault(show_imdb_id, {})[cast_imdb_id] = {
                "row_idx": row_idx,
                "cast_name": cast_name,
                "current_episodes": current_episodes,
                "current_show_name": show_name,
            }

    @staticmethod
    def _group_consecutive_indices(indices: List[int]) -> List[Tuple[int, int]]:
        """Group a sorted list of indices into inclusive ranges for batch deletion."""
        if not indices:
            return []

        grouped: List[Tuple[int, int]] = []
        start = prev = indices[0]

        for idx in indices[1:]:
            if idx == prev + 1:
                prev = idx
                continue

            grouped.append((start, prev))
            start = prev = idx

        grouped.append((start, prev))
        return grouped

    def _show_name_mismatch(self, show_imdb_id: str, expected_name: str) -> bool:
        """Return True if any cached CastInfo row for the show has a different ShowName."""
        if not show_imdb_id:
            return False

        target = (expected_name or "").strip()

        show_entries = self._castinfo_by_show.get(show_imdb_id, {})
        if not show_entries:
            return False

        for entry in show_entries.values():
            current = (entry.get("current_show_name") or "").strip()

            if not current and target:
                return True

            if current and current != target:
                return True

        return False

    @classmethod
    def _parse_recent_episode_date(cls, value: str) -> Optional[date]:
        """Best-effort parser for the ShowInfo "Most Recent Episode" column."""
        raw = (value or "").strip()
        if not raw:
            return None

        candidates = [raw]
        if len(raw) >= 10:
            candidates.append(raw[:10])
        if " " in raw:
            candidates.append(raw.split(" ")[0])
        if raw.endswith("Z"):
            candidates.append(raw[:-1])
            candidates.append(raw[:-1] + "+00:00")

        for candidate in candidates:
            candidate = candidate.strip()
            if not candidate:
                continue

            if len(candidate) == 10 and candidate[4] == "-" and candidate[7] == "-":
                try:
                    return datetime.strptime(candidate, "%Y-%m-%d").date()
                except ValueError:
                    pass

            try:
                parsed = datetime.fromisoformat(candidate)
                return parsed.date()
            except ValueError:
                pass

            for fmt in cls._RECENT_EPISODE_FORMATS:
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    return parsed.date()
                except ValueError:
                    continue

        return None

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Produce a case/punctuation-insensitive key for name comparisons."""
        return "".join(ch for ch in name.lower() if ch.isalnum())
    
    def get_shows_to_process(self) -> Tuple[List[Dict[str, str]], List[str]]:
        """Get all shows from ShowInfo to process (fills CastInfo columns A-G) AND shows to remove."""
        try:
            ws = self._get_showinfo_ws()
            rows = self._with_gspread_retry(
                "load ShowInfo sheet",
                lambda: ws.get_all_values(),
            )
        except Exception as e:
            print(f"❌ Error reading ShowInfo: {e}")
            return [], []
        
        # Get existing show IMDb IDs from CastInfo
        existing_show_ids = self.get_existing_show_ids()
        print(f"Found {len(existing_show_ids)} shows already in CastInfo")
        
        shows = []
        shows_to_remove = []
        missing_count = 0
        qualifying_count = 0
        skipped_stale = 0
        name_mismatch_shows = 0

        today = datetime.now(timezone.utc).date()
        recent_window_start = today - timedelta(days=self.RECENT_EPISODE_WINDOW_DAYS)

        print(f"\n🔍 Analyzing ALL shows from ShowInfo:")

        for row in rows[1:]:  # Skip header
            if len(row) < 8:
                continue
            
            show_name = row[0].strip()      # Column A: Show
            imdb_id = row[4].strip()        # Column E: IMDbSeriesID
            tmdb_id = row[5].strip()        # Column F: TMDbSeriesID
            recent_episode = row[6].strip() # Column G: Most Recent Episode
            threshold_flag = row[7].strip() # Column H: OVERRIDE / thresholds
            
            if not show_name or not imdb_id:
                continue
                
            # Handle SKIP shows - mark for removal and skip processing entirely
            if threshold_flag == "SKIP":
                if imdb_id in existing_show_ids:
                    shows_to_remove.append(imdb_id)
                    print(f"  🗑️  Will remove: {show_name} (marked SKIP)")
                continue  # Don't process SKIP shows at all
            
            # Only process shows that have required data (show name and IMDb ID)
            # This processes ALL shows from ShowInfo regardless of starting letter
            if not show_name or not imdb_id:
                continue
            
            already_in_castinfo = imdb_id in existing_show_ids
            process_show = False

            if already_in_castinfo:
                recent_episode_date = self._parse_recent_episode_date(recent_episode)
                name_mismatch = self._show_name_mismatch(imdb_id, show_name)

                if name_mismatch:
                    print(f"  📝 Will resync names: {show_name} (cast rows out of sync)")
                    process_show = True
                    name_mismatch_shows += 1
                elif recent_episode_date and recent_episode_date >= recent_window_start:
                    print(
                        f"  🔄 Will update: {show_name} (recent episode {recent_episode_date.isoformat()})"
                    )
                    process_show = True
                else:
                    human_date = recent_episode_date.isoformat() if recent_episode_date else (recent_episode or "unknown")
                    print(
                        f"  ⏸️  Skipping: {show_name} (last episode {human_date}) – outside {self.RECENT_EPISODE_WINDOW_DAYS}-day window"
                    )
                    skipped_stale += 1
                    continue
            else:
                print(f"  ➕ Will add: {show_name} (missing from CastInfo)")
                missing_count += 1
                process_show = True
            
            # Parse episode threshold from Column H
            min_episodes = 4  # default for blank entries
            if threshold_flag == "Y":
                min_episodes = 2
            elif threshold_flag and threshold_flag.isdigit():
                min_episodes = int(threshold_flag)
            elif threshold_flag and not threshold_flag == "SKIP":
                # Non-numeric, non-Y, non-SKIP value - use default
                min_episodes = 4
            
            # Special overrides for specific shows
            if "X Factor" in show_name:
                min_episodes = 6  # Override for X Factor shows
            
            if not process_show:
                continue

            shows.append(
                {
                    "name": show_name,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "min_episodes": min_episodes,
                    "recent_episode": recent_episode,
                }
            )
            qualifying_count += 1

            print(f"  📺 {show_name} - Min episodes: {min_episodes} (from column H: '{threshold_flag}')")
        
        print(f"\n📊 Show filtering results:")
        print(f"  🔍 Shows missing from CastInfo: {missing_count}")
        print(f"  🎯 Qualifying shows to process: {qualifying_count}")
        print(f"  🗑️  Shows to remove (SKIP): {len(shows_to_remove)}")
        print(
            f"  ⏸️  Shows skipped (no episode within last {self.RECENT_EPISODE_WINDOW_DAYS} days): {skipped_stale}"
        )
        if name_mismatch_shows:
            print(f"  📝 Shows queued for name resyncs: {name_mismatch_shows}")

        return shows, shows_to_remove
    
    def get_existing_show_ids(self) -> Set[str]:
        """Get set of show IMDb IDs that already exist in CastInfo"""
        try:
            self._ensure_castinfo_cache()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()

        existing_show_ids = set()
        for show_imdb_id, _ in self._row_lookup.values():
            if show_imdb_id:
                existing_show_ids.add(show_imdb_id)

        return existing_show_ids
    
    def remove_shows_from_castinfo(self, shows_to_remove: List[str], dry_run: bool) -> int:
        """Remove all rows for shows marked as SKIP."""
        if not shows_to_remove:
            return 0
            
        try:
            self._ensure_castinfo_cache()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return 0

        ws = self._get_castinfo_ws()

        rows_to_delete = [
            row_idx
            for row_idx, (show_id, _) in self._row_lookup.items()
            if show_id in shows_to_remove
        ]

        rows_to_delete.sort()

        print(f"\n🗑️  Found {len(rows_to_delete)} rows to remove for SKIP shows")

        if not rows_to_delete:
            return 0

        if dry_run:
            print(f"  🔍 DRY RUN - would delete {len(rows_to_delete)} rows")
            return len(rows_to_delete)

        sheet_id = ws.id
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start - 1,
                        "endIndex": end,
                    }
                }
            }
            for start, end in self._group_consecutive_indices(rows_to_delete)
        ]

        try:
            self._with_gspread_retry(
                f"delete {len(rows_to_delete)} CastInfo rows",
                lambda: self.ss.batch_update({"requests": requests}),
                is_write=True,
            )
            time.sleep(self.GOOGLE_SHEETS_POST_WRITE_DELAY)
        except Exception as e:
            print(f"    ❌ Failed to delete rows in batch: {e}")
            return 0

        for row_idx in reversed(rows_to_delete):
            if 0 <= row_idx - 1 < len(self._castinfo_rows):
                self._castinfo_rows.pop(row_idx - 1)

        self._rebuild_castinfo_lookup_from_rows()

        return len(rows_to_delete)
    
    def get_all_existing_cast_imdb_ids(self) -> Set[str]:
        """Get all IMDb IDs that exist in CastInfo."""
        try:
            self._ensure_castinfo_cache()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()

        imdb_ids = set()
        for show_entries in self._castinfo_by_show.values():
            imdb_ids.update(show_entries.keys())

        return imdb_ids
    
    def get_existing_cast_for_show(self, show_imdb_id: str) -> Dict[str, dict]:
        """Get existing cast entries for a specific show."""
        try:
            self._ensure_castinfo_cache()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return {}

        show_entries = self._castinfo_by_show.get(show_imdb_id, {})
        existing_cast: Dict[str, dict] = {}

        for imdb_id, info in show_entries.items():
            existing_cast[imdb_id] = dict(info)

        return existing_cast

    def correct_existing_cast_ids(
        self,
        ws: gspread.Worksheet,
        show_name: str,
        show_imdb_id: str,
        existing_cast: Dict[str, dict],
        api_cast_data: Dict[str, dict],
        global_existing_cast_ids: Set[str],
        dry_run: bool,
    ) -> Tuple[Dict[str, dict], int, int, int]:
        """Fix rows whose IMDb IDs do not appear in the show's IMDb credits.
        Also updates ShowName (column D) if it differs from ShowInfo.

        Returns the updated cast mapping alongside counts for ID replacements, unresolved matches, and name updates.
        """

        if not existing_cast:
            return existing_cast, 0, 0, 0

        try:
            self._ensure_castinfo_cache()
        except Exception as e:
            print(f"    ❌ Error refreshing CastInfo cache: {e}")
            return existing_cast, 0, 0, 0

        # Build lookup from normalized name -> IMDb credits pulled from the API
        api_name_lookup: Dict[str, List[Tuple[str, dict]]] = {}
        for api_imdb_id, credit in api_cast_data.items():
            display_name = credit.get("name", "").strip()
            if not display_name:
                continue
            key = self._normalize_name(display_name)
            if not key:
                continue
            api_name_lookup.setdefault(key, []).append((api_imdb_id, credit))

        processed_entries: List[Dict[str, object]] = []
        name_updates_planned: List[Tuple[int, str]] = []
        id_updates_planned: List[Dict[str, object]] = []
        rows_to_delete_info: List[Dict[str, object]] = []
        ids_to_remove_from_global: Set[str] = set()
        ids_to_add_to_global: Set[str] = set()

        name_updates_count = 0
        id_updates_count = 0
        removed_count = 0

        for imdb_id, cast_info in sorted(existing_cast.items(), key=lambda item: item[1].get("row_idx", 0)):
            cast_name = cast_info.get("cast_name", "")
            row_idx = cast_info.get("row_idx")
            current_show_name = cast_info.get("current_show_name", "")

            entry_snapshot = {
                "row_idx": row_idx,
                "original_imdb_id": imdb_id,
                "final_imdb_id": imdb_id,
                "cast_info": dict(cast_info),
                "keep": True,
            }

            if current_show_name and current_show_name != show_name:
                message = f"    📝 {cast_name} (row {row_idx}): ShowName '{current_show_name}' → '{show_name}' (syncing from ShowInfo)"
                if dry_run:
                    print(f"{message} - DRY RUN")
                    name_updates_count += 1
                else:
                    print(message)
                    name_updates_planned.append((row_idx, show_name))
                entry_snapshot["cast_info"]["current_show_name"] = show_name

            if imdb_id in api_cast_data:
                raw_episodes = api_cast_data[imdb_id].get("episodes")
                try:
                    episodes_val = int(raw_episodes)
                except (TypeError, ValueError):
                    episodes_val = 0

                if episodes_val <= 0:
                    action_text = "would remove" if dry_run else "removing"
                    print(
                        f"    🗑️  {cast_name or 'Unknown name'} (row {row_idx}): 0 recorded episodes – {action_text} from CastInfo"
                    )
                    entry_snapshot["keep"] = False
                    ids_to_remove_from_global.add(imdb_id)
                    if dry_run:
                        removed_count += 1
                    else:
                        rows_to_delete_info.append(
                            {
                                "row_idx": row_idx,
                                "imdb_id": imdb_id,
                                "cast_name": cast_name,
                            }
                        )
                    processed_entries.append(entry_snapshot)
                    continue

                processed_entries.append(entry_snapshot)
                continue

            normalized = self._normalize_name(cast_name)
            potential_matches = api_name_lookup.get(normalized, []) if normalized else []

            if not potential_matches:
                action_text = "would remove" if dry_run else "removing"
                print(
                    f"    🗑️  {cast_name or 'Unknown name'} (row {row_idx}): IMDbID {imdb_id} not in IMDb credits – {action_text} from CastInfo"
                )
                entry_snapshot["keep"] = False
                ids_to_remove_from_global.add(imdb_id)
                if dry_run:
                    removed_count += 1
                else:
                    rows_to_delete_info.append({
                        "row_idx": row_idx,
                        "imdb_id": imdb_id,
                        "cast_name": cast_name,
                    })
                processed_entries.append(entry_snapshot)
                continue

            unique_matches = {match_id for match_id, _ in potential_matches}
            if len(unique_matches) > 1:
                pretty_ids = ", ".join(sorted(unique_matches))
                print(
                    f"    ⚠️  {cast_name} (row {row_idx}): multiple IMDb candidates in API ({pretty_ids}) – leaving existing ID"
                )
                processed_entries.append(entry_snapshot)
                continue

            correct_id, _ = potential_matches[0]

            if correct_id == imdb_id:
                processed_entries.append(entry_snapshot)
                continue

            message = (
                f"    🔁 {cast_name} (row {row_idx}): IMDbID {imdb_id} → {correct_id} (matched by name in API credits)"
            )

            entry_snapshot["final_imdb_id"] = correct_id

            if dry_run:
                print(f"{message} - DRY RUN")
                ids_to_remove_from_global.add(imdb_id)
                ids_to_add_to_global.add(correct_id)
                id_updates_count += 1
            else:
                print(message)
                id_updates_planned.append({
                    "row_idx": row_idx,
                    "old_id": imdb_id,
                    "new_id": correct_id,
                    "cast_name": cast_name,
                })
                ids_to_remove_from_global.add(imdb_id)
                ids_to_add_to_global.add(correct_id)

            processed_entries.append(entry_snapshot)

        ws = self._get_castinfo_ws()
        updated_cast: Dict[str, dict] = {}

        if dry_run:
            for entry in processed_entries:
                if entry["keep"]:
                    updated_cast[entry["final_imdb_id"]] = entry["cast_info"]

            global_existing_cast_ids.difference_update(ids_to_remove_from_global)
            global_existing_cast_ids.update(ids_to_add_to_global)

            return updated_cast, id_updates_count, removed_count, name_updates_count

        value_updates = []
        for row_idx, updated_show_name in name_updates_planned:
            value_updates.append({"range": f"D{row_idx}", "values": [[updated_show_name]]})
        for payload in id_updates_planned:
            value_updates.append({"range": f"C{payload['row_idx']}", "values": [[payload["new_id"]]]})

        value_updates_success = True
        if value_updates:
            try:
                self._with_gspread_retry(
                    f"update {len(value_updates)} CastInfo cells",
                    lambda: ws.batch_update(value_updates),
                    is_write=True,
                )
                time.sleep(self.GOOGLE_SHEETS_POST_WRITE_DELAY)
            except Exception as e:
                print(f"    ❌ Failed to batch update CastInfo values: {e}")
                value_updates_success = False

        if value_updates_success:
            for row_idx, updated_show_name in name_updates_planned:
                if 0 <= row_idx - 1 < len(self._castinfo_rows):
                    row = self._castinfo_rows[row_idx - 1]
                    while len(row) < 4:
                        row.append("")
                    row[3] = updated_show_name
            for payload in id_updates_planned:
                row_idx = payload["row_idx"]
                new_id = payload["new_id"]
                if 0 <= row_idx - 1 < len(self._castinfo_rows):
                    row = self._castinfo_rows[row_idx - 1]
                    while len(row) < 3:
                        row.append("")
                    row[2] = new_id
            name_updates_count = len(name_updates_planned)
            id_updates_count = len(id_updates_planned)
        else:
            name_updates_planned.clear()
            id_updates_planned.clear()
            ids_to_remove_from_global.clear()
            ids_to_add_to_global.clear()

        delete_success = True
        if rows_to_delete_info:
            row_indices = sorted(info["row_idx"] for info in rows_to_delete_info)
            sheet_id = ws.id
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start - 1,
                            "endIndex": end,
                        }
                    }
                }
                for start, end in self._group_consecutive_indices(row_indices)
            ]

            try:
                self._with_gspread_retry(
                    f"delete {len(row_indices)} rows for {show_name}",
                    lambda: self.ss.batch_update({"requests": requests}),
                    is_write=True,
                )
                time.sleep(self.GOOGLE_SHEETS_POST_WRITE_DELAY)
            except Exception as e:
                print(f"    ❌ Failed to delete rows for {show_name}: {e}")
                delete_success = False

            if delete_success:
                for row_idx in reversed(row_indices):
                    if 0 <= row_idx - 1 < len(self._castinfo_rows):
                        self._castinfo_rows.pop(row_idx - 1)
                removed_count = len(row_indices)
            else:
                ids_to_remove_from_global.difference_update({info["imdb_id"] for info in rows_to_delete_info})

        operations_successful = value_updates_success and delete_success

        if operations_successful:
            self._rebuild_castinfo_lookup_from_rows()
            global_existing_cast_ids.difference_update(ids_to_remove_from_global)
            global_existing_cast_ids.update(ids_to_add_to_global)

            for entry in processed_entries:
                if entry["keep"]:
                    updated_cast[entry["final_imdb_id"]] = entry["cast_info"]
        else:
            self._ensure_castinfo_cache(force_refresh=True)
            show_entries = self._castinfo_by_show.get(show_imdb_id, {})
            for imdb_key, info in show_entries.items():
                updated_cast[imdb_key] = dict(info)
            if not value_updates_success:
                name_updates_count = 0
                id_updates_count = 0
            if not delete_success:
                removed_count = 0

        if rows_to_delete_info and not delete_success:
            print(f"    ⚠️  No rows deleted for {show_name} due to earlier error")

        return updated_cast, id_updates_count, removed_count, name_updates_count

    def should_add_cast_member(self, name: str, imdb_id: str, episodes: int, show_min_episodes: int, existing_cast_ids: Set[str]) -> Tuple[bool, str]:
        """Determine if a cast member should be added based on show's minimum threshold."""

        if episodes == 0:
            return False, "No episode data"
        
        is_already_in_castinfo = imdb_id in existing_cast_ids
        
        # ALL cast members must meet the show's minimum threshold, regardless of whether they exist elsewhere
        if episodes >= show_min_episodes:
            if is_already_in_castinfo:
                return True, f"Existing person, {episodes} episodes (meets threshold: {show_min_episodes}+)"
            else:
                return True, f"New person, {episodes} episodes (threshold: {show_min_episodes}+)"
        else:
            if is_already_in_castinfo:
                return False, f"Existing person with only {episodes} episodes (need {show_min_episodes}+)"
            else:
                return False, f"New person with only {episodes} episodes (need {show_min_episodes}+)"
    
    def add_missing_cast_members(
        self,
        ws: gspread.Worksheet,
        show_name: str,
        show_imdb_id: str,
        show_tmdb_id: str,
        api_cast_data: Dict[str, dict],
        existing_cast: Dict[str, dict],
        existing_cast_ids: Set[str],
        show_min_episodes: int,
        dry_run: bool,
    ) -> Tuple[int, List[str]]:
        """Add missing cast members based on show's minimum episode threshold."""

        new_rows = []
        added_ids: List[str] = []
        
        for imdb_id, cast_info in api_cast_data.items():
            # Skip if already exists for this show
            if imdb_id in existing_cast:
                continue
                
            name = cast_info.get('name', '')
            episodes = cast_info.get('episodes', 0)
            characters = cast_info.get('characters') or []
            attributes = cast_info.get('attributes') or []
            is_credited = cast_info.get('is_credited', True)

            if not name or not episodes:
                continue

            def _contains_uncredited(values):
                for value in values:
                    if isinstance(value, str) and 'uncredited' in value.lower():
                        return True
                return False

            if (not is_credited) or _contains_uncredited(characters) or _contains_uncredited(attributes):
                print(f"      ⚠️  Skipping {name} ({imdb_id}) due to uncredited appearance")
                continue
                
            should_add, reason = self.should_add_cast_member(name, imdb_id, episodes, show_min_episodes, existing_cast_ids)
            
            if should_add:
                # Build new row: CastName, CastID, Cast IMDbID, ShowName, Show IMDbID, ShowID, TotalEpisodes, TotalSeasons
                # Only fill columns A-F, leave G&H blank for v2 script
                new_row = [
                    name,               # A: CastName
                    "",                 # B: TMDbCastID (unknown here)
                    imdb_id,            # C: IMDbCastID
                    show_name,          # D: ShowName
                    show_imdb_id,       # E: IMDbSeriesID
                    show_tmdb_id or "", # F: TMDbSeriesID
                    "",                 # G: TotalEpisodes (left for later scripts)
                    "",                 # H: TotalSeasons (left for later scripts)
                ]
                new_rows.append(new_row)
                added_ids.append(imdb_id)
                print(f"      ➕ NEW: {name} ({imdb_id}) - {episodes} episodes ({reason}) [G/H left blank for v2 script]")
                
                if dry_run:
                    print(f"        🔍 DRY RUN - would add")
        
        # Add ALL new rows for this show in a SINGLE batch operation to avoid API issues
        if new_rows and not dry_run:
            try:
                print(f"  📦 Adding {len(new_rows)} cast members in single batch operation...")
                self._with_gspread_retry(
                    f"append {len(new_rows)} CastInfo rows",
                    lambda: ws.append_rows(new_rows),
                    is_write=True,
                )
                time.sleep(self.GOOGLE_SHEETS_POST_WRITE_DELAY)
                print(f"  ✅ Successfully batched {len(new_rows)} new cast members")
                if self._castinfo_rows is not None:
                    self._castinfo_rows.extend(new_rows)
                    self._rebuild_castinfo_lookup_from_rows()
            except Exception as e:
                print(f"  ❌ Error in batch adding cast members: {e}")
                return 0, []
        elif new_rows and dry_run:
            print(f"  📦 Would batch add {len(new_rows)} cast members in single operation")

        return len(new_rows), added_ids
    
    def process_show(self, show_info: Dict[str, str], existing_cast_ids: Set[str], dry_run: bool) -> Tuple[int, int, int, int]:
        """Process a single show - update existing data, fix IMDb IDs, and add missing cast."""
        show_name = show_info["name"]
        show_imdb_id = show_info["imdb_id"]
        show_min_episodes = show_info["min_episodes"]
        
        print(f"\n🎭 Processing: {show_name} ({show_imdb_id}) - Min episodes: {show_min_episodes}")
        
        # Get existing cast for this show
        existing_cast = self.get_existing_cast_for_show(show_imdb_id)
        print(f"  👥 Found {len(existing_cast)} existing cast entries")
        
        # Get API cast data
        api_cast_data = self.imdb_api.get_all_cast_credits(show_imdb_id)
        if not api_cast_data:
            print(f"  ❌ No API data returned")
            return 0, 0, 0, 0
            
        print(f"  📊 API returned {len(api_cast_data)} cast members")
        
        # Reconcile incorrect IMDb IDs, update ShowNames, and update existing entries
        ws = self._get_castinfo_ws()
        existing_cast, cast_id_updates, removed_count, name_updates = self.correct_existing_cast_ids(
            ws,
            show_name,
            show_imdb_id,
            existing_cast,
            api_cast_data,
            existing_cast_ids,
            dry_run,
        )
        # Add missing cast members with show-specific threshold
        additions_made, added_ids = self.add_missing_cast_members(
            ws,
            show_name,
            show_imdb_id,
            show_info.get("tmdb_id", ""),
            api_cast_data,
            existing_cast,
            existing_cast_ids,
            show_min_episodes,
            dry_run,
        )
        
        # Update global existing_cast_ids set with new additions
        if not dry_run and added_ids:
            existing_cast_ids.update(added_ids)

        action_text = "would update" if dry_run else "updated"
        print(f"  🔁 {action_text} {cast_id_updates} IMDbCastIDs")
        if name_updates:
            name_text = "would update" if dry_run else "updated"
            print(f"  📝 {name_text} {name_updates} ShowNames from ShowInfo")
        if removed_count:
            remove_text = "would remove" if dry_run else "removed"
            print(f"  🗑️  {remove_text} {removed_count} cast rows not present on IMDb")
        print(f"  ➕ {'Would add' if dry_run else 'Added'} {additions_made} new cast")

        return additions_made, cast_id_updates, removed_count, name_updates
    
    def run_update(self, dry_run: bool = False):
        """Main update process for shows A-G."""
        print("🚀 Starting enhanced CastInfo update for all shows...")
        print("   📝 Will add missing cast members based on column H thresholds")
        print("   🎯 Processing ALL shows from ShowInfo using column H minimum episode thresholds") 
        print("   🗑️  Will remove cast for shows marked SKIP")
        print("   📊 Default minimum: 4 episodes (blank column H)")
        print("    Fills CastInfo columns A-F only (G&H left for v2 script)")
        
        shows, shows_to_remove = self.get_shows_to_process()
        
        # First, remove shows marked as SKIP
        if shows_to_remove:
            print(f"\n🗑️  Removing cast for {len(shows_to_remove)} SKIP shows...")
            removed_rows = self.remove_shows_from_castinfo(shows_to_remove, dry_run)
            print(f"  {'Would remove' if dry_run else 'Removed'} {removed_rows} cast rows for SKIP shows")
        
        if not shows:
            print("❌ No qualifying shows found to process")
            return
        
        # Get all existing IMDb IDs for cross-referencing
        print("📊 Loading existing cast IMDb IDs...")
        existing_cast_ids = self.get_all_existing_cast_imdb_ids()
        print(f"  Found {len(existing_cast_ids)} unique cast members across all shows")
        
        total_additions = 0
        total_id_fixes = 0
        total_removed = 0
        total_name_updates = 0

        for idx, show_info in enumerate(shows, start=1):
            additions, id_fixes, removed, name_updates = self.process_show(
                show_info,
                existing_cast_ids,
                dry_run
            )
            total_additions += additions
            total_id_fixes += id_fixes
            total_removed += removed
            total_name_updates += name_updates
            
            # Add delay between shows to avoid rate limits
            if not dry_run:
                time.sleep(2.0)  # 2 second delay between shows
            
            # After every 100 shows, pause briefly to avoid read quota bursts
            if idx % 100 == 0:
                pause_seconds = 30 if dry_run else 45
                print(f"⏱️ Processed {idx} shows – pausing for {pause_seconds}s to stay under Google Sheets quotas…")
                time.sleep(pause_seconds)
        
        print(f"\n🎉 Processing complete!")
        print(f"  📊 Total shows processed: {len(shows)}")
        print(f"  🔄 Episode updates: NONE (columns G/H untouched)")
        print(f"  ➕ Total new cast added: {total_additions}")
        action_text = "would update" if dry_run else "updated"
        print(f"  🔁 {action_text} {total_id_fixes} IMDbCastIDs")
        if total_name_updates:
            name_text = "would update" if dry_run else "updated"
            print(f"  📝 {name_text} {total_name_updates} ShowNames synced from ShowInfo")
        remove_text = "would remove" if dry_run else "removed"
        if total_removed:
            print(f"  🗑️  {remove_text} {total_removed} cast rows not present on IMDb")
        if shows_to_remove:
            print(f"  🗑️  Shows removed/cleaned: {len(shows_to_remove)}")
        if dry_run:
            print("  🔍 This was a DRY RUN - no actual changes made")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enhanced CastInfo updater: add missing cast for all shows from ShowInfo (fills columns A-F only, G&H left for v2 script)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    args = parser.parse_args()
    
    updater = EnhancedCastInfoUpdater()
    updater.run_update(dry_run=args.dry_run)

if __name__ == "__main__":
    main()

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

NOTE: Only fills columns A-F. Columns G (TotalEpisodes) and H (TotalSeasons) are left 
blank for the v2UniversalSeasonExtractor script to fill later.
The "A-F" refers to filling CastInfo columns A-F, not filtering ShowInfo alphabetically.
"""

import gspread
import requests
import time
from typing import Dict, List, Set, Tuple

class IMDbAPIClient:
    """Client for IMDbAPI.dev to get accurate episode counts."""
    
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        self.session.timeout = 30  # 30 second timeout
        self._cache = {}
        
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
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
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
    
    def __init__(self):
        # Connect to Google Sheets
        SERVICE_KEY_PATH = "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
        gc = gspread.service_account(filename=SERVICE_KEY_PATH)
        self.ss = gc.open("Realitease2025Data")
        self.imdb_api = IMDbAPIClient()

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Produce a case/punctuation-insensitive key for name comparisons."""
        return "".join(ch for ch in name.lower() if ch.isalnum())
    
    def get_shows_to_process(self) -> Tuple[List[Dict[str, str]], List[str]]:
        """Get all shows from ShowInfo to process (fills CastInfo columns A-G) AND shows to remove."""
        try:
            ws = self.ss.worksheet("ShowInfo")
            rows = ws.get_all_values()
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
            
            # Check if show exists in CastInfo - we'll process both existing and new shows
            already_in_castinfo = imdb_id in existing_show_ids
            if already_in_castinfo:
                print(f"  🔄 Will update: {show_name} (already in CastInfo - will update episodes)")
            else:
                print(f"  ➕ Will add: {show_name} (missing from CastInfo)")
                missing_count += 1
            
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
            
            shows.append({
                "name": show_name,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "min_episodes": min_episodes,
                "recent_episode": recent_episode
            })
            qualifying_count += 1
            
            print(f"  📺 {show_name} - Min episodes: {min_episodes} (from column H: '{threshold_flag}')")
        
        print(f"\n📊 Show filtering results:")
        print(f"  🔍 Shows missing from CastInfo: {missing_count}")
        print(f"  🎯 Qualifying shows to process: {qualifying_count}")
        print(f"  🗑️  Shows to remove (SKIP): {len(shows_to_remove)}")
        
        return shows, shows_to_remove
    
    def get_existing_show_ids(self) -> Set[str]:
        """Get set of show IMDb IDs that already exist in CastInfo"""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()
        
        existing_show_ids = set()
        
        for row in all_data[1:]:  # Skip header
            if len(row) > 4:
                show_imdb_id = row[4].strip()  # E: Show IMDbID
                if show_imdb_id:
                    existing_show_ids.add(show_imdb_id)
        
        return existing_show_ids
    
    def remove_shows_from_castinfo(self, shows_to_remove: List[str], dry_run: bool) -> int:
        """Remove all rows for shows marked as SKIP."""
        if not shows_to_remove:
            return 0
            
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return 0
        
        rows_to_delete = []
        
        # Find rows to delete (work backwards to maintain row numbers)
        for i in range(len(all_data) - 1, 0, -1):  # Skip header, work backwards
            row = all_data[i]
            if len(row) > 4:
                show_imdb_id = row[4].strip()  # E: Show IMDbID
                if show_imdb_id in shows_to_remove:
                    rows_to_delete.append(i + 1)  # +1 for 1-based indexing
        
        print(f"\n🗑️  Found {len(rows_to_delete)} rows to remove for SKIP shows")
        
        if rows_to_delete and not dry_run:
            # Delete rows one by one (in reverse order to maintain indices)
            for row_idx in sorted(rows_to_delete, reverse=True):
                try:
                    ws.delete_rows(row_idx)
                    time.sleep(0.5)  # Rate limiting
                except Exception as e:
                    print(f"    ❌ Failed to delete row {row_idx}: {e}")
        
        if dry_run and rows_to_delete:
            print(f"  🔍 DRY RUN - would delete {len(rows_to_delete)} rows")
        
        return len(rows_to_delete)
    
    def get_all_existing_cast_imdb_ids(self) -> Set[str]:
        """Get all IMDb IDs that exist in CastInfo."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()
            
        imdb_ids = set()
        for row in all_values[1:]:  # Skip header
            if len(row) > 2:  # Column C - Cast IMDbID
                imdb_id = row[2].strip()
                if imdb_id and imdb_id.startswith("nm"):
                    imdb_ids.add(imdb_id)
        
        return imdb_ids
    
    def get_existing_cast_for_show(self, show_imdb_id: str) -> Dict[str, dict]:
        """Get existing cast entries for a specific show."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return {}
        
        existing_cast = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) < 8:
                continue
                
            cast_imdb_id = row[2].strip()  # Column C
            show_imdb_id_row = row[4].strip()  # Column E
            current_episodes = row[6].strip()  # Column G
            cast_name = row[0].strip()  # Column A
            
            if show_imdb_id_row == show_imdb_id and cast_imdb_id:
                existing_cast[cast_imdb_id] = {
                    "row_idx": i,
                    "cast_name": cast_name,
                    "current_episodes": current_episodes
                }

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
    ) -> Tuple[Dict[str, dict], int, int]:
        """Fix rows whose IMDb IDs do not appear in the show's IMDb credits.

        Returns the updated cast mapping alongside counts for ID replacements and unresolved matches.
        """

        if not existing_cast:
            return existing_cast, 0, 0

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

        updated_cast: Dict[str, dict] = {}
        id_updates = 0
        removals = 0
        rows_to_delete: List[int] = []

        for imdb_id, cast_info in existing_cast.items():
            cast_name = cast_info.get("cast_name", "")
            row_idx = cast_info.get("row_idx")

            if imdb_id in api_cast_data:
                updated_cast[imdb_id] = cast_info
                continue

            normalized = self._normalize_name(cast_name)
            potential_matches = api_name_lookup.get(normalized, []) if normalized else []

            if not potential_matches:
                action_text = "would remove" if dry_run else "removing"
                print(
                    f"    🗑️  {cast_name or 'Unknown name'} (row {row_idx}): IMDbID {imdb_id} not in IMDb credits – {action_text} from CastInfo"
                )
                if not dry_run:
                    rows_to_delete.append(row_idx)
                removals += 1
                global_existing_cast_ids.discard(imdb_id)
                continue

            unique_matches = {match_id for match_id, _ in potential_matches}
            if len(unique_matches) > 1:
                pretty_ids = ", ".join(sorted(unique_matches))
                print(
                    f"    ⚠️  {cast_name} (row {row_idx}): multiple IMDb candidates in API ({pretty_ids}) – leaving existing ID"
                )
                updated_cast[imdb_id] = cast_info
                continue

            correct_id, _ = potential_matches[0]

            if correct_id == imdb_id:
                updated_cast[imdb_id] = cast_info
                continue

            message = (
                f"    🔁 {cast_name} (row {row_idx}): IMDbID {imdb_id} → {correct_id} (matched by name in API credits)"
            )

            if dry_run:
                print(f"{message} - DRY RUN")
                global_existing_cast_ids.discard(imdb_id)
                global_existing_cast_ids.add(correct_id)
                updated_cast[correct_id] = cast_info
                id_updates += 1
                continue

            try:
                ws.update_cell(row_idx, 3, correct_id)
                time.sleep(0.5)  # avoid rate limits
                print(message)
            except Exception as e:
                print(
                    f"    ❌ Failed to update IMDbID for {cast_name} on row {row_idx}: {e}"
                )
                updated_cast[imdb_id] = cast_info
                continue

            global_existing_cast_ids.discard(imdb_id)
            global_existing_cast_ids.add(correct_id)
            updated_cast[correct_id] = cast_info
            id_updates += 1

        if rows_to_delete and not dry_run:
            for row_idx in sorted(rows_to_delete, reverse=True):
                try:
                    ws.delete_rows(row_idx)
                    time.sleep(0.5)
                except Exception as e:
                    print(f"    ❌ Failed to delete row {row_idx}: {e}")

        if dry_run and rows_to_delete:
            print(f"    🔍 DRY RUN - would delete {len(rows_to_delete)} rows")

        return updated_cast, id_updates, removals

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
    ) -> int:
        """Add missing cast members based on show's minimum episode threshold."""
        
        new_rows = []
        
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
                print(f"      ➕ NEW: {name} ({imdb_id}) - {episodes} episodes ({reason}) [G/H left blank for v2 script]")
                
                if dry_run:
                    print(f"        🔍 DRY RUN - would add")
        
        # Add ALL new rows for this show in a SINGLE batch operation to avoid API issues
        if new_rows and not dry_run:
            try:
                print(f"  📦 Adding {len(new_rows)} cast members in single batch operation...")
                ws.append_rows(new_rows)
                print(f"  ✅ Successfully batched {len(new_rows)} new cast members")
            except Exception as e:
                print(f"  ❌ Error in batch adding cast members: {e}")
                return 0
        elif new_rows and dry_run:
            print(f"  📦 Would batch add {len(new_rows)} cast members in single operation")
        
        return len(new_rows)
    
    def process_show(self, show_info: Dict[str, str], existing_cast_ids: Set[str], dry_run: bool) -> Tuple[int, int, int]:
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
            return 0, 0, 0
            
        print(f"  📊 API returned {len(api_cast_data)} cast members")
        
        # Reconcile incorrect IMDb IDs and update existing entries
        ws = self.ss.worksheet("CastInfo")
        existing_cast, cast_id_updates, removed_count = self.correct_existing_cast_ids(
            ws,
            show_name,
            show_imdb_id,
            existing_cast,
            api_cast_data,
            existing_cast_ids,
            dry_run,
        )
        # Add missing cast members with show-specific threshold
        additions_made = self.add_missing_cast_members(
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
        if not dry_run and additions_made > 0:
            for imdb_id, cast_info in api_cast_data.items():
                if imdb_id not in existing_cast:  # New addition
                    existing_cast_ids.add(imdb_id)

        action_text = "would update" if dry_run else "updated"
        print(f"  🔁 {action_text} {cast_id_updates} IMDbCastIDs")
        if removed_count:
            remove_text = "would remove" if dry_run else "removed"
            print(f"  🗑️  {remove_text} {removed_count} cast rows not present on IMDb")
        print(f"  ➕ {'Would add' if dry_run else 'Added'} {additions_made} new cast")

        return additions_made, cast_id_updates, removed_count
    
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

        for idx, show_info in enumerate(shows, start=1):
            additions, id_fixes, removed = self.process_show(
                show_info,
                existing_cast_ids,
                dry_run
            )
            total_additions += additions
            total_id_fixes += id_fixes
            total_removed += removed
            
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

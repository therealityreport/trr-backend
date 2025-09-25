#!/usr/bin/env python3
"""RealiteaseInfo Step 2: Populate show data (columns D–I)."""

from __future__ import annotations

import sys
import time
import re
import os
from typing import Dict
from collections import defaultdict
from pathlib import Path


def _bootstrap_environment() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    venv_lib = repo_root / ".venv" / "lib"

    if venv_lib.exists():
        site_candidates = sorted(venv_lib.glob("python*/site-packages"), reverse=True)
        for candidate in site_candidates:
            if candidate.is_dir():
                path_str = str(candidate)
                if path_str not in sys.path:
                    sys.path.insert(0, path_str)
                break

    return repo_root


REPO_ROOT = _bootstrap_environment()

import gspread  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

# Load environment variables from repo root
load_dotenv(REPO_ROOT / ".env")

class RealiteaseInfoStep2:
    def __init__(self):
        print("🎬 Starting RealiteaseInfo Step 2: Show Data Population")
        print("=" * 60)
        
        # Initialize Google Sheets
        try:
            self.gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
            self.spreadsheet = self.gc.open('Realitease2025Data')
            print("✅ Connected to Google Sheets")
        except Exception as e:
            print(f"❌ Failed to connect to Google Sheets: {e}")
            raise
            
        # Get sheets
        self.cast_info_sheet = self.get_sheet('CastInfo')
        self.realitease_sheet = self.get_sheet('RealiteaseInfo')
        self.show_info_sheet = self.get_sheet('ShowInfo')
        
        # Load data
        self.cast_info_data = self.load_cast_info_data()
        self.show_info_data = self.load_show_info_data()

        # Pre-build show info lookup for quick access
        self.show_info_lookup = self._build_show_info_lookup(self.show_info_data)
        print(f"✅ Built ShowInfo lookup for {len(self.show_info_lookup)} shows")
        self.cast_info_lookup = self._build_cast_info_lookup(self.cast_info_data)
        print(f"✅ Built CastInfo lookup for {len(self.cast_info_lookup)} cast members")
        
    def get_sheet(self, sheet_name):
        """Get a specific sheet by name"""
        try:
            sheet = self.spreadsheet.worksheet(sheet_name)
            print(f"✅ Found {sheet_name} sheet")
            return sheet
        except gspread.exceptions.WorksheetNotFound:
            print(f"❌ {sheet_name} sheet not found")
            raise
            
    def load_cast_info_data(self):
        """Load all data from CastInfo sheet"""
        print("🔄 Loading CastInfo data...")
        try:
            cast_headers = [
                'Name', 'TMDbCastID', 'IMDbCastID', 'ShowName',
                'IMDbSeriesID', 'TMDbSeriesID', 'TotalEpisodes', 'TotalSeasons'
            ]
            data = self.cast_info_sheet.get_all_records(expected_headers=cast_headers)
            print(f"📊 Loaded {len(data)} records from CastInfo")
            return data
        except Exception as e:
            print(f"❌ Failed to load CastInfo data: {e}")
            return []
            
    def load_show_info_data(self):
        """Load all data from ShowInfo sheet"""
        print("🔄 Loading ShowInfo data...")
        try:
            show_headers = [
                'Show', 'Network', 'ShowTotalSeasons', 'ShowTotalEpisodes',
                'IMDbSeriesID', 'TMDbSeriesID', 'Most Recent Episode', 'OVERRIDE'
            ]
            data = self.show_info_sheet.get_all_records(expected_headers=show_headers)
            print(f"📊 Loaded {len(data)} records from ShowInfo")
            return data
        except Exception as e:
            print(f"❌ Failed to load ShowInfo data: {e}")
            return []

    @staticmethod
    def _build_show_info_lookup(show_records):
        lookup = {}
        for record in show_records:
            imdb_id = (record.get('IMDbSeriesID') or '').strip()
            if not imdb_id:
                continue

            tmdb_val = record.get('TMDbSeriesID', '')
            tmdb_id = str(tmdb_val).strip() if tmdb_val is not None else ''
            if tmdb_id.lower() == 'none':
                tmdb_id = ''

            lookup[imdb_id] = {
                'show_name': (record.get('Show') or '').strip(),
                'tmdb_id': tmdb_id,
                'total_seasons': record.get('ShowTotalSeasons', ''),
                'total_episodes': record.get('ShowTotalEpisodes', ''),
            }
        return lookup

    @staticmethod
    def _build_cast_info_lookup(cast_records):
        cast_lookup = defaultdict(list)
        for record in cast_records:
            cast_imdb_id = (record.get('IMDbCastID') or '').strip()
            if cast_imdb_id:
                cast_lookup[cast_imdb_id].append(record)
        return cast_lookup

    def _aggregate_show_data_per_cast(self, cast_lookup, show_info_lookup):
        aggregated = {}

        for cast_imdb_id, records in cast_lookup.items():
            per_show: Dict[str, dict] = {}

            for record in records:
                show_imdb_id = (record.get('IMDbSeriesID') or '').strip()
                show_name = (record.get('ShowName') or '').strip()
                if not show_imdb_id and not show_name:
                    continue

                show_info = show_info_lookup.get(show_imdb_id, {}) if show_imdb_id else {}
                if not show_name:
                    show_name = show_info.get('show_name', '')

                show_tmdb_id = str(record.get('TMDbSeriesID') or '').strip()
                if show_tmdb_id.lower() == 'none':
                    show_tmdb_id = ''
                if not show_tmdb_id:
                    show_tmdb_id = str(show_info.get('tmdb_id') or '').strip()
                    if show_tmdb_id.lower() == 'none':
                        show_tmdb_id = ''

                key = show_imdb_id or f"name::{show_name.lower()}"
                entry = per_show.setdefault(
                    key,
                    {
                        'name': show_name,
                        'tmdb_id': show_tmdb_id,
                        'episodes': 0,
                        'seasons': set(),
                        'imdb_id': show_imdb_id,
                    },
                )

                # Update name/tmdb if we encounter better data later
                if not entry['name'] and show_name:
                    entry['name'] = show_name
                if not entry['tmdb_id'] and show_tmdb_id:
                    entry['tmdb_id'] = show_tmdb_id
                if not entry['imdb_id'] and show_imdb_id:
                    entry['imdb_id'] = show_imdb_id

                episodes_val = record.get('TotalEpisodes', '')
                entry['episodes'] += self._parse_positive_int(episodes_val)

                seasons_val = record.get('TotalSeasons', '')
                seasons_text = str(seasons_val).strip() if seasons_val is not None else ''
                if seasons_text and seasons_text.lower() != 'none':
                    parts = re.split(r'[;,]', seasons_text)
                    for part in parts:
                        part = part.strip()
                        if not part:
                            continue
                        numbers = re.findall(r'\d+', part)
                        if numbers:
                            for number in numbers:
                                entry['seasons'].add(number)
                        else:
                            entry['seasons'].add(part.lower())

            shows = []
            show_imdb_ids = []
            show_tmdb_ids = []
            total_seasons = 0
            total_episodes = 0

            for entry in per_show.values():
                show_name = entry.get('name', '')
                if not show_name:
                    continue
                shows.append(show_name)

                imdb_value = entry.get('imdb_id', '') or ''
                if imdb_value:
                    show_imdb_ids.append(imdb_value)
                else:
                    show_imdb_ids.append('')

                tmdb_value = str(entry.get('tmdb_id') or '').strip()
                if tmdb_value.lower() == 'none':
                    tmdb_value = ''
                show_tmdb_ids.append(tmdb_value)

                total_seasons += len(entry.get('seasons', set()))
                total_episodes += entry.get('episodes', 0)

            aggregated[cast_imdb_id] = {
                'shows': shows,
                'show_imdb_ids': show_imdb_ids,
                'show_tmdb_ids': show_tmdb_ids,
                'total_seasons': total_seasons,
                'total_episodes': total_episodes,
                'total_shows': len(shows),
            }

        return aggregated

    @staticmethod
    def _parse_positive_int(value):
        """Parse integers safely from mixed strings (returns 0 when absent)."""
        if value is None:
            return 0
        value = str(value).strip()
        if not value or value.lower() in {"none", "nan", "n/a"}:
            return 0
        match = re.search(r'-?\d+', value.replace(',', ''))
        if match:
            try:
                return max(0, int(match.group()))
            except ValueError:
                pass
        try:
            return max(0, int(float(value)))
        except (ValueError, TypeError):
            return 0
            
    def aggregate_show_data_by_realitease_order(self, limit_records=None):
        """Process RealiteaseInfo in consecutive row order, updating show data for each cast member.
        This version reads RealiteaseInfo column B (CastIMDbID) **by position** and matches it to
        CastInfo column C (IMDbCastID). It also tracks the true sheet row numbers for safe updates.
        """
        print("📊 Processing RealiteaseInfo in consecutive row order...")

        # Limit cast info if requested
        if limit_records:
            print(f"📊 Processing first {limit_records} CastInfo records (for aggregation)")
            limited_records = self.cast_info_data[:limit_records]
            self.cast_info_lookup = self._build_cast_info_lookup(limited_records)
        else:
            print(f"📊 Processing all {len(self.cast_info_data)} CastInfo records")

        # Precompute aggregated show data for each cast member (keyed by IMDbCastID)
        print("  🔄 Aggregating show data per cast member...")
        aggregated_show_data = self._aggregate_show_data_per_cast(self.cast_info_lookup, self.show_info_lookup)
        print(f"  ✅ Aggregated show data for {len(aggregated_show_data)} cast members")

        # Read ALL values so we can reference **physical** row numbers and columns
        print("  📋 Reading RealiteaseInfo sheet values...")
        all_values = self.realitease_sheet.get_all_values()
        if not all_values:
            print("  ❌ RealiteaseInfo sheet is empty")
            return []

        # Row 1 is headers. Data starts at row 2
        consecutive_updates = []
        skipped_without_castinfo = 0

        for idx in range(1, len(all_values)):
            row = all_values[idx]
            # Expect: A=CastName (0), B=CastIMDbID (1)
            cast_name = (row[0].strip() if len(row) > 0 and row[0] else 'Unknown')
            cast_imdb_id = (row[1].strip() if len(row) > 1 and row[1] else '')

            if not cast_imdb_id:
                skipped_without_castinfo += 1
                continue

            show_data = aggregated_show_data.get(cast_imdb_id)
            if not show_data or not show_data['shows']:
                skipped_without_castinfo += 1
                continue

            # Compose output columns D–I
            show_names = ', '.join(show_data['shows'])
            show_imdb_ids = ', '.join([sid for sid in show_data['show_imdb_ids'] if sid])
            show_tmdb_ids = ', '.join([tid for tid in show_data['show_tmdb_ids'] if tid])
            total_shows = len(show_data['shows'])
            total_seasons = show_data['total_seasons']
            total_episodes = show_data['total_episodes']

            # Use actual sheet row number (header is row 1)
            consecutive_updates.append({
                'row_num': idx + 1,
                'cast_name': cast_name,
                'cast_imdb_id': cast_imdb_id,
                'data': [show_names, show_imdb_ids, show_tmdb_ids, total_shows, total_seasons, total_episodes]
            })

        if skipped_without_castinfo:
            print(
                f"  ⚠️ Skipped {skipped_without_castinfo} rows with no CastInfo linkage "
                "(leaving existing values untouched)"
            )

        print(f"  ✅ Prepared {len(consecutive_updates)} consecutive updates")
        return consecutive_updates
        
    def process_batch_updates(self, batch_updates, dry_run=True):
        """Process a batch of updates to the sheet"""
        if not batch_updates:
            return 0
            
        if dry_run:
            print(f"  🔍 DRY RUN: Would process batch of {len(batch_updates)} updates")
            return len(batch_updates)
        
        # Sort batch by row number to ensure consistent ordering
        batch_updates.sort(key=lambda x: x['row_num'])
        
        # Check if rows are consecutive for range update
        row_numbers = [update['row_num'] for update in batch_updates]
        is_consecutive = all(row_numbers[i] == row_numbers[i-1] + 1 for i in range(1, len(row_numbers)))
        
        if is_consecutive and len(batch_updates) > 1:
            # Use range update for consecutive rows
            start_row = row_numbers[0]
            end_row = row_numbers[-1]
            range_name = f'D{start_row}:I{end_row}'
            
            batch_data = [update['data'] for update in batch_updates]
            print(f"  📝 Batch updating {len(batch_updates)} consecutive rows ({start_row}-{end_row})")
            self.realitease_sheet.update(values=batch_data, range_name=range_name)
            time.sleep(1)  # Minimal rate limiting for batch
            
            for update in batch_updates:
                print(f"    ✅ Updated row {update['row_num']} - {update['cast_name']}")
        else:
            # Update individual rows in batch
            print(f"  📝 Batch updating {len(batch_updates)} non-consecutive rows")
            
            # Group consecutive rows for sub-batches
            sub_batches = []
            current_batch = [batch_updates[0]]
            
            for i in range(1, len(batch_updates)):
                if batch_updates[i]['row_num'] == current_batch[-1]['row_num'] + 1:
                    current_batch.append(batch_updates[i])
                else:
                    sub_batches.append(current_batch)
                    current_batch = [batch_updates[i]]
            sub_batches.append(current_batch)
            
            # Process each sub-batch
            for sub_batch in sub_batches:
                if len(sub_batch) == 1:
                    # Single row update
                    update = sub_batch[0]
                    range_name = f'D{update["row_num"]}:I{update["row_num"]}'
                    self.realitease_sheet.update(values=[update['data']], range_name=range_name)
                    print(f"    ✅ Updated row {update['row_num']} - {update['cast_name']}")
                else:
                    # Consecutive sub-batch
                    start_row = sub_batch[0]['row_num']
                    end_row = sub_batch[-1]['row_num']
                    range_name = f'D{start_row}:I{end_row}'
                    batch_data = [update['data'] for update in sub_batch]
                    self.realitease_sheet.update(values=batch_data, range_name=range_name)
                    
                    for update in sub_batch:
                        print(f"    ✅ Updated row {update['row_num']} - {update['cast_name']}")
                
                time.sleep(0.5)  # Small delay between sub-batches
        
        return len(batch_updates)
        
    def update_show_data_consecutive(self, batch_updates, dry_run=True):
        """Update RealiteaseInfo in strictly consecutive row blocks with retry logic.
        This avoids writing into unintended rows when there are gaps between target rows.
        """
        if not batch_updates:
            return 0

        # Sort by row number to ensure consistent ordering
        batch_updates.sort(key=lambda x: x['row_num'])

        # Split into truly consecutive runs
        runs = []
        current = [batch_updates[0]]
        for upd in batch_updates[1:]:
            if upd['row_num'] == current[-1]['row_num'] + 1:
                current.append(upd)
            else:
                runs.append(current)
                current = [upd]
        runs.append(current)

        total_rows_updated = 0

        for run in runs:
            start_row = run[0]['row_num']
            end_row = run[-1]['row_num']
            range_name = f'D{start_row}:I{end_row}'
            batch_data = [u['data'] for u in run]

            try:
                existing_values = self.realitease_sheet.get(range_name)
            except Exception:
                existing_values = []

            changed_cells = self._count_changed_cells(existing_values, batch_data)

            if dry_run:
                print(
                    f"    🔍 DRY RUN: Would batch update rows {start_row}-{end_row} ("
                    f"{len(run)} rows / {changed_cells} cells changed)"
                )
                total_rows_updated += len(run)
                continue

            print(
                f"    📝 Batch updating rows {start_row}-{end_row} ("
                f"{len(run)} rows / {changed_cells} cells)"
            )

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.realitease_sheet.update(values=batch_data, range_name=range_name)
                    for u in run:
                        print(f"    ✅ Updated row {u['row_num']} - {u['cast_name']}")
                    break
                except Exception as e:
                    if "429" in str(e) or "quota" in str(e).lower():
                        wait_time = (attempt + 1) * 60
                        print(
                            f"    ⚠️  API quota limit hit. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}..."
                        )
                        time.sleep(wait_time)
                        if attempt == max_retries - 1:
                            print(f"    ❌ Failed after {max_retries} retries: {str(e)}")
                            raise
                    else:
                        print(f"    ❌ Update failed: {str(e)}")
                        raise

            total_rows_updated += len(run)

        return total_rows_updated

    @staticmethod
    def _count_changed_cells(existing_values, new_values):
        changed = 0
        rows = max(len(existing_values), len(new_values))
        for r in range(rows):
            existing_row = existing_values[r] if r < len(existing_values) else []
            new_row = new_values[r] if r < len(new_values) else []
            cols = max(len(existing_row), len(new_row))
            for c in range(cols):
                existing_cell = existing_row[c] if c < len(existing_row) else ''
                new_cell = new_row[c] if c < len(new_row) else ''
                if str(existing_cell) != str(new_cell):
                    changed += 1
        return changed
        
    def run(self, dry_run=True, limit_records=None, batch_size=500, start_batch=1):
        """Run Step 2: Update show data with consecutive processing"""
        try:
            # Load required data
            self.load_cast_info_data()
            self.load_show_info_data()
            
            # Aggregate show data in RealiteaseInfo row order for consecutive updates
            consecutive_updates = self.aggregate_show_data_by_realitease_order(limit_records=limit_records)
            
            if not consecutive_updates:
                print("✅ No updates needed - all show data is current!")
                return
            
            print(f"📊 Found {len(consecutive_updates)} cast members to process")
            print(f"🔄 Processing in batches of {batch_size}, starting from batch {start_batch}")
            
            total_updates_made = 0
            
            # Calculate starting point
            start_index = (start_batch - 1) * batch_size
            if start_index >= len(consecutive_updates):
                print(f"❌ Start batch {start_batch} is beyond available data")
                return
            
            # Process updates in smaller batches starting from specified batch
            for i in range(start_index, len(consecutive_updates), batch_size):
                batch = consecutive_updates[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(consecutive_updates) + batch_size - 1) // batch_size
                
                print(f"\n📦 Processing batch {batch_num}/{total_batches}: rows {batch[0]['row_num']}-{batch[-1]['row_num']} ({len(batch)} updates)")
                
                try:
                    # Update this batch
                    updates_made = self.update_show_data_consecutive(batch, dry_run=dry_run)
                    total_updates_made += updates_made
                    
                    if not dry_run:
                        print(f"   ✅ Batch {batch_num} complete: {updates_made} updates made")
                        
                        # Save progress checkpoint
                        with open('step2_progress.txt', 'w') as f:
                            f.write(f"Last completed batch: {batch_num}\n")
                        
                        # Rest every 10 batches to avoid API quota limits
                        if batch_num % 20 == 0 and batch_num < total_batches:
                            print(f"   😴 Resting for 15 seconds after batch {batch_num} to avoid API limits...")
                            time.sleep(15)
                        else:
                            time.sleep(2)  # Normal pause between batches
                
                except Exception as e:
                    print(f"❌ Error in batch {batch_num}: {str(e)}")
                    print(f"💾 Progress saved - you can resume from batch {batch_num}")
                    with open('step2_progress.txt', 'w') as f:
                        f.write(f"Failed at batch: {batch_num}\nResume from batch: {batch_num}\n")
                    raise
            
            print(f"\n🎉 STEP 2 SUMMARY:")
            print(f"   📊 Total cast members processed: {len(consecutive_updates)}")
            print(f"   🔄 Show data updates made: {total_updates_made}")
            
            if dry_run:
                print(f"\n🔍 This was a DRY RUN - no data was actually written")
                print(f"   To execute for real, set dry_run=False")
            else:
                print(f"\n✅ All show data has been updated in RealiteaseInfo!")
                # Clean up progress file on successful completion
                try:
                    os.remove('step2_progress.txt')
                except:
                    pass
                
        except Exception as e:
            print(f"❌ Error in Step 2: {e}")
            raise


def main():
    """Main function"""
    try:
        # Initialize and run Step 2
        step2 = RealiteaseInfoStep2()
        # Run the corrected script to rebuild all show data with proper column mappings
        step2.run(dry_run=False, limit_records=None, batch_size=500, start_batch=1)
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")


if __name__ == "__main__":
    main()

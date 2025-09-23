#!/usr/bin/env python3
"""
RealiteaseInfo Step 1: Populate/refresh cast members (Columns A–E)

This script:
1. Reads CastInfo sheet
2. Identifies unique cast members by Cast IMDb ID
3. Adds missing cast members to RealiteaseInfo sheet (columns A–E)
4. Updates existing rows' Cast IMDb/TMDb IDs and show lists (columns B–E) when CastInfo changes
5. Preserves the remaining bio/enrichment columns
"""

import gspread
import time
import re
from collections import defaultdict
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class RealiteaseInfoStep1:
    def __init__(self):
        print("🎬 Starting RealiteaseInfo Step 1: Cast Member Population")
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
        self.realitease_sheet = self.get_or_create_realitease_sheet()
        
        # Load data
        self.cast_info_data = self.load_cast_info_data()
        self.qualifying_cast_ids = set()

    @staticmethod
    def _clean_str(value):
        if value is None:
            return ''
        value = str(value).strip()
        if value.lower() == 'none':
            return ''
        return value
        
    def get_sheet(self, sheet_name):
        """Get a specific sheet by name"""
        try:
            sheet = self.spreadsheet.worksheet(sheet_name)
            print(f"✅ Found {sheet_name} sheet")
            return sheet
        except gspread.exceptions.WorksheetNotFound:
            print(f"❌ {sheet_name} sheet not found")
            raise
            
    def get_or_create_realitease_sheet(self):
        """Get or create the RealiteaseInfo sheet with proper headers"""
        try:
            sheet = self.spreadsheet.worksheet('RealiteaseInfo')
            print("✅ Found existing RealiteaseInfo sheet")
            
            # Check if headers exist and are correct
            headers = sheet.row_values(1)
            expected_headers = [
                'CastName', 'CastIMDbID', 'CastTMDbID', 'ShowNames', 'ShowIMDbIDs', 
                'ShowTMDbIDs', 'TotalShows', 'TotalSeasons', 'TotalEpisodes', 
                'Gender', 'Birthday', 'Zodiac'
            ]
            
            if headers != expected_headers:
                print("🔧 Headers need updating")
                sheet.update('A1:L1', [expected_headers])
                print("✅ Headers updated")
            else:
                print("✅ Headers are already in correct format")
                
            return sheet
            
        except gspread.exceptions.WorksheetNotFound:
            print("📝 Creating new RealiteaseInfo sheet...")
            sheet = self.spreadsheet.add_worksheet(title='RealiteaseInfo', rows=1000, cols=12)
            
            # Add headers
            headers = [
                'CastName', 'CastIMDbID', 'CastTMDbID', 'ShowNames', 'ShowIMDbIDs', 
                'ShowTMDbIDs', 'TotalShows', 'TotalSeasons', 'TotalEpisodes', 
                'Gender', 'Birthday', 'Zodiac'
            ]
            sheet.update('A1:L1', [headers])
            print("✅ Created RealiteaseInfo sheet with headers")
            return sheet
            
    def load_cast_info_data(self):
        """Load all data from CastInfo sheet"""
        print("🔄 Loading CastInfo data...")
        try:
            headers = [
                'Name', 'TMDbCastID', 'IMDbCastID', 'ShowName',
                'IMDbSeriesID', 'TMDbSeriesID', 'TotalEpisodes', 'TotalSeasons'
            ]
            data = self.cast_info_sheet.get_all_records(expected_headers=headers)
            print(f"📊 Loaded {len(data)} records from CastInfo")
            return data
        except Exception as e:
            print(f"❌ Failed to load CastInfo data: {e}")
            return []
            
    def get_unique_cast_members(self):
        """Extract unique cast members from CastInfo data, applying inclusion filters."""
        print("🔍 Identifying qualifying cast members...")

        aggregates = {}

        for i, record in enumerate(self.cast_info_data):
            cast_name = self._clean_str(record.get('Name', ''))
            cast_imdb_id = self._clean_str(record.get('IMDbCastID', ''))
            cast_tmdb_id = self._clean_str(record.get('TMDbCastID', ''))

            if not cast_imdb_id or not cast_name:
                print(f"  ⚠️ Skipping record {i+1} - missing Cast IMDb ID or Name")
                continue

            aggregate = aggregates.setdefault(cast_imdb_id, {
                'cast_name': cast_name,
                'cast_tmdb_id': cast_tmdb_id,
                'show_ids': set(),
                'show_names': [],
                'show_imdb_ids': [],
                'total_episodes': 0,
                'season_tokens': set(),
                'has_invalid_counts': False,
            })

            if not aggregate['cast_tmdb_id'] and cast_tmdb_id:
                aggregate['cast_tmdb_id'] = cast_tmdb_id
            if aggregate['cast_name'] != cast_name:
                # Keep the first name but log discrepancy
                print(f"  ⚠️ Name mismatch for {cast_imdb_id}: '{aggregate['cast_name']}' vs '{cast_name}'")

            show_imdb_raw = self._clean_str(record.get('IMDbSeriesID', ''))
            show_name_value = self._clean_str(record.get('ShowName', ''))

            if show_name_value and show_name_value not in aggregate['show_names']:
                aggregate['show_names'].append(show_name_value)

            if show_imdb_raw and show_imdb_raw not in aggregate['show_imdb_ids']:
                aggregate['show_imdb_ids'].append(show_imdb_raw)

            show_identifier = show_imdb_raw.lower() if show_imdb_raw else ''
            if not show_identifier:
                fallback_name = show_name_value
                if fallback_name:
                    show_identifier = f"name::{fallback_name.lower()}"
                else:
                    show_identifier = ''
            if show_identifier:
                aggregate['show_ids'].add(show_identifier)

            episodes_val = self._clean_str(record.get('TotalEpisodes', ''))
            seasons_val = self._clean_str(record.get('TotalSeasons', ''))

            if '**' in episodes_val or '**' in seasons_val:
                aggregate['has_invalid_counts'] = True

            try:
                episodes_num = int(float(episodes_val)) if episodes_val not in ('', 'None', 'none') else 0
            except (ValueError, TypeError):
                episodes_num = 0
            aggregate['total_episodes'] += max(0, episodes_num)

            if seasons_val:
                tokens = re.split(r'[;,]', seasons_val)
                tokens_added = False
                for token in tokens:
                    token = token.strip()
                    if token:
                        match = re.search(r'\d+', token)
                        if match:
                            aggregate['season_tokens'].add(match.group(0))
                        else:
                            aggregate['season_tokens'].add(token.lower())
                        tokens_added = True
                if not tokens_added:
                    try:
                        aggregate['season_tokens'].add(str(int(float(seasons_val))))
                    except (ValueError, TypeError):
                        pass

        unique_cast = {}
        qualifying_ids = set()

        for cast_imdb_id, data in aggregates.items():
            if self._cast_meets_criteria(data):
                qualifying_ids.add(cast_imdb_id)
                unique_cast[cast_imdb_id] = {
                    'cast_name': data['cast_name'],
                    'cast_imdb_id': cast_imdb_id,
                    'cast_tmdb_id': data['cast_tmdb_id'],
                    'show_names': list(data.get('show_names', [])),
                    'show_imdb_ids': list(data.get('show_imdb_ids', [])),
                }

        self.qualifying_cast_ids = qualifying_ids
        print(f"📊 Found {len(unique_cast)} qualifying cast members out of {len(aggregates)} candidates")
        return unique_cast

    def _cast_meets_criteria(self, data):
        if data['has_invalid_counts']:
            return False
        show_count = len([sid for sid in data['show_ids'] if sid])
        if show_count == 0:
            return False
        if show_count == 1:
            seasons_count = len([token for token in data['season_tokens'] if token])
            episodes_total = data['total_episodes']
            if seasons_count < 2 and episodes_total < 5:
                return False
        return True
        
    def add_missing_cast_members(self, unique_cast, dry_run=True):
        """Add missing cast members and refresh core columns (A–E)."""
        if dry_run:
            print("🔍 DRY RUN MODE - No data will be written to the sheet")
            print("🔄 Checking for missing cast members...")
        else:
            print("🔄 Adding missing cast members to RealiteaseInfo sheet...")
            
        # Get existing cast members
        print("  📋 Reading existing RealiteaseInfo data...")
        current_data = self.realitease_sheet.get_all_records()
        existing_cast_ids = set()
        existing_rows = {}
        
        for idx, record in enumerate(current_data, start=2):
            cast_imdb_id = self._clean_str(record.get('CastIMDbID', ''))
            if cast_imdb_id:
                existing_cast_ids.add(cast_imdb_id)
                existing_rows.setdefault(cast_imdb_id, []).append({
                    'row_num': idx,
                    'CastName': self._clean_str(record.get('CastName', '')),
                    'CastIMDbID': cast_imdb_id,
                    'CastTMDbID': self._clean_str(record.get('CastTMDbID', '')),
                    'ShowNames': self._clean_str(record.get('ShowNames', '')),
                    'ShowIMDbIDs': self._clean_str(record.get('ShowIMDbIDs', '')),
                })
                
        print(f"  📊 Found {len(existing_cast_ids)} existing cast members in RealiteaseInfo")
        
        # Find missing cast members
        missing_cast = []
        existing_updates = []
        for cast_imdb_id, cast_data in unique_cast.items():
            if cast_imdb_id not in existing_cast_ids:
                missing_cast.append(cast_data)
            else:
                row_infos = existing_rows.get(cast_imdb_id, [])
                for row_info in row_infos:
                    new_tmdb_id = self._clean_str(cast_data.get('cast_tmdb_id'))
                    show_names_str = self._clean_str(', '.join(cast_data.get('show_names', [])))
                    show_imdb_ids_str = self._clean_str(', '.join([sid for sid in cast_data.get('show_imdb_ids', []) if sid]))
                    new_values = [
                        cast_imdb_id,
                        new_tmdb_id,
                        show_names_str,
                        show_imdb_ids_str,
                    ]
                    existing_values = [
                        self._clean_str(row_info.get('CastIMDbID', '')),
                        self._clean_str(row_info.get('CastTMDbID', '')),
                        self._clean_str(row_info.get('ShowNames', '')),
                        self._clean_str(row_info.get('ShowIMDbIDs', '')),
                    ]
                    if any((existing_values[i] or '') != (new_values[i] or '') for i in range(4)):
                        existing_updates.append({
                            'row_num': row_info['row_num'],
                            'cast_name': row_info.get('CastName') or cast_data['cast_name'],
                            'values': new_values,
                        })
                
        print(f"  🔍 Found {len(missing_cast)} missing cast members")
        
        if not missing_cast:
            print("✅ All cast members already exist in RealiteaseInfo!")
            existing_updates_count = self._apply_existing_updates(existing_updates, dry_run=dry_run)
            if existing_updates_count:
                print(f"  🔄 Updated {existing_updates_count} existing cast rows (columns B-E)")
            return 0, existing_updates_count
            
        # Add missing cast members in batches
        updates_made = 0
        next_row = len(current_data) + 2  # +2 because row 1 is headers
        batch_data = []
        batch_size = 500  # Increased batch size to reduce API calls
        
        for i, cast_data in enumerate(missing_cast):
            show_names_str = self._clean_str(', '.join(cast_data.get('show_names', [])))
            show_imdb_ids_str = self._clean_str(', '.join([sid for sid in cast_data.get('show_imdb_ids', []) if sid]))
            cast_tmdb_value = self._clean_str(cast_data.get('cast_tmdb_id'))
            row_data = [
                cast_data['cast_name'],      # A - CastName
                cast_data['cast_imdb_id'],   # B - CastIMDbID  
                cast_tmdb_value,             # C - CastTMDbID
                show_names_str,              # D - ShowNames
                show_imdb_ids_str,           # E - ShowIMDbIDs
                '',                          # F - ShowTMDbIDs (empty for now)
                '',                          # G - TotalShows (empty for now)
                '',                          # H - TotalSeasons (empty for now)
                '',                          # I - TotalEpisodes (empty for now)
                '',                          # J - Gender (empty for now)
                '',                          # K - Birthday (empty for now)
                ''                           # L - Zodiac (empty for now)
            ]
            
            batch_data.append(row_data)
            
            if dry_run:
                print(f"  🔍 DRY RUN: Would add row {next_row + i} - {cast_data['cast_name']} ({cast_data['cast_imdb_id']})")
            
            # Process batch when we reach batch_size or at the end
            if len(batch_data) >= batch_size or i == len(missing_cast) - 1:
                if not dry_run:
                    start_row = next_row + updates_made
                    end_row = start_row + len(batch_data) - 1
                    range_name = f'A{start_row}:L{end_row}'
                    
                    print(f"  📝 Adding batch of {len(batch_data)} cast members (rows {start_row}-{end_row})")
                    self.realitease_sheet.update(values=batch_data, range_name=range_name)
                    time.sleep(1)  # Rate limiting for batch
                    
                    for j, cast_data in enumerate(missing_cast[updates_made:updates_made + len(batch_data)]):
                        print(f"    ✅ Added row {start_row + j} - {cast_data['cast_name']} ({cast_data['cast_imdb_id']})")
                
                updates_made += len(batch_data)
                batch_data = []
        
        existing_updates_count = self._apply_existing_updates(
            existing_updates,
            dry_run=dry_run,
            max_updates=500,
        )

        print(f"✅ Step 1 complete! Added {updates_made} new cast members")
        if existing_updates_count:
            print(f"  🔄 Updated {existing_updates_count} existing cast rows (columns B-E)")
        return updates_made, existing_updates_count

    def remove_non_qualifying_rows(self, allowed_cast_ids, dry_run=True):
        """Remove existing rows that no longer meet inclusion criteria."""
        print("🔍 Checking for existing cast members that no longer qualify...")
        all_values = self.realitease_sheet.get_all_values()
        rows_to_delete = []

        for idx, row in enumerate(all_values[1:], start=2):  # skip header
            if len(row) < 2:
                continue
            cast_imdb_id = row[1].strip()
            if cast_imdb_id and cast_imdb_id not in allowed_cast_ids:
                rows_to_delete.append(idx)

        if not rows_to_delete:
            print("✅ No rows need to be removed")
            return 0

        if dry_run:
            print(f"🔍 DRY RUN: Would remove {len(rows_to_delete)} rows that fail the new criteria")
            return len(rows_to_delete)

        print(f"🗑️ Removing {len(rows_to_delete)} rows that no longer meet the criteria")
        for row_num in reversed(rows_to_delete):
            try:
                self.realitease_sheet.delete_rows(row_num)
                print(f"   🗑️ Removed row {row_num}")
                time.sleep(0.2)
            except Exception as e:
                print(f"   ❌ Failed to delete row {row_num}: {e}")
        return len(rows_to_delete)
    
    def _apply_existing_updates(self, updates, dry_run=True, max_updates=None):
        if not updates:
            return 0

        updates = sorted(updates, key=lambda x: x['row_num'])

        processed_updates = updates
        if max_updates and len(updates) > max_updates:
            processed_updates = updates[:max_updates]
            remaining = len(updates) - max_updates
            print(
                f"  ⚠️ Limiting existing-row updates to {max_updates} this run "
                f"({remaining} remaining for next pass)"
            )

        if dry_run:
            for update in processed_updates:
                print(
                    f"  🔍 DRY RUN: Would update row {update['row_num']} columns B-E for "
                    f"{update['cast_name']} ({update['values'][0]})"
                )
            return len(processed_updates)

        for start_idx in range(0, len(processed_updates), 500):
            chunk = processed_updates[start_idx:start_idx + 500]
            requests = []
            for entry in chunk:
                range_name = f"B{entry['row_num']}:E{entry['row_num']}"
                requests.append({
                    'range': range_name,
                    'values': [entry['values']],
                })

            if requests:
                self.realitease_sheet.batch_update(requests, value_input_option='RAW')
                for entry in chunk:
                    print(
                        f"  ✅ Updated row {entry['row_num']} columns B-E for {entry['cast_name']} "
                        f"({entry['values'][0]})"
                    )
                time.sleep(0.5)

        return len(processed_updates)

    def run(self, dry_run=True):
        """Run Step 1: Populate unique cast members"""
        try:
            # Get unique cast members from CastInfo that meet criteria
            unique_cast = self.get_unique_cast_members()

            # Skip deletion for now (handled in a future dedicated cleanup step)
            removed_count = 0

            # Add missing cast members to RealiteaseInfo
            added_count, updated_existing = self.add_missing_cast_members(unique_cast, dry_run=dry_run)
            
            print(f"\n🎉 STEP 1 SUMMARY:")
            print(f"   📊 Total unique cast members: {len(unique_cast)}")
            print(f"   🗑️ Rows removed: {removed_count}")
            print(f"   ➕ New members added: {added_count}")
            print(f"   🔄 Existing rows updated: {updated_existing}")
            
            if dry_run:
                print(f"\n🔍 This was a DRY RUN - no data was actually written")
                print(f"   To execute for real, set dry_run=False")
            else:
                print(f"\n✅ All cast members are now in RealiteaseInfo!")
                print(f"   Ready for Step 2: Show data aggregation")
                
        except Exception as e:
            print(f"❌ Error in Step 1: {e}")
            raise


def main():
    """Main function"""
    try:
        # Initialize and run Step 1
        step1 = RealiteaseInfoStep1()
        step1.run(dry_run=False)  # Run for real!
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")


if __name__ == "__main__":
    main()

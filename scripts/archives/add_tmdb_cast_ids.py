#!/usr/bin/env python3
"""
RealiteaseInfo TMDb Cast ID Addition Script

This script adds TMDb Person IDs to the RealiteaseInfo sheet by:
1. Reading existing RealiteaseInfo rows
2. Using Column C (CastIMDbID) to find TMDb Person IDs
3. Adding TMDb Person IDs to a new column (Column M)
4. Using TMDb Find API to convert IMDb Person ID → TMDb Person ID

RealiteaseInfo Sheet Structure:
A: CastName
B: CastBio  
C: CastIMDbID
D: ShowNames
E: ShowIMDbIDs
F: ShowTMDbIDs  
G: TotalShows
H: TotalSeasons
I: TotalEpisodes
J: CastBirthYear
K: CastBirthPlace
L: CastImage
M: CastTMDbID (NEW - to be added)

Features:
- Batch processing with progress tracking
- TMDb API rate limiting (40 requests/10 seconds)
- Error handling and retry logic
- Dry-run mode for testing
- Resume functionality from specific batch
- Detailed logging of conversions
"""

import argparse
import gspread
import os
import requests
import time
from dotenv import load_dotenv
from typing import List, Tuple, Optional

# Load environment
load_dotenv()

# Configuration
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
TMDB_BEARER = os.getenv('TMDB_BEARER')

# If TMDB_BEARER is not found, check for alternatives
if not TMDB_BEARER:
    print("⚠️  TMDB_BEARER not found in environment")
    print("💡 You need to set your TMDb API Bearer token")
    print("   Either add it to .env file as: TMDB_BEARER=your_token_here")
    print("   Or export it: export TMDB_BEARER=your_token_here")
    exit(1)

REQUEST_DELAY = 0.3  # 40 requests per 10 seconds = ~0.25s delay
BATCH_SIZE = 100     # Process in batches for better progress tracking


class RealiteaseInfoTMDbAdder:
    """Add TMDb Person IDs to RealiteaseInfo sheet."""
    
    def __init__(self):
        # TMDb API setup
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        if not self.tmdb_api_key:
            print("⚠️  TMDB_API_KEY not found in environment")
            print("💡 You need to set your TMDb API key")
            print("   Either add it to .env file as: TMDB_API_KEY=your_key_here")
            exit(1)
        
        self.tmdb_session = requests.Session()
        self.tmdb_session.headers.update({
            'accept': 'application/json'
        })
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        
        # Google Sheets setup
        print("✅ Connecting to Google Sheets...")
        # Initialize Google Sheets connection using environment variable path
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '../../keys/trr-backend-df2c438612e1.json')
        if not os.path.isabs(creds_path):
            # Convert relative path to absolute from current script location
            script_dir = os.path.dirname(os.path.abspath(__file__))
            creds_path = os.path.join(script_dir, creds_path)
        
        self.gc = gspread.service_account(filename=creds_path)
        print("✅ Opening spreadsheet...")
        self.sheet = self.gc.open_by_key(SPREADSHEET_ID)
        print("✅ Accessing RealiteaseInfo worksheet...")
        self.realitease_worksheet = self.sheet.worksheet('RealiteaseInfo')
        print("✅ Connected to RealiteaseInfo sheet")
        
        # Statistics
        self.stats = {
            'total_rows': 0,
            'rows_needing_tmdb': 0,
            'successful_conversions': 0,
            'successful_conversions_direct': 0,
            'successful_conversions_show_cast': 0,
            'failed_conversions': 0,
            'api_errors': 0,
            'already_has_tmdb': 0
        }

    def convert_imdb_to_tmdb_id(self, imdb_id: str, cast_name: str, show_tmdb_ids: str = "") -> Optional[str]:
        """
        Convert IMDb Person ID to TMDb Person ID using multiple strategies:
        1. Direct lookup via TMDb Find API with IMDb ID
        2. Fallback: Search by name in show cast lists if show TMDb IDs are available
        """
        try:
            if not imdb_id or not imdb_id.startswith('nm'):
                print(f"    ⚠️ Invalid IMDb ID format: {imdb_id}")
                return None
                
            print(f"    🔄 Converting {imdb_id} → TMDb ID for {cast_name}")
            
            # Strategy 1: Direct IMDb ID lookup
            find_url = f"{self.tmdb_base_url}/find/{imdb_id}"
            params = {
                'api_key': self.tmdb_api_key,
                'external_source': 'imdb_id'
            }
            
            time.sleep(REQUEST_DELAY)  # Rate limiting
            find_response = self.tmdb_session.get(find_url, params=params)
            
            if find_response.status_code != 200:
                print(f"    ❌ TMDb API error for {imdb_id}: HTTP {find_response.status_code}")
                self.stats['api_errors'] += 1
                return None
            
            find_data = find_response.json()
            
            # Check direct person results first
            if find_data.get('person_results'):
                tmdb_person_id = find_data['person_results'][0]['id']
                print(f"    ✅ Direct lookup: {imdb_id} → TMDb ID: {tmdb_person_id}")
                self.stats['successful_conversions'] += 1
                self.stats['successful_conversions_direct'] += 1
                return str(tmdb_person_id)
            
            # Strategy 2: Fallback to show cast search if we have TMDb show IDs
            if show_tmdb_ids:
                print(f"    🎬 Direct lookup failed, searching in show cast lists...")
                tmdb_id = self.search_person_in_show_cast(cast_name, show_tmdb_ids)
                if tmdb_id:
                    print(f"    ✅ Show cast lookup: {cast_name} → TMDb ID: {tmdb_id}")
                    self.stats['successful_conversions'] += 1
                    self.stats['successful_conversions_show_cast'] += 1
                    return str(tmdb_id)
            
            print(f"    ❌ No TMDb person found for IMDb ID: {imdb_id}")
            self.stats['failed_conversions'] += 1
            return None
            
        except Exception as e:
            print(f"    ❌ Error converting IMDb ID {imdb_id}: {e}")
            self.stats['api_errors'] += 1
            return None

    def search_person_in_show_cast(self, person_name: str, show_tmdb_ids: str) -> Optional[int]:
        """
        Search for a person by name in the cast lists of their known shows.
        """
        try:
            # Parse TMDb show IDs (assuming comma-separated)
            show_ids = [id.strip() for id in show_tmdb_ids.split(',') if id.strip()]
            
            for show_id in show_ids[:3]:  # Limit to first 3 shows to avoid too many API calls
                if not show_id.isdigit():
                    continue
                    
                print(f"      🔍 Searching in show {show_id} cast for {person_name}")
                
                # Get show credits
                credits_url = f"{self.tmdb_base_url}/tv/{show_id}/credits"
                params = {'api_key': self.tmdb_api_key}
                
                time.sleep(REQUEST_DELAY)  # Rate limiting
                credits_response = self.tmdb_session.get(credits_url, params=params)
                
                if credits_response.status_code != 200:
                    print(f"      ⚠️ Could not fetch credits for show {show_id}")
                    continue
                
                credits_data = credits_response.json()
                
                # Search in cast
                cast_list = credits_data.get('cast', [])
                for cast_member in cast_list:
                    if self.names_match(person_name, cast_member.get('name', '')):
                        print(f"      🎯 Found {person_name} as {cast_member.get('name')} in cast")
                        return cast_member.get('id')
                
                # Search in crew (some reality show personalities appear in crew)
                crew_list = credits_data.get('crew', [])
                for crew_member in crew_list:
                    if self.names_match(person_name, crew_member.get('name', '')):
                        print(f"      🎯 Found {person_name} as {crew_member.get('name')} in crew")
                        return crew_member.get('id')
            
            return None
            
        except Exception as e:
            print(f"      ⚠️ Error searching in show cast: {str(e)}")
            return None

    def names_match(self, name1: str, name2: str) -> bool:
        """
        Check if two names match, accounting for variations like:
        - 'John Smith' vs 'John W. Smith'
        - 'Jane Doe-Johnson' vs 'Jane Johnson'
        - Case differences
        """
        if not name1 or not name2:
            return False
            
        # Normalize names
        name1_clean = name1.lower().strip()
        name2_clean = name2.lower().strip()
        
        # Exact match
        if name1_clean == name2_clean:
            return True
        
        # Split into words and compare
        words1 = name1_clean.split()
        words2 = name2_clean.split()
        
        # Check if first and last names match (ignoring middle names/initials)
        if len(words1) >= 2 and len(words2) >= 2:
            if words1[0] == words2[0] and words1[-1] == words2[-1]:
                return True
        
        return False

    def get_realitease_data(self) -> List[List[str]]:
        """Read all RealiteaseInfo data."""
        print("📋 Reading RealiteaseInfo sheet...")
        all_values = self.realitease_worksheet.get_all_values()
        print(f"📊 Found {len(all_values)} total rows (including header)")
        return all_values

    def find_rows_needing_tmdb_ids(self, all_data: List[List[str]]) -> List[Tuple[int, List[str]]]:
        """Find rows where TMDb CastID is missing but Cast IMDb ID exists."""
        if not all_data:
            return []
        
        # Skip header row and check structure
        header = all_data[0] if len(all_data) > 0 else []
        data_rows = all_data[1:] if len(all_data) > 1 else []
        
        print(f"📋 Sheet has {len(header)} columns")
        for i, col in enumerate(header):
            print(f"  Column {chr(65+i)}: {col}")
        
        rows_needing_update = []
        
        # Find the columns we need - Cast IMDb ID is in Column B (index 1)
        cast_imdb_col = 1  # Column B
        cast_tmdb_col = -1
        
        for i, col_name in enumerate(header):
            if 'tmdb' in col_name.lower() and 'cast' in col_name.lower():
                cast_tmdb_col = i
        
        print(f"📍 Cast IMDb ID column: B (index {cast_imdb_col})")
        print(f"📍 Cast TMDb ID column: {chr(65+cast_tmdb_col) if cast_tmdb_col >= 0 else 'NOT FOUND - will use next available'}")
        
        # If no TMDb column exists, we'll add to the next available column
        if cast_tmdb_col == -1:
            cast_tmdb_col = len(header)  # Next column after existing ones
            print(f"📍 Will add TMDb IDs to new column: {chr(65+cast_tmdb_col)}")
        
        for row_idx, row in enumerate(data_rows, start=2):  # Start at 2 (after header)
            cast_name = row[0] if len(row) > 0 else ""
            cast_imdb_id = row[cast_imdb_col] if cast_imdb_col >= 0 and cast_imdb_col < len(row) else ""
            cast_tmdb_id = row[cast_tmdb_col] if cast_tmdb_col < len(row) else ""
            
            # Need update if: TMDb ID is empty AND IMDb ID exists
            if not cast_tmdb_id and cast_imdb_id and cast_imdb_id.startswith('nm'):
                rows_needing_update.append((row_idx, row, cast_imdb_col, cast_tmdb_col))
            elif cast_tmdb_id:
                self.stats['already_has_tmdb'] += 1
        
        self.stats['total_rows'] = len(data_rows)
        self.stats['rows_needing_tmdb'] = len(rows_needing_update)
        
        print(f"🎯 Found {len(rows_needing_update)} rows needing TMDb ID addition")
        print(f"📊 {self.stats['already_has_tmdb']} rows already have TMDb IDs")
        return rows_needing_update

    def update_realitease_row(self, row_number: int, tmdb_id: str, tmdb_col: int, dry_run: bool = False) -> bool:
        """Update a single RealiteaseInfo row with TMDb ID in the specified column."""
        try:
            col_letter = chr(65 + tmdb_col)  # Convert column index to letter (A=0, B=1, etc.)
            
            if dry_run:
                print(f"    🔍 DRY RUN: Would update row {row_number}, column {col_letter} with TMDb ID: {tmdb_id}")
                return True
            
            # Update the specified column for the specific row
            cell_range = f'{col_letter}{row_number}'
            self.realitease_worksheet.update(cell_range, [[tmdb_id]])
            print(f"    ✅ Updated row {row_number}, column {col_letter} with TMDb ID: {tmdb_id}")
            return True
            
        except Exception as e:
            print(f"    ❌ Error updating row {row_number}: {e}")
            return False

    def update_realitease_batch(self, batch_updates: List[Tuple[int, str]], dry_run: bool = False) -> bool:
        """Update multiple RealiteaseInfo rows with TMDb IDs using batch update."""
        try:
            if not batch_updates:
                return True
                
            if dry_run:
                for row_number, tmdb_id in batch_updates:
                    print(f"    🔍 DRY RUN: Would update row {row_number}, column M with TMDb ID: {tmdb_id}")
                return True
            
            # Prepare batch data for consecutive rows
            start_row = batch_updates[0][0]
            end_row = batch_updates[-1][0]
            
            # Check if rows are consecutive
            row_numbers = [row_num for row_num, _ in batch_updates]
            is_consecutive = all(row_numbers[j] == row_numbers[j-1] + 1 for j in range(1, len(row_numbers)))
            
            if is_consecutive and len(batch_updates) > 1:
                # Batch update for consecutive rows
                range_name = f'M{start_row}:M{end_row}'
                batch_data = [[tmdb_id] for _, tmdb_id in batch_updates]
                
                print(f"    📝 Batch updating rows {start_row}-{end_row} ({len(batch_updates)} rows)")
                self.realitease_worksheet.update(values=batch_data, range_name=range_name)
                return True
            else:
                # Individual updates for non-consecutive rows
                for row_number, tmdb_id in batch_updates:
                    cell_range = f'M{row_number}'
                    self.realitease_worksheet.update(cell_range, [[tmdb_id]])
                    print(f"    ✅ Updated row {row_number} with TMDb ID: {tmdb_id}")
                    time.sleep(0.1)  # Small delay between individual updates
                return True
            
        except Exception as e:
            print(f"    ❌ Error in batch update: {e}")
            return False

    def process_batch(self, batch: List[Tuple[int, List[str], int, int]], batch_num: int, total_batches: int, dry_run: bool = False):
        """Process a batch of rows needing TMDb ID updates."""
        print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} rows)")
        
        batch_updates = []
        
        for row_number, row_data, imdb_col, tmdb_col in batch:
            cast_name = row_data[0] if len(row_data) > 0 else "Unknown"
            cast_imdb_id = row_data[imdb_col] if imdb_col < len(row_data) else ""
            show_tmdb_ids = row_data[5] if len(row_data) > 5 else ""  # Column F (ShowTMDbIDs)
            
            print(f"  👤 Row {row_number}: {cast_name} ({cast_imdb_id})")
            
            # Convert IMDb ID to TMDb ID (with show cast fallback)
            tmdb_id = self.convert_imdb_to_tmdb_id(cast_imdb_id, cast_name, show_tmdb_ids)
            
            if tmdb_id:
                batch_updates.append((row_number, tmdb_id, tmdb_col))
        
        # Update all successful conversions in this batch
        if batch_updates:
            print(f"  📝 Updating {len(batch_updates)} rows with TMDb IDs...")
            for row_number, tmdb_id, tmdb_col in batch_updates:
                self.update_realitease_row(row_number, tmdb_id, tmdb_col, dry_run)
                if not dry_run:
                    time.sleep(0.1)  # Small delay between updates

    def add_tmdb_ids(self, dry_run: bool = False, max_rows: Optional[int] = None, start_batch: int = 1):
        """Main method to add TMDb IDs to RealiteaseInfo sheet."""
        print("🚀 Starting RealiteaseInfo TMDb ID Addition")
        print(f"🔍 Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
        
        # Read all data
        all_data = self.get_realitease_data()
        
        # Find rows needing updates
        rows_to_update = self.find_rows_needing_tmdb_ids(all_data)
        
        if not rows_to_update:
            print("✅ No rows need TMDb ID additions!")
            return
        
        # Apply max_rows limit if specified
        if max_rows:
            rows_to_update = rows_to_update[:max_rows]
            print(f"🎯 Limited to first {max_rows} rows for this run")
        
        # Calculate starting point for resume functionality
        start_index = (start_batch - 1) * BATCH_SIZE
        if start_index >= len(rows_to_update):
            print(f"❌ Start batch {start_batch} is beyond available data")
            return
        
        # Process in batches starting from specified batch
        total_rows = len(rows_to_update)
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"📊 Processing {total_rows} rows in {total_batches} batches, starting from batch {start_batch}")
        
        for i in range(start_index, total_rows, BATCH_SIZE):
            batch = rows_to_update[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            try:
                self.process_batch(batch, batch_num, total_batches, dry_run)
                
                # Save progress
                if not dry_run:
                    with open('tmdb_addition_progress.txt', 'w') as f:
                        f.write(f"Last completed batch: {batch_num}\\n")
                
                # Small delay between batches to avoid rate limits
                if batch_num < total_batches:
                    print(f"   😴 Resting 2 seconds before next batch...")
                    time.sleep(2)
                    
            except Exception as e:
                print(f"❌ Error in batch {batch_num}: {str(e)}")
                print(f"💾 Progress saved - you can resume from batch {batch_num}")
                with open('tmdb_addition_progress.txt', 'w') as f:
                    f.write(f"Failed at batch: {batch_num}\\nResume from batch: {batch_num}\\n")
                raise
        
        # Print final statistics
        self.print_statistics(dry_run)

    def print_statistics(self, dry_run: bool = False):
        """Print final statistics."""
        print("\\n" + "="*60)
        print("📊 FINAL STATISTICS")
        print("="*60)
        print(f"Total rows in RealiteaseInfo: {self.stats['total_rows']}")
        print(f"Rows already with TMDb IDs: {self.stats['already_has_tmdb']}")
        print(f"Rows needing TMDb IDs: {self.stats['rows_needing_tmdb']}")
        print(f"Successful conversions: {self.stats['successful_conversions']}")
        print(f"  - Direct IMDb→TMDb lookups: {self.stats.get('successful_conversions_direct', 0)}")
        print(f"  - Show cast name lookups: {self.stats.get('successful_conversions_show_cast', 0)}")
        print(f"Failed conversions: {self.stats['failed_conversions']}")
        print(f"API errors: {self.stats['api_errors']}")
        
        success_rate = 0
        if self.stats['rows_needing_tmdb'] > 0:
            success_rate = (self.stats['successful_conversions'] / self.stats['rows_needing_tmdb']) * 100
        
        print(f"Success rate: {success_rate:.1f}%")
        
        if dry_run:
            print("\\n🔍 This was a DRY RUN - no data was actually updated")
        else:
            print(f"\\n✅ Added TMDb IDs to {self.stats['successful_conversions']} rows")


def main():
    parser = argparse.ArgumentParser(description='Add TMDb Person IDs to RealiteaseInfo sheet')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be done without actually updating the sheet')
    parser.add_argument('--max-rows', type=int,
                       help='Maximum number of rows to process (useful for testing)')
    parser.add_argument('--start-batch', type=int, default=1,
                       help='Batch number to start from (for resuming)')
    
    args = parser.parse_args()
    
    try:
        adder = RealiteaseInfoTMDbAdder()
        adder.add_tmdb_ids(
            dry_run=args.dry_run,
            max_rows=args.max_rows,
            start_batch=args.start_batch
        )
        
    except Exception as e:
        print(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

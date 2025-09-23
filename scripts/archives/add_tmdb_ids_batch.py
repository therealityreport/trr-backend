#!/usr/bin/env python3
"""
RealiteaseInfo TMDb Cast ID Batch Addition Script

This script adds TMDb Person IDs to the RealiteaseInfo sheet by:
1. Reading all RealiteaseInfo rows
2. Using Cast IMDb ID (Column C) to find TMDb Person IDs via TMDb Find API
3. Adding TMDb Person IDs to a new column (Column M - CastTMDbID)
4. Processing in consecutive batches for efficient API usage

Features:
- Batch processing for efficiency
- TMDb API rate limiting
- Resume from specific row
- Dry-run mode for testing
"""

import gspread
import os
import requests
import time
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Configuration
TMDB_BEARER = os.getenv('TMDB_BEARER')
REQUEST_DELAY = 0.3  # Rate limiting
BATCH_SIZE = 100     # Rows per batch

class RealiteaseInfoTMDbUpdater:
    """Add TMDb Person IDs to RealiteaseInfo sheet in batches."""
    
    def __init__(self):
        print("🚀 Starting RealiteaseInfo TMDb ID Batch Addition")
        
        # TMDb setup
        self.tmdb_session = requests.Session()
        self.tmdb_session.headers.update({
            'Authorization': f'Bearer {TMDB_BEARER}',
            'accept': 'application/json'
        })
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        
        # Google Sheets setup
        print("✅ Connecting to Google Sheets...")
        self.gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sheet = self.gc.open('Realitease2025Data')
        self.realitease_sheet = self.sheet.worksheet('RealiteaseInfo')
        print("✅ Connected to RealiteaseInfo sheet")
        
        # Stats
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'api_errors': 0
        }

    def convert_imdb_to_tmdb_id(self, imdb_id: str) -> str:
        """Convert IMDb Person ID to TMDb Person ID."""
        if not imdb_id or not imdb_id.startswith('nm'):
            return ""
            
        try:
            find_url = f"{self.tmdb_base_url}/find/{imdb_id}?external_source=imdb_id"
            time.sleep(REQUEST_DELAY)
            
            response = self.tmdb_session.get(find_url)
            if response.status_code != 200:
                self.stats['api_errors'] += 1
                return ""
            
            data = response.json()
            if data.get('person_results'):
                tmdb_id = str(data['person_results'][0]['id'])
                self.stats['successful'] += 1
                return tmdb_id
            
            self.stats['failed'] += 1
            return ""
            
        except Exception as e:
            print(f"    ❌ Error converting {imdb_id}: {e}")
            self.stats['api_errors'] += 1
            return ""

    def process_batch(self, start_row: int, batch_data: list, dry_run: bool = False):
        """Process a batch of rows and get TMDb IDs."""
        end_row = start_row + len(batch_data) - 1
        print(f"\n📦 Processing rows {start_row}-{end_row} ({len(batch_data)} rows)")
        
        # Prepare batch updates
        tmdb_ids = []
        
        for i, row in enumerate(batch_data):
            current_row = start_row + i
            cast_name = row[0] if len(row) > 0 else "Unknown"
            imdb_id = row[2] if len(row) > 2 else ""  # Column C
            
            print(f"  👤 Row {current_row}: {cast_name} ({imdb_id})")
            
            if imdb_id:
                tmdb_id = self.convert_imdb_to_tmdb_id(imdb_id)
                tmdb_ids.append([tmdb_id])
                if tmdb_id:
                    print(f"    ✅ Found TMDb ID: {tmdb_id}")
                else:
                    print(f"    ❌ No TMDb ID found")
            else:
                tmdb_ids.append([""])
                print(f"    ⚠️ No IMDb ID")
            
            self.stats['processed'] += 1
        
        # Update the sheet with batch of TMDb IDs
        if not dry_run and tmdb_ids:
            try:
                range_name = f'M{start_row}:M{end_row}'
                print(f"  📝 Batch updating column M, rows {start_row}-{end_row}")
                self.realitease_sheet.update(values=tmdb_ids, range_name=range_name)
                print(f"  ✅ Successfully updated {len(tmdb_ids)} rows")
            except Exception as e:
                print(f"  ❌ Batch update failed: {e}")
        elif dry_run:
            print(f"  🔍 DRY RUN: Would update column M, rows {start_row}-{end_row}")

    def add_tmdb_ids(self, start_row: int = 2, dry_run: bool = False):
        """Main method to add TMDb IDs to all rows starting from specified row."""
        print(f"🔍 Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
        print(f"🏁 Starting from row {start_row}")
        
        # Get all data
        print("📋 Reading RealiteaseInfo data...")
        all_data = self.realitease_sheet.get_all_values()
        
        if len(all_data) < 2:
            print("❌ No data found")
            return
        
        # Skip header and start from specified row
        data_rows = all_data[start_row-1:]  # -1 because list is 0-indexed
        total_rows = len(data_rows)
        
        print(f"📊 Processing {total_rows} rows starting from row {start_row}")
        
        # Process in batches
        for i in range(0, total_rows, BATCH_SIZE):
            batch = data_rows[i:i + BATCH_SIZE]
            current_start_row = start_row + i
            
            try:
                self.process_batch(current_start_row, batch, dry_run)
                
                # Rest between batches
                if i + BATCH_SIZE < total_rows:
                    print(f"  😴 Resting 2 seconds...")
                    time.sleep(2)
                    
            except Exception as e:
                print(f"❌ Error in batch starting at row {current_start_row}: {e}")
                print(f"💾 Resume with: --start-row {current_start_row}")
                break
        
        # Print final stats
        print(f"\n📊 FINAL STATS:")
        print(f"   Processed: {self.stats['processed']}")
        print(f"   Successful: {self.stats['successful']}")
        print(f"   Failed: {self.stats['failed']}")
        print(f"   API Errors: {self.stats['api_errors']}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Add TMDb IDs to RealiteaseInfo')
    parser.add_argument('--start-row', type=int, default=9991, help='Row to start from')
    parser.add_argument('--dry-run', action='store_true', help='Test mode')
    
    args = parser.parse_args()
    
    try:
        updater = RealiteaseInfoTMDbUpdater()
        updater.add_tmdb_ids(start_row=args.start_row, dry_run=args.dry_run)
    except Exception as e:
        print(f"💥 Fatal error: {e}")


if __name__ == '__main__':
    main()

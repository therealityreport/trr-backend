#!/usr/bin/env python3
"""
FinalInfo Step 1 - FinalList Builder

This script constructs the initial FinalList from RealiteaseInfo and WWHLinfo.
It does NOT call external APIs. Run this first before Step 2/3.

Creates the final consolidated FinalList sheet with the following logic:
- Name, IMDbCastID, CorrectDate, AlternativeNames, IMDbSeriesIDs columns
- Include rows with 6+ total episodes of 1 show OR listed in WWHLInfo CastIDs
- Exclude rows missing data in columns D-G of RealiteaseInfo
- Consolidate duplicates with complex merging rules

Requirements:
1. Must have 6+ episodes of 1 show OR be in WWHLInfo
2. Must have complete data in RealiteaseInfo columns D-G (ShowNames, ShowIMDbIDs, ShowTMDbIDs, TotalShows)
3. Handle duplicate consolidation by name and IMDbCastID
"""

import gspread
import os
import argparse
from collections import defaultdict
import sys
from dotenv import load_dotenv

class FinalListBuilder:
    def __init__(self, dry_run=True):
        load_dotenv()
        self.dry_run = dry_run
        
        # Connect to Google Sheets
        self.gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sheet = self.gc.open('Realitease2025Data')
        
        # Get worksheets
        self.realitease_worksheet = self.sheet.worksheet('RealiteaseInfo')
        self.wwhl_worksheet = self.sheet.worksheet('WWHLinfo')
        
        # Data storage
        self.realitease_data = []
        self.wwhl_cast_ids = set()
        self.final_list = []
        
        print("🎯 FinalList Builder initialized")

    def load_data(self):
        """Load data from both sheets"""
        print("📥 Loading RealiteaseInfo data...")
        
        # Load RealiteaseInfo
        realitease_rows = self.realitease_worksheet.get_all_values()
        headers = realitease_rows[0]
        
        for i, row in enumerate(realitease_rows[1:], 2):
            # Pad row to ensure we have enough columns
            while len(row) < 13:
                row.append("")
                
            cast_data = {
                'row_num': i,
                'name': row[0] if len(row) > 0 else "",
                'imdb_cast_id': row[1] if len(row) > 1 else "",
                'tmdb_cast_id': row[2] if len(row) > 2 else "",
                'show_names': row[3] if len(row) > 3 else "",
                'show_imdb_ids': row[4] if len(row) > 4 else "",
                'show_tmdb_ids': row[5] if len(row) > 5 else "",
                'total_shows': row[6] if len(row) > 6 else "",
                'total_seasons': row[7] if len(row) > 7 else "",
                'total_episodes': row[8] if len(row) > 8 else "",
                'gender': row[9] if len(row) > 9 else "",
                'birthday': row[10] if len(row) > 10 else "",
                'zodiac': row[11] if len(row) > 11 else "",
                'notes': row[12] if len(row) > 12 else ""
            }
            self.realitease_data.append(cast_data)
        
        print(f"   ✅ Loaded {len(self.realitease_data)} RealiteaseInfo records")
        
        # Load WWHLInfo cast IDs
        print("📥 Loading WWHLInfo cast IDs...")
        wwhl_rows = self.wwhl_worksheet.get_all_values()
        
        for i, row in enumerate(wwhl_rows[1:], 2):
            if len(row) > 5:  # Column F (IMDbCastIDs)
                imdb_cast_ids_str = row[5]
                if imdb_cast_ids_str:
                    # Split comma-separated IDs and clean them
                    cast_ids = [id.strip() for id in imdb_cast_ids_str.split(',') if id.strip()]
                    self.wwhl_cast_ids.update(cast_ids)
        
        print(f"   ✅ Found {len(self.wwhl_cast_ids)} unique WWHLInfo cast IDs")

    def meets_inclusion_criteria(self, cast_data):
        """Check if a record meets the inclusion criteria"""
        # Check for missing data in columns D-G
        required_fields = ['show_names', 'show_imdb_ids', 'show_tmdb_ids', 'total_shows']
        for field in required_fields:
            if not cast_data[field] or cast_data[field].strip() == "":
                return False, f"Missing {field}"
        
        # Check if in WWHLInfo (priority inclusion)
        if cast_data['imdb_cast_id'] in self.wwhl_cast_ids:
            # Must have at least 1 episode
            try:
                episodes = int(cast_data['total_episodes']) if cast_data['total_episodes'] else 0
                if episodes >= 1:
                    return True, "WWHLInfo priority (1+ episodes)"
            except ValueError:
                pass
            return False, "WWHLInfo cast but no valid episodes"
        
        # Check for 6+ episodes requirement
        try:
            episodes = int(cast_data['total_episodes']) if cast_data['total_episodes'] else 0
            if episodes >= 6:
                return True, f"6+ episodes ({episodes})"
        except ValueError:
            pass
        
        return False, "Less than 6 episodes and not in WWHLInfo"

    def parse_episode_counts_by_show(self, cast_data):
        """Parse show names and episode counts to find shows with 6+ episodes"""
        show_names = cast_data['show_names']
        if not show_names:
            return []
        
        # This is a simplified parser - may need adjustment based on actual data format
        shows = [show.strip() for show in show_names.split(',')]
        
        # For now, we'll use total episodes / total shows as average
        try:
            total_episodes = int(cast_data['total_episodes']) if cast_data['total_episodes'] else 0
            total_shows = int(cast_data['total_shows']) if cast_data['total_shows'] else 1
            avg_episodes = total_episodes / total_shows if total_shows > 0 else 0
            
            # Simple heuristic: if average is 6+, assume at least one show has 6+
            if avg_episodes >= 6:
                return [(shows[0], total_episodes)] if shows else []
        except (ValueError, ZeroDivisionError):
            pass
        
        return []

    def consolidate_duplicates(self, qualified_records):
        """Consolidate duplicate records based on complex rules"""
        print("🔄 Consolidating duplicates...")
        
        # Group by name (case-insensitive)
        name_groups = defaultdict(list)
        for record in qualified_records:
            name_key = record['name'].lower().strip()
            if name_key:
                name_groups[name_key].append(record)
        
        # Group by IMDbCastID
        imdb_groups = defaultdict(list)
        for record in qualified_records:
            imdb_id = record['imdb_cast_id'].strip()
            if imdb_id:
                imdb_groups[imdb_id].append(record)
        
        consolidated = []
        processed_records = set()
        
        # Process IMDbCastID duplicates first (higher priority)
        for imdb_id, records in imdb_groups.items():
            if len(records) > 1:
                print(f"   🔍 Found {len(records)} records with same IMDbCastID: {imdb_id}")
                
                # Check for 'THIS' in notes column
                this_record = None
                for record in records:
                    if 'THIS' in record.get('notes', '').upper():
                        this_record = record
                        break
                
                if this_record:
                    primary_record = this_record
                    print(f"      ✅ Using 'THIS' record: {primary_record['name']}")
                else:
                    # Use record with more shows/episodes
                    primary_record = max(records, key=lambda r: (
                        int(r['total_shows']) if r['total_shows'].isdigit() else 0,
                        int(r['total_episodes']) if r['total_episodes'].isdigit() else 0
                    ))
                    print(f"      ✅ Using record with most shows/episodes: {primary_record['name']}")
                
                # Create consolidated record
                alt_names = []
                show_names_set = set()
                show_imdb_ids_set = set()
                
                for record in records:
                    if record != primary_record and record['name'].strip():
                        alt_names.append(record['name'].strip())
                    
                    # Combine show data
                    if record['show_names']:
                        show_names_set.update([s.strip() for s in record['show_names'].split(',')])
                    if record['show_imdb_ids']:
                        show_imdb_ids_set.update([s.strip() for s in record['show_imdb_ids'].split(',')])
                
                consolidated_record = {
                    'name': primary_record['name'],
                    'imdb_cast_id': imdb_id,
                    'correct_date': '',  # Leave blank as specified
                    'alternative_names': ', '.join(alt_names),
                    'imdb_series_ids': ', '.join(sorted(show_imdb_ids_set))
                }
                
                consolidated.append(consolidated_record)
                processed_records.update(id(r) for r in records)
        
        # Process name duplicates (different IMDbCastIDs)
        for name_key, records in name_groups.items():
            # Skip already processed records
            unprocessed_records = [r for r in records if id(r) not in processed_records]
            
            if len(unprocessed_records) > 1:
                print(f"   🔍 Found {len(unprocessed_records)} records with same name: {name_key}")
                
                # Use record with more shows/episodes for primary IMDbCastID
                primary_record = max(unprocessed_records, key=lambda r: (
                    int(r['total_shows']) if r['total_shows'].isdigit() else 0,
                    int(r['total_episodes']) if r['total_episodes'].isdigit() else 0
                ))
                
                # Combine all show data
                show_names_set = set()
                show_imdb_ids_set = set()
                
                for record in unprocessed_records:
                    if record['show_names']:
                        show_names_set.update([s.strip() for s in record['show_names'].split(',')])
                    if record['show_imdb_ids']:
                        show_imdb_ids_set.update([s.strip() for s in record['show_imdb_ids'].split(',')])
                
                consolidated_record = {
                    'name': primary_record['name'],
                    'imdb_cast_id': primary_record['imdb_cast_id'],
                    'correct_date': '',
                    'alternative_names': '',
                    'imdb_series_ids': ', '.join(sorted(show_imdb_ids_set))
                }
                
                consolidated.append(consolidated_record)
                processed_records.update(id(r) for r in unprocessed_records)
        
        # Add remaining unique records
        for record in qualified_records:
            if id(record) not in processed_records:
                consolidated_record = {
                    'name': record['name'],
                    'imdb_cast_id': record['imdb_cast_id'],
                    'correct_date': '',
                    'alternative_names': '',
                    'imdb_series_ids': record['show_imdb_ids']
                }
                consolidated.append(consolidated_record)
        
        print(f"   ✅ Consolidated {len(qualified_records)} records into {len(consolidated)} final records")
        return consolidated

    def build_final_list(self):
        """Build the final consolidated list"""
        print("🔨 Building FinalList...")
        
        qualified_records = []
        inclusion_stats = defaultdict(int)
        
        for cast_data in self.realitease_data:
            qualifies, reason = self.meets_inclusion_criteria(cast_data)
            
            if qualifies:
                qualified_records.append(cast_data)
                inclusion_stats[reason] += 1
                
                if self.dry_run:
                    print(f"   ✅ {cast_data['name']:<30} | {reason}")
            elif self.dry_run:
                print(f"   ❌ {cast_data['name']:<30} | {reason}")
        
        print(f"\n📊 Inclusion Statistics:")
        for reason, count in inclusion_stats.items():
            print(f"   {reason}: {count}")
        
        print(f"\n✅ {len(qualified_records)} records qualify for FinalList")
        
        # Consolidate duplicates
        self.final_list = self.consolidate_duplicates(qualified_records)
        
        return self.final_list

    def create_final_sheet(self):
        """Create the FinalList sheet in Google Sheets with batch updates"""
        if self.dry_run:
            print("🧪 DRY RUN - Would create FinalList sheet")
            return
        
        print("📝 Creating FinalList sheet...")
        
        try:
            # Try to delete existing sheet
            existing_sheet = self.sheet.worksheet('FinalList')
            self.sheet.del_worksheet(existing_sheet)
            print("   🗑️  Deleted existing FinalList sheet")
        except gspread.WorksheetNotFound:
            pass
        
        # Create new sheet
        finallist_worksheet = self.sheet.add_worksheet(
            title='FinalList',
            rows=len(self.final_list) + 10,
            cols=5
        )
        
        # Set headers
        headers = ['Name', 'IMDbCastID', 'CorrectDate', 'AlternativeNames', 'IMDbSeriesIDs']
        finallist_worksheet.update('A1:E1', [headers])
        print("   ✅ Headers set")
        
        # Prepare data
        data_rows = []
        for record in self.final_list:
            data_rows.append([
                record['name'],
                record['imdb_cast_id'],
                record['correct_date'],
                record['alternative_names'],
                record['imdb_series_ids']
            ])
        
        # Write data in batches of 200 rows
        if data_rows:
            batch_size = 200
            total_batches = (len(data_rows) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(data_rows))
                batch_data = data_rows[start_idx:end_idx]
                
                start_row = start_idx + 2  # +2 because row 1 is headers, and we're 1-indexed
                end_row = end_idx + 1      # +1 for 1-indexed
                
                print(f"📤 Writing batch {batch_num + 1}/{total_batches}: rows {start_row}-{end_row} ({len(batch_data)} records)")
                
                finallist_worksheet.update(f'A{start_row}:E{end_row}', batch_data)
                print(f"   ✅ Batch {batch_num + 1} written successfully")
        
        print(f"   ✅ Created FinalList sheet with {len(self.final_list)} records in {total_batches} batches")

    def run(self):
        """Run the complete FinalList building process"""
        try:
            self.load_data()
            self.build_final_list()
            
            if not self.dry_run:
                self.create_final_sheet()
            
            print(f"\n🎉 FinalList building complete!")
            print(f"   📊 Final record count: {len(self.final_list)}")
            
            if self.dry_run:
                print(f"   🧪 This was a DRY RUN - no sheets were modified")
                print(f"   🚀 Run without --dry-run to create the actual FinalList sheet")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='FinalInfo Step 1: Build consolidated FinalList sheet')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run in dry-run mode (no sheet creation)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show verbose output including all qualifying records')
    
    args = parser.parse_args()
    
    builder = FinalListBuilder(dry_run=args.dry_run)
    builder.run()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
RealiteaseInfo Sheet Builder
============================

Creates a Google Sheet called 'RealiteaseInfo' with unique cast members and their show information.

Sheet Structure:
- CastName: Cast member's name
- CastIMDbID: Their IMDb ID
- CastTMDbID: Their TMDb ID
- ShowNames: Comma-separated list of all shows they appeared in
- ShowIMDbIDs: Comma-separated list of all show IMDb IDs
- ShowTMDbIDs: Comma-separated list of all show TMDb IDs
- TotalShows: Number of different shows they appeared in
- TotalSeasons: Total seasons across all shows
- TotalEpisodes: Total episodes across all shows
- Gender: M/F (placeholder for future use)
- Birthday: YYYY-MM-DD format (placeholder for future use)
- Zodiac: Astrological sign (placeholder for future use)

Data Source: CastInfo sheet
"""

import gspread
import time
from collections import defaultdict
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class RealiteaseInfoBuilder:
    def __init__(self):
        """Initialize the RealiteaseInfo builder"""
        print("🎬 Starting RealiteaseInfo Sheet Builder")
        print("=" * 60)
        
        # Initialize Google Sheets connection
        self.gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
        self.spreadsheet = self.gc.open("Realitease2025Data")
        
        # Get or create RealiteaseInfo sheet
        self.realitease_sheet = self.get_or_create_realitease_sheet()
        
        # Load CastInfo data
        self.cast_info_data = self.load_cast_info_data()
        
        
    def get_or_create_realitease_sheet(self):
        """Get existing RealiteaseInfo sheet or create a new one"""
        try:
            sheet = self.spreadsheet.worksheet("RealiteaseInfo")
            print("✅ Found existing RealiteaseInfo sheet")
            
            # Check if headers need updating
            current_headers = sheet.row_values(1)
            expected_headers = [
                "CastName",
                "CastIMDbID", 
                "CastTMDbID",
                "ShowNames",
                "ShowIMDbIDs",
                "ShowTMDbIDs",
                "TotalShows",
                "TotalSeasons",
                "TotalEpisodes",
                "Gender",
                "Birthday",
                "Zodiac"
            ]
            
            if current_headers != expected_headers:
                print("🔄 Updating headers to new format...")
                sheet.update(values=[expected_headers], range_name='A1:L1')
                
                # Format headers
                sheet.format('A1:L1', {
                    'textFormat': {'bold': True},
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9}
                })
                print("✅ Headers updated successfully")
            else:
                print("✅ Headers are already in correct format")
                
            return sheet
        except gspread.WorksheetNotFound:
            print("🔄 Creating new RealiteaseInfo sheet...")
            
            # Create new sheet
            sheet = self.spreadsheet.add_worksheet(title="RealiteaseInfo", rows=10000, cols=12)
            
            # Set up headers
            headers = [
                "CastName",
                "CastIMDbID", 
                "CastTMDbID",
                "ShowNames",
                "ShowIMDbIDs",
                "ShowTMDbIDs",
                "TotalShows",
                "TotalSeasons",
                "TotalEpisodes",
                "Gender",
                "Birthday",
                "Zodiac"
            ]
            
            sheet.update(values=[headers], range_name='A1:L1')
            
            # Format headers
            sheet.format('A1:L1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9}
            })
            
            print("✅ Created RealiteaseInfo sheet with headers")
            return sheet
            
    def load_cast_info_data(self):
        """Load all data from CastInfo sheet"""
        print("🔄 Loading CastInfo data...")
        
        try:
            cast_info_sheet = self.spreadsheet.worksheet("CastInfo")
            
            # Get all data
            all_data = cast_info_sheet.get_all_records()
            print(f"📊 Loaded {len(all_data)} records from CastInfo")
            
            return all_data
            
        except gspread.WorksheetNotFound:
            print("❌ CastInfo sheet not found!")
            return []
        except Exception as e:
            print(f"❌ Error loading CastInfo data: {e}")
            return []
    
    
    def build_cast_aggregation(self):
        """Aggregate all shows for each unique cast member"""
        print("� Building cast member aggregation...")
        
        cast_aggregation = defaultdict(lambda: {
            'cast_name': '',
            'cast_imdb_id': '',
            'cast_tmdb_id': '',
            'shows': [],
            'show_imdb_ids': [],
            'show_tmdb_ids': [],
            'total_seasons': 0,
            'total_episodes': 0
        })
        
        # Aggregate all show data from CastInfo (limit to first 20 for testing)
        print("  📊 Aggregating show data from CastInfo (first 20 records for testing)...")
        test_records = self.cast_info_data[:20]  # Limit to first 20 records
        print(f"  🧪 Processing {len(test_records)} test records out of {len(self.cast_info_data)} total")
        
        for i, record in enumerate(test_records):
            print(f"  📝 Processing record {i+1}/20: {record.get('Name', 'Unknown')} in {record.get('ShowName', 'Unknown Show')}")
            
            cast_name = record.get('Name', '').strip()                # Column A - 'Name' not 'CastName'
            cast_tmdb_id = str(record.get('CastID', '')).strip()      # Column B - 'CastID' not 'TMDb CastID'
            cast_imdb_id = record.get('Cast IMDbID', '').strip()      # Column C - This is correct
            show_name = record.get('ShowName', '').strip()            # Column D - This is correct
            show_imdb_id = record.get('Show IMDbID', '').strip()      # Column E - This is correct
            show_tmdb_id = str(record.get('ShowID', '')).strip()      # Column F - 'ShowID' not 'TMDb ShowID'
            total_episodes = record.get('TotalEpisodes', 0)           # Column G - This is correct
            seasons = str(record.get('TotalSeasons', '')).strip()    # Column H - 'TotalSeasons' not 'Seasons'
            
            print(f"    - Cast: {cast_name} (IMDb: {cast_imdb_id}, TMDb: {cast_tmdb_id})")
            print(f"    - Show: {show_name} (IMDb: {show_imdb_id}, TMDb: {show_tmdb_id})")
            print(f"    - Episodes: {total_episodes}, Seasons: {seasons}")
            
            # Skip if missing essential data
            if not cast_imdb_id or not cast_name or not show_name:
                print(f"    ⚠️ Skipping - missing essential data")
                continue
                
            # Use Cast IMDb ID as the unique key
            cast_key = cast_imdb_id
            
            # Set cast info (should be consistent across records)
            if not cast_aggregation[cast_key]['cast_name']:
                cast_aggregation[cast_key]['cast_name'] = cast_name
                cast_aggregation[cast_key]['cast_imdb_id'] = cast_imdb_id
                cast_aggregation[cast_key]['cast_tmdb_id'] = cast_tmdb_id
                print(f"    ✅ New cast member: {cast_name}")
            
            # Add show info if not already present
            if show_name not in cast_aggregation[cast_key]['shows']:
                cast_aggregation[cast_key]['shows'].append(show_name)
                cast_aggregation[cast_key]['show_imdb_ids'].append(show_imdb_id)
                cast_aggregation[cast_key]['show_tmdb_ids'].append(show_tmdb_id)
                print(f"    📺 Added show: {show_name}")
            else:
                print(f"    🔄 Show already exists: {show_name}")
            
            # Add to totals from ALL shows (each record represents a different show)
            try:
                episodes = int(total_episodes) if total_episodes else 0
                cast_aggregation[cast_key]['total_episodes'] += episodes
                print(f"    📊 Added {episodes} episodes (total now: {cast_aggregation[cast_key]['total_episodes']})")
            except (ValueError, TypeError):
                print(f"    ⚠️ Could not parse episodes: {total_episodes}")
                pass
            
            # Count seasons - could be comma-separated like "1,2,3" or single number
            try:
                if seasons:
                    if ',' in seasons:
                        season_count = len([s.strip() for s in seasons.split(',') if s.strip()])
                    else:
                        season_count = 1 if seasons.strip() else 0
                    cast_aggregation[cast_key]['total_seasons'] += season_count
                    print(f"    📊 Added {season_count} seasons (total now: {cast_aggregation[cast_key]['total_seasons']})")
            except Exception as e:
                print(f"    ⚠️ Could not parse seasons: {seasons} - {e}")
                pass
        
        print(f"  ✅ Aggregated {len(cast_aggregation)} unique cast members from {len(test_records)} test records")
        
        # Print summary of aggregated data
        print("📋 AGGREGATION SUMMARY:")
        for cast_key, data in cast_aggregation.items():
            print(f"  👤 {data['cast_name']} ({cast_key}):")
            print(f"     📺 Shows: {len(data['shows'])} - {', '.join(data['shows'][:3])}{'...' if len(data['shows']) > 3 else ''}")
            print(f"     📊 Totals: {data['total_episodes']} episodes, {data['total_seasons']} seasons")
        return cast_aggregation
    
    
    def write_to_sheet(self, cast_aggregation, dry_run=True):
        """Write aggregated data to RealiteaseInfo sheet WITHOUT clearing ANY data"""
        if dry_run:
            print("🔍 DRY RUN MODE - No data will be written to the sheet")
            print("🔄 Simulating write to RealiteaseInfo sheet...")
        else:
            print("🔄 Writing data to RealiteaseInfo sheet (preserving existing data)...")
        
        # Get current data to preserve existing bio data
        print("  🔍 Reading existing data to preserve bio data...")
        current_data = self.realitease_sheet.get_all_records()
        existing_cast_map = {}
        
        for i, record in enumerate(current_data):
            cast_imdb_id = record.get('CastIMDbID', '')
            if cast_imdb_id:
                existing_cast_map[cast_imdb_id] = {
                    'row_index': i + 2,  # +2 for header and 1-indexing
                    'gender': record.get('Gender', ''),
                    'birthday': record.get('Birthday', ''),
                    'zodiac': record.get('Zodiac', ''),
                    'cast_name': record.get('CastName', ''),
                    'show_names': record.get('ShowNames', ''),
                    'total_shows': record.get('TotalShows', 0),
                    'total_seasons': record.get('TotalSeasons', 0),
                    'total_episodes': record.get('TotalEpisodes', 0)
                }
        
        # Find next available row for new cast members
        next_row = len(current_data) + 2  # +2 for header and 1-indexing
        
        # Process each cast member
        updates_made = 0
        new_members_added = 0
        
        for cast_key, data in cast_aggregation.items():
            cast_imdb_id = data['cast_imdb_id']
            
            # Join lists with commas
            show_names = ', '.join(data['shows'])
            show_imdb_ids = ', '.join(data['show_imdb_ids'])
            show_tmdb_ids = ', '.join(data['show_tmdb_ids'])
            total_shows = len(data['shows'])
            total_seasons = data['total_seasons']
            total_episodes = data['total_episodes']
            
            if cast_imdb_id in existing_cast_map:
                # Cast member exists - update their show data and preserve bio data
                existing = existing_cast_map[cast_imdb_id]
                row_num = existing['row_index']
                
                # Only update if show data has changed
                if (existing['show_names'] != show_names or 
                    existing['total_shows'] != total_shows or
                    existing['total_seasons'] != total_seasons or
                    existing['total_episodes'] != total_episodes):
                    
                    row_data = [
                        data['cast_name'],           # A
                        data['cast_imdb_id'],        # B  
                        data['cast_tmdb_id'],        # C
                        show_names,                  # D
                        show_imdb_ids,               # E
                        show_tmdb_ids,               # F
                        total_shows,                 # G
                        total_seasons,               # H
                        total_episodes,              # I
                        existing['gender'],          # J - preserve existing
                        existing['birthday'],        # K - preserve existing  
                        existing['zodiac']           # L - preserve existing
                    ]
                    
                    # Update this specific row
                    if dry_run:
                        print(f"  🔍 DRY RUN: Would update row {row_num} for {data['cast_name']}")
                    else:
                        range_name = f'A{row_num}:L{row_num}'
                        self.realitease_sheet.update(values=[row_data], range_name=range_name)
                        time.sleep(0.8)  # Rate limiting
                    updates_made += 1
                    print(f"  ✅ Updated existing cast member: {data['cast_name']} (row {row_num})")
            else:
                # New cast member - add them
                row_data = [
                    data['cast_name'],           # A
                    data['cast_imdb_id'],        # B  
                    data['cast_tmdb_id'],        # C
                    show_names,                  # D
                    show_imdb_ids,               # E
                    show_tmdb_ids,               # F
                    total_shows,                 # G
                    total_seasons,               # H
                    total_episodes,              # I
                    '',                          # J - empty gender for new member
                    '',                          # K - empty birthday for new member  
                    ''                           # L - empty zodiac for new member
                ]
                
                # Add at next available row
                if dry_run:
                    print(f"  🔍 DRY RUN: Would add row {next_row} for {data['cast_name']}")
                else:
                    range_name = f'A{next_row}:L{next_row}'
                    self.realitease_sheet.update(values=[row_data], range_name=range_name)
                    time.sleep(0.8)  # Rate limiting
                new_members_added += 1
                next_row += 1
                print(f"  ✅ Added new cast member: {data['cast_name']} (row {next_row-1})")
        
        total_cast_count = len(cast_aggregation)
        print(f"✅ Processing complete! Updated: {updates_made}, New members: {new_members_added}, Total cast: {total_cast_count}")
        return total_cast_count
    
    
    def add_summary_stats(self, total_cast_count):
        """Add summary statistics to the sheet"""
        print("🔄 Adding summary statistics...")
        
        # Find an empty area for stats (after the data)
        stats_start_row = total_cast_count + 5  # Leave some space
        
        stats_data = [
            ["📊 REALITEASE INFO SUMMARY", ""],
            ["=" * 30, ""],
            ["Total Unique Cast Members:", total_cast_count],
            ["Data Source:", "CastInfo Sheet"],
            ["Last Updated:", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ["", ""],
            ["📝 Notes:", ""],
            ["- CastTMDbID: From CastInfo Column B (TMDb CastID)", ""],
            ["- ShowTMDbIDs: From CastInfo Column F (TMDb ShowID)", ""],
            ["- TotalShows: Number of different shows each cast member appeared in", ""],
            ["- TotalSeasons: Total seasons across all shows for each cast member", ""],
            ["- TotalEpisodes: Total episodes across all shows for each cast member", ""],
            ["- Gender/Birthday/Zodiac: Placeholders for future bio data extraction", ""]
        ]
        
        # Write stats
        for i, row in enumerate(stats_data):
            range_name = f'A{stats_start_row + i}:B{stats_start_row + i}'
            self.realitease_sheet.update(values=[row], range_name=range_name)
            time.sleep(0.5)
        
        print("✅ Added summary statistics")
    
    def cleanup(self):
        """Clean up resources"""
        print("✅ Cleanup complete")
    
    def run(self):
        """Main execution method"""
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"🔍 Checking CastInfo data...")
        if not self.cast_info_data:
            print("❌ No CastInfo data found. Exiting.")
            return
        
        print(f"📊 Found {len(self.cast_info_data)} records in CastInfo")
        
        # Show first few records for debugging
        print("🔍 First 3 CastInfo records:")
        for i, record in enumerate(self.cast_info_data[:3]):
            print(f"  Record {i+1}: {dict(record)}")
        
        # Build aggregation
        try:
            cast_aggregation = self.build_cast_aggregation()
        except Exception as e:
            print(f"❌ Error during aggregation: {e}")
            import traceback
            traceback.print_exc()
            return
        
        if not cast_aggregation:
            print("❌ No cast data to process. Exiting.")
            return
        
        # Write data to sheet (DRY RUN)
        print("🔄 Writing cast and show data to sheet (DRY RUN)...")
        try:
            total_count = self.write_to_sheet(cast_aggregation, dry_run=True)
            print(f"✅ Successfully processed {total_count} cast members with show data (DRY RUN)")
        except Exception as e:
            print(f"❌ Error during write simulation: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # Skip summary stats for dry run
        print("⏭️ Skipping summary stats for dry run")
        
        # Cleanup
        self.cleanup()
        
        print("\n🎉 RealiteaseInfo Dry Run Complete!")
        print(f"📊 Final Stats:")
        print(f"   👤 Unique Cast Members: {total_count}")
        print(f"   📺 Source Records: {len(self.cast_info_data)}")
        print(f"   ⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main entry point"""
    builder = None
    try:
        builder = RealiteaseInfoBuilder()
        builder.run()
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if builder:
            builder.cleanup()


if __name__ == "__main__":
    main()

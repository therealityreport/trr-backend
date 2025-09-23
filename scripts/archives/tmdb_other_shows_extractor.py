#!/usr/bin/env python3

import os
import sys
import time
import re
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(load_env_path)

class TMDBOtherShowsExtractor:
    def __init__(self):
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        
        if not self.tmdb_bearer and not self.tmdb_api_key:
            raise ValueError("Neither TMDB_BEARER nor TMDB_API_KEY found in environment variables")
        
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        
        # Use bearer token if available, otherwise use API key
        if self.tmdb_bearer:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_bearer}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB Bearer token for authentication")
        else:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_api_key}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB API key for authentication")
        
        # Test API key first
        test_response = self.session.get(f"{self.base_url}/configuration")
        if test_response.status_code != 200:
            print(f"❌ TMDB API Key test failed: {test_response.status_code}")
            print(f"❌ Response: {test_response.text}")
            raise ValueError("Invalid TMDB API key or API access issue")
        else:
            print("✅ TMDB API Key validated successfully")
        
        # Google Sheets setup
        self.gc = None
        self.worksheet = None
        
        # Show mappings
        self.show_configs = {
            "116250": {"name": "Bling Empire", "id": "116250"},
            "118829": {"name": "The Real World Homecoming", "id": "118829"},
            "203423": {"name": "The Challenge: USA", "id": "203423"}
        }
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 TMDB Other Shows: Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("ViableCast")
            
            print("✅ TMDB Other Shows: Google Sheets connection successful")
            return True
            
        except FileNotFoundError as e:
            print(f"❌ TMDB Other Shows: Credentials file not found: {str(e)}")
            return False
        except gspread.exceptions.SpreadsheetNotFound as e:
            print(f"❌ TMDB Other Shows: Spreadsheet not found: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ TMDB Other Shows: Google Sheets setup failed: {str(e)}")
            print(f"❌ TMDB Other Shows: Error type: {type(e)}")
            return False

    def extract_show_episodes(self, tmdb_person_id, tmdb_show_id, cast_name):
        """Extract episode information for a person from a specific show"""
        try:
            show_config = self.show_configs.get(str(tmdb_show_id))
            show_name = show_config["name"] if show_config else f"Show {tmdb_show_id}"
            
            print(f"🎭 TMDB {show_name}: Processing {cast_name} (Person: {tmdb_person_id}, Show: {tmdb_show_id})")
            
            # Get TV credits for the person
            print(f"🔍 TMDB {show_name}: Getting TV credits for person ID: {tmdb_person_id}")
            credits_url = f"{self.base_url}/person/{tmdb_person_id}/tv_credits"
            credits_response = self.session.get(credits_url)
            
            if credits_response.status_code != 200:
                print(f"❌ TMDB {show_name}: Failed to get TV credits for {cast_name}: {credits_response.status_code}")
                return None
            
            credits_data = credits_response.json()
            
            # Look for cast credits for the specific show
            cast_credits = []
            crew_credits = []
            
            for credit in credits_data.get('cast', []):
                if str(credit.get('id')) == str(tmdb_show_id):
                    cast_credits.append(credit)
                    print(f"✅ TMDB {show_name}: Found cast credit for {cast_name}")
            
            for credit in credits_data.get('crew', []):
                if str(credit.get('id')) == str(tmdb_show_id):
                    crew_credits.append(credit)
                    print(f"✅ TMDB {show_name}: Found crew credit for {cast_name}")
            
            all_credits = cast_credits + crew_credits
            
            if not all_credits:
                # Debug: show what shows this person has credits for
                all_show_ids = []
                for credit in credits_data.get('cast', []) + credits_data.get('crew', []):
                    show_id = credit.get('id')
                    show_name_debug = credit.get('name', 'Unknown')
                    if show_id:
                        all_show_ids.append(f"{show_id}:{show_name_debug}")
                print(f"🔍 TMDB {show_name}: {cast_name} has credits for shows: {all_show_ids[:3]}...")
                print(f"⚠️ TMDB {show_name}: No {show_name} credits found for {cast_name}")
                return None
            
            # Process each credit to get detailed season information
            total_episodes = 0
            all_seasons = set()
            
            for credit in all_credits:
                credit_id = credit.get('credit_id')
                episode_count = credit.get('episode_count', 0)
                
                print(f"🔍 TMDB {show_name}: Processing credit - ID: {credit_id}, Episode count: {episode_count}")
                print(f"📊 TMDB {show_name}: Using episode count from credit: {episode_count}")
                
                if credit_id and episode_count > 0:
                    # Get detailed season information
                    print(f"🔍 TMDB {show_name}: Getting detailed seasons for credit_id: {credit_id}")
                    detailed_seasons = self.get_credit_details(credit_id, cast_name, show_name)
                    
                    if detailed_seasons:
                        print(f"📺 TMDB {show_name}: Found seasons {detailed_seasons} for {cast_name}")
                        for season_num in detailed_seasons:
                            all_seasons.add(season_num)
                    
                    total_episodes += episode_count
                else:
                    # If no credit_id or episode count, still count the credit
                    if episode_count > 0:
                        total_episodes += episode_count
                    else:
                        total_episodes += 1  # Assume at least 1 episode if no count provided
            
            # Process seasons - exclude season 0 unless it's the only season
            seasons_list = sorted([s for s in all_seasons if s is not None and s != 0])
            
            # Only include Season 0 if it's the only season found
            if 0 in all_seasons and not seasons_list:
                seasons_list = [0]
            
            # Format seasons string with spaces
            if seasons_list:
                seasons_str = ", ".join(map(str, seasons_list))
            else:
                seasons_str = ""  # Leave blank instead of "Unknown"
            
            print(f"✅ TMDB {show_name}: {cast_name} - {total_episodes} episodes, Season(s) {seasons_str}")
            
            return {
                'episodes': total_episodes,
                'seasons': seasons_str
            }
            
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error processing {cast_name}: {e}")
            return None

    def get_credit_details(self, credit_id, cast_name, show_name):
        """Get detailed season information for a specific credit"""
        try:
            print(f"🔍 TMDB {show_name}: Getting credit details for ID: {credit_id}")
            credit_url = f"{self.base_url}/credit/{credit_id}"
            credit_response = self.session.get(credit_url)
            
            if credit_response.status_code != 200:
                print(f"❌ TMDB {show_name}: Failed to get credit details for {cast_name}: {credit_response.status_code}")
                return []
            
            credit_data = credit_response.json()
            
            # Extract season information from media
            seasons = []
            if 'media' in credit_data and 'seasons' in credit_data['media']:
                seasons_data = credit_data['media']['seasons']
                print(f"🔍 TMDB {show_name}: Credit details returned {len(seasons_data)} seasons")
                
                for season in seasons_data:
                    season_number = season.get('season_number')
                    if season_number is not None and season_number > 0:  # Exclude season 0 (specials)
                        seasons.append(season_number)
                        print(f"✅ TMDB {show_name}: Found Season {season_number}")
                        
            return seasons
            
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error getting credit details for {cast_name}: {e}")
            return []

    def update_spreadsheet(self, row_index, episodes, seasons, cast_name, show_name, current_episodes="", current_seasons=""):
        """Update the Google Sheets with extracted data"""
        try:
            print(f"🔄 TMDB {show_name}: Updating row {row_index} for {cast_name}")
            
            # Determine which columns to update based on current values
            current_episodes_val = current_episodes.strip().upper() if current_episodes else ""
            current_seasons_val = current_seasons.strip().upper() if current_seasons else ""
            
            should_update_episodes = not current_episodes_val or current_episodes_val == "SKIP"
            should_update_seasons = not current_seasons_val or current_seasons_val == "SKIP"
            
            updates_made = []
            
            # Update Episodes column (column 7) if needed
            if should_update_episodes:
                self.worksheet.update_cell(row_index, 7, episodes)   # Column 6 (EpisodeCount) - 1-indexed so 7
                updates_made.append(f"Episodes: {episodes}")
            
            # Update Seasons column (column 8) if needed  
            if should_update_seasons:
                self.worksheet.update_cell(row_index, 8, seasons)    # Column 7 (Seasons) - 1-indexed so 8
                updates_made.append(f"Seasons: {seasons}")
            
            if updates_made:
                print(f"✅ TMDB {show_name}: Updated {cast_name} - {', '.join(updates_made)}")
            else:
                print(f"ℹ️ TMDB {show_name}: No updates needed for {cast_name} - data already exists")
            
            # Add small delay to avoid hitting rate limits
            time.sleep(0.5)
            return True
            
        except Exception as e:
            print(f"⚠️ TMDB {show_name}: Failed to update spreadsheet for {cast_name}: {e}")
            print(f"⚠️ TMDB {show_name}: Row index: {row_index}, Episodes: {episodes}, Seasons: {seasons}")
            return False

    def process_shows(self, target_show_ids=None, max_rows=None):
        """Process cast members for specified shows"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from the spreadsheet
            all_data = self.worksheet.get_all_values()
            
            if len(all_data) < 2:  # Header + at least one row
                print("❌ No data found in spreadsheet")
                return False
            
            headers = all_data[0]
            data_rows = all_data[1:]
            
            print(f"📊 Found {len(data_rows)} total rows in spreadsheet")
            
            # Limit to max_rows if specified and start from row 2012
            if max_rows:
                # Start from row 2012 (index 2011) and go to max_rows
                start_row = 2011  # Row 2012 is at index 2011 (0-based)
                data_rows = data_rows[start_row:max_rows]
                print(f"📊 Processing rows {start_row + 2} to {max_rows} ({len(data_rows)} rows)")
            
            # Filter rows by target show IDs if specified
            if target_show_ids:
                target_rows = []
                for i, row in enumerate(data_rows):
                    if len(row) > 4:  # Make sure there's a ShowID column
                        show_id = row[4] if len(row) > 4 else ''
                        if show_id in target_show_ids:
                            target_rows.append((i, row))
                
                print(f"📊 Found {len(target_rows)} rows matching target show IDs: {target_show_ids}")
            else:
                # Process all rows within the limit
                target_rows = [(i, row) for i, row in enumerate(data_rows)]
                print(f"📊 Processing all {len(target_rows)} rows")
            
            if not target_rows:
                print("❌ No matching rows found")
                return False
            
            # Process each matching row
            processed_count = 0
            updated_count = 0
            start_row_offset = 2011 if max_rows else 0  # Offset for starting from row 2012
            
            for row_idx, row in target_rows:
                processed_count += 1
                row_index = row_idx + 2 + start_row_offset  # Account for header and starting offset                # Parse row data
                cast_name = row[0] if len(row) > 0 else ''           # Column 0: CastName
                tmdb_person_id = row[1] if len(row) > 1 else ''      # Column 1: CastID (TMDB Person ID)
                episodes = row[6] if len(row) > 6 else ''            # Column 6: Episodes
                seasons = row[7] if len(row) > 7 else ''             # Column 7: Seasons
                tmdb_show_id = row[4] if len(row) > 4 else ''        # Column 4: ShowID (TMDB Show ID)
                
                # Check if we should process this row
                episodes_val = episodes.strip().upper() if episodes else ""
                seasons_val = seasons.strip().upper() if seasons else ""
                
                # Skip if we already have valid data (not empty, not "SKIP")
                has_episodes_data = episodes_val and episodes_val != "SKIP"
                has_seasons_data = seasons_val and seasons_val != "SKIP"
                
                if has_episodes_data and has_seasons_data:
                    print(f"⏭️ TMDB: Skipping {cast_name} - already has data (Episodes: {episodes}, Seasons: {seasons})")
                    continue
                else:
                    # Process if either field is empty or contains "SKIP"
                    if not has_episodes_data and not has_seasons_data:
                        print(f"🔄 TMDB: Processing {cast_name} - both Episodes and Seasons are empty/SKIP")
                    elif not has_episodes_data:
                        print(f"🔄 TMDB: Processing {cast_name} - Episodes is empty/SKIP")
                    elif not has_seasons_data:
                        print(f"🔄 TMDB: Processing {cast_name} - Seasons is empty/SKIP")
                    else:
                        print(f"🔄 TMDB: Processing {cast_name} - found SKIP, will try to find data")
                
                # Validate required data
                if not all([cast_name, tmdb_person_id, tmdb_show_id]):
                    print(f"⚠️ TMDB: Skipping row {row_index} - missing required data")
                    continue
                
                row_data = {
                    'cast_name': cast_name.strip(),
                    'tmdb_person_id': tmdb_person_id.strip(),
                    'tmdb_show_id': tmdb_show_id.strip(),
                    'row_index': row_index
                }
                
                show_config = self.show_configs.get(str(tmdb_show_id))
                show_name = show_config["name"] if show_config else f"Show {tmdb_show_id}"
                
                print(f"🎭 TMDB {show_name}: Processing {processed_count}/{len(target_rows)}: {cast_name}")
                
                # Extract episode data
                result = self.extract_show_episodes(
                    row_data['tmdb_person_id'], 
                    row_data['tmdb_show_id'], 
                    row_data['cast_name']
                )
                
                if result:
                    # Update spreadsheet
                    if self.update_spreadsheet(
                        row_data['row_index'],
                        result['episodes'],
                        result['seasons'],
                        row_data['cast_name'],
                        show_name,
                        episodes,  # Current episodes value
                        seasons   # Current seasons value
                    ):
                        updated_count += 1
                else:
                    print(f"❌ TMDB {show_name}: No data found for {cast_name}")
                    # If we had SKIP and couldn't find data, leave it as SKIP
                    episodes_val = episodes.strip().upper() if episodes else ""
                    seasons_val = seasons.strip().upper() if seasons else ""
                    if episodes_val == "SKIP" or seasons_val == "SKIP":
                        print(f"⚠️ TMDB {show_name}: Leaving SKIP for {cast_name} - no data found")
                
                # Add delay between requests to avoid rate limiting
                time.sleep(1)
            
            print(f"\n🎉 TMDB: Processing complete!")
            print(f"✅ Successfully updated: {updated_count}/{processed_count} cast members")
            return True
            
        except Exception as e:
            print(f"❌ Error processing shows: {e}")
            return False

def main():
    """Main function"""
    extractor = TMDBOtherShowsExtractor()
    
    # Process first 1213 rows for all shows
    target_shows = None  # Process all shows, not just specific ones
    
    print("🚀 Starting TMDB Other Shows Extractor...")
    print(f"🎯 Processing rows 2012-3016 for all shows")
    
    success = extractor.process_shows(target_shows, max_rows=3016)
    
    if success:
        print("✅ Extraction completed successfully!")
    else:
        print("❌ Extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

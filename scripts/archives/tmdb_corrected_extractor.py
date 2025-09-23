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

class TMDBCorrectedExtractor:
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
            raise ValueError("Invalid TMDB API key or API access issue")
        else:
            print("✅ TMDB API Key validated successfully")
        
        # Google Sheets setup
        self.gc = None
        self.worksheet = None
        
        # Row range configuration
        self.start_row = 3079
        self.end_row = 3555
        
        # Processing counters
        self.processed_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        
    def setup_google_sheets(self):
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 TMDB Corrected: Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("ViableCast")
            
            print("✅ TMDB Corrected: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ TMDB Corrected: Google Sheets setup failed: {str(e)}")
            return False

    def setup_google_sheets(self):

    def test_tmdb_person_exists(self, person_id):
        """Test if a TMDB person ID exists by trying to get their basic info"""
        try:
            test_url = f"{self.base_url}/person/{person_id}"
            test_response = self.session.get(test_url)
            
            if test_response.status_code == 200:
                person_data = test_response.json()
                name = person_data.get('name', 'Unknown')
                print(f"✅ Confirmed TMDB person exists: {person_id} ({name})")
                return True
            else:
                print(f"❌ TMDB person not found: {person_id} (Status: {test_response.status_code})")
                return False
                
        except Exception as e:
            print(f"❌ Error testing TMDB person {person_id}: {e}")
            return False

    def get_credit_details_improved(self, credit_id, cast_name, show_name):
        """Get detailed episode and season information from credit details using improved extraction"""
        try:
            print(f"🔍 TMDB {show_name}: Getting credit details for ID: {credit_id}")
            credit_url = f"{self.base_url}/credit/{credit_id}"
            credit_response = self.session.get(credit_url)
            
            if credit_response.status_code != 200:
                print(f"❌ TMDB {show_name}: Failed to get credit details for {cast_name}: {credit_response.status_code}")
                return None
            
            credit_data = credit_response.json()
            
            total_episodes = 0
            seasons_found = set()
            
            # PRIORITY 1: Extract from episodes array (most reliable)
            episodes_array = credit_data.get('media', {}).get('episodes', [])
            if episodes_array:
                print(f"🎯 TMDB {show_name}: Found {len(episodes_array)} episodes in episodes array")
                total_episodes = len(episodes_array)
                
                # Extract season numbers from episodes
                for episode in episodes_array:
                    season_num = episode.get('season_number')
                    if season_num is not None and season_num > 0:  # Exclude season 0 (specials)
                        seasons_found.add(season_num)
                        
                print(f"✅ TMDB {show_name}: Episodes array - {total_episodes} episodes, seasons: {sorted(seasons_found)}")
            
            # PRIORITY 2: Extract from seasons array (if episodes array was empty)
            if not episodes_array:
                seasons_array = credit_data.get('media', {}).get('seasons', [])
                if seasons_array:
                    print(f"🎯 TMDB {show_name}: Found {len(seasons_array)} seasons in seasons array")
                    
                    for season in seasons_array:
                        season_number = season.get('season_number')
                        episode_count = season.get('episode_count', 0)
                        
                        if season_number is not None and season_number > 0:  # Exclude season 0
                            seasons_found.add(season_number)
                            total_episodes += episode_count
                            print(f"✅ TMDB {show_name}: Season {season_number} - {episode_count} episodes")
            
            # Return results if we found valid data
            if total_episodes > 0 and seasons_found:
                return {
                    'episodes': total_episodes,
                    'seasons': sorted(list(seasons_found))
                }
            elif total_episodes > 0:
                # Have episodes but no seasons - still useful
                return {
                    'episodes': total_episodes,
                    'seasons': []
                }
            else:
                print(f"⚠️ TMDB {show_name}: No episodes or seasons found in credit details")
                return None
                
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error getting credit details for {cast_name}: {e}")
            return None

    def get_tmdb_person_id(self, person_id, cast_name):
        """Determine the correct TMDB person ID based on the input format"""
        try:
            # Check if it's an IMDb ID (starts with 'nm' followed by digits)
            if person_id.startswith('nm') and len(person_id) > 2:
                print(f"🔄 Detected IMDb ID: {person_id}, converting to TMDB ID...")
                converted_id = self.convert_imdb_to_tmdb_id(person_id, cast_name)
                if converted_id:
                    return converted_id
                else:
                    print(f"❌ IMDb ID {person_id} not found in TMDB database for {cast_name}")
                    return None
            
            # Check if it's a numeric TMDB Person ID (all digits)
            elif person_id.isdigit():
                print(f"✅ Detected numeric TMDB Person ID: {person_id}")
                # Test if this TMDB ID actually exists
                if self.test_tmdb_person_exists(person_id):
                    return person_id
                else:
                    print(f"❌ TMDB Person ID {person_id} does not exist for {cast_name}")
                    return None
            
            # Check if it's a long alphanumeric TMDB Object ID (24+ characters, mixed letters/numbers)
            elif len(person_id) >= 20 and any(c.isalpha() for c in person_id) and any(c.isdigit() for c in person_id):
                print(f"✅ Detected TMDB Object ID: {person_id}")
                # Test if this TMDB ID actually exists
                if self.test_tmdb_person_exists(person_id):
                    return person_id
                else:
                    print(f"❌ TMDB Object ID {person_id} does not exist for {cast_name}")
                    return None
            
            # Check if it's a shorter alphanumeric ID (some TMDB IDs are shorter)
            elif len(person_id) >= 8 and any(c.isalpha() for c in person_id):
                print(f"✅ Detected alphanumeric TMDB ID: {person_id}")
                # Test if this TMDB ID actually exists
                if self.test_tmdb_person_exists(person_id):
                    return person_id
                else:
                    print(f"❌ TMDB ID {person_id} does not exist for {cast_name}")
                    return None
            
            else:
                print(f"⚠️ Unknown ID format: {person_id} (length: {len(person_id)})")
                # Try using it as-is and test if it exists
                if self.test_tmdb_person_exists(person_id):
                    return person_id
                else:
                    print(f"❌ ID {person_id} does not exist in TMDB for {cast_name}")
                    return None
                
        except Exception as e:
            print(f"❌ Error determining TMDB person ID for {person_id}: {e}")
            return None

    def extract_show_episodes(self, person_tmdb_id, tmdb_show_id, cast_name, show_name):
        """Extract episode information for a person from a specific show"""
        try:
            print(f"🎭 TMDB {show_name}: Processing {cast_name} (Person: {person_tmdb_id}, Show: {tmdb_show_id})")
            
            # Validate that we have a TMDB Person ID
            if not person_tmdb_id or not person_tmdb_id.strip():
                print(f"❌ No TMDB Person ID provided for {cast_name}")
                return None
            
            # Get TV credits for the person
            print(f"🔍 TMDB {show_name}: Getting TV credits for TMDB person ID: {person_tmdb_id}")
            credits_url = f"{self.base_url}/person/{person_tmdb_id}/tv_credits"
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
                print(f"⚠️ TMDB {show_name}: No credits found for {cast_name} in show {tmdb_show_id}")
                return None
            
            # Process each credit to get detailed season information
            total_episodes = 0
            all_seasons = set()
            
            for credit in all_credits:
                credit_id = credit.get('credit_id')
                episode_count = credit.get('episode_count', 0)
                
                print(f"🔍 TMDB {show_name}: Processing credit - ID: {credit_id}, Episode count: {episode_count}")
                
                if credit_id:
                    # Get detailed season information using improved logic
                    print(f"🔍 TMDB {show_name}: Getting detailed seasons for credit_id: {credit_id}")
                    credit_result = self.get_credit_details_improved(credit_id, cast_name, show_name)
                    
                    if credit_result:
                        episodes_from_credit = credit_result.get('episodes', 0)
                        seasons_from_credit = credit_result.get('seasons', [])
                        
                        print(f"📺 TMDB {show_name}: Credit details - Episodes: {episodes_from_credit}, Seasons: {seasons_from_credit}")
                        
                        total_episodes += episodes_from_credit
                        for season_num in seasons_from_credit:
                            all_seasons.add(season_num)
                    else:
                        # Fallback to episode count from main credit
                        if episode_count > 0:
                            total_episodes += episode_count
                        else:
                            total_episodes += 1  # Assume at least 1 episode
                else:
                    # No credit_id, use episode count from main credit
                    if episode_count > 0:
                        total_episodes += episode_count
                    else:
                        total_episodes += 1  # Assume at least 1 episode
            
            # If we didn't find any episodes or seasons, return None to skip the row
            if total_episodes == 0 and not all_seasons:
                print(f"❌ TMDB {show_name}: No episodes or seasons found for {cast_name} - will skip row")
                return None
            
            # Process seasons - exclude season 0 unless it's the only season
            seasons_list = sorted([s for s in all_seasons if s is not None and s != 0])
            
            # Only include Season 0 if it's the only season found
            if 0 in all_seasons and not seasons_list:
                seasons_list = [0]
            
            # Format seasons string with spaces
            if seasons_list:
                seasons_str = ", ".join(map(str, seasons_list))
            else:
                # If no seasons found but we have episodes, assume season 1
                if total_episodes > 0:
                    print(f"⚠️ TMDB {show_name}: Found {total_episodes} episodes but no seasons for {cast_name}, assuming Season 1")
                    seasons_str = "1"
                else:
                    print(f"❌ TMDB {show_name}: No valid seasons found for {cast_name} - will skip row")
                    return None
            
            print(f"✅ TMDB {show_name}: {cast_name} - {total_episodes} episodes, Season(s) {seasons_str}")
            
            return {
                'episodes': total_episodes,
                'seasons': seasons_str
            }
            
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error processing {cast_name}: {e}")
            return None

    def update_spreadsheet(self, row_index, episodes, seasons, cast_name, show_name):
        """Update the Google Sheets with extracted data"""
        try:
            print(f"🔄 TMDB {show_name}: Updating row {row_index} for {cast_name}")
            
            # Update Episodes column (column G = 7)
            self.worksheet.update_cell(row_index, 7, episodes)
            
            # Update Seasons column (column H = 8)  
            self.worksheet.update_cell(row_index, 8, seasons)
            
            print(f"✅ TMDB {show_name}: Updated {cast_name} - Episodes: {episodes}, Seasons: {seasons}")
            
            # Add small delay to avoid hitting rate limits
            time.sleep(0.5)
            return True
            
        except Exception as e:
            print(f"⚠️ TMDB {show_name}: Failed to update spreadsheet for {cast_name}: {e}")
            return False

    def process_range(self):
        """Process cast members in the specified row range"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from the spreadsheet
            all_data = self.worksheet.get_all_values()
            
            if len(all_data) < self.end_row:
                print(f"❌ Spreadsheet only has {len(all_data)} rows, but need at least {self.end_row}")
                return False
            
            headers = all_data[0]
            
            print(f"📊 TMDB Corrected: Processing rows {self.start_row} to {self.end_row}")
            print(f"📊 TMDB Corrected: Total rows to process: {self.end_row - self.start_row + 1}")
            
            # Process each row in the specified range
            for row_num in range(self.start_row, self.end_row + 1):
                row_index = row_num - 1  # Convert to 0-based index
                row = all_data[row_index]
                
                self.processed_count += 1
                
                # Parse row data (corrected column mapping)
                cast_name = row[2] if len(row) > 2 else ''           # Column C: CastName
                person_tmdb_id = row[1] if len(row) > 1 else ''      # Column B: Person TMDb ID
                episodes = row[6] if len(row) > 6 else ''            # Column G: Episodes
                seasons = row[7] if len(row) > 7 else ''             # Column H: Seasons
                tmdb_show_id = row[4] if len(row) > 4 else ''        # Column E: Show TMDb ID
                show_name = row[5] if len(row) > 5 else ''           # Column F: Show name
                
                # Check if we already have both episodes and seasons data
                episodes_val = episodes.strip() if episodes else ""
                seasons_val = seasons.strip() if seasons else ""
                
                if episodes_val and seasons_val:
                    print(f"⏭️ TMDB: Row {row_num} - {cast_name} already has data (Episodes: {episodes}, Seasons: {seasons})")
                    self.skipped_count += 1
                    continue
                
                # Validate required data
                if not all([cast_name, person_tmdb_id, tmdb_show_id]):
                    print(f"⚠️ TMDB: Row {row_num} - missing required data (CastName: '{cast_name}', PersonTMDbID: '{person_tmdb_id}', ShowID: '{tmdb_show_id}')")
                    self.skipped_count += 1
                    continue
                
                print(f"\n🎭 TMDB: Processing row {row_num}/{self.end_row}: {cast_name} from {show_name}")
                
                # Extract episode data
                result = self.extract_show_episodes(
                    person_tmdb_id.strip(), 
                    tmdb_show_id.strip(), 
                    cast_name.strip(),
                    show_name.strip() if show_name else f"Show {tmdb_show_id}"
                )
                
                if result:
                    # Update spreadsheet
                    if self.update_spreadsheet(
                        row_num,  # 1-indexed row number for Google Sheets
                        result['episodes'],
                        result['seasons'],
                        cast_name,
                        show_name
                    ):
                        self.updated_count += 1
                    else:
                        self.failed_count += 1
                else:
                    print(f"❌ TMDB: Row {row_num} - No data found for {cast_name}, SKIPPING ENTIRE ROW")
                    self.failed_count += 1
                
                # Add delay between requests to avoid rate limiting
                time.sleep(2.0)  # Increased delay for API stability
                
                # Progress update every 10 rows
                if self.processed_count % 10 == 0:
                    success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
                    print(f"📈 TMDB Progress: {self.processed_count} processed, {self.updated_count} updated, {self.skipped_count} skipped, {self.failed_count} failed (Success: {success_rate:.1f}%)")
            
            # Final summary
            success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
            print(f"\n🎉 TMDB Corrected: Processing complete!")
            print(f"📊 Total rows processed: {self.processed_count}")
            print(f"✅ Successfully updated: {self.updated_count}")
            print(f"⏭️ Skipped (already had data): {self.skipped_count}")
            print(f"❌ Failed/No data found: {self.failed_count}")
            print(f"📈 Success rate: {success_rate:.1f}%")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing range: {e}")
            return False

def main():
    """Main function"""
    extractor = TMDBCorrectedExtractor()
    
    print("🚀 Starting TMDB Corrected Extractor...")
    print(f"🎯 Processing rows {extractor.start_row} to {extractor.end_row}")
    print(f"🔄 Will convert IMDb IDs to TMDB IDs automatically")
    print(f"🔄 Will SKIP entire rows if episodes and seasons cannot be found")
    
    success = extractor.process_range()
    
    if success:
        print("✅ Extraction completed successfully!")
    else:
        print("❌ Extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

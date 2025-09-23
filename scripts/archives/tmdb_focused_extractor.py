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

class TMDBFocusedExtractor:
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
        
        # Row range configuration
        self.start_row = 3079
        self.end_row = 3555
        
        # Processing counters
        self.processed_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 TMDB Focused: Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("ViableCast")
            
            print("✅ TMDB Focused: Google Sheets connection successful")
            return True
            
        except FileNotFoundError as e:
            print(f"❌ TMDB Focused: Credentials file not found: {str(e)}")
            return False
        except gspread.exceptions.SpreadsheetNotFound as e:
            print(f"❌ TMDB Focused: Spreadsheet not found: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ TMDB Focused: Google Sheets setup failed: {str(e)}")
            print(f"❌ TMDB Focused: Error type: {type(e)}")
            return False

    def convert_imdb_to_tmdb_id(self, imdb_id, cast_name):
        """Convert IMDb ID to TMDB Person ID using TMDB's find endpoint"""
        try:
            if not imdb_id or not imdb_id.startswith('nm'):
                print(f"⚠️ Invalid IMDb ID format: {imdb_id}")
                return None
                
            print(f"🔄 Converting IMDb ID {imdb_id} to TMDB ID for {cast_name}")
            find_url = f"{self.base_url}/find/{imdb_id}?external_source=imdb_id"
            find_response = self.session.get(find_url)
            
            if find_response.status_code != 200:
                print(f"❌ Failed to find TMDB ID for {imdb_id}: {find_response.status_code}")
                return None
            
            find_data = find_response.json()
            
            # Check person results
            if find_data.get('person_results'):
                tmdb_person_id = find_data['person_results'][0]['id']
                print(f"✅ Converted {imdb_id} → TMDB ID: {tmdb_person_id}")
                return str(tmdb_person_id)
            
            print(f"❌ No TMDB person found for IMDb ID: {imdb_id}")
            return None
            
        except Exception as e:
            print(f"❌ Error converting IMDb ID {imdb_id}: {e}")
            return None

    def extract_show_episodes(self, person_id, tmdb_show_id, cast_name, show_name):
        """Extract episode information for a person from a specific show"""
        try:
            print(f"🎭 TMDB {show_name}: Processing {cast_name} (Person: {person_id}, Show: {tmdb_show_id})")
            
            # Convert IMDb ID to TMDB ID if needed
            tmdb_person_id = person_id
            if person_id.startswith('nm'):
                print(f"🔄 Detected IMDb ID, converting to TMDB ID...")
                tmdb_person_id = self.convert_imdb_to_tmdb_id(person_id, cast_name)
                if not tmdb_person_id:
                    print(f"❌ Could not convert IMDb ID {person_id} to TMDB ID")
                    return None
            
            # Get TV credits for the person
            print(f"🔍 TMDB {show_name}: Getting TV credits for TMDB person ID: {tmdb_person_id}")
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
                print(f"⚠️ TMDB {show_name}: No credits found for {cast_name} in show {tmdb_show_id}")
                return None
            
            # Process each credit to get detailed season information
            total_episodes = 0
            all_seasons = set()
            
            for credit in all_credits:
                credit_id = credit.get('credit_id')
                episode_count_from_credit = credit.get('episode_count', 0)
                
                print(f"🔍 TMDB {show_name}: Processing credit - ID: {credit_id}, Episode count: {episode_count_from_credit}")
                
                # Always try to get detailed information if we have a credit_id
                if credit_id:
                    print(f"🔍 TMDB {show_name}: Getting detailed info for credit_id: {credit_id}")
                    detailed_info = self.get_credit_details(credit_id, cast_name, show_name)
                    
                    if detailed_info:
                        # Use episode count from detailed info if available, otherwise from credit
                        detailed_episode_count = detailed_info.get('episode_count')
                        if detailed_episode_count:
                            total_episodes += detailed_episode_count
                            print(f"📺 TMDB {show_name}: Used {detailed_episode_count} episodes from credit details")
                        elif episode_count_from_credit > 0:
                            total_episodes += episode_count_from_credit
                            print(f"📺 TMDB {show_name}: Used {episode_count_from_credit} episodes from credit summary")
                        else:
                            total_episodes += 1  # At least 1 episode if we have detailed info
                        
                        # Add seasons from detailed info
                        detailed_seasons = detailed_info.get('seasons', [])
                        if detailed_seasons:
                            print(f"📺 TMDB {show_name}: Found seasons {detailed_seasons} for {cast_name}")
                            for season_num in detailed_seasons:
                                all_seasons.add(season_num)
                    else:
                        # Fallback to credit episode count
                        if episode_count_from_credit > 0:
                            total_episodes += episode_count_from_credit
                        else:
                            total_episodes += 1  # Assume at least 1 episode
                else:
                    # No credit_id, use episode count from credit
                    if episode_count_from_credit > 0:
                        total_episodes += episode_count_from_credit
                    else:
                        total_episodes += 1  # Assume at least 1 episode
            
            # Process seasons - exclude season 0 unless it's the only season
            seasons_list = sorted([s for s in all_seasons if s is not None and s != 0])
            
            # Only include Season 0 if it's the only season found
            if 0 in all_seasons and not seasons_list:
                seasons_list = [0]
            
            # If we still don't have seasons but have episodes, try to get show info directly
            if not seasons_list and total_episodes > 0:
                print(f"🔍 TMDB {show_name}: No seasons from credits, trying direct show lookup")
                direct_seasons = self.get_show_seasons_alternative(tmdb_show_id, cast_name, show_name)
                if direct_seasons:
                    seasons_list = direct_seasons
                    print(f"✅ TMDB {show_name}: Got seasons from direct show lookup: {seasons_list}")
            
            # Format seasons string
            if seasons_list:
                seasons_str = ", ".join(map(str, seasons_list))
            else:
                # If we have episodes but no seasons, assume season 1
                if total_episodes > 0:
                    print(f"⚠️ TMDB {show_name}: Found {total_episodes} episodes but no seasons for {cast_name}, assuming Season 1")
                    seasons_str = "1"
                else:
                    print(f"❌ TMDB {show_name}: No episodes or seasons found for {cast_name} - will skip row")
                    return None
            
            # Ensure we have at least some episode count
            if total_episodes == 0:
                total_episodes = 1  # Assume at least 1 episode if we got this far
            
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
            
            seasons = set()
            episode_count = 0
            
            if 'media' in credit_data:
                media_data = credit_data['media']
                
                # PRIORITY 1: Check episodes array for season information (most reliable)
                if 'episodes' in media_data:
                    episodes_data = media_data['episodes']
                    episode_count = len(episodes_data)
                    print(f"🔍 TMDB {show_name}: Found {episode_count} episodes in credit details")
                    
                    for episode in episodes_data:
                        season_number = episode.get('season_number')
                        if season_number is not None and season_number > 0:
                            seasons.add(season_number)
                    
                    if seasons:
                        sorted_seasons = sorted(list(seasons))
                        print(f"✅ TMDB {show_name}: Found seasons {sorted_seasons} from episodes ({episode_count} total episodes)")
                        return {
                            'seasons': sorted_seasons,
                            'episode_count': episode_count
                        }
                
                # PRIORITY 2: Check seasons array if episodes didn't work
                if 'seasons' in media_data:
                    seasons_data = media_data['seasons']
                    print(f"🔍 TMDB {show_name}: Credit details returned {len(seasons_data)} seasons in seasons array")
                    
                    for season in seasons_data:
                        season_number = season.get('season_number')
                        if season_number is not None and season_number > 0:
                            seasons.add(season_number)
                    
                    if seasons:
                        sorted_seasons = sorted(list(seasons))
                        print(f"✅ TMDB {show_name}: Found seasons {sorted_seasons} from seasons array")
                        return {
                            'seasons': sorted_seasons,
                            'episode_count': episode_count if episode_count > 0 else None
                        }
            
            print(f"⚠️ TMDB {show_name}: No season data found in credit details")
            return None
            
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error getting credit details for {cast_name}: {e}")
            return None

    def get_show_seasons_alternative(self, show_id, cast_name, show_name):
        """Alternative method to get show seasons directly from show data"""
        try:
            print(f"🔍 TMDB {show_name}: Getting show seasons for show ID: {show_id}")
            show_url = f"{self.base_url}/tv/{show_id}"
            show_response = self.session.get(show_url)
            
            if show_response.status_code != 200:
                print(f"❌ TMDB {show_name}: Failed to get show details: {show_response.status_code}")
                return []
            
            show_data = show_response.json()
            
            seasons = []
            if 'seasons' in show_data:
                for season in show_data['seasons']:
                    season_number = season.get('season_number')
                    if season_number is not None and season_number > 0:  # Exclude season 0 (specials)
                        seasons.append(season_number)
                        print(f"✅ TMDB {show_name}: Found Season {season_number} from show data")
            
            return sorted(seasons)
            
        except Exception as e:
            print(f"❌ TMDB {show_name}: Error getting show seasons for {cast_name}: {e}")
            return []

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
            
            print(f"📊 TMDB Focused: Processing rows {self.start_row} to {self.end_row}")
            print(f"📊 TMDB Focused: Total rows to process: {self.end_row - self.start_row + 1}")
            
            # Process each row in the specified range
            for row_num in range(self.start_row, self.end_row + 1):
                row_index = row_num - 1  # Convert to 0-based index
                row = all_data[row_index]
                
                self.processed_count += 1
                
                # Parse row data
                cast_name = row[2] if len(row) > 2 else ''           # Column 2: CastName
                tmdb_person_id = row[3] if len(row) > 3 else ''      # Column 3: Cast IMDb ID / TMDB Person ID
                episodes = row[6] if len(row) > 6 else ''            # Column 6: Episodes (G)
                seasons = row[7] if len(row) > 7 else ''             # Column 7: Seasons (H)
                tmdb_show_id = row[4] if len(row) > 4 else ''        # Column 4: ShowID (TMDB Show ID)
                show_name = row[5] if len(row) > 5 else ''           # Column 5: Show name
                
                # Check if we already have both episodes and seasons data
                episodes_val = episodes.strip() if episodes else ""
                seasons_val = seasons.strip() if seasons else ""
                
                if episodes_val and seasons_val:
                    print(f"⏭️ TMDB: Row {row_num} - {cast_name} already has data (Episodes: {episodes}, Seasons: {seasons})")
                    self.skipped_count += 1
                    continue
                
                # Validate required data
                if not all([cast_name, tmdb_person_id, tmdb_show_id]):
                    print(f"⚠️ TMDB: Row {row_num} - missing required data (CastName: '{cast_name}', PersonID: '{tmdb_person_id}', ShowID: '{tmdb_show_id}')")
                    self.skipped_count += 1
                    continue
                
                print(f"\n🎭 TMDB: Processing row {row_num}/{self.end_row}: {cast_name} from {show_name}")
                
                # Extract episode data
                result = self.extract_show_episodes(
                    tmdb_person_id.strip(), 
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
                time.sleep(1.5)
                
                # Progress update every 10 rows
                if self.processed_count % 10 == 0:
                    success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
                    print(f"📈 TMDB Progress: {self.processed_count} processed, {self.updated_count} updated, {self.skipped_count} skipped, {self.failed_count} failed (Success: {success_rate:.1f}%)")
            
            # Final summary
            success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
            print(f"\n🎉 TMDB Focused: Processing complete!")
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
    extractor = TMDBFocusedExtractor()
    
    print("🚀 Starting TMDB Focused Extractor...")
    print(f"🎯 Processing rows {extractor.start_row} to {extractor.end_row}")
    print(f"🔄 Will SKIP entire rows if episodes and seasons cannot be found")
    
    success = extractor.process_range()
    
    if success:
        print("✅ Extraction completed successfully!")
    else:
        print("❌ Extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

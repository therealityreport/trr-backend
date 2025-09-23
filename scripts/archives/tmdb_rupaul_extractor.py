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

class TMDBRuPaulExtractor:
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
        self.setup_google_sheets()
        
        print("🎭 TMDB RuPaul Extractor: Initialized successfully")

    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 TMDB RuPaul: Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("ViableCast")
            
            print("✅ TMDB RuPaul: Google Sheets connection successful")
            
        except FileNotFoundError as e:
            print(f"❌ TMDB RuPaul: Credentials file not found: {str(e)}")
            raise
        except gspread.exceptions.SpreadsheetNotFound as e:
            print(f"❌ TMDB RuPaul: Spreadsheet not found: {str(e)}")
            raise
        except Exception as e:
            print(f"❌ TMDB RuPaul: Google Sheets setup failed: {str(e)}")
            print(f"❌ TMDB RuPaul: Error type: {type(e)}")
            raise

    def get_person_tmdb_data(self, tmdb_person_id):
        """Get person's TV credits from TMDB"""
        try:
            print(f"🔍 TMDB RuPaul: Getting TV credits for person ID: {tmdb_person_id}")
            
            url = f"{self.base_url}/person/{tmdb_person_id}/tv_credits"
            response = self.session.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ TMDB RuPaul: API error {response.status_code} for person {tmdb_person_id}")
                return None
                
        except Exception as e:
            print(f"⚠️ TMDB RuPaul: Error getting person data: {str(e)}")
            return None

    def get_credit_details(self, credit_id):
        """Get detailed episode information using credit_id"""
        try:
            print(f"🔍 TMDB RuPaul: Getting credit details for ID: {credit_id}")
            
            url = f"{self.base_url}/credit/{credit_id}"
            response = self.session.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ TMDB RuPaul: Credit API error {response.status_code} for {credit_id}")
                return None
                
        except Exception as e:
            print(f"⚠️ TMDB RuPaul: Error getting credit details: {str(e)}")
            return None

    def extract_rupaul_episodes(self, tmdb_person_id, tmdb_show_id, cast_name):
        """Extract RuPaul's Drag Race episode data for a person"""
        try:
            print(f"🎭 TMDB RuPaul: Processing {cast_name} (Person: {tmdb_person_id}, Show: {tmdb_show_id})")
            
            # Handle different ID formats - sometimes IDs are full TMDB object IDs
            if len(tmdb_person_id) > 10:  # Looks like a TMDB object ID
                print(f"🔍 TMDB RuPaul: Long person ID detected, may need different approach: {tmdb_person_id}")
                # For now, try to extract numeric ID if possible
                numeric_match = re.search(r'(\d+)', tmdb_person_id)
                if numeric_match:
                    numeric_person_id = numeric_match.group(1)
                    print(f"🔍 TMDB RuPaul: Extracted numeric ID: {numeric_person_id}")
                else:
                    print(f"⚠️ TMDB RuPaul: Cannot extract numeric ID from {tmdb_person_id}")
                    return None
            else:
                numeric_person_id = tmdb_person_id
            
            # Get person's TV credits using numeric ID
            credits_data = self.get_person_tmdb_data(numeric_person_id)
            if not credits_data:
                return None
            
            # Look for RuPaul's Drag Race in their credits
            rupaul_credits = []
            
            # Check cast credits
            for credit in credits_data.get('cast', []):
                if str(credit.get('id')) == str(tmdb_show_id):
                    rupaul_credits.append(credit)
                    print(f"✅ TMDB RuPaul: Found cast credit for {cast_name}")
            
            # Check crew credits  
            for credit in credits_data.get('crew', []):
                if str(credit.get('id')) == str(tmdb_show_id):
                    rupaul_credits.append(credit)
                    print(f"✅ TMDB RuPaul: Found crew credit for {cast_name}")
            
            if not rupaul_credits:
                print(f"⚠️ TMDB RuPaul: No RuPaul credits found for {cast_name}")
                # Let's also check what shows they do have credits for
                all_show_ids = []
                for credit in credits_data.get('cast', []) + credits_data.get('crew', []):
                    show_id = credit.get('id')
                    show_name = credit.get('name', '')
                    if show_id:
                        all_show_ids.append(f"{show_id}:{show_name}")
                print(f"🔍 TMDB RuPaul: {cast_name} has credits for shows: {all_show_ids[:3]}...")
                return None
            
            # Process each credit to get detailed episode information
            total_episodes = 0
            seasons_found = set()
            episode_details = []
            
            for credit in rupaul_credits:
                credit_id = credit.get('credit_id')
                episode_count_from_credit = credit.get('episode_count', 0)
                
                print(f"🔍 TMDB RuPaul: Processing credit - ID: {credit_id}, Episode count: {episode_count_from_credit}")
                
                if episode_count_from_credit > 0:
                    total_episodes = max(total_episodes, episode_count_from_credit)
                    print(f"📊 TMDB RuPaul: Using episode count from credit: {episode_count_from_credit}")
                
                if credit_id:
                    print(f"🔍 TMDB RuPaul: Getting detailed episodes for credit_id: {credit_id}")
                    
                    credit_details = self.get_credit_details(credit_id)
                    if credit_details:
                        media = credit_details.get('media', {})
                        episodes = media.get('episodes', [])
                        
                        print(f"📺 TMDB RuPaul: Found {len(episodes)} detailed episodes for {cast_name}")
                        
                        for episode in episodes:
                            season_num = episode.get('season_number')
                            episode_num = episode.get('episode_number')
                            episode_name = episode.get('name', '')
                            air_date = episode.get('air_date', '')
                            
                            if season_num is not None:
                                seasons_found.add(season_num)
                                if len(episodes) > total_episodes:  # Use detailed count if higher
                                    total_episodes = len(episodes)
                                episode_details.append({
                                    'season': season_num,
                                    'episode': episode_num,
                                    'name': episode_name,
                                    'air_date': air_date
                                })
                                
                                print(f"✅ TMDB RuPaul: S{season_num}E{episode_num}: {episode_name}")
            
            # Format results
            if total_episodes > 0:
                seasons_list = sorted(list(seasons_found))
                
                # Filter out Season 0 (special episodes) unless it's the only season
                if len(seasons_list) > 1 and 0 in seasons_list:
                    seasons_list = [s for s in seasons_list if s != 0]
                
                # Format as comma-separated list of specific seasons with spaces
                if len(seasons_list) == 1:
                    seasons_str = str(seasons_list[0])
                elif len(seasons_list) > 1:
                    seasons_str = ", ".join(map(str, seasons_list))
                else:
                    seasons_str = "1"  # Default if no season info
                
                result = {
                    'episode_count': total_episodes,
                    'seasons': seasons_str,
                    'found': True,
                    'source': 'tmdb',
                    'details': episode_details
                }
                
                print(f"✅ TMDB RuPaul: {cast_name} - {total_episodes} episodes, Season(s) {seasons_str}")
                return result
            
            print(f"⚠️ TMDB RuPaul: No episode data found for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ TMDB RuPaul: Error extracting episodes for {cast_name}: {str(e)}")
            return None

    def process_rupaul_rows(self):
        """Process only RuPaul's Drag Race rows"""
        try:
            print("🚀 TMDB RuPaul: Starting RuPaul's Drag Race processing")
            
            # Get all data from the sheet
            all_data = self.worksheet.get_all_values()
            headers = all_data[0] if all_data else []
            
            print(f"📋 TMDB RuPaul: Loaded {len(all_data)} total rows")
            if headers:
                print(f"📋 TMDB RuPaul: Total columns: {len(headers)}")
                for i, header in enumerate(headers):
                    if header.strip():  # Only show non-empty headers
                        print(f"📋 TMDB RuPaul: Column {i}: '{header}'")
            
            # Let's also check a few sample rows to understand the structure
            print(f"📋 TMDB RuPaul: Sample rows (first 3):")
            for i, row in enumerate(all_data[1:4], start=2):
                if len(row) > 10:  # Show more columns if they exist
                    print(f"📋 TMDB RuPaul: Row {i}: {row[:15]}...")  # Show first 15 columns
                else:
                    print(f"📋 TMDB RuPaul: Row {i}: {row}")
            
            # Find RuPaul rows - let's check all possible places for the data
            rupaul_rows = []
            rupaul_show_indicators = [
                'rupaul', 'drag race', 'rpdr'
            ]
            
            # Also look for specific TMDB show IDs that correspond to RuPaul shows
            rupaul_tmdb_ids = [
                '63971',  # RuPaul's Drag Race
                '73721',  # RuPaul's Drag Race UK  
                '112802', # RuPaul's Drag Race All Stars
                # Add more as needed
            ]
            
            for i, row in enumerate(all_data[1:], start=2):  # Start from row 2 (index 1)
                if len(row) < 8:  # Ensure row has enough columns (0-7)
                    continue
                
                # Column mapping based on headers:
                # 0: Show IMDbID, 1: CastID (TMDB Person ID), 2: CastName, 3: Cast IMDbID
                # 4: ShowID (TMDB Show ID), 5: ShowName, 6: EpisodeCount, 7: Seasons
                
                show_name = row[5].lower() if len(row) > 5 else ''        # Column 5: ShowName
                tmdb_person_id = row[1] if len(row) > 1 else ''           # Column 1: CastID (TMDB Person ID) 
                tmdb_show_id = row[4] if len(row) > 4 else ''             # Column 4: ShowID (TMDB Show ID)
                cast_name = row[2] if len(row) > 2 else ''                # Column 2: CastName
                episodes_col = row[6] if len(row) > 6 else ''             # Column 6: EpisodeCount
                seasons_col = row[7] if len(row) > 7 else ''              # Column 7: Seasons
                
                # Check if it's a RuPaul show
                is_rupaul = any(indicator in show_name for indicator in rupaul_show_indicators)
                
                if is_rupaul:
                    # Only process if:
                    # 1. Has TMDB person ID and show ID
                    # 2. Episodes column is empty or says "SKIP"
                    # 3. Cast name exists
                    if (tmdb_person_id and tmdb_show_id and cast_name and
                        (not episodes_col or episodes_col.upper() == 'SKIP')):
                        
                        rupaul_rows.append({
                            'row_index': i,
                            'tmdb_person_id': tmdb_person_id,
                            'tmdb_show_id': tmdb_show_id,
                            'cast_name': cast_name,
                            'show_name': row[5] if len(row) > 5 else '',  # Column 5: ShowName
                            'current_episodes': episodes_col,
                            'current_seasons': seasons_col
                        })
            
            print(f"📊 TMDB RuPaul: Found {len(rupaul_rows)} RuPaul rows to process")
            
            if not rupaul_rows:
                print("⚠️ TMDB RuPaul: No RuPaul rows found that need processing")
                return
            
            # Process each RuPaul row
            successful_updates = 0
            
            for i, row_data in enumerate(rupaul_rows, 1):
                print(f"\n🎭 TMDB RuPaul: Processing {i}/{len(rupaul_rows)}: {row_data['cast_name']}")
                
                # Extract episode data using TMDB
                result = self.extract_rupaul_episodes(
                    row_data['tmdb_person_id'],
                    row_data['tmdb_show_id'], 
                    row_data['cast_name']
                )
                
                if result and result.get('found'):
                    try:
                        # Update the spreadsheet
                        row_index = row_data['row_index']
                        episode_count = result['episode_count']
                        seasons = result['seasons']
                        
                        # Update columns 6 (EpisodeCount) and 7 (Seasons)
                        self.worksheet.update_cell(row_index, 7, episode_count)  # Column 6 (EpisodeCount) - 1-indexed so 7
                        self.worksheet.update_cell(row_index, 8, seasons)        # Column 7 (Seasons) - 1-indexed so 8
                        
                        print(f"✅ TMDB RuPaul: Updated {row_data['cast_name']} - {episode_count} episodes, Season {seasons}")
                        successful_updates += 1
                        
                        # Rate limiting
                        time.sleep(1)
                        
                    except Exception as e:
                        print(f"⚠️ TMDB RuPaul: Failed to update spreadsheet for {row_data['cast_name']}: {str(e)}")
                else:
                    print(f"❌ TMDB RuPaul: No data found for {row_data['cast_name']}")
                
                # Rate limiting between API calls
                time.sleep(0.5)
            
            print(f"\n🎉 TMDB RuPaul: Processing complete!")
            print(f"✅ Successfully updated: {successful_updates}/{len(rupaul_rows)} cast members")
            
        except Exception as e:
            print(f"❌ TMDB RuPaul: Error in processing: {str(e)}")
            raise

def main():
    try:
        extractor = TMDBRuPaulExtractor()
        extractor.process_rupaul_rows()
        
    except KeyboardInterrupt:
        print("\n🛑 TMDB RuPaul: Process interrupted by user")
    except Exception as e:
        print(f"❌ TMDB RuPaul: Fatal error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

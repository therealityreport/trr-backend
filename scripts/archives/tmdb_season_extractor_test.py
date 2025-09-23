#!/usr/bin/env python3

import requests
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sys
import time
import re
import os
from urllib.parse import quote
from dotenv import load_dotenv

class TMDBSeasonExtractor:
    def __init__(self):
        # Load environment variables
        load_dotenv('/Users/thomashulihan/Projects/TRR-Backend/.env')
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        self.setup_google_sheets()
        
        if not self.tmdb_api_key:
            print("❌ TMDB Test: API key not found in .env file")
            raise ValueError("TMDB API key is required")
        else:
            print(f"✅ TMDB Test: API key loaded: {self.tmdb_api_key[:8]}...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 TMDB Test: Setting up Google Sheets connection...")
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-e16bfa49d861.json', 
                scope
            )
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open("The Reality TV Database").sheet1
            print("✅ TMDB Test: Google Sheets connection successful")
        except Exception as e:
            print(f"❌ TMDB Test: Google Sheets setup failed: {str(e)}")
            raise

    def get_tmdb_api_key(self):
        """Get TMDB API key from environment"""
        return self.tmdb_api_key

    def search_person_by_name(self, person_name):
        """Search for a person on TMDB by name"""
        try:
            url = f"{self.tmdb_base_url}/search/person"
            params = {
                "api_key": self.tmdb_api_key,
                "query": person_name,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('results'):
                    # Return the first match
                    person = data['results'][0]
                    print(f"✅ TMDB Test: Found person {person_name} with ID {person['id']}")
                    return person
                else:
                    print(f"⚠️ TMDB Test: No person found for {person_name}")
                    return None
            else:
                print(f"❌ TMDB Test: TMDB API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ TMDB Test: Error searching person: {str(e)}")
            return None

    def search_tv_show_by_name(self, show_name):
        """Search for a TV show on TMDB by name"""
        try:
            url = f"{self.tmdb_base_url}/search/tv"
            params = {
                "api_key": self.tmdb_api_key,
                "query": show_name,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('results'):
                    # Return the first match
                    show = data['results'][0]
                    print(f"✅ TMDB Test: Found show {show_name} with ID {show['id']}")
                    return show
                else:
                    print(f"⚠️ TMDB Test: No show found for {show_name}")
                    return None
            else:
                print(f"❌ TMDB Test: TMDB API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ TMDB Test: Error searching show: {str(e)}")
            return None

    def get_person_tv_credits(self, person_id):
        """Get all TV credits for a person"""
        try:
            url = f"{self.tmdb_base_url}/person/{person_id}/tv_credits"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ TMDB Test: Found TV credits for person {person_id}")
                return data
            else:
                print(f"❌ TMDB Test: TMDB API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ TMDB Test: Error getting TV credits: {str(e)}")
            return None

    def get_tv_show_details(self, show_id):
        """Get detailed information about a TV show"""
        try:
            url = f"{self.tmdb_base_url}/tv/{show_id}"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ TMDB Test: Found show details for ID {show_id}")
                return data
            else:
                print(f"❌ TMDB Test: TMDB API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ TMDB Test: Error getting show details: {str(e)}")
            return None

    def get_person_episodes_in_show(self, person_id, show_id):
        """Get specific episodes a person appeared in for a show"""
        try:
            # Get person's TV credits
            credits = self.get_person_tv_credits(person_id)
            if not credits:
                return None
            
            # Look for the specific show in their credits
            show_credits = None
            for credit in credits.get('cast', []) + credits.get('crew', []):
                if credit.get('id') == show_id:
                    show_credits = credit
                    break
            
            if not show_credits:
                print(f"⚠️ TMDB Test: Person {person_id} not found in show {show_id}")
                return None
            
            # Extract episode information
            episode_count = show_credits.get('episode_count', 0)
            
            # Get show details for season information
            show_details = self.get_tv_show_details(show_id)
            if show_details:
                total_seasons = show_details.get('number_of_seasons', 1)
                
                # For reality TV, we might need to make assumptions about seasons
                # This is where TMDB might have limitations vs IMDb
                result = {
                    'episode_count': episode_count,
                    'total_seasons': total_seasons,
                    'estimated_seasons': self.estimate_seasons_from_episodes(episode_count, total_seasons),
                    'show_name': show_details.get('name'),
                    'first_air_date': show_details.get('first_air_date'),
                    'last_air_date': show_details.get('last_air_date')
                }
                
                print(f"✅ TMDB Test: Found {episode_count} episodes for person in {result['show_name']}")
                return result
            
            return None
            
        except Exception as e:
            print(f"❌ TMDB Test: Error getting person episodes: {str(e)}")
            return None

    def estimate_seasons_from_episodes(self, episode_count, total_seasons):
        """Estimate which seasons a person appeared in based on episode count"""
        if episode_count == 0:
            return None
        
        if episode_count >= total_seasons * 8:  # Likely appeared in most/all seasons
            if total_seasons == 1:
                return "1"
            else:
                return f"1-{total_seasons}"
        elif episode_count <= 15:  # Likely appeared in 1-2 seasons
            return "1"  # Conservative estimate
        else:
            # Estimate based on typical reality TV season lengths (10-15 episodes)
            estimated_seasons = max(1, min(total_seasons, episode_count // 12))
            if estimated_seasons == 1:
                return "1"
            else:
                return f"1-{estimated_seasons}"

    def test_taylor_ware_example(self):
        """Test the specific Taylor Ware / Laguna Beach example"""
        print("🎯 TMDB Test: Testing Taylor Ware in Laguna Beach example...")
        
        # Method 1: Direct API calls using known IDs
        print("\n--- Method 1: Using provided IDs ---")
        person_id = 1234353  # Taylor Ware's TMDB ID
        show_id = 3605       # Laguna Beach TMDB ID
        
        result = self.get_person_episodes_in_show(person_id, show_id)
        if result:
            print(f"📊 TMDB Result for Taylor Ware:")
            print(f"   Episodes: {result['episode_count']}")
            print(f"   Estimated Seasons: {result['estimated_seasons']}")
            print(f"   Show: {result['show_name']}")
            print(f"   Air Dates: {result['first_air_date']} to {result['last_air_date']}")
        
        # Method 2: Search by name
        print("\n--- Method 2: Searching by name ---")
        person = self.search_person_by_name("Taylor Ware")
        show = self.search_tv_show_by_name("Laguna Beach")
        
        if person and show:
            result2 = self.get_person_episodes_in_show(person['id'], show['id'])
            if result2:
                print(f"📊 TMDB Search Result:")
                print(f"   Person ID: {person['id']} (vs provided {person_id})")
                print(f"   Show ID: {show['id']} (vs provided {show_id})")
                print(f"   Episodes: {result2['episode_count']}")
                print(f"   Estimated Seasons: {result2['estimated_seasons']}")

    def test_with_spreadsheet_data(self, start_row=2, max_rows=10):
        """Test TMDB approach with actual spreadsheet data"""
        print(f"🔄 TMDB Test: Testing with spreadsheet data from row {start_row}...")
        
        try:
            # Get data from spreadsheet
            all_values = self.sheet.get_all_values()
            header_row = all_values[0]
            
            # Find column indices
            cast_name_col = None
            show_name_col = None
            episode_count_col = None
            seasons_col = None
            
            for i, header in enumerate(header_row):
                if 'Cast Name' in header:
                    cast_name_col = i
                elif 'Show Name' in header:
                    show_name_col = i
                elif 'Episode Count' in header:
                    episode_count_col = i
                elif 'Seasons' in header:
                    seasons_col = i
            
            if cast_name_col is None or show_name_col is None:
                print("❌ TMDB Test: Could not find required columns")
                return
            
            print(f"📋 TMDB Test: Found columns - Cast: {cast_name_col}, Show: {show_name_col}")
            
            # Test with a few rows
            successes = 0
            total_tested = 0
            
            for row_idx in range(start_row - 1, min(start_row - 1 + max_rows, len(all_values))):
                row = all_values[row_idx]
                cast_name = row[cast_name_col] if cast_name_col < len(row) else ""
                show_name = row[show_name_col] if show_name_col < len(row) else ""
                current_episodes = row[episode_count_col] if episode_count_col and episode_count_col < len(row) else ""
                current_seasons = row[seasons_col] if seasons_col and seasons_col < len(row) else ""
                
                if not cast_name or not show_name:
                    continue
                
                total_tested += 1
                print(f"\n🎭 TMDB Test: Row {row_idx + 1} - {cast_name} in {show_name}")
                print(f"   Current data: {current_episodes} episodes, seasons {current_seasons}")
                
                # Search for person and show
                person = self.search_person_by_name(cast_name)
                show = self.search_tv_show_by_name(show_name)
                
                if person and show:
                    result = self.get_person_episodes_in_show(person['id'], show['id'])
                    if result and result['episode_count'] > 0:
                        successes += 1
                        print(f"   ✅ TMDB found: {result['episode_count']} episodes, estimated seasons {result['estimated_seasons']}")
                        
                        # Compare with current data
                        if current_episodes and current_episodes.isdigit():
                            current_ep_count = int(current_episodes)
                            if current_ep_count != result['episode_count']:
                                print(f"   🔍 Difference detected: Current {current_ep_count} vs TMDB {result['episode_count']}")
                    else:
                        print(f"   ⚠️ TMDB: No episode data found")
                else:
                    print(f"   ❌ TMDB: Person or show not found")
                
                time.sleep(0.5)  # Be nice to TMDB API
            
            success_rate = (successes / total_tested * 100) if total_tested > 0 else 0
            print(f"\n📈 TMDB Test Results: {successes}/{total_tested} successful ({success_rate:.1f}%)")
            
        except Exception as e:
            print(f"❌ TMDB Test: Error testing spreadsheet data: {str(e)}")

def main():
    """Main function to test TMDB approach"""
    print("🎬 TMDB Season Extractor Test Starting...")
    
    extractor = TMDBSeasonExtractor()
    
    # Test 1: Taylor Ware example
    extractor.test_taylor_ware_example()
    
    # Test 2: Sample of spreadsheet data
    print("\n" + "="*50)
    extractor.test_with_spreadsheet_data(start_row=2, max_rows=5)
    
    print("\n🎉 TMDB Test completed!")
    print("\n💡 Next Steps:")
    print("1. Get a real TMDB API key from https://www.themoviedb.org/settings/api")
    print("2. Replace 'YOUR_TMDB_API_KEY_HERE' with your actual key")
    print("3. Run this script to see TMDB data quality vs IMDb")
    print("4. If TMDB data is good, we can create a comprehensive TMDB extractor")

if __name__ == "__main__":
    main()

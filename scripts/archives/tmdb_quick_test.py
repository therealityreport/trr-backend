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

class TMDBQuickTest:
    def __init__(self):
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.tmdb_bearer}',
            'Content-Type': 'application/json'
        })
        
    def get_credit_details(self, credit_id, cast_name, show_name):
        """Get detailed season information for a specific credit"""
        try:
            print(f"🔍 TMDB {show_name}: Getting credit details for ID: {credit_id}")
            credit_url = f"{self.base_url}/credit/{credit_id}"
            credit_response = self.session.get(credit_url)
            
            if credit_response.status_code != 200:
                print(f"❌ TMDB {show_name}: Failed to get credit details for {cast_name}: {credit_response.status_code}")
                return None
            
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

    def test_specific_credits(self):
        """Test with the specific credit IDs you provided"""
        test_cases = [
            {
                'name': 'Kathy Hilton',
                'show_name': 'Paris in Love',
                'credit_id': '65faa9c604733f0164e60334'
            },
            {
                'name': 'Kim Kardashian',
                'show_name': 'The Kardashians', 
                'credit_id': '6261446f6eecee13a4131657'
            },
            {
                'name': 'Winter House Cast',
                'show_name': 'Winter House',
                'credit_id': '638a1bc370b444008c52a447'
            }
        ]
        
        for test_case in test_cases:
            print(f"\n🧪 Testing: {test_case['name']} in {test_case['show_name']}")
            result = self.get_credit_details(
                test_case['credit_id'], 
                test_case['name'], 
                test_case['show_name']
            )
            
            if result:
                episodes = result.get('episode_count', 'Unknown')
                seasons = result.get('seasons', [])
                seasons_str = ", ".join(map(str, seasons)) if seasons else "Unknown"
                print(f"🎉 RESULT: {episodes} episodes, Seasons: {seasons_str}")
            else:
                print(f"❌ FAILED: No data found")

if __name__ == "__main__":
    tester = TMDBQuickTest()
    tester.test_specific_credits()

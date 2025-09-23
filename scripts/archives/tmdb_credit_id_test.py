#!/usr/bin/env python3

import requests
import json
import time
import os
from dotenv import load_dotenv
import re

class TMDBCreditIDTest:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        
        if not self.tmdb_api_key:
            print("❌ TMDB API key not found in .env file")
            return
        
        print(f"✅ TMDB API key loaded: {self.tmdb_api_key[:8]}...")

    def get_person_tv_credits_with_credit_ids(self, person_id):
        """Get TV credits with credit_id information"""
        try:
            url = f"{self.tmdb_base_url}/person/{person_id}/tv_credits"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"❌ TMDB API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting TV credits: {str(e)}")
            return None

    def get_credit_details(self, credit_id):
        """Get specific credit details using credit_id"""
        try:
            url = f"{self.tmdb_base_url}/credit/{credit_id}"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"❌ Credit API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting credit details: {str(e)}")
            return None

    def test_carson_extraction_with_fix(self):
        """Test the fixed extraction logic for Carson Kressley"""
        print("\n🔍 Testing Fixed Extraction Logic for Carson Kressley")
        print("="*55)
        
        # Simulate the extraction function logic
        credit_id = "64ed74375258ae00c94dd80f"
        
        print(f"🔍 Getting credit details for: {credit_id}")
        credit_data = self.get_credit_details(credit_id)
        
        if not credit_data:
            print("❌ Failed to get credit details")
            return
        
        # Extract season information using the new logic
        seasons = []
        if 'media' in credit_data:
            media = credit_data['media']
            
            # First try: Check the seasons array
            if 'seasons' in media and media['seasons']:
                seasons_data = media['seasons']
                print(f"🔍 Credit details returned {len(seasons_data)} seasons from seasons array")
                
                for season in seasons_data:
                    season_number = season.get('season_number')
                    if season_number is not None and season_number > 0:
                        seasons.append(season_number)
                        print(f"✅ Found Season {season_number}")
            
            # Second try: If seasons array is empty, extract from episodes
            elif 'episodes' in media and media['episodes']:
                episodes_data = media['episodes']
                print(f"🔍 Credit details returned {len(episodes_data)} episodes, extracting seasons from episodes")
                
                season_numbers = set()
                for episode in episodes_data:
                    season_number = episode.get('season_number')
                    if season_number is not None and season_number > 0:
                        season_numbers.add(season_number)
                
                seasons = sorted(list(season_numbers))
                for season_num in seasons:
                    print(f"✅ Found Season {season_num}")
            else:
                print(f"🔍 Credit details returned 0 seasons")
        
        # Get episode count
        episode_count = len(media.get('episodes', []))
        
        print(f"\n🎯 Final Result:")
        print(f"   Episodes: {episode_count}")
        print(f"   Seasons: {seasons}")
        print(f"   Formatted: {episode_count} episodes, Season(s) {', '.join(map(str, seasons))}")

    def test_carson_kressley_credit(self):
        """Test Carson Kressley's specific credit ID that should have season data"""
        print("🔍 Testing Carson Kressley's Credit ID")
        print("="*50)
        
        # From your URL: https://www.themoviedb.org/tv/1508-dancing-with-the-stars/episodes?credit_id=64ed74375258ae00c94dd80f&language=en-US&person_id=52534d9119c2957940102ceb
        credit_id = "64ed74375258ae00c94dd80f"
        person_id = 1215451  # Carson Kressley's TMDB ID
        show_id = 1508       # Dancing with the Stars TMDB ID
        
        print(f"👤 Carson Kressley ID: {person_id}")
        print(f"📺 Dancing with the Stars ID: {show_id}")
        print(f"🎯 Credit ID: {credit_id}")
        
        # Test the credit details directly
        print(f"\n🔍 Getting credit details for: {credit_id}")
        credit_details = self.get_credit_details(credit_id)
        
        if credit_details:
            print(f"\n📊 Full Credit Response:")
            print(json.dumps(credit_details, indent=2))
        else:
            print("❌ No credit details returned")
        
        # Also test getting his TV credits to see the credit info
        print(f"\n🔍 Getting Carson's TV credits for Dancing with the Stars")
        credits = self.get_person_tv_credits_with_credit_ids(person_id)
        if credits:
            # Find Dancing with the Stars credit
            dwts_credit = None
            for credit in credits.get('cast', []) + credits.get('crew', []):
                if credit.get('id') == show_id:
                    dwts_credit = credit
                    print(f"\n📋 Dancing with the Stars Credit Found:")
                    print(f"   Credit ID: {credit.get('credit_id', 'N/A')}")
                    print(f"   Character: {credit.get('character', 'N/A')}")
                    print(f"   Episode Count: {credit.get('episode_count', 0)}")
                    break
            
            if not dwts_credit:
                print("❌ Dancing with the Stars not found in credits")

    def analyze_taylor_ware_with_credit_id(self):
        """Analyze Taylor Ware using the credit_id approach"""
        print("🔍 Analyzing Taylor Ware with Credit ID Approach")
        print("="*60)
        
        # From your URL: person_id=52575365760ee36aaa1ed1d8, credit_id=56aa7a099251417e14000046
        # But let's first get her TMDB person ID
        person_id = 1234353  # Taylor Ware's TMDB ID
        show_id = 3605       # Laguna Beach TMDB ID
        
        print(f"👤 Taylor Ware ID: {person_id}")
        print(f"📺 Laguna Beach ID: {show_id}")
        
        # Get her TV credits
        credits = self.get_person_tv_credits_with_credit_ids(person_id)
        if not credits:
            return
        
        # Find Laguna Beach credit with credit_id
        laguna_credit = None
        for credit in credits.get('cast', []) + credits.get('crew', []):
            if credit.get('id') == show_id:
                laguna_credit = credit
                break
        
        if not laguna_credit:
            print("❌ Laguna Beach not found in credits")
            return
        
        print(f"\n📋 Laguna Beach Credit Found:")
        print(f"   Credit ID: {laguna_credit.get('credit_id', 'N/A')}")
        print(f"   Character: {laguna_credit.get('character', 'N/A')}")
        print(f"   Episode Count: {laguna_credit.get('episode_count', 0)}")
        
        # Get detailed credit information
        credit_id = laguna_credit.get('credit_id')
        if credit_id:
            print(f"\n🔍 Getting detailed credit info for: {credit_id}")
            credit_details = self.get_credit_details(credit_id)
            
            if credit_details:
                print(f"\n📊 Detailed Credit Information:")
                print(f"   Credit Type: {credit_details.get('credit_type', 'N/A')}")
                print(f"   Department: {credit_details.get('department', 'N/A')}")
                print(f"   Job: {credit_details.get('job', 'N/A')}")
                
                # Check if there's media information
                media = credit_details.get('media', {})
                if media:
                    print(f"\n📺 Media Details:")
                    print(f"   Show: {media.get('name', 'N/A')}")
                    print(f"   Show ID: {media.get('id', 'N/A')}")
                    
                    # Look for season/episode information
                    seasons = media.get('seasons', [])
                    if seasons:
                        print(f"\n🎬 Season Information:")
                        for season in seasons:
                            season_num = season.get('season_number', 'N/A')
                            episode_count = season.get('episode_count', 0)
                            print(f"   Season {season_num}: {episode_count} episodes")
                    
                    # Look for episode information
                    episodes = media.get('episodes', [])
                    if episodes:
                        print(f"\n📺 Specific Episodes:")
                        for episode in episodes:
                            season_num = episode.get('season_number', 'N/A')
                            episode_num = episode.get('episode_number', 'N/A')
                            episode_name = episode.get('name', 'N/A')
                            air_date = episode.get('air_date', 'N/A')
                            print(f"   S{season_num}E{episode_num}: {episode_name} ({air_date})")
                
                # Print the full response for analysis
                print(f"\n🔍 Full Credit Response:")
                print(json.dumps(credit_details, indent=2)[:1000] + "...")
        
        # Try the credit_id from your URL example
        example_credit_id = "56aa7a099251417e14000046"
        print(f"\n🎯 Testing with your example credit ID: {example_credit_id}")
        example_credit = self.get_credit_details(example_credit_id)
        if example_credit:
            print(f"✅ Example credit found!")
            print(json.dumps(example_credit, indent=2)[:1500] + "...")

    def build_episode_url_approach(self, show_id, person_id, credit_id):
        """Build the URL approach like your example"""
        print(f"\n🔗 URL Approach Analysis")
        print("="*40)
        
        # Your example URL structure
        base_url = "https://www.themoviedb.org/tv"
        show_slug = f"{show_id}-laguna-beach-the-real-orange-county"  # Show ID + slug
        params = f"episodes?credit_id={credit_id}&language=en-US&person_id={person_id}"
        
        full_url = f"{base_url}/{show_slug}/{params}"
        print(f"📍 Example URL: {full_url}")
        
        print(f"\n💡 URL Components:")
        print(f"   Base: {base_url}")
        print(f"   Show: {show_id} (with slug)")
        print(f"   Credit ID: {credit_id}")
        print(f"   Person ID: {person_id}")
        
        print(f"\n🎯 This URL shows episode details on the web interface")
        print(f"   We need to find the API equivalent to get this data")

def main():
    """Test the credit_id approach"""
    print("🎬 TMDB Credit ID Episode Analysis")
    
    tester = TMDBCreditIDTest()
    
    if not tester.tmdb_api_key:
        print("❌ Cannot proceed without TMDB API key")
        return
    
    # Test Carson Kressley's specific case first
    tester.test_carson_kressley_credit()
    
    # Test the fixed extraction logic
    tester.test_carson_extraction_with_fix()
    
    # Analyze Taylor Ware with credit_id approach
    tester.analyze_taylor_ware_with_credit_id()
    
    # Analyze the URL approach
    tester.build_episode_url_approach(
        show_id=3605,
        person_id="52575365760ee36aaa1ed1d8",  # From your URL
        credit_id="56aa7a099251417e14000046"   # From your URL
    )

if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import requests
import json
import time
import os
from dotenv import load_dotenv

class TMDBDetailedTest:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        
        if not self.tmdb_api_key:
            print("❌ TMDB API key not found in .env file")
            return
        
        print(f"✅ TMDB API key loaded: {self.tmdb_api_key[:8]}...")

    def get_detailed_tv_credits(self, person_id):
        """Get detailed TV credits with episode information"""
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
            print(f"❌ Error getting detailed TV credits: {str(e)}")
            return None

    def get_season_details(self, show_id, season_number):
        """Get detailed information about a specific season"""
        try:
            url = f"{self.tmdb_base_url}/tv/{show_id}/season/{season_number}"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"❌ Season {season_number} not found or API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting season details: {str(e)}")
            return None

    def get_episode_credits(self, show_id, season_number, episode_number):
        """Get credits for a specific episode"""
        try:
            url = f"{self.tmdb_base_url}/tv/{show_id}/season/{season_number}/episode/{episode_number}/credits"
            params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"❌ Episode S{season_number}E{episode_number} credits not found: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting episode credits: {str(e)}")
            return None

    def analyze_taylor_ware_episodes(self):
        """Deep dive into Taylor Ware's Laguna Beach episodes"""
        print("🔍 Deep Analysis: Taylor Ware in Laguna Beach")
        print("="*60)
        
        # Taylor Ware ID: 1234353, Laguna Beach ID: 3605
        person_id = 1234353
        show_id = 3605
        
        # Get detailed credits
        credits = self.get_detailed_tv_credits(person_id)
        if not credits:
            return
        
        # Find Laguna Beach in credits
        laguna_credit = None
        for credit in credits.get('cast', []) + credits.get('crew', []):
            if credit.get('id') == show_id:
                laguna_credit = credit
                break
        
        if not laguna_credit:
            print("❌ Laguna Beach not found in Taylor Ware's credits")
            return
        
        print(f"📺 Show Credit Details:")
        print(f"   Character: {laguna_credit.get('character', 'N/A')}")
        print(f"   Job: {laguna_credit.get('job', 'N/A')}")
        print(f"   Episode Count: {laguna_credit.get('episode_count', 0)}")
        print(f"   First Air Date: {laguna_credit.get('first_air_date', 'N/A')}")
        print(f"   Last Air Date: {laguna_credit.get('last_air_date', 'N/A')}")
        
        # Try to get season-by-season breakdown
        print(f"\n🔍 Searching for specific episodes...")
        
        # Laguna Beach had 3 seasons, let's check each
        for season_num in range(1, 4):
            print(f"\n📅 Season {season_num}:")
            season_details = self.get_season_details(show_id, season_num)
            
            if season_details:
                episodes = season_details.get('episodes', [])
                print(f"   Total episodes in season: {len(episodes)}")
                
                # Check each episode for Taylor Ware
                taylor_episodes = []
                for episode in episodes[:5]:  # Check first 5 episodes to avoid rate limits
                    episode_num = episode.get('episode_number')
                    episode_name = episode.get('name', 'Unknown')
                    
                    print(f"   Checking S{season_num}E{episode_num}: {episode_name}")
                    
                    # Get episode credits
                    ep_credits = self.get_episode_credits(show_id, season_num, episode_num)
                    if ep_credits:
                        # Look for Taylor Ware in episode cast
                        found_in_episode = False
                        for cast_member in ep_credits.get('cast', []):
                            if cast_member.get('id') == person_id:
                                taylor_episodes.append({
                                    'season': season_num,
                                    'episode': episode_num,
                                    'name': episode_name,
                                    'character': cast_member.get('character', 'Unknown'),
                                    'air_date': episode.get('air_date')
                                })
                                found_in_episode = True
                                print(f"   ✅ FOUND Taylor Ware in S{season_num}E{episode_num}")
                                break
                        
                        if not found_in_episode:
                            print(f"   ⚪ Not in S{season_num}E{episode_num}")
                    
                    time.sleep(0.1)  # Be nice to API
                
                if taylor_episodes:
                    print(f"\n🎉 Taylor Ware Episodes Found:")
                    for ep in taylor_episodes:
                        print(f"   S{ep['season']}E{ep['episode']}: {ep['name']}")
                        print(f"      Character: {ep['character']}")
                        print(f"      Air Date: {ep['air_date']}")
            else:
                print(f"   ❌ Could not get season {season_num} details")
            
            time.sleep(0.5)  # Rate limiting

    def compare_with_imdb_approach(self):
        """Compare what we can get from TMDB vs IMDb"""
        print("\n" + "="*60)
        print("📊 TMDB vs IMDb Detailed Comparison")
        print("="*60)
        
        print("✅ TMDB API Capabilities:")
        print("• Person's total episode count per show")
        print("• Show season/episode structure") 
        print("• Episode-by-episode cast lists")
        print("• Character names per episode")
        print("• Air dates for episodes")
        print("• Crew roles and departments")
        print("• Fast API responses (1-2 seconds)")
        
        print("\n🔍 IMDb Scraping Capabilities:")
        print("• Individual cast member episode buttons")
        print("• Season tabs after clicking episode button")
        print("• Episode markers (S3.E1, S3.E2, etc.)")
        print("• Total episode counts")
        print("• Slow scraping (25-30 seconds)")
        print("• Browser automation required")
        print("• Timeout and loading issues")
        
        print("\n💡 Best Approach:")
        print("• Use TMDB for bulk data extraction (much faster)")
        print("• TMDB provides episode counts + basic episode info")
        print("• For missing data, fall back to IMDb scraping")
        print("• Hybrid approach: TMDB first, IMDb for gaps")

def main():
    """Test detailed TMDB episode information"""
    print("🎬 TMDB Detailed Episode Analysis")
    
    tester = TMDBDetailedTest()
    
    if not tester.tmdb_api_key:
        print("❌ Cannot proceed without TMDB API key")
        return
    
    # Deep dive into Taylor Ware episodes
    tester.analyze_taylor_ware_episodes()
    
    # Comparison analysis
    tester.compare_with_imdb_approach()

if __name__ == "__main__":
    main()

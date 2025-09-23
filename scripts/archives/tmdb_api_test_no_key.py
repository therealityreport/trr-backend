#!/usr/bin/env python3

import requests
import json
import time

class TMDBAPITester:
    def __init__(self):
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        
    def test_api_endpoints_without_key(self):
        """Test TMDB API endpoints to see what data structure looks like"""
        print("🔍 Testing TMDB API endpoints (without API key)...")
        
        # Test endpoints that might work without key or give us structure info
        test_endpoints = [
            f"{self.tmdb_base_url}/search/person?query=Taylor%20Ware",
            f"{self.tmdb_base_url}/search/tv?query=Laguna%20Beach",
            f"{self.tmdb_base_url}/person/1234353",
            f"{self.tmdb_base_url}/tv/3605",
            f"{self.tmdb_base_url}/person/1234353/tv_credits"
        ]
        
        for endpoint in test_endpoints:
            try:
                print(f"\n🎯 Testing: {endpoint}")
                response = requests.get(endpoint)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 401:
                    print("   ✅ Expected 401 (API key required) - endpoint exists")
                elif response.status_code == 200:
                    print("   ✅ Success! Data returned:")
                    data = response.json()
                    print(f"   📊 Response keys: {list(data.keys())}")
                else:
                    print(f"   ❌ Unexpected status: {response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
            
            time.sleep(0.5)

    def analyze_tmdb_vs_imdb_approach(self):
        """Analyze the benefits of TMDB vs IMDb approach"""
        print("\n" + "="*60)
        print("📊 TMDB vs IMDb Analysis")
        print("="*60)
        
        print("\n✅ TMDB Advantages:")
        print("• Official API with structured JSON responses")
        print("• Rate limits but no web scraping issues")
        print("• Episode counts per person per show")
        print("• Season information available")
        print("• No need for Selenium/browser automation")
        print("• Much faster and more reliable")
        print("• No timeout issues or page loading problems")
        
        print("\n⚠️ TMDB Potential Limitations:")
        print("• May have less complete cast data than IMDb")
        print("• Reality TV shows might be less well-documented")
        print("• API rate limits (but still faster than web scraping)")
        print("• Need to get API key")
        
        print("\n🎯 TMDB Implementation Strategy:")
        print("1. Get TMDB API key (free)")
        print("2. For each cast member + show combination:")
        print("   a. Search for person by name → get person_id")
        print("   b. Search for show by name → get show_id") 
        print("   c. Get person's TV credits → find episodes in specific show")
        print("   d. Extract episode count and season info")
        print("3. Update spreadsheet with reliable data")
        print("4. Much faster than current IMDb scraping (seconds vs minutes)")
        
        print("\n🚀 Expected Performance:")
        print("• Current IMDb: ~25-30 seconds per cast member")
        print("• TMDB API: ~1-2 seconds per cast member")
        print("• 10-15x speed improvement")
        print("• Near 100% reliability (no browser issues)")

    def show_api_key_instructions(self):
        """Show how to get TMDB API key"""
        print("\n" + "="*50)
        print("🔑 How to Get TMDB API Key")
        print("="*50)
        print("1. Go to: https://www.themoviedb.org/")
        print("2. Create a free account")
        print("3. Go to: https://www.themoviedb.org/settings/api")
        print("4. Request an API key (usually approved instantly)")
        print("5. Copy your API key")
        print("6. Replace 'YOUR_TMDB_API_KEY_HERE' in the test script")
        print("\nAPI Key format: 32-character string like:")
        print("abc123def456ghi789jkl012mno345pq")

def main():
    """Test TMDB approach"""
    print("🎬 TMDB API Test (No Key Required)")
    
    tester = TMDBAPITester()
    
    # Test API endpoints
    tester.test_api_endpoints_without_key()
    
    # Show analysis
    tester.analyze_tmdb_vs_imdb_approach()
    
    # Show API key instructions
    tester.show_api_key_instructions()

if __name__ == "__main__":
    main()

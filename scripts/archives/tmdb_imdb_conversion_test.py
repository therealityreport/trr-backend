#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(load_env_path)

def test_imdb_to_tmdb_conversion():
    """Test IMDb ID to TMDB ID conversion"""
    
    tmdb_bearer = os.getenv('TMDB_BEARER')
    base_url = "https://api.themoviedb.org/3"
    
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {tmdb_bearer}',
        'Content-Type': 'application/json'
    })
    
    # Test cases from your examples
    test_cases = [
        ("nm8647248", "Dorit Kemsley"),  # Should convert to 1763625
        ("nm0724187", "Kathy Hilton"),   # Should convert to TMDB ID
        ("nm2578007", "Kim Kardashian"), # Should convert to TMDB ID
    ]
    
    for imdb_id, name in test_cases:
        print(f"\n🔄 Testing conversion for {name} (IMDb: {imdb_id})")
        
        try:
            find_url = f"{base_url}/find/{imdb_id}?external_source=imdb_id"
            find_response = session.get(find_url)
            
            if find_response.status_code == 200:
                find_data = find_response.json()
                
                if find_data.get('person_results'):
                    tmdb_id = find_data['person_results'][0]['id']
                    tmdb_name = find_data['person_results'][0]['name']
                    print(f"✅ {name}: {imdb_id} → TMDB ID: {tmdb_id} (Name: {tmdb_name})")
                    
                    # Test getting TV credits with the TMDB ID
                    credits_url = f"{base_url}/person/{tmdb_id}/tv_credits"
                    credits_response = session.get(credits_url)
                    
                    if credits_response.status_code == 200:
                        credits_data = credits_response.json()
                        print(f"✅ TV Credits found: {len(credits_data.get('cast', []))} cast, {len(credits_data.get('crew', []))} crew")
                        
                        # Show some show IDs they're in
                        show_ids = []
                        for credit in (credits_data.get('cast', []) + credits_data.get('crew', []))[:3]:
                            show_id = credit.get('id')
                            show_name = credit.get('name', 'Unknown')
                            if show_id:
                                show_ids.append(f"{show_id}:{show_name}")
                        print(f"📺 Sample shows: {show_ids}")
                    else:
                        print(f"❌ Failed to get TV credits: {credits_response.status_code}")
                else:
                    print(f"❌ No TMDB person found for {imdb_id}")
            else:
                print(f"❌ Find request failed: {find_response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_imdb_to_tmdb_conversion()

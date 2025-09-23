#!/usr/bin/env python3
"""
Test script to show the new column structure for CastInfo
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fetch_cast_info_imdb_clean import CastInfoBuilder

def main():
    print("🔍 Testing new CastInfo structure...")
    
    builder = CastInfoBuilder()
    
    # Test with one show
    show_info = {
        'ShowName': 'The Real Housewives of Atlanta',
        'ShowID': '10160',
        'Show IMDbID': 'tt1051497'
    }
    
    cast_rows = builder.build_cast_for_show(show_info)
    
    if cast_rows:
        print(f"\n📊 Sample data structure (first 3 cast members):")
        print(f"Headers: CastName | CastID | Cast IMDbID | ShowName | Show IMDbID | ShowID | TotalEpisodes | TotalSeasons")
        print("-" * 120)
        
        for i, row in enumerate(cast_rows[:3]):
            print(f"Row {i+1}: {' | '.join(str(col) for col in row)}")
    
    print(f"\n✅ Total cast members found: {len(cast_rows)}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fill Gender, Birthday, and Zodiac in RealiteaseInfo sheet from TMDb API.

This script enhances the RealiteaseInfo sheet with biographical data:
- Column H (Gender): M/F/Other from TMDb
- Column I (Birthday): YYYY-MM-DD format from TMDb  
- Column J (Zodiac): Calculated from birthday

RealiteaseInfo columns: CastName, CastIMDbID, CastTMDbID, ShowNames, ShowIMDbIDs, ShowTMDbIDs, ShowCount, Gender, Birthday, Zodiac

Features:
- Uses TMDb Person API to get gender and birthday
- Calculates zodiac sign from birthday
- Only updates empty cells (preserves existing data)
- Rate-limited API calls to respect TMDb limits
"""

import os
import time
import argparse
from datetime import datetime
from typing import Dict, Any, Optional
import requests
import gspread
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SA_KEYFILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "../../keys/trr-backend-e16bfa49d861.json"
)
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BEARER = os.getenv("TMDB_BEARER")

def calculate_zodiac(birthday):
    """Calculate zodiac sign from birthday (YYYY-MM-DD format)"""
    if not birthday or len(birthday) < 10:
        return ""
    
    try:
        # Parse birthday (expecting YYYY-MM-DD format)
        month, day = birthday.split('-')[1:3]
        month, day = int(month), int(day)
        
        if (month == 3 and day >= 21) or (month == 4 and day <= 19):
            return "Aries"
        elif (month == 4 and day >= 20) or (month == 5 and day <= 20):
            return "Taurus"
        elif (month == 5 and day >= 21) or (month == 6 and day <= 20):
            return "Gemini"
        elif (month == 6 and day >= 21) or (month == 7 and day <= 22):
            return "Cancer"
        elif (month == 7 and day >= 23) or (month == 8 and day <= 22):
            return "Leo"
        elif (month == 8 and day >= 23) or (month == 9 and day <= 22):
            return "Virgo"
        elif (month == 9 and day >= 23) or (month == 10 and day <= 22):
            return "Libra"
        elif (month == 10 and day >= 23) or (month == 11 and day <= 21):
            return "Scorpio"
        elif (month == 11 and day >= 22) or (month == 12 and day <= 21):
            return "Sagittarius"
        elif (month == 12 and day >= 22) or (month == 1 and day <= 19):
            return "Capricorn"
        elif (month == 1 and day >= 20) or (month == 2 and day <= 18):
            return "Aquarius"
        elif (month == 2 and day >= 19) or (month == 3 and day <= 20):
            return "Pisces"
        else:
            return ""
    except (ValueError, IndexError):
        return ""

def map_gender(tmdb_gender_code):
    """Map TMDb gender code to readable format"""
    gender_map = {
        0: "Unknown",
        1: "F",      # Female
        2: "M",      # Male
        3: "Other"   # Non-binary/Other
    }
    return gender_map.get(tmdb_gender_code, "Unknown")

def connect_to_sheet():
    """Connect to Google Sheets and return the RealiteaseInfo worksheet."""
    try:
        gc = gspread.service_account(filename=SA_KEYFILE)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        realitease_sheet = spreadsheet.worksheet("RealiteaseInfo")
        return realitease_sheet
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None

def setup_tmdb_session():
    """Setup TMDb API session with authentication"""
    session = requests.Session()
    
    if TMDB_BEARER:
        session.headers.update({"Authorization": f"Bearer {TMDB_BEARER}"})
        print("✅ Using TMDb Bearer token authentication")
    elif TMDB_API_KEY:
        print("✅ Using TMDb API key authentication")
    else:
        print("❌ No TMDb authentication found - set TMDB_BEARER or TMDB_API_KEY in .env")
        return None
    
    return session

def fetch_tmdb_person_details(session, tmdb_person_id, retries=3):
    """Fetch person details from TMDb API"""
    if not tmdb_person_id or not str(tmdb_person_id).strip():
        return None
    
    # Clean up TMDb ID (remove any prefixes)
    person_id = str(tmdb_person_id).replace('person/', '').strip()
    if not person_id.isdigit():
        return None
    
    url = f"https://api.themoviedb.org/3/person/{person_id}"
    params = {}
    
    # Add API key if not using Bearer token
    if not TMDB_BEARER and TMDB_API_KEY:
        params["api_key"] = TMDB_API_KEY
    
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'gender': data.get('gender', 0),
                    'birthday': data.get('birthday', ''),
                    'name': data.get('name', ''),
                }
            elif response.status_code == 404:
                print(f"   ❌ TMDb person {person_id} not found")
                return None
            elif response.status_code == 429:  # Rate limited
                wait_time = 2 ** attempt
                print(f"   ⏳ Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"   ❌ TMDb API error {response.status_code} for person {person_id}")
                return None
                
        except Exception as e:
            wait_time = 2 ** attempt  
            print(f"   ❌ Network error for person {person_id}: {e}")
            if attempt < retries - 1:
                time.sleep(wait_time)
            continue
    
    return None

def process_realitease_info(sheet, session, start_row=2, limit=0, rate_delay=0.5):
    """Process RealiteaseInfo sheet and fill missing biographical data"""
    print("📊 Loading RealiteaseInfo sheet data...")
    
    try:
        # Get all data from the sheet
        all_values = sheet.get_all_values()
        
        if len(all_values) < 2:
            print("❌ RealiteaseInfo sheet has no data rows")
            return
        
        headers = all_values[0]
        print(f"📋 Headers: {headers}")
        
        # Expected column indices for RealiteaseInfo
        # CastName, CastIMDbID, CastTMDbID, ShowNames, ShowIMDbIDs, ShowTMDbIDs, ShowCount, Gender, Birthday, Zodiac
        col_indices = {
            'cast_name': 0,      # A: CastName
            'cast_imdb_id': 1,   # B: CastIMDbID  
            'cast_tmdb_id': 2,   # C: CastTMDbID
            'show_names': 3,     # D: ShowNames
            'show_imdb_ids': 4,  # E: ShowIMDbIDs
            'show_tmdb_ids': 5,  # F: ShowTMDbIDs
            'show_count': 6,     # G: ShowCount
            'gender': 7,         # H: Gender
            'birthday': 8,       # I: Birthday  
            'zodiac': 9          # J: Zodiac
        }
        
        # Determine which rows to process
        data_rows = all_values[start_row-1:]  # Convert to 0-based indexing
        if limit > 0:
            data_rows = data_rows[:limit]
        
        print(f"🔄 Processing {len(data_rows)} rows starting from row {start_row}")
        
        updates_made = 0
        processed_count = 0
        
        for i, row in enumerate(data_rows):
            row_number = start_row + i
            processed_count += 1
            
            if len(row) < 10:  # Ensure we have all columns
                continue
            
            cast_name = row[col_indices['cast_name']].strip()
            cast_tmdb_id = row[col_indices['cast_tmdb_id']].strip()
            current_gender = row[col_indices['gender']].strip()
            current_birthday = row[col_indices['birthday']].strip()
            current_zodiac = row[col_indices['zodiac']].strip()
            
            if not cast_name:
                continue
            
            # Check if we need to fetch data
            needs_gender = not current_gender
            needs_birthday = not current_birthday
            needs_zodiac = not current_zodiac
            
            if not any([needs_gender, needs_birthday, needs_zodiac]):
                if processed_count % 50 == 0:
                    print(f"   ✅ Row {row_number}: {cast_name} - already has all data")
                continue
            
            if not cast_tmdb_id:
                print(f"   ⚠️ Row {row_number}: {cast_name} - no TMDb ID")
                continue
            
            print(f"🔍 Row {row_number}: {cast_name} (TMDb: {cast_tmdb_id})")
            
            # Fetch TMDb data
            person_data = fetch_tmdb_person_details(session, cast_tmdb_id)
            
            if not person_data:
                print(f"   ❌ Could not fetch TMDb data")
                time.sleep(rate_delay)
                continue
            
            # Prepare updates
            updates = []
            
            if needs_gender and person_data['gender'] is not None:
                gender = map_gender(person_data['gender'])
                if gender != "Unknown":
                    updates.append(('H', gender))  # Column H: Gender
                    print(f"   ✅ Gender: {gender}")
            
            if needs_birthday and person_data['birthday']:
                birthday = person_data['birthday']
                updates.append(('I', birthday))  # Column I: Birthday
                print(f"   ✅ Birthday: {birthday}")
                
                # Calculate zodiac if needed
                if needs_zodiac:
                    zodiac = calculate_zodiac(birthday)
                    if zodiac:
                        updates.append(('J', zodiac))  # Column J: Zodiac
                        print(f"   ✅ Zodiac: {zodiac}")
            
            # Apply updates to sheet
            if updates:
                for col_letter, value in updates:
                    try:
                        sheet.update_cell(row_number, ord(col_letter) - ord('A') + 1, value)
                        time.sleep(0.1)  # Small delay between cell updates
                    except Exception as e:
                        print(f"   ❌ Error updating {col_letter}{row_number}: {e}")
                
                updates_made += len(updates)
                print(f"   ✅ Updated {len(updates)} fields")
            else:
                print(f"   ⚠️ No usable data found")
            
            # Rate limiting
            time.sleep(rate_delay)
            
            # Progress update
            if processed_count % 10 == 0:
                print(f"📈 Progress: {processed_count}/{len(data_rows)} rows processed, {updates_made} updates made")
        
        print(f"\n🎉 Processing complete!")
        print(f"   Rows processed: {processed_count}")
        print(f"   Updates made: {updates_made}")
        
    except Exception as e:
        print(f"❌ Error processing RealiteaseInfo sheet: {e}")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Fill Gender, Birthday, and Zodiac in RealiteaseInfo from TMDb")
    parser.add_argument("--start-row", type=int, default=2, help="Start row (1-based, default: 2)")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows to process (0 = all)")
    parser.add_argument("--rate-delay", type=float, default=0.5, help="Delay between TMDb calls (seconds)")
    
    args = parser.parse_args()
    
    print("🎯 RealiteaseInfo Biographical Data Enhancer")
    print("=" * 60)
    print("This script fills Gender, Birthday, and Zodiac columns using TMDb API")
    print(f"Target columns: H (Gender), I (Birthday), J (Zodiac)")
    print()
    
    # Connect to sheet
    sheet = connect_to_sheet()
    if not sheet:
        return
    
    print(f"✅ Connected to '{SPREADSHEET_NAME}' - RealiteaseInfo sheet")
    
    # Setup TMDb session
    session = setup_tmdb_session()
    if not session:
        return
    
    # Ask for confirmation
    limit_text = f" (limit: {args.limit})" if args.limit > 0 else " (all rows)"
    print(f"\n❓ Process rows starting from {args.start_row}{limit_text}? (y/N): ", end="")
    confirmation = input().strip().lower()
    
    if confirmation != 'y':
        print("❌ Operation cancelled.")
        return
    
    # Process the sheet
    process_realitease_info(
        sheet, 
        session, 
        start_row=args.start_row, 
        limit=args.limit, 
        rate_delay=args.rate_delay
    )
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fetch Missing Person Info Script

This script:
1. Reads the "Final Info" sheet to get missing people (columns E, F, G)
2. Compares their TMDb Cast ID (Column F) with CastID (Column A) of RealiteaseInfo sheet
3. Fetches birthday, birth sign, and gender from FamousBirthdays.com
4. Updates the RealiteaseInfo sheet with the gathered information

Author: GitHub Copilot
Date: August 27, 2025
"""

import gspread
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
import json

class MissingPersonInfoFetcher:
    def __init__(self):
        """Initialize the fetcher with Google Sheets connection"""
        print("🔄 Connecting to Google Sheets...")
        
        # Connect to Google Sheets
        gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        
        # Open the source spreadsheet (REALITEASE_FINAL by ID for reading missing people data)
        source_spreadsheet = gc.open_by_key('1-ZCTd0RdACEpyz9HkgaO7_R-Y6Wbrf-5M2XCtMXnMjM')
        
        # Open the target spreadsheet (Realitease2025Data for updating)
        target_spreadsheet = gc.open("Realitease2025Data")
        
        # List available worksheets in source
        source_worksheets = [ws.title for ws in source_spreadsheet.worksheets()]
        print(f"📋 Source worksheets: {source_worksheets}")
        
        # List available worksheets in target
        target_worksheets = [ws.title for ws in target_spreadsheet.worksheets()]
        print(f"📋 Target worksheets: {target_worksheets}")
        
        # Try to find the FinalInfo sheet in source
        final_sheet_name = None
        for sheet_name in source_worksheets:
            if sheet_name == 'FinalInfo':
                final_sheet_name = sheet_name
                break
        
        if not final_sheet_name:
            print("❌ Could not find FinalInfo sheet. Available source sheets:")
            for ws in source_worksheets:
                print(f"   - {ws}")
            raise Exception("FinalInfo sheet not found")
        
        # Get the source sheet for reading missing people data
        self.final_info_sheet = source_spreadsheet.worksheet(final_sheet_name)
        
        # Try to find RealiteaseInfo sheet in target spreadsheet
        realitease_sheet_name = None
        for sheet_name in target_worksheets:
            if 'realitease' in sheet_name.lower():
                realitease_sheet_name = sheet_name
                break
        
        if not realitease_sheet_name:
            print("❌ Could not find RealiteaseInfo sheet in target. Available target sheets:")
            for ws in target_worksheets:
                print(f"   - {ws}")
            raise Exception("RealiteaseInfo sheet not found in Realitease2025Data")
        
        # Get the target sheet for updating
        self.realitease_sheet = target_spreadsheet.worksheet(realitease_sheet_name)
        
        print(f"✅ Connected to Google Sheets:")
        print(f"   📄 Source - FinalInfo sheet: '{final_sheet_name}'")
        print(f"   📄 Target - RealiteaseInfo sheet in Realitease2025Data: '{realitease_sheet_name}'")
        
    def get_missing_people_data(self):
        """Get data from FinalInfo sheet"""
        print("🔍 Reading data from FinalInfo sheet...")
        
        # Get all data from the FinalInfo sheet
        all_data = self.final_info_sheet.get_all_records()
        
        final_info_data = {}
        
        for i, row in enumerate(all_data):
            cast_id = str(row.get('CastID', '')).strip()
            cast_name = row.get('CastName', '').strip()
            gender = row.get('Gender', '').strip()
            birthday = row.get('Birthday', '').strip()
            zodiac = row.get('Zodiac', '').strip()
            
            if cast_id and cast_name:
                final_info_data[cast_id] = {
                    'row_index': i + 2,  # +2 for header and 1-indexing
                    'cast_id': cast_id,
                    'cast_name': cast_name,
                    'gender': gender,
                    'birthday': birthday,
                    'zodiac': zodiac
                }
        
        print(f"📊 Loaded {len(final_info_data)} entries from FinalInfo (indexed by CastID)")
        return final_info_data
    
    def get_realitease_cast_data(self):
        """Get current cast data from RealiteaseInfo sheet that needs updating"""
        print("🔍 Reading current RealiteaseInfo data...")
        
        # Get all data from RealiteaseInfo sheet
        all_data = self.realitease_sheet.get_all_records()
        
        cast_data = []
        
        for i, row in enumerate(all_data):
            cast_tmdb_id = str(row.get('CastTMDbID', '')).strip()
            cast_name = row.get('CastName', '').strip()
            current_gender = row.get('Gender', '').strip()
            current_birthday = row.get('Birthday', '').strip()
            current_zodiac = row.get('Zodiac', '').strip()
            
            # Check if this entry is missing any bio data
            needs_update = (
                not current_gender or 
                not current_birthday or 
                not current_zodiac or
                current_gender in ['', 'Unknown'] or
                current_birthday in ['', 'Unknown'] or
                current_zodiac in ['', 'Unknown']
            )
            
            if cast_tmdb_id and cast_name and needs_update:
                cast_data.append({
                    'row_index': i + 2,  # +2 for header and 1-indexing
                    'cast_name': cast_name,
                    'cast_tmdb_id': cast_tmdb_id,
                    'current_gender': current_gender,
                    'current_birthday': current_birthday,
                    'current_zodiac': current_zodiac,
                    'needs_gender': not current_gender or current_gender in ['', 'Unknown'],
                    'needs_birthday': not current_birthday or current_birthday in ['', 'Unknown'],
                    'needs_zodiac': not current_zodiac or current_zodiac in ['', 'Unknown']
                })
        
        print(f"📊 Found {len(cast_data)} cast members in RealiteaseInfo needing bio data updates")
        return cast_data
    
    def find_matching_cast(self, final_info_data, realitease_cast):
        """Find matches between RealiteaseInfo (CastTMDbID) and FinalInfo (CastID)"""
        print("🔍 Finding matches between RealiteaseInfo CastTMDbID and FinalInfo CastID...")
        
        matches = []
        
        for realitease_entry in realitease_cast:
            cast_tmdb_id = realitease_entry['cast_tmdb_id']
            
            # Try to find a match by comparing CastTMDbID with CastID in FinalInfo
            if cast_tmdb_id in final_info_data:
                final_entry = final_info_data[cast_tmdb_id]
                
                matches.append({
                    'realitease_entry': realitease_entry,
                    'final_info_entry': final_entry
                })
                
                print(f"  ✅ Match found: TMDb ID {cast_tmdb_id} -> {realitease_entry['cast_name']} (Row {realitease_entry['row_index']})")
        
        print(f"📊 Found {len(matches)} cast members with matching data for updates")
        return matches
    
    def create_famousbirthdays_url(self, cast_name):
        """Create FamousBirthdays.com URL from cast name"""
        # Convert name to URL format
        # Remove special characters, convert to lowercase, replace spaces with hyphens
        url_name = re.sub(r'[^\w\s-]', '', cast_name.lower())
        url_name = re.sub(r'\s+', '-', url_name)
        url_name = re.sub(r'-+', '-', url_name)  # Remove multiple consecutive hyphens
        url_name = url_name.strip('-')  # Remove leading/trailing hyphens
        
        return f"https://www.famousbirthdays.com/people/{url_name}.html"
    
    def fetch_person_bio_data(self, cast_name):
        """Fetch birthday, birth sign, and gender from FamousBirthdays.com"""
        url = self.create_famousbirthdays_url(cast_name)
        
        print(f"    🌐 Fetching: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Initialize result
            result = {
                'birthday': '',
                'birth_sign': '',
                'gender': '',
                'success': False,
                'url': url
            }
            
            # Look for the bio-module__info section or similar structure
            bio_info = soup.find('div', class_='bio-module__info')
            
            if not bio_info:
                # Try alternative selectors for birthday and birth sign
                print(f"    🔍 Looking for birthday and birth sign with alternative selectors...")
                
                # Look for birthday
                birthday_text = ''
                birthday_patterns = [
                    soup.find(text=re.compile(r'BIRTHDAY', re.I)),
                    soup.find('span', text=re.compile(r'Birthday', re.I))
                ]
                
                for pattern in birthday_patterns:
                    if pattern:
                        parent = pattern.parent if hasattr(pattern, 'parent') else pattern
                        siblings = parent.find_next_siblings() if parent else []
                        for sibling in siblings:
                            if sibling.get_text():
                                birthday_text = sibling.get_text().strip()
                                break
                        if birthday_text:
                            break
                
                # Look for birth sign
                birth_sign_text = ''
                sign_patterns = [
                    soup.find(text=re.compile(r'BIRTH SIGN', re.I)),
                    soup.find('span', text=re.compile(r'Birth Sign', re.I))
                ]
                
                for pattern in sign_patterns:
                    if pattern:
                        parent = pattern.parent if hasattr(pattern, 'parent') else pattern
                        siblings = parent.find_next_siblings() if parent else []
                        for sibling in siblings:
                            if sibling.get_text():
                                birth_sign_text = sibling.get_text().strip()
                                break
                        if birth_sign_text:
                            break
                
                result['birthday'] = birthday_text
                result['birth_sign'] = birth_sign_text
                
            else:
                # Extract birthday from bio-module__person-attributes
                attributes_div = bio_info.find('div', class_='bio-module__person-attributes')
                if attributes_div:
                    # Look for Birthday
                    birthday_p = attributes_div.find('p', string=re.compile(r'Birthday', re.I))
                    if not birthday_p:
                        # Try to find span with "Birthday" text
                        birthday_span = attributes_div.find('span', string=re.compile(r'Birthday', re.I))
                        if birthday_span:
                            birthday_p = birthday_span.parent
                    
                    if birthday_p:
                        birthday_span = birthday_p.find('span', class_=False)  # Get the second span
                        if birthday_span:
                            result['birthday'] = birthday_span.get_text().strip()
                    
                    # Look for Birth Sign
                    birth_sign_p = attributes_div.find('p', string=re.compile(r'Birth Sign', re.I))
                    if not birth_sign_p:
                        birth_sign_span = attributes_div.find('span', string=re.compile(r'Birth Sign', re.I))
                        if birth_sign_span:
                            birth_sign_p = birth_sign_span.parent
                    
                    if birth_sign_p:
                        birth_sign_span = birth_sign_p.find('span', class_=False)
                        if birth_sign_span:
                            result['birth_sign'] = birth_sign_span.get_text().strip()
            
            # Look for gender in About section by analyzing pronouns
            about_section = soup.find('h2', string=re.compile(r'About', re.I))
            if about_section:
                about_content = about_section.find_next('p')
                if about_content:
                    about_text = about_content.get_text().lower()
                    
                    # Count pronouns to determine gender
                    he_count = len(re.findall(r'\bhe\b|\bhis\b|\bhim\b', about_text))
                    she_count = len(re.findall(r'\bshe\b|\bher\b|\bhers\b', about_text))
                    
                    if he_count > she_count and he_count > 0:
                        result['gender'] = 'Male'
                    elif she_count > he_count and she_count > 0:
                        result['gender'] = 'Female'
                    else:
                        result['gender'] = 'Unknown'
            
            # Clean up the birthday format
            if result['birthday']:
                # Parse birthday and convert to consistent format
                birthday = result['birthday']
                # Remove extra links/formatting and extract date
                birthday_clean = re.sub(r'<[^>]+>', '', birthday)  # Remove HTML tags
                birthday_clean = re.sub(r'\s+', ' ', birthday_clean).strip()
                result['birthday'] = birthday_clean
            
            # Check if we got any useful data
            if result['birthday'] or result['birth_sign'] or result['gender']:
                result['success'] = True
                print(f"    ✅ Success: Birthday={result['birthday']}, Sign={result['birth_sign']}, Gender={result['gender']}")
            else:
                print(f"    ⚠️ No bio data found")
            
            return result
            
        except requests.RequestException as e:
            print(f"    ❌ Request error: {e}")
            return {
                'birthday': '',
                'birth_sign': '',
                'gender': '',
                'success': False,
                'error': str(e),
                'url': url
            }
        except Exception as e:
            print(f"    ❌ Parse error: {e}")
            return {
                'birthday': '',
                'birth_sign': '',
                'gender': '',
                'success': False,
                'error': str(e),
                'url': url
            }
    
    def calculate_zodiac_from_birthday(self, birthday_str):
        """Calculate zodiac sign from birthday string"""
        try:
            # Try to parse various date formats
            date_patterns = [
                r'(\w+)\s+(\d+),?\s+(\d{4})',  # "September 12, 1977"
                r'(\d+)/(\d+)/(\d{4})',        # "9/12/1977"
                r'(\d{4})-(\d+)-(\d+)',        # "1977-09-12"
            ]
            
            month_map = {
                'january': 1, 'february': 2, 'march': 3, 'april': 4,
                'may': 5, 'june': 6, 'july': 7, 'august': 8,
                'september': 9, 'october': 10, 'november': 11, 'december': 12
            }
            
            month = day = year = None
            
            for pattern in date_patterns:
                match = re.search(pattern, birthday_str.lower())
                if match:
                    if pattern == date_patterns[0]:  # Month name format
                        month_name = match.group(1).lower()
                        month = month_map.get(month_name)
                        day = int(match.group(2))
                        year = int(match.group(3))
                    elif pattern == date_patterns[1]:  # MM/DD/YYYY
                        month = int(match.group(1))
                        day = int(match.group(2))
                        year = int(match.group(3))
                    elif pattern == date_patterns[2]:  # YYYY-MM-DD
                        year = int(match.group(1))
                        month = int(match.group(2))
                        day = int(match.group(3))
                    break
            
            if not (month and day):
                return ''
            
            # Calculate zodiac sign
            zodiac_signs = [
                (1, 20, "Capricorn"), (2, 19, "Aquarius"), (3, 21, "Pisces"),
                (4, 20, "Aries"), (5, 21, "Taurus"), (6, 21, "Gemini"),
                (7, 23, "Cancer"), (8, 23, "Leo"), (9, 23, "Virgo"),
                (10, 23, "Libra"), (11, 22, "Scorpio"), (12, 22, "Sagittarius")
            ]
            
            for i, (end_month, end_day, sign) in enumerate(zodiac_signs):
                if month < end_month or (month == end_month and day <= end_day):
                    return sign
            
            return "Capricorn"  # December 23-31
            
        except Exception as e:
            print(f"    ⚠️ Error calculating zodiac: {e}")
            return ''
    
    def update_realitease_sheet_batch(self, matches_batch):
        """Update the RealiteaseInfo sheet with bio data from FinalInfo in batches"""
        print(f"    🔄 Processing batch of {len(matches_batch)} entries...")
        
        batch_updates = []
        successful_updates = 0
        failed_updates = 0
        
        for match in matches_batch:
            realitease_entry = match['realitease_entry']
            final_info_entry = match['final_info_entry']
            
            row_index = realitease_entry['row_index']
            cast_name = realitease_entry['cast_name']
            
            updates_made = []
            
            try:
                # Prepare Gender update if needed and available (Column H)
                if realitease_entry['needs_gender'] and final_info_entry['gender']:
                    batch_updates.append({
                        'range': f'H{row_index}',
                        'values': [[final_info_entry['gender']]]
                    })
                    updates_made.append(f"Gender: {final_info_entry['gender']}")
                
                # Prepare Birthday update if needed and available (Column I)
                if realitease_entry['needs_birthday'] and final_info_entry['birthday']:
                    batch_updates.append({
                        'range': f'I{row_index}',
                        'values': [[final_info_entry['birthday']]]
                    })
                    updates_made.append(f"Birthday: {final_info_entry['birthday']}")
                
                # Prepare Zodiac update if needed and available (Column J)
                if realitease_entry['needs_zodiac'] and final_info_entry['zodiac']:
                    batch_updates.append({
                        'range': f'J{row_index}',
                        'values': [[final_info_entry['zodiac']]]
                    })
                    updates_made.append(f"Zodiac: {final_info_entry['zodiac']}")
                
                if updates_made:
                    print(f"      ✅ Will update {cast_name} (row {row_index}): {', '.join(updates_made)}")
                    successful_updates += 1
                else:
                    print(f"      ⚠️ No updates for {cast_name} (no new data available)")
                    failed_updates += 1
                    
            except Exception as e:
                print(f"      ❌ Error preparing {cast_name}: {e}")
                failed_updates += 1
        
        # Execute all updates in a single batch request
        if batch_updates:
            try:
                print(f"    📤 Executing batch update with {len(batch_updates)} cell updates...")
                self.realitease_sheet.batch_update(batch_updates)
                print(f"    ✅ Batch update completed successfully!")
                time.sleep(2.0)  # Longer pause between batches
            except Exception as e:
                print(f"    ❌ Batch update failed: {e}")
                successful_updates = 0
                failed_updates = len(matches_batch)
        
        return successful_updates, failed_updates
    
    def run_fetch_missing_info(self):
        """Main method to run the missing person info fetching process"""
        print("🚀 Starting Missing Person Info Fetcher")
        print("=" * 60)
        start_time = datetime.now()
        print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Get FinalInfo data
            final_info_data = self.get_missing_people_data()
            if not final_info_data:
                print("❌ No FinalInfo data found")
                return
            
            # Step 2: Get current RealiteaseInfo data that needs updating
            realitease_cast = self.get_realitease_cast_data()
            if not realitease_cast:
                print("❌ No RealiteaseInfo entries needing updates found")
                return
            
            # Step 3: Find matches with FinalInfo
            matches = self.find_matching_cast(final_info_data, realitease_cast)
            print(f"📊 Found {len(matches)} cast members with matching data from FinalInfo")
            
            # Step 4: Find entries that need FamousBirthdays.com scraping
            unmatched_entries = []
            for realitease_entry in realitease_cast:
                cast_tmdb_id = realitease_entry['cast_tmdb_id']
                if cast_tmdb_id not in final_info_data:
                    unmatched_entries.append(realitease_entry)
            
            print(f"📊 Found {len(unmatched_entries)} cast members needing FamousBirthdays.com scraping")
            
            # Process FinalInfo matches first
            if matches:
                print(f"\n🔄 Processing {len(matches)} FinalInfo matches in batches of 10...")
                successful_updates, failed_updates = self.process_finalinfo_matches(matches)
            else:
                successful_updates, failed_updates = 0, 0
            
            # Process FamousBirthdays.com scraping
            if unmatched_entries:
                print(f"\n🌐 Processing {len(unmatched_entries)} entries via FamousBirthdays.com in batches of 10...")
                scrape_successful, scrape_failed = self.process_famousbirthdays_scraping(unmatched_entries)
                successful_updates += scrape_successful
                failed_updates += scrape_failed
            
            # Summary
            print(f"\n✅ Processing complete!")
            print(f"📊 Total Results: {successful_updates} successful, {failed_updates} failed")
            
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"⏰ Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⏱️ Total duration: {duration}")
            
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            raise

def main():
    """Main function"""
    fetcher = MissingPersonInfoFetcher()
    fetcher.run_fetch_missing_info()

if __name__ == "__main__":
    main()

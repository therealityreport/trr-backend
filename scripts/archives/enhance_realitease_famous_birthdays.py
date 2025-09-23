#!/usr/bin/env python3

import os
import sys
import time
import re
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from datetime import datetime
import urllib.parse

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(load_env_path)

class RealiteaseInfoFamousBirthdaysEnhancer:
    def __init__(self):
        """Initialize the Famous Birthdays scraper for RealiteaseInfo sheet"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        })
        
        # Google Sheets setup
        self.gc = None
        self.worksheet = None
        
        # Row range configuration - will be set based on user choice
        self.start_row = None
        self.end_row = None
        
        # Processing counters
        self.processed_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.data_found_count = 0
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection to RealiteaseInfo sheet"""
        try:
            print("🔄 Setting up Google Sheets connection to RealiteaseInfo...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("RealiteaseInfo")
            
            print("✅ Google Sheets connection successful - Connected to RealiteaseInfo sheet")
            return True
            
        except FileNotFoundError as e:
            print(f"❌ Credentials file not found: {str(e)}")
            return False
        except gspread.exceptions.SpreadsheetNotFound as e:
            print(f"❌ Spreadsheet not found: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {str(e)}")
            print(f"❌ Error type: {type(e)}")
            return False

    def calculate_zodiac(self, birthday_str):
        """Calculate zodiac sign from birthday string (YYYY-MM-DD)"""
        if not birthday_str:
            return None
            
        try:
            date_obj = datetime.strptime(birthday_str, '%Y-%m-%d')
            month = date_obj.month
            day = date_obj.day
            
            # Zodiac sign calculation
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
                
        except Exception as e:
            print(f"   ⚠️ Error calculating zodiac for {birthday_str}: {e}")
            return None

    def search_famous_birthdays(self, cast_name, show_names):
        """Search for a person on Famous Birthdays"""
        try:
            # First, try the direct URL approach with different name formats
            name_slug = cast_name.lower().replace(' ', '-').replace('.', '').replace("'", "")
            
            # Try different URL patterns
            url_patterns = [
                f"https://www.famousbirthdays.com/people/{name_slug}.html",
            ]
            
            # Also try with first-last and last-first name patterns
            name_parts = cast_name.split()
            if len(name_parts) == 2:
                url_patterns.append(f"https://www.famousbirthdays.com/people/{name_parts[1].lower()}-{name_parts[0].lower()}.html")
            
            for url in url_patterns:
                print(f"   🔗 Trying direct URL: {url}")
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    print(f"   ✅ Found via direct URL: {url}")
                    return url
                else:
                    print(f"   ❌ 404 for: {url}")
            
            # If direct URLs don't work, try search with show names
            search_query = f"{cast_name} {show_names}".strip()
            search_url = f"https://www.famousbirthdays.com/search/people?q={urllib.parse.quote(search_query)}"
            
            print(f"   🔍 Searching: {search_url}")
            response = self.session.get(search_url, timeout=10)
            
            if response.status_code != 200:
                print(f"   ❌ Search failed with status {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for search results
            results = soup.find_all('div', class_='search-result')
            
            for result in results[:3]:  # Check first 3 results
                link = result.find('a')
                if link:
                    result_url = link.get('href', '')
                    if result_url.startswith('/'):
                        result_url = f"https://www.famousbirthdays.com{result_url}"
                    
                    # Check if the name is a reasonable match
                    result_name = link.text.strip() if link.text else ''
                    
                    name_match = self.fuzzy_name_match(cast_name.lower(), result_name.lower())
                    if name_match:
                        print(f"   ✅ Found match: {result_name} at {result_url}")
                        return result_url
            
            print(f"   ⚠️ No results found for: {cast_name}")
            return None
            
        except Exception as e:
            print(f"   ❌ Search error for {cast_name}: {e}")
            return None

    def fuzzy_name_match(self, target_name, found_name):
        """Check if names are a reasonable match"""
        # Remove common titles and suffixes
        clean_patterns = [' jr', ' sr', ' iii', ' ii', ' iv']
        for pattern in clean_patterns:
            target_name = target_name.replace(pattern, '')
            found_name = found_name.replace(pattern, '')
        
        target_parts = set(target_name.split())
        found_parts = set(found_name.split())
        
        # If at least 2 parts match, or if one is subset of other, consider it a match
        matching_parts = target_parts & found_parts
        
        if len(matching_parts) >= 2:
            return True
        
        if len(target_parts) == 1 and target_parts.issubset(found_parts):
            return True
            
        if len(found_parts) == 1 and found_parts.issubset(target_parts):
            return True
        
        return False

    def scrape_famous_birthdays_page(self, url, cast_name):
        """Scrape data from a Famous Birthdays page"""
        try:
            print(f"   📄 Scraping page: {url}")
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                print(f"   ❌ Failed to load page: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Initialize result
            result = {
                'name': None,
                'birthday': None,
                'gender': None,
                'zodiac': None
            }
            
            # Extract name - try multiple selectors
            name_elem = soup.find('h1') or soup.find('div', class_='name') or soup.find('h1', class_='bio-name')
            if name_elem:
                result['name'] = name_elem.text.strip()
                print(f"   Debug - Name: {result['name']}")
            
            # Extract birthday - updated patterns
            birthday_patterns = [
                # Pattern 1: "Birthday: Month Day, Year" or "Birthday:Month Day,Year" (with or without spaces)
                (re.compile(r'Birthday\s*[:]\s*(\w+)\s+(\d{1,2})\s*,\s*(\d{4})', re.IGNORECASE), '%B %d %Y'),
                # Pattern 2: Handle "BirthdayMonth Day,Year" (no colon, no space after Birthday)
                (re.compile(r'Birthday\s*(\w+)\s+(\d{1,2})\s*,\s*(\d{4})', re.IGNORECASE), '%B %d %Y'),
                # Pattern 3: Birth date in various formats
                (re.compile(r'Born\s*[:]\s*(\w+)\s+(\d{1,2})\s*,\s*(\d{4})', re.IGNORECASE), '%B %d %Y'),
                # Pattern 4: Date format like "October 6, 1986"
                (re.compile(r'(\w+)\s+(\d{1,2})\s*,\s*(\d{4})'), '%B %d %Y'),
            ]
            
            page_text = soup.get_text()
            
            for pattern, date_format in birthday_patterns:
                match = pattern.search(page_text)
                if match:
                    try:
                        if len(match.groups()) == 3:
                            month, day, year = match.groups()
                            date_str = f"{month} {day} {year}"
                            parsed_date = datetime.strptime(date_str, date_format)
                            result['birthday'] = parsed_date.strftime('%Y-%m-%d')
                            # Calculate zodiac sign
                            result['zodiac'] = self.calculate_zodiac(result['birthday'])
                            print(f"   Debug - Birthday found: {result['birthday']}, Zodiac: {result['zodiac']}")
                            break
                    except Exception as e:
                        print(f"   Debug - Failed to parse date from match: {match.group(0)}, error: {e}")
            
            # Also check for birthday in specific elements
            if not result['birthday']:
                birthday_elem = soup.find('span', string=re.compile(r'Birthday', re.IGNORECASE))
                if birthday_elem:
                    parent = birthday_elem.parent
                    if parent:
                        text = parent.get_text()
                        print(f"   Debug - Birthday element text: {text}")
                        for pattern, date_format in birthday_patterns:
                            match = pattern.search(text)
                            if match:
                                try:
                                    if len(match.groups()) == 3:
                                        month, day, year = match.groups()
                                        date_str = f"{month} {day} {year}"
                                        parsed_date = datetime.strptime(date_str, date_format)
                                        result['birthday'] = parsed_date.strftime('%Y-%m-%d')
                                        result['zodiac'] = self.calculate_zodiac(result['birthday'])
                                        print(f"   Debug - Birthday parsed: {result['birthday']}, Zodiac: {result['zodiac']}")
                                        break
                                except Exception as e:
                                    print(f"   Debug - Failed to parse date: {e}")
            
            # Extract gender - look for pronouns or gender indicators
            bio_text = page_text.lower()
            
            # Check for explicit gender mentions
            if 'gender: male' in bio_text or 'gender: m' in bio_text:
                result['gender'] = 'M'
            elif 'gender: female' in bio_text or 'gender: f' in bio_text:
                result['gender'] = 'F'
            # Check for pronouns in bio
            elif ' he ' in bio_text or ' his ' in bio_text or ' him ' in bio_text:
                result['gender'] = 'M'
            elif ' she ' in bio_text or ' her ' in bio_text or ' hers ' in bio_text:
                result['gender'] = 'F'
            
            print(f"   Debug - Gender: {result['gender']}")
            
            # Return result even if only partial data found
            has_data = any([result['birthday'], result['gender'], result['zodiac']])
            
            if has_data:
                print(f"   ✅ Extracted data - Birthday: {result['birthday']}, Gender: {result['gender']}, Zodiac: {result['zodiac']}")
                return result
            else:
                print(f"   ⚠️ No relevant data found on page")
                return None
            
        except Exception as e:
            print(f"   ❌ Error scraping page for {cast_name}: {e}")
            return None

    def update_spreadsheet(self, row_index, data, cast_name):
        """Update the RealiteaseInfo Google Sheet with extracted data"""
        try:
            updates_made = []
            
            # RealiteaseInfo column mapping:
            # A: CastName, B: CastIMDbID, C: CastTMDbID, D: ShowNames, E: ShowIMDbIDs, 
            # F: ShowTMDbIDs, G: ShowCount, H: Gender, I: Birthday, J: Zodiac
            
            # Update Gender column (column H = 8) if found
            if data.get('gender'):
                self.worksheet.update_cell(row_index, 8, data['gender'])
                updates_made.append(f"Gender: {data['gender']}")
                time.sleep(0.3)
            
            # Update Birthday column (column I = 9) if found
            if data.get('birthday'):
                self.worksheet.update_cell(row_index, 9, data['birthday'])
                updates_made.append(f"Birthday: {data['birthday']}")
                time.sleep(0.3)
            
            # Update Zodiac column (column J = 10) if found
            if data.get('zodiac'):
                self.worksheet.update_cell(row_index, 10, data['zodiac'])
                updates_made.append(f"Zodiac: {data['zodiac']}")
                time.sleep(0.3)
            
            if updates_made:
                print(f"   ✅ Updated {cast_name}: {', '.join(updates_made)}")
                return True
            else:
                print(f"   ⚠️ No updates made for {cast_name}")
                return False
            
        except Exception as e:
            print(f"   ❌ Failed to update spreadsheet for {cast_name}: {e}")
            return False

    def process_range(self):
        """Process cast members in the specified row range"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from the spreadsheet
            print("📊 Loading RealiteaseInfo spreadsheet data...")
            all_data = self.worksheet.get_all_values()
            
            total_rows = len(all_data)
            print(f"📊 Total rows in RealiteaseInfo: {total_rows}")
            
            # Get user input for range
            print("\n🎯 Select processing range:")
            print(f"1. Process all rows (2 to {total_rows})")
            print("2. Process specific range")
            print("3. Process from specific row to end")
            
            choice = input("\nEnter choice (1-3): ").strip()
            
            if choice == '1':
                self.start_row = 2
                self.end_row = total_rows
            elif choice == '2':
                self.start_row = int(input("Enter start row: "))
                self.end_row = int(input("Enter end row: "))
            elif choice == '3':
                self.start_row = int(input("Enter start row: "))
                self.end_row = total_rows
            else:
                print("❌ Invalid choice")
                return False
            
            # Validate range
            if self.start_row < 2:
                self.start_row = 2
            if self.end_row > total_rows:
                self.end_row = total_rows
            
            print(f"\n📊 Processing rows {self.start_row} to {self.end_row}")
            print(f"📊 Total rows to process: {self.end_row - self.start_row + 1}")
            
            # Confirm before proceeding
            confirm = input("\nProceed? (y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ Cancelled by user")
                return False
            
            # Process each row in the specified range
            for row_num in range(self.start_row, self.end_row + 1):
                row_index = row_num - 1  # Convert to 0-based index
                row = all_data[row_index]
                
                self.processed_count += 1
                
                # Parse row data - RealiteaseInfo columns
                cast_name = row[0] if len(row) > 0 else ''       # Column A: CastName
                show_names = row[3] if len(row) > 3 else ''      # Column D: ShowNames
                gender = row[7] if len(row) > 7 else ''          # Column H: Gender
                birthday = row[8] if len(row) > 8 else ''        # Column I: Birthday
                zodiac = row[9] if len(row) > 9 else ''          # Column J: Zodiac
                
                # Skip if we already have all data
                if birthday and gender and zodiac:
                    print(f"⏭️ Row {row_num}: {cast_name} - Already complete")
                    self.skipped_count += 1
                    continue
                
                # Skip if no cast name
                if not cast_name:
                    print(f"⚠️ Row {row_num}: No cast name, skipping")
                    self.skipped_count += 1
                    continue
                
                print(f"\n🎭 Row {row_num}/{self.end_row}: {cast_name} from {show_names or 'Unknown Shows'}")
                
                # Search for the person
                fb_url = self.search_famous_birthdays(cast_name, show_names)
                
                if fb_url:
                    # Scrape the page
                    data = self.scrape_famous_birthdays_page(fb_url, cast_name)
                    
                    if data:
                        # Only update fields we don't already have
                        update_data = {}
                        if not gender and data.get('gender'):
                            update_data['gender'] = data['gender']
                        if not birthday and data.get('birthday'):
                            update_data['birthday'] = data['birthday']
                        if not zodiac and data.get('zodiac'):
                            update_data['zodiac'] = data['zodiac']
                        
                        if update_data:
                            if self.update_spreadsheet(row_num, update_data, cast_name):
                                self.updated_count += 1
                                self.data_found_count += 1
                            else:
                                self.failed_count += 1
                        else:
                            print(f"   ℹ️ No new data to update")
                            self.skipped_count += 1
                    else:
                        print(f"   ⚠️ No data extracted from page")
                        self.failed_count += 1
                else:
                    print(f"   ⚠️ Person not found on Famous Birthdays")
                    self.failed_count += 1
                
                # Add delay to avoid rate limiting
                time.sleep(1.5)
                
                # Progress update every 25 rows
                if self.processed_count % 25 == 0:
                    self.print_progress()
            
            # Final summary
            print("\n" + "="*60)
            print("🎉 Processing complete!")
            self.print_progress()
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing range: {e}")
            import traceback
            traceback.print_exc()
            return False

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
        data_rate = (self.data_found_count / self.processed_count * 100) if self.processed_count > 0 else 0
        
        print(f"📊 Progress Report:")
        print(f"   • Processed: {self.processed_count}")
        print(f"   • Updated: {self.updated_count}")
        print(f"   • Skipped: {self.skipped_count}")
        print(f"   • Failed: {self.failed_count}")
        print(f"   • Data found: {self.data_found_count}")
        print(f"   • Success rate: {success_rate:.1f}%")
        print(f"   • Data retrieval rate: {data_rate:.1f}%")

def main():
    """Main function"""
    print("🚀 Starting RealiteaseInfo Famous Birthdays Data Enhancer...")
    print("📝 This tool will enhance your RealiteaseInfo cast data with:")
    print("   • Birthdays")
    print("   • Gender")
    print("   • Zodiac signs (calculated from birthdays)")
    print("   from Famous Birthdays website\n")
    
    enhancer = RealiteaseInfoFamousBirthdaysEnhancer()
    
    success = enhancer.process_range()
    
    if success:
        print("\n✅ RealiteaseInfo enhancement completed successfully!")
    else:
        print("\n❌ RealiteaseInfo enhancement failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

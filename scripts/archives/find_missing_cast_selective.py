#!/usr/bin/env python3
"""
Find and add missing MAIN CAST members from IMDb.
This script is much more selective and only adds legitimate cast members,
not crew, producers, or single-episode appearances.
"""

import os
import time
import gspread
import requests
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Set, Optional
import re

# Load environment variables
load_dotenv()

class SelectiveIMDbCastScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Connect to Google Sheets
        self.gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sh = self.gc.open_by_key(os.getenv('SPREADSHEET_ID'))
        
        # Load existing data
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load existing cast and show data from Google Sheets."""
        print("🔄 Loading existing data from Google Sheets...")
        
        # Load CastInfo
        cast_ws = self.sh.worksheet('CastInfo')
        cast_data = cast_ws.get_all_records()
        
        # Create mapping of existing cast: {show_imdb_id: {cast_name.lower()}}
        self.existing_cast = {}
        # Also create a set of all existing Cast IMDb IDs for cross-show checking
        self.existing_cast_imdb_ids = set()
        # Create a set of existing show+cast combinations to prevent duplicates
        self.existing_show_cast_combinations = set()
        
        for row in cast_data:
            show_imdb_id = str(row.get('Show IMDbID', '')).strip()
            cast_name = str(row.get('CastName', '')).strip()
            cast_imdb_id = str(row.get('Cast IMDbID', '')).strip()
            
            if show_imdb_id and cast_name:
                if show_imdb_id not in self.existing_cast:
                    self.existing_cast[show_imdb_id] = set()
                self.existing_cast[show_imdb_id].add(cast_name.lower())
            
            if cast_imdb_id:
                self.existing_cast_imdb_ids.add(cast_imdb_id)
                
            # Track show+cast combinations to prevent duplicates
            if show_imdb_id and cast_imdb_id:
                self.existing_show_cast_combinations.add(f"{show_imdb_id}|{cast_imdb_id}")
        
        print(f"✅ Loaded existing cast for {len(self.existing_cast)} shows")
        print(f"✅ Loaded {len(self.existing_cast_imdb_ids)} unique Cast IMDb IDs for cross-show checking")
        
        # Load ShowInfo
        show_ws = self.sh.worksheet('ShowInfo')
        show_data = show_ws.get_all_records()
        
        # Create mapping: {imdb_id: show_info}
        self.shows_with_imdb = {}
        for row in show_data:
            imdb_id = str(row.get('IMDbSeriesID', '')).strip()
            if imdb_id:
                self.shows_with_imdb[imdb_id] = {
                    'show_name': str(row.get('ShowName', '')).strip(),
                    'tmdb_id': str(row.get('TheMovieDB ID', '')).strip(),
                    'network': str(row.get('Network', '')).strip(),
                    'total_seasons': str(row.get('ShowTotalSeasons', '')).strip(),
                    'total_episodes': str(row.get('ShowTotalEpisodes', '')).strip()
                }
        
        print(f"✅ Loaded {len(self.shows_with_imdb)} shows with IMDb IDs")
        
        # Get next CastID
        self.next_cast_id = self.get_next_cast_id()
        print(f"📊 Next available CastID: {self.next_cast_id}")
    
    def get_next_cast_id(self) -> int:
        """Get the next available CastID."""
        cast_ws = self.sh.worksheet('CastInfo')
        cast_ids = cast_ws.col_values(2)[1:]  # Skip header, get CastID column
        
        # Find the highest CastID
        max_id = 0
        for cast_id in cast_ids:
            try:
                cast_id_int = int(str(cast_id).strip())
                if cast_id_int > max_id:
                    max_id = cast_id_int
            except (ValueError, AttributeError):
                continue
        
        return max_id + 1
    
    def is_crew_member(self, name: str, character: str) -> bool:
        """Check if this person is likely a crew member based on name or character."""
        crew_indicators = [
            # Job titles
            'producer', 'director', 'writer', 'editor', 'cinematographer',
            'executive', 'creator', 'composer', 'music', 'sound', 'camera',
            'production', 'assistant', 'coordinator', 'supervisor', 'manager',
            'operator', 'technician', 'consultant', 'researcher', 'script',
            
            # Known crew names that commonly appear
            'scott dunlop', 'alex coletti', 'manny rodriguez', 'douglas ross',
            'kathleen french', 'alex baskin', 'brett staneart'
        ]
        
        name_lower = name.lower()
        character_lower = character.lower()
        
        return any(indicator in name_lower or indicator in character_lower 
                  for indicator in crew_indicators)
    
    def is_valid_cast_member(self, name: str, character: str, episode_info: str, 
                           show_name: str) -> bool:
        """
        Determine if this person should be considered a main cast member.
        
        Criteria for inclusion:
        1. NOT a crew member
        2. Has either:
           - A meaningful character name (not just "Self")
           - Appears in multiple episodes
           - Is a family member/spouse (for reality shows)
        """
        
        # Skip crew members
        if self.is_crew_member(name, character):
            return False
        
        # Skip if name is too short or generic
        if len(name) < 2:
            return False
        
        # Skip obvious non-cast
        skip_characters = ['archive footage', 'uncredited', 'voice']
        if any(skip in character.lower() for skip in skip_characters):
            return False
        
        # Extract episode count
        episode_count = 0
        episode_numbers = re.findall(r'(\d+)\s*episode', episode_info, re.IGNORECASE)
        if episode_numbers:
            episode_count = int(episode_numbers[0])
        
        # For reality shows (like Real Housewives), be more inclusive
        is_reality_show = any(keyword in show_name.lower() for keyword in 
                             ['housewives', 'dating', 'bachelor', 'love island', 
                              'survivor', 'big brother', 'traitors'])
        
        if is_reality_show:
            # For reality shows, include:
            # - People with character names that aren't just "Self"
            # - People with 2+ episodes
            # - People with no episode info (likely main cast)
            
            if character and character.lower() not in ['self', '']:
                return True
            
            if episode_count >= 2:
                return True
                
            if not episode_info:  # No episode info might mean main cast
                return True
                
            # Skip single episode appearances in reality shows
            if episode_count == 1:
                return False
        
        else:
            # For scripted shows, be more selective
            # Include people with actual character names
            if character and character.lower() not in ['self', '']:
                # Still require multiple episodes for scripted shows
                return episode_count >= 2 or episode_count == 0
            
            # Require 3+ episodes for people without character names
            return episode_count >= 3
        
        return False
    
    def is_valid_cast_member_with_episodes(self, name: str, character: str, show_name: str, imdb_person_id: str) -> tuple:
        """
        Stricter validation that also extracts episode count.
        Returns (is_valid, episode_count)
        
        Include cast members who either:
        1. Already appear in another show AND have at least 1 episode
        2. Don't appear elsewhere but have 8+ episodes (their only show)
        """
        
        # Skip crew members
        if self.is_crew_member(name, character):
            return False, 0
        
        # Skip if name is too short
        if len(name) < 2:
            return False, 0
        
        # Extract episode count from character field
        episode_count = 0
        episode_match = re.search(r'(\d+)\s+episode', character)
        if episode_match:
            episode_count = int(episode_match.group(1))
        
        # Check if this person already appears in another show
        already_exists = imdb_person_id in self.existing_cast_imdb_ids
        
        # Stricter validation logic:
        # 1. If they already exist in another show, require at least 1 episode
        # 2. If they don't exist elsewhere, require 8+ episodes
        
        if already_exists:
            # Person already appears in another show - require at least 1 episode
            return episode_count >= 1, episode_count
        else:
            # New person - require 8+ episodes for their only show
            return episode_count >= 8, episode_count
    
    def scrape_main_cast_only(self, imdb_id: str, show_name: str) -> List[Dict]:
        """
        Scrape ONLY main cast members from IMDb cast page.
        Very selective - focuses on actual cast, not crew or minor roles.
        """
        print(f"🔍 Scraping MAIN CAST for '{show_name}' (IMDb: {imdb_id})")
        
        try:
            # Use fullcredits page for most complete cast list
            cast_url = f"https://www.imdb.com/title/{imdb_id}/fullcredits/"
            
            print(f"  📡 Accessing: {cast_url}")
            response = requests.get(cast_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the Cast section using modern IMDb structure
            cast_section = soup.find('div', {'data-testid': 'sub-section-cast'})
            
            if not cast_section:
                print("  ❌ No cast section found")
                return []
            
            print("  ✓ Found cast section")
            
            # Find all name links in the cast section
            name_links = cast_section.find_all('a', href=re.compile(r'/name/nm\d+/'))
            valid_cast = []
            
            print(f"  📊 Processing {len(name_links)} cast entries...")
            
            
            # Process each name link
            for i, name_link in enumerate(name_links):
                try:
                    actor_name = name_link.get_text(strip=True)
                    if not actor_name or len(actor_name) < 2:
                        continue
                        
                    href = name_link.get('href', '')
                    imdb_match = re.search(r'/name/(nm\d+)/', href)
                    if not imdb_match:
                        continue
                    
                    imdb_person_id = imdb_match.group(1)
                    
                    # Extract character and episode info from the parent element
                    character = ""
                    episode_info = ""
                    
                    # Try to find character/episode info in nearby elements
                    parent = name_link.parent
                    if parent:
                        # Look for character or role information in the same parent
                        character_text = parent.get_text(strip=True)
                        if character_text != actor_name:
                            # Extract character if it's different from the name
                            character = character_text.replace(actor_name, '').strip()
                            character = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', character)
                            episode_info = character  # For now, treat them the same
                    
                    # Apply filtering with episode count logic
                    is_valid, episode_count = self.is_valid_cast_member_with_episodes(
                        actor_name, character, show_name, imdb_person_id)
                    
                    if is_valid:
                        # Check for duplicates: Show IMDb ID + Cast IMDb ID combination
                        combination_key = f"{imdb_id}|{imdb_person_id}"
                        if combination_key in self.existing_show_cast_combinations:
                            # Skip duplicate
                            if len(valid_cast) <= 15:
                                print(f"    🚫 Duplicate: {actor_name} (already exists for this show)")
                            continue
                        
                        valid_cast.append({
                            'name': actor_name,
                            'imdb_person_id': imdb_person_id,
                            'character': character,
                            'episode_info': episode_info,
                            'episode_count': episode_count
                        })
                        
                        # Log first few for verification
                        if len(valid_cast) <= 15:
                            exists_note = " (exists in other show)" if imdb_person_id in self.existing_cast_imdb_ids else ""
                            print(f"    ✓ {actor_name} | Episodes: {episode_count}{exists_note}")
                    else:
                        # Log first few rejections for debugging
                        if i < 10:
                            print(f"    ❌ Skipped: {actor_name} | Episodes: {episode_count} (insufficient)")
                
                except Exception as e:
                    print(f"    Error processing name link {i}: {e}")
                    continue
                
                except Exception as e:
                    print(f"    Error processing name link {i}: {e}")
                    continue
            
            print(f"  ✅ Selected {len(valid_cast)} main cast members from {len(name_links)} total entries")
            return valid_cast
            
        except Exception as e:
            print(f"  ❌ Error scraping {show_name}: {e}")
            return []
    
    def find_missing_main_cast(self, imdb_id: str, show_info: Dict) -> List[Dict]:
        """Find missing main cast members for a specific show."""
        show_name = show_info['show_name']
        
        # Get main cast from IMDb
        imdb_cast = self.scrape_main_cast_only(imdb_id, show_name)
        if not imdb_cast:
            return []
        
        # Get existing cast for this show
        existing_cast_names = self.existing_cast.get(imdb_id, set())
        
        # Find missing cast
        missing_cast = []
        for cast_member in imdb_cast:
            cast_name = cast_member['name']
            cast_imdb_id = cast_member['imdb_person_id']
            
            # Check for duplicates by name OR by Show+Cast IMDb ID combination
            combination_key = f"{imdb_id}|{cast_imdb_id}"
            
            if (cast_name.lower() not in existing_cast_names and 
                combination_key not in self.existing_show_cast_combinations):
                missing_cast.append({
                    'show_imdb_id': imdb_id,
                    'cast_name': cast_name,
                    'imdb_person_id': cast_imdb_id,
                    'character': cast_member['character'],
                    'episode_info': cast_member['episode_info'],
                    'episode_count': cast_member['episode_count'],  # Add episode count
                    'show_name': show_name,
                    'tmdb_id': show_info['tmdb_id'],
                    'network': show_info['network']
                })
        
        print(f"  📊 Found {len(missing_cast)} missing main cast members")
        
        if missing_cast:
            print("  Missing cast:")
            for cast in missing_cast:
                exists_note = " (exists in other show)" if cast['imdb_person_id'] in self.existing_cast_imdb_ids else ""
                print(f"    - {cast['cast_name']} ({cast['episode_count']} episodes{exists_note})")
        
        return missing_cast
    
    def add_missing_cast_to_sheet(self, missing_cast: List[Dict]):
        """Add missing cast members to CastInfo sheet."""
        if not missing_cast:
            print("✅ No missing cast to add")
            return
        
        print(f"🔄 Adding {len(missing_cast)} missing main cast members...")
        
        cast_ws = self.sh.worksheet('CastInfo')
        
        # Prepare new rows
        new_rows = []
        current_cast_id = self.next_cast_id
        
        for cast_member in missing_cast:
            new_row = [
                cast_member['show_imdb_id'],        # Show IMDbID
                str(current_cast_id),               # CastID
                cast_member['cast_name'],           # CastName
                cast_member['imdb_person_id'],      # Cast IMDbID
                cast_member['tmdb_id'],             # ShowID (TMDb)
                cast_member['show_name'],           # ShowName
                str(cast_member['episode_count']),  # EpisodeCount
                "",                                 # Seasons (to be filled later)
                ""                                  # Seasons-Update (to be filled later)
            ]
            new_rows.append(new_row)
            current_cast_id += 1
        
        # Add rows in small batches
        batch_size = 20
        for i in range(0, len(new_rows), batch_size):
            batch = new_rows[i:i + batch_size]
            print(f"  Adding batch {i//batch_size + 1}/{(len(new_rows) + batch_size - 1)//batch_size}")
            
            cast_ws.append_rows(batch)
            
            # Rate limiting
            if i + batch_size < len(new_rows):
                time.sleep(2)
        
        print(f"✅ Added {len(missing_cast)} main cast members")
        self.next_cast_id = current_cast_id
    
    def process_all_shows(self):
        """Process ALL shows with IMDb IDs using selective filtering."""
        all_show_ids = list(self.shows_with_imdb.keys())
        
        print(f"🎯 Processing ALL {len(all_show_ids)} shows with selective filtering...")
        
        total_added = 0
        processed_count = 0
        
        for imdb_id in all_show_ids:
            processed_count += 1
            show_info = self.shows_with_imdb[imdb_id]
            print(f"\n🔄 Processing [{processed_count}/{len(all_show_ids)}]: {show_info['show_name']}")
            
            missing_cast = self.find_missing_main_cast(imdb_id, show_info)
            
            if missing_cast:
                added_count = self.add_missing_cast_to_sheet(missing_cast)
                if added_count is not None:
                    total_added += added_count
            else:
                print("  ✅ No missing main cast members found")
        
        print(f"\n🎉 All shows completed! Added {total_added} main cast members total.")

    def process_priority_shows(self):
        """Process priority shows (Real Housewives series)."""
        priority_shows = [
            "tt1252370",  # RHOA
            "tt1411598",  # RHONJ  
            "tt1191056",  # RHONY
            "tt1720601",  # RHOBH
        ]
        
        print(f"🎯 Processing {len(priority_shows)} priority shows with selective filtering...")
        
        total_added = 0
        
        for imdb_id in priority_shows:
            if imdb_id in self.shows_with_imdb:
                show_info = self.shows_with_imdb[imdb_id]
                print(f"\n🔄 Processing: {show_info['show_name']}")
                
                missing_cast = self.find_missing_main_cast(imdb_id, show_info)
                
                if missing_cast:
                    self.add_missing_cast_to_sheet(missing_cast)
                    total_added += len(missing_cast)
                else:
                    print(f"  ✅ No missing main cast members found")
                
                # Rate limiting between shows
                time.sleep(5)
            else:
                print(f"⚠️  Priority show {imdb_id} not found in ShowInfo")
        
        print(f"\n🎉 Priority shows completed! Added {total_added} main cast members total.")

def main():
    """Main execution function."""
    print("🚀 Starting SELECTIVE missing cast member discovery...")
    print("🎯 Focus: MAIN CAST ONLY (no crew, producers, or single-episode roles)")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        scraper = SelectiveIMDbCastScraper()
        scraper.process_all_shows()  # Changed from process_priority_shows() to process_all_shows()
        
    except Exception as e:
        print(f"\n❌ Script failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import sys
import time
import re
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(load_env_path)

class WWHLExtractor:
    def __init__(self):
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        
        if not self.tmdb_bearer and not self.tmdb_api_key:
            raise ValueError("Neither TMDB_BEARER nor TMDB_API_KEY found in environment variables")
        
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        
        # Use bearer token if available, otherwise use API key
        if self.tmdb_bearer:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_bearer}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB Bearer token for authentication")
        else:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_api_key}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB API key for authentication")
        
        # Test API key first
        test_response = self.session.get(f"{self.base_url}/configuration")
        if test_response.status_code != 200:
            print(f"❌ TMDB API Key test failed: {test_response.status_code}")
            raise ValueError("Invalid TMDB API key or API access issue")
        else:
            print("✅ TMDB API Key validated successfully")
        
        # Google Sheets setup
        self.gc = None
        self.source_worksheet = None
        self.wwhl_worksheet = None
        
        # WWHL TMDb Show ID (constant)
        self.WWHL_SHOW_ID = "22980"
        
        # Processing counters
        self.processed_count = 0
        self.found_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        
        # Cache for episode details to reduce API calls
        self.episode_cache = {}
        
    def setup_google_sheets(self):
        """Set up Google Sheets connection"""
        try:
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            
            # Get CastInfo sheet for TMDb/IMDb mapping
            self.castinfo_worksheet = workbook.worksheet("CastInfo")
            
            # Get RealiteaseInfo sheet for reality TV cast checking
            self.realiteaseinfo_worksheet = workbook.worksheet("RealiteaseInfo")
            
            # Create or get WWHL sheet
            try:
                self.wwhl_worksheet = workbook.worksheet("WWHLinfo")
                print("✅ Found existing WWHLinfo sheet")
                
                # Check if headers exist, if not add them
                try:
                    first_row = self.wwhl_worksheet.row_values(1)
                    if not first_row or first_row[0] != "EpisodeTMDbID":
                        # Add headers with all columns in correct order
                        headers = [
                            "EpisodeTMDbID", 
                            "EpisodeMarker", 
                            "Season",
                            "Episode",
                            "AirDate",
                            "GuestStarNames",
                            "GuestStarTMDbIDs",
                            "GuestStarIMDbIDs",
                            "Cast_Source"
                        ]
                        self.wwhl_worksheet.update('A1:I1', [headers])
                        print("✅ Updated headers in existing WWHLinfo sheet")
                except Exception as e:
                    print(f"⚠️ Could not check/update headers: {e}")
                    
            except gspread.WorksheetNotFound:
                # Create new WWHLinfo sheet
                self.wwhl_worksheet = workbook.add_worksheet(title="WWHLinfo", rows=1000, cols=8)
                
                # Add headers with all columns in correct order
                headers = [
                    "EpisodeTMDbID", 
                    "EpisodeMarker", 
                    "Season",
                    "Episode",
                    "AirDate",
                    "GuestStarNames",
                    "GuestStarTMDbIDs",
                    "GuestStarIMDbIDs",
                    "Cast_Source"
                ]
                self.wwhl_worksheet.update('A1:I1', [headers])
                print("✅ Created WWHLinfo sheet with headers")
            
            print("✅ WWHL: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ WWHL: Google Sheets setup failed: {str(e)}")
            return False

    def get_episode_details(self, season_number, episode_number):
        """Get detailed episode information including air date and IMDb ID"""
        cache_key = f"S{season_number}E{episode_number}"
        
        # Check cache first
        if cache_key in self.episode_cache:
            return self.episode_cache[cache_key]
        
        try:
            print(f"📅 WWHL: Getting episode details for S{season_number}E{episode_number}")
            
            # Get episode details from TMDb
            episode_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_number}/episode/{episode_number}"
            response = self.session.get(episode_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get episode details for S{season_number}E{episode_number}: {response.status_code}")
                return None, None
            
            episode_data = response.json()
            
            # Extract air date
            air_date = episode_data.get('air_date', '')
            if air_date:
                # Format date as MM/DD/YYYY for consistency
                try:
                    date_obj = datetime.strptime(air_date, '%Y-%m-%d')
                    formatted_date = date_obj.strftime('%m/%d/%Y')
                except:
                    formatted_date = air_date  # Use original if parsing fails
            else:
                formatted_date = ''
            
            # Get episode's IMDb ID from external IDs
            external_ids_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_number}/episode/{episode_number}/external_ids"
            external_response = self.session.get(external_ids_url)
            
            episode_imdb_id = ''
            if external_response.status_code == 200:
                external_data = external_response.json()
                episode_imdb_id = external_data.get('imdb_id', '')
                if episode_imdb_id:
                    print(f"✅ WWHL: Found episode IMDb ID: {episode_imdb_id}")
            
            # Cache the result
            self.episode_cache[cache_key] = (formatted_date, episode_imdb_id)
            
            return formatted_date, episode_imdb_id
            
        except Exception as e:
            print(f"❌ WWHL: Error getting episode details for S{season_number}E{episode_number}: {e}")
            return '', ''

    def load_castinfo_mapping(self):
        """Load CastInfo sheet to create TMDb ID to IMDb ID mapping"""
        try:
            print("🔄 Loading CastInfo mapping...")
            castinfo_data = self.castinfo_worksheet.get_all_values()
            
            if len(castinfo_data) < 2:
                print("⚠️ CastInfo sheet is empty or has no data")
                return {}
            
            # Find column indexes
            headers = [h.strip().lower() for h in castinfo_data[0]]
            tmdb_col = None
            imdb_col = None
            
            for i, header in enumerate(headers):
                if 'tmdb' in header and 'id' in header:
                    tmdb_col = i
                if 'imdb' in header and 'id' in header:
                    imdb_col = i
            
            if tmdb_col is None or imdb_col is None:
                print(f"⚠️ Could not find TMDb or IMDb columns in CastInfo")
                return {}
            
            # Create mapping
            mapping = {}
            for row in castinfo_data[1:]:  # Skip header
                if len(row) > max(tmdb_col, imdb_col):
                    tmdb_id = row[tmdb_col].strip()
                    imdb_id = row[imdb_col].strip()
                    if tmdb_id and imdb_id:
                        mapping[tmdb_id] = imdb_id
            
            print(f"✅ Loaded {len(mapping)} TMDb→IMDb mappings from CastInfo")
            return mapping
            
        except Exception as e:
            print(f"❌ Error loading CastInfo mapping: {e}")
            return {}
    
    def load_realiteaseinfo_mapping(self):
        """Load RealiteaseInfo sheet to create TMDb ID set for reality TV cast checking"""
        try:
            print("🔄 Loading RealiteaseInfo mapping...")
            realitease_data = self.realiteaseinfo_worksheet.get_all_values()
            
            if len(realitease_data) < 2:
                print("⚠️ RealiteaseInfo sheet is empty or has no data")
                return set()
            
            # Find TMDb column
            headers = [h.strip().lower() for h in realitease_data[0]]
            tmdb_col = None
            
            for i, header in enumerate(headers):
                if 'tmdb' in header and 'id' in header:
                    tmdb_col = i
                    break
            
            if tmdb_col is None:
                print(f"⚠️ Could not find TMDb column in RealiteaseInfo")
                return set()
            
            # Create set of TMDb IDs
            tmdb_set = set()
            for row in realitease_data[1:]:  # Skip header
                if len(row) > tmdb_col:
                    tmdb_id = row[tmdb_col].strip()
                    if tmdb_id:
                        tmdb_set.add(tmdb_id)
            
            print(f"✅ Loaded {len(tmdb_set)} TMDb IDs from RealiteaseInfo")
            return tmdb_set
            
        except Exception as e:
            print(f"❌ Error loading RealiteaseInfo mapping: {e}")
            return set()
    
    def get_all_wwhl_episodes(self):
        """Get all WWHL episodes from TMDb"""
        try:
            print(f"🔍 Getting all seasons for WWHL (TMDb ID: {self.WWHL_SHOW_ID})")
            
            # Get show details to see how many seasons
            show_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}"
            response = self.session.get(show_url)
            
            if response.status_code != 200:
                print(f"❌ Failed to get show details: {response.status_code}")
                return []
            
            show_data = response.json()
            total_seasons = show_data.get('number_of_seasons', 0)
            print(f"📺 WWHL has {total_seasons} seasons")
            
            all_episodes = []
            
            # Get episodes for each season
            for season_num in range(1, total_seasons + 1):
                print(f"📋 Getting episodes for Season {season_num}...")
                
                season_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_num}"
                response = self.session.get(season_url)
                
                if response.status_code != 200:
                    print(f"⚠️ Failed to get Season {season_num}: {response.status_code}")
                    continue
                
                season_data = response.json()
                episodes = season_data.get('episodes', [])
                
                for episode in episodes:
                    episode_data = {
                        'tmdb_id': episode.get('id'),
                        'season_number': season_num,
                        'episode_number': episode.get('episode_number'),
                        'air_date': episode.get('air_date'),
                        'name': episode.get('name'),
                        'guest_stars': episode.get('guest_stars', [])
                    }
                    all_episodes.append(episode_data)
                
                print(f"✅ Found {len(episodes)} episodes in Season {season_num}")
                time.sleep(0.1)  # Rate limiting
            
            print(f"🎉 Total episodes found: {len(all_episodes)}")
            return all_episodes
            
        except Exception as e:
            print(f"❌ Error getting WWHL episodes: {e}")
            return []
        """Get TV credits for a person from TMDb"""
        try:
            print(f"🔍 WWHL: Getting TV credits for {cast_name} (TMDb ID: {person_tmdb_id})")
            
            if not person_tmdb_id or not person_tmdb_id.strip():
                print(f"❌ No TMDB Person ID provided for {cast_name}")
                return None
            
            credits_url = f"{self.base_url}/person/{person_tmdb_id}/tv_credits"
            response = self.session.get(credits_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get TV credits for {cast_name}: {response.status_code}")
                return None
            
            credits_data = response.json()
            
            # Look for WWHL credits in both cast and crew
            cast_credits = []
            crew_credits = []
            
            # Check cast credits
            for credit in credits_data.get('cast', []):
                if str(credit.get('id')) == self.WWHL_SHOW_ID:
                    character = credit.get('character', '').lower()
                    # Look for "self" or guest appearances
                    if 'self' in character or 'guest' in character or character == '':
                        cast_credits.append(credit)
                        print(f"✅ WWHL: Found cast credit for {cast_name} - Character: {credit.get('character')}")
            
            # Check crew credits  
            for credit in credits_data.get('crew', []):
                if str(credit.get('id')) == self.WWHL_SHOW_ID:
                    job = credit.get('job', '').lower()
                    if 'guest' in job or 'self' in job:
                        crew_credits.append(credit)
                        print(f"✅ WWHL: Found crew credit for {cast_name} - Job: {credit.get('job')}")
            
            all_credits = cast_credits + crew_credits
            
            if not all_credits:
                print(f"⚠️ WWHL: No WWHL credits found for {cast_name}")
                return None
            
            return all_credits
            
        except Exception as e:
            print(f"❌ WWHL: Error getting TV credits for {cast_name}: {e}")
            return None

    def get_credit_details_improved(self, credit_id, cast_name):
        """Get detailed episode information from credit details"""
        try:
            print(f"🔍 WWHL: Getting credit details for {cast_name} (Credit ID: {credit_id})")
            credit_url = f"{self.base_url}/credit/{credit_id}"
            response = self.session.get(credit_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get credit details for {cast_name}: {response.status_code}")
                return None
            
            credit_data = response.json()
            
            episodes = []
            
            # Extract from episodes array (most reliable)
            episodes_array = credit_data.get('media', {}).get('episodes', [])
            if episodes_array:
                print(f"🎯 WWHL: Found {len(episodes_array)} episodes in episodes array")
                
                for episode in episodes_array:
                    episode_info = {
                        'season_number': episode.get('season_number'),
                        'episode_number': episode.get('episode_number'),
                        'air_date': episode.get('air_date'),
                        'name': episode.get('name', ''),
                        'episode_id': episode.get('id')
                    }
                    
                    # Only include episodes with valid season/episode numbers
                    if episode_info['season_number'] and episode_info['episode_number']:
                        episodes.append(episode_info)
                        print(f"📺 WWHL: Found episode S{episode_info['season_number']}E{episode_info['episode_number']} - {episode_info['name']}")
            
            return episodes if episodes else None
            
        except Exception as e:
            print(f"❌ WWHL: Error getting credit details for {cast_name}: {e}")
            return None

    def get_episode_cast(self, season_number, episode_number):
        """Get cast information for a specific WWHL episode"""
        try:
            print(f"🎭 WWHL: Getting cast for S{season_number}E{episode_number}")
            episode_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_number}/episode/{episode_number}/credits"
            response = self.session.get(episode_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get episode cast for S{season_number}E{episode_number}: {response.status_code}")
                return None, None
            
            cast_data = response.json()
            
            # Andy Cohen's TMDb ID (host - should be filtered out)
            ANDY_COHEN_TMDB_ID = 54772
            
            # Extract guest cast
            guest_cast = []
            
            # Check guest_stars array (primary source for WWHL guests)
            guest_stars = cast_data.get('guest_stars', [])
            print(f"🎯 WWHL: Found {len(guest_stars)} guest_stars in S{season_number}E{episode_number}")
            
            for guest in guest_stars:
                character = guest.get('character', '').lower()
                tmdb_id = guest.get('id')
                name = guest.get('name', '')
                
                # Include guests with "Self" roles or empty character field
                if 'self' in character or 'guest' in character or character == '':
                    guest_info = {
                        'name': name,
                        'tmdb_id': tmdb_id,
                        'character': guest.get('character', 'Self'),
                        'credit_id': guest.get('credit_id')
                    }
                    guest_cast.append(guest_info)
                    print(f"✅ WWHL: Added guest_star: {name} (TMDb: {tmdb_id})")
            
            # Also check regular cast array (excluding Andy Cohen)
            all_cast = cast_data.get('cast', [])
            
            for cast_member in all_cast:
                character = cast_member.get('character', '').lower()
                tmdb_id = cast_member.get('id')
                name = cast_member.get('name', '')
                
                # Skip Andy Cohen and check if not already in list
                if tmdb_id != ANDY_COHEN_TMDB_ID and not any(g.get('tmdb_id') == tmdb_id for g in guest_cast):
                    if 'self' in character or 'guest' in character or character == '':
                        guest_info = {
                            'name': name,
                            'tmdb_id': tmdb_id,
                            'character': cast_member.get('character', 'Self'),
                            'credit_id': cast_member.get('credit_id')
                        }
                        guest_cast.append(guest_info)
                        print(f"✅ WWHL: Added cast guest: {name} (TMDb: {tmdb_id})")
            
            print(f"👥 WWHL: Found {len(guest_cast)} total guests in S{season_number}E{episode_number}")
            return guest_cast, cast_data
            
        except Exception as e:
            print(f"❌ WWHL: Error getting episode cast for S{season_number}E{episode_number}: {e}")
            return None, None

    def get_imdb_ids_for_tmdb_ids(self, tmdb_ids):
        """Get IMDb IDs for a list of TMDb person IDs"""
        imdb_ids = []
        
        for tmdb_id in tmdb_ids:
            try:
                person_url = f"{self.base_url}/person/{tmdb_id}/external_ids"
                response = self.session.get(person_url)
                
                if response.status_code == 200:
                    external_data = response.json()
                    imdb_id = external_data.get('imdb_id', '')
                    imdb_ids.append(imdb_id)
                    if imdb_id:
                        print(f"✅ WWHL: Found IMDb ID {imdb_id} for TMDb {tmdb_id}")
                else:
                    imdb_ids.append("")
                
                time.sleep(0.3)  # Rate limiting
                
            except Exception as e:
                print(f"❌ WWHL: Error getting IMDb ID for TMDb {tmdb_id}: {e}")
                imdb_ids.append("")
        
        return imdb_ids

    def get_person_external_ids(self, tmdb_id):
        """Get external IDs (including IMDb) for a TMDb person ID"""
        try:
            person_url = f"{self.base_url}/person/{tmdb_id}/external_ids"
            response = self.session.get(person_url)
            
            if response.status_code == 200:
                external_data = response.json()
                return external_data.get('imdb_id', '')
            else:
                print(f"❌ Failed to get external IDs for TMDb {tmdb_id}: {response.status_code}")
                return ''
                
        except Exception as e:
            print(f"❌ Error getting external IDs for TMDb {tmdb_id}: {e}")
            return ''
            return ''

    def process_cast_member(self, cast_name, cast_imdb_id, person_tmdb_id):
        """Process a single cast member to find their WWHL appearances"""
        try:
            print(f"\n🎭 WWHL: Processing {cast_name} (IMDb: {cast_imdb_id}, TMDb: {person_tmdb_id})")
            
            # Get WWHL credits for this person
            wwhl_credits = self.get_person_tmdb_credits(person_tmdb_id, cast_name)
            
            if not wwhl_credits:
                return []
            
            appearances = []
            processed_episodes = set()  # Track processed episodes to avoid duplicates
            
            # Process each credit to get detailed episode information
            for credit in wwhl_credits:
                credit_id = credit.get('credit_id')
                episode_count = credit.get('episode_count', 0)
                
                if credit_id:
                    # Get detailed episode information
                    episodes = self.get_credit_details_improved(credit_id, cast_name)
                    
                    if episodes:
                        # Process each episode appearance
                        for episode in episodes:
                            season_num = episode['season_number']
                            episode_num = episode['episode_number']
                            
                            if not season_num or not episode_num:
                                continue
                            
                            # Skip if already processed this episode
                            episode_key = f"S{season_num}E{episode_num}"
                            if episode_key in processed_episodes:
                                continue
                            processed_episodes.add(episode_key)
                            
                            # Create episode marker
                            episode_marker = episode_key
                            
                            # Get episode date and IMDb ID
                            episode_date, episode_imdb_id = self.get_episode_details(season_num, episode_num)
                            
                            # Get other guests in this episode
                            episode_cast, _ = self.get_episode_cast(season_num, episode_num)
                            
                            # Filter out the current cast member and Andy Cohen from other guests
                            other_guests = []
                            if episode_cast:
                                for guest in episode_cast:
                                    guest_tmdb_id = guest.get('tmdb_id')
                                    # Exclude current cast member and Andy Cohen (54772)
                                    if (str(guest_tmdb_id) != str(person_tmdb_id) and 
                                        guest_tmdb_id != 54772):
                                        other_guests.append(guest)
                            
                            # Extract TMDb IDs, Names, and get IMDb IDs
                            other_tmdb_ids = [str(guest.get('tmdb_id', '')) for guest in other_guests if guest.get('tmdb_id')]
                            other_guest_names = [guest.get('name', '') for guest in other_guests if guest.get('name')]
                            other_imdb_ids = self.get_imdb_ids_for_tmdb_ids(other_tmdb_ids) if other_tmdb_ids else []
                            
                            # Format the lists
                            guest_names_str = ", ".join([name for name in other_guest_names if name and name.strip()])
                            tmdb_ids_str = ", ".join([tid for tid in other_tmdb_ids if tid and tid.strip()])
                            imdb_ids_str = ", ".join([iid for iid in other_imdb_ids if iid and iid.strip()])
                            
                            appearance = {
                                'cast_name': cast_name,
                                'cast_imdb_id': cast_imdb_id or '',
                                'cast_tmdb_id': person_tmdb_id,
                                'episode_marker': episode_marker,
                                'episode_imdb_id': episode_imdb_id or '',
                                'episode_date': episode_date or '',
                                'episode_credit_id': credit_id or '',
                                'other_guest_names': guest_names_str,
                                'other_imdb_ids': imdb_ids_str,
                                'other_tmdb_ids': tmdb_ids_str
                            }
                            
                            appearances.append(appearance)
                            print(f"✅ WWHL: Found appearance - {episode_marker} ({episode_date}) with {len(other_guests)} other guests")
                    else:
                        # Log if no detailed episodes found
                        if episode_count > 0:
                            print(f"⚠️ WWHL: No detailed episodes found for credit {credit_id}, but shows {episode_count} episodes")
                
                time.sleep(0.5)  # Rate limiting between credits
            
            return appearances
            
        except Exception as e:
            print(f"❌ WWHL: Error processing {cast_name}: {e}")
            return []

    def write_episodes_to_sheet(self, episodes):
        """Write WWHL episodes to the WWHLinfo sheet"""
        if not episodes:
            return
        
        try:
            # Get current data to find next empty row
            existing_data = self.wwhl_worksheet.get_all_values()
            next_row = len(existing_data) + 1
            
            # Prepare data for batch update
            rows_to_add = []
            for episode in episodes:
                row = [
                    episode['tmdb_id'],
                    episode['episode_marker'],
                    episode['season_number'],
                    episode['episode_number'],
                    episode['air_date'],
                    episode['guest_star_names'],
                    episode['guest_star_tmdb_ids'],
                    episode['guest_star_imdb_ids'],
                    episode['cast_source']
                ]
                rows_to_add.append(row)
            
            if rows_to_add:
                # Batch update with retry logic for rate limits
                max_retries = 3
                retry_delay = 60  # Start with 60 seconds
                
                for attempt in range(max_retries):
                    try:
                        range_name = f"A{next_row}:I{next_row + len(rows_to_add) - 1}"
                        self.wwhl_worksheet.update(range_name, rows_to_add)
                        print(f"✅ WWHL: Added {len(rows_to_add)} episodes to sheet")
                        time.sleep(1)  # Reduced delay after successful write
                        break  # Success, exit retry loop
                        
                    except Exception as write_error:
                        if "429" in str(write_error) or "Quota exceeded" in str(write_error):
                            if attempt < max_retries - 1:
                                print(f"⚠️ Rate limit hit, waiting {retry_delay} seconds before retry {attempt + 2}/{max_retries}...")
                                time.sleep(retry_delay)
                                retry_delay *= 2  # Exponential backoff
                            else:
                                print(f"❌ WWHL: Max retries reached for this batch. Skipping...")
                                break
                        else:
                            print(f"❌ WWHL: Write error: {write_error}")
                            break
            
        except Exception as e:
            print(f"❌ WWHL: Error preparing data: {e}")

    def fill_missing_data(self):
        """Fill missing IMDb IDs and Cast_Source data in existing rows"""
        try:
            print("🔄 Checking for missing data in existing episodes...")
            
            # Get all existing data
            all_data = self.wwhl_worksheet.get_all_values()
            if len(all_data) <= 1:
                print("📋 No existing data to update")
                return True
            
            headers = all_data[0]
            print(f"🔍 Found headers: {headers}")
            updates_needed = []
            
            # Find column indexes
            tmdb_ids_col = None
            imdb_ids_col = None
            cast_source_col = None
            episode_marker_col = None
            
            for i, header in enumerate(headers):
                if 'GuestStarTMDbIDs' in header:
                    tmdb_ids_col = i
                elif 'GuestStarIMDbIDs' in header:
                    imdb_ids_col = i
                elif 'Cast_Source' in header or 'CastSource' in header:
                    cast_source_col = i
                elif 'EpisodeMarker' in header:
                    episode_marker_col = i
            
            print(f"🔍 Column indexes - TMDb: {tmdb_ids_col}, IMDb: {imdb_ids_col}, Cast_Source: {cast_source_col}")
            
            # If Cast_Source column doesn't exist, add it
            if cast_source_col is None:
                print("📝 Adding missing Cast_Source column...")
                # Update the header row to add Cast_Source
                headers.append('Cast_Source')
                # Update with the correct range including the new column
                range_name = f'A1:{chr(ord("A") + len(headers) - 1)}1'
                self.wwhl_worksheet.update(range_name, [headers])
                cast_source_col = len(headers) - 1
                print(f"✅ Added Cast_Source column at index {cast_source_col}")
            
            if None in [tmdb_ids_col, imdb_ids_col]:
                print("⚠️ Could not find required columns for updates")
                return False
            
            print(f"🔍 Checking {len(all_data) - 1} existing episodes for missing data...")
            
            for row_idx, row in enumerate(all_data[1:], start=2):  # Start at row 2 (skip header)
                if len(row) <= max(tmdb_ids_col, imdb_ids_col, cast_source_col):
                    continue
                
                episode_marker = row[episode_marker_col] if episode_marker_col < len(row) else ""
                tmdb_ids_str = row[tmdb_ids_col] if tmdb_ids_col < len(row) else ""
                imdb_ids_str = row[imdb_ids_col] if imdb_ids_col < len(row) else ""
                cast_source = row[cast_source_col] if cast_source_col < len(row) else ""
                
                needs_update = False
                new_imdb_ids = ""
                new_cast_source = cast_source
                
                # Process TMDb IDs to find corresponding IMDb IDs
                if tmdb_ids_str and tmdb_ids_str.strip():
                    tmdb_ids = [tid.strip() for tid in tmdb_ids_str.split(',') if tid.strip()]
                    existing_imdb_ids = [iid.strip() for iid in imdb_ids_str.split(',') if iid.strip()] if imdb_ids_str else []
                    
                    # Only process if we have fewer IMDb IDs than TMDb IDs or no IMDb IDs at all
                    if len(existing_imdb_ids) < len(tmdb_ids) or not imdb_ids_str.strip():
                        print(f"🔧 Processing {episode_marker}: {len(tmdb_ids)} TMDb IDs, {len(existing_imdb_ids)} existing IMDb IDs")
                        
                        imdb_ids = []
                        cast_sources = []
                        
                        for i, tmdb_id in enumerate(tmdb_ids):
                                imdb_id = ""
                                cast_type = "NONE"
                                
                                # Use existing IMDb ID if available
                                if i < len(existing_imdb_ids) and existing_imdb_ids[i]:
                                    imdb_id = existing_imdb_ids[i]
                                    # Determine cast source for existing ID
                                    if tmdb_id in self.tmdb_to_imdb_mapping:
                                        cast_type = "CAST INFO"
                                    elif tmdb_id in self.realitease_tmdb_set:
                                        cast_type = "REALITEASE"
                                else:
                                    # Try CastInfo mapping first
                                    if tmdb_id in self.tmdb_to_imdb_mapping:
                                        imdb_id = self.tmdb_to_imdb_mapping[tmdb_id]
                                        cast_type = "CAST INFO"
                                        print(f"  ✅ Found in CastInfo: TMDb {tmdb_id} → IMDb {imdb_id}")
                                    else:
                                        # Try TMDb API as fallback
                                        try:
                                            imdb_id = self.get_person_external_ids(tmdb_id)
                                            if imdb_id and imdb_id.strip():
                                                print(f"  ✅ Found via TMDb API: TMDb {tmdb_id} → IMDb {imdb_id}")
                                                
                                                # Check if this person is in RealiteaseInfo
                                                if tmdb_id in self.realitease_tmdb_set:
                                                    cast_type = "REALITEASE"
                                                else:
                                                    cast_type = "NONE"
                                            else:
                                                print(f"  ❌ No IMDb ID found for TMDb {tmdb_id}")
                                        except Exception as e:
                                            print(f"  ❌ Error getting external IDs for TMDb {tmdb_id}: {e}")
                                
                                # Ensure imdb_id is never None
                                if imdb_id is None:
                                    imdb_id = ""
                                
                                imdb_ids.append(imdb_id)
                                cast_sources.append(cast_type)
                                time.sleep(0.1)  # Rate limiting
                        
                        # Update the row data - filter out empty IMDb IDs and join properly
                        filtered_imdb_ids = [iid for iid in imdb_ids if iid and iid.strip()]
                        if len(filtered_imdb_ids) == 1:
                            new_imdb_ids = filtered_imdb_ids[0]  # Single ID, no comma
                        elif len(filtered_imdb_ids) > 1:
                            new_imdb_ids = ", ".join(filtered_imdb_ids)  # Multiple IDs with comma-space
                        else:
                            new_imdb_ids = ""  # No valid IDs found
                        
                        # Determine overall cast source (priority: REALITEASE > CAST INFO > NONE)
                        if "REALITEASE" in cast_sources:
                            new_cast_source = "REALITEASE"
                        elif "CAST INFO" in cast_sources:
                            new_cast_source = "CAST INFO"
                        else:
                            new_cast_source = "NONE"
                        
                        # Check if this is actually an update
                        if new_imdb_ids != imdb_ids_str or new_cast_source != cast_source:
                            needs_update = True
                            print(f"  📝 Update needed: IMDb IDs: '{imdb_ids_str}' → '{new_imdb_ids}', Cast Source: '{cast_source}' → '{new_cast_source}'")
                
                # Check if Cast_Source needs to be set (even if no IMDb ID updates needed)
                if not cast_source or cast_source.strip() == "":
                    if tmdb_ids_str and tmdb_ids_str.strip():
                        tmdb_ids = [tid.strip() for tid in tmdb_ids_str.split(',') if tid.strip()]
                        cast_sources = []
                        
                        for tmdb_id in tmdb_ids:
                            if tmdb_id in self.tmdb_to_imdb_mapping:
                                cast_sources.append("CAST INFO")
                            elif tmdb_id in self.realitease_tmdb_set:
                                cast_sources.append("REALITEASE")
                            else:
                                cast_sources.append("NONE")
                        
                        # Determine overall cast source (priority: REALITEASE > CAST INFO > NONE)
                        if "REALITEASE" in cast_sources:
                            new_cast_source = "REALITEASE"
                        elif "CAST INFO" in cast_sources:
                            new_cast_source = "CAST INFO"
                        else:
                            new_cast_source = "NONE"
                        
                        needs_update = True
                        print(f"  📝 Setting Cast Source for {episode_marker}: '{cast_source}' → '{new_cast_source}'")
                    else:
                        # No guests, set to NONE
                        new_cast_source = "NONE"
                        needs_update = True
                        print(f"  📝 Setting Cast Source for {episode_marker} (no guests): '{cast_source}' → 'NONE'")
                
                # Add to updates if needed
                if needs_update:
                    # Ensure row has enough columns
                    while len(row) <= cast_source_col:
                        row.append("")
                    
                    row[imdb_ids_col] = new_imdb_ids
                    row[cast_source_col] = new_cast_source
                    
                    updates_needed.append({
                        'range': f'A{row_idx}:{chr(ord("A") + len(row) - 1)}{row_idx}',
                        'values': [row]
                    })
                    
                    # Apply updates in smaller batches to avoid memory issues
                    if len(updates_needed) >= 10:  # Write every 10 updates
                        try:
                            for update in updates_needed:
                                self.wwhl_worksheet.update(update['range'], update['values'])
                                time.sleep(0.2)  # Rate limiting
                            print(f"✅ Applied batch of {len(updates_needed)} updates")
                            updates_needed = []  # Clear the batch
                        except Exception as e:
                            print(f"❌ Error applying batch updates: {e}")
                            time.sleep(1)
            
            # Apply updates in batches if any are needed
            if updates_needed:
                print(f"📝 Applying {len(updates_needed)} updates to sheet...")
                batch_size = 50
                
                for i in range(0, len(updates_needed), batch_size):
                    batch = updates_needed[i:i + batch_size]
                    try:
                        # Apply individual updates instead of batch_update
                        for update in batch:
                            self.wwhl_worksheet.update(update['range'], update['values'])
                            time.sleep(0.2)  # Rate limiting
                        
                        print(f"  ✅ Updated batch {i//batch_size + 1}/{(len(updates_needed) + batch_size - 1)//batch_size}")
                        time.sleep(1)  # Rate limiting between batches
                        
                    except Exception as e:
                        print(f"  ❌ Error updating batch: {e}")
                        # Retry with exponential backoff
                        for retry in range(3):
                            try:
                                time.sleep(2 ** retry)
                                for update in batch:
                                    self.wwhl_worksheet.update(update['range'], update['values'])
                                    time.sleep(0.2)
                                print(f"  ✅ Retry successful for batch {i//batch_size + 1}")
                                break
                            except Exception as retry_e:
                                print(f"  ❌ Retry {retry + 1} failed: {retry_e}")
                                if retry == 2:
                                    print(f"  ❌ Giving up on batch {i//batch_size + 1}")
                
                print(f"✅ Successfully updated {len(updates_needed)} episodes with missing data!")
            else:
                print("✅ No missing data found - all episodes are complete!")
            
            return True
            
        except Exception as e:
            print(f"❌ Error filling missing data: {e}")
            return False

    def process_all_episodes(self):
        """Process all WWHL episodes and extract guest information"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Load CastInfo mapping for TMDb→IMDb conversion
            self.tmdb_to_imdb_mapping = self.load_castinfo_mapping()
            
            # Load RealiteaseInfo TMDb IDs for reality TV cast checking
            self.realitease_tmdb_set = self.load_realiteaseinfo_mapping()
            
            # Check existing episodes to resume from where we left off
            existing_data = self.wwhl_worksheet.get_all_values()
            existing_episodes = set()
            if len(existing_data) > 1:  # Has data beyond headers
                for row in existing_data[1:]:  # Skip header
                    if len(row) > 1:  # Has episode marker
                        existing_episodes.add(row[1])  # Episode marker is column B
            
            print(f"📋 Found {len(existing_episodes)} existing episodes in sheet")
            
            # Get all WWHL episodes
            all_episodes = self.get_all_wwhl_episodes()
            
            if not all_episodes:
                print("❌ No episodes found")
                return False
            
            # Filter out episodes that already exist
            new_episodes = []
            for episode in all_episodes:
                episode_marker = f"S{episode['season_number']}E{episode['episode_number']}"
                if episode_marker not in existing_episodes:
                    new_episodes.append(episode)
            
            print(f"📊 Total episodes available: {len(all_episodes)}")
            print(f"📊 New episodes to process: {len(new_episodes)}")
            
            if not new_episodes:
                print("✅ All episodes already processed!")
                # Still run missing data check even if no new episodes
                print(f"\n🔧 Checking existing episodes for missing data...")
                self.fill_missing_data()
                return True
            
            processed_episodes = []
            batch_episodes = []
            batch_size = 50  # Increased batch size for faster processing
            
            self.processed_count = 0
            
            for episode in new_episodes:
                self.processed_count += 1
                
                # Create episode marker
                episode_marker = f"S{episode['season_number']}E{episode['episode_number']}"
                
                # Get guest stars from episode data
                guest_stars = episode.get('guest_stars', [])
                guest_names = []
                guest_tmdb_ids = []
                guest_imdb_ids = []
                
                # Track cast sources for this episode
                has_realitease_cast = False
                has_castinfo_only = False
                
                for guest in guest_stars:
                    name = guest.get('name', '').strip()
                    tmdb_id = str(guest.get('id', '')).strip()
                    
                    if name and tmdb_id:
                        guest_names.append(name)
                        guest_tmdb_ids.append(tmdb_id)
                        
                        # First try to get IMDb ID from CastInfo mapping
                        imdb_id = self.tmdb_to_imdb_mapping.get(tmdb_id, '')
                        
                        # If not found in mapping, get it from TMDb API
                        if not imdb_id:
                            try:
                                person_url = f"{self.base_url}/person/{tmdb_id}/external_ids"
                                response = self.session.get(person_url)
                                if response.status_code == 200:
                                    external_data = response.json()
                                    imdb_id = external_data.get('imdb_id', '')
                                time.sleep(0.1)  # Rate limiting
                            except:
                                imdb_id = ''
                        
                        guest_imdb_ids.append(imdb_id)
                        
                        # Check cast source
                        if tmdb_id in self.realitease_tmdb_set:
                            has_realitease_cast = True
                        elif imdb_id:  # Has IMDb ID from CastInfo but not in RealiteaseInfo
                            has_castinfo_only = True
                
                # Determine cast source
                if has_realitease_cast:
                    cast_source = "REALITEASE"
                elif has_castinfo_only:
                    cast_source = "CAST INFO"
                else:
                    cast_source = "NONE"
                
                episode_data = {
                    'tmdb_id': episode['tmdb_id'] or '',
                    'episode_marker': episode_marker,
                    'season_number': episode['season_number'],
                    'episode_number': episode['episode_number'],
                    'air_date': episode['air_date'] or '',
                    'guest_star_names': ', '.join(guest_names),
                    'guest_star_tmdb_ids': ', '.join(guest_tmdb_ids),
                    'guest_star_imdb_ids': ', '.join([iid for iid in guest_imdb_ids if iid and iid.strip()]),
                    'cast_source': cast_source
                }
                
                batch_episodes.append(episode_data)
                processed_episodes.append(episode_data)
                
                # Process in batches
                if len(batch_episodes) >= batch_size:
                    self.write_episodes_to_sheet(batch_episodes)
                    batch_episodes = []
                    print(f"📈 WWHL Progress: {self.processed_count}/{len(new_episodes)} new episodes processed")
                    
                    # Minimal delay between batches for speed
                    if self.processed_count % 200 == 0:  # Longer pause every 200 episodes
                        print("⏸️ Brief pause to respect API limits...")
                        time.sleep(10)
                    # No delay for normal batches - let rate limiting handle it
            
            # Write any remaining episodes
            if batch_episodes:
                self.write_episodes_to_sheet(batch_episodes)
            
            # Fill any missing data in existing episodes
            print(f"\n🔧 Checking existing episodes for missing data...")
            self.fill_missing_data()
            
            # Final summary
            print(f"\n🎉 WWHL: Processing complete!")
            print(f"📊 New episodes processed: {self.processed_count}")
            print(f"✅ Episodes with guest stars: {sum(1 for ep in processed_episodes if ep['guest_star_names'])}")
            print(f"📋 Total episodes now in sheet: {len(existing_episodes) + self.processed_count}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing episodes: {e}")
            import traceback
            traceback.print_exc()
            return False

    def process_all_cast(self):
        """Process all cast members from ViableCast sheet to find WWHL appearances"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from ViableCast sheet
            all_data = self.source_worksheet.get_all_values()
            
            if len(all_data) < 2:
                print(f"❌ RealiteaseInfo sheet only has {len(all_data)} rows, need at least 2 (header + data)")
                return False
            
            # Dynamic header mapping
            headers = [h.strip().lower() for h in all_data[0]]
            header_map = {h: i for i, h in enumerate(headers)}
            print(f"🔍 Available headers in RealiteaseInfo: {headers}")
            
            def col_of(fragment, default=None):
                for k, idx in header_map.items():
                    if fragment in k:
                        return idx
                return default
            
            # Find column indexes dynamically
            idx_cast_name = col_of("castname")
            idx_cast_imdb = col_of("castimdbid") 
            idx_cast_tmdb = col_of("casttmdbid")  # TMDb Person ID
            
            if idx_cast_name is None or idx_cast_tmdb is None:
                print("❌ Header lookup failed. Ensure headers 'CastName' and 'CastID' exist in RealiteaseInfo sheet.")
                return False
            
            print(f"📊 WWHL: Processing cast members from RealiteaseInfo sheet")
            print(f"📊 WWHL: Found columns - CastName: {idx_cast_name}, Cast IMDbID: {idx_cast_imdb}, CastID (TMDb): {idx_cast_tmdb}")
            print(f"📊 WWHL: Total rows to check: {len(all_data) - 1}")
            
            batch_appearances = []
            
            # Process each row
            for row_num in range(1, len(all_data)):  # Skip header row
                row = all_data[row_num]
                
                self.processed_count += 1
                
                # Parse row data using dynamic column indexes
                def safe_get(i):
                    return row[i].strip() if i is not None and i < len(row) else ""
                
                cast_name = safe_get(idx_cast_name)
                cast_imdb_id = safe_get(idx_cast_imdb) 
                person_tmdb_id = safe_get(idx_cast_tmdb)
                
                # Validate required data
                if not all([cast_name, person_tmdb_id]):
                    print(f"⚠️ WWHL: Row {row_num + 1} - missing required data")
                    self.skipped_count += 1
                    continue
                
                # Process this cast member for WWHL appearances
                appearances = self.process_cast_member(cast_name, cast_imdb_id, person_tmdb_id)
                
                if appearances:
                    batch_appearances.extend(appearances)
                    self.found_count += len(appearances)
                    print(f"✅ WWHL: Found {len(appearances)} WWHL appearances for {cast_name}")
                else:
                    self.skipped_count += 1
                
                # Write batch every 10 appearances
                if len(batch_appearances) >= 10:
                    self.write_appearances_to_sheet(batch_appearances)
                    batch_appearances = []
                
                # Add delay between cast members
                time.sleep(1.5)
                
                # Progress update every 10 cast members
                if self.processed_count % 10 == 0:
                    success_rate = (self.found_count / self.processed_count * 100) if self.processed_count > 0 else 0
                    print(f"📈 WWHL Progress: {self.processed_count} processed, {self.found_count} appearances found, Success rate: {success_rate:.1f}%")
            
            # Write any remaining appearances
            if batch_appearances:
                self.write_appearances_to_sheet(batch_appearances)
            
            # Final summary
            print(f"\n🎉 WWHL: Processing complete!")
            print(f"📊 Total cast members processed: {self.processed_count}")
            print(f"✅ WWHL appearances found: {self.found_count}")
            print(f"⏭️ Cast members skipped (no WWHL): {self.skipped_count}")
            
            # Display cache efficiency
            print(f"📦 Episode cache hits: {len(self.episode_cache)} unique episodes cached")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing cast members: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main function"""
    extractor = WWHLExtractor()
    
    print("🚀 Starting Enhanced WWHL Episode Extractor...")
    print(f"� Getting all episodes from WWHL (Show ID: {extractor.WWHL_SHOW_ID})")
    print(f"📅 Will extract episode details, air dates, and guest stars")
    print(f"📝 Results will be written to WWHLinfo sheet with 8 columns:")
    print(f"   • EpisodeTMDbID, EpisodeMarker, Season, Episode")
    print(f"   • AirDate, GuestStarNames, GuestStarTMDbIDs, GuestStarIMDbIDs")
    print("="*60)
    
    success = extractor.process_all_episodes()
    
    if success:
        print("\n✅ WWHL episode extraction completed successfully!")
    else:
        print("\n❌ WWHL episode extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
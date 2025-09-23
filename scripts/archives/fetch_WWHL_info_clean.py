import requests
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from datetime import datetime
import csv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WWHLEpisodeFetcher:
    def __init__(self):
        import os
        from dotenv import load_dotenv
        
        # Load environment variables
        load_dotenv()
        
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        
        if not self.tmdb_bearer and not self.tmdb_api_key:
            raise ValueError("Neither TMDB_BEARER nor TMDB_API_KEY found in environment variables")
        
        self.base_url = "https://api.themoviedb.org/3"
        self.WWHL_SHOW_ID = 22980  # Watch What Happens Live with Andy Cohen TMDb ID
        
        # Setup session with proper headers
        self.session = requests.Session()
        if self.tmdb_bearer:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_bearer}',
                'Content-Type': 'application/json'
            })
        else:
            self.session.params = {'api_key': self.tmdb_api_key}
        
        # Google Sheets setup
        self.gc = None
        self.spreadsheet = None
        self.wwhl_worksheet = None
        self.castinfo_worksheet = None
        self.realiteaseinfo_worksheet = None
        
        # Mappings
        self.tmdb_to_imdb_mapping = {}
        self.realitease_tmdb_set = set()
        
        print("🎬 WWHL Episode Fetcher initialized")
        print(f"📺 Target Show ID: {self.WWHL_SHOW_ID}")

    def setup_google_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            print("🔗 Setting up Google Sheets connection...")
            
            import os
            
            # Load credentials from the service account file
            key_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'keys', 'trr-backend-df2c438612e1.json')
            creds = Credentials.from_service_account_file(
                key_file_path,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            
            # Authorize gspread
            self.gc = gspread.authorize(creds)
            
            # Open the spreadsheet
            self.spreadsheet = self.gc.open("Realitease2025Data")
            print("✅ Connected to Realitease2025Data spreadsheet")
            
            # Get the worksheets
            try:
                self.wwhl_worksheet = self.spreadsheet.worksheet("WWHLinfo")
                print("✅ Found WWHLinfo worksheet")
            except gspread.WorksheetNotFound:
                print("📝 Creating WWHLinfo worksheet...")
                self.wwhl_worksheet = self.spreadsheet.add_worksheet(title="WWHLinfo", rows=3000, cols=10)
                print("✅ Created WWHLinfo worksheet")
            
            try:
                self.castinfo_worksheet = self.spreadsheet.worksheet("CastInfo")
                print("✅ Found CastInfo worksheet")
            except gspread.WorksheetNotFound:
                print("⚠️ CastInfo worksheet not found - TMDb→IMDb mapping will be empty")
                self.castinfo_worksheet = None
            
            try:
                self.realiteaseinfo_worksheet = self.spreadsheet.worksheet("RealiteaseInfo")
                print("✅ Found RealiteaseInfo worksheet")
            except gspread.WorksheetNotFound:
                print("⚠️ RealiteaseInfo worksheet not found - Reality TV cast detection will be disabled")
                self.realiteaseinfo_worksheet = None
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to setup Google Sheets: {e}")
            return False

    def load_castinfo_mapping(self):
        """Load CastInfo sheet to create TMDb→IMDb mapping"""
        try:
            print("🔄 Loading CastInfo mapping...")
            if not self.castinfo_worksheet:
                print("⚠️ CastInfo worksheet not available")
                return {}
            
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
            if not self.realiteaseinfo_worksheet:
                print("⚠️ RealiteaseInfo worksheet not available")
                return set()
                
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

    def get_episode_details(self, season_number, episode_number):
        """Get detailed information about a specific WWHL episode"""
        try:
            url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_number}/episode/{episode_number}"
            response = self.session.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ Episode S{season_number}E{episode_number} not found: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting episode S{season_number}E{episode_number}: {e}")
            return None

    def get_credit_details_improved(self, credit_id, cast_name):
        """Get detailed credit information with person TMDb ID"""
        try:
            url = f"{self.base_url}/credit/{credit_id}"
            response = self.session.get(url)
            
            if response.status_code == 200:
                credit_data = response.json()
                person = credit_data.get('person', {})
                person_tmdb_id = person.get('id')
                person_name = person.get('name', cast_name)
                
                return {
                    'person_tmdb_id': person_tmdb_id,
                    'person_name': person_name
                }
            else:
                print(f"⚠️ Failed to get credit details for {cast_name}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting credit details for {cast_name}: {e}")
            return None

    def get_episode_cast(self, season_number, episode_number):
        """Get cast information for a specific episode"""
        try:
            # Get guest stars from episode details
            episode_details = self.get_episode_details(season_number, episode_number)
            if not episode_details:
                return []
            
            guest_stars = episode_details.get('guest_stars', [])
            cast_info = []
            
            for guest in guest_stars:
                guest_name = guest.get('name', 'Unknown')
                credit_id = guest.get('credit_id')
                
                # Get detailed credit information to get person TMDb ID
                credit_details = self.get_credit_details_improved(credit_id, guest_name)
                
                if credit_details:
                    cast_info.append({
                        'name': credit_details['person_name'],
                        'tmdb_id': credit_details['person_tmdb_id'],
                        'character': guest.get('character', ''),
                        'order': guest.get('order', 999)
                    })
                else:
                    # Fallback if credit details fail
                    cast_info.append({
                        'name': guest_name,
                        'tmdb_id': None,
                        'character': guest.get('character', ''),
                        'order': guest.get('order', 999)
                    })
                
                # Rate limiting
                time.sleep(0.1)
            
            return cast_info
            
        except Exception as e:
            print(f"❌ Error getting cast for S{season_number}E{episode_number}: {e}")
            return []

    def get_person_external_ids(self, tmdb_id):
        """Get external IDs (including IMDb) for a person from TMDb"""
        try:
            url = f"{self.base_url}/person/{tmdb_id}/external_ids"
            response = self.session.get(url)
            
            if response.status_code == 200:
                external_ids = response.json()
                imdb_id = external_ids.get('imdb_id', '')
                return imdb_id if imdb_id else None
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error getting external IDs for TMDb ID {tmdb_id}: {e}")
            return None

    def get_imdb_ids_for_tmdb_ids(self, tmdb_ids):
        """Convert TMDb IDs to IMDb IDs using CastInfo mapping and TMDb API"""
        imdb_ids = []
        
        for tmdb_id in tmdb_ids:
            imdb_id = ""
            
            # First check CastInfo mapping
            if str(tmdb_id) in self.tmdb_to_imdb_mapping:
                imdb_id = self.tmdb_to_imdb_mapping[str(tmdb_id)]
                print(f"🔗 Found {tmdb_id} → {imdb_id} in CastInfo mapping")
            else:
                # Try TMDb API
                try:
                    api_imdb_id = self.get_person_external_ids(tmdb_id)
                    if api_imdb_id:
                        imdb_id = api_imdb_id
                        print(f"🌐 Found {tmdb_id} → {imdb_id} via TMDb API")
                    time.sleep(0.1)  # Rate limiting
                except:
                    print(f"⚠️ Could not find IMDb ID for TMDb ID {tmdb_id}")
            
            imdb_ids.append(imdb_id)
        
        return imdb_ids

    def process_cast_member(self, cast_name, cast_imdb_id, person_tmdb_id):
        """Process a cast member and determine their source category"""
        cast_source = "NONE"
        
        # Check if person is in CastInfo (regular cast)
        if str(person_tmdb_id) in self.tmdb_to_imdb_mapping:
            cast_source = "CAST INFO"
        # Check if person is in RealiteaseInfo (reality TV cast)
        elif str(person_tmdb_id) in self.realitease_tmdb_set:
            cast_source = "REALITEASE"
        else:
            cast_source = "NONE"
        
        return {
            'name': cast_name,
            'imdb_id': cast_imdb_id,
            'tmdb_id': person_tmdb_id,
            'cast_source': cast_source
        }

    def write_episodes_to_sheet(self, episodes):
        """Write episode data to Google Sheets"""
        try:
            print(f"📝 Writing {len(episodes)} episodes to Google Sheets...")
            
            # Prepare headers
            headers = [
                'TMDbID', 'EpisodeMarker', 'Season', 'Episode', 'AirDate', 
                'GuestNames', 'GuestStarTMDbIDs', 'GuestStarIMDbIDs', 'Cast_Source'
            ]
            
            # Check if sheet has headers
            try:
                existing_data = self.wwhl_worksheet.get_all_values()
                if not existing_data or existing_data[0] != headers:
                    # Add headers if they don't exist or are different
                    if not existing_data:
                        self.wwhl_worksheet.append_row(headers)
                        print("✅ Added headers to worksheet")
            except:
                # Sheet is empty, add headers
                self.wwhl_worksheet.append_row(headers)
                print("✅ Added headers to worksheet")
            
            # Prepare data rows
            rows = []
            for episode in episodes:
                season_num = episode['season_number']
                episode_num = episode['episode_number']
                episode_marker = f"S{season_num}E{episode_num}"
                air_date = episode['air_date'] or ""
                
                # Get cast information
                cast_info = self.get_episode_cast(season_num, episode_num)
                
                # Extract cast data
                guest_names = []
                guest_tmdb_ids = []
                guest_imdb_ids = []
                cast_sources = []
                
                for cast_member in cast_info:
                    if cast_member['tmdb_id']:
                        processed_member = self.process_cast_member(
                            cast_member['name'], 
                            "", # IMDb ID will be filled later
                            cast_member['tmdb_id']
                        )
                        
                        guest_names.append(processed_member['name'])
                        guest_tmdb_ids.append(str(processed_member['tmdb_id']))
                        cast_sources.append(processed_member['cast_source'])
                
                # Convert TMDb IDs to IMDb IDs
                if guest_tmdb_ids:
                    imdb_ids = self.get_imdb_ids_for_tmdb_ids(guest_tmdb_ids)
                    guest_imdb_ids = imdb_ids
                
                # Determine overall cast source (priority: REALITEASE > CAST INFO > NONE)
                overall_cast_source = "NONE"
                if "REALITEASE" in cast_sources:
                    overall_cast_source = "REALITEASE"
                elif "CAST INFO" in cast_sources:
                    overall_cast_source = "CAST INFO"
                
                # Format for spreadsheet
                guest_names_str = ", ".join(guest_names) if guest_names else ""
                guest_tmdb_ids_str = ", ".join(guest_tmdb_ids) if guest_tmdb_ids else ""
                guest_imdb_ids_str = ", ".join([iid for iid in guest_imdb_ids if iid]) if guest_imdb_ids else ""
                
                row = [
                    22980,  # TMDbID (correct WWHL ID)
                    episode_marker,     # EpisodeMarker
                    season_num,         # Season
                    episode_num,        # Episode
                    air_date,          # AirDate
                    guest_names_str,    # GuestNames
                    guest_tmdb_ids_str, # GuestStarTMDbIDs
                    guest_imdb_ids_str, # GuestStarIMDbIDs
                    overall_cast_source # Cast_Source
                ]
                
                rows.append(row)
                print(f"✅ Processed {episode_marker}: {len(guest_names)} guests, Cast Source: {overall_cast_source}")
            
            # Batch write all rows
            if rows:
                self.wwhl_worksheet.append_rows(rows)
                print(f"🎉 Successfully wrote {len(rows)} episodes to Google Sheets!")
            
            return True
            
        except Exception as e:
            print(f"❌ Error writing episodes to sheet: {e}")
            return False

    def fetch_missing_seasons(self, missing_seasons):
        """Fetch episodes for missing seasons"""
        try:
            print(f"🚀 Fetching episodes for missing seasons: {missing_seasons}")
            
            new_episodes = []
            for season_num in missing_seasons:
                print(f"📋 Getting episodes for Season {season_num}...")
                
                season_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_num}"
                response = self.session.get(season_url)
                
                if response.status_code != 200:
                    print(f"⚠️ Failed to get Season {season_num}: {response.status_code}")
                    continue
                
                season_data = response.json()
                episodes = season_data.get('episodes', [])
                
                if len(episodes) == 0:
                    print(f"⚠️ Season {season_num} has no episodes")
                    continue
                
                print(f"📊 Found {len(episodes)} episodes in Season {season_num}")
                
                for episode in episodes:
                    episode_info = {
                        'season_number': season_num,
                        'episode_number': episode.get('episode_number'),
                        'air_date': episode.get('air_date'),
                        'episode_name': episode.get('name', ''),
                        'overview': episode.get('overview', ''),
                        'guest_stars': episode.get('guest_stars', [])
                    }
                    new_episodes.append(episode_info)
                
                # Rate limiting between seasons
                time.sleep(0.5)
            
            print(f"🎉 Found {len(new_episodes)} new episodes from missing seasons")
            return new_episodes
            
        except Exception as e:
            print(f"❌ Error fetching missing seasons: {e}")
            return []

    def process_all_episodes_and_missing_seasons(self):
        """Process all missing seasons and episodes"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Load CastInfo mapping for TMDb→IMDb conversion
            self.tmdb_to_imdb_mapping = self.load_castinfo_mapping()
            
            # Load RealiteaseInfo TMDb IDs for reality TV cast checking
            self.realitease_tmdb_set = self.load_realiteaseinfo_mapping()
            
            # Check existing episodes to see what we have
            existing_data = self.wwhl_worksheet.get_all_values()
            existing_episodes = set()
            episodes_by_season = {}
            
            if len(existing_data) > 1:  # Has data beyond headers
                for row in existing_data[1:]:  # Skip header
                    if len(row) > 2:  # Has season and episode data
                        try:
                            season_num = int(row[2])  # Season column
                            episode_num = int(row[3])  # Episode column
                            episode_marker = row[1] if len(row) > 1 else ""  # Episode marker
                            existing_episodes.add(episode_marker)
                            
                            if season_num not in episodes_by_season:
                                episodes_by_season[season_num] = set()
                            episodes_by_season[season_num].add(episode_num)
                        except:
                            pass
            
            print(f"📋 Found {len(existing_episodes)} existing episodes in sheet")
            
            # Now check which seasons are complete vs incomplete
            complete_seasons = set()
            incomplete_seasons = []
            missing_seasons = []
            
            for season_num in range(1, 23):  # Check seasons 1-22
                if season_num not in episodes_by_season:
                    missing_seasons.append(season_num)
                    continue
                
                # Get the actual episode count for this season from TMDb
                season_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_num}"
                response = self.session.get(season_url)
                
                if response.status_code == 200:
                    season_data = response.json()
                    tmdb_episodes = season_data.get('episodes', [])
                    expected_episode_count = len(tmdb_episodes)
                    actual_episode_count = len(episodes_by_season[season_num])
                    
                    if actual_episode_count == expected_episode_count:
                        complete_seasons.add(season_num)
                        print(f"✅ Season {season_num}: Complete ({actual_episode_count}/{expected_episode_count} episodes)")
                    else:
                        incomplete_seasons.append((season_num, actual_episode_count, expected_episode_count))
                        print(f"⚠️ Season {season_num}: Incomplete ({actual_episode_count}/{expected_episode_count} episodes)")
                else:
                    print(f"❌ Could not verify Season {season_num} from TMDb")
                
                time.sleep(0.1)  # Rate limiting
            
            print(f"📊 Complete seasons: {sorted(list(complete_seasons))}")
            print(f"📊 Incomplete seasons: {[s[0] for s in incomplete_seasons]}")
            print(f"📊 Missing seasons: {missing_seasons}")
            
            # Collect all seasons that need processing (missing + incomplete)
            seasons_to_process = missing_seasons + [s[0] for s in incomplete_seasons]
            
            
            if seasons_to_process:
                print(f"🚀 Processing seasons: {sorted(seasons_to_process)}")
                
                # For incomplete seasons, we need to get all episodes and filter out existing ones
                all_new_episodes = []
                
                for season_num in sorted(seasons_to_process):
                    print(f"� Getting episodes for Season {season_num}...")
                    
                    season_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_num}"
                    response = self.session.get(season_url)
                    
                    if response.status_code != 200:
                        print(f"⚠️ Failed to get Season {season_num}: {response.status_code}")
                        continue
                    
                    season_data = response.json()
                    episodes = season_data.get('episodes', [])
                    
                    if len(episodes) == 0:
                        print(f"⚠️ Season {season_num} has no episodes")
                        continue
                    
                    # Filter out episodes that already exist
                    new_episodes_for_season = []
                    for episode in episodes:
                        episode_marker = f"S{season_num}E{episode.get('episode_number')}"
                        if episode_marker not in existing_episodes:
                            episode_info = {
                                'season_number': season_num,
                                'episode_number': episode.get('episode_number'),
                                'air_date': episode.get('air_date'),
                                'episode_name': episode.get('name', ''),
                                'overview': episode.get('overview', ''),
                                'guest_stars': episode.get('guest_stars', [])
                            }
                            new_episodes_for_season.append(episode_info)
                    
                    print(f"📊 Season {season_num}: {len(new_episodes_for_season)} new episodes to process (out of {len(episodes)} total)")
                    all_new_episodes.extend(new_episodes_for_season)
                    
                    # Rate limiting between seasons
                    time.sleep(0.5)
                
                print(f"🎉 Found {len(all_new_episodes)} new episodes to process")
                
                if all_new_episodes:
                    # Process episodes in batches
                    batch_size = 50
                    for i in range(0, len(all_new_episodes), batch_size):
                        batch = all_new_episodes[i:i + batch_size]
                        batch_num = i//batch_size + 1
                        print(f"\n🔄 Processing batch {batch_num}: episodes {i+1}-{min(i+batch_size, len(all_new_episodes))}")
                        
                        if not self.write_episodes_to_sheet(batch):
                            print(f"❌ Failed to write batch {batch_num}")
                            return False
                        
                        # Longer pause between batches
                        if i + batch_size < len(all_new_episodes):
                            print("⏳ Pausing 30 seconds between batches...")
                            time.sleep(30)
                    
                    print("✅ All missing and incomplete seasons processed!")
                    
            else:
                print("✅ All seasons 1-22 are complete in the sheet!")
            
            print("🎉 Episode processing completed!")
            return True
            
        except Exception as e:
            print(f"❌ Error in process_all_episodes_and_missing_seasons: {e}")
            return False

def main():
    """Main execution function"""
    print("🎬 Starting WWHL Episode Data Collection...")
    
    fetcher = WWHLEpisodeFetcher()
    
    # Process all missing seasons and episodes
    success = fetcher.process_all_episodes_and_missing_seasons()
    
    if success:
        print("🎉 WWHL episode processing completed successfully!")
    else:
        print("❌ WWHL episode processing failed")

if __name__ == "__main__":
    main()

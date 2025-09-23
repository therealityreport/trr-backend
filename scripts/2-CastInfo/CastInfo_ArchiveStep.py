#!/usr/bin/env python3
# v6UniversalSeasonExtractorCastInfo_Parallel_Full6.py
"""
v6 Universal Season Extractor - Optimized Page Reuse

Key Optimization:
- Loads each show page ONCE and processes ALL cast members without reloading
- Caches modal data when possible
- Minimizes page navigations
- Better detection of when we need to reload vs when we can reuse
"""

print("🚀 Starting v6 Optimized Page-Reuse CastInfo Season Extractor!")

import os
import sys
import time
import threading
import random
import re
import uuid
import shutil
import gspread
import traceback
import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

print("🔍 Basic imports successful!")

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException,
    StaleElementReferenceException
)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

print("🔍 All imports successful!")

# ---------- Configuration ----------
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GSPREAD_SERVICE_ACCOUNT",
    "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
)
WORKBOOK_NAME = "Realitease2025Data"
SHEET_NAME = "CastInfo"

# Parallel processing configuration
NUM_BROWSERS = 6
SHOW_BATCH_SIZE = 1

# Fixed columns for CastInfo
COL_G_EPISODES = 7  # TotalEpisodes
COL_H_SEASONS = 8   # Seasons

# Optimized timeouts - REDUCED for speed
PAGE_LOAD_TIMEOUT = 15
MODAL_TIMEOUT = 8  # Reduced from 12
ELEMENT_TIMEOUT = 5

# Missing data indicator
MISSING_DATA_MARKER = "**"

# ========== Results Manager (Same as v5) ==========
class ResultsManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.results_lock = threading.Lock()
        self.pending_show_updates = {}
        self.pending_deletions = []
        self.processed_count = 0
        self.deleted_crew = 0
        self.errors = 0
        self.missing_data_count = 0
        
    def add_show_batch(self, show_name, show_updates):
        """Add a complete show's updates and immediately write to sheets."""
        with self.results_lock:
            if not show_updates:
                return
                
            self.pending_show_updates[show_name] = show_updates
            cast_count = len(show_updates)
            self.processed_count += cast_count
            print(f"📦 Batching show '{show_name}': {cast_count} cast members")
            
            # Immediately flush this show's updates
            self._flush_show_batch(show_name)
    
    def _flush_show_batch(self, show_name):
        """Write a show's updates to Google Sheets."""
        if show_name not in self.pending_show_updates:
            return
            
        show_updates = self.pending_show_updates[show_name]
        print(f"💾 Writing {len(show_updates)} updates for '{show_name}'...")
        
        # Prepare batch update
        update_data = []
        for row_num, data in show_updates.items():
            episodes = data.get('episodes', MISSING_DATA_MARKER)
            seasons = data.get('seasons', MISSING_DATA_MARKER)
            
            if episodes == MISSING_DATA_MARKER:
                self.missing_data_count += 1
            if seasons == MISSING_DATA_MARKER:
                self.missing_data_count += 1
                
            update_data.append({
                'range': f"G{row_num}:H{row_num}",
                'values': [[str(episodes), str(seasons)]]
            })
        
        # Execute batch update
        if update_data:
            try:
                result = self.sheet.batch_update(update_data)
                print(f"✅ Updated {len(update_data)} rows for '{show_name}'")
            except Exception as e:
                print(f"❌ Batch update failed: {e}")
                self.errors += 1
                # Try individual updates as fallback
                for item in update_data[:5]:
                    try:
                        self.sheet.update(values=item['values'], range_name=item['range'])
                        time.sleep(0.1)
                    except Exception:
                        self.errors += 1
        
        del self.pending_show_updates[show_name]
    
    def add_crew_deletion(self, row_num):
        """Mark a crew member for deletion."""
        with self.results_lock:
            self.pending_deletions.append(row_num)
            self.deleted_crew += 1
            print(f"🗑️ Marking row {row_num} for deletion (crew)")
    
    def add_error(self):
        """Record an error."""
        with self.results_lock:
            self.errors += 1
    
    def get_stats(self):
        """Get current statistics."""
        with self.results_lock:
            return {
                'processed': self.processed_count,
                'deleted_crew': self.deleted_crew,
                'errors': self.errors,
                'missing_data': self.missing_data_count,
                'pending_shows': len(self.pending_show_updates)
            }

# ========== Optimized Worker Thread ==========
class CastInfoWorker:
    def __init__(self, worker_id, results_manager):
        self.worker_id = worker_id
        self.results_manager = results_manager
        self.driver = None
        self.restart_count = 0
        self.max_restarts = 3
        self.current_show_id = None  # Track current loaded show
        
    def setup_webdriver(self):
        """Setup Chrome WebDriver in headless mode."""
        try:
            chrome_options = Options()
            
            # HEADLESS MODE FOR PERFORMANCE
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            
            # Performance optimizations
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            
            # Memory optimization
            chrome_options.add_argument("--memory-pressure-off")
            chrome_options.add_argument("--max_old_space_size=2048")
            
            # Window size for headless
            chrome_options.add_argument("--window-size=1920,1080")
            
            # User agent
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            
            # Unique profile
            user_data_dir = f"/tmp/chrome_worker_{self.worker_id}_{int(time.time())}"
            if os.path.exists(user_data_dir):
                shutil.rmtree(user_data_dir, ignore_errors=True)
            os.makedirs(user_data_dir, exist_ok=True)
            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
            
            # Disable automation flags
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            self.user_data_dir = user_data_dir
            
            print(f"✅ Worker {self.worker_id}: WebDriver ready (headless)")
            return True
            
        except Exception as e:
            print(f"❌ Worker {self.worker_id}: Setup failed: {e}")
            return False
    
    def is_responsive(self):
        """Check if browser is responsive."""
        try:
            self.driver.current_url
            return True
        except:
            return False
    
    def smart_click(self, element):
        """Try multiple click methods."""
        try:
            element.click()
            return True
        except:
            try:
                self.driver.execute_script("arguments[0].click();", element)
                return True
            except:
                return False
    
    def load_show_page_if_needed(self, show_imdb_id):
        """Only load page if it's not already loaded."""
        # Check if we already have this show loaded
        if self.current_show_id == show_imdb_id:
            try:
                # Verify page is still valid
                self.driver.find_element(By.TAG_NAME, "body")
                print(f"  ♻️ Reusing already loaded page for {show_imdb_id}")
                return True
            except:
                print(f"  ⚠️ Page became stale, reloading...")
                
        # Load the page
        url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits/?ref_=tt_cl_sm"
        
        try:
            self.driver.get(url)
            
            # Wait for cast section
            WebDriverWait(self.driver, 10).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".cast_list")),  
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='title-cast']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#cast")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".sc-2840b417-3"))  
                )
            )
            
            # Quick scroll to load all content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.3)  # Reduced from 0.5
            self.driver.execute_script("window.scrollTo(0, 0);")
            
            self.current_show_id = show_imdb_id
            print(f"  📄 Loaded page for {show_imdb_id}")
            return True
            
        except TimeoutException:
            print(f"  ⚠️ Worker {self.worker_id}: Page load timeout")
            self.current_show_id = None
            return False
        except Exception as e:
            print(f"  ❌ Worker {self.worker_id}: Load failed: {e}")
            self.current_show_id = None
            return False
    
    def find_cast_member(self, cast_imdb_id, cast_name):
        """Find cast member on page - optimized version."""
        # Try by ID first - most reliable
        if cast_imdb_id and cast_imdb_id.startswith('nm'):
            try:
                # Direct search for ID
                element = self.driver.find_element(
                    By.XPATH, 
                    f"//a[contains(@href, '/name/{cast_imdb_id}/')]"
                )
                return element
            except:
                pass
        
        # Try by name if ID fails
        if cast_name:
            try:
                # Look for name in cast links
                elements = self.driver.find_elements(By.XPATH, "//a[contains(@href,'/name/')]")
                for elem in elements:
                    try:
                        if cast_name in elem.text:
                            return elem
                    except:
                        continue
            except:
                pass
        
        return None
    
    def find_episodes_button(self, cast_anchor):
        """Find episodes button near cast member - optimized."""
        try:
            # Find parent container (li or tr)
            parent = cast_anchor.find_element(By.XPATH, "./ancestor::*[self::li or self::tr or self::div][1]")
            
            # Look for button with "episode" text
            buttons = parent.find_elements(By.XPATH, ".//button | .//a")
            for button in buttons:
                try:
                    if 'episode' in button.text.lower():
                        return button
                except:
                    continue
                    
        except Exception:
            pass
        
        return None
    
    def extract_from_modal_fast(self):
        """Fast extraction from modal with reduced waits."""
        episodes = MISSING_DATA_MARKER
        seasons = MISSING_DATA_MARKER
        
        try:
            # Wait for modal - reduced timeout
            WebDriverWait(self.driver, MODAL_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='dialog'], .ipc-prompt"))
            )
            time.sleep(0.3)  # Reduced from 0.8
            
            # Get all text from modal at once
            try:
                modal = self.driver.find_element(By.CSS_SELECTOR, "[role='dialog'], .ipc-prompt")
                modal_text = modal.text
            except:
                modal_text = ""
            
            # Extract episodes - look for pattern
            episode_match = re.search(r"(\d+)\s+episodes?", modal_text, re.I)
            if episode_match:
                episodes = int(episode_match.group(1))
            
            # Extract seasons from tabs quickly
            try:
                season_tabs = self.driver.find_elements(By.CSS_SELECTOR, "li[data-testid^='season-tab-']")
                if season_tabs:
                    seasons_set = set()
                    for tab in season_tabs:
                        tab_id = tab.get_attribute("data-testid")
                        if tab_id:
                            match = re.search(r"season-tab-(\d+)", tab_id)
                            if match:
                                seasons_set.add(int(match.group(1)))
                    
                    if seasons_set:
                        seasons = ", ".join(str(s) for s in sorted(seasons_set))
            except:
                pass
            
            # If no seasons from tabs, get from first episode quickly
            if seasons == MISSING_DATA_MARKER:
                season_match = re.search(r"S(\d+)\.E\d+", modal_text)
                if season_match:
                    seasons = season_match.group(1)
            
        except TimeoutException:
            pass
        except Exception:
            pass
        
        return episodes, seasons
    
    def close_modal_fast(self):
        """Close modal as fast as possible."""
        try:
            # Try ESC key first - usually fastest
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.1)  # Minimal wait
            return True
        except:
            try:
                # Fallback to close button
                close_btn = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Close']")
                self.smart_click(close_btn)
                time.sleep(0.1)
                return True
            except:
                return False
    
    def process_cast_member_on_page(self, cast_data):
        """Process cast member on already loaded page - FAST version."""
        row_num = cast_data["row_num"]
        cast_name = cast_data["cast_name"]
        cast_imdb_id = cast_data["cast_imdb_id"]
        
        try:
            # Find cast member
            cast_anchor = self.find_cast_member(cast_imdb_id, cast_name)
            if not cast_anchor:
                return {
                    'episodes': MISSING_DATA_MARKER,
                    'seasons': MISSING_DATA_MARKER
                }
            
            # Find episodes button
            episodes_btn = self.find_episodes_button(cast_anchor)
            if not episodes_btn:
                return {
                    'episodes': MISSING_DATA_MARKER,
                    'seasons': MISSING_DATA_MARKER
                }
            
            # Scroll and click - minimal wait
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", episodes_btn)
            time.sleep(0.1)  # Reduced wait
            
            if not self.smart_click(episodes_btn):
                return {
                    'episodes': MISSING_DATA_MARKER,
                    'seasons': MISSING_DATA_MARKER
                }
            
            # Extract from modal
            episodes, seasons = self.extract_from_modal_fast()
            
            # Close modal
            self.close_modal_fast()
            
            if episodes != MISSING_DATA_MARKER or seasons != MISSING_DATA_MARKER:
                print(f"  ✅ {cast_name}: {episodes} eps, {seasons} seasons")
            
            return {
                'episodes': episodes,
                'seasons': seasons
            }
            
        except Exception as e:
            return {
                'episodes': MISSING_DATA_MARKER,
                'seasons': MISSING_DATA_MARKER
            }
    
    def process_show(self, show_name, cast_list):
        """Process all cast members for a show ON THE SAME PAGE."""
        print(f"📺 Worker {self.worker_id}: Processing '{show_name}' ({len(cast_list)} cast)")
        
        if not cast_list:
            return
        
        # Get show ID from first cast member
        show_imdb_id = cast_list[0]["show_imdb_id"]
        
        # Load show page ONCE
        if not self.load_show_page_if_needed(show_imdb_id):
            print(f"❌ Worker {self.worker_id}: Failed to load show page")
            # Add missing markers for all cast
            show_updates = {}
            for cast_data in cast_list:
                show_updates[cast_data['row_num']] = {
                    'episodes': MISSING_DATA_MARKER,
                    'seasons': MISSING_DATA_MARKER
                }
            self.results_manager.add_show_batch(show_name, show_updates)
            return
        
        # Process ALL cast members on this page
        show_updates = {}
        processed = 0
        
        for i, cast_data in enumerate(cast_list):
            cast_name = cast_data['cast_name']
            row_num = cast_data['row_num']
            
            # Progress indicator
            if (i + 1) % 10 == 0 or (i + 1) == len(cast_list):
                print(f"  Progress: {i+1}/{len(cast_list)}")
            
            # Process on already loaded page
            result = self.process_cast_member_on_page(cast_data)
            
            if result:
                show_updates[row_num] = result
                if result['episodes'] != MISSING_DATA_MARKER:
                    processed += 1
            
            # Minimal delay between cast members
            if i < len(cast_list) - 1:  # No delay after last member
                time.sleep(0.05)  # Very small delay
        
        # Batch update for the show
        if show_updates:
            self.results_manager.add_show_batch(show_name, show_updates)
        
        print(f"✅ Worker {self.worker_id}: Completed '{show_name}' - {processed}/{len(cast_list)} found")
    
    def run(self, assigned_shows):
        """Main worker loop."""
        if not self.setup_webdriver():
            return
        
        try:
            total_cast = sum(len(cast_list) for _, cast_list in assigned_shows)
            print(f"🎯 Worker {self.worker_id}: {len(assigned_shows)} shows, {total_cast} cast members")
            
            for show_name, cast_list in assigned_shows:
                self.process_show(show_name, cast_list)
                # Small pause between shows only
                time.sleep(0.2)
            
            print(f"✅ Worker {self.worker_id}: All shows completed")
            
        except Exception as e:
            print(f"❌ Worker {self.worker_id}: Fatal error: {e}")
            self.results_manager.add_error()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.driver:
                self.driver.quit()
            if hasattr(self, 'user_data_dir') and os.path.exists(self.user_data_dir):
                shutil.rmtree(self.user_data_dir, ignore_errors=True)
            print(f"🔒 Worker {self.worker_id}: Cleaned up")
        except:
            pass

# ========== Main Extractor (Same as v5) ==========
class v6UniversalSeasonExtractor:
    def __init__(self):
        self.sheet = None
        self.results_manager = None
        self.skipped_filled = 0
    
    def setup_sheets(self):
        """Connect to Google Sheets."""
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            wb = gc.open(WORKBOOK_NAME)
            self.sheet = wb.worksheet(SHEET_NAME)
            self.results_manager = ResultsManager(self.sheet)
            
            print("✅ Connected to Google Sheets")
            print(f"📊 Workbook: {WORKBOOK_NAME}")
            print(f"📊 Sheet: {SHEET_NAME}")
            
            return True
            
        except Exception as e:
            print(f"❌ Sheets setup failed: {e}")
            return False
    
    def load_cast_data(self):
        """Load all cast members needing processing."""
        try:
            all_values = self.sheet.get_all_values()
            print(f"📊 Total rows: {len(all_values)}")
            
            rows = []
            
            for r in range(1, len(all_values)):  # Skip header
                row = all_values[r]
                if len(row) < 8:
                    continue
                
                cast_name = row[0].strip()
                cast_imdb_id = row[2].strip()
                show_name = row[3].strip()
                show_imdb_id = row[4].strip()
                episodes = row[6].strip() if len(row) > 6 else ""
                seasons = row[7].strip() if len(row) > 7 else ""
                
                # Skip if both filled
                if episodes and seasons:
                    self.skipped_filled += 1
                    continue
                
                # Skip if missing required data
                if not show_imdb_id or not (cast_imdb_id or cast_name):
                    continue
                
                rows.append({
                    "row_num": r + 1,
                    "cast_name": cast_name,
                    "cast_imdb_id": cast_imdb_id,
                    "show_name": show_name,
                    "show_imdb_id": show_imdb_id
                })
            
            print(f"📊 Rows to process: {len(rows)}")
            print(f"⏭️ Skipped (filled): {self.skipped_filled}")
            
            return rows
            
        except Exception as e:
            print(f"❌ Load failed: {e}")
            return []
    
    def group_by_show(self, cast_members):
        """Group cast members by show."""
        shows = {}
        for cast_data in cast_members:
            show_name = cast_data['show_name']
            if show_name not in shows:
                shows[show_name] = []
            shows[show_name].append(cast_data)
        
        shows_list = list(shows.items())
        print(f"📺 {len(shows_list)} unique shows found")
        
        return shows_list
    
    def run(self):
        """Main execution."""
        print("\n🚀 v6 Optimized Page-Reuse CastInfo Extractor")
        print(f"⚙️ Configuration: {NUM_BROWSERS} browsers, page reuse optimization")
        print("⚡ Speed: Loads each show page ONCE, processes all cast on same page")
        
        if not self.setup_sheets():
            return False
        
        try:
            # Load data
            cast_members = self.load_cast_data()
            if not cast_members:
                print("📭 No data to process")
                return True
            
            # Group by show
            shows_list = self.group_by_show(cast_members)
            
            # Start parallel processing
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=NUM_BROWSERS) as executor:
                futures = {}
                
                for worker_id in range(1, NUM_BROWSERS + 1):
                    # Distribute shows across workers
                    assigned_shows = shows_list[(worker_id-1)::NUM_BROWSERS]
                    
                    worker = CastInfoWorker(worker_id, self.results_manager)
                    future = executor.submit(worker.run, assigned_shows)
                    futures[future] = worker_id
                
                # Wait for completion
                completed = 0
                for future in as_completed(futures):
                    worker_id = futures[future]
                    completed += 1
                    
                    stats = self.results_manager.get_stats()
                    elapsed = time.time() - start_time
                    
                    print(f"\n📊 Worker {worker_id} done ({completed}/{NUM_BROWSERS})")
                    print(f"⏱️ Elapsed: {elapsed:.1f}s")
                    print(f"✅ Processed: {stats['processed']}")
                    print(f"❌ Errors: {stats['errors']}")
            
            # Final stats
            final_stats = self.results_manager.get_stats()
            total_time = time.time() - start_time
            
            print("\n" + "="*60)
            print("🎉 PROCESSING COMPLETE!")
            print("="*60)
            print(f"⏱️ Total time: {total_time:.1f}s")
            print(f"✅ Processed: {final_stats['processed']} rows")
            print(f"🗑️ Crew deleted: {final_stats['deleted_crew']} rows")
            print(f"⏭️ Skipped: {self.skipped_filled} rows")
            print(f"❌ Errors: {final_stats['errors']}")
            print(f"⚠️ Missing data: {final_stats['missing_data']} cells")
            
            if final_stats['processed'] > 0:
                rate = final_stats['processed'] / (total_time / 60)
                print(f"📈 Rate: {rate:.1f} rows/minute")
            
            return True
            
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            traceback.print_exc()
            return False

# ---------- Entry Point ----------
def main():
    try:
        extractor = v6UniversalSeasonExtractor()
        success = extractor.run()
        
        if success:
            print("\n✅ Script completed successfully!")
        else:
            print("\n❌ Script failed!")
            
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

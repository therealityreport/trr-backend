#!/usr/bin/env python3
# v7UniversalSeasonExtractorCastInfo_Parallel_Full7.py
"""
v7 Universal Season Extractor - Best of v3 + v6 Combined

Combines:
- v6's page reuse optimization (load once, process all cast)
- v3's comprehensive selector logic and fallbacks
- v3's visible browser mode for better compatibility
- v3's precise XPath structures
- v3's main show page fallback
- v6's speed optimizations where they don't hurt accuracy

Key Features:
- Visible browser mode (not headless) for better content loading
- Comprehensive selectors with precise XPath and multiple fallbacks
- Page reuse - loads each show page ONCE
- Main show page fallback for reality show contestants
- Batch updates per show
- Better modal extraction
"""

print("🚀 Starting v7 Combined Best-of-Both CastInfo Season Extractor!")

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

# Timeouts - balanced for reliability and speed
PAGE_LOAD_TIMEOUT = 20
MODAL_TIMEOUT = 12
ELEMENT_TIMEOUT = 8

# Missing data indicator
MISSING_DATA_MARKER = "**"

# Recency filtering (align with CastInfo Step 1 behaviour)
RECENT_EPISODE_WINDOW_DAYS = 7
RECENT_EPISODE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%b %d %Y",
    "%B %d %Y",
)

# ========== Results Manager (from v6) ==========
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
        self.show_stats = {}  # Track successful updates per show
        
    def add_show_batch(self, show_name, show_updates):
        """Add a complete show's updates and immediately write to sheets."""
        with self.results_lock:
            if not show_updates:
                return
                
            self.pending_show_updates[show_name] = show_updates
            cast_count = len(show_updates)
            self.processed_count += cast_count
            
            # Count successful extractions (non-** values)
            successful_count = sum(
                1 for data in show_updates.values()
                if data.get('episodes') != MISSING_DATA_MARKER 
                or data.get('seasons') != MISSING_DATA_MARKER
            )
            
            # Track show stats
            if show_name not in self.show_stats:
                self.show_stats[show_name] = 0
            self.show_stats[show_name] += successful_count
            
            print(f"📦 Batching show '{show_name}': {cast_count} cast members ({successful_count} with data)")
            
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
                'pending_shows': len(self.pending_show_updates),
                'show_stats': dict(self.show_stats)  # Copy of show statistics
            }

# ========== v7 Worker combining best of v3 and v6 ==========
class CastInfoWorker:
    def __init__(self, worker_id, results_manager):
        self.worker_id = worker_id
        self.results_manager = results_manager
        self.driver = None
        self.restart_count = 0
        self.max_restarts = 3
        self.current_show_id = None  # Track current loaded show (from v6)
        
    def setup_webdriver(self):
        """Setup Chrome WebDriver in VISIBLE mode (from v3) for better compatibility."""
        try:
            chrome_options = Options()
            
            # NO HEADLESS MODE - use visible browser for better compatibility (from v3)
            # chrome_options.add_argument("--headless=new")  # DISABLED
            
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
            
            # Window size
            chrome_options.add_argument("--window-size=1920,1080")
            
            # User agent
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            
            # Disable automation flags (from v3)
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Unique profile
            user_data_dir = f"/tmp/chrome_worker_{self.worker_id}_{int(time.time())}"
            if os.path.exists(user_data_dir):
                shutil.rmtree(user_data_dir, ignore_errors=True)
            os.makedirs(user_data_dir, exist_ok=True)
            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            self.user_data_dir = user_data_dir
            
            print(f"✅ Worker {self.worker_id}: WebDriver ready (visible mode)")
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
        """Try multiple click methods (from v3)."""
        methods = [
            lambda: element.click(),
            lambda: self.driver.execute_script("arguments[0].click();", element),
            lambda: ActionChains(self.driver).move_to_element(element).click().perform(),
        ]
        
        for i, method in enumerate(methods):
            try:
                method()
                return True
            except Exception:
                if i < len(methods) - 1:
                    time.sleep(0.1)
        return False
    
    def load_show_page_if_needed(self, show_imdb_id):
        """Only load page if it's not already loaded (from v6)."""
        # Check if we already have this show loaded
        if self.current_show_id == show_imdb_id:
            try:
                # Verify page is still valid
                self.driver.find_element(By.TAG_NAME, "body")
                print(f"  ♻️ Reusing already loaded page for {show_imdb_id}")
                return True
            except:
                print(f"  ⚠️ Page became stale, reloading...")
        
        # Try full credits page with cast anchor (from v3)
        url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits/?ref_=tt_cl_sm#cast"
        
        try:
            self.driver.get(url)
            
            # Wait for cast section (comprehensive selectors from v3)
            WebDriverWait(self.driver, 15).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".cast_list")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='title-cast']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#cast")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".sc-2840b417-3")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#self"))  # From v3
                )
            )
            
            # Scroll to load all content (from v3)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            self.driver.execute_script("window.scrollTo(0, 0);")
            
            # Additional wait for all people to load (from v3)
            time.sleep(1.0)
            
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
    
    def escape_xpath_string(self, text):
        """Properly escape quotes in XPath strings (from v3)."""
        if "'" not in text:
            return f"'{text}'"
        elif '"' not in text:
            return f'"{text}"'
        else:
            # Handle mixed quotes by concatenating
            parts = text.split("'")
            xpath_parts = [f"'{part}'" if part else "''" for part in parts]
            quote_separator = ', "\'", '
            return f"concat({quote_separator.join(xpath_parts)})"
    
    def find_cast_member(self, cast_imdb_id, cast_name):
        """Find cast member using comprehensive selectors from v3."""
        print(f"  🔍 Searching for: {cast_name} (ID: {cast_imdb_id})")
        
        # PRIMARY: Try precise XPath structures (from v3)
        if cast_imdb_id and cast_imdb_id.startswith('nm'):
            precise_selectors = [
                # Cast section -> cast rows -> individual cast member
                f"//section[3]//ul/li//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"/html/body/div[2]/main/div/section/div/section/div/div[1]/section[3]//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"//section[3]/div[2]/ul/li//a[contains(@href, '{cast_imdb_id}')]",
                f"//section[3]//a[contains(@href, '{cast_imdb_id}')]",
            ]
            
            for selector in precise_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    print(f"  ✅ Found {cast_name} by precise XPath")
                    return element
                except NoSuchElementException:
                    continue
        
        # FALLBACK: Comprehensive selectors from v3
        if cast_imdb_id and cast_imdb_id.startswith('nm'):
            fallback_selectors = [
                f"//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"//a[contains(@href, '{cast_imdb_id}')]",
                f"//td[@class='primary_photo']//a[contains(@href, '{cast_imdb_id}')]",
                f"//*[contains(@id, 'self') or contains(@class, 'self')]//a[contains(@href, '{cast_imdb_id}')]",
                f"//table//a[contains(@href, '{cast_imdb_id}')]"
            ]
            
            for selector in fallback_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    print(f"  ✅ Found {cast_name} by fallback IMDb ID")
                    return element
                except NoSuchElementException:
                    continue
        
        # NAME SEARCH: Try by name if ID fails
        if cast_name:
            escaped_name = self.escape_xpath_string(cast_name)
            name_selectors = [
                f"//a[contains(@href,'/name/') and normalize-space(text())={escaped_name}]",
                f"//a[contains(@href,'/name/') and contains(text(), {escaped_name})]",
                f"//a[contains(@class, 'name-credits--title-text') and contains(text(), {escaped_name})]",
            ]
            
            for selector in name_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    print(f"  ✅ Found {cast_name} by name match")
                    return element
                except NoSuchElementException:
                    continue
        
        print(f"  ⚠️ Could not find {cast_name} (ID: {cast_imdb_id})")
        return None
    
    def find_episodes_button(self, cast_anchor):
        """Find episodes button using precise structure from v3."""
        try:
            # Find parent container
            parent = None
            parent_selectors = [
                "./ancestor::li[1]",
                "./ancestor::div[contains(@class, 'sc-2840b417')]",
                "./ancestor::div[contains(@class, 'sc-')]",
                "./ancestor::tr[1]",
            ]
            
            for selector in parent_selectors:
                try:
                    parent = cast_anchor.find_element(By.XPATH, selector)
                    break
                except:
                    continue
            
            if not parent:
                parent = cast_anchor.find_element(By.XPATH, "./parent::*[1]")
            
            # Look for button
            button_patterns = [
                ".//button[contains(text(), 'episode')]",
                ".//button[@class='ipc-link ipc-link--base']",
                ".//button[contains(@class, 'ipc-link')]",
                ".//a[contains(text(), 'episode')]",
            ]
            
            for pattern in button_patterns:
                try:
                    button = parent.find_element(By.XPATH, pattern)
                    button_text = button.text.lower()
                    if 'episode' in button_text or button.tag_name == 'button':
                        return button
                except:
                    continue
                    
        except Exception:
            pass
        
        return None
    
    def extract_from_modal(self):
        """Extract episodes and seasons from modal (comprehensive from v3)."""
        episodes = MISSING_DATA_MARKER
        seasons = MISSING_DATA_MARKER
        
        try:
            # Wait for modal
            WebDriverWait(self.driver, MODAL_TIMEOUT).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[role='dialog']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-prompt")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-prompt-header"))
                )
            )
            time.sleep(0.5)
            
            # Extract episodes - try precise XPath first (from user's selectors)
            precise_episode_selectors = [
                "/html/body/div[5]/div[2]/div/div[2]/div/div[1]/div/div[2]/div[2]/ul/li[1]",
                "//div[5]//div[2]//ul/li[contains(text(), 'episode')]",
                ".ipc-prompt-header__subtitle li",
                ".ipc-inline-list__item",
            ]
            
            for selector in precise_episode_selectors:
                try:
                    if selector.startswith('/'):
                        elem = self.driver.find_element(By.XPATH, selector)
                    else:
                        elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    text = elem.text
                    episode_match = re.search(r"(\d+)\s+episodes?", text, re.I)
                    if episode_match:
                        episodes = int(episode_match.group(1))
                        print(f"    Found {episodes} episodes in modal")
                        break
                except:
                    continue
            
            # Extract seasons - try tabs first
            try:
                season_tabs = self.driver.find_elements(By.CSS_SELECTOR, "li[data-testid^='season-tab-']")
                if season_tabs:
                    seasons_set = set()
                    for tab in season_tabs:
                        tab_id = tab.get_attribute("data-testid")
                        if tab_id and 'season-tab-' in tab_id:
                            match = re.search(r"season-tab-(\d+)", tab_id)
                            if match:
                                seasons_set.add(int(match.group(1)))
                    
                    if seasons_set:
                        seasons = ", ".join(str(s) for s in sorted(seasons_set))
                        print(f"    Found seasons from tabs: {seasons}")
            except:
                pass
            
            # If no seasons from tabs, extract from first episode marker
            if seasons == MISSING_DATA_MARKER:
                try:
                    # Look for episode marker
                    episode_selectors = [
                        ".ipc-inline-list__item",
                        "a.episodic-credits-bottomsheet__menu-item",
                        "div[data-testid^='episodic-credits-bottomsheet-row'] a",
                    ]
                    
                    for selector in episode_selectors:
                        try:
                            first_episode = self.driver.find_element(By.CSS_SELECTOR, selector)
                            episode_text = first_episode.text
                            
                            # Extract season from various formats
                            patterns = [
                                r"S(\d+)\.E\d+",  # S2.E1
                                r"Season\s+(\d+)",  # Season 2
                                r"(\d+)x\d+"  # 2x01
                            ]
                            
                            for pattern in patterns:
                                season_match = re.search(pattern, episode_text, re.I)
                                if season_match:
                                    seasons = season_match.group(1)
                                    print(f"    Found season {seasons} from episode marker")
                                    break
                            
                            if seasons != MISSING_DATA_MARKER:
                                break
                                
                        except:
                            continue
                except:
                    pass
            
        except TimeoutException:
            print(f"  ⚠️ Modal timeout")
        except Exception as e:
            print(f"  ⚠️ Modal error: {e}")
        
        return episodes, seasons
    
    def close_modal(self):
        """Close modal quickly."""
        try:
            # Try ESC key first - usually fastest
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.1)
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
    
    def try_main_show_page_fallback(self, show_imdb_id):
        """Try main show page as fallback (from v3)."""
        main_show_url = f"https://www.imdb.com/title/{show_imdb_id}/"
        print(f"  🌐 Trying main show page fallback...")
        
        try:
            self.driver.get(main_show_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1.0)
            
            # Try to expand cast section
            try:
                expand_buttons = self.driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'See full cast') or contains(text(), 'more') or contains(text(), 'More')]")
                for button in expand_buttons:
                    try:
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.5)
                        print(f"    ✅ Expanded cast section on main show page")
                        break
                    except:
                        continue
            except:
                pass
            
            return True
            
        except Exception as e:
            print(f"  ⚠️ Main show page fallback failed: {e}")
            return False
    
    def process_cast_member_on_page(self, cast_data, allow_fallback=True):
        """Process cast member on already loaded page."""
        row_num = cast_data["row_num"]
        cast_name = cast_data["cast_name"]
        cast_imdb_id = cast_data["cast_imdb_id"]
        show_imdb_id = cast_data["show_imdb_id"]
        
        try:
            # Find cast member
            cast_anchor = self.find_cast_member(cast_imdb_id, cast_name)
            
            # If not found and we're on fullcredits, try main page fallback (from v3)
            if not cast_anchor and allow_fallback and "fullcredits" in self.driver.current_url:
                if self.try_main_show_page_fallback(show_imdb_id):
                    # Try finding again on main page
                    cast_anchor = self.find_cast_member(cast_imdb_id, cast_name)
                    if cast_anchor:
                        print(f"  ✅ Found {cast_name} on main show page!")
            
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
            
            # Scroll and click
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", episodes_btn)
            time.sleep(0.2)
            
            if not self.smart_click(episodes_btn):
                return {
                    'episodes': MISSING_DATA_MARKER,
                    'seasons': MISSING_DATA_MARKER
                }
            
            # Extract from modal
            episodes, seasons = self.extract_from_modal()
            
            # Close modal
            self.close_modal()
            
            if episodes != MISSING_DATA_MARKER or seasons != MISSING_DATA_MARKER:
                # Format seasons output more clearly
                if seasons and seasons != MISSING_DATA_MARKER:
                    if ',' in str(seasons):  # Multiple seasons
                        seasons_text = f"Seasons: {seasons}"
                    else:  # Single season
                        seasons_text = f"Season {seasons}"
                else:
                    seasons_text = "Season: **"
                print(f"    ✅ {cast_name}: {episodes} episodes, {seasons_text}")
            
            return {
                'episodes': episodes,
                'seasons': seasons
            }
            
        except Exception as e:
            print(f"  ❌ Error with {cast_name}: {e}")
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
            
            # Show detailed progress: person X of Y total
            print(f"  [{i+1}/{len(cast_list)}] Processing {cast_name}")
            
            # Process on already loaded page
            result = self.process_cast_member_on_page(cast_data)
            
            if result:
                show_updates[row_num] = result
                if result['episodes'] != MISSING_DATA_MARKER:
                    processed += 1
            
            # Batch update every 100 cast members for large shows
            if len(show_updates) >= 100:
                print(f"  📦 Intermediate batch: Writing 100 updates for '{show_name}'...")
                self.results_manager.add_show_batch(show_name, show_updates)
                show_updates = {}  # Reset for next batch
            
            # Small delay between cast members
            if i < len(cast_list) - 1:
                time.sleep(0.1)
        
        # Batch update any remaining cast members
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
                time.sleep(0.3)  # Small pause between shows
            
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

# ========== Main Extractor (from v6) ==========
class v7UniversalSeasonExtractor:
    def __init__(self):
        self.sheet = None
        self.workbook = None
        self.results_manager = None
        self.skipped_filled = 0
        today = datetime.datetime.utcnow().date()
        self.today = today
        self.recent_window_start = today - datetime.timedelta(days=RECENT_EPISODE_WINDOW_DAYS)
        self.showinfo_recent_dates = {}
        self.recent_shows_reprocessed = {}
        self.recent_rows = 0
        self._recent_flag_cache = {}
    
    def setup_sheets(self):
        """Connect to Google Sheets."""
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            wb = gc.open(WORKBOOK_NAME)
            self.workbook = wb
            self.sheet = wb.worksheet(SHEET_NAME)
            self.results_manager = ResultsManager(self.sheet)
            
            print("✅ Connected to Google Sheets")
            print(f"📊 Workbook: {WORKBOOK_NAME}")
            print(f"📊 Sheet: {SHEET_NAME}")

            self.showinfo_recent_dates = self.load_showinfo_recent_dates(wb)
            
            return True
            
        except Exception as e:
            print(f"❌ Sheets setup failed: {e}")
            return False

    @staticmethod
    def parse_recent_episode_date(value):
        raw = (value or "").strip()
        if not raw:
            return None

        candidates = [raw]
        if len(raw) >= 10:
            candidates.append(raw[:10])
        if " " in raw:
            candidates.append(raw.split(" ")[0])
        if raw.endswith("Z"):
            candidates.append(raw[:-1])
            candidates.append(raw[:-1] + "+00:00")

        for candidate in candidates:
            candidate = candidate.strip()
            if not candidate:
                continue

            if len(candidate) == 10 and candidate[4] == "-" and candidate[7] == "-":
                try:
                    return datetime.datetime.strptime(candidate, "%Y-%m-%d").date()
                except ValueError:
                    pass

            try:
                parsed = datetime.datetime.fromisoformat(candidate)
                return parsed.date()
            except ValueError:
                pass

            for fmt in RECENT_EPISODE_FORMATS:
                try:
                    parsed = datetime.datetime.strptime(candidate, fmt)
                    return parsed.date()
                except ValueError:
                    continue

        return None

    def load_showinfo_recent_dates(self, workbook):
        mapping = {}
        try:
            showinfo_ws = workbook.worksheet("ShowInfo")
        except Exception as e:
            print(f"⚠️ Could not open ShowInfo sheet: {e}")
            return mapping

        try:
            rows = showinfo_ws.get_all_values()
        except Exception as e:
            print(f"⚠️ Could not read ShowInfo sheet: {e}")
            return mapping

        if not rows:
            print("⚠️ ShowInfo sheet returned no data")
            return mapping

        header = [h.strip() for h in rows[0]]
        col_map = {name: idx for idx, name in enumerate(header)}
        imdb_idx = col_map.get("IMDbSeriesID")
        recent_idx = col_map.get("Most Recent Episode")

        if imdb_idx is None or recent_idx is None:
            print("⚠️ ShowInfo sheet missing 'IMDbSeriesID' or 'Most Recent Episode' columns")
            return mapping

        tracked = 0
        parsed = 0

        for row in rows[1:]:
            if len(row) <= max(imdb_idx, recent_idx):
                continue

            imdb_id = row[imdb_idx].strip()
            if not imdb_id:
                continue

            tracked += 1
            recent_value = row[recent_idx].strip()
            recent_date = self.parse_recent_episode_date(recent_value)
            if recent_date:
                mapping[imdb_id] = recent_date
                parsed += 1

        if parsed:
            print(f"📅 Loaded recent-episode dates for {parsed} shows (tracked: {tracked})")
        else:
            print("⚠️ No recent-episode dates parsed from ShowInfo")

        return mapping

    def should_process_show(self, show_imdb_id, show_name):
        if not show_imdb_id:
            return False

        if show_imdb_id not in self._recent_flag_cache:
            recent_date = self.showinfo_recent_dates.get(show_imdb_id)
            flag = bool(recent_date and recent_date >= self.recent_window_start)
            self._recent_flag_cache[show_imdb_id] = flag

            if flag and show_imdb_id not in self.recent_shows_reprocessed:
                human = recent_date.isoformat() if recent_date else "unknown"
                print(f"  🔁 Will refresh '{show_name}' (IMDb {show_imdb_id}) due to recent episode {human}")
                self.recent_shows_reprocessed[show_imdb_id] = {"name": show_name, "date": recent_date}

        return self._recent_flag_cache[show_imdb_id]

    def load_cast_data(self):
        """Load all cast members needing processing."""
        try:
            all_values = self.sheet.get_all_values()
            print(f"📊 Total rows: {len(all_values)}")
            
            rows = []
            
            for r in range(1, len(all_values)):
                row = all_values[r]
                if len(row) < 8:
                    continue
                
                cast_name = row[0].strip()
                cast_imdb_id = row[2].strip()
                show_name = row[3].strip()
                show_imdb_id = row[4].strip()
                episodes = row[6].strip() if len(row) > 6 else ""
                seasons = row[7].strip() if len(row) > 7 else ""

                needs_recent_update = self.should_process_show(show_imdb_id, show_name)
                
                # Skip if both filled and the show does not need a recency refresh
                if episodes and seasons and not needs_recent_update:
                    self.skipped_filled += 1
                    continue
                
                # Skip if missing required data
                if not show_imdb_id or not (cast_imdb_id or cast_name):
                    continue

                if episodes and seasons and needs_recent_update:
                    self.recent_rows += 1
                
                rows.append({
                    "row_num": r + 1,
                    "cast_name": cast_name,
                    "cast_imdb_id": cast_imdb_id,
                    "show_name": show_name,
                    "show_imdb_id": show_imdb_id
                })
            
            print(f"📊 Rows to process: {len(rows)}")
            print(f"⏭️ Skipped (filled): {self.skipped_filled}")
            if self.recent_shows_reprocessed:
                print(f"  🔁 Shows scheduled due to recent episodes: {len(self.recent_shows_reprocessed)}")
                if self.recent_rows:
                    print(f"  🔁 Rows reprocessed due to recent airings: {self.recent_rows}")
            
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
        print("\n🚀 v7 Combined Best-of-Both CastInfo Extractor")
        print(f"⚙️ Configuration: {NUM_BROWSERS} browsers, visible mode")
        print("✨ Combines v6 speed with v3 compatibility")
        
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
            if self.recent_shows_reprocessed:
                print(f"  🔁 Shows refreshed (recent episodes): {len(self.recent_shows_reprocessed)}")
                if self.recent_rows:
                    print(f"  🔁 Rows refreshed due to recency: {self.recent_rows}")
            print(f"❌ Errors: {final_stats['errors']}")
            print(f"⚠️ Missing data: {final_stats['missing_data']} cells")
            
            if final_stats['processed'] > 0:
                rate = final_stats['processed'] / (total_time / 60)
                print(f"📈 Rate: {rate:.1f} rows/minute")
            
            # Show breakdown by show
            show_stats = final_stats.get('show_stats', {})
            if show_stats:
                print("\n" + "="*60)
                print("📊 SHOWS WITH SUCCESSFUL DATA EXTRACTION:")
                print("="*60)
                
                # Sort shows by number of successful updates (descending)
                sorted_shows = sorted(show_stats.items(), key=lambda x: x[1], reverse=True)
                
                total_successful = 0
                for show_name, count in sorted_shows:
                    if count > 0:  # Only show shows with successful extractions
                        print(f"  ✅ {show_name}: {count} cast members with data")
                        total_successful += count
                
                if total_successful > 0:
                    print(f"\n  📈 Total successful extractions: {total_successful}")
                    rows_with_blanks = final_stats['processed'] - total_successful
                    if rows_with_blanks > 0:
                        print(f"  ⚠️ Rows updated with ** markers only: {rows_with_blanks}")
                else:
                    print("  ⚠️ No successful data extractions (all rows marked with **)")
            
            return True
            
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            traceback.print_exc()
            return False

# ---------- Entry Point ----------
def main():
    try:
        extractor = v7UniversalSeasonExtractor()
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

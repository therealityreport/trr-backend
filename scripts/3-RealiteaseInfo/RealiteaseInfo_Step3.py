#!/usr/bin/env python3

from __future__ import annotations

import sys
import time
import re
import unicodedata
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


def _bootstrap_environment() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    venv_lib = repo_root / ".venv" / "lib"

    if venv_lib.exists():
        def version_key(path: Path) -> tuple[int, ...]:
            name = path.parent.name  # e.g. "python3.13"
            parts = name.replace('python', '').split('.')
            return tuple(int(part) for part in parts if part.isdigit())

        site_candidates = sorted(
            (p for p in venv_lib.glob("python*/site-packages") if p.is_dir()),
            key=version_key,
            reverse=True,
        )

        for candidate in site_candidates:
            path_str = str(candidate)
            if path_str in sys.path:
                # Preferred path already available; avoid inserting lower versions.
                break
            sys.path.insert(0, path_str)
            print(f"🔧 Added site-packages path: {path_str}")
            break

    return repo_root


REPO_ROOT = _bootstrap_environment()

import os  # noqa: E402

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
import gspread  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

try:
    import google.generativeai as genai  # type: ignore
except ImportError as import_err:
    print(f"⚠️ google-generativeai import failed: {import_err}")
    genai = None

# Load environment variables
load_dotenv(REPO_ROOT / ".env")

# Column indices (0-based) for RealiteaseInfo sheet structure
REALITEASE_COLUMN_INDEX = {
    'cast_name': 0,
    'cast_imdb_id': 1,
    'cast_tmdb_id': 2,
    'show_names': 3,
    'show_imdb_ids': 4,
    'show_tmdb_ids': 5,
    'total_shows': 6,
    'total_seasons': 7,
    'total_episodes': 8,
    'gender': 9,
    'birthday': 10,
    'zodiac': 11,
}

REALITEASE_COLUMN_NUMBER = {
    field: index + 1 for field, index in REALITEASE_COLUMN_INDEX.items()
}


def get_realitease_value(row, field):
    index = REALITEASE_COLUMN_INDEX[field]
    return row[index] if len(row) > index else ''


def column_number_to_letter(column_number):
    result = ''
    number = column_number
    while number > 0:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def normalize_name(name: str) -> str:
    if not name:
        return ''
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s'-]", ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_first_last(name: str) -> Tuple[str, str]:
    normalized = normalize_name(name)
    if not normalized:
        return '', ''
    parts = normalized.split()
    if not parts:
        return '', ''
    first = parts[0]
    last = parts[-1].split('-')[-1]
    return first, last


def names_match_exact(first_name: str, last_name: str, candidate: str) -> bool:
    cand_first, cand_last = extract_first_last(candidate)
    if not cand_first or not cand_last:
        return False
    if first_name != cand_first:
        return False
    if last_name == cand_last:
        return True
    cand_variants = set(cand_last.split('-'))
    name_variants = set(last_name.split('-'))
    return bool(cand_variants & name_variants)


def parse_date_text_to_yyyy_mm_dd(text: str) -> Optional[str]:
    if not text:
        return None

    cleaned = text.strip().replace('–', '-').replace('—', '-').replace('/', '-').replace('\n', ' ')

    try:
        return datetime.strptime(cleaned[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        pass

    parts = [p for p in cleaned.split('-') if p]
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        year, month, day = parts
        try:
            return datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')
        except ValueError:
            pass

    month_map = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12,
    }

    tokens = re.split(r'[ ,]+', cleaned.lower())
    if len(tokens) >= 3:
        if tokens[0] in month_map and tokens[1].isdigit() and tokens[-1].isdigit():
            month = month_map[tokens[0]]
            day = int(tokens[1])
            year = int(tokens[-1])
            try:
                return datetime(year, month, day).strftime('%Y-%m-%d')
            except ValueError:
                pass
        if tokens[-2] in month_map and tokens[-3].isdigit() and tokens[-1].isdigit():
            day = int(tokens[-3])
            month = month_map[tokens[-2]]
            year = int(tokens[-1])
            try:
                return datetime(year, month, day).strftime('%Y-%m-%d')
            except ValueError:
                pass

    digits = re.findall(r'(\d{1,4})', cleaned)
    if len(digits) >= 3:
        a, b, c = digits[:3]
        if len(c) == 2:
            c = '19' + c if int(c) >= 50 else '20' + c
        try:
            return datetime(int(c), int(a), int(b)).strftime('%Y-%m-%d')
        except ValueError:
            try:
                return datetime(int(c), int(b), int(a)).strftime('%Y-%m-%d')
            except ValueError:
                pass

    return None


def save_debug_html(html: str, person_name: str, wiki_domain: str, title: str):
    try:
        debug_dir = os.path.join(os.path.dirname(__file__), 'debug_html')
        os.makedirs(debug_dir, exist_ok=True)
        safe_name = re.sub(r'[^\w\-_]', '_', f"{person_name}_{wiki_domain}_{title}")
        filename = f"debug_{safe_name}.html"
        filepath = os.path.join(debug_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"      🔍 Debug HTML saved: {filepath}")
    except Exception as e:
        print(f"      ⚠️ Could not save debug HTML: {e}")


class FamousBirthdaysEnhancer:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        })
        self.gc = None
        self.worksheet = None
        self.batch_updates = []
        self.batch_size = 100
        self.processed_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.sources_used = {}
        self.interval_start_time = time.time()
        self.interval_start_updates = 0
        self.last_gender_analysis_source = None
        self.gemini_model_name = (
            os.getenv("GOOGLE_GEMINI_MODEL")
            or os.getenv("GEMINI_MODEL")
            or "gemini-2.5-flash"
        )
        self.gemini_model = None
        self.configure_gemini()
        self.tmdb_api_key = os.getenv('TMDB_API_KEY') or ''
        self.tmdb_session = requests.Session() if self.tmdb_api_key else None
        self.tmdb_base_url = 'https://api.themoviedb.org/3'
        self.tmdb_credits_cache = {}
        self.tmdb_person_cache = {}
        self.tmdb_show_limit = int(os.getenv('REALITEASE_TMDB_SHOW_LIMIT', '5'))
        self.setup_wiki_mappings()

    def configure_gemini(self):
        api_key = os.getenv("GOOGLE_GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key or not genai:
            if not api_key:
                print("⚠️ GOOGLE_GEMINI_API_KEY not set; skipping Gemini integration")
            elif not genai:
                print("⚠️ google-generativeai not installed; skipping Gemini integration")
            return
        try:
            genai.configure(api_key=api_key)
            self.gemini_model = genai.GenerativeModel(self.gemini_model_name)
            print(f"✅ Google Gemini configured (model: {self.gemini_model_name})")
        except Exception as e:
            print(f"⚠️ Failed to configure Google Gemini: {e}")
            self.gemini_model = None

    def setup_wiki_mappings(self):
        self.franchise_wikis = {
            'bachelor-nation.fandom.com': [
                'the bachelor', 'the bachelorette', 'bachelor in paradise',
                'the golden bachelor', 'the golden bachelorette'
            ],
            'real-housewives.fandom.com': [
                'the real housewives of atlanta', 'the real housewives of new jersey',
                'the real housewives of new york city', 'the real housewives of beverly hills',
                'the real housewives of miami', 'the real housewives of orange county',
                'the real housewives of dubai', 'the real housewives ultimate girls trip',
                'the real housewives of salt lake city', 'the real housewives of dallas',
                'the real housewives of potomac', 'the real housewives of d.c.'
            ],
            'vanderpump-rules.fandom.com': ['vanderpump rules', 'vanderpump villa', 'the valley'],
            'thechallenge.fandom.com': ['the challenge', 'the challenge: all stars', 'the challenge: usa'],
            'survivor.fandom.com': ['survivor'],
            'bigbrother.fandom.com': ['big brother', 'celebrity big brother', 'big brother reindeer games'],
            'loveisland.fandom.com': ['love island', 'love island: all stars', 'love island games', 'love island: beyond the villa'],
            'rupaulsdragrace.fandom.com': ["rupaul's drag race", "rupaul's drag race all stars", "rupaul's drag race global all stars"],
            'belowdeck.fandom.com': ['below deck', 'below deck mediterranean', 'below deck sailing yacht', 'below deck adventure', 'below deck down under'],
            'jerseyshore.fandom.com': ['jersey shore', 'jersey shore: family vacation', 'snooki & jwoww'],
            'kardashians.fandom.com': ['keeping up with the kardashians', 'the kardashians', 'life of kylie'],
            'loveandhiphop.fandom.com': ['love & hip hop atlanta', 'love & hip hop new york'],
            'amazingrace.fandom.com': ['the amazing race'],
            'toohottohandle.fandom.com': ['too hot to handle', 'perfect match'],
            'thecircle.fandom.com': ['the circle'],
            'dancemoms.fandom.com': ['dance moms'],
            'antm.fandom.com': ["america's next top model"],
            'badgirlsclub.fandom.com': ['bad girls club', 'baddies east reunion'],
            'thehills.fandom.com': ['the hills', 'the hills: new beginnings', 'laguna beach'],
            'realworld.fandom.com': ['the real world', 'the real world homecoming'],
            'teenmom.fandom.com': ['teen mom og']
        }
        self.show_to_wiki = {}
        for wiki, shows in self.franchise_wikis.items():
            for show in shows:
                self.show_to_wiki[show.lower()] = wiki

    def get_tmdb_aggregate_credits(self, show_id: str):
        if not self.tmdb_session or not self.tmdb_api_key:
            return None
        if show_id in self.tmdb_credits_cache:
            return self.tmdb_credits_cache[show_id]
        try:
            time.sleep(0.25)
            response = self.tmdb_session.get(
                f"{self.tmdb_base_url}/tv/{show_id}/aggregate_credits",
                params={'api_key': self.tmdb_api_key},
                timeout=15,
            )
            if response.status_code != 200:
                print(f"      ⚠️ TMDb credits request failed for show {show_id}: HTTP {response.status_code}")
                return None
            data = response.json()
            self.tmdb_credits_cache[show_id] = data
            return data
        except Exception as exc:
            print(f"      ⚠️ TMDb credits request error for show {show_id}: {exc}")
            return None

    def get_tmdb_person_details(self, person_id: int):
        if not self.tmdb_session or not self.tmdb_api_key:
            return None
        if person_id in self.tmdb_person_cache:
            return self.tmdb_person_cache[person_id]
        try:
            time.sleep(0.25)
            response = self.tmdb_session.get(
                f"{self.tmdb_base_url}/person/{person_id}",
                params={'api_key': self.tmdb_api_key},
                timeout=15,
            )
            if response.status_code != 200:
                print(f"      ⚠️ TMDb person lookup failed for {person_id}: HTTP {response.status_code}")
                return None
            data = response.json()
            self.tmdb_person_cache[person_id] = data
            return data
        except Exception as exc:
            print(f"      ⚠️ TMDb person lookup error for {person_id}: {exc}")
            return None

    def calculate_zodiac(self, birthday_str):
        if not birthday_str or len(birthday_str) < 10:
            return ''
        try:
            year_str, month_str, day_str = birthday_str.split('-')
            month = int(month_str)
            day = int(day_str)
        except Exception:
            return ''
        if (month == 3 and day >= 21) or (month == 4 and day <= 19):
            return "Aries"
        if (month == 4 and day >= 20) or (month == 5 and day <= 20):
            return "Taurus"
        if (month == 5 and day >= 21) or (month == 6 and day <= 20):
            return "Gemini"
        if (month == 6 and day >= 21) or (month == 7 and day <= 22):
            return "Cancer"
        if (month == 7 and day >= 23) or (month == 8 and day <= 22):
            return "Leo"
        if (month == 8 and day >= 23) or (month == 9 and day <= 22):
            return "Virgo"
        if (month == 9 and day >= 23) or (month == 10 and day <= 22):
            return "Libra"
        if (month == 10 and day >= 23) or (month == 11 and day <= 21):
            return "Scorpio"
        if (month == 11 and day >= 22) or (month == 12 and day <= 21):
            return "Sagittarius"
        if (month == 12 and day >= 22) or (month == 1 and day <= 19):
            return "Capricorn"
        if (month == 1 and day >= 20) or (month == 2 and day <= 18):
            return "Aquarius"
        if (month == 2 and day >= 19) or (month == 3 and day <= 20):
            return "Pisces"
        return ''

    def setup_google_sheets(self):
        try:
            print("🔄 Setting up Google Sheets connection...")
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            key_file_path = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("RealiteaseInfo")
            print("✅ Google Sheets connection successful - Connected to RealiteaseInfo")
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {str(e)}")
            return False

    def analyze_text_for_gender(self, text, cast_name=""):
        if not text:
            return None
        self.last_gender_analysis_source = None
        if self.gemini_model:
            snippet = re.sub(r'\s+', ' ', text).strip()[:4000]
            prompt = (
                "Return JSON with a field 'gender' (values: 'M', 'F', or empty if unknown). "
                f"Based solely on this biography text about {cast_name or 'the person'}: \"{snippet}\""
            )
            try:
                print("      🤖 Asking Gemini to infer gender from biography text...")
                response = self.gemini_model.generate_content(prompt)
                response_text = (response.text or "").strip()
                if response_text:
                    json_text = response_text
                    if json_text.startswith('```'):
                        json_text = json_text.strip('`').strip()
                        if json_text.lower().startswith('json'):
                            json_text = json_text[4:].strip()
                    import json
                    data = json.loads(json_text)
                    gender_value = str(data.get('gender', '')).strip().upper()
                    if gender_value in {'M', 'F'}:
                        print(f"      ✅ Gemini classified gender: {gender_value}")
                        self.last_gender_analysis_source = 'gemini_text_analysis'
                        return gender_value
                print("      ⚠️ Gemini did not return a clear gender value")
            except Exception as e:
                print(f"      ⚠️ Gemini gender analysis error: {e}")
        gender = self._analyze_text_for_gender_pronouns(text, cast_name)
        if gender:
            self.last_gender_analysis_source = 'text_analysis_fallback'
        return gender

    def _analyze_text_for_gender_pronouns(self, text, cast_name=""):
        text_lower = text.lower()
        cast_name_lower = cast_name.lower() if cast_name else ""
        if cast_name:
            text_lower = text_lower.replace(cast_name_lower, "PERSON")
        print(f"      🔍 Analyzing text for gender markers (fallback)...")
        explicit_male = ['gender: male', 'sex: male', 'gender: m', 'is a male', 'male actor',
                         'male contestant', 'male model', 'male singer', 'male dancer']
        explicit_female = ['gender: female', 'sex: female', 'gender: f', 'is a female',
                           'female actress', 'female contestant', 'female model', 'female singer',
                           'female dancer', 'actress']
        for term in explicit_male:
            if term in text_lower:
                print(f"      ✅ Found explicit male gender: '{term}'")
                return 'M'
        for term in explicit_female:
            if term in text_lower:
                print(f"      ✅ Found explicit female gender: '{term}'")
                return 'F'
        male_pronoun_patterns = [
            r'\bhe\s+(?:is|was|has|had|will|would|can|could|should|might)\b',
            r'\bhim\s+(?:to|from|with|by|for|as|in|on|at)\b',
            r'\bhis\s+(?:career|life|work|role|performance|appearance|family|wife|girlfriend)\b',
            r'(?:made|gave|brought|took|sent|showed|told)\s+him\b',
            r'\bhe\s+(?:appeared|starred|played|worked|lived|grew|born|died)\b'
        ]
        female_pronoun_patterns = [
            r'\bshe\s+(?:is|was|has|had|will|would|can|could|should|might)\b',
            r'\bher\s+(?:to|from|with|by|for|as|in|on|at)\b',
            r'\bher\s+(?:career|life|work|role|performance|appearance|family|husband|boyfriend)\b',
            r'(?:made|gave|brought|took|sent|showed|told)\s+her\b',
            r'\bshe\s+(?:appeared|starred|played|worked|lived|grew|born|died)\b'
        ]
        male_matches = sum(len(re.findall(pattern, text_lower)) for pattern in male_pronoun_patterns)
        female_matches = sum(len(re.findall(pattern, text_lower)) for pattern in female_pronoun_patterns)
        print(f"      📊 Contextual pronoun matches - Male: {male_matches}, Female: {female_matches}")
        simple_male = len(re.findall(r'\b(he|him|his|himself)\b', text_lower))
        simple_female = len(re.findall(r'\b(she|her|hers|herself)\b', text_lower))
        print(f"      📊 Simple pronoun count - Male: {simple_male}, Female: {simple_female}")
        male_titles = ['mr', 'mr.', 'mister', 'sir', 'king', 'prince', 'duke', 'lord',
                       'boyfriend', 'husband', 'father', 'dad', 'son', 'brother', 'uncle',
                       'nephew', 'grandfather', 'grandson', 'widower', 'bachelor']
        female_titles = ['ms', 'ms.', 'mrs', 'mrs.', 'miss', 'madam', 'lady', 'queen',
                         'princess', 'duchess', 'girlfriend', 'wife', 'mother', 'mom', 'mum',
                         'daughter', 'sister', 'aunt', 'niece', 'grandmother', 'granddaughter',
                         'widow', 'bachelorette']
        male_title_count = sum(1 for title in male_titles if re.search(r'\b' + title + r'\b', text_lower))
        female_title_count = sum(1 for title in female_titles if re.search(r'\b' + title + r'\b', text_lower))
        if male_title_count > 0 or female_title_count > 0:
            print(f"      📊 Title/relationship count - Male: {male_title_count}, Female: {female_title_count}")
        total_male_evidence = male_matches * 2 + simple_male + male_title_count * 3
        total_female_evidence = female_matches * 2 + simple_female + female_title_count * 3
        print(f"      📊 Total evidence score - Male: {total_male_evidence}, Female: {total_female_evidence}")
        if total_male_evidence >= 3 and total_male_evidence > total_female_evidence * 1.5:
            print(f"      ✅ Determined MALE based on comprehensive analysis")
            return 'M'
        if total_female_evidence >= 3 and total_female_evidence > total_male_evidence * 1.5:
            print(f"      ✅ Determined FEMALE based on comprehensive analysis")
            return 'F'
        if total_male_evidence > 0 and total_female_evidence == 0:
            print(f"      ✅ Determined MALE (only male indicators found)")
            return 'M'
        if total_female_evidence > 0 and total_male_evidence == 0:
            print(f"      ✅ Determined FEMALE (only female indicators found)")
            return 'F'
        print(f"      ⚠️ Could not determine gender conclusively")
        return None

    def process_cast_member(self, row_data, row_num):
        cast_name = row_data.get('cast_name', '')
        show_names = row_data.get('show_names', '')
        print(f"\n{'='*60}")
        print(f"🎭 Row {row_num}: {cast_name} from {show_names or 'Unknown Shows'}")
        print(f"   📊 Current data - Gender: '{row_data.get('gender') or 'EMPTY'}', "
              f"Birthday: '{row_data.get('birthday') or 'EMPTY'}', "
              f"Zodiac: '{row_data.get('zodiac') or 'EMPTY'}'")
        needs_gender = not row_data.get('gender') or row_data.get('gender').strip() == ''
        needs_birthday = not row_data.get('birthday') or row_data.get('birthday').strip() == ''
        needs_zodiac = not row_data.get('zodiac') or row_data.get('zodiac').strip() == ''
        print(f"   🎯 Needs - Gender: {needs_gender}, Birthday: {needs_birthday}, Zodiac: {needs_zodiac}")
        updates_made = {}
        field_sources = {
            'gender': 'existing' if row_data.get('gender') else None,
            'birthday': 'existing' if row_data.get('birthday') else None,
            'zodiac': 'existing' if row_data.get('zodiac') else None,
        }
        if row_data.get('birthday') and not row_data.get('zodiac'):
            zodiac = self.calculate_zodiac(row_data['birthday'])
            if zodiac:
                updates_made['zodiac'] = zodiac
                row_data['zodiac'] = zodiac
                field_sources['zodiac'] = 'calculated_from_existing_birthday'
                print(f"   ♈ Calculated zodiac from existing birthday: {zodiac}")
        if needs_gender or needs_birthday:
            print("🔍 Searching for missing data...")
            all_text_content = ""
            show_tmdb_ids = row_data.get('show_tmdb_ids', '')
            cast_tmdb_id = (row_data.get('cast_tmdb_id') or '').strip()
            sources_to_try = []
            if self.tmdb_api_key and cast_tmdb_id:
                sources_to_try.append(('tmdb', lambda _name, _show, name=cast_name, shows=show_tmdb_ids, person_id=cast_tmdb_id: self.search_tmdb_cast(name, shows, person_id)))
            sources_to_try.append(('fandom_wiki', self.search_fandom_wiki))
            if self.tmdb_api_key and not cast_tmdb_id:
                sources_to_try.append(('tmdb', lambda _name, _show, name=cast_name, shows=show_tmdb_ids: self.search_tmdb_cast(name, shows, '')))
            sources_to_try.append(('famous_birthdays', self.search_famous_birthdays))
            if self.gemini_model:
                sources_to_try.append(('gemini', self.search_gemini))
            for source_name, source_func in sources_to_try:
                if (not needs_birthday or row_data.get('birthday')) and (not needs_gender or row_data.get('gender')):
                    print(f"   ✅ All needed data found, stopping search")
                    break
                print(f"   🔍 Trying {source_name}...")
                try:
                    result = source_func(cast_name, show_names)
                except Exception as e:
                    print(f"      ❌ {source_name} failed with error: {str(e)}")
                    result = None
                if result:
                    if result.get('bio'):
                        all_text_content += " " + result['bio']
                    if source_name == 'fandom_wiki' and result.get('gender') and result.get('bio'):
                        self.last_gender_analysis_source = 'text_analysis_fallback'
                    if needs_birthday and result.get('birthday') and not row_data.get('birthday'):
                        updates_made['birthday'] = result['birthday']
                        row_data['birthday'] = result['birthday']
                        field_sources['birthday'] = source_name
                        zodiac = self.calculate_zodiac(result['birthday'])
                        if zodiac and (needs_zodiac and not row_data.get('zodiac')):
                            updates_made['zodiac'] = zodiac
                            row_data['zodiac'] = zodiac
                            field_sources['zodiac'] = 'calculated_from_new_birthday'
                            print(f"   ♈ Calculated zodiac: {zodiac}")
                        self.track_source(f"{source_name}_birthday")
                        print(f"   ✅ Added birthday: {result['birthday']}")
                    if needs_gender and result.get('gender') and not row_data.get('gender'):
                        updates_made['gender'] = result['gender']
                        row_data['gender'] = result['gender']
                        field_sources['gender'] = source_name
                        if source_name == 'fandom_wiki' and self.last_gender_analysis_source:
                            self.track_source('text_analysis_gender')
                        else:
                            self.track_source(f"{source_name}_gender")
                        print(f"   ✅ Added gender: {result['gender']}")
                    if needs_birthday and result.get('birthday') and row_data.get('birthday') and result['birthday'] != row_data['birthday']:
                        print(f"   ℹ️ Preserving existing birthday: {row_data.get('birthday')} (found different: {result.get('birthday')})")
                    if needs_gender and result.get('gender') and row_data.get('gender') and result['gender'] != row_data['gender']:
                        print(f"   ℹ️ Preserving existing gender: {row_data.get('gender')} (found different: {result.get('gender')})")
            if needs_gender and not row_data.get('gender') and all_text_content:
                print("   🔍 Analyzing collected text for gender...")
                gender = self.analyze_text_for_gender(all_text_content, cast_name)
                if gender:
                    updates_made['gender'] = gender
                    row_data['gender'] = gender
                    source_key = self.last_gender_analysis_source or 'text_analysis_fallback'
                    field_sources['gender'] = source_key
                    if source_key == 'gemini_text_analysis':
                        self.track_source('gemini_text_analysis_gender')
                    else:
                        self.track_source('text_analysis_gender')
                    print(f"   ✅ Gender from text analysis: {gender}")
            elif row_data.get('gender'):
                print(f"   ℹ️ Preserving existing gender: {row_data.get('gender')}")
        else:
            print("   ✅ All data already present, no search needed")
        if updates_made:
            print(f"   📝 Updates to apply: {list(updates_made.keys())}")
        else:
            print(f"   ℹ️ No new data to add")
        self.log_cast_summary(row_data, field_sources, updates_made)
        return updates_made

    def search_fandom_wiki(self, cast_name, show_names):
        try:
            if not show_names:
                print(f"      ⚠️ No show names provided for fandom search")
                return None
            wiki_domain = None
            shows_list = [s.strip() for s in show_names.split(',')]
            for show in shows_list:
                wiki_domain = self.show_to_wiki.get(show.lower())
                if wiki_domain:
                    print(f"      🎯 Found wiki domain: {wiki_domain} for show: {show}")
                    break
            if not wiki_domain:
                print(f"      ⚠️ No matching fandom wiki found for shows: {shows_list}")
                return None
            clean_name = cast_name.replace('.', '').replace("'", "")
            urls_to_try = [
                f"https://{wiki_domain}/wiki/{clean_name.title().replace(' ', '_')}",
                f"https://{wiki_domain}/wiki/{clean_name.replace(' ', '_')}",
                f"https://{wiki_domain}/wiki/{clean_name.title().replace(' ', '-')}",
                f"https://{wiki_domain}/wiki/{clean_name.replace(' ', '-')}",
                f"https://{wiki_domain}/wiki/{clean_name.lower().replace(' ', '_')}",
                f"https://{wiki_domain}/wiki/{clean_name.lower().replace(' ', '-')}",
            ]
            for url in urls_to_try:
                print(f"      🔗 Trying fandom URL: {url}")
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    page_text = soup.get_text().lower()
                    if 'this page is a redirect' in page_text or 'disambiguation' in page_text:
                        print(f"      ⚠️ Page is redirect/disambiguation, skipping")
                        continue
                    result = {}
                    birthday = self.extract_date_from_page(soup)
                    if birthday:
                        result['birthday'] = birthday
                        print(f"      ✅ Found birthday: {birthday}")
                    gender = self.analyze_text_for_gender(page_text, cast_name)
                    if gender:
                        result['gender'] = gender
                        print(f"      ✅ Found gender: {gender}")
                    result['bio'] = soup.get_text()[:1000] if soup.get_text() else ""
                    if result:
                        print(f"      ✅ Successfully scraped {wiki_domain}")
                        return result
                else:
                    print(f"      ❌ HTTP {response.status_code} for {url}")
            print(f"      ⚠️ No valid pages found on {wiki_domain}")
            return None
        except Exception as e:
            print(f"      ❌ Fandom wiki error: {str(e)}")
            return None

    def search_gemini(self, cast_name, show_name):
        if not self.gemini_model:
            return None
        show_context = f" They appear on: {show_name}." if show_name else ""
        prompt = (
            "Provide a JSON object with fields 'birthday' and 'gender' for the reality TV personality "
            f"{cast_name}.{show_context} Birthday must be in YYYY-MM-DD format if known, otherwise use an empty string. "
            "Gender should be 'M', 'F', or empty if unknown. Respond with JSON only and do not include explanations."
        )
        try:
            response = self.gemini_model.generate_content(prompt)
            text = (response.text or "").strip()
            if not text:
                return None
            json_text = text
            if json_text.startswith('```'):
                json_text = json_text.strip('`').strip()
                if json_text.lower().startswith('json'):
                    json_text = json_text[4:].strip()
            import json
            data = json.loads(json_text)
            birthday_raw = str(data.get('birthday', '') or '').strip()
            gender_raw = str(data.get('gender', '') or '').strip().upper()
            result = {}
            if birthday_raw:
                parsed = parse_date_text_to_yyyy_mm_dd(birthday_raw)
                if parsed:
                    result['birthday'] = parsed
            if gender_raw in {'M', 'F'}:
                result['gender'] = gender_raw
            if result:
                print("      ✅ Gemini provided data")
            else:
                print("      ⚠️ Gemini did not return usable data")
            return result or None
        except Exception as e:
            message = str(e)
            print(f"      ⚠️ Gemini error (model {self.gemini_model_name}): {message}")
            if '404' in message and 'models/' in message:
                print("      ℹ️ Hint: Update GOOGLE_GEMINI_MODEL to an available model")
            return None

    def search_tmdb_cast(self, cast_name, show_tmdb_ids, cast_tmdb_id=''):
        if not self.tmdb_api_key:
            return None
        first, last = extract_first_last(cast_name)
        if not first or not last:
            return None
        if cast_tmdb_id and cast_tmdb_id.isdigit():
            details = self.get_tmdb_person_details(int(cast_tmdb_id))
            if not details:
                return None
            result = {}
            birthday = details.get('birthday')
            if birthday:
                result['birthday'] = birthday
            gender_val = details.get('gender')
            if gender_val == 1:
                result['gender'] = 'F'
            elif gender_val == 2:
                result['gender'] = 'M'
            biography = details.get('biography')
            if biography:
                result['bio'] = biography[:1000]
            if result:
                print("      ✅ TMDb (person lookup) provided data")
                return result
            return None
        if not show_tmdb_ids:
            return None
        show_ids = [sid.strip() for sid in show_tmdb_ids.split(',') if sid.strip().isdigit()]
        for show_id in show_ids[: self.tmdb_show_limit]:
            print(f"      🔍 Searching TMDb show {show_id} for {cast_name}")
            credits = self.get_tmdb_aggregate_credits(show_id)
            if not credits:
                continue
            for bucket in ('cast', 'crew'):
                for person in credits.get(bucket, []):
                    candidate_name = person.get('name') or person.get('original_name') or ''
                    if names_match_exact(first, last, candidate_name):
                        tmdb_id = person.get('id')
                        print(f"      🎯 TMDb match: {candidate_name} (ID {tmdb_id})")
                        details = self.get_tmdb_person_details(tmdb_id)
                        if not details:
                            continue
                        result = {}
                        birthday = details.get('birthday')
                        if birthday:
                            result['birthday'] = birthday
                        gender_val = details.get('gender')
                        if gender_val == 1:
                            result['gender'] = 'F'
                        elif gender_val == 2:
                            result['gender'] = 'M'
                        biography = details.get('biography')
                        if biography:
                            result['bio'] = biography[:1000]
                        if result:
                            print("      ✅ TMDb provided data")
                            return result
        print("      ⚠️ TMDb search found no matching person")
        return None

    def search_famous_birthdays(self, cast_name, show_name):
        try:
            name_slug = cast_name.lower().replace(' ', '-').replace('.', '').replace("'", "")
            urls_to_try = [f"https://www.famousbirthdays.com/people/{name_slug}.html"]
            name_parts = cast_name.split()
            if len(name_parts) == 2:
                urls_to_try.append(f"https://www.famousbirthdays.com/people/{name_parts[1].lower()}-{name_parts[0].lower()}.html")
            for url in urls_to_try:
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    result = {}
                    page_text = soup.get_text()
                    birthday = self.extract_famous_birthdays_date(soup)
                    if birthday:
                        result['birthday'] = birthday
                    gender = self.analyze_text_for_gender(page_text, cast_name)
                    if gender:
                        result['gender'] = gender
                    result['bio'] = page_text[:1000] if page_text else ""
                    if result:
                        print(f"      ✅ Found on Famous Birthdays")
                        return result
            return None
        except Exception as e:
            return None

    def scrape_url_for_data(self, url, cast_name):
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return None
            soup = BeautifulSoup(response.text, 'html.parser')
            result = {}
            page_text = soup.get_text()
            birthday = self.extract_date_from_page(soup)
            if birthday:
                result['birthday'] = birthday
            gender = self.analyze_text_for_gender(page_text, cast_name)
            if gender:
                result['gender'] = gender
            result['bio'] = page_text[:1000] if page_text else ""
            return result if result else None
        except Exception as e:
            return None

    def extract_date_from_page(self, soup):
        try:
            infobox_label_keys = {
                "born","birth","birth date","birthdate","date of birth","dob","birthday","birth_date",
                "born on","birth_day","birthplace","date born","birth date:"
            }
            bday_span = soup.find("span", class_="bday")
            if bday_span:
                dt = bday_span.get_text(strip=True)
                parsed_date = self.parse_date_string(dt)
                if parsed_date:
                    return parsed_date
            time_tag = soup.find("time", attrs={"itemprop": "birthDate"})
            if time_tag and time_tag.has_attr("datetime"):
                dt = time_tag["datetime"].strip()
                parsed_date = self.parse_date_string(dt)
                if parsed_date:
                    return parsed_date
            infobox_selectors = [
                ".portable-infobox .pi-item.pi-data",
                ".portable-infobox .pi-data",
                ".infobox .pi-item.pi-data",
                ".infobox .pi-data"
            ]
            for selector in infobox_selectors:
                nodes = soup.select(selector)
                for node in nodes:
                    ds = (node.get("data-source") or "").strip().lower()
                    if ds in infobox_label_keys or ds == "born" or any(key in ds for key in ["birth", "born"]):
                        val = node.select_one(".pi-data-value")
                        if val:
                            val_text = val.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date
                    lab = node.select_one(".pi-data-label")
                    val = node.select_one(".pi-data-value")
                    if lab and val:
                        lab_txt = (lab.get_text(" ", strip=True) or "").lower()
                        if any(k in lab_txt for k in infobox_label_keys):
                            val_text = val.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date
            for td in soup.find_all("td"):
                if td.find("b") or td.find("strong"):
                    label_text = td.get_text(strip=True).lower()
                    if any(k in label_text for k in infobox_label_keys):
                        next_td = td.find_next_sibling("td")
                        if next_td:
                            val_text = next_td.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label_text = tds[0].get_text(strip=True).lower()
                    if any(k in label_text for k in infobox_label_keys):
                        val_text = tds[1].get_text(" ", strip=True)
                        parsed_date = self.parse_date_string(val_text)
                        if parsed_date:
                            return parsed_date
            return None
        except Exception as e:
            return None

    def extract_famous_birthdays_date(self, soup):
        try:
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    import json
                    data = json.loads(script.get_text())
                    if 'birthDate' in data:
                        birth_date = data['birthDate']
                        if 'T' in birth_date:
                            birth_date = birth_date.split('T')[0]
                        return birth_date
                except:
                    continue
            bio_section = soup.find('div', class_='bio-module')
            if bio_section:
                bio_text = bio_section.get_text()
                parsed_date = self.parse_date_string(bio_text)
                if parsed_date:
                    return parsed_date
            page_text = soup.get_text()
            patterns = [
                r'Birthday\s*[:]*\s*(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',
                r'Born\s*[:]*\s*(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',
                r'(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',
            ]
            for pattern in patterns:
                import re as _re
                match = _re.search(pattern, page_text, _re.IGNORECASE)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        month, day, year = groups
                        date_str = f"{month} {day} {year}"
                        parsed_date = self.parse_date_string(date_str)
                        if parsed_date:
                            return parsed_date
            return None
        except Exception as e:
            return None

    def parse_date_string(self, date_str):
        if not date_str:
            return None
        DATE_MONTHS = {
            "january": "01","february": "02","march": "03","april": "04","may": "05","june": "06",
            "july": "07","august": "08","september": "09","october": "10","november": "11","december": "12",
            "jan": "01","feb": "02","mar": "03","apr": "04","may": "05","jun": "06","jul": "07","aug": "08",
            "sep": "09","sept": "09","oct": "10","nov": "11","dec": "12"
        }
        def normalize_text(s):
            return re.sub(r"\s+", " ", (s or "").strip())
        t = re.sub(r'\s*\(age.*?\)', '', (date_str or '')).strip()
        t = re.sub(r'\s*\(.*?\)', '', t)
        t = re.sub(r'^(birthdate|birthday|born|date of birth)', '', t, flags=re.IGNORECASE)
        t = re.sub(r"\s+", " ", t).lower()
        m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", t)
        if m:
            y, mo, d = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        m = re.search(r"\b([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", t)
        if m:
            mon, day, year = m.groups()
            mm = DATE_MONTHS.get(mon.lower())
            if mm:
                return f"{year}-{mm}-{int(day):02d}"
        m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)\s+(\d{4})\b", t)
        if m:
            day, mon, year = m.groups()
            mm = DATE_MONTHS.get(mon.lower())
            if mm:
                return f"{year}-{mm}-{int(day):02d}"
        m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
        if m:
            mo, d, y = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"
        m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", t)
        if m:
            first, second, year = m.groups()
            if int(first) > 12:
                return f"{year}-{int(second):02d}-{int(first):02d}"
            return f"{year}-{int(first):02d}-{int(second):02d}"
        return None

    def track_source(self, source_name):
        if source_name not in self.sources_used:
            self.sources_used[source_name] = 0
        self.sources_used[source_name] += 1

    def maybe_log_interval_progress(self):
        now = time.time()
        if now - self.interval_start_time >= 300:
            interval_updates = self.updated_count - self.interval_start_updates
            print(f"⏱️ 5-minute progress: {interval_updates} rows updated (total {self.updated_count})")
            self.interval_start_time = now
            self.interval_start_updates = self.updated_count

    def log_cast_summary(self, row_data, field_sources, updates_made):
        cast_name = (row_data.get('cast_name') or 'Unknown').strip()
        gender_value = (row_data.get('gender') or '').strip()
        birthday_value = (row_data.get('birthday') or '').strip()
        zodiac_value = (row_data.get('zodiac') or '').strip()
        name_display = cast_name.upper()
        gender_display = self.format_gender_for_log(gender_value)
        birthday_display = self.format_birthday_for_log(birthday_value)
        zodiac_display = zodiac_value.upper() if zodiac_value else 'UNKNOWN'
        print(f"   📘 Summary Log: {name_display}, {gender_display}, {birthday_display}, ZODIAC ({zodiac_display})")
        source_details = []
        for field, label in [('gender', 'Gender'), ('birthday', 'Birthday'), ('zodiac', 'Zodiac')]:
            value_present = bool(row_data.get(field))
            if not value_present:
                source_details.append(f"{label}: Not found")
                continue
            source_key = field_sources.get(field)
            source_label = self.describe_source_label(source_key)
            if field in updates_made:
                source_label += " (new)"
            source_details.append(f"{label}: {source_label}")
        if source_details:
            print("   📗 Source Detail: " + " | ".join(source_details))

    def format_gender_for_log(self, gender_value):
        if not gender_value:
            return 'UNKNOWN'
        gender_upper = gender_value.upper()
        if gender_upper in {'M', 'MALE'}:
            return 'MALE'
        if gender_upper in {'F', 'FEMALE'}:
            return 'FEMALE'
        return gender_upper

    def format_birthday_for_log(self, birthday_value):
        if not birthday_value:
            return 'UNKNOWN'
        birthday_clean = birthday_value.strip()
        try:
            dt = datetime.strptime(birthday_clean, '%Y-%m-%d')
            return dt.strftime('%m-%d-%Y')
        except ValueError:
            pass
        try:
            dt = datetime.strptime(birthday_clean, '%m-%d-%Y')
            return dt.strftime('%m-%d-%Y')
        except ValueError:
            pass
        return birthday_clean.upper()

    def describe_source_label(self, source_key):
        source_map = {
            'existing': 'Existing spreadsheet value',
            'fandom_wiki': 'Fandom Wiki',
            'tmdb': 'TMDb aggregate credits',
            'famous_birthdays': 'Famous Birthdays',
            'imdb': 'IMDb',
            'gemini': 'Gemini AI lookup',
            'gemini_text_analysis': 'Gemini text analysis',
            'text_analysis_fallback': 'Text analysis fallback',
            'calculated_from_existing_birthday': 'Calculated from existing birthday',
            'calculated_from_new_birthday': 'Calculated from new birthday',
        }
        if not source_key:
            return 'Unknown source'
        return source_map.get(source_key, source_key.replace('_', ' ').title())

    def add_to_batch(self, row_num, updates):
        if updates:
            self.batch_updates.append((row_num, updates))
            if len(self.batch_updates) >= self.batch_size:
                self.process_batch()

    def process_batch(self):
        if not self.batch_updates:
            return
        print(f"\n📤 Processing batch of {len(self.batch_updates)} updates...")
        try:
            requests_payload = []
            for row_num, updates in self.batch_updates:
                for field, value in updates.items():
                    column_map = {
                        'gender': REALITEASE_COLUMN_NUMBER['gender'],
                        'birthday': REALITEASE_COLUMN_NUMBER['birthday'],
                        'zodiac': REALITEASE_COLUMN_NUMBER['zodiac'],
                    }
                    if field in column_map:
                        column_letter = column_number_to_letter(column_map[field])
                        cell_range = f"RealiteaseInfo!{column_letter}{row_num}"
                        requests_payload.append({
                            'range': cell_range,
                            'values': [[str(value)]]
                        })
            if requests_payload:
                body = {
                    'valueInputOption': 'RAW',
                    'data': requests_payload
                }
                self.worksheet.spreadsheet.values_batch_update(body)
                print(f"   ✅ Batch update successful: {len(self.batch_updates)} rows updated")
                self.updated_count += len(self.batch_updates)
                self.maybe_log_interval_progress()
            self.batch_updates = []
            time.sleep(1)
        except Exception as e:
            print(f"   ❌ Batch update failed: {e}")
            for row_num, updates in self.batch_updates:
                self.update_spreadsheet_single(row_num, updates)
            self.batch_updates = []

    def update_spreadsheet_single(self, row_num, updates):
        try:
            columns = {
                'gender': REALITEASE_COLUMN_NUMBER['gender'],
                'birthday': REALITEASE_COLUMN_NUMBER['birthday'],
                'zodiac': REALITEASE_COLUMN_NUMBER['zodiac'],
            }
            updated_fields = 0
            for field, value in updates.items():
                if field in columns:
                    self.worksheet.update_cell(row_num, columns[field], value)
                    time.sleep(0.3)
                    updated_fields += 1
            if updated_fields:
                self.updated_count += 1
                self.maybe_log_interval_progress()
            return True
        except Exception as e:
            print(f"   ❌ Failed to update row {row_num}: {e}")
            return False

    def process_range(self):
        if not self.setup_google_sheets():
            return False
        try:
            print("📊 Loading spreadsheet data...")
            all_data = self.worksheet.get_all_values()
            total_rows = len(all_data)
            print(f"📊 Total rows in spreadsheet: {total_rows}")
            print("\n🎯 Select processing range:")
            print(f"1. Process all rows (2 to {total_rows})")
            print("2. Process specific range")
            print("3. Process from specific row to end")
            print("4. Process rows with missing data only")
            print("5. Process all rows (bottom-up)")
            choice = input("\nEnter choice (1-5): ").strip()
            if choice == '1':
                start_row = 2
                end_row = total_rows
                reverse_processing = False
            elif choice == '2':
                start_row = int(input("Enter start row: "))
                end_row = int(input("Enter end row: "))
                reverse_processing = False
            elif choice == '3':
                start_row = int(input("Enter start row: "))
                end_row = total_rows
                reverse_processing = False
            elif choice == '4':
                start_row = 2
                end_row = total_rows
                print("Will process only rows with missing gender/birthday/zodiac data")
                reverse_processing = False
            elif choice == '5':
                start_row = 2
                end_row = total_rows
                reverse_processing = True
            else:
                print("❌ Invalid choice")
                return False
            if start_row < 2:
                start_row = 2
            if end_row > total_rows:
                end_row = total_rows
            print(f"\n📊 Processing rows {start_row} to {end_row}")
            print(f"📊 Maximum rows to process: {end_row - start_row + 1}")
            print(f"📊 Batch updates every {self.batch_size} rows")
            if choice not in {'5'}:
                reverse_input = input("Process bottom-up? (y/n): ").strip().lower()
                reverse_processing = reverse_input == 'y'
            print("\n📚 Available data sources:")
            print("   ✅ Gemini AI lookup")
            print("   ✅ Fandom Wikis (show-specific)")
            print("   ✅ TMDb aggregate credits")
            print("   ✅ Famous Birthdays")
            print("   ✅ Gemini-driven gender analysis fallback")
            confirm = input("\nProceed? (y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ Cancelled by user")
                return False
            if reverse_processing:
                row_numbers = range(end_row, start_row - 1, -1)
            else:
                row_numbers = range(start_row, end_row + 1)
            for row_num in row_numbers:
                row_index = row_num - 1
                row = all_data[row_index]
                row_data = {
                    'cast_name': get_realitease_value(row, 'cast_name'),
                    'cast_imdb_id': get_realitease_value(row, 'cast_imdb_id'),
                    'cast_tmdb_id': get_realitease_value(row, 'cast_tmdb_id'),
                    'show_names': get_realitease_value(row, 'show_names'),
                    'show_imdb_ids': get_realitease_value(row, 'show_imdb_ids'),
                    'show_tmdb_ids': get_realitease_value(row, 'show_tmdb_ids'),
                    'total_shows': get_realitease_value(row, 'total_shows'),
                    'total_seasons': get_realitease_value(row, 'total_seasons'),
                    'total_episodes': get_realitease_value(row, 'total_episodes'),
                    'gender': get_realitease_value(row, 'gender'),
                    'birthday': get_realitease_value(row, 'birthday'),
                    'zodiac': get_realitease_value(row, 'zodiac'),
                }
                if not row_data['cast_name']:
                    continue
                if choice == '4':
                    has_all_data = all([
                        row_data['gender'],
                        row_data['birthday'],
                        row_data['zodiac']
                    ])
                    if has_all_data:
                        self.skipped_count += 1
                        continue
                self.processed_count += 1
                updates = self.process_cast_member(row_data, row_num)
                if updates:
                    self.add_to_batch(row_num, updates)
                else:
                    self.skipped_count += 1
                if self.processed_count % 10 == 0:
                    self.print_progress()
            if self.batch_updates:
                print("\n📤 Processing final batch...")
                self.process_batch()
            print("\n" + "="*60)
            print("🎉 Processing complete!")
            self.print_final_summary()
            return True
        except Exception as e:
            print(f"❌ Error processing range: {e}")
            import traceback
            traceback.print_exc()
            return False

    def print_progress(self):
        success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
        print(f"\n📊 Progress Report ({self.processed_count} rows processed):")
        print(f"   • Updated: {self.updated_count} ({success_rate:.1f}%)")
        print(f"   • Skipped: {self.skipped_count}")
        print(f"   • In batch queue: {len(self.batch_updates)}")

    def print_final_summary(self):
        success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
        print(f"\n📊 Final Statistics:")
        print(f"   • Total processed: {self.processed_count}")
        print(f"   • Successfully updated: {self.updated_count}")
        print(f"   • Skipped (no new data): {self.skipped_count}")
        print(f"   • Success rate: {success_rate:.1f}%")
        if self.sources_used:
            print(f"\n📚 Data sources breakdown:")
            for source, count in sorted(self.sources_used.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / sum(self.sources_used.values()) * 100)
                print(f"   • {source}: {count} ({percentage:.1f}%)")


def main():
    print("🚀 Starting Reality TV Data Scraper for RealiteaseInfo...")
    print("📝 This tool will enhance your cast data with:")
    print("   • Gender (Column H) - with Gemini-assisted analysis")
    print("   • Birthday (Column I)")
    print("   • Zodiac Sign (Column J) - auto-calculated from birthday")
    print("\n✨ Features:")
    print("   • Gemini-first gender detection with smart fallback analysis")
    print("   • Automatic zodiac calculation")
    bootstrap_scraper = FamousBirthdaysEnhancer()
    print(f"   • Batch updates every {bootstrap_scraper.batch_size} rows for efficiency")
    print("\n📚 Data sources:")
    print("   1. TMDb aggregate credits")
    print("   2. Fandom Wikis (show-specific)")
    print("   3. Famous Birthdays")
    print("   4. Gemini AI lookup")
    print("   5. Gemini-driven gender analysis fallback\n")
    scraper = bootstrap_scraper
    success = scraper.process_range()
    if success:
        print("\n✅ Data enhancement completed successfully!")
    else:
        print("\n❌ Data enhancement failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()

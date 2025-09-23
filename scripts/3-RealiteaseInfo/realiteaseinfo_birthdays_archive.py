#!/usr/bin/env python3
"""
Fill Birthday (and Zodiac) in RealiteaseInfo by scraping Fandom/Wikia.

RealiteaseInfo columns:
  A: CastName
  B: CastIMDbID
  C: CastTMDbID
  D: ShowNames (comma-separated)
  E: ShowIMDbIDs
  F: ShowTMDbIDs
  G: TotalShows
  H: TotalSeasons
  I: TotalEpisodes
  J: Gender
  K: Birthday (YYYY-MM-DD)
  L: Zodiac

What this script does:
- For each row missing Birthday (I), try to find the cast member's page on the *most relevant Fandom wiki*
  for any of the shows listed in ShowNames (D).
- Query the wiki’s MediaWiki API to find the best article match for the person’s name.
- Fetch the article HTML and parse the portable-infobox for a birthday field (handles many label/data-source variants,
  including <time datetime="YYYY-MM-DD">).
- Normalize to YYYY-MM-DD when possible; if only month/day/year text exists, we parse and normalize.
- If found, write I (birthday) and J (zodiac) if empty.
- Rate-limited and robust headers.

Usage:
  python "scripts/Person Details/enhance_realitease_fandom_birthdays.py" --start-row 2 --limit 100 --rate-delay 1.5
"""

import os
import re
import time
import json
import argparse
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import gspread
from dotenv import load_dotenv

# ------------------------------------------------------------
# Config & bootstrap
# ------------------------------------------------------------
load_dotenv()

SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_KEY_PATH = BASE_DIR / "keys" / "trr-backend-df2c438612e1.json"
SA_KEYFILE = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", str(DEFAULT_KEY_PATH))).expanduser()
if not SA_KEYFILE.is_absolute():
    SA_KEYFILE = (SCRIPT_DIR / SA_KEYFILE).resolve()

# Conservative headers (avoid brotli unless you install brotli/brotlicffi)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",  # drop 'br' unless you install brotli
    "Connection": "keep-alive",
}

# ------------------------------------------------------------
# Show → preferred Fandom wiki domains
# (Multiple domains per show allowed; we’ll try in order.)
# This is a best-effort curated list + sensible fallbacks.
# ------------------------------------------------------------
SHOW_TO_WIKIS: Dict[str, List[str]] = {
    # Housewives / Bravo-verse
    "The Real Housewives of Atlanta": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of New Jersey": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of New York City": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Beverly Hills": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Miami": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Orange County": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Salt Lake City": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Dubai": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Dallas": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Potomac": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of D.C.": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives Ultimate Girls Trip": ["realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Atlanta: Kandi's Wedding": ["realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of New Jersey: Teresa Checks In": ["realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Real Housewives of Atlanta: Porsha's Family Matters": ["realhousewives.fandom.com", "realitytv-girl.fandom.com"],

    "Vanderpump Rules": ["vanderpumprules.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "The Valley": ["vanderpumprules.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Vanderpump Villa": ["vanderpumpvilla.fandom.com", "vanderpumprules.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Summer House": ["summerhousebravo.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Winter House": ["winterhouse.fandom.com", "summerhousebravo.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Southern Charm": ["southerncharm.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Southern Charm Savannah": ["southerncharm.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Married to Medicine": ["married2med.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Shahs of Sunset": ["shahsofsunset.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Don't Be Tardy": ["dontbetardy.fandom.com", "realhousewives.fandom.com", "realitytv-girl.fandom.com"],
    "Kandi & The Gang": ["realhousewives.fandom.com", "realitytv-girl.fandom.com"],

    # Love & Hip Hop franchise
    "Love & Hip Hop": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love & Hip Hop: Atlanta": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love & Hip Hop: New York": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love & Hip Hop: Hollywood": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love & Hip Hop: Miami": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love and Hip Hop": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love and Hip Hop: Atlanta": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love and Hip Hop: New York": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love and Hip Hop: Hollywood": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],
    "Love and Hip Hop: Miami": ["love-hip-hop.fandom.com", "realitytv-girl.fandom.com"],

    # Below Deck-verse
    "Below Deck": ["belowdeck.fandom.com"],
    "Below Deck Mediterranean": ["belowdeckmed.fandom.com", "belowdeck.fandom.com"],
    "Below Deck Sailing Yacht": ["belowdecksailingyacht.fandom.com", "belowdeck.fandom.com"],
    "Below Deck Adventure": ["belowdeckadventure.fandom.com", "belowdeck.fandom.com"],
    "Below Deck Down Under": ["belowdeck.fandom.com"],

    # Competition / CBS / NBC / ABC
    "Big Brother": ["bigbrother.fandom.com"],
    "Celebrity Big Brother": ["bigbrother.fandom.com"],
    "Big Brother Reindeer Games": ["bigbrother.fandom.com"],

    "Survivor": ["survivor.fandom.com"],
    "The Amazing Race": ["theamazingrace.fandom.com"],

    "The Bachelor": ["bachelor-nation.fandom.com", "thebachelor.fandom.com"],
    "The Bachelorette": ["bachelor-nation.fandom.com", "thebachelorette.fandom.com"],
    "Bachelor in Paradise": ["bachelor-nation.fandom.com"],

    "The Traitors": ["thetraitors.fandom.com"],
    "The Traitors (US)": ["thetraitors.fandom.com"],

    "Project Runway": ["projectrunway.fandom.com"],
    "Project Runway All Stars": ["projectrunway.fandom.com"],
    "Top Chef": ["topchef.fandom.com"],
    "MasterChef": ["masterchef.fandom.com"],
    "Worst Cooks in America": ["worstcooksinamerica.fandom.com"],

    "Dancing with the Stars": ["dancingwiththestars.fandom.com"],
    "America's Got Talent": ["agt.fandom.com"],
    "American Idol": ["americanidol.fandom.com"],
    "The Voice": ["thevoice.fandom.com"],
    "Shark Tank": ["sharktank.fandom.com"],
    "Are You Smarter Than a Celebrity": ["areyousmarter.fandom.com"],

    # Netflix / MTV / E! / etc.
    "The Challenge": ["thechallenge.fandom.com"],
    "The Challenge: All Stars": ["thechallenge.fandom.com"],
    "The Challenge: USA": ["thechallenge.fandom.com"],

    "Love Island": ["loveisland.fandom.com"],
    "Love Island: All Stars": ["loveisland.fandom.com"],
    "Love Island Games": ["loveisland.fandom.com"],
    "Love Island: Beyond the Villa": ["loveisland.fandom.com"],

    "Too Hot to Handle": ["toohottohandle.fandom.com"],
    "Perfect Match": ["perfectmatchnetflix.fandom.com", "toohottohandle.fandom.com"],

    "The Circle": ["thecircle-netflix.fandom.com"],
    "Selling Sunset": ["selling-sunset.fandom.com"],
    "Selling The OC": ["selling-the-oc.fandom.com"],
    "Buying Beverly Hills": ["buying-beverly-hills.fandom.com"],

    "Dance Moms": ["dancemoms.fandom.com"],
    "Jersey Shore": ["jerseyshore.fandom.com"],
    "Jersey Shore: Family Vacation": ["jerseyshore.fandom.com"],
    "Siesta Key": ["siestakey.fandom.com"],
    "The Real World": ["mtvrealworld.fandom.com"],

    "RuPaul's Drag Race": ["rupaulsdragrace.fandom.com"],
    "RuPaul's Drag Race All Stars": ["rupaulsdragrace.fandom.com"],
    "RuPaul's Drag Race Global All Stars": ["rupaulsdragrace.fandom.com"],

    "Keeping Up with the Kardashians": ["kardashians.fandom.com"],
    "The Kardashians": ["kardashians.fandom.com"],
    "Kourtney and Khloé Take Miami": ["kardashians.fandom.com"],
    "Kourtney & Khloé Take the Hamptons": ["kardashians.fandom.com"],
    "Kourtney and Kim Take New York": ["kardashians.fandom.com"],
    "Dash Dolls": ["kardashians.fandom.com"],
    "I Am Cait": ["kardashians.fandom.com"],
    "Khloé & Lamar": ["kardashians.fandom.com"],
    "Rob & Chyna": ["kardashians.fandom.com"],
    "Life of Kylie": ["kardashians.fandom.com"],
    "Paris in Love": ["kardashians.fandom.com"],

    "America's Next Top Model": ["antm.fandom.com", "americasnexttopmodel.fandom.com"],
    "Bad Girls Club": ["badgirlsclub.fandom.com"],

    # Peacock / FOX / misc.
    "House of Villains": ["houseofvillains.fandom.com"],
    "The Masked Singer": ["themaskedsinger.fandom.com"],
    "Celebrity Family Feud": ["familyfeud.fandom.com"],

    # Business/Bravo/etc.
    "Million Dollar Listing Los Angeles": ["milliondollarlisting.fandom.com"],
    "Million Dollar Listing New York": ["milliondollarlisting.fandom.com"],

    # Other reality franchises (best-effort guesses + fallbacks)
    "Sister Wives": ["sister-wives.fandom.com"],
    "Love Is Blind": ["love-is-blind.fandom.com"],
    "Botched": ["botched.fandom.com"],
    "The Hills": ["thehills.fandom.com"],
    "The Hills: New Beginnings": ["thehills.fandom.com"],
    "The Osbournes": ["theosbournes.fandom.com"],
    "The Simple Life": ["thesimplelife.fandom.com"],
    "Ex on the Beach": ["exonthebeach.fandom.com"],
    "1000-lb Sisters": ["1000lb-sisters.fandom.com"],
    "Welcome to Plathville": ["plathville.fandom.com"],

    # If a show is missing here, we’ll fall back to global Fandom search.
}

GLOBAL_FANDOM_SEARCH_API = "https://www.fandom.com/api/v1/Search/List"

DATE_MONTHS = {
    "january": "01","february": "02","march": "03","april": "04","may": "05","june": "06",
    "july": "07","august": "08","september": "09","october": "10","november": "11","december": "12",
    "jan": "01","feb": "02","mar": "03","apr": "04","may": "05","jun": "06","jul": "07","aug": "08",
    "sep": "09","sept": "09","oct": "10","nov": "11","dec": "12"
}

INFOBOX_LABEL_KEYS = {
    # Common infobox label texts OR data-source values on Fandom portable-infobox
    "born","birth","birth date","birthdate","date of birth","dob","birthday","birth_date",
    "born on","birth_day","birthplace","date born","birth date:"
}

def calculate_zodiac(birthday: str) -> str:
    if not birthday or len(birthday) < 10:
        return ""
    try:
        y, m, d = birthday.split("-")
        m, d = int(m), int(d)
    except Exception:
        return ""
    if (m == 3 and d >= 21) or (m == 4 and d <= 19): return "Aries"
    if (m == 4 and d >= 20) or (m == 5 and d <= 20): return "Taurus"
    if (m == 5 and d >= 21) or (m == 6 and d <= 20): return "Gemini"
    if (m == 6 and d >= 21) or (m == 7 and d <= 22): return "Cancer"
    if (m == 7 and d >= 23) or (m == 8 and d <= 22): return "Leo"
    if (m == 8 and d >= 23) or (m == 9 and d <= 22): return "Virgo"
    if (m == 9 and d >= 23) or (m == 10 and d <= 22): return "Libra"
    if (m == 10 and d >= 23) or (m == 11 and d <= 21): return "Scorpio"
    if (m == 11 and d >= 22) or (m == 12 and d <= 21): return "Sagittarius"
    if (m == 12 and d >= 22) or (m == 1 and d <= 19): return "Capricorn"
    if (m == 1 and d >= 20) or (m == 2 and d <= 18): return "Aquarius"
    if (m == 2 and d >= 19) or (m == 3 and d <= 20): return "Pisces"
    return ""

def connect_to_sheet():
    try:
        gc = gspread.service_account(filename=str(SA_KEYFILE))
        ss = gc.open(SPREADSHEET_NAME)
        return ss.worksheet("RealiteaseInfo")
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None

def setup_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.timeout = 25
    return s

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_date_text_to_yyyy_mm_dd(text: str) -> Optional[str]:
    """
    Accepts typical Fandom infobox date strings (e.g., "July 15, 1973", "15 July 1973", "1973-07-15").
    Returns YYYY-MM-DD when day & month & year are available; otherwise None.
    """
    if not text:
        return None
    t = normalize_text(text).lower()

    # Clean up common extra text patterns (age, parentheses, etc.)
    t = re.sub(r'\s*\(age.*?\)', '', t)  # Remove "(age 39)" etc.
    t = re.sub(r'\s*\(.*?\)', '', t)     # Remove any other parentheses content
    t = normalize_text(t)

    # Try ISO first (YYYY-MM-DD)
    m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", t)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # "Month DD, YYYY" or "Month DDth, YYYY" (with ordinal suffixes)
    m = re.search(r"\b([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", t)
    if m:
        mon, day, year = m.groups()
        mm = DATE_MONTHS.get(mon.lower())
        if mm:
            return f"{year}-{mm}-{int(day):02d}"

    # "DD Month YYYY" or "DDth Month YYYY" (with ordinal suffixes)
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)\s+(\d{4})\b", t)
    if m:
        day, mon, year = m.groups()
        mm = DATE_MONTHS.get(mon.lower())
        if mm:
            return f"{year}-{mm}-{int(day):02d}"

    # "MM/DD/YYYY" or "MM-DD-YYYY"
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
    if m:
        mo, d, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # "DD/MM/YYYY" (European format) - try if first attempt fails
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", t)
    if m:
        first, second, year = m.groups()
        # Assume European format if first number > 12 (can't be month)
        if int(first) > 12:
            return f"{year}-{int(second):02d}-{int(first):02d}"
        # Otherwise assume American format
        return f"{year}-{int(first):02d}-{int(second):02d}"

    # "YYYY/MM/DD" format
    m = re.search(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b", t)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # Handle formats like "Born: July 15, 1973" (strip extra text)
    m = re.search(r"(?:born:?\s*)?([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", t)
    if m:
        mon, day, year = m.groups()
        mm = DATE_MONTHS.get(mon.lower())
        if mm:
            return f"{year}-{mm}-{int(day):02d}"

    return None

def wiki_candidates_for_shownames(shownames_field: str) -> List[str]:
    """
    From the comma-separated ShowNames cell, generate a prioritized list of candidate wiki domains.
    """
    cands: List[str] = []
    if not shownames_field:
        return cands
    shows = [normalize_text(x) for x in re.split(r"[;,]", shownames_field) if normalize_text(x)]
    for show in shows:
        if show in SHOW_TO_WIKIS:
            for d in SHOW_TO_WIKIS[show]:
                if d not in cands:
                    cands.append(d)
    return cands

def fandom_search_on_wiki(session: requests.Session, wiki_domain: str, person_name: str) -> Optional[str]:
    """
    Use MediaWiki API on the given wiki to find the best page title for the person.
    Returns the page title string if found.
    """
    url = f"https://{wiki_domain}/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": person_name,
        "utf8": 1,
        "srlimit": 5,
        "format": "json",
    }
    try:
        r = session.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get("query", {}).get("search", [])
        if not results:
            return None
        # Pick the top result
        return results[0].get("title")
    except Exception:
        return None

def fandom_fetch_page_html(session: requests.Session, wiki_domain: str, title: str) -> Optional[str]:
    """
    Fetch the actual article HTML for a wiki page title.
    """
    # Direct page URL
    page_url = f"https://{wiki_domain}/wiki/{title.replace(' ', '_')}"
    try:
        r = session.get(page_url, timeout=25)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def save_debug_html(html: str, person_name: str, wiki_domain: str, title: str):
    """Save HTML for manual inspection when debug mode is on"""
    import os
    debug_dir = "debug_html"
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)
    
    safe_name = re.sub(r'[^\w\-_\.]', '_', person_name)
    safe_domain = re.sub(r'[^\w\-_\.]', '_', wiki_domain)
    safe_title = re.sub(r'[^\w\-_\.]', '_', title)
    filename = f"{debug_dir}/{safe_name}_{safe_domain}_{safe_title}.html"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"    💾 Debug: Saved HTML to {filename}")
    except Exception as e:
        print(f"    ❌ Debug: Failed to save HTML: {e}")

def extract_birthday_from_infobox(html: str, debug: bool = False, person_name: str = "", wiki_domain: str = "", title: str = "") -> Optional[str]:
    """
    Parse the Fandom page HTML and try to extract a YYYY-MM-DD birthday from portable-infobox or traditional table.
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")

    if debug:
        print("    🔍 Debug: Looking for birthday in HTML...")
        if person_name and wiki_domain and title:
            save_debug_html(html, person_name, wiki_domain, title)

    # 1) Check for hidden structured data first (most reliable)
    bday_span = soup.find("span", class_="bday")
    if bday_span:
        dt = bday_span.get_text(strip=True)
        if debug:
            print(f"    🔍 Debug: Found .bday span: '{dt}'")
        iso = parse_date_text_to_yyyy_mm_dd(dt)
        if iso:
            if debug:
                print(f"    ✅ Debug: Parsed bday span to: {iso}")
            return iso

    # 2) <time itemprop="birthDate" datetime="YYYY-MM-DD">
    time_tag = soup.find("time", attrs={"itemprop": "birthDate"})
    if time_tag and time_tag.has_attr("datetime"):
        dt = time_tag["datetime"].strip()
        if debug:
            print(f"    🔍 Debug: Found time[itemprop=birthDate]: '{dt}'")
        iso = parse_date_text_to_yyyy_mm_dd(dt)
        if iso:
            if debug:
                print(f"    ✅ Debug: Parsed time tag to: {iso}")
            return iso

    # 3) Portable infobox structure - check multiple selectors
    infobox_selectors = [
        ".portable-infobox .pi-item.pi-data",
        ".portable-infobox .pi-data",
        ".infobox .pi-item.pi-data",
        ".infobox .pi-data"
    ]
    
    for selector in infobox_selectors:
        nodes = soup.select(selector)
        if debug and nodes:
            print(f"    🔍 Debug: Found {len(nodes)} nodes with selector '{selector}'")
        for node in nodes:
            # data-source (like data-source="Born" or data-source="Birthdate")
            ds = (node.get("data-source") or "").strip().lower()
            if ds in INFOBOX_LABEL_KEYS or ds == "born" or any(key in ds for key in ["birth", "born"]):
                val = node.select_one(".pi-data-value")
                if val:
                    val_text = val.get_text(" ", strip=True)
                    if debug:
                        print(f"    🔍 Debug: Found data-source='{ds}' with value: '{val_text}'")
                    bd = parse_date_text_to_yyyy_mm_dd(val_text)
                    if bd:
                        if debug:
                            print(f"    ✅ Debug: Parsed infobox data-source to: {bd}")
                        return bd
            # label text
            lab = node.select_one(".pi-data-label")
            val = node.select_one(".pi-data-value")
            if lab and val:
                lab_txt = (lab.get_text(" ", strip=True) or "").lower()
                if any(k in lab_txt for k in INFOBOX_LABEL_KEYS):
                    val_text = val.get_text(" ", strip=True)
                    if debug:
                        print(f"    🔍 Debug: Found label '{lab_txt}' with value: '{val_text}'")
                    bd = parse_date_text_to_yyyy_mm_dd(val_text)
                    if bd:
                        if debug:
                            print(f"    ✅ Debug: Parsed infobox label to: {bd}")
                        return bd

    # 4) Traditional table structure (like MTV Real World wiki)
    # Look for <td><b>Born</b></td> followed by <td>date</td>
    for td in soup.find_all("td"):
        if td.find("b") or td.find("strong"):
            label_text = td.get_text(strip=True).lower()
            if any(k in label_text for k in INFOBOX_LABEL_KEYS):
                # Find the next td sibling
                next_td = td.find_next_sibling("td")
                if next_td:
                    val_text = next_td.get_text(" ", strip=True)
                    if debug:
                        print(f"    🔍 Debug: Found table label '{label_text}' with next td: '{val_text}'")
                    bd = parse_date_text_to_yyyy_mm_dd(val_text)
                    if bd:
                        if debug:
                            print(f"    ✅ Debug: Parsed table td to: {bd}")
                        return bd

    # 5) Look for table rows with "Born" or similar labels
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            label_text = tds[0].get_text(strip=True).lower()
            if any(k in label_text for k in INFOBOX_LABEL_KEYS):
                val_text = tds[1].get_text(" ", strip=True)
                if debug:
                    print(f"    🔍 Debug: Found table row label '{label_text}' with value: '{val_text}'")
                bd = parse_date_text_to_yyyy_mm_dd(val_text)
                if bd:
                    if debug:
                        print(f"    ✅ Debug: Parsed table row to: {bd}")
                    return bd

    # 6) Look for th/td combinations (header/data pattern)
    for th in soup.find_all("th"):
        label_text = th.get_text(strip=True).lower()
        if any(k in label_text for k in INFOBOX_LABEL_KEYS):
            # Look for adjacent td
            next_td = th.find_next_sibling("td")
            if next_td:
                val_text = next_td.get_text(" ", strip=True)
                if debug:
                    print(f"    🔍 Debug: Found th label '{label_text}' with next td: '{val_text}'")
                bd = parse_date_text_to_yyyy_mm_dd(val_text)
                if bd:
                    if debug:
                        print(f"    ✅ Debug: Parsed th/td to: {bd}")
                    return bd
            # Or in same row
            tr = th.find_parent("tr")
            if tr:
                tds = tr.find_all("td")
                for td in tds:
                    val_text = td.get_text(" ", strip=True)
                    if val_text:
                        if debug:
                            print(f"    🔍 Debug: Found th '{label_text}' with row td: '{val_text}'")
                        bd = parse_date_text_to_yyyy_mm_dd(val_text)
                        if bd:
                            if debug:
                                print(f"    ✅ Debug: Parsed th row td to: {bd}")
                            return bd

    # 7) Other microdata (less common but try)
    birth_spans = soup.select('[itemprop="birthDate"], [data-source="birth_date"], [class*="birth"], [class*="born"]')
    if debug and birth_spans:
        print(f"    🔍 Debug: Found {len(birth_spans)} elements with birth-related attributes")
    for sp in birth_spans:
        txt = sp.get_text(" ", strip=True)
        if txt:
            if debug:
                print(f"    🔍 Debug: Found birth element: '{txt}'")
            bd = parse_date_text_to_yyyy_mm_dd(txt)
            if bd:
                if debug:
                    print(f"    ✅ Debug: Parsed birth element to: {bd}")
                return bd

    # 8) Look for divs or spans with birth-related classes
    birth_elements = soup.select('div[class*="birth"], span[class*="birth"], div[class*="born"], span[class*="born"]')
    if debug and birth_elements:
        print(f"    🔍 Debug: Found {len(birth_elements)} elements with birth-related classes")
    for elem in birth_elements:
        txt = elem.get_text(" ", strip=True)
        if txt:
            if debug:
                print(f"    🔍 Debug: Found birth class element: '{txt}'")
            bd = parse_date_text_to_yyyy_mm_dd(txt)
            if bd:
                if debug:
                    print(f"    ✅ Debug: Parsed birth class element to: {bd}")
                return bd

    # 9) Last resort: any text chunk that looks like a date in the infobox
    for infobox_sel in [".portable-infobox", ".infobox"]:
        infobox = soup.select_one(infobox_sel)
        if infobox:
            if debug:
                print(f"    🔍 Debug: Checking {infobox_sel} for date patterns...")
            # Split into smaller chunks to avoid false positives
            for elem in infobox.find_all(["div", "span", "td", "th"]):
                elem_text = elem.get_text(" ", strip=True)
                if elem_text and len(elem_text) < 100:  # Avoid long text blocks
                    guess = parse_date_text_to_yyyy_mm_dd(elem_text)
                    if guess:
                        if debug:
                            print(f"    ✅ Debug: Found date pattern in {infobox_sel}: '{elem_text}' -> {guess}")
                        return guess

    if debug:
        print("    ❌ Debug: No birthday found in HTML")

    return None

def extract_gender_from_infobox(html: str) -> Optional[str]:
    """
    Extract gender from Fandom portable infobox or traditional table, with fallback to pronoun analysis.
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")

    # 1) Direct gender field in portable infobox (most reliable)
    for node in soup.select(".portable-infobox .pi-item.pi-data"):
        # Check data-source attribute
        ds = (node.get("data-source") or "").strip().lower()
        if ds == "gender":
            val = node.select_one(".pi-data-value")
            if val:
                gender_text = val.get_text(" ", strip=True).lower()
                if "male" in gender_text and "female" not in gender_text:
                    return "Male"
                elif "female" in gender_text:
                    return "Female"
        
        # Check label text
        lab = node.select_one(".pi-data-label")
        val = node.select_one(".pi-data-value")
        if lab and val:
            lab_txt = (lab.get_text(" ", strip=True) or "").lower()
            if "gender" in lab_txt:
                gender_text = val.get_text(" ", strip=True).lower()
                if "male" in gender_text and "female" not in gender_text:
                    return "Male"
                elif "female" in gender_text:
                    return "Female"

    # 2) Traditional table structure (like MTV Real World wiki)
    # Look for <td><b>Gender</b></td> followed by <td>gender</td>
    for td in soup.find_all("td"):
        if td.find("b"):
            label_text = td.get_text(strip=True).lower()
            if "gender" in label_text:
                # Find the next td sibling
                next_td = td.find_next_sibling("td")
                if next_td:
                    gender_text = next_td.get_text(" ", strip=True).lower()
                    if "male" in gender_text and "female" not in gender_text:
                        return "Male"
                    elif "female" in gender_text:
                        return "Female"

    # 3) Look for table rows with "Gender" labels
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            label_text = tds[0].get_text(strip=True).lower()
            if "gender" in label_text:
                gender_text = tds[1].get_text(" ", strip=True).lower()
                if "male" in gender_text and "female" not in gender_text:
                    return "Male"
                elif "female" in gender_text:
                    return "Female"

    # 4) Pronoun analysis fallback
    return detect_gender_from_pronouns(html)

def detect_gender_from_pronouns(html: str) -> Optional[str]:
    """
    Detect gender by counting pronouns in the article content.
    """
    if not html:
        return None
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Get main content text (excluding infobox and navigation)
    content_areas = []
    
    # Main article content
    main_content = soup.select_one(".mw-parser-output")
    if main_content:
        # Remove infobox and navigation elements
        for elem in main_content.select(".portable-infobox, .navbox, .mw-collapsible"):
            elem.decompose()
        content_areas.append(main_content.get_text(" ", strip=True))
    
    # Combine all content
    full_text = " ".join(content_areas).lower()
    
    # Count pronouns
    he_count = len(re.findall(r'\bhe\b|\bhim\b|\bhis\b|\bhimself\b', full_text))
    she_count = len(re.findall(r'\bshe\b|\bher\b|\bhers\b|\bherself\b', full_text))
    
    # Decision logic
    if he_count > she_count and he_count >= 2:
        return "Male"
    elif she_count > he_count and she_count >= 2:
        return "Female"
    
    return None

def global_fandom_search(session: requests.Session, name: str, limit: int = 6) -> List[Tuple[str, str]]:
    """
    Search across Fandom for a person's page (returns list of (wiki_domain, title)).
    """
    try:
        params = {
            "query": name,
            "limit": limit,
            "minArticleQuality": 10,
            "namespaces": 0,
        }
        r = session.get(GLOBAL_FANDOM_SEARCH_API, params=params, timeout=20)
        if r.status_code != 200:
            return []
        data = r.json()
        items = data.get("items", []) or []
        hits: List[Tuple[str, str]] = []
        for it in items:
            # item has 'url' like https://<wiki>/wiki/<Title>
            url = it.get("url") or ""
            m = re.match(r"^https?://([^/]+)/wiki/(.+)$", url)
            if not m:
                continue
            domain, title = m.groups()
            hits.append((domain, title.replace("_", " ")))
        return hits
    except Exception:
        return []

def process_realitease_info_fandom(
    sheet,
    session,
    start_row: int,
    limit: int,
    rate_delay: float,
    reverse: bool = False,
    debug: bool = False,
    dry_run: bool = False,
):
    print("📊 Loading RealiteaseInfo sheet data...")
    all_rows = sheet.get_all_values()
    if len(all_rows) < 2:
        print("❌ RealiteaseInfo sheet has no data rows")
        return

    headers = all_rows[0]
    print(f"📋 Headers: {headers}")

    # A..J (0..9) — expect at least 10 columns
    if reverse:
        # For reverse processing, start from the end and work backward
        if start_row > len(all_rows):
            start_row = len(all_rows)
        rows = all_rows[start_row - 1::-1]  # Reverse slice from start_row to beginning
    else:
        rows = all_rows[start_row - 1:]
        
    if limit > 0:
        rows = rows[:limit]

    updates = 0
    processed = 0
    batch_updates = []  # Store pending updates for batch processing

    for i, row in enumerate(rows):
        processed += 1
        if reverse:
            row_num = start_row - i  # Count down when in reverse
        else:
            row_num = start_row + i  # Count up when forward

        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        cast_name = (row[0] or "").strip()
        shownames = (row[3] or "").strip()
        gender = (row[9] or "").strip()
        birthday = (row[10] or "").strip()
        zodiac = (row[11] or "").strip()

        if not cast_name:
            continue
        
        # Skip if ALL THREE fields are already filled (H, I, AND J)
        if birthday and gender and zodiac:
            if processed % 30 == 0:
                print(f"   ✅ Row {row_num}: {cast_name} — already has complete data (gender, birthday, zodiac)")
            continue

        print(f"🔍 Row {row_num}: {cast_name}")
        wiki_domains = wiki_candidates_for_shownames(shownames)

        found_bd: Optional[str] = None
        found_gender: Optional[str] = None

        # 1) Try per-show wikis
        for domain in wiki_domains:
            title = fandom_search_on_wiki(session, domain, cast_name)
            if not title:
                continue
            html = fandom_fetch_page_html(session, domain, title)
            if not html:
                continue
            
            # Extract birthday if missing
            if not birthday and not found_bd:
                bd = extract_birthday_from_infobox(html, debug, cast_name, domain, title)
                if bd:
                    found_bd = bd
                    print(f"   🎂 Found birthday on {domain} → {title}: {bd}")
            
            # Extract gender if missing
            if not gender and not found_gender:
                gd = extract_gender_from_infobox(html)
                if gd:
                    found_gender = gd
                    print(f"   👤 Found gender on {domain} → {title}: {gd}")
            
            # If we found everything we need, break
            if (birthday or found_bd) and (gender or found_gender):
                break
                
            # Small delay between tries on the same person
            time.sleep(0.4)

        # 2) Fallback: global Fandom search (best N) if still missing data
        if (not birthday and not found_bd) or (not gender and not found_gender):
            hits = global_fandom_search(session, cast_name, limit=6)
            for domain, title in hits:
                html = fandom_fetch_page_html(session, domain, title)
                if not html:
                    continue
                
                # Extract birthday if still missing
                if not birthday and not found_bd:
                    bd = extract_birthday_from_infobox(html, debug, cast_name, domain, title)
                    if bd:
                        found_bd = bd
                        print(f"   🎂 Found birthday via global search on {domain} → {title}: {bd}")
                
                # Extract gender if still missing
                if not gender and not found_gender:
                    gd = extract_gender_from_infobox(html)
                    if gd:
                        found_gender = gd
                        print(f"   👤 Found gender via global search on {domain} → {title}: {gd}")
                
                # If we found everything we need, break
                if (birthday or found_bd) and (gender or found_gender):
                    break
                    
                time.sleep(0.4)

        # 3) Prepare updates for batch processing
        row_updates = []
        
        if found_gender and not gender:
            row_updates.append((row_num, 10, found_gender))  # Column J (10th column, 1-indexed)

        if found_bd and not birthday:
            row_updates.append((row_num, 11, found_bd))  # Column K (11th column, 1-indexed)
            # Calculate zodiac if we found a birthday and zodiac is empty
            if not zodiac:
                z = calculate_zodiac(found_bd)
                if z:
                    row_updates.append((row_num, 12, z))  # Column L (12th column, 1-indexed)
        elif birthday and not zodiac:
            # Calculate zodiac for existing birthday if zodiac is missing
            z = calculate_zodiac(birthday)
            if z:
                row_updates.append((row_num, 12, z))  # Column L (12th column, 1-indexed)

        # Add to batch updates
        if row_updates:
            batch_updates.extend(row_updates)
            updates += len(row_updates)
            print(f"   ✅ Queued {len(row_updates)} updates for batch processing")
        else:
            missing = []
            if not birthday and not found_bd:
                missing.append("birthday")
            if not gender and not found_gender:
                missing.append("gender")
            if missing:
                print(f"   ⚠️ No usable {' or '.join(missing)} found on Fandom")

        # Batch update every 20 rows or at the end
        if len(batch_updates) >= 20 or processed == len(rows):
            if batch_updates:
                if dry_run:
                    print(f"\n🔄 DRY RUN: Would batch update {len(batch_updates)} cells...")
                else:
                    print(f"\n🔄 Performing batch update of {len(batch_updates)} cells...")
                    try:
                        # Convert to Google Sheets batch update format
                        update_cells = []
                        for row_num, col_num, value in batch_updates:
                            update_cells.append({
                                'range': f'{chr(64 + col_num)}{row_num}',
                                'values': [[value]]
                            })
                        
                        # Use batch_update for efficiency
                        sheet.batch_update(update_cells)
                        print(f"   ✅ Successfully batch updated {len(batch_updates)} cells")
                        time.sleep(1)  # Brief pause after batch update
                    except Exception as e:
                        print(f"   ❌ Batch update failed, falling back to individual updates: {e}")
                        # Fallback to individual updates
                        for row_num, col_num, value in batch_updates:
                            try:
                                sheet.update_cell(row_num, col_num, value)
                                time.sleep(0.2)
                            except Exception as e2:
                                print(f"   ❌ Error updating cell {chr(64 + col_num)}{row_num}: {e2}")

                batch_updates = []  # Reset batch

        # Rate limiting between people
        time.sleep(rate_delay)

        if processed % 10 == 0:
            print(f"📈 Progress: {processed}/{len(rows)} processed, {updates} updates queued")

    print("\n🎉 Done!")
    print(f"   Rows processed: {processed}")
    print(f"   Total updates queued: {updates}")

def main():
    ap = argparse.ArgumentParser(description="Fill Birthday, Gender, and Zodiac from Fandom/Wikia into RealiteaseInfo (with batch updates every 20 rows)")
    ap.add_argument("--start-row", type=int, default=2, help="1-based start row (default 2)")
    ap.add_argument("--limit", type=int, default=0, help="Limit rows to process (0 = all)")
    ap.add_argument("--rate-delay", type=float, default=0.2, help="Delay between people (seconds)")
    ap.add_argument("--reverse", action="store_true", help="Process rows in reverse order (from end to start)")
    ap.add_argument("--debug", action="store_true", help="Enable debug mode to see detailed extraction attempts")
    ap.add_argument("--dry-run", action="store_true", help="Collect data without writing updates to Google Sheets")
    args = ap.parse_args()

    print("🎯 RealiteaseInfo Fandom Birthday & Gender Enhancer")
    print("=" * 60)
    print("Target columns: J (Gender), K (Birthday), L (Zodiac)")
    print("Batch updates every 20 rows for efficiency")
    print("Skips rows with complete data in ALL three columns")
    if args.reverse:
        print("Processing in REVERSE order (bottom to top)")
    print()

    sheet = connect_to_sheet()
    if not sheet:
        return

    session = setup_session()

    # Confirmation
    direction = "reverse" if args.reverse else "forward"
    lim = f" (limit: {args.limit})" if args.limit > 0 else " (all rows)"
    print(f"❓ Process rows starting from {args.start_row}{lim} in {direction} order with {args.rate_delay}s delay? (y/N): ", end="")
    if (input().strip().lower() != "y"):
        print("❌ Cancelled.")
        return

    process_realitease_info_fandom(
        sheet,
        session,
        args.start_row,
        args.limit,
        args.rate_delay,
        reverse=args.reverse,
        debug=args.debug,
        dry_run=args.dry_run,
    )
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()
# Gemini setup
try:
    import google.generativeai as genai
except ImportError:
    genai = None

GOOGLE_GEMINI_API_KEY = os.getenv('GOOGLE_GEMINI_API_KEY') or os.getenv('GEMINI_API_KEY')
DEFAULT_GEMINI_MODEL = os.getenv('GOOGLE_GEMINI_MODEL') or os.getenv('GEMINI_MODEL') or 'gemini-2.5-flash'

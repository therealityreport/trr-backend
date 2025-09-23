# scripts/fetch_person_details.py
#
# Fill Gender (col J) and Birthday (col L) in UpdateInfo from TMDb.
# - Never overwrites user-entered update columns (Gender-Update, Birthday-Update).
# - Will overwrite placeholders in base columns ("unknown **" for Gender, " **" for Birthday) when TMDb has a real value.
# - Will NOT overwrite non-placeholder values in base columns.
#
# Uses .env at repo root for credentials:
#     GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
#     SPREADSHEET_ID=...
#     TMDB_API_KEY=...      (or TMDB_BEARER=...; either works)
#
# Run:
#   source .venv/bin/activate
#   python -u scripts/fetch_person_details.py
# Optional:
#   python -u scripts/fetch_person_details.py --start-row 2 --limit 500 --batch-size 200

import os
import time
import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import sys

import requests
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError as GspreadAPIError

# from scripts.build_update_info import get_existing

# ---------------- CLI ----------------
parser = argparse.ArgumentParser(
    description="Fill missing Gender/Birthday in UpdateInfo from TMDb.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--start-row", type=int, default=2,
                    help="1-based row to start from in UpdateInfo (2 = first data row).")
parser.add_argument("--limit", type=int, default=0,
                    help="Max number of data rows to process (0 = no limit).")
parser.add_argument("--batch-size", type=int, default=200,
                    help="How many single-cell updates to send per batch when not streaming.")
parser.add_argument("--rate-delay", type=float, default=0.35,
                    help="Delay between TMDb calls (seconds).")
parser.add_argument("--stream-writes", action="store_true",
                    help="If set, send Sheets updates incrementally while processing.")
parser.add_argument("--flush-every", type=int, default=0,
                    help="When streaming, flush every N queued single-cell updates (overrides --batch-size).")
parser.add_argument("--verbose", action="store_true",
                    help="Print effective arguments at startup for debugging.")
parser.add_argument("--sheet", dest="sheet_title", default="UpdateInfo",
                    help="Worksheet title to update (default: UpdateInfo).")
parser.add_argument("--allow-other-sheet", action="store_true",
                    help="Allow writing to a worksheet other than 'UpdateInfo'. Use with caution.")
args = parser.parse_args()

# ---------------- Env / Clients ----------------
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=str(ROOT / ".env"), override=True)

GOOGLE_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BEARER = os.getenv("TMDB_BEARER")

if not GOOGLE_CREDS or not Path(GOOGLE_CREDS).exists():
    # soft fallback to first JSON under ./keys
    keys_dir = ROOT / "keys"
    if keys_dir.exists():
        for p in keys_dir.iterdir():
            if p.suffix == ".json":
                GOOGLE_CREDS = str(p)
                break
if not GOOGLE_CREDS:
    raise SystemExit("Missing GOOGLE_APPLICATION_CREDENTIALS (.env)")

if not SPREADSHEET_ID:
    raise SystemExit("Missing SPREADSHEET_ID (.env)")

# Sheets client
creds = Credentials.from_service_account_file(
    GOOGLE_CREDS,
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(creds)
sh = client.open_by_key(SPREADSHEET_ID)
print(f"Connected to spreadsheet: {sh.title}", flush=True)

if args.verbose or args.stream_writes or args.flush_every:
    print(
        "Args => sheet=%s start_row=%s limit=%s batch_size=%s rate_delay=%s stream_writes=%s flush_every=%s" %
        (getattr(args, "sheet_title", "UpdateInfo"), args.start_row, args.limit, args.batch_size, args.rate_delay, args.stream_writes, args.flush_every),
        flush=True,
    )

def ws(title: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        raise SystemExit(f"Worksheet '{title}' not found.")

SHEET_TITLE = args.sheet_title
# Fail-fast if targeting a suspicious sheet unless explicitly allowed
if SHEET_TITLE.lower() != "updateinfo" and not getattr(args, "allow_other_sheet", False):
    raise SystemExit(
        f"Refusing to write to worksheet '{SHEET_TITLE}'. "
        "This script is intended for 'UpdateInfo'. Pass --allow-other-sheet to override."
    )
update_ws = ws(SHEET_TITLE)
print(f"Target worksheet: {SHEET_TITLE}", flush=True)

def send_updates(chunk):
    """Batch write to the *selected* worksheet only, qualifying every range with 'SHEET_TITLE!'."""
    fixed = []
    for item in chunk:
        rng = item.get("range", "")
        if "!" not in rng:
            rng = f"{SHEET_TITLE}!{rng}"
        fixed.append({"range": rng, "values": item.get("values", [])})
    body_req = {"valueInputOption": "RAW", "data": fixed}
    attempts = 0
    while True:
        try:
            update_ws.spreadsheet.values_batch_update(body=body_req)
            return
        except GspreadAPIError as e:
            attempts += 1
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                wait_s = min(60 * attempts, 180)
                print(f"  Sheets 429 rate limit. Sleeping {wait_s}s …")
                time.sleep(wait_s)
                continue
            raise

# ---------------- TMDb ----------------
session = requests.Session()
session.headers.update({"accept": "application/json"})
if TMDB_BEARER:
    session.headers.update({"Authorization": f"Bearer {TMDB_BEARER}"})

def tmdb_person(person_id: str, retries: int = 3, backoff: float = 0.8) -> Dict[str, Any]:
    """Fetch TMDb person details using Bearer if provided; otherwise API key."""
    params = {}
    if not TMDB_BEARER:
        if not TMDB_API_KEY:
            return {}
        params["api_key"] = TMDB_API_KEY

    url = f"https://api.themoviedb.org/3/person/{person_id}"
    delay = args.rate_delay
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code == 200:
                time.sleep(delay)
                return r.json()
            # Retry on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                wait_s = min(60, backoff * attempt * 2)
                print(f"  TMDb {person_id}: {r.status_code}; retrying in {wait_s:.1f}s …")
                time.sleep(wait_s)
                continue
            # Otherwise give up for this person
            print(f"  TMDb {person_id}: {r.status_code} {r.text[:120]}")
            return {}
        except requests.RequestException as e:
            wait_s = min(60, backoff * attempt * 2)
            print(f"  TMDb {person_id}: network error {e}; retry in {wait_s:.1f}s …")
            time.sleep(wait_s)
    return {}

def map_gender(code: Any) -> str:
    # TMDb: 0/None unknown; 1=Female; 2=Male; (3 sometimes used as 'Non-binary' but rare)
    try:
        code = int(code)
    except Exception:
        return ""
    if code == 1:
        return "Female"
    if code == 2:
        return "Male"
    return ""  # keep empty so we don't write junk

def safe_bday(s: Any) -> str:
    s = (s or "").strip()
    # TMDb returns YYYY-MM-DD or empty; pass through
    return s

def is_placeholder_gender(s: str) -> bool:
    s = (s or "").strip().lower()
    return s == "unknown **"

def is_placeholder_bday(s: str) -> bool:
    s = (s or "").strip()
    return s == " **"

# ---------------- Read UpdateInfo ----------------
# Expected columns (A..N):
#   A CastID, B CastName, C Name-Update, D TotalShows, E TotalEpisodes,
#   F Shows, G Shows-Update, H Seasons, I Seasons-Update,
#   J Gender, K Gender-Update, L Birthday, M Birthday-Update, N Birthday-Grab
header = update_ws.row_values(1)
if not header:
    raise SystemExit("UpdateInfo is empty (missing header).")

def idx(name: str) -> int:
    try:
        return header.index(name)
    except ValueError:
        return -1

i_cast = idx("CastID")
i_gender = idx("Gender")
i_gender_u = idx("Gender-Update")
i_bday = idx("Birthday")
i_bday_u = idx("Birthday-Update")

# Extra sanity: bail if the sheet doesn't look like UpdateInfo (prevents writing into 'SHOWS' or 'ShowInfo')
required_cols = ("CastID", "Gender", "Gender-Update", "Birthday", "Birthday-Update")
missing = [c for c in required_cols if c not in header]
if missing:
    raise SystemExit(
        f"Worksheet '{SHEET_TITLE}' is missing required columns {missing}. "
        "Are you pointing at the right tab?"
    )

if min(i_cast, i_gender, i_gender_u, i_bday, i_bday_u) == -1:
    raise SystemExit("UpdateInfo is missing one of required columns: CastID, Gender, Gender-Update, Birthday, Birthday-Update")

# Fetch in chunks so big sheets don't stall
def read_body(batch_rows: int = 1000) -> List[List[str]]:
    out: List[List[str]] = []
    start = args.start_row
    last_col_letter = "N"  # up to Birthday-Grab
    while True:
        end = start + batch_rows - 1
        rng = f"A{start}:{last_col_letter}{end}"
        rows = update_ws.get(rng)
        if not rows:
            break
        out.extend(rows)
        if len(rows) < batch_rows:
            break
        start = end + 1
    return out

print("Reading UpdateInfo data…", flush=True)
body = read_body(batch_rows=1000)
total_rows = len(body)
print(f"Found {total_rows} data rows starting at row {args.start_row}.", flush=True)

# ---------------- Build updates ----------------
updates: List[Dict[str, Any]] = []
processed = 0
written = 0

# Counters for visibility
want_gender_cnt = 0
want_bday_cnt = 0
wrote_gender_cnt = 0
wrote_bday_cnt = 0

BATCH = max(1, int(args.batch_size))
STREAM_BATCH = max(1, int(args.flush_every)) if args.flush_every else BATCH

for offset, row in enumerate(body):
    sheet_row = args.start_row + offset  # absolute 1-based row in sheet
    if args.limit and processed >= args.limit:
        break

    cast_id = (row[i_cast] if len(row) > i_cast else "").strip()
    if not cast_id:
        processed += 1
        continue

    gender_cur = (row[i_gender] if len(row) > i_gender else "").strip()
    gender_upd = (row[i_gender_u] if len(row) > i_gender_u else "").strip()
    bday_cur   = (row[i_bday] if len(row) > i_bday else "").strip()
    bday_upd   = (row[i_bday_u] if len(row) > i_bday_u else "").strip()

    # Respect user-entered *-Update columns; if those have values, skip this row for that field.
    can_write_gender = not gender_upd
    can_write_bday   = not bday_upd

    # Determine if we should update the base cells:
    # - update if empty OR placeholder; but never overwrite a non-placeholder.
    want_gender = can_write_gender and ((not gender_cur) or is_placeholder_gender(gender_cur))
    want_bday   = can_write_bday and ((not bday_cur) or is_placeholder_bday(bday_cur))

    if want_gender:
        want_gender_cnt += 1
    if want_bday:
        want_bday_cnt += 1

    if not (want_gender or want_bday):
        processed += 1
        continue

    person = tmdb_person(cast_id)
    if not person:
        processed += 1
        continue

    g = map_gender(person.get("gender"))  # "Female"/"Male" or ""
    bd = safe_bday(person.get("birthday"))  # "YYYY-MM-DD" or ""

    # Only write when TMDb provided a real value
    if want_gender and g:
        updates.append({"range": f"J{sheet_row}", "values": [[g]]})
        wrote_gender_cnt += 1
    if want_bday and bd:
        updates.append({"range": f"L{sheet_row}", "values": [[bd]]})
        wrote_bday_cnt += 1

    processed += 1
    if processed % 50 == 0:
        print(f"Processed {processed}/{total_rows} rows…", flush=True)

    # If streaming is enabled, flush in chunks as we go
    if args.stream_writes and len(updates) >= STREAM_BATCH:
        to_send = updates[:STREAM_BATCH]
        send_updates(to_send)
        del updates[:STREAM_BATCH]
        written += len(to_send)
        print(f"  (stream) wrote {written} updates so far…", flush=True)
        print(f"    stats so far: want_gender={want_gender_cnt}, want_bday={want_bday_cnt}, wrote_gender={wrote_gender_cnt}, wrote_bday={wrote_bday_cnt}", flush=True)

print(f"Summary before final writes: want_gender={want_gender_cnt}, want_bday={want_bday_cnt}, wrote_gender={wrote_gender_cnt}, wrote_bday={wrote_bday_cnt}", flush=True)

# ---------------- Write updates in batches ----------------
if not updates:
    print("Nothing to update. All Gender/Birthday fields already filled or have *-Update values.")
else:
    print(f"Applying remaining {len(updates)} single-cell updates in batches of {BATCH}…", flush=True)
    for i in range(0, len(updates), BATCH):
        chunk = updates[i:i + BATCH]
        send_updates(chunk)
        written += len(chunk)
        print(f"  Wrote {written} updates total…", flush=True)
        time.sleep(0.25)
    pass
    print(f"Total single-cell writes: {written}", flush=True)
print(f"Final summary: want_gender={want_gender_cnt}, want_bday={want_bday_cnt}, wrote_gender={wrote_gender_cnt}, wrote_bday={wrote_bday_cnt}", flush=True)
if args.stream_writes:
    print("✓ Done (streamed writes).")
else:
    print("✓ Done (batch writes).")
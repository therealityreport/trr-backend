#!/usr/bin/env python3
"""
FinalInfo Step 3 - Sheet Normalization and Batch Update

This step performs light normalization on the FinalList sheet and writes
updates in batches of 200 rows:
- Normalize spacing and dedupe in AlternativeNames
- Ensure ImageURL is a single URL (trimmed) or blank
- No external services (Firebase is purposefully decoupled)
"""

import argparse
import gspread
from dotenv import load_dotenv
import os

BATCH_SIZE = 200


def parse_alt_names(value: str):
    if not value:
        return []
    parts = [p.strip() for p in value.split(',') if p.strip()]
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def normalize_image(value: str):
    return (value or '').strip()


def run(dry_run: bool, batch_size: int = BATCH_SIZE):
    load_dotenv()
    gc = gspread.service_account(filename=os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', 'keys/trr-backend-df2c438612e1.json'))
    sheet = gc.open('Realitease2025Data')
    ws = sheet.worksheet('FinalList')

    rows = ws.get_all_values()
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}

    # Ensure columns exist
    required = ['Name', 'IMDbCastID', 'CorrectDate', 'AlternativeNames', 'IMDbSeriesIDs']
    for r in required:
        if r not in idx:
            raise RuntimeError(f"Missing required column: {r}")

    # Optional ImageURL column (may be added by Step 2)
    has_image = 'ImageURL' in idx

    data_rows = rows[1:]
    updates = []

    for r in data_rows:
        while len(r) < len(headers):
            r.append('')
        # Normalize AlternativeNames
        alt_norm = ', '.join(parse_alt_names(r[idx['AlternativeNames']] if 'AlternativeNames' in idx else ''))
        if has_image:
            img_norm = normalize_image(r[idx['ImageURL']])
        else:
            img_norm = ''
        # Prepare row with potential changes
        new_row = r.copy()
        if 'AlternativeNames' in idx:
            new_row[idx['AlternativeNames']] = alt_norm
        if has_image:
            new_row[idx['ImageURL']] = img_norm
        updates.append(new_row)

    if dry_run:
        print(f"🧪 DRY RUN - Would update {len(updates)} rows in batches of {batch_size}")
        print("   Example diff (first row with changes if any):")
        for i, (old, new) in enumerate(zip(data_rows, updates), start=2):
            if old != new:
                print(f"   Row {i}:\n     OLD: {old}\n     NEW: {new}")
                break
        return

    # Write in batches
    total = len(updates)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        start_row = start + 2  # account for header
        end_row = end + 1
        print(f"📤 Writing rows {start_row}-{end_row} ({end-start})")
        ws.update(range_name=f'A{start_row}:{chr(64+len(headers))}{end_row}', values=updates[start:end])
    print(f"✅ Updated {total} rows in batches of {batch_size}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Normalize FinalList and batch update in N-row chunks')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Rows per update batch (default: 200)')
    args = ap.parse_args()
    run(dry_run=args.dry_run, batch_size=args.batch_size)
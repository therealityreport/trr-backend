#!/usr/bin/env python3
"""
Quick snapshot to verify FinalList contents after Step 3:
- Connects using the same service account
- Prints row counts, non-empty AlternativeNames/ImageURL counts
- Shows a few sample rows for sanity
"""

import os
import gspread
from dotenv import load_dotenv


def main():
    load_dotenv()
    keyfile = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', 'keys/trr-backend-df2c438612e1.json')
    gc = gspread.service_account(filename=keyfile)
    sh_name = os.getenv('REALITEASE_SHEET_NAME', 'Realitease2025Data')
    ws_name = 'FinalList'
    sh = gc.open(sh_name)
    ws = sh.worksheet(ws_name)

    rows = ws.get_all_values()
    if not rows:
        print('❌ No data returned from sheet')
        return

    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    data = rows[1:]

    alt_idx = idx.get('AlternativeNames', None)
    img_idx = idx.get('ImageURL', None)

    non_empty_alt = 0
    non_empty_img = 0
    for r in data:
        if alt_idx is not None and alt_idx < len(r) and r[alt_idx].strip():
            non_empty_alt += 1
        if img_idx is not None and img_idx < len(r) and r[img_idx].strip():
            non_empty_img += 1

    print('📊 FinalList Snapshot')
    print(f"   Sheet: {sh_name} / {ws_name}")
    print(f"   Total rows (excluding header): {len(data)}")
    print(f"   Non-empty AlternativeNames: {non_empty_alt}")
    print(f"   Non-empty ImageURL: {non_empty_img}")
    print(f"   Headers: {headers}")

    # Show first 3 rows that have AlternativeNames or ImageURL populated
    shown = 0
    print('\n🔎 Sample rows with data:')
    for i, r in enumerate(data, start=2):
        alt = r[alt_idx] if alt_idx is not None and alt_idx < len(r) else ''
        img = r[img_idx] if img_idx is not None and img_idx < len(r) else ''
        if alt or img:
            name = r[idx['Name']] if 'Name' in idx else ''
            imdb = r[idx['IMDbCastID']] if 'IMDbCastID' in idx else ''
            print(f"   Row {i}: Name={name} IMDbCastID={imdb} AlternativeNames='{alt}' ImageURL='{img[:80]}'")
            shown += 1
        if shown >= 3:
            break


if __name__ == '__main__':
    main()

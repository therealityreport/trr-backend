#!/usr/bin/env python3
"""WWHLInfo Step 4: Check cast IDs against RealiteaseInfo and populate REALITEASE column."""

import argparse
import os
import re
from typing import Dict, List, Set

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


class WWHLInfoChecker:
    """Check WWHLInfo cast IDs against RealiteaseInfo and populate REALITEASE column."""

    def __init__(self):
        print("🔍 Starting WWHLInfo Step 4: REALITEASE Column Checker")
        print("=" * 60)
        
        self.load_env()
        self.setup_sheets()
        self.realitease_ids = self.load_realitease_ids()

    def load_env(self):
        """Load environment variables."""
        env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        load_dotenv(env_path)

    def setup_sheets(self):
        """Setup Google Sheets connection."""
        key_path = os.path.join(os.path.dirname(__file__), "..", "..", "keys", "trr-backend-df2c438612e1.json")
        creds = Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        client = gspread.authorize(creds)
        
        spreadsheet_name = os.getenv('SPREADSHEET_NAME', 'Realitease2025Data')
        spreadsheet = client.open(spreadsheet_name)
        
        self.wwhl_ws = spreadsheet.worksheet("WWHLinfo")
        self.realitease_ws = spreadsheet.worksheet("RealiteaseInfo")
        
        print(f"✅ Connected to Google Sheets ({spreadsheet_name})")
        print("✅ Located WWHLinfo and RealiteaseInfo sheets")

    def load_realitease_ids(self) -> Set[str]:
        """Load all cast IDs from RealiteaseInfo sheet."""
        print("📋 Loading cast IDs from RealiteaseInfo sheet...")
        
        data = self.realitease_ws.get_all_values()
        if len(data) < 2:
            return set()

        headers = [h.strip().lower() for h in data[0]]
        imdb_idx = headers.index("castimdbid") if "castimdbid" in headers else None
        tmdb_idx = headers.index("casttmdbid") if "casttmdbid" in headers else None

        realitease_ids = set()

        for row in data[1:]:
            # Get IMDb ID
            if imdb_idx is not None and len(row) > imdb_idx:
                imdb_id = row[imdb_idx].strip()
                if imdb_id:
                    # Ensure proper IMDb format
                    if not imdb_id.startswith("nm"):
                        imdb_id = f"nm{imdb_id}"
                    realitease_ids.add(imdb_id)

            # Get TMDb ID
            if tmdb_idx is not None and len(row) > tmdb_idx:
                tmdb_id = row[tmdb_idx].strip()
                if tmdb_id and tmdb_id.lower() not in {'none', 'n/a'}:
                    realitease_ids.add(tmdb_id)

        print(f"📊 Loaded {len(realitease_ids)} unique cast IDs from RealiteaseInfo")
        return realitease_ids

    def parse_ids(self, ids_str: str) -> List[str]:
        """Parse comma-separated IDs and return list of clean IDs."""
        if not ids_str or ids_str.strip() == '':
            return []
        
        ids = []
        for id_part in ids_str.split(','):
            clean_id = id_part.strip()
            if clean_id and clean_id.lower() not in {'none', 'n/a', ''}:
                ids.append(clean_id)
        
        return ids

    def determine_realitease_status(self, imdb_ids_str: str, tmdb_ids_str: str) -> str:
        """Determine REALITEASE status based on cast IDs."""
        imdb_ids = self.parse_ids(imdb_ids_str)
        tmdb_ids = self.parse_ids(tmdb_ids_str)
        
        # Count how many people are found in RealiteaseInfo
        # Each person has one IMDb OR one TMDb (or both), so we count by position
        imdb_found_count = sum(1 for id in imdb_ids if id in self.realitease_ids)
        tmdb_found_count = sum(1 for id in tmdb_ids if id in self.realitease_ids)
        
        # For people counting: assume each position corresponds to one person
        # The number of people is the max of IMDb count or TMDb count
        total_people = max(len(imdb_ids), len(tmdb_ids))
        
        # Count unique people found: take the max since both lists should be same length
        # representing the same people in same order
        people_found_in_realitease = max(imdb_found_count, tmdb_found_count)
        
        # Apply the logic based on your criteria
        if people_found_in_realitease == 0:
            return "NONE"
        elif total_people == 1:
            # Only one person total on the episode
            return "SOLO"
        elif people_found_in_realitease >= 2:
            # Two or more different people found in RealiteaseInfo
            return "REALITEASE"  
        elif people_found_in_realitease == 1:
            # Only one person found in RealiteaseInfo but there are multiple guests
            return "ALMOST"
        else:
            return "NONE"

    def check_and_update(self, dry_run: bool = True, batch_size: int = 50) -> int:
        """Check all WWHLinfo rows and update REALITEASE column."""
        print("🔍 Analyzing WWHLinfo rows...")
        
        all_values = self.wwhl_ws.get_all_values()
        if len(all_values) < 2:
            print("❌ No data found in WWHLinfo sheet")
            return 0

        headers = all_values[0]
        updates = []
        rows_to_update = 0

        # Find column indices
        imdb_col_idx = None
        tmdb_col_idx = None
        realitease_col_idx = None
        
        for i, header in enumerate(headers):
            if header.strip().lower() == 'imdbcastids':
                imdb_col_idx = i
            elif header.strip().lower() == 'tmdbcastids':
                tmdb_col_idx = i
            elif header.strip().lower() == 'realitease':
                realitease_col_idx = i

        if imdb_col_idx is None or tmdb_col_idx is None or realitease_col_idx is None:
            print(f"❌ Could not find required columns. Found: IMDbCastIDs={imdb_col_idx}, TMDbCastIDs={tmdb_col_idx}, REALITEASE={realitease_col_idx}")
            return 0

        print(f"📍 Column positions: IMDbCastIDs={imdb_col_idx+1}, TMDbCastIDs={tmdb_col_idx+1}, REALITEASE={realitease_col_idx+1}")

        for row_idx, row in enumerate(all_values[1:], start=2):
            # Pad row if necessary
            while len(row) <= max(imdb_col_idx, tmdb_col_idx, realitease_col_idx):
                row.append('')

            imdb_ids = row[imdb_col_idx] if len(row) > imdb_col_idx else ''
            tmdb_ids = row[tmdb_col_idx] if len(row) > tmdb_col_idx else ''
            current_realitease = row[realitease_col_idx] if len(row) > realitease_col_idx else ''
            
            # Determine new REALITEASE status
            new_status = self.determine_realitease_status(imdb_ids, tmdb_ids)
            
            # Skip if no change needed
            if new_status == current_realitease.strip():
                continue
            
            # Log the analysis
            imdb_list = self.parse_ids(imdb_ids)
            tmdb_list = self.parse_ids(tmdb_ids)
            imdb_in_realitease = [id for id in imdb_list if id in self.realitease_ids]
            tmdb_in_realitease = [id for id in tmdb_list if id in self.realitease_ids]
            
            total_people = max(len(imdb_list), len(tmdb_list))
            people_found = max(len(imdb_in_realitease), len(tmdb_in_realitease))
            
            print(f"\n🔎 Row {row_idx}:")
            print(f"   👥 Total people on episode: {total_people}")
            print(f"   📋 People found in RealiteaseInfo: {people_found}")
            print(f"   📝 Status: '{current_realitease}' → '{new_status}'")

            # Prepare update
            cell_range = f"{chr(65 + realitease_col_idx)}{row_idx}"  # Convert to letter (A=65)
            updates.append({
                "range": cell_range,
                "values": [[new_status]]
            })
            rows_to_update += 1

            # Write updates in batches
            if not dry_run and len(updates) >= batch_size:
                print(f"\n📤 Writing batch of {len(updates)} updates to Google Sheets...")
                self.wwhl_ws.batch_update(updates)
                print(f"   ✅ Batch written successfully")
                updates = []

        # Write any remaining updates
        if not dry_run and updates:
            print(f"\n📤 Writing final batch of {len(updates)} updates to Google Sheets...")
            self.wwhl_ws.batch_update(updates)
            print(f"   ✅ Final batch written successfully")

        if dry_run:
            print(f"\n🔍 DRY RUN: Would update {rows_to_update} rows")
        else:
            print(f"\n✅ Successfully updated {rows_to_update} rows")

        return rows_to_update

    def print_summary(self):
        """Print summary of REALITEASE status distribution."""
        print("\n📊 REALITEASE Status Summary:")
        all_values = self.wwhl_ws.get_all_values()
        
        if len(all_values) < 2:
            print("   No data to analyze")
            return

        headers = all_values[0]
        realitease_col_idx = None
        
        for i, header in enumerate(headers):
            if header.strip().lower() == 'realitease':
                realitease_col_idx = i
                break

        if realitease_col_idx is None:
            print("   REALITEASE column not found")
            return

        status_counts = {}
        for row in all_values[1:]:
            if len(row) > realitease_col_idx:
                status = row[realitease_col_idx].strip()
                status_counts[status] = status_counts.get(status, 0) + 1

        for status, count in sorted(status_counts.items()):
            print(f"   {status or '(empty)'}: {count} episodes")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check WWHLinfo cast IDs and populate REALITEASE column")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    parser.add_argument("--summary", action="store_true", help="Show summary of current REALITEASE status distribution")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of updates to batch together (default: 50)")
    return parser.parse_args()


def main():
    args = parse_args()
    
    checker = WWHLInfoChecker()
    
    if args.summary:
        checker.print_summary()
        return
    
    updated_count = checker.check_and_update(dry_run=args.dry_run, batch_size=args.batch_size)
    
    print(f"\n🎉 STEP 4 SUMMARY:")
    if args.dry_run:
        print(f"   📊 Rows that would be updated: {updated_count}")
        print("   (Run without --dry-run to apply the updates)")
    else:
        print(f"   ✅ Rows updated: {updated_count}")
    
    checker.print_summary()


if __name__ == '__main__':
    main()
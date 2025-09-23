#!/usr/bin/env python3
"""RealiteaseInfo Step 3: Remove cast rows that no longer meet minimum criteria."""

import os
import re
import time
from collections import defaultdict

import gspread
from dotenv import load_dotenv


load_dotenv()


class RealiteaseInfoStep3:
    """Remove RealiteaseInfo rows whose CastIMDbID does not meet inclusion rules."""

    def __init__(self):
        print("🧹 Starting RealiteaseInfo Step 3: Cast Cleanup")
        print("=" * 60)

        self.gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
        spreadsheet_name = os.getenv('SPREADSHEET_NAME', 'Realitease2025Data')
        self.spreadsheet = self.gc.open(spreadsheet_name)
        print(f"✅ Connected to Google Sheets ({spreadsheet_name})")

        self.cast_info_sheet = self.spreadsheet.worksheet('CastInfo')
        self.realitease_sheet = self.spreadsheet.worksheet('RealiteaseInfo')
        print("✅ Located CastInfo and RealiteaseInfo sheets")

        self.cast_info_data = self._load_cast_info()

    def _load_cast_info(self):
        headers = [
            'Name', 'TMDbCastID', 'IMDbCastID', 'ShowName',
            'IMDbSeriesID', 'TMDbSeriesID', 'TotalEpisodes', 'TotalSeasons'
        ]
        print("🔄 Loading CastInfo records...")
        data = self.cast_info_sheet.get_all_records(expected_headers=headers)
        print(f"📊 Loaded {len(data)} rows from CastInfo")
        return data

    @staticmethod
    def _parse_positive_int(value):
        if value is None:
            return 0
        value = str(value).strip()
        if not value or value.lower() in {"none", "nan", "n/a"}:
            return 0
        match = re.search(r'-?\d+', value.replace(',', ''))
        if match:
            try:
                return max(0, int(match.group()))
            except ValueError:
                pass
        try:
            return max(0, int(float(value)))
        except (ValueError, TypeError):
            return 0

    def _build_cast_aggregates(self):
        aggregates = {}

        for idx, record in enumerate(self.cast_info_data, start=1):
            cast_name = (record.get('Name') or '').strip()
            cast_imdb_id = (record.get('IMDbCastID') or '').strip()
            tmdb_val = record.get('TMDbCastID')
            cast_tmdb_id = str(tmdb_val).strip() if tmdb_val not in (None, '') else ''
            if cast_tmdb_id.lower() == 'none':
                cast_tmdb_id = ''

            if not cast_imdb_id or not cast_name:
                continue

            aggregate = aggregates.setdefault(cast_imdb_id, {
                'cast_name': cast_name,
                'cast_tmdb_id': cast_tmdb_id,
                'show_ids': set(),
                'total_episodes': 0,
                'season_tokens': set(),
                'has_invalid_counts': False,
            })

            if not aggregate['cast_tmdb_id'] and cast_tmdb_id:
                aggregate['cast_tmdb_id'] = cast_tmdb_id

            show_imdb_id = (record.get('IMDbSeriesID') or '').strip().lower()
            if not show_imdb_id:
                show_name = (record.get('ShowName') or '').strip().lower()
                if show_name:
                    show_imdb_id = f"name::{show_name}"
            if show_imdb_id:
                aggregate['show_ids'].add(show_imdb_id)

            episodes_val = record.get('TotalEpisodes')
            seasons_val = record.get('TotalSeasons')

            episodes_text = str(episodes_val or '').strip()
            seasons_text = str(seasons_val or '').strip()
            if '**' in episodes_text or '**' in seasons_text:
                aggregate['has_invalid_counts'] = True

            aggregate['total_episodes'] += self._parse_positive_int(episodes_val)

            if seasons_text and seasons_text.lower() != 'none':
                tokens = re.split(r'[;,]', seasons_text)
                for token in tokens:
                    token = token.strip()
                    if not token:
                        continue
                    numbers = re.findall(r'\d+', token)
                    if numbers:
                        aggregate['season_tokens'].update(numbers)
                    else:
                        aggregate['season_tokens'].add(token.lower())

        return aggregates

    def _cast_meets_criteria(self, aggregate):
        if aggregate['has_invalid_counts']:
            return False
        show_count = len([sid for sid in aggregate['show_ids'] if sid])
        if show_count == 0:
            return False
        if show_count == 1:
            seasons_count = len([token for token in aggregate['season_tokens'] if token])
            episodes_total = aggregate['total_episodes']
            if seasons_count < 2 and episodes_total < 5:
                return False
        return True

    def _determine_allowed_ids(self):
        aggregates = self._build_cast_aggregates()
        allowed = {cid for cid, data in aggregates.items() if self._cast_meets_criteria(data)}
        print(f"📊 {len(allowed)} cast members meet the criteria out of {len(aggregates)} with valid IDs")
        return allowed

    def remove_non_qualifying(self, dry_run=True):
        allowed_ids = self._determine_allowed_ids()

        all_rows = self.realitease_sheet.get_all_values()
        to_delete = []
        for idx, row in enumerate(all_rows[1:], start=2):
            if len(row) < 2:
                continue
            cast_imdb_id = row[1].strip()
            if cast_imdb_id and cast_imdb_id not in allowed_ids:
                to_delete.append(idx)

        if not to_delete:
            print("✅ No rows need to be removed.")
            return 0

        print(f"🗑️ Identified {len(to_delete)} rows that no longer qualify.")
        if dry_run:
            print("🔍 Dry run enabled – no rows deleted. Sample IDs:")
            for row_num in to_delete[:10]:
                print(f"   • Row {row_num}")
            return len(to_delete)

        for row_num in reversed(to_delete):
            try:
                self.realitease_sheet.delete_rows(row_num)
                print(f"   🗑️ Removed row {row_num}")
                time.sleep(0.2)
            except Exception as exc:
                print(f"   ❌ Failed to remove row {row_num}: {exc}")
        print("✅ Cleanup complete")
        return len(to_delete)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='RealiteaseInfo Step 3 – remove non-qualifying cast rows')
    parser.add_argument('--dry-run', action='store_true', help='Preview rows that would be deleted without removing them')
    args = parser.parse_args()

    step3 = RealiteaseInfoStep3()
    deleted = step3.remove_non_qualifying(dry_run=args.dry_run)

    print("\n🎉 STEP 3 SUMMARY:")
    if args.dry_run:
        print(f"   🗒️ Rows that would be removed: {deleted}")
        print("   (Run without --dry-run to apply the deletions)")
    else:
        print(f"   🗑️ Rows removed: {deleted}")


if __name__ == '__main__':
    main()

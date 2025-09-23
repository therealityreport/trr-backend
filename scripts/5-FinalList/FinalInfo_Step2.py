#!/usr/bin/env python3
"""
FinalInfo Step 2 - IMDb API Enhancement

Enhances the FinalList sheet by:
1. Fetching alternative names from IMDb API for each cast member
2. Adding image URLs from IMDb API
3. Updating AlternativeNames and adding ImageURL columns

Uses the IMDb API: https://api.imdbapi.dev/names/{imdb_id}
"""

import argparse
import sys
import time
import requests
import gspread
from dotenv import load_dotenv


class FinalInfoEnhancer:
    def __init__(self, dry_run=True, batch_size=200, limit=None, stream_writes=False, delay=1.0, max_retries=3, retry_base=0.7):
        load_dotenv()
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.limit = limit
        self.stream_writes = stream_writes
        self.request_delay = delay
        self.max_retries = max_retries
        self.retry_base = retry_base

        self.gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sheet = self.gc.open('Realitease2025Data')
        try:
            self.finallist_worksheet = self.sheet.worksheet('FinalList')
        except gspread.WorksheetNotFound:
            print('❌ FinalList sheet not found. Please run FinalInfo_Step1.py first.')
            sys.exit(1)

        self.api_base_url = 'https://api.imdbapi.dev/names/'

        self.headers = []
        self.has_image_url = False
        self.finallist_data = []
        self.api_cache = {}
        self.stats = {
            'total_records': 0,
            'api_success': 0,
            'api_errors': 0,
            'alt_names_added': 0,
            'images_added': 0,
        }
        print('🎯 FinalInfo Step 2 Enhancer initialized')
        print(f'   📊 Batch size: {self.batch_size}')
        print(f'   ⏱️  API delay: {self.request_delay}s between requests')
        print(f'   🔁 Retries: max {self.max_retries}, base {self.retry_base}s')
        if self.limit:
            print(f'   🔢 Processing limit: {self.limit} records (for testing)')

    def load_finallist_data(self):
        print('📥 Loading FinalList data...')
        self.headers = self.finallist_worksheet.row_values(1)
        print(f'   📋 Current headers: {self.headers}')
        self.has_image_url = ('ImageURL' in self.headers) or ('imageURL' in self.headers)

        print('   🔍 Checking sheet dimensions...')
        all_values = self.finallist_worksheet.get_all_values()
        total_rows = len(all_values) - 1
        print(f'   📈 Found {total_rows} data rows to process')
        if total_rows <= 0:
            print('❌ No data found in FinalList sheet')
            sys.exit(1)

        # Add ImageURL column if missing (only when not dry-run)
        if not self.has_image_url:
            print('   🔧 Adding ImageURL column...')
            if not self.dry_run:
                new_headers = self.headers + ['ImageURL']
                self.finallist_worksheet.update('A1:F1', [new_headers])
                self.headers = new_headers
                self.has_image_url = True
                print('   ✅ ImageURL column added')

        records_to_process = all_values[1:]
        if self.limit and self.limit < len(records_to_process):
            records_to_process = records_to_process[:self.limit]
            print(f'   🔢 Limited to first {self.limit} records for testing')

        for i, row in enumerate(records_to_process, start=2):
            if i % 1000 == 0:
                print(f'      📍 Processing row {i-1}/{len(records_to_process)}')
            while len(row) < len(self.headers):
                row.append('')
            record = {
                'row_num': i,
                'name': row[0] if len(row) > 0 else '',
                'imdb_cast_id': row[1] if len(row) > 1 else '',
                'correct_date': row[2] if len(row) > 2 else '',
                'alternative_names': row[3] if len(row) > 3 else '',
                'imdb_series_ids': row[4] if len(row) > 4 else '',
                'image_url': row[5] if len(row) > 5 and self.has_image_url else '',
            }
            self.finallist_data.append(record)
        self.stats['total_records'] = len(self.finallist_data)
        print(f"   ✅ Loaded {self.stats['total_records']} FinalList records")

    def fetch_imdb_data(self, imdb_id):
        if not imdb_id or not imdb_id.startswith('nm'):
            return None
        if imdb_id in self.api_cache:
            return self.api_cache[imdb_id]
        url = f'{self.api_base_url}{imdb_id}'
        print(f'   🌐 Fetching: {imdb_id}')
        attempt = 0
        while attempt <= self.max_retries:
            try:
                resp = requests.get(url, headers={'accept': 'application/json'}, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    self.api_cache[imdb_id] = data
                    self.stats['api_success'] += 1
                    time.sleep(self.request_delay)
                    return data
                if resp.status_code == 404:
                    print(f'      ⚠️  Not found: {imdb_id}')
                    self.api_cache[imdb_id] = None
                    self.stats['api_errors'] += 1
                    time.sleep(self.request_delay)
                    return None
                # Retry on 5xx, 522, 429
                if resp.status_code in (429, 500, 502, 503, 504, 522):
                    attempt += 1
                    if attempt > self.max_retries:
                        print(f'      ❌ API Error {resp.status_code} after {self.max_retries} retries: {imdb_id}')
                        self.stats['api_errors'] += 1
                        time.sleep(self.request_delay)
                        return None
                    backoff = self.retry_base * (2 ** (attempt - 1))
                    jitter = min(0.5, backoff * 0.1)
                    print(f'      🔁 Retry {attempt}/{self.max_retries} in {backoff + jitter:.1f}s (status {resp.status_code})')
                    time.sleep(backoff + jitter)
                    continue
                # Other errors – log and continue
                print(f'      ❌ API Error {resp.status_code}: {imdb_id}')
                self.stats['api_errors'] += 1
                time.sleep(self.request_delay)
                return None
            except requests.RequestException as e:
                attempt += 1
                if attempt > self.max_retries:
                    print(f'      ❌ Request failed after retries for {imdb_id}: {e}')
                    self.stats['api_errors'] += 1
                    time.sleep(self.request_delay)
                    return None
                backoff = self.retry_base * (2 ** (attempt - 1))
                jitter = min(0.5, backoff * 0.1)
                print(f'      🔁 Network error, retry {attempt}/{self.max_retries} in {backoff + jitter:.1f}s: {e}')
                time.sleep(backoff + jitter)
        return None

    def extract_alternative_names(self, imdb_data):
        names = []
        if not imdb_data:
            return names
        if imdb_data.get('alternativeNames'):
            names.extend(imdb_data['alternativeNames'])
        birth = imdb_data.get('birthName')
        display = imdb_data.get('displayName')
        if birth and birth != display and birth not in names:
            names.append(birth)
        return names

    def extract_image_url(self, imdb_data):
        if imdb_data and isinstance(imdb_data.get('primaryImage'), dict):
            return imdb_data['primaryImage'].get('url', '')
        return ''

    def merge_alternative_names(self, existing_names, new_names):
        existing_list = []
        if existing_names:
            existing_list = [n.strip() for n in existing_names.split(',') if n.strip()]
        merged = existing_list.copy()
        for n in new_names:
            n = n.strip()
            if n and n not in merged:
                merged.append(n)
        return ', '.join(merged)

    def _record_to_row(self, record):
        return [
            record['name'],
            record['imdb_cast_id'],
            record['correct_date'],
            record['alternative_names'],
            record['imdb_series_ids'],
            record['image_url'],
        ]

    def _write_contiguous_batch(self, start_row, records):
        if not records:
            return
        end_row = start_row + len(records) - 1
        values = [self._record_to_row(r) for r in records]
        print(f"📤 Writing streamed batch: rows {start_row}-{end_row} ({len(records)} records)")
        self.finallist_worksheet.update(f'A{start_row}:F{end_row}', values)
        print('   ✅ Streamed batch written successfully')

    def enhance_records(self):
        print(f'🔍 Enhancing {len(self.finallist_data)} records with IMDb API data...')
        enhanced_records = []
        buffer = []
        buffer_start_row = None
        for i, record in enumerate(self.finallist_data):
            if i % 100 == 0:
                print(f"📍 Progress: {i+1}/{len(self.finallist_data)} records ({((i+1)/len(self.finallist_data)*100):.1f}%)")
            print(f"🔎 Processing {i+1}/{len(self.finallist_data)}: {record['name']} ({record['imdb_cast_id']})")

            imdb_data = self.fetch_imdb_data(record['imdb_cast_id'])
            new_alt = self.extract_alternative_names(imdb_data)
            image_url = self.extract_image_url(imdb_data)
            enhanced_alt = self.merge_alternative_names(record['alternative_names'], new_alt)

            enhanced = record.copy()
            enhanced['alternative_names'] = enhanced_alt
            enhanced['image_url'] = image_url

            if new_alt:
                self.stats['alt_names_added'] += 1
                print(f"   ✅ Added alternative names: {', '.join(new_alt)}")
            if image_url:
                self.stats['images_added'] += 1
                print(f"   ✅ Added image URL: {image_url[:60]}...")

            enhanced_records.append(enhanced)

            if self.stream_writes and not self.dry_run:
                buffer.append(enhanced)
                if buffer_start_row is None:
                    buffer_start_row = enhanced['row_num']
                if len(buffer) >= self.batch_size:
                    self._write_contiguous_batch(buffer_start_row, buffer)
                    buffer = []
                    buffer_start_row = None

        if self.stream_writes and not self.dry_run and buffer:
            self._write_contiguous_batch(buffer_start_row, buffer)

        print('📊 API Enhancement Complete:')
        print(f"   ✅ {self.stats['api_success']} successful API calls")
        print(f"   ❌ {self.stats['api_errors']} failed API calls")
        print(f"   📝 {self.stats['alt_names_added']} records with new alternative names")
        print(f"   🖼️  {self.stats['images_added']} records with images")
        return enhanced_records

    def update_finallist_sheet(self, enhanced_records):
        if self.dry_run:
            print('🧪 DRY RUN - Would update FinalList sheet')
            return
        if self.stream_writes:
            print('ℹ️  Stream-writes enabled, skipping final bulk update')
            return
        print(f'📝 Updating FinalList sheet in batches of {self.batch_size}...')
        update_data = [self._record_to_row(r) for r in enhanced_records]
        total = len(update_data)
        total_batches = (total + self.batch_size - 1) // self.batch_size
        for b in range(total_batches):
            start_idx = b * self.batch_size
            end_idx = min(start_idx + self.batch_size, total)
            start_row = start_idx + 2
            end_row = end_idx + 1
            batch = update_data[start_idx:end_idx]
            print(f"📤 Writing batch {b+1}/{total_batches}: rows {start_row}-{end_row} ({len(batch)} records)")
            self.finallist_worksheet.update(f'A{start_row}:F{end_row}', batch)
            print(f'   ✅ Batch {b+1} written successfully')
        print(f'   ✅ Updated FinalList sheet with enhanced data in {total_batches} batches')

    def print_stats(self):
        print('\n📊 Enhancement Statistics:')
        print(f"   Total records processed: {self.stats['total_records']}")
        print(f"   API requests successful: {self.stats['api_success']}")
        print(f"   API requests failed: {self.stats['api_errors']}")
        print(f"   Records with new alternative names: {self.stats['alt_names_added']}")
        print(f"   Records with images added: {self.stats['images_added']}")
        total_requests = self.stats['api_success'] + self.stats['api_errors']
        if total_requests:
            success_rate = (self.stats['api_success'] / total_requests) * 100
            print(f"   API success rate: {success_rate:.1f}%")

    def run(self):
        try:
            self.load_finallist_data()
            enhanced_records = self.enhance_records()
            self.update_finallist_sheet(enhanced_records)
            self.print_stats()
            print('\n🎉 FinalInfo Step 2 enhancement complete!')
            if self.dry_run:
                print('   🧪 This was a DRY RUN - no sheets were modified')
                print('   🚀 Run without --dry-run to update the actual FinalList sheet')
        except Exception as e:
            print(f'❌ Error: {e}')
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='FinalInfo Step 2: Enhance FinalList with IMDb API data')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no sheet updates)')
    parser.add_argument('--batch-size', type=int, default=200, help='Number of rows to update per batch (default: 200)')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay in seconds between API requests (default: 1.0)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of records to process (for testing)')
    parser.add_argument('--stream-writes', action='store_true', help='Write each processed batch to the sheet incrementally')
    parser.add_argument('--max-retries', type=int, default=3, help='Max retries for transient errors (default: 3)')
    parser.add_argument('--retry-base', type=float, default=0.7, help='Base seconds for exponential backoff (default: 0.7)')

    args = parser.parse_args()
    enhancer = FinalInfoEnhancer(
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        limit=args.limit,
        stream_writes=args.stream_writes,
        delay=args.delay,
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )
    enhancer.run()


if __name__ == '__main__':
    main()
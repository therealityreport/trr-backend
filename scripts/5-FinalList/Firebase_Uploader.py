#!/usr/bin/env python3
"""
Firebase Uploader (separate utility)

Uploads the enhanced FinalList data to Firebase (Realtime DB or Firestore).
This tool is intentionally decoupled from the numbered FinalInfo steps.
"""
import os
import sys
import time
import argparse
import gspread
from dotenv import load_dotenv

try:
    import firebase_admin
    from firebase_admin import credentials, db, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

class FirebaseUploader:
    def __init__(self, dry_run=True, batch_size=100, use_firestore=False):
        load_dotenv()
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.use_firestore = use_firestore

        if not FIREBASE_AVAILABLE:
            print("❌ Firebase Admin SDK is required. Install with: pip install firebase-admin")
            sys.exit(1)

        self.gc = gspread.service_account(filename=os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', 'keys/trr-backend-df2c438612e1.json'))
        self.sheet = self.gc.open('Realitease2025Data')
        try:
            self.ws = self.sheet.worksheet('FinalList')
        except gspread.WorksheetNotFound:
            print("❌ FinalList sheet not found")
            sys.exit(1)

        self.data = []
        self.app = None
        self.db_ref = None
        self.fs = None

    def init_firebase(self):
        key_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_FILE', 'keys/firebase-admin-service-account.json')
        if not os.path.exists(key_path):
            print(f"❌ Firebase key not found at {key_path}")
            sys.exit(1)
        cred = credentials.Certificate(key_path)
        config = {}
        if not self.use_firestore:
            db_url = os.getenv('FIREBASE_DATABASE_URL')
            if not db_url:
                print("❌ FIREBASE_DATABASE_URL not set")
                sys.exit(1)
            config['databaseURL'] = db_url
        self.app = firebase_admin.initialize_app(cred, config)
        if self.use_firestore:
            self.fs = firestore.client()
        else:
            self.db_ref = db.reference('/cast_members')

    def load(self):
        rows = self.ws.get_all_values()
        headers = rows[0]
        idx = {h:i for i,h in enumerate(headers)}
        for r in rows[1:]:
            while len(r) < len(headers):
                r.append("")
            self.data.append({
                'id': r[idx['IMDbCastID']],
                'name': r[idx['Name']],
                'imdbCastId': r[idx['IMDbCastID']],
                'alternativeNames': [s.strip() for s in r[idx['AlternativeNames']].split(',') if s.strip()] if 'AlternativeNames' in idx else [],
                'imdbSeriesIds': [s.strip() for s in r[idx['IMDbSeriesIDs']].split(',') if s.strip()] if 'IMDbSeriesIDs' in idx else [],
                'imageUrl': r[idx['ImageURL']] if 'ImageURL' in idx and r[idx['ImageURL']] else None,
                'lastUpdated': int(time.time()),
                'source': 'realitease_final_list',
            })

    def upload_batch(self, batch):
        if self.dry_run:
            print(f"🧪 Would upload {len(batch)} records")
            return
        if self.use_firestore:
            collection = self.fs.collection('cast_members')
            b = self.fs.batch()
            for rec in batch:
                b.set(collection.document(rec['id']), rec)
            b.commit()
        else:
            self.db_ref.update({rec['id']: rec for rec in batch})

    def run(self):
        self.init_firebase()
        self.load()
        total = len(self.data)
        bs = self.batch_size
        for i in range(0, total, bs):
            batch = self.data[i:i+bs]
            print(f"📤 Uploading {i+1}-{min(i+bs,total)} of {total}")
            self.upload_batch(batch)
            if not self.dry_run:
                time.sleep(0.4)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch-size', type=int, default=100)
    ap.add_argument('--firestore', action='store_true')
    args = ap.parse_args()
    FirebaseUploader(dry_run=args.dry_run, batch_size=args.batch_size, use_firestore=args.firestore).run()

#!/usr/bin/env python3
import gspread

def test_connection():
    try:
        gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
        spreadsheet = gc.open('Realitease2025Data')
        print("✅ Connected successfully!")
        
        # List sheets
        worksheets = spreadsheet.worksheets()
        print(f"📊 Found {len(worksheets)} worksheets:")
        for ws in worksheets:
            print(f"  - {ws.title}")
            
        # Test CastInfo sheet
        cast_sheet = spreadsheet.worksheet('CastInfo')
        data = cast_sheet.get_all_records()
        print(f"📊 CastInfo has {len(data)} records")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()

#!/usr/bin/env python3
"""
Quick test to update existing CastInfo entries with IMDb IDs
"""

import gspread
from dotenv import load_dotenv
import os

# Load environment
load_dotenv('../.env')

def test_update():
    # Connect to Google Sheets
    gc = gspread.service_account(filename='../keys/trr-backend-df2c438612e1.json')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    sh = gc.open_by_key(spreadsheet_id)
    
    # Get CastInfo data
    cast_ws = sh.worksheet('CastInfo')
    data = cast_ws.get_all_values()
    
    print(f"Current CastInfo has {len(data)-1} rows")
    print("Header:", data[0] if data else "No data")
    
    # Show first few rows to verify structure
    for i, row in enumerate(data[1:6], 1):
        print(f"Row {i+1}: {row}")

if __name__ == "__main__":
    test_update()

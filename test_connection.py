#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

import gspread
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent


def _resolve_service_account_path() -> Path:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    candidate = Path(raw) if raw else Path("keys/service-account.json")
    candidate = candidate.expanduser()
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    return candidate


def test_connection() -> None:
    load_dotenv(dotenv_path=REPO_ROOT / ".env")

    try:
        creds_path = _resolve_service_account_path()
        if not creds_path.exists():
            raise FileNotFoundError(
                "Google service account JSON not found. "
                "Set GOOGLE_APPLICATION_CREDENTIALS (or GOOGLE_SERVICE_ACCOUNT_FILE) or place it at keys/service-account.json."  # noqa: E501
            )

        gc = gspread.service_account(filename=str(creds_path))

        spreadsheet_id = (os.getenv("SPREADSHEET_ID") or "").strip()
        if spreadsheet_id:
            spreadsheet = gc.open_by_key(spreadsheet_id)
        else:
            spreadsheet_name = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
            spreadsheet = gc.open(spreadsheet_name)

        print("✅ Connected successfully!")

        worksheets = spreadsheet.worksheets()
        print(f"📊 Found {len(worksheets)} worksheets:")
        for ws in worksheets:
            print(f"  - {ws.title}")

        if "CastInfo" in [ws.title for ws in worksheets]:
            cast_sheet = spreadsheet.worksheet("CastInfo")
            data = cast_sheet.get_all_records()
            print(f"📊 CastInfo has {len(data)} records")
        else:
            print("ℹ️  CastInfo sheet not found (skipping record count).")

    except Exception as exc:
        print(f"❌ Error: {exc}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_connection()

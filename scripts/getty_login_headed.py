#!/usr/bin/env python3
"""
Launch a HEADED Playwright Chromium browser with the openai-agent Chrome profile
and navigate to the Getty Images sign-in page.

The user logs in manually. Once authenticated, the Chrome profile persists the
session cookies so that `getty_local_prefetch.py` can reuse them headlessly.

Usage:
    python scripts/getty_login_headed.py

Press Ctrl+C in the terminal once you're done to close the browser.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

PROFILE_DIR = Path.home() / ".chrome-profiles" / "openai-agent"
GETTY_SIGN_IN_URL = "https://www.gettyimages.com/sign-in"


def main() -> None:
    # Create profile directory if it doesn't exist
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Chrome profile directory: {PROFILE_DIR}")
    print(f"Navigating to: {GETTY_SIGN_IN_URL}")
    print("Log in manually. Press Ctrl+C in this terminal when done.\n")

    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        # Find Chrome executable (same logic as getty_local_prefetch.py)
        chrome_path = None
        for candidate in [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium-browser",
        ]:
            if os.path.isfile(candidate):
                chrome_path = candidate
                break

        launch_kwargs: dict = {
            "user_data_dir": str(PROFILE_DIR),
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
        }
        if chrome_path:
            launch_kwargs["executable_path"] = chrome_path
            print(f"Using Chrome at: {chrome_path}")

        ctx = pw.chromium.launch_persistent_context(**launch_kwargs)
        page = ctx.new_page()
        page.goto(GETTY_SIGN_IN_URL, wait_until="domcontentloaded", timeout=60_000)

        print("\n--- Browser is open. Log in to Getty Images now. ---")
        print("After login, I'll check if authentication was successful.\n")

        # Keep browser open until user presses Ctrl+C or closes it
        try:
            while True:
                page.wait_for_timeout(2000)
                current_url = str(page.url or "")
                if "/sign-in" not in current_url and "gettyimages.com" in current_url:
                    print(f"✓ Authenticated! Redirected to: {current_url}")
                    print("Session cookies are now saved in the Chrome profile.")
                    print("You can close this browser (Ctrl+C) and re-run the Getty scrape.\n")
                    # Keep the browser open so user can verify
                    page.wait_for_timeout(5000)
                    break
        except KeyboardInterrupt:
            print("\nClosing browser...")

        # Check final auth state
        final_url = str(page.url or "")
        if "/sign-in" not in final_url:
            print(f"✓ Login successful! Profile saved at: {PROFILE_DIR}")
        else:
            print("✗ Still on sign-in page. You may need to try again.")

        ctx.close()


if __name__ == "__main__":
    main()

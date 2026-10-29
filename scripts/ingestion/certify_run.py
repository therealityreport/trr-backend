#!/usr/bin/env python3
"""Certify an explicit Instagram account/date range using retained evidence.

Legacy artifact field names remain stable for previously saved certificates.
"""

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ingestion.certify_january import main

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Recover a selected Instagram run; its frozen intent supplies the date range.

Use --help for exact run, generation and package inputs. Preparation is read-only;
--execute starts the requested recovery with durable ownership and progress limits.
"""

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ingestion.recover_january_window import main

if __name__ == "__main__":
    raise SystemExit(main())

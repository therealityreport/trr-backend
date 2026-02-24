#!/usr/bin/env python3
from __future__ import annotations

import importlib

_mod = importlib.import_module("scripts.backfill.backfill_bravo_video_thumbnails")
main = _mod.main

if __name__ == "__main__":
    raise SystemExit(main())

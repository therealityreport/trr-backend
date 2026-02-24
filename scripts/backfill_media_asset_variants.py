#!/usr/bin/env python3
from __future__ import annotations

import importlib

_mod = importlib.import_module("scripts.media.backfill_media_asset_variants")
for _k, _v in _mod.__dict__.items():
    if _k.startswith("__"):
        continue
    globals()[_k] = _v

if __name__ == "__main__":
    if hasattr(_mod, "main"):
        raise SystemExit(_mod.main())

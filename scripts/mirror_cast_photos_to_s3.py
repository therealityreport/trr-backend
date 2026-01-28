#!/usr/bin/env python3
from __future__ import annotations

import importlib

_mod = importlib.import_module("scripts.media.mirror_cast_photos_to_s3")
for _k, _v in _mod.__dict__.items():
    if _k.startswith("__"):  # skip dunder
        continue
    globals()[_k] = _v

if __name__ == "__main__":
    if hasattr(_mod, "main"):
        raise SystemExit(_mod.main())

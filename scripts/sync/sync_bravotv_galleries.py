#!/usr/bin/env python3
from __future__ import annotations

import sys
import warnings
from pathlib import Path

SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from media.sync_bravotv_galleries import main  # noqa: E402

if __name__ == "__main__":
    print(
        "Deprecated: scripts/sync/sync_bravotv_galleries.py; use scripts/media/sync_bravotv_galleries.py instead.",
        file=sys.stderr,
    )
    warnings.warn(
        "scripts/sync/sync_bravotv_galleries.py is deprecated; use scripts/media/sync_bravotv_galleries.py instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    raise SystemExit(main())

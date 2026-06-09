#!/usr/bin/env python3
from __future__ import annotations

import warnings
import sys

from media.bravotv_get_images import main


if __name__ == "__main__":
    print(
        "Deprecated: scripts/bravotv_get_images.py; use scripts/media/bravotv_get_images.py instead.",
        file=sys.stderr,
    )
    warnings.warn(
        "scripts/bravotv_get_images.py is deprecated; use scripts/media/bravotv_get_images.py instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    raise SystemExit(main())

#!/usr/bin/env python3
"""Dedicated Instagram comments Scrapling worker wrapper."""

from __future__ import annotations

import os
import sys

from scripts.socials.worker import main as worker_main


def main() -> int:
    os.environ.setdefault("SOCIAL_WORKER_LANE", "instagram_comments_scrapling")
    os.environ.setdefault("SOCIAL_WORKER_SCRIPT", "scripts.socials.instagram.comments_worker")
    argv = [
        sys.argv[0],
        "--stage",
        "comments_scrapling",
        "--platform",
        "instagram",
        *sys.argv[1:],
    ]
    return worker_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())

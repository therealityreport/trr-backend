"""Shared social pipeline orchestration.

Pipeline modules own launch/progress orchestration for scraper workflows. They
may call platform modules and persistence adapters, but they should not own API
request parsing, route response shaping, or worker queue lifecycle primitives.
"""

from __future__ import annotations

"""Compatibility re-export; the parser lives in ``trr_backend.socials.tiktok.parsing.profile``.

These helpers parse direct profile preview payloads only. They are not part of
the queued TikTok posts Scraping lane and do not define a comments ingestion
contract.
"""

from __future__ import annotations

from trr_backend.socials.tiktok.parsing.profile import extract_profile_preview_sections

__all__ = ["extract_profile_preview_sections"]

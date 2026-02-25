"""
Social media scraping and analytics module.

This module provides tools for fetching and analyzing social media posts
from platforms like Instagram, TikTok, Twitter, YouTube, etc.

Submodules:
- crawlee_runtime: Incremental Crawlee wrappers for queue-stage execution.
- instagram: Instagram scraping and analytics
- tiktok: TikTok scraping and analytics
- twitter: Twitter/X search and analytics
- youtube: YouTube channel video scraping
"""

from . import crawlee_runtime, instagram, tiktok, twitter, youtube

__all__ = ["crawlee_runtime", "instagram", "tiktok", "twitter", "youtube"]

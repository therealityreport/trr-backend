"""
Social media scraping and analytics module.

This module provides tools for fetching and analyzing social media posts
from platforms like Instagram, TikTok, Twitter, YouTube, etc.

Submodules:
- instagram: Instagram scraping and analytics
- tiktok: TikTok scraping and analytics
- twitter: Twitter/X search and analytics
- youtube: YouTube channel video scraping
"""

from . import instagram, tiktok, twitter, youtube

__all__ = ["instagram", "tiktok", "twitter", "youtube"]

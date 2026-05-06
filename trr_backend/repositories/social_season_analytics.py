"""Compatibility alias for the social season analytics implementation."""

from __future__ import annotations

import sys as _sys

from trr_backend.socials import social_season_analytics_impl as _impl

_sys.modules[__name__] = _impl

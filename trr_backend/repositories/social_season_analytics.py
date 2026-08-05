"""Compatibility alias for the social season analytics implementation."""

from __future__ import annotations

import sys as _sys
from importlib import import_module as _import_module

_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"
_impl = _import_module(_IMPL_MODULE)

_sys.modules[__name__] = _impl

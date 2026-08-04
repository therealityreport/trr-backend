"""Windowing and scheduling helpers for the social control plane."""

from __future__ import annotations

import sys as _sys

from trr_backend.socials import windowing as _windowing

_sys.modules[__name__] = _windowing

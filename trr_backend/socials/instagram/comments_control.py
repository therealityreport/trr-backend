"""Compatibility bridge for Instagram comments control entrypoints.

Canonical owner: `trr_backend.socials.pipelines.comments.instagram`.
Retained while legacy imports from `trr_backend.socials.instagram.comments_control`
and core compatibility wrappers are migrated to the universal pipelines package.
"""

from __future__ import annotations

import sys

from trr_backend.socials.pipelines.comments import instagram as _pipeline
from trr_backend.socials.pipelines.comments.instagram import *  # noqa: F403
from trr_backend.socials.pipelines.comments.instagram import (
    __all__ as _pipeline_all,
)

__all__ = [*_pipeline_all]

# Make the historical module path an alias of the canonical pipeline module so
# monkeypatches on `trr_backend.socials.instagram.comments_control` affect the
# executable owner.
sys.modules[__name__] = _pipeline

"""Regression coverage for hosted-media SQL fragments used in analytics reads."""

from __future__ import annotations

import re

from trr_backend.socials.social_season_analytics_impl import _hosted_media_sql_usable_condition


def test_hosted_media_sql_usable_condition_escapes_literal_like_percent_patterns() -> None:
    sql = _hosted_media_sql_usable_condition("p")

    assert "like '%%asset_wrong_content_type%%'" in sql
    assert not re.search(r"(?<!%)%(?!%)", sql)

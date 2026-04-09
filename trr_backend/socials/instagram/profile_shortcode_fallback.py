from __future__ import annotations

import re

_PROFILE_SHORTCODE_RE = re.compile(r"/(?:p|reel|tv)/([A-Za-z0-9_-]{5,32})(?=[/?#'\"&]|$)")


def extract_profile_shortcodes(html: str, *, limit: int = 24) -> list[str]:
    seen: set[str] = set()
    shortcodes: list[str] = []
    for match in _PROFILE_SHORTCODE_RE.finditer(str(html or "")):
        shortcode = str(match.group(1) or "").strip()
        if not shortcode or shortcode in seen:
            continue
        seen.add(shortcode)
        shortcodes.append(shortcode)
        if len(shortcodes) >= max(1, int(limit)):
            break
    return shortcodes

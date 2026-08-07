from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any, cast

from bs4 import BeautifulSoup
from bs4.element import Tag

_REVISION_PATTERNS = (
    r"fandom_revid:(\d+)",
    r"wgRevisionId\"\s*:\s*(\d+)",
    r"wgRevisionId\s*=\s*(\d+)",
    r"\"revisionId\"\s*:\s*(\d+)",
)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _find_article_root(soup: BeautifulSoup) -> Tag:
    for selector in (
        "div.mw-parser-output",
        "div.page-content",
        "div.article-content",
        "article#content",
        "div#mw-content-text",
    ):
        node = soup.select_one(selector)
        if node:
            return node
    return soup


def _extract_page_title(soup: BeautifulSoup) -> str | None:
    for selector in ("h1.page-header__title", "h1#firstHeading", "h1"):
        node = soup.select_one(selector)
        if node:
            text = _normalize_text(node.get_text(" ", strip=True))
            if text:
                return text
    title = soup.title.string if soup.title else None
    return _normalize_text(title)


def _parse_revision_id(html: str) -> int | None:
    for pattern in _REVISION_PATTERNS:
        match = re.search(pattern, html)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            continue
    return None


def _canonicalize_section(title: str | None) -> str:
    normalized = _normalize_text(title) or ""
    key = normalized.casefold()
    if "cast" in key:
        return "Casting"
    if "biograph" in key or "overview" in key:
        return "Biography"
    if "tagline" in key:
        return "Taglines"
    if "reunion" in key and "seating" in key:
        return "Reunion Seating"
    return normalized


def _extract_table_rows(node: Tag) -> list[dict[str, str | None]]:
    rows = node.find_all("tr")
    if not rows:
        return []
    headers = [_normalize_text(cell.get_text(" ", strip=True)) or "" for cell in rows[0].find_all(["th", "td"])]
    out: list[dict[str, str | None]] = []
    for row in rows[1:]:
        values = [_normalize_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
        if not values:
            continue
        if headers and len(values) == len(headers):
            out.append(dict(zip(headers, values, strict=False)))
        else:
            out.append({f"col_{idx + 1}": value for idx, value in enumerate(values)})
    return out


def _extract_dynamic_sections(article_root: Tag) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for heading in article_root.find_all(["h2", "h3", "h4"]):
        title = _normalize_text(heading.get_text(" ", strip=True))
        if not title:
            continue

        paragraphs: list[str] = []
        bullets: list[str] = []
        table_rows: list[dict[str, str | None]] = []

        for sibling in heading.next_siblings:
            sibling_name = getattr(sibling, "name", None)
            if sibling_name in {"h2", "h3", "h4"}:
                break
            if sibling_name == "p":
                text = _normalize_text(sibling.get_text(" ", strip=True))
                if text:
                    paragraphs.append(text)
                continue
            if sibling_name in {"ul", "ol"}:
                for li in cast(Tag, sibling).find_all("li"):
                    text = _normalize_text(li.get_text(" ", strip=True))
                    if text:
                        bullets.append(text)
                continue
            if sibling_name == "table":
                table_rows.extend(_extract_table_rows(cast(Tag, sibling)))

        if not paragraphs and not bullets and not table_rows:
            continue

        sections.append(
            {
                "title": title,
                "canonical_title": _canonicalize_section(title),
                "paragraphs": paragraphs,
                "bullets": bullets,
                "table_rows": table_rows,
            }
        )
    return sections


def _extract_summary(article_root: Tag) -> str | None:
    for paragraph in article_root.find_all("p"):
        text = _normalize_text(paragraph.get_text(" ", strip=True))
        if text:
            return text
    return None


def parse_fandom_season_html(
    html: str,
    *,
    source_url: str,
) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    article_root = _find_article_root(soup)
    summary = _extract_summary(article_root)
    sections = _extract_dynamic_sections(article_root)
    return {
        "source": "fandom",
        "source_url": source_url,
        "page_title": _extract_page_title(soup),
        "page_revision_id": _parse_revision_id(html or ""),
        "scraped_at": datetime.now(UTC).isoformat(),
        "summary": summary,
        "dynamic_sections": sections or None,
        "raw_html_sha256": hashlib.sha256((html or "").encode("utf-8")).hexdigest() if html else None,
    }

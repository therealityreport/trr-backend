from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

_IMDB_TITLE_ID_RE = re.compile(r"^(tt[0-9]+)$", re.IGNORECASE)
_IMDB_COMPANY_HREF_RE = re.compile(r"/company/(co[0-9]+)(?:/|\?|$)", re.IGNORECASE)

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


class ImdbCompanyCreditsError(RuntimeError):
    """Raised when IMDb company-credits fetching/parsing fails."""


@dataclass(frozen=True)
class ImdbCompanyCredit:
    name: str
    company_url: str
    company_id: str | None
    category: str | None


@dataclass(frozen=True)
class ImdbCompanyCreditsResult:
    imdb_id: str
    source_url: str
    companies: list[ImdbCompanyCredit]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _load_extra_headers_from_env() -> dict[str, str]:
    raw = (os.getenv("IMDB_EXTRA_HEADERS_JSON") or "").strip()
    if not raw:
        raw = (os.getenv("IMDB_HEADERS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in payload.items():
        key_text = _normalize_text(key)
        value_text = _normalize_text(value)
        if key_text and value_text:
            out[key_text] = value_text
    return out


def _extract_category(anchor) -> str | None:
    heading = anchor.find_previous(["h2", "h3", "h4"]) if anchor else None
    if heading is not None:
        label = _normalize_text(heading.get_text(" ", strip=True))
        if label:
            return label

    section = anchor.find_parent("section") if anchor else None
    if section is not None:
        aria = _normalize_text(section.get("aria-label"))
        if aria:
            return aria
        identifier = _normalize_text(section.get("id"))
        if identifier:
            return identifier

    return None


def parse_imdb_company_credits_html(html: str, *, base_url: str = "https://www.imdb.com") -> list[ImdbCompanyCredit]:
    soup = BeautifulSoup(html or "", "html.parser")
    out: list[ImdbCompanyCredit] = []
    seen: set[tuple[str, str]] = set()

    for anchor in soup.find_all("a", href=True):
        href = _normalize_text(anchor.get("href"))
        if not href:
            continue
        match = _IMDB_COMPANY_HREF_RE.search(href)
        if not match:
            continue

        company_id = _normalize_text(match.group(1)).lower() or None
        company_name = _normalize_text(anchor.get_text(" ", strip=True))
        if not company_name:
            continue

        company_url = urljoin(base_url, href.split("#", 1)[0])
        dedupe_key = (company_id or "", company_url)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        out.append(
            ImdbCompanyCredit(
                name=company_name,
                company_url=company_url,
                company_id=company_id,
                category=_extract_category(anchor),
            )
        )

    return out


def fetch_imdb_company_credits(
    imdb_id: str,
    *,
    timeout_seconds: float = 20.0,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
) -> ImdbCompanyCreditsResult:
    title_id = _normalize_text(imdb_id)
    if not _IMDB_TITLE_ID_RE.match(title_id):
        raise ImdbCompanyCreditsError("invalid_imdb_id")

    source_url = f"https://www.imdb.com/title/{title_id}/companycredits/?ref_=tt_dt_cmpy#production"
    merged_headers = {**_DEFAULT_HEADERS, **_load_extra_headers_from_env(), **(extra_headers or {})}

    sess = session or requests.Session()
    try:
        response = sess.get(source_url, headers=merged_headers, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise ImdbCompanyCreditsError(f"imdb_companycredits_request_failed: {exc}") from exc

    if response.status_code >= 400:
        raise ImdbCompanyCreditsError(f"imdb_companycredits_http_{response.status_code}")

    html = response.text or ""
    companies = parse_imdb_company_credits_html(html)
    return ImdbCompanyCreditsResult(imdb_id=title_id, source_url=source_url, companies=companies)

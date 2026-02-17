from __future__ import annotations

import re
import unicodedata
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

_WIKI_ROLE_MAP = {
    "main": "Housewife",
    "friend": "Friend",
    "guest": "Guest",
}

_KID_RE = re.compile(r"\b(son|daughter|child|kid|children)\b", re.IGNORECASE)
_RELATION_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bex\s*[- ]?husband\b", re.IGNORECASE), "Ex-Husband"),
    (re.compile(r"\bhusband\b", re.IGNORECASE), "Husband"),
    (re.compile(r"\bex\s*[- ]?spouse\b", re.IGNORECASE), "Ex-Husband"),
    (re.compile(r"\bspouse\b", re.IGNORECASE), "Husband"),
    (re.compile(r"\bex\s*[- ]?boyfriend\b", re.IGNORECASE), "Ex-Boyfriend"),
    (re.compile(r"\bboyfriend\b", re.IGNORECASE), "Boyfriend"),
    (re.compile(r"\bex\s*[- ]?partner\b", re.IGNORECASE), "Ex-Boyfriend"),
    (re.compile(r"\bpartner\b", re.IGNORECASE), "Boyfriend"),
    (re.compile(r"\bex\s*[- ]?fiance(?:e|é)?\b", re.IGNORECASE), "Ex-Fiance"),
    (re.compile(r"\bfiance(?:e|é)?\b", re.IGNORECASE), "Fiance"),
]

if requests is not None:
    _FETCH_ERRORS: tuple[type[BaseException], ...] = (requests.RequestException, urllib.error.URLError)
else:
    _FETCH_ERRORS = (urllib.error.URLError,)


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower().strip()).strip("_")


def build_default_wikipedia_url(show_name: str) -> str:
    return f"https://en.wikipedia.org/wiki/{quote(show_name.replace(' ', '_'))}"


def build_default_fandom_url(show_name: str) -> str:
    safe = quote(show_name.replace(" ", "_"))
    return f"https://real-housewives.fandom.com/wiki/{safe}"


def build_person_wikipedia_url(person_name: str) -> str:
    safe = quote(person_name.replace(" ", "_"))
    return f"https://en.wikipedia.org/wiki/{safe}"


def build_person_fandom_url(person_name: str) -> str:
    safe = quote(person_name.replace(" ", "_"))
    return f"https://real-housewives.fandom.com/wiki/{safe}"


def fetch_html(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> tuple[str, str]:
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}

    if requests is not None:
        requester = session or requests
        response = requester.get(url, headers=merged_headers, timeout=(5, 30), allow_redirects=True)
        response.raise_for_status()
        return response.text or "", str(response.url)

    request = urllib.request.Request(url, headers=merged_headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        data = response.read() or b""
        charset = response.headers.get_content_charset() or "utf-8"
        return data.decode(charset, errors="replace"), str(response.geturl())


def _extract_season_headers(rows: list[Any]) -> list[int]:
    seasons: list[int] = []
    seen: set[int] = set()
    for row in rows[:4]:
        for cell in row.find_all(["th", "td"]):
            text = _normalize_text(cell.get_text(" ", strip=True))
            if not text or not re.fullmatch(r"\d{1,3}", text):
                continue
            season = int(text)
            if season <= 0 or season > 200 or season in seen:
                continue
            seen.add(season)
            seasons.append(season)
    return seasons


def _map_wiki_role(value: str) -> str | None:
    lowered = _normalize_text(value).lower()
    if not lowered or "tba" in lowered:
        return None
    for token, role in _WIKI_ROLE_MAP.items():
        if token in lowered:
            return role
    return None


def parse_wikipedia_cast_matrix_html(html: str) -> dict[str, dict[int, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    table = None
    for candidate in soup.select("table.wikitable"):
        text = candidate.get_text(" ", strip=True)
        lowered = text.lower()
        if "cast member" in lowered and "seasons" in lowered:
            table = candidate
            break
    if table is None:
        return {}

    rows = table.find_all("tr")
    if not rows:
        return {}

    seasons = _extract_season_headers(rows)
    if not seasons:
        return {}

    matrix: dict[str, dict[int, str]] = {}
    for row in rows:
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) < 2:
            continue
        first_text = _normalize_text(cells[0].get_text(" ", strip=True))
        if not first_text:
            continue
        first_lower = first_text.lower()
        if first_lower in {"cast member", "season", "seasons"}:
            continue
        if re.fullmatch(r"\d+", first_text):
            continue
        if "friends of the housewives" in first_lower:
            continue

        role_by_season: dict[int, str] = {}
        season_idx = 0
        for cell in cells[1:]:
            span_raw = _normalize_text(cell.get("colspan"))
            try:
                span = max(1, int(span_raw)) if span_raw else 1
            except ValueError:
                span = 1
            role = _map_wiki_role(_normalize_text(cell.get_text(" ", strip=True)))
            for _ in range(span):
                if season_idx >= len(seasons):
                    break
                season = seasons[season_idx]
                if role:
                    role_by_season[season] = role
                season_idx += 1

        if role_by_season:
            matrix[first_text] = role_by_season

    return matrix


def _is_fandom_active_cell(cell: Any) -> bool:
    classes = " ".join(str(c).strip().lower() for c in (cell.get("class") or []))
    if "table-yes" in classes:
        return True
    if "table-no" in classes:
        return False

    style = _normalize_text(cell.get("style")).lower().replace(" ", "")
    if "background-color:#decde5" in style or "background-color:#dab1da" in style:
        return True

    text = _normalize_text(cell.get_text(" ", strip=True)).lower()
    if text in {"", "-", "—", "n/a"}:
        return False
    if "tba" in text:
        return False
    if any(token in text for token in ("main", "friend", "guest")):
        return True
    return False


def _map_fandom_cell_role(value: str, *, section_default: str | None) -> str | None:
    lowered = _normalize_text(value).lower()
    if "tba" in lowered:
        return None
    if "guest" in lowered:
        return "Guest"
    if "friend" in lowered:
        return "Friend"
    if "main" in lowered or "housewife" in lowered:
        return "Housewife"
    return section_default


def parse_fandom_cast_matrix_html(html: str) -> dict[str, dict[int, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.select_one("table.fandom-table.wikitable") or soup.select_one("table.wikitable")
    if table is None:
        return {}

    rows = table.find_all("tr")
    if not rows:
        return {}

    seasons = _extract_season_headers(rows)
    if not seasons:
        return {}

    matrix: dict[str, dict[int, str]] = {}
    section_default: str | None = None

    for row in rows:
        cells = row.find_all(["th", "td"], recursive=False)
        if not cells:
            continue

        first_text = _normalize_text(cells[0].get_text(" ", strip=True)).lower()
        remaining_texts = [_normalize_text(cell.get_text(" ", strip=True)) for cell in cells[1:]]
        numeric_header_row = bool(remaining_texts) and all(
            bool(text) and re.fullmatch(r"\d{1,3}", text) for text in remaining_texts
        )
        if numeric_header_row and "friends of the housewives" in first_text:
            section_default = "Friend"
            continue
        if numeric_header_row and "housewives" in first_text:
            section_default = "Housewife"
            continue

        if len(cells) == 1:
            if "friends of the housewives" in first_text:
                section_default = "Friend"
                continue
            if "housewives" in first_text:
                section_default = "Housewife"
                continue

        name_cell = cells[0]
        name_text = _normalize_text(name_cell.get_text(" ", strip=True))
        if not name_text:
            continue
        lower_name = name_text.lower()
        if lower_name in {"housewives", "friends of the housewives", "cast member", "seasons"}:
            continue
        if re.fullmatch(r"\d+", name_text):
            continue

        role_by_season: dict[int, str] = {}
        season_idx = 0
        for cell in cells[1:]:
            span_raw = _normalize_text(cell.get("colspan"))
            try:
                span = max(1, int(span_raw)) if span_raw else 1
            except ValueError:
                span = 1

            role = _map_fandom_cell_role(cell.get_text(" ", strip=True), section_default=section_default)
            active = _is_fandom_active_cell(cell)
            for _ in range(span):
                if season_idx >= len(seasons):
                    break
                season = seasons[season_idx]
                if active and role:
                    role_by_season[season] = role
                season_idx += 1

        if role_by_season:
            matrix[name_text] = role_by_season

    return matrix


def merge_cast_matrices(
    wikipedia_primary: dict[str, dict[int, str]],
    fandom_fallback: dict[str, dict[int, str]],
) -> dict[str, dict[int, str]]:
    merged: dict[str, dict[int, str]] = {name: dict(seasons) for name, seasons in wikipedia_primary.items()}

    by_slug: dict[str, str] = {_slugify(name): name for name in merged}
    for fallback_name, fallback_seasons in fandom_fallback.items():
        existing_key = by_slug.get(_slugify(fallback_name))
        if existing_key is None:
            merged[fallback_name] = dict(fallback_seasons)
            by_slug[_slugify(fallback_name)] = fallback_name
            continue
        for season, role in fallback_seasons.items():
            merged[existing_key].setdefault(season, role)

    return merged


def infer_relationship_role(value: str | None) -> str | None:
    if not value:
        return None
    normalized = _strip_accents(_normalize_text(value))
    for pattern, role in _RELATION_PATTERNS:
        if pattern.search(normalized):
            return role
    return None


def _extract_segment_text(node: Any) -> str:
    if getattr(node, "name", None):
        return _normalize_text(node.get_text(" ", strip=True))
    return _normalize_text(str(node))


def _extract_infobox_entries(value_node: Any) -> list[dict[str, str | None]]:
    segments: list[str] = []
    current: list[str] = []

    for child in value_node.children:
        if getattr(child, "name", None) == "br":
            text = _normalize_text(" ".join(current))
            if text:
                segments.append(text)
            current = []
            continue
        text = _extract_segment_text(child)
        if text:
            current.append(text)

    tail = _normalize_text(" ".join(current))
    if tail:
        segments.append(tail)

    links: dict[str, str] = {}
    for link in value_node.find_all("a", href=True):
        text = _normalize_text(link.get_text(" ", strip=True))
        if text and text not in links:
            links[text] = str(link["href"])

    out: list[dict[str, str | None]] = []
    for segment in segments:
        clean = segment.rstrip(",")
        if clean.startswith("(") and clean.endswith(")") and out:
            out[-1]["relation"] = clean.strip("()")
            continue

        match = re.match(r"^(.*?)(?:\s*\(([^)]*)\))?$", clean)
        if not match:
            continue
        name = _normalize_text(match.group(1))
        if not name:
            continue
        relation = _normalize_text(match.group(2)) or None
        entry: dict[str, str | None] = {
            "name": name,
            "relation": relation,
            "url": links.get(name),
        }
        out.append(entry)

    return out


def _parse_single_entry(raw: str, *, url: str | None = None) -> dict[str, str | None] | None:
    clean = _normalize_text(raw).rstrip(",")
    if not clean:
        return None
    match = re.match(r"^(.*?)(?:\s*\(([^)]*)\))?$", clean)
    if not match:
        return None
    name = _normalize_text(match.group(1))
    if not name:
        return None
    relation = _normalize_text(match.group(2)) or None
    return {"name": name, "relation": relation, "url": url}


def _extract_person_entries(value_node: Any) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    seen: set[str] = set()

    def _append(entry: dict[str, str | None] | None) -> None:
        if not entry:
            return
        name = _normalize_text(entry.get("name"))
        if not name:
            return
        key = _slugify(name)
        if key in seen:
            return
        seen.add(key)
        entries.append(
            {
                "name": name,
                "relation": _normalize_text(entry.get("relation")) or None,
                "url": entry.get("url"),
            }
        )

    for list_item in value_node.find_all("li"):
        item_entries = _extract_infobox_entries(list_item)
        if item_entries:
            for entry in item_entries:
                _append(entry)
            continue
        text = _normalize_text(list_item.get_text(" ", strip=True))
        first_link = list_item.find("a", href=True)
        _append(_parse_single_entry(text, url=str(first_link["href"]) if first_link else None))
    if entries:
        return entries

    for entry in _extract_infobox_entries(value_node):
        _append(entry)
    if entries:
        return entries

    for link in value_node.find_all("a", href=True):
        name = _normalize_text(link.get_text(" ", strip=True))
        _append({"name": name, "relation": None, "url": str(link["href"])})
    if entries:
        return entries

    text = _normalize_text(value_node.get_text(" ", strip=True))
    if text:
        for chunk in re.split(r",|;", text):
            _append(_parse_single_entry(chunk, url=None))
    return entries


def _extract_season_number(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"\b(\d{1,2})\b", text)
    if not match:
        return None
    season = int(match.group(1))
    if season <= 0 or season > 200:
        return None
    return season


def _guess_partner_name(row: Any, row_text: str, partner_idx: int | None) -> str | None:
    if partner_idx is not None:
        cells = row.find_all(["td", "th"])
        if partner_idx < len(cells):
            cell = cells[partner_idx]
            first_link = cell.find("a")
            if first_link:
                text = _normalize_text(first_link.get_text(" ", strip=True))
                if text:
                    return text
            text = _normalize_text(cell.get_text(" ", strip=True))
            if text:
                return text

    first_link = row.find("a")
    if first_link:
        text = _normalize_text(first_link.get_text(" ", strip=True))
        if text:
            return text

    tokens = [token.strip() for token in re.split(r"[–—-]", row_text) if token.strip()]
    if tokens:
        return _normalize_text(tokens[0])
    return None


def extract_relationship_data_from_fandom_html(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    article = soup.select_one("div.mw-parser-output") or soup

    kid_names: list[str] = []
    season_partner_roles: list[dict[str, Any]] = []
    global_partner_roles: list[dict[str, str]] = []
    missing_season_evidence: list[str] = []

    infobox = article.select_one("aside.portable-infobox") or article.select_one(".portable-infobox")
    if infobox:
        for item in infobox.select(".pi-item.pi-data"):
            label_node = item.select_one(".pi-data-label")
            value_node = item.select_one(".pi-data-value")
            label = _normalize_text(label_node.get_text(" ", strip=True) if label_node else item.get("data-source"))
            if not label or value_node is None:
                continue
            label_cf = _strip_accents(label.casefold())
            entries = _extract_person_entries(value_node)
            is_family = label_cf in {"family", "relatives"}
            is_relationship = "romance" in label_cf or "relationship" in label_cf
            is_children = any(token in label_cf for token in ("child", "children", "kid", "son", "daughter"))

            if is_family or is_children:
                for entry in entries:
                    relation = _normalize_text(entry.get("relation"))
                    name = _normalize_text(entry.get("name"))
                    if not name:
                        continue
                    if relation and _KID_RE.search(relation):
                        kid_names.append(name)
                        continue
                    inferred = infer_relationship_role(relation or label_cf)
                    if inferred:
                        global_partner_roles.append({"name": name, "role": inferred})
                        missing_season_evidence.append(f"{name} ({inferred})")
                continue

            if is_relationship:
                for entry in entries:
                    name = _normalize_text(entry.get("name"))
                    relation = _normalize_text(entry.get("relation"))
                    role = infer_relationship_role(relation or label_cf or name)
                    if role and name:
                        global_partner_roles.append({"name": name, "role": role})
                        missing_season_evidence.append(f"{name} ({role})")

    for table in article.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [
            _strip_accents(_normalize_text(cell.get_text(" ", strip=True)).lower())
            for cell in rows[0].find_all(["th", "td"])
        ]
        if not headers:
            continue
        if not any("season" in header for header in headers):
            continue
        relationship_tokens = (
            "relationship",
            "status",
            "partner",
            "spouse",
            "romance",
            "boyfriend",
            "husband",
            "fiance",
        )
        if not any(any(token in header for token in relationship_tokens) for header in headers):
            continue

        season_idx = next((idx for idx, header in enumerate(headers) if "season" in header), None)
        partner_idx = next(
            (
                idx
                for idx, header in enumerate(headers)
                if any(
                    token in header
                    for token in ("partner", "spouse", "name", "romance", "boyfriend", "husband", "fiance")
                )
            ),
            None,
        )
        status_idx = next(
            (idx for idx, header in enumerate(headers) if any(token in header for token in ("status", "relationship"))),
            None,
        )

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells or season_idx is None or season_idx >= len(cells):
                continue
            season = _extract_season_number(cells[season_idx].get_text(" ", strip=True))
            if season is None:
                continue

            row_text = _normalize_text(row.get_text(" ", strip=True))
            status_text = None
            if status_idx is not None and status_idx < len(cells):
                status_text = _normalize_text(cells[status_idx].get_text(" ", strip=True))
            role = infer_relationship_role(status_text or row_text)
            if role is None:
                continue

            partner_name = _guess_partner_name(row, row_text, partner_idx)
            if not partner_name:
                missing_season_evidence.append(f"Season {season}: {role}")
                continue

            season_partner_roles.append(
                {
                    "season": season,
                    "name": partner_name,
                    "role": role,
                }
            )

    unique_kids: list[str] = []
    seen_kids: set[str] = set()
    for kid in kid_names:
        cleaned = _normalize_text(kid)
        if not cleaned:
            continue
        key = _slugify(cleaned)
        if key in seen_kids:
            continue
        seen_kids.add(key)
        unique_kids.append(cleaned)

    unique_missing: list[str] = []
    seen_missing: set[str] = set()
    for item in missing_season_evidence:
        cleaned = _normalize_text(item)
        if not cleaned or cleaned in seen_missing:
            continue
        seen_missing.add(cleaned)
        unique_missing.append(cleaned)

    deduped_relationships: list[dict[str, Any]] = []
    seen_relationships: set[tuple[int, str, str]] = set()
    for row in season_partner_roles:
        season = int(row.get("season") or 0)
        name = _normalize_text(str(row.get("name") or ""))
        role = _normalize_text(str(row.get("role") or ""))
        if season <= 0 or not name or not role:
            continue
        key = (season, _slugify(name), role)
        if key in seen_relationships:
            continue
        seen_relationships.add(key)
        deduped_relationships.append({"season": season, "name": name, "role": role})

    deduped_global_relationships: list[dict[str, str]] = []
    seen_global_relationships: set[tuple[str, str]] = set()
    for row in global_partner_roles:
        name = _normalize_text(str(row.get("name") or ""))
        role = _normalize_text(str(row.get("role") or ""))
        if not name or not role:
            continue
        key = (_slugify(name), role)
        if key in seen_global_relationships:
            continue
        seen_global_relationships.add(key)
        deduped_global_relationships.append({"name": name, "role": role})

    return {
        "season_partner_roles": deduped_relationships,
        "global_partner_roles": deduped_global_relationships,
        "kid_names": unique_kids,
        "missing_season_evidence": unique_missing,
    }


def extract_relationship_data_from_wikipedia_html(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    infobox = soup.select_one("table.infobox.biography.vcard") or soup.select_one("table.infobox")

    kid_names: list[str] = []
    global_partner_roles: list[dict[str, str]] = []
    missing_season_evidence: list[str] = []
    season_partner_roles: list[dict[str, Any]] = []

    if infobox:
        for row in infobox.select("tr"):
            label_node = row.find("th")
            value_node = row.find("td")
            if label_node is None or value_node is None:
                continue
            label = _normalize_text(label_node.get_text(" ", strip=True))
            if not label:
                continue
            label_cf = _strip_accents(label.casefold())
            entries = _extract_person_entries(value_node)
            if not entries:
                continue

            is_relationship = any(
                token in label_cf for token in ("spouse", "partner", "romance", "relationship", "significant other")
            )
            is_family = any(token in label_cf for token in ("family", "relative", "children", "child", "kids"))

            if not is_relationship and not is_family:
                continue

            for entry in entries:
                name = _normalize_text(entry.get("name"))
                relation = _normalize_text(entry.get("relation"))
                if not name:
                    continue
                relation_context = " ".join(part for part in (relation, label_cf) if part).strip()
                if is_family and relation_context and _KID_RE.search(relation_context):
                    kid_names.append(name)
                    continue
                role = infer_relationship_role(relation_context)
                if role:
                    global_partner_roles.append({"name": name, "role": role})
                    missing_season_evidence.append(f"{name} ({role})")

    unique_kids: list[str] = []
    seen_kids: set[str] = set()
    for kid in kid_names:
        cleaned = _normalize_text(kid)
        if not cleaned:
            continue
        key = _slugify(cleaned)
        if key in seen_kids:
            continue
        seen_kids.add(key)
        unique_kids.append(cleaned)

    deduped_global_relationships: list[dict[str, str]] = []
    seen_global_relationships: set[tuple[str, str]] = set()
    for row in global_partner_roles:
        name = _normalize_text(str(row.get("name") or ""))
        role = _normalize_text(str(row.get("role") or ""))
        if not name or not role:
            continue
        key = (_slugify(name), role)
        if key in seen_global_relationships:
            continue
        seen_global_relationships.add(key)
        deduped_global_relationships.append({"name": name, "role": role})

    unique_missing: list[str] = []
    seen_missing: set[str] = set()
    for item in missing_season_evidence:
        cleaned = _normalize_text(item)
        if not cleaned or cleaned in seen_missing:
            continue
        seen_missing.add(cleaned)
        unique_missing.append(cleaned)

    return {
        "season_partner_roles": season_partner_roles,
        "global_partner_roles": deduped_global_relationships,
        "kid_names": unique_kids,
        "missing_season_evidence": unique_missing,
    }


def try_fetch_html(url: str) -> tuple[str | None, str | None, str | None]:
    try:
        html, final_url = fetch_html(url)
        return html, final_url, None
    except _FETCH_ERRORS as exc:
        return None, None, str(exc)

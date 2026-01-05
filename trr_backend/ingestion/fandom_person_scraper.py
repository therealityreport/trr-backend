from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any, Mapping
from urllib.parse import quote, unquote, urlparse, urlunparse

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

_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")


def fetch_fandom_person_html(
    url: str,
    *,
    session: requests.Session | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    merged = {**_DEFAULT_HEADERS, **(headers or {})}

    def _fetch_api_html(page_url: str) -> tuple[str, str]:
        parsed = urlparse(page_url)
        page = parsed.path.split("/wiki/")[-1] if "/wiki/" in parsed.path else parsed.path.rsplit("/", 1)[-1]
        page = unquote(page)
        api_url = f"{parsed.scheme}://{parsed.netloc}/api.php?action=parse&page={quote(page)}&prop=text|revid&format=json"
        api_req = urllib.request.Request(api_url, headers=merged)
        with urllib.request.urlopen(api_req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        html = data.get("parse", {}).get("text", {}).get("*", "")
        revid = data.get("parse", {}).get("revid")
        if revid:
            html = f"<!-- fandom_revid:{revid} -->{html}"
        return html, page_url

    def _is_client_challenge(html: str) -> bool:
        text = html or ""
        return "Client Challenge" in text and "loading-error" in text

    if requests is not None:
        requester = session or requests
        resp = requester.get(url, headers=merged, timeout=(5, 30), allow_redirects=True)
        resp.raise_for_status()
        html = resp.text or ""
        final_url = str(resp.url)
        if _is_client_challenge(html):
            return _fetch_api_html(final_url)
        return html, final_url

    request = urllib.request.Request(url, headers=merged)
    with urllib.request.urlopen(request, timeout=30) as resp:
        data = resp.read() or b""
        charset = resp.headers.get_content_charset() or "utf-8"
        html = data.decode(charset, errors="replace")
        final_url = str(resp.geturl())
        if _is_client_challenge(html):
            return _fetch_api_html(final_url)
        return html, final_url


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _parse_revision_id(html: str) -> int | None:
    patterns = [
        r"fandom_revid:(\d+)",
        r"wgRevisionId\"\s*:\s*(\d+)",
        r"wgRevisionId\s*=\s*(\d+)",
        r"\"revisionId\"\s*:\s*(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _extract_page_title(soup: BeautifulSoup) -> str | None:
    for selector in ("h1.page-header__title", "h1#firstHeading", "h1"):
        node = soup.select_one(selector)
        if node:
            text = _normalize_text(node.get_text(" ", strip=True))
            if text:
                return text
    title = soup.title.string if soup.title else None
    return _normalize_text(title)


def _parse_birthdate(value: str | None) -> date | None:
    if not value:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    if match:
        try:
            return date.fromisoformat(match.group(1))
        except ValueError:
            return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _find_article_root(soup: BeautifulSoup) -> BeautifulSoup:
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


def _extract_summary(article_root: BeautifulSoup, soup: BeautifulSoup) -> str | None:
    candidates: list[str] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            for key in ("abstract", "description"):
                text = _normalize_text(payload.get(key))
                if text:
                    candidates.append(text)
        elif isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                for key in ("abstract", "description"):
                    text = _normalize_text(item.get(key))
                    if text:
                        candidates.append(text)

    first_paragraph = article_root.find("p")
    if first_paragraph:
        text = _normalize_text(first_paragraph.get_text(" ", strip=True))
        if text:
            candidates.append(text)

    if not candidates:
        return None
    return max(candidates, key=len)


def _extract_link_entries(container: BeautifulSoup) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for link in container.find_all("a", href=True):
        name = _normalize_text(link.get_text(" ", strip=True))
        if not name:
            continue
        entries.append({"name": name, "url": str(link["href"])})
    if entries:
        return entries

    text = _normalize_text(container.get_text(" ", strip=True))
    if text:
        return [{"name": text}]
    return []


def _extract_list_values(container: BeautifulSoup) -> list[str]:
    list_items = container.find_all("li")
    if list_items:
        items = [_normalize_text(li.get_text(" ", strip=True)) for li in list_items]
        items = [item for item in items if item]
        if items:
            return items

    items = [
        _normalize_text(link.get_text(" ", strip=True))
        for link in container.find_all("a")
    ]
    items = [item for item in items if item]
    if items:
        return items

    text = _normalize_text(container.get_text(" ", strip=True))
    if not text:
        return []
    parts = [part.strip() for part in re.split(r",|/|\n", text) if part.strip()]
    return parts or [text]


def _canonicalize_image_url(url: str | None) -> str | None:
    if not url:
        return None
    trimmed = url.strip()
    if trimmed.startswith("//"):
        trimmed = f"https:{trimmed}"
    parsed = urlparse(trimmed)
    path = re.sub(r"/scale-to-width-down/\d+", "", parsed.path)
    cleaned = parsed._replace(path=path, query="", fragment="")
    return urlunparse(cleaned)


def _strip_scale_to_width(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    path = re.sub(r"/scale-to-width-down/\d+", "", parsed.path)
    cleaned = parsed._replace(path=path)
    return urlunparse(cleaned)


def _encode_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    path = quote(unquote(parsed.path), safe="/")
    query = quote(unquote(parsed.query), safe="=&")
    cleaned = parsed._replace(path=path, query=query)
    return urlunparse(cleaned)


def _image_file_name(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path
    name = path.rsplit("/", 1)[-1]
    return name or None


def _looks_like_image_url(url: str | None) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return any(lowered.endswith(ext) for ext in _IMAGE_EXTENSIONS) or "/images/" in lowered


def _parse_data_attrs(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    unescaped = html_lib.unescape(raw)
    try:
        data = json.loads(unescaped)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _extract_image_entry(
    container: BeautifulSoup,
    *,
    source_page_url: str,
    context_section: str,
    context_type: str,
    season: int | None,
    position: int | None,
) -> dict[str, Any] | None:
    figure = container if container.name == "figure" else container.find("figure")
    data_attrs = _parse_data_attrs(figure.get("data-attrs") if figure else None)

    image_url = None
    if data_attrs.get("url"):
        image_url = str(data_attrs["url"])

    anchor = container.find("a", href=True)
    if not anchor and container.name == "img":
        parent = container.parent
        if parent and parent.name == "a" and parent.get("href"):
            anchor = parent
    if anchor and _looks_like_image_url(anchor["href"]):
        link = str(anchor["href"])
        if not image_url:
            image_url = link
        elif "scale-to-width-down" in image_url and "scale-to-width-down" not in link:
            image_url = link

    img = container.find("img") if container.name != "img" else container
    thumb_url = None
    alt_text = None
    width = None
    height = None
    if img:
        thumb_url = img.get("data-src") or img.get("src")
        alt_text = img.get("alt")
        try:
            width = int(img.get("width")) if img.get("width") else None
        except ValueError:
            width = None
        try:
            height = int(img.get("height")) if img.get("height") else None
        except ValueError:
            height = None

    if data_attrs.get("width"):
        try:
            width = int(data_attrs.get("width"))
        except (TypeError, ValueError):
            pass
    if data_attrs.get("height"):
        try:
            height = int(data_attrs.get("height"))
        except (TypeError, ValueError):
            pass

    if data_attrs.get("alt"):
        alt_text = data_attrs.get("alt")

    if not image_url:
        image_url = thumb_url

    image_url = _normalize_text(image_url)
    if not image_url:
        return None
    if "scale-to-width-down" in image_url:
        expanded = _strip_scale_to_width(image_url)
        if expanded:
            image_url = expanded

    image_url = _encode_url(image_url)
    image_url_canonical = _canonicalize_image_url(image_url)
    image_url_canonical = _encode_url(image_url_canonical)
    thumb_url = _normalize_text(thumb_url)
    if thumb_url and not thumb_url.startswith("http"):
        thumb_url = f"https:{thumb_url}" if thumb_url.startswith("//") else thumb_url
    if image_url and image_url.startswith("//"):
        image_url = f"https:{image_url}"

    file_name = data_attrs.get("fileName") or _image_file_name(image_url_canonical or image_url)
    alt_text = _normalize_text(alt_text)

    url_path = None
    try:
        url_path = urlparse(image_url).path
    except Exception:
        url_path = None

    return {
        "source": "fandom",
        "source_page_url": source_page_url,
        "url": image_url,
        "url_path": url_path,
        "image_url": image_url,
        "thumb_url": thumb_url,
        "image_url_canonical": image_url_canonical,
        "file_name": file_name,
        "alt_text": alt_text,
        "width": width,
        "height": height,
        "context_section": context_section,
        "context_type": context_type,
        "season": season,
        "position": position,
    }


def _collect_section_images(
    section: BeautifulSoup,
    *,
    source_page_url: str,
    context_section: str,
    context_type: str,
    season: int | None,
    start_position: int,
    seen: set[str],
) -> list[dict[str, Any]]:
    photos: list[dict[str, Any]] = []
    position = start_position

    for node in section.find_all(["figure", "img"]):
        if node.name == "img" and node.find_parent("figure") is not None:
            continue
        entry = _extract_image_entry(
            node,
            source_page_url=source_page_url,
            context_section=context_section,
            context_type=context_type,
            season=season,
            position=position,
        )
        if not entry:
            continue
        canonical = entry.get("image_url_canonical")
        if canonical and canonical in seen:
            continue
        if canonical:
            seen.add(canonical)
        photos.append(entry)
        position += 1

    return photos


def _parse_infobox(article_root: BeautifulSoup, source_page_url: str) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    infobox = article_root.select_one("aside.portable-infobox") or article_root.select_one(".portable-infobox")
    if not infobox:
        return {}, {}, []

    infobox_raw: dict[str, Any] = {}
    fields: dict[str, Any] = {}
    photos: list[dict[str, Any]] = []
    seen: set[str] = set()

    label_map = {
        "full name": "full_name",
        "birthdate": "birthdate_display",
        "born": "birthdate_display",
        "gender": "gender",
        "resides": "resides_in",
        "resides in": "resides_in",
        "hair color": "hair_color",
        "eye color": "eye_color",
        "height": "height_display",
        "weight": "weight_display",
        "romances": "romances",
        "family": "family",
        "friends": "friends",
        "enemies": "enemies",
        "installment": "installment",
        "main seasons": "main_seasons_display",
    }

    for item in infobox.select(".pi-item.pi-data"):
        label_node = item.select_one(".pi-data-label")
        value_node = item.select_one(".pi-data-value")
        label = _normalize_text(label_node.get_text(" ", strip=True) if label_node else item.get("data-source"))
        if not label or not value_node:
            continue
        value_text = _normalize_text(value_node.get_text(" ", strip=True))
        infobox_raw[label] = value_text

        key = label_map.get(label.casefold())
        if not key:
            continue
        if key == "romances":
            fields[key] = _extract_list_values(value_node) or None
            continue
        if key in {"family", "friends", "enemies"}:
            entries = _extract_link_entries(value_node)
            fields[key] = entries or None
            continue

        if key == "installment":
            fields["installment"] = value_text
            link = value_node.find("a", href=True)
            if link:
                fields["installment_url"] = str(link["href"])
            continue

        fields[key] = value_text

    birthdate_display = fields.get("birthdate_display")
    if birthdate_display:
        parsed_birthdate = _parse_birthdate(birthdate_display)
        if parsed_birthdate:
            fields["birthdate"] = parsed_birthdate.isoformat()

    photo_entries = _collect_section_images(
        infobox,
        source_page_url=source_page_url,
        context_section="infobox",
        context_type="hero",
        season=None,
        start_position=1,
        seen=seen,
    )
    photos.extend(photo_entries)

    return fields, infobox_raw, photos


def _find_heading(article_root: BeautifulSoup, label: str) -> BeautifulSoup | None:
    label_cf = label.casefold()
    for tag in article_root.find_all(["h2", "h3", "h4"]):
        text = _normalize_text(tag.get_text(" ", strip=True))
        if text and label_cf in text.casefold():
            return tag
    return None


def _extract_season_number(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"\b(\d{1,2})\b", text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _parse_taglines(
    article_root: BeautifulSoup,
    *,
    source_page_url: str,
    seen: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    taglines: list[dict[str, Any]] = []
    photos: list[dict[str, Any]] = []

    heading = _find_heading(article_root, "Taglines")
    if not heading:
        return taglines, photos

    table = heading.find_next("table")
    if not table:
        return taglines, photos

    rows = table.find_all("tr")
    if not rows:
        return taglines, photos

    headers = [
        _normalize_text(th.get_text(" ", strip=True)) or ""
        for th in rows[0].find_all(["th", "td"])
    ]

    for idx, row in enumerate(rows[1:], start=1):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [_normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
        data = dict(zip(headers, values))

        season = _extract_season_number(data.get("Season") or values[0] if values else None)
        opening_order = data.get("Opening Order") or data.get("Order")
        tagline_text = data.get("Tagline") or (values[2] if len(values) > 2 else None)
        taglines.append(
            {
                "season": season,
                "opening_order": opening_order,
                "tagline": tagline_text,
            }
        )

        photo_entries = _collect_section_images(
            row,
            source_page_url=source_page_url,
            context_section="taglines",
            context_type="intro_card",
            season=season,
            start_position=idx,
            seen=seen,
        )
        photos.extend(photo_entries)

    return taglines, photos


def _parse_reunion_seating(
    article_root: BeautifulSoup,
    *,
    source_page_url: str,
    seen: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seating: list[dict[str, Any]] = []
    photos: list[dict[str, Any]] = []

    heading = _find_heading(article_root, "Reunion Seating")
    if not heading:
        return seating, photos

    table = heading.find_next("table")
    if not table:
        return seating, photos

    rows = table.find_all("tr")
    if not rows:
        return seating, photos

    headers = [
        _normalize_text(th.get_text(" ", strip=True)) or ""
        for th in rows[0].find_all(["th", "td"])
    ]

    for idx, row in enumerate(rows[1:], start=1):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [_normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
        data = dict(zip(headers, values))

        season = _extract_season_number(data.get("Season") or values[0] if values else None)
        side = data.get("Side") or (values[1] if len(values) > 1 else None)
        seat_order = data.get("Seat Order") or data.get("Seat") or (values[2] if len(values) > 2 else None)

        seating.append(
            {
                "season": season,
                "side": side,
                "seat_order": seat_order,
            }
        )

        photo_entries = _collect_section_images(
            row,
            source_page_url=source_page_url,
            context_section="reunion_seating",
            context_type="reunion_look",
            season=season,
            start_position=idx,
            seen=seen,
        )
        photos.extend(photo_entries)

    return seating, photos


def _parse_trivia(article_root: BeautifulSoup) -> list[str] | None:
    heading = _find_heading(article_root, "Trivia")
    if not heading:
        return None
    list_node = heading.find_next(["ul", "ol"])
    if not list_node:
        return None
    items = [
        _normalize_text(li.get_text(" ", strip=True))
        for li in list_node.find_all("li")
    ]
    items = [item for item in items if item]
    return items or None


def _collect_article_images(
    article_root: BeautifulSoup,
    *,
    source_page_url: str,
    seen: set[str],
) -> list[dict[str, Any]]:
    photos: list[dict[str, Any]] = []
    position = 1

    for node in article_root.find_all(["figure", "img"]):
        if node.name == "img" and node.find_parent("figure") is not None:
            continue
        entry = _extract_image_entry(
            node,
            source_page_url=source_page_url,
            context_section="article",
            context_type="inline",
            season=None,
            position=position,
        )
        if not entry:
            continue
        canonical = entry.get("image_url_canonical")
        if canonical and canonical in seen:
            continue
        if canonical:
            seen.add(canonical)
        photos.append(entry)
        position += 1

    return photos


def parse_fandom_person_html(html: str, *, source_url: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    soup = BeautifulSoup(html or "", "html.parser")
    article_root = _find_article_root(soup)

    page_title = _extract_page_title(soup)
    page_revision_id = _parse_revision_id(html or "")
    summary = _extract_summary(article_root, soup)

    infobox_fields, infobox_raw, infobox_photos = _parse_infobox(article_root, source_url)

    seen: set[str] = set()
    photos: list[dict[str, Any]] = []

    for entry in infobox_photos:
        canonical = entry.get("image_url_canonical")
        if canonical:
            seen.add(canonical)
        photos.append(entry)

    taglines, taglines_photos = _parse_taglines(article_root, source_page_url=source_url, seen=seen)
    photos.extend(taglines_photos)

    reunion_seating, reunion_photos = _parse_reunion_seating(article_root, source_page_url=source_url, seen=seen)
    photos.extend(reunion_photos)

    photos.extend(_collect_article_images(article_root, source_page_url=source_url, seen=seen))

    trivia = _parse_trivia(article_root)

    cast_fandom = {
        "source": "fandom",
        "source_url": source_url,
        "page_title": page_title,
        "page_revision_id": page_revision_id,
        "scraped_at": datetime.utcnow().isoformat(),
        "summary": summary,
        "taglines": taglines or None,
        "reunion_seating": reunion_seating or None,
        "trivia": trivia,
        "infobox_raw": infobox_raw or None,
        "raw_html_sha256": hashlib.sha256((html or "").encode("utf-8")).hexdigest() if html else None,
    }
    cast_fandom.update({k: v for k, v in infobox_fields.items() if v is not None})

    return cast_fandom, photos

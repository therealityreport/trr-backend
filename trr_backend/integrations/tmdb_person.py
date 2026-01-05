"""TMDb Person API integration for fetching person details and external IDs."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import requests

from trr_backend.utils.env import load_env


def _get_bearer_token() -> str:
    """Get TMDb bearer token from environment."""
    load_env()
    token = (os.getenv("TMDB_BEARER") or "").strip()
    if not token:
        raise RuntimeError("TMDB_BEARER environment variable is required")
    return token


def _get_session() -> requests.Session:
    """Create a requests session with TMDb auth headers."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {_get_bearer_token()}",
        "accept": "application/json",
    })
    return session


@dataclass
class TMDbPersonDetails:
    """TMDb person details from /3/person/{id}."""
    tmdb_id: int
    name: str | None = None
    also_known_as: list[str] = field(default_factory=list)
    biography: str | None = None
    birthday: date | None = None
    deathday: date | None = None
    gender: int = 0  # 0=not set, 1=female, 2=male, 3=non-binary
    adult: bool = True
    homepage: str | None = None
    known_for_department: str | None = None
    place_of_birth: str | None = None
    popularity: float = 0.0
    profile_path: str | None = None


@dataclass
class TMDbExternalIds:
    """TMDb external IDs from /3/person/{id}/external_ids."""
    tmdb_id: int
    imdb_id: str | None = None
    freebase_mid: str | None = None
    freebase_id: str | None = None
    tvrage_id: int | None = None
    wikidata_id: str | None = None
    facebook_id: str | None = None
    instagram_id: str | None = None
    tiktok_id: str | None = None
    twitter_id: str | None = None
    youtube_id: str | None = None


@dataclass
class TMDbPersonFull:
    """Combined TMDb person details and external IDs."""
    details: TMDbPersonDetails
    external_ids: TMDbExternalIds
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_cast_tmdb_row(self, person_id: str) -> dict[str, Any]:
        """Convert to a row for the cast_tmdb table."""
        return {
            "person_id": person_id,
            "tmdb_id": self.details.tmdb_id,
            "name": self.details.name,
            "also_known_as": self.details.also_known_as or [],
            "biography": self.details.biography,
            "birthday": self.details.birthday.isoformat() if self.details.birthday else None,
            "deathday": self.details.deathday.isoformat() if self.details.deathday else None,
            "gender": self.details.gender,
            "adult": self.details.adult,
            "homepage": self.details.homepage,
            "known_for_department": self.details.known_for_department,
            "place_of_birth": self.details.place_of_birth,
            "popularity": self.details.popularity,
            "profile_path": self.details.profile_path,
            "imdb_id": self.external_ids.imdb_id,
            "freebase_mid": self.external_ids.freebase_mid,
            "freebase_id": self.external_ids.freebase_id,
            "tvrage_id": self.external_ids.tvrage_id,
            "wikidata_id": self.external_ids.wikidata_id,
            "facebook_id": self.external_ids.facebook_id,
            "instagram_id": self.external_ids.instagram_id,
            "tiktok_id": self.external_ids.tiktok_id,
            "twitter_id": self.external_ids.twitter_id,
            "youtube_id": self.external_ids.youtube_id,
            "fetched_at": self.fetched_at.isoformat(),
        }


def _parse_date(value: str | None) -> date | None:
    """Parse a date string (YYYY-MM-DD) to a date object."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def fetch_tmdb_person_details(
    tmdb_id: int,
    *,
    session: requests.Session | None = None,
    retries: int = 3,
    backoff: float = 1.0,
) -> TMDbPersonDetails | None:
    """
    Fetch person details from TMDb API.

    Args:
        tmdb_id: TMDb person ID
        session: Optional requests session (will create one if not provided)
        retries: Number of retries for transient errors
        backoff: Base backoff time in seconds

    Returns:
        TMDbPersonDetails or None if not found
    """
    session = session or _get_session()
    url = f"https://api.themoviedb.org/3/person/{tmdb_id}"

    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)

            if resp.status_code == 404:
                return None

            if resp.status_code == 429:
                # Rate limited - wait and retry
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                # Server error - retry
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            return TMDbPersonDetails(
                tmdb_id=data.get("id", tmdb_id),
                name=data.get("name"),
                also_known_as=data.get("also_known_as") or [],
                biography=data.get("biography"),
                birthday=_parse_date(data.get("birthday")),
                deathday=_parse_date(data.get("deathday")),
                gender=data.get("gender", 0),
                adult=data.get("adult", True),
                homepage=data.get("homepage"),
                known_for_department=data.get("known_for_department"),
                place_of_birth=data.get("place_of_birth"),
                popularity=data.get("popularity", 0.0),
                profile_path=data.get("profile_path"),
            )

        except requests.RequestException:
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue
            raise

    return None


def fetch_tmdb_external_ids(
    tmdb_id: int,
    *,
    session: requests.Session | None = None,
    retries: int = 3,
    backoff: float = 1.0,
) -> TMDbExternalIds | None:
    """
    Fetch external IDs from TMDb API.

    Args:
        tmdb_id: TMDb person ID
        session: Optional requests session (will create one if not provided)
        retries: Number of retries for transient errors
        backoff: Base backoff time in seconds

    Returns:
        TMDbExternalIds or None if not found
    """
    session = session or _get_session()
    url = f"https://api.themoviedb.org/3/person/{tmdb_id}/external_ids"

    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)

            if resp.status_code == 404:
                return None

            if resp.status_code == 429:
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            return TMDbExternalIds(
                tmdb_id=data.get("id", tmdb_id),
                imdb_id=data.get("imdb_id"),
                freebase_mid=data.get("freebase_mid"),
                freebase_id=data.get("freebase_id"),
                tvrage_id=data.get("tvrage_id"),
                wikidata_id=data.get("wikidata_id"),
                facebook_id=data.get("facebook_id"),
                instagram_id=data.get("instagram_id"),
                tiktok_id=data.get("tiktok_id"),
                twitter_id=data.get("twitter_id"),
                youtube_id=data.get("youtube_id"),
            )

        except requests.RequestException:
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue
            raise

    return None


def fetch_tmdb_person_full(
    tmdb_id: int,
    *,
    session: requests.Session | None = None,
    retries: int = 3,
    backoff: float = 1.0,
    delay_between_requests: float = 0.25,
) -> TMDbPersonFull | None:
    """
    Fetch full person data (details + external IDs) from TMDb API.

    Args:
        tmdb_id: TMDb person ID
        session: Optional requests session
        retries: Number of retries for transient errors
        backoff: Base backoff time in seconds
        delay_between_requests: Delay between the two API calls

    Returns:
        TMDbPersonFull or None if not found
    """
    session = session or _get_session()

    details = fetch_tmdb_person_details(
        tmdb_id, session=session, retries=retries, backoff=backoff
    )
    if not details:
        return None

    time.sleep(delay_between_requests)

    external_ids = fetch_tmdb_external_ids(
        tmdb_id, session=session, retries=retries, backoff=backoff
    )
    if not external_ids:
        # Create empty external IDs if fetch failed
        external_ids = TMDbExternalIds(tmdb_id=tmdb_id)

    return TMDbPersonFull(
        details=details,
        external_ids=external_ids,
        fetched_at=datetime.now(timezone.utc),
    )

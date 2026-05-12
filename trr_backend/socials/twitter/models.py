"""Twitter/X platform-local DTO helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TwitterPersistenceSummary:
    """Route-visible summary for standalone Twitter search persistence."""

    requested: bool
    succeeded: bool
    scrape_query_label: str
    scrape_run_id: str | None
    tweets_upserted: int
    tweet_memberships_created: int
    tweet_memberships_total: int
    requested_via: str
    error: str | None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def failed(
        cls,
        *,
        scrape_query_label: str,
        tweet_memberships_total: int,
        error: Exception | str,
        requested_via: str = "api",
    ) -> TwitterPersistenceSummary:
        return cls(
            requested=True,
            succeeded=False,
            scrape_query_label=scrape_query_label,
            scrape_run_id=None,
            tweets_upserted=0,
            tweet_memberships_created=0,
            tweet_memberships_total=max(0, int(tweet_memberships_total)),
            requested_via=requested_via,
            error=str(error),
        )

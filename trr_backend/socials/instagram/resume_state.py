from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

_MAX_SEEN_CURSORS = 128
_DEFAULT_TTL = timedelta(hours=6)


@dataclass
class InstagramResumeState:
    next_cursor: str | None = None
    pages_scanned: int = 0
    posts_checked: int = 0
    seen_cursors: list[str] = field(default_factory=list)
    best_before: datetime | None = None
    last_transport: str | None = None

    def normalized_seen_cursors(self) -> list[str]:
        values = [str(item).strip() for item in self.seen_cursors if str(item).strip()]
        if len(values) > _MAX_SEEN_CURSORS:
            values = values[-_MAX_SEEN_CURSORS:]
        return values

    def is_fresh(self, *, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return self.best_before is None or self.best_before > current

    def to_metadata(self) -> dict[str, Any]:
        return {
            "next_cursor": self.next_cursor,
            "pages_scanned": max(0, int(self.pages_scanned)),
            "posts_checked": max(0, int(self.posts_checked)),
            "seen_cursors": self.normalized_seen_cursors(),
            "best_before": (self.best_before or (datetime.now(UTC) + _DEFAULT_TTL)).isoformat(),
            "last_transport": self.last_transport,
        }

    @classmethod
    def from_metadata(cls, payload: dict[str, object] | None) -> InstagramResumeState | None:
        if not isinstance(payload, dict):
            return None
        best_before_raw = str(payload.get("best_before") or "").strip()
        best_before = datetime.fromisoformat(best_before_raw) if best_before_raw else None
        state = cls(
            next_cursor=str(payload.get("next_cursor") or "").strip() or None,
            pages_scanned=max(0, int(payload.get("pages_scanned") or 0)),
            posts_checked=max(0, int(payload.get("posts_checked") or 0)),
            seen_cursors=[str(item) for item in payload.get("seen_cursors") or []],
            best_before=best_before,
            last_transport=str(payload.get("last_transport") or "").strip() or None,
        )
        return state if state.is_fresh() else None

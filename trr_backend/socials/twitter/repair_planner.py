"""Coverage-weighted repair planning for X/Twitter interaction backfills."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

InteractionKind = Literal["reply", "quote"]

LOW_YIELD_UNIQUE_THRESHOLD = 3
LOW_YIELD_DUPLICATE_THRESHOLD = 50
EXHAUSTED_STATUSES = frozenset({"exhausted", "auth_blocked"})


@dataclass(frozen=True)
class TwitterCatalogCoverage:
    source_id: str
    posted_at: datetime | str | None = None
    replies_count: int = 0
    quotes_count: int = 0


@dataclass(frozen=True)
class TwitterInteractionState:
    root_source_id: str
    interaction_kind: InteractionKind
    strategy: str = "default"
    status: str = "pending"
    reported_count: int = 0
    saved_count_before: int = 0
    saved_count_after: int = 0
    unique_saved_delta: int = 0
    duplicate_count: int = 0
    off_root_count: int = 0
    pages_scanned: int = 0
    exhaustion_reason: str | None = None
    last_error_code: str | None = None


@dataclass(frozen=True)
class TwitterRepairCandidate:
    root_source_id: str
    interaction_kind: InteractionKind
    reported_count: int
    saved_count: int
    raw_missing: int
    actionable_missing: int
    exhausted_missing: int
    expected_unique_gain: int
    priority: int
    strategy: str = "default"
    suppressed: bool = False
    suppression_reason: str | None = None
    prior_status: str | None = None
    prior_unique_saved_delta: int = 0
    prior_duplicate_count: int = 0
    prior_off_root_count: int = 0

    def to_job_config(self, *, account: str, base_config: Mapping[str, Any] | None = None) -> dict[str, Any]:
        config = dict(base_config or {})
        config.update(
            {
                "account": account,
                "stage": "shared_account_posts",
                "platform": "twitter",
                "twitter_comments_in_posts_stage": True,
                "comment_anchor_source_ids": {"twitter": [self.root_source_id]},
                "interaction_kind": self.interaction_kind,
                "repair_strategy": self.strategy,
                "planner_mode": "twitter_root_repair",
                "completion_target_posts": 1,
            }
        )
        return config


def _as_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _catalog_from_row(row: TwitterCatalogCoverage | Mapping[str, Any]) -> TwitterCatalogCoverage:
    if isinstance(row, TwitterCatalogCoverage):
        return row
    source_id = _as_text(row.get("source_id") or row.get("tweet_id") or row.get("root_source_id"))
    return TwitterCatalogCoverage(
        source_id=source_id,
        posted_at=row.get("posted_at"),
        replies_count=_as_int(row.get("replies_count") if "replies_count" in row else row.get("replies")),
        quotes_count=_as_int(row.get("quotes_count") if "quotes_count" in row else row.get("quotes")),
    )


def _state_from_row(row: TwitterInteractionState | Mapping[str, Any]) -> TwitterInteractionState | None:
    if isinstance(row, TwitterInteractionState):
        return row
    root_source_id = _as_text(row.get("root_source_id") or row.get("source_id"))
    raw_kind = _as_text(row.get("interaction_kind")).lower()
    if raw_kind in {"replies", "comments"}:
        raw_kind = "reply"
    if raw_kind in {"quotes", "quote_tweets"}:
        raw_kind = "quote"
    if not root_source_id or raw_kind not in {"reply", "quote"}:
        return None
    return TwitterInteractionState(
        root_source_id=root_source_id,
        interaction_kind=raw_kind,  # type: ignore[arg-type]
        strategy=_as_text(row.get("strategy")) or "default",
        status=_as_text(row.get("status")).lower() or "pending",
        reported_count=_as_int(row.get("reported_count")),
        saved_count_before=_as_int(row.get("saved_count_before")),
        saved_count_after=_as_int(row.get("saved_count_after")),
        unique_saved_delta=_as_int(row.get("unique_saved_delta")),
        duplicate_count=_as_int(row.get("duplicate_count")),
        off_root_count=_as_int(row.get("off_root_count")),
        pages_scanned=_as_int(row.get("pages_scanned")),
        exhaustion_reason=_as_text(row.get("exhaustion_reason")) or None,
        last_error_code=_as_text(row.get("last_error_code")) or None,
    )


def _latest_states_by_root_kind(
    states: Sequence[TwitterInteractionState | Mapping[str, Any]],
) -> dict[tuple[str, InteractionKind], TwitterInteractionState]:
    latest: dict[tuple[str, InteractionKind], TwitterInteractionState] = {}
    for row in states:
        state = _state_from_row(row)
        if state is None:
            continue
        latest[(state.root_source_id, state.interaction_kind)] = state
    return latest


def _low_yield_suppression_reason(state: TwitterInteractionState, raw_missing: int) -> str | None:
    if raw_missing <= 0:
        return None
    if state.status not in {"completed", "failed"}:
        return None
    if state.unique_saved_delta > LOW_YIELD_UNIQUE_THRESHOLD:
        return None
    if state.duplicate_count < LOW_YIELD_DUPLICATE_THRESHOLD:
        return None
    return "low_unique_yield"


def build_twitter_repair_plan(
    *,
    catalog_rows: Sequence[TwitterCatalogCoverage | Mapping[str, Any]],
    saved_replies_by_root: Mapping[str, int] | None = None,
    saved_quotes_by_root: Mapping[str, int] | None = None,
    interaction_states: Sequence[TwitterInteractionState | Mapping[str, Any]] | None = None,
    force: bool = False,
    include_suppressed: bool = False,
    max_candidates: int | None = None,
) -> list[TwitterRepairCandidate]:
    """Return ranked root/phase repair candidates from current coverage inputs."""

    saved_replies = saved_replies_by_root or {}
    saved_quotes = saved_quotes_by_root or {}
    states_by_root_kind = _latest_states_by_root_kind(interaction_states or [])
    candidates: list[TwitterRepairCandidate] = []
    priority_seed = 0

    for raw_row in catalog_rows:
        row = _catalog_from_row(raw_row)
        if not row.source_id:
            continue
        for kind, reported_count, saved_count in (
            ("reply", row.replies_count, _as_int(saved_replies.get(row.source_id))),
            ("quote", row.quotes_count, _as_int(saved_quotes.get(row.source_id))),
        ):
            raw_missing = max(0, reported_count - saved_count)
            if raw_missing <= 0:
                continue
            state = states_by_root_kind.get((row.source_id, kind))  # type: ignore[arg-type]
            suppressed = False
            suppression_reason: str | None = None
            exhausted_missing = 0
            actionable_missing = raw_missing
            expected_unique_gain = raw_missing
            if state is not None:
                prior_status = state.status.lower()
                if prior_status in EXHAUSTED_STATUSES and not force:
                    suppressed = True
                    suppression_reason = prior_status
                    exhausted_missing = raw_missing
                    actionable_missing = 0
                    expected_unique_gain = 0
                low_yield_reason = _low_yield_suppression_reason(state, raw_missing)
                if low_yield_reason and not force:
                    suppressed = True
                    suppression_reason = low_yield_reason
                    actionable_missing = 0
                    expected_unique_gain = max(0, state.unique_saved_delta)
                elif state.unique_saved_delta > 0:
                    expected_unique_gain = min(raw_missing, max(state.unique_saved_delta, raw_missing // 2))
            if suppressed and not include_suppressed:
                continue
            priority_seed += 1
            candidates.append(
                TwitterRepairCandidate(
                    root_source_id=row.source_id,
                    interaction_kind=kind,  # type: ignore[arg-type]
                    reported_count=reported_count,
                    saved_count=saved_count,
                    raw_missing=raw_missing,
                    actionable_missing=actionable_missing,
                    exhausted_missing=exhausted_missing,
                    expected_unique_gain=expected_unique_gain,
                    priority=priority_seed,
                    suppressed=suppressed,
                    suppression_reason=suppression_reason,
                    prior_status=state.status if state else None,
                    prior_unique_saved_delta=state.unique_saved_delta if state else 0,
                    prior_duplicate_count=state.duplicate_count if state else 0,
                    prior_off_root_count=state.off_root_count if state else 0,
                )
            )

    candidates.sort(
        key=lambda candidate: (
            candidate.suppressed,
            -candidate.expected_unique_gain,
            -candidate.actionable_missing,
            -candidate.raw_missing,
            candidate.root_source_id,
            candidate.interaction_kind,
        )
    )
    if max_candidates is not None:
        return candidates[: max(0, int(max_candidates))]
    return candidates

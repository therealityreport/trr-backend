"""Read-only discovery provenance for the exact windows shown to an operator."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from trr_backend.db import pg

MAX_WINDOWS = 64
MAX_WINDOWS_JSON = 12_000


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Timestamp must include a timezone")
        return value.astimezone(UTC)
    if not isinstance(value, str):
        raise ValueError("Window bounds must be ISO timestamps with a timezone")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Window bounds must be ISO timestamps with a timezone") from exc
    if parsed.tzinfo is None:
        raise ValueError("Window bounds must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def parse_windows(raw: str | None) -> list[dict] | None:
    if raw is None:
        return None
    if len(raw) > MAX_WINDOWS_JSON:
        raise ValueError("windows is too large")
    try:
        values = json.loads(raw)
    except ValueError as exc:
        raise ValueError("windows must be a JSON array") from exc
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_WINDOWS:
        raise ValueError("windows must contain between 1 and 64 ranges")
    seen: set[str] = set()
    result = []
    for value in values:
        if not isinstance(value, dict) or set(value) != {"key", "start", "end"}:
            raise ValueError("Each window requires key, start and end")
        key = value["key"]
        if not isinstance(key, str) or not re.fullmatch(
            r"(?:period-(?:preseason|postseason)|episode-[1-9][0-9]{0,3})", key
        ):
            raise ValueError("Invalid canonical window key")
        if key in seen:
            raise ValueError("Duplicate window key")
        seen.add(key)
        start, end = _instant(value["start"]), _instant(value["end"])
        if end <= start:
            raise ValueError("Window end must follow start")
        result.append({"key": key, "start": _iso(start), "end": _iso(end)})
    return result


def fetch_proofs(community_id: str, season_id: str) -> list[dict]:
    # Do not use run status: discovery may succeed while a later enrichment phase fails.
    # Empty windows still need positive committed discovery evidence.
    return pg.fetch_all(
        """select r.id::text as run_id, r.status, r.created_at, r.completed_at, r.updated_at,
                  m.configuration, m.coverage
           from social.reddit_refresh_runs r
           join social.reddit_run_manifests m on m.run_id=r.id
           where r.community_id=%s and r.season_id=%s and r.contract_version=2
             and m.requested_phases ? 'discovery'
             and m.coverage->>'state'='complete' and m.coverage->>'scope'='window'
             and m.coverage->>'chronological_boundary_proven'='true'
             and exists(select 1 from social.reddit_work_targets t
                        where t.run_id=r.id and t.kind='discovery' and t.required and t.state='committed')
             and not exists(select 1 from social.reddit_work_targets t
                            where t.run_id=r.id and t.kind='discovery' and t.required and t.state<>'committed')
           order by r.created_at, r.id""",
        [community_id, season_id],
    )


def qualifying_intervals(proofs: list[dict], *, now: datetime | None = None) -> list[tuple[datetime, datetime, str]]:
    now = now or datetime.now(UTC)
    intervals = []
    for proof in proofs:
        config, coverage = proof.get("configuration") or {}, proof.get("coverage") or {}
        if (
            not isinstance(config, dict)
            or not isinstance(coverage, dict)
            or config.get("selected_post_ids")
            or "discovery" not in (config.get("requested_phases") or [])
            or coverage.get("scope") != "window"
            or coverage.get("state") != "complete"
            or coverage.get("chronological_boundary_proven") is not True
        ):
            continue
        try:
            start, end = _instant(coverage.get("requested_start")), _instant(coverage.get("requested_end"))
            # Evidence and immutable request must describe the same interval.
            if start != _instant(config.get("period_start")) or end != _instant(config.get("period_end")):
                continue
        except ValueError:
            continue
        # A request extending into the future cannot prove posts not yet published.
        if proof.get("created_at") is None:
            continue
        end = min(end, _instant(proof["created_at"]))
        if start < end and end <= now:
            intervals.append((start, end, str(proof["run_id"])))
    return intervals


def classify_windows(windows: list[dict], proofs: list[dict], *, now: datetime | None = None) -> dict[str, dict]:
    now = now or datetime.now(UTC)
    intervals = qualifying_intervals(proofs, now=now)
    results = {}
    for window in windows:
        start, end = _instant(window["start"]), _instant(window["end"])
        clips = sorted((max(start, a), min(end, b), rid) for a, b, rid in intervals if a < end and b > start)
        merged: list[list[datetime]] = []
        for a, b, _ in clips:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        state = "upcoming" if start >= now else "uncovered"
        if merged:
            state = "covered" if len(merged) == 1 and merged[0] == [start, end] else "partial"
        results[window["key"]] = {
            "coverage_state": state,
            "window_start": _iso(start),
            "window_end": _iso(end),
            "covered_from": _iso(merged[0][0]) if merged else None,
            "covered_through": _iso(merged[0][1]) if merged else None,
            "covered_intervals": [{"start": _iso(a), "end": _iso(b)} for a, b in merged],
            "evidence_run_ids": sorted({rid for _, _, rid in clips}),
        }
    return results

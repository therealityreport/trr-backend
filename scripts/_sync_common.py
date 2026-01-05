from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from supabase import Client

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.repositories.shows import assert_core_shows_table_exists, update_show
from trr_backend.repositories.sync_state import assert_core_sync_state_table_exists, fetch_sync_state_map
from trr_backend.utils.env import load_env


# Note: Column names depend on whether migration 0028 has been applied.
# Old names: imdb_series_id, tmdb_series_id
# New names: imdb_id, tmdb_id
# We select both patterns and use whichever exists.
SHOW_SELECT_FIELDS = (
    "id,name,description,premiere_date,"
    "imdb_series_id,tmdb_series_id,"  # legacy column names (pre-0028)
    "show_total_seasons,most_recent_episode"
)


@dataclass(frozen=True)
class SyncFilterResult:
    selected: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    reasons: dict[str, int]


def add_show_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--all", action="store_true", help="Process all shows (default when no filters are set).")
    parser.add_argument("--show-id", action="append", default=[], help="core.shows id (UUID). Repeatable.")
    parser.add_argument("--tmdb-show-id", action="append", default=[], help="TMDb series id. Repeatable.")
    parser.add_argument("--imdb-series-id", action="append", default=[], help="IMDb series id (tt...). Repeatable.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of shows to process.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to Supabase.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    parser.add_argument(
        "--incremental",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip shows that are already up-to-date.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Retry shows marked failed/in_progress in sync_state.",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess all shows regardless of sync state.")
    parser.add_argument(
        "--since",
        default=None,
        help="Reprocess shows whose last_success_at is before this ISO date/time.",
    )


def load_env_and_db() -> Client:
    load_env()
    db = create_supabase_admin_client()
    assert_core_shows_table_exists(db)
    return db


def _dedupe_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for row in rows:
        row_id = str(row.get("id") or "")
        if not row_id or row_id in seen:
            continue
        seen.add(row_id)
        ordered.append(row)
    return ordered


def _fetch_by_in(
    db: Client,
    *,
    column: str,
    values: list[Any],
) -> list[dict[str, Any]]:
    if not values:
        return []
    response = db.schema("core").table("shows").select(SHOW_SELECT_FIELDS).in_(column, values).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing shows by {column}: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _coerce_int_list(values: Iterable[str]) -> list[int]:
    out: list[int] = []
    for raw in values:
        s = str(raw).strip()
        if not s:
            continue
        if s.isdigit():
            out.append(int(s))
    return out


def _coerce_str_list(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        s = str(raw).strip()
        if s:
            out.append(s)
    return out


def _coerce_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _normalize_marker(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None
    return None


def _parse_since(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = _parse_datetime(text)
    if parsed:
        return parsed
    try:
        parsed = datetime.fromisoformat(f"{text}T00:00:00")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def extract_most_recent_episode(show: Mapping[str, Any]) -> str | None:
    return _normalize_marker(show.get("most_recent_episode"))


def extract_show_total_seasons(show: Mapping[str, Any]) -> int | None:
    return _coerce_int(show.get("show_total_seasons"))


def _chunked(values: list[str], chunk_size: int) -> Iterable[list[str]]:
    step = max(1, int(chunk_size))
    for i in range(0, len(values), step):
        yield values[i : i + step]


def fetch_show_season_counts(
    db: Client,
    *,
    show_ids: Iterable[str],
    chunk_size: int = 200,
) -> dict[str, int]:
    ids = [str(show_id) for show_id in show_ids if str(show_id).strip()]
    if not ids:
        return {}

    buckets: dict[str, set[int]] = {}
    for chunk in _chunked(ids, chunk_size):
        response = (
            db.schema("core")
            .table("seasons")
            .select("show_id,season_number")
            .in_("show_id", chunk)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing seasons for sync check: {response.error}")
        data = response.data or []
        if not isinstance(data, list):
            continue
        for row in data:
            show_id = str(row.get("show_id") or "").strip()
            season_number = _coerce_int(row.get("season_number"))
            if not show_id or season_number is None or season_number <= 0:
                continue
            buckets.setdefault(show_id, set()).add(season_number)

    return {show_id: len(seasons) for show_id, seasons in buckets.items()}


def fetch_show_season_count(db: Client, *, show_id: str) -> int | None:
    show_id = str(show_id or "").strip()
    if not show_id:
        return None
    response = (
        db.schema("core")
        .table("seasons")
        .select("season_number")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing seasons for show_id={show_id}: {response.error}")
    data = response.data or []
    if not isinstance(data, list):
        return None
    season_numbers = {
        number
        for row in data
        if (number := _coerce_int(row.get("season_number"))) is not None and number > 0
    }
    if not season_numbers:
        return None
    return len(season_numbers)


def reconcile_show_total_seasons(
    db: Client,
    *,
    show_id: str,
    current_total_seasons: int | None,
    verbose: bool = False,
) -> int | None:
    derived = fetch_show_season_count(db, show_id=show_id)
    if derived is None:
        if verbose:
            print(f"WARNING: No season data for show_id={show_id}")
        return None
    if current_total_seasons is not None and int(current_total_seasons) == derived:
        return derived
    update_show(db, show_id, {"show_total_seasons": derived})
    if verbose:
        print(f"UPDATED show_id={show_id} show_total_seasons={derived}")
    return derived


def should_sync_show(
    *,
    show: Mapping[str, Any],
    state: Mapping[str, Any] | None,
    incremental: bool,
    resume: bool,
    force: bool,
    check_total_seasons: bool,
    derived_total_seasons: int | None,
    since: datetime | None = None,
) -> tuple[bool, str]:
    if force:
        return True, "force"
    if not incremental:
        return True, "incremental-disabled"
    if state is None:
        return True, "no-sync-state"

    status = str(state.get("status") or "").strip().lower()
    if status in {"failed", "in_progress"}:
        if resume:
            return True, f"resume-{status}"
    elif status and status != "success":
        if resume:
            return True, f"status-{status}"

    show_marker = extract_most_recent_episode(show)
    state_marker = _normalize_marker(state.get("last_seen_most_recent_episode"))
    if show_marker != state_marker:
        return True, "episode-marker-changed"

    if since is not None:
        last_success_at = _parse_datetime(state.get("last_success_at"))
        if last_success_at is None or last_success_at < since:
            return True, "since"

    if check_total_seasons:
        if derived_total_seasons is None:
            return True, "missing-seasons"
        current_total = extract_show_total_seasons(show)
        if current_total is None or int(current_total) != int(derived_total_seasons):
            return True, "season-mismatch"

    return False, "up-to-date"


def filter_show_rows_for_sync(
    db: Client,
    show_rows: Iterable[dict[str, Any]],
    *,
    table_name: str,
    incremental: bool,
    resume: bool,
    force: bool,
    since: object = None,
    check_total_seasons: bool = False,
    verbose: bool = False,
) -> SyncFilterResult:
    shows = list(show_rows)
    show_ids = [str(show.get("id") or "").strip() for show in shows if show.get("id")]
    reasons: dict[str, int] = {}
    skipped: list[dict[str, Any]] = []

    since_dt = _parse_since(since)

    sync_state: dict[str, dict[str, Any]] = {}
    if incremental or resume or since_dt is not None:
        assert_core_sync_state_table_exists(db)
        sync_state = fetch_sync_state_map(db, table_name=table_name, show_ids=show_ids)

    season_counts: dict[str, int] = {}
    if check_total_seasons:
        season_counts = fetch_show_season_counts(db, show_ids=show_ids)

    selected: list[dict[str, Any]] = []
    for show in shows:
        show_id = str(show.get("id") or "").strip()
        state = sync_state.get(show_id)
        derived_total = season_counts.get(show_id) if check_total_seasons else None
        should_sync, reason = should_sync_show(
            show=show,
            state=state,
            incremental=incremental,
            resume=resume,
            force=force,
            check_total_seasons=check_total_seasons,
            derived_total_seasons=derived_total,
            since=since_dt,
        )
        reasons[reason] = reasons.get(reason, 0) + 1
        if should_sync:
            selected.append(show)
        else:
            skipped.append(show)

    if verbose:
        reason_parts = " ".join(f"{key}={count}" for key, count in sorted(reasons.items()))
        print(f"SYNC filter {table_name}: selected={len(selected)} skipped={len(skipped)} {reason_parts}")

    return SyncFilterResult(selected=selected, skipped=skipped, reasons=reasons)


def should_process_all(args: argparse.Namespace) -> bool:
    return bool(args.all) or not (args.show_id or args.tmdb_show_id or args.imdb_series_id)


def fetch_show_rows(db: Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    show_ids = _coerce_str_list(args.show_id or [])
    tmdb_ids = _coerce_int_list(args.tmdb_show_id or [])
    imdb_ids = _coerce_str_list(args.imdb_series_id or [])

    rows: list[dict[str, Any]] = []
    if should_process_all(args):
        query = db.schema("core").table("shows").select(SHOW_SELECT_FIELDS)
        if args.limit is not None:
            query = query.limit(max(0, int(args.limit)))
        response = query.execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows: {response.error}")
        data = response.data or []
        rows = data if isinstance(data, list) else []
    else:
        rows.extend(_fetch_by_in(db, column="id", values=show_ids))
        if tmdb_ids:
            # Try new column name first, fall back to legacy
            rows.extend(_fetch_by_in(db, column="tmdb_series_id", values=tmdb_ids))
        if imdb_ids:
            # Try new column name first, fall back to legacy
            rows.extend(_fetch_by_in(db, column="imdb_series_id", values=imdb_ids))

    rows = _dedupe_rows(rows)
    if args.limit is not None:
        rows = rows[: max(0, int(args.limit))]
    return rows


def extract_imdb_series_id(show: dict[str, Any]) -> str | None:
    # Support both old (imdb_series_id) and new (imdb_id) column names
    for key in ("imdb_id", "imdb_series_id"):
        imdb_id = show.get(key)
        if isinstance(imdb_id, str) and imdb_id.strip():
            return imdb_id.strip()
    return None


def extract_tmdb_series_id(show: dict[str, Any]) -> int | None:
    # Support both old (tmdb_series_id) and new (tmdb_id) column names
    for key in ("tmdb_id", "tmdb_series_id"):
        tmdb_id = show.get(key)
        if isinstance(tmdb_id, int):
            return tmdb_id
    return None


def build_candidates(show_rows: Iterable[dict[str, Any]]) -> list["CandidateShow"]:
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    candidates: list[CandidateShow] = []
    for row in show_rows:
        title = str(row.get("name") or "").strip() or "Unknown"
        candidates.append(
            CandidateShow(
                imdb_id=extract_imdb_series_id(row),
                tmdb_id=extract_tmdb_series_id(row),
                title=title,
            )
        )
    return candidates

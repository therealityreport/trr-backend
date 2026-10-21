"""Checkpointed Reddit discovery with explicit source coverage limitations."""

from __future__ import annotations

import hashlib
from typing import Any

from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_refresh as legacy


def query_plan(payload: dict) -> list[dict]:
    subreddit = legacy._normalize_subreddit(payload["subreddit"])
    flairs = dict.fromkeys(
        payload.get("analysis_all_flairs", [])
        + payload.get("analysis_flairs", [])
        + payload.get("force_include_flairs", [])
    )
    required = [
        {
            "key": "flair:" + flair,
            "path": f"/r/{subreddit}/search",
            "params": {
                "q": 'flair:"' + flair.replace('"', '\\"') + '"',
                "restrict_sr": "on",
                "sort": "new",
                "t": "all",
            },
            "required": True,
            "search": True,
        }
        for flair in flairs
        if flair.strip()
    ]
    optional = [
        {
            "key": "term:" + term,
            "path": f"/r/{subreddit}/search",
            "params": {"q": '"' + term.replace('"', '\\"') + '"', "restrict_sr": "on", "sort": "new", "t": "all"},
            "required": False,
            "search": True,
        }
        for term in legacy._build_terms(payload.get("show_name", ""), payload.get("show_aliases", []))
    ]
    listing = [
        {
            "key": "listing:" + sort,
            "path": f"/r/{subreddit}/{sort}",
            "params": {"t": "all"},
            "required": True,
            "search": False,
        }
        for sort in (
            ["new"] if payload.get("exhaustive_window", True) else payload.get("sort_modes") or ["new", "hot", "top"]
        )
    ]
    # Exact flairs precede every optional expansion. All queries exist in the ledger even when deferred.
    return (required + optional if payload.get("search_backfill", True) else []) + listing


def match_rows(rows: list[dict], payload: dict) -> list[dict]:
    return legacy._apply_match_metadata(
        rows=rows,
        subreddit=payload["subreddit"],
        terms=legacy._build_terms(payload.get("show_name", ""), payload.get("show_aliases", [])),
        cast_terms=legacy._build_cast_terms(payload.get("cast_names", [])),
        analysis_flairs=payload.get("analysis_flairs", []),
        analysis_all_flairs=payload.get("analysis_all_flairs", []),
        force_include_flairs=payload.get("force_include_flairs", []),
        show_focused=bool(payload.get("is_show_focused")),
    )[0]


def save_page(rows: list[dict], payload: dict, execution: capture.Execution, conn: Any) -> list[dict]:
    from trr_backend.repositories import reddit_workflows

    for row in rows:
        row["subreddit"] = payload["subreddit"]
    rows = legacy._filter_by_window(
        rows,
        period_start=legacy._parse_iso(payload.get("period_start")),
        period_end=legacy._parse_iso(payload.get("period_end")),
    )
    # Tombstones are an enduring reimport guard, including during a forced capture.
    if rows:
        reddit_workflows.lock_content(post_ids=[r["reddit_post_id"] for r in rows], conn=conn)
        with legacy.pg.db_cursor(conn=conn) as cur:
            cur.execute(
                "select reddit_post_id from social.reddit_lifecycle_enrollment "
                "where reddit_post_id=any(%s::text[]) and state in ('deleted','removed')",
                [[r["reddit_post_id"] for r in rows]],
            )
            deleted = {r["reddit_post_id"] for r in cur.fetchall()}
        rows = [r for r in rows if r["reddit_post_id"] not in deleted]
        rows = reddit_workflows.scrub_tombstoned_authors(rows, conn=conn)
    legacy._upsert_posts(rows, conn=conn)
    matched = match_rows(rows, payload)
    manual = legacy.pg.fetch_all(
        """select reddit_post_id from social.reddit_period_post_matches
        where community_id=%s and season_id=%s and admin_approved is not null""",
        [payload["community_id"], payload["season_id"]],
        conn=conn,
    )
    manual_ids = {r["reddit_post_id"] for r in manual}
    automatic = [r for r in matched if r["reddit_post_id"] not in manual_ids]
    legacy._replace_period_matches(
        community_id=payload["community_id"],
        season_id=payload["season_id"],
        period_key=payload["period_key"],
        period_start=legacy._parse_iso(payload.get("period_start")),
        period_end=legacy._parse_iso(payload.get("period_end")),
        run_id=execution.run_id,
        rows=automatic,
        conn=conn,
        preserve_existing_assignments=True,
    )
    for row in matched:
        reddit_workflows.enroll_capture(post_id=row["reddit_post_id"], run_id=execution.run_id, conn=conn)
    if rows:
        reddit_workflows.invalidate_analysis(post_ids=[r["reddit_post_id"] for r in rows], conn=conn)
    return matched


def discover(payload: dict, client: Any, execution: capture.Execution) -> dict:
    selected = payload.get("selected_post_ids") or []
    plan = [] if selected else query_plan(payload)
    search_budget = int(payload.get("max_backfill_queries") or 30)
    search_count = 0
    summaries = []
    start = legacy._parse_iso(payload.get("period_start"))
    for query in plan:
        capture.ensure_target(execution.run_id, "discovery", query["key"], required=query["required"])
    for query in plan:
        execution.check()
        key = query["key"]
        target = capture.ensure_target(execution.run_id, "discovery", key, required=query["required"])
        if target["state"] in capture.SATISFIED:
            summaries.append(target["result"])
            continue
        if query["search"]:
            search_count += 1
            if search_count > search_budget:
                summaries.append(
                    {"query": key, "reason": "query_budget_exhausted", "complete": False, "required": query["required"]}
                )
                continue
        target = capture.start_target(execution.run_id, "discovery", key, execution.claim_token)
        if target is None:
            summaries.append({"query": key, "reason": "retry_pending", "complete": False})
            continue
        checkpoint = target.get("checkpoint") or {}
        committed_checkpoint = dict(checkpoint)
        seen = set(checkpoint.get("seen_cursors", []))
        after = checkpoint.get("after")
        pages = int(checkpoint.get("pages", 0))
        max_pages = (
            int(payload.get("max_backfill_pages_per_query") or 10)
            if query["search"]
            else int(payload.get("max_pages") or 100)
        )
        observed = list(checkpoint.get("observed_dates", []))
        reason = "page_budget_exhausted"
        complete = False
        try:
            while pages < max_pages:
                execution.check()
                params = {
                    **query["params"],
                    "limit": 100
                    if payload.get("exhaustive_window", True)
                    else int(payload.get("limit_per_mode") or 35),
                    "raw_json": 1,
                }
                if after:
                    params["after"] = after
                response = client.get_json(query["path"], params=params)
                data = response.get("data")
                if not isinstance(data, dict) or not isinstance(data.get("children"), list):
                    raise ValueError("Malformed Reddit listing")
                rows = legacy._parse_listing_rows(data["children"], source_sort=key)
                if any(
                    not isinstance(c, dict) or c.get("kind") != "t3" or not c.get("data", {}).get("id")
                    for c in data["children"]
                ):
                    raise ValueError("Malformed Reddit submission in listing")
                dates = [r["posted_at"] for r in rows if r.get("posted_at")]
                observed = sorted(set(observed + dates))
                observed = [observed[0], observed[-1]] if observed else []
                next_after = data.get("after")
                reached = bool(start and dates and min(legacy._parse_iso(d) for d in dates) <= start)
                repeated = bool(next_after and next_after in seen)
                pages += 1
                if next_after:
                    seen.add(next_after)
                checkpoint = {
                    "after": next_after,
                    "pages": pages,
                    "seen_cursors": sorted(seen),
                    "observed_dates": observed,
                }
                # A chronological listing reaching the requested lower boundary is positive evidence.
                # Search indexing and ranked samples cannot establish exhaustive historical coverage.
                complete = reached and not query["search"] and key == "listing:new"
                if complete:
                    reason = "boundary_reached"
                elif repeated:
                    reason = "cursor_repeated"
                elif not next_after:
                    reason = "listing_exhausted_without_boundary"
                elif not payload.get("exhaustive_window", True):
                    reason = "ranked_sample"
                summary = {
                    "query": key,
                    "complete": complete,
                    "reason": reason,
                    "oldest_observed": observed[0] if observed else None,
                    "newest_observed": observed[-1] if observed else None,
                    "boundary_reached": reached,
                    "listing_exhausted": not next_after,
                    "cursor_repeated": repeated,
                    "pages": pages,
                }
                with execution.transaction() as conn:
                    save_page(rows, payload, execution, conn)
                    # Page targets document the atomic page save; query remains recoverable at its next cursor.
                    page_key = key + ":page:" + str(pages)
                    capture.ensure_target(
                        execution.run_id, "discovery", page_key, conn=conn, required=query["required"]
                    )
                    capture.finish_target(
                        execution.run_id,
                        "discovery",
                        page_key,
                        execution.claim_token,
                        "committed",
                        result={"count": len(rows)},
                        conn=conn,
                    )
                    capture.finish_target(
                        execution.run_id,
                        "discovery",
                        key,
                        execution.claim_token,
                        "running",
                        result=summary,
                        checkpoint=checkpoint,
                        conn=conn,
                    )
                committed_checkpoint = dict(checkpoint)
                after = next_after
                if reached or repeated or not next_after or not payload.get("exhaustive_window", True):
                    break
            summary = {
                "query": key,
                "complete": complete,
                "reason": reason,
                "oldest_observed": observed[0] if observed else None,
                "newest_observed": observed[-1] if observed else None,
                "pages": pages,
            }
            # A successfully executed search is satisfied activity, with a separate explicit coverage gap.
            capture.finish_target(
                execution.run_id,
                "discovery",
                key,
                execution.claim_token,
                "committed",
                result=summary,
                checkpoint=checkpoint,
            )
            summaries.append(summary)
        except (capture.LostClaim, capture.IdentityBlockedError):
            raise
        except Exception as exc:
            capture.finish_target(
                execution.run_id,
                "discovery",
                key,
                execution.claim_token,
                "retry_wait",
                checkpoint=committed_checkpoint,
                error=str(exc),
                retry_after=getattr(exc, "retry_after_seconds", None),
            )
            summaries.append({"query": key, "complete": False, "reason": "query_error", "error": str(exc)})
    seed_urls = (
        [f"https://www.reddit.com/comments/{pid}" for pid in selected]
        if selected
        else payload.get("seed_post_urls", [])
    )
    seed_complete = True
    for url in seed_urls:
        post_id = legacy._extract_seed_post_id(url)
        key = "seed:" + (post_id or hashlib.sha256(url.encode()).hexdigest())
        target = capture.ensure_target(execution.run_id, "discovery", key)
        if target["state"] in capture.SATISFIED:
            continue
        if not capture.start_target(execution.run_id, "discovery", key, execution.claim_token):
            seed_complete = False
            continue
        try:
            if not post_id:
                raise ValueError("Invalid seed Reddit URL")
            response = client.get_json("/api/info", params={"id": "t3_" + post_id, "raw_json": 1})
            children = response.get("data", {}).get("children")
            if not isinstance(children, list) or not children:
                raise ValueError("Seed submission absent without explicit unavailability evidence")
            rows = legacy._parse_listing_rows(children, source_sort="seed_url")
            with execution.transaction() as conn:
                save_page(rows, payload, execution, conn)
                capture.finish_target(
                    execution.run_id,
                    "discovery",
                    key,
                    execution.claim_token,
                    "committed",
                    result={"count": len(rows)},
                    conn=conn,
                )
        except (capture.LostClaim, capture.IdentityBlockedError):
            raise
        except Exception as exc:
            seed_complete = False
            capture.finish_target(
                execution.run_id, "discovery", key, execution.claim_token, "retry_wait", error=str(exc)
            )
    boundary_proven = any(s.get("query") == "listing:new" and s.get("complete") for s in summaries)
    required_keys = {q["key"] for q in plan if q["required"]}
    required_unsearched = any(
        s.get("query") in required_keys
        and s.get("reason") in {"query_budget_exhausted", "query_error", "retry_pending"}
        for s in summaries
    )
    coverage = {
        "state": "complete"
        if (bool(selected) and seed_complete) or (boundary_proven and not required_unsearched)
        else "partial",
        "scope": "selected_posts" if selected else "window",
        "requested_start": payload.get("period_start"),
        "requested_end": payload.get("period_end"),
        "chronological_boundary_proven": boundary_proven,
        "queries": summaries,
        "reasons": sorted({s["reason"] for s in summaries if not s.get("complete")}),
    }
    with execution.transaction() as conn:
        capture._one(
            "update social.reddit_run_manifests set coverage=%s::jsonb where run_id=%s returning run_id",
            [capture._json(coverage), execution.run_id],
            conn=conn,
        )
    return coverage

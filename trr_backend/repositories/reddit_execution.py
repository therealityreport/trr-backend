"""Version two run execution. Every database write is fenced by the live claim."""

from __future__ import annotations

import hashlib
import html
import logging
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlsplit

import requests

from trr_backend.db import pg
from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_discovery as discovery
from trr_backend.repositories import reddit_refresh as legacy
from trr_backend.repositories import reddit_workflows as workflows

logger = logging.getLogger(__name__)


def comment_capture(post: dict, client: Any, execution: capture.Execution) -> None:
    pid = post["reddit_post_id"]
    target = capture.start_target(execution.run_id, "comments", pid, execution.claim_token)
    if target is None:
        return

    def checkpoint(value: dict) -> None:
        with execution.transaction() as conn:
            workflows.lock_content(post_ids=[pid], conn=conn)
            value = {
                **value,
                "comments": workflows.scrub_tombstoned_authors(
                    workflows.filter_comment_tombstones(value.get("comments", []), conn=conn), conn=conn
                ),
            }
            legacy._upsert_comments(value["comments"], conn=conn)
            workflows.enroll_capture(post_id=pid, run_id=execution.run_id, conn=conn)
            capture.finish_target(
                execution.run_id, "comments", pid, execution.claim_token, "running", checkpoint=value, conn=conn
            )

    try:
        result = client.get_comments(pid, checkpoint=target.get("checkpoint") or None, on_checkpoint=checkpoint)
        state = (
            "source_unavailable"
            if result.get("source_unavailable")
            else ("committed" if result.get("complete") else "retry_wait")
        )
        evidence = result.get("evidence") or {}
        with execution.transaction() as conn:
            workflows.lock_content(post_ids=[pid], conn=conn)
            result["comments"] = workflows.scrub_tombstoned_authors(
                workflows.filter_comment_tombstones(result["comments"], conn=conn), conn=conn
            )
            if evidence.get("checkpoint"):
                evidence["checkpoint"] = {**evidence["checkpoint"], "comments": result["comments"]}
            legacy._upsert_comments(result["comments"], conn=conn)
            workflows.enroll_capture(post_id=pid, run_id=execution.run_id, conn=conn)
            capture.record_capture(
                pid,
                "comments",
                state,
                execution.run_id,
                source_revision=hashlib.sha256(capture._json(result["comments"]).encode()).hexdigest(),
                evidence={
                    "reported_count": post.get("num_comments"),
                    "saved_count": len(result["comments"]),
                    **evidence,
                },
                conn=conn,
            )
            capture.finish_target(
                execution.run_id,
                "comments",
                pid,
                execution.claim_token,
                state,
                result={
                    "saved_count": len(result["comments"]),
                    "evidence": evidence,
                    "unresolved_ids": result.get("unresolved_ids", []),
                },
                checkpoint=evidence.get("checkpoint"),
                conn=conn,
            )
    except (capture.LostClaim, capture.IdentityBlockedError):
        raise
    except Exception as exc:
        with execution.transaction() as conn:
            workflows.lock_content(post_ids=[pid], conn=conn)
            saved_checkpoint = getattr(exc, "checkpoint", None)
            if saved_checkpoint:
                saved_checkpoint = {
                    **saved_checkpoint,
                    "comments": workflows.scrub_tombstoned_authors(
                        workflows.filter_comment_tombstones(saved_checkpoint.get("comments", []), conn=conn), conn=conn
                    ),
                }
            capture.record_capture(
                pid, "comments", "retry_wait", execution.run_id, evidence={"error": str(exc)}, conn=conn
            )
            capture.finish_target(
                execution.run_id,
                "comments",
                pid,
                execution.claim_token,
                "retry_wait",
                checkpoint=saved_checkpoint,
                error=str(exc),
                retry_after=getattr(exc, "retry_after_seconds", None),
                conn=conn,
            )


def media_references(post: dict, comments: list[dict]) -> list[dict]:
    refs: dict[tuple[str, str | None], dict] = {}

    def add(url: Any, kind: str, comment_id: str | None = None):
        value = legacy._sanitize_reddit_media_url(html.unescape(str(url or "")))
        if value:
            refs[value, comment_id] = {"source_url": value, "media_type": kind, "reddit_comment_id": comment_id}

    raw = post.get("raw_payload") or {}
    url = raw.get("url") or post.get("url") or ""
    if re.search(r"\.(jpg|jpeg|png|gif|webp|mp4)(\?|$)", url, re.I):
        add(url, "video" if ".mp4" in url.lower() else "image")
    if str(raw.get("thumbnail", "")).startswith("http"):
        add(raw["thumbnail"], "thumbnail")
    for item in (raw.get("preview") or {}).get("images", []):
        add((item.get("source") or {}).get("url"), "image")
    for meta in (raw.get("media_metadata") or {}).values():
        add((meta.get("s") or {}).get("u"), "image")
    for container in (raw.get("secure_media"), raw.get("media"), raw.get("preview")):
        if not isinstance(container, dict):
            continue
        for video in (container.get("reddit_video"), container.get("reddit_video_preview")):
            if isinstance(video, dict):
                add(video.get("fallback_url"), "video")
    for comment in comments:
        for text in (
            comment.get("body"),
            comment.get("body_html"),
            (comment.get("raw_payload") or {}).get("body_html"),
        ):
            for url, kind in legacy._extract_reddit_media_urls(str(text or "")):
                add(url, kind, comment["reddit_comment_id"])
    return list(refs.values())


def media_download_url(source_url: str) -> str:
    """Resolve Tenor's GIF share links without treating their HTML as media."""
    source = urlsplit(source_url)
    if source.hostname not in {"tenor.com", "www.tenor.com"}:
        return source_url
    if source.scheme != "https":
        raise RuntimeError("Tenor share URLs must use HTTPS")

    class ImageMetadata(HTMLParser):
        def __init__(self):
            super().__init__()
            self.images: list[str] = []

        def handle_starttag(self, tag, attrs):
            attributes = dict(attrs)
            if tag == "meta" and attributes.get("property") == "og:image":
                self.images.append(attributes.get("content") or "")

    def is_cdn(url: str) -> bool:
        parsed = urlsplit(url)
        return parsed.scheme == "https" and bool(re.fullmatch(r"(?:media\d*|c)\.tenor\.com", parsed.hostname or ""))

    url = source_url
    for _ in range(4):
        with requests.get(url, timeout=20, allow_redirects=False, stream=True) as response:
            if response.status_code in {301, 302, 303, 307, 308}:
                destination = urljoin(url, response.headers.get("Location") or "")
                if is_cdn(destination):
                    return destination
                parsed = urlsplit(destination)
                if parsed.scheme != "https" or parsed.hostname not in {"tenor.com", "www.tenor.com"}:
                    raise RuntimeError("Tenor share redirect is outside its media provider")
                url = destination
                continue
            response.raise_for_status()
            if response.headers.get("Content-Type", "").lower().startswith(("image/", "video/")):
                return url
            document = bytearray()
            for chunk in response.iter_content(64 * 1024):
                document.extend(chunk)
                if len(document) > 2 * 1024 * 1024:
                    raise RuntimeError("Tenor share metadata exceeds its size limit")
            parser = ImageMetadata()
            parser.feed(document.decode("utf-8", errors="replace"))
            for image in parser.images:
                if is_cdn(image):
                    return image
            raise RuntimeError("Tenor share page has no verified media URL")
    raise RuntimeError("Tenor share redirect limit exhausted")


def media_capture(post: dict, execution: capture.Execution) -> None:
    from trr_backend.media.s3_mirror import mirror_url_to_s3

    pid = post["reddit_post_id"]
    if not capture.start_target(execution.run_id, "media", pid, execution.claim_token):
        return
    comments = pg.fetch_all("select * from social.reddit_comments where reddit_post_id=%s", [pid])
    refs = media_references(post, comments)
    failed = False
    for ref in refs:
        execution.check()
        url = ref["source_url"]
        key = pid + ":" + hashlib.sha256((url + ":" + (ref["reddit_comment_id"] or "")).encode()).hexdigest()
        target = capture.ensure_target(execution.run_id, "media", key, post_id=pid)
        if target["state"] in capture.SATISFIED:
            continue
        if not capture.start_target(execution.run_id, "media", key, execution.claim_token):
            failed = True
            continue
        try:
            asset: dict[str, Any] | None = pg.fetch_one(
                "select * from social.reddit_assets where source_url=%s and state='mirrored'", [url]
            )
            if asset is None:
                mirrored = mirror_url_to_s3(
                    media_download_url(url), max_bytes=100 * 1024 * 1024, key_namespace="reddit"
                )
                if mirrored.status not in {"mirrored", "skipped"} or not mirrored.hosted_key or not mirrored.hosted_url:
                    raise RuntimeError(mirrored.error or "Media upload did not succeed")
                asset = {
                    name: getattr(mirrored, name, None)
                    for name in ("hosted_key", "hosted_url", "sha256", "content_type", "size_bytes")
                }
            # Upload alone is not capture. The asset, association and target commit atomically.
            with execution.transaction() as conn:
                workflows.lock_content(post_ids=[pid], conn=conn)
                deleted = capture._one(
                    "select state from social.reddit_lifecycle_enrollment "
                    "where reddit_post_id=%s and state in ('deleted','removed')",
                    [pid],
                    conn=conn,
                )
                if (deleted and ref["reddit_comment_id"] is None) or (
                    ref["reddit_comment_id"]
                    and capture._one(
                        "select id from social.reddit_comment_tombstones where id=%s",
                        [ref["reddit_comment_id"]],
                        conn=conn,
                    )
                ):
                    capture.finish_target(
                        execution.run_id,
                        "media",
                        key,
                        execution.claim_token,
                        "source_unavailable",
                        result={"evidence": {"reason": "confirmed_lifecycle_tombstone"}},
                        conn=conn,
                    )
                    continue
                capture._one(
                    "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ["reddit-object:" + asset["hosted_key"]],
                    conn=conn,
                )
                if capture._one(
                    "select hosted_key from social.reddit_object_deletion_outbox where hosted_key=%s",
                    [asset["hosted_key"]],
                    conn=conn,
                ):
                    # The object may have been deleted between upload and this association transaction.
                    # Under the shared deletion lock, absence remains retryable rather than committed.
                    from trr_backend.media.s3_mirror import get_object_storage_bucket, get_s3_client

                    get_s3_client().head_object(Bucket=get_object_storage_bucket(), Key=asset["hosted_key"])
                saved = capture._one(
                    """insert into social.reddit_assets
                    (source_url,media_type,state,hosted_key,hosted_url,sha256,content_type,size_bytes)
                    values(%s,%s,'mirrored',%s,%s,%s,%s,%s) on conflict(source_url) do update set
                    state='mirrored',hosted_key=excluded.hosted_key,hosted_url=excluded.hosted_url,
                    sha256=excluded.sha256,content_type=excluded.content_type,size_bytes=excluded.size_bytes,
                    error=null,updated_at=now() returning id""",
                    [
                        url,
                        ref["media_type"],
                        asset["hosted_key"],
                        asset["hosted_url"],
                        asset["sha256"],
                        asset["content_type"],
                        asset["size_bytes"],
                    ],
                    conn=conn,
                )
                if saved is None:
                    raise RuntimeError("Reddit asset upsert returned no row")
                association = pid + ":" + (ref["reddit_comment_id"] or "post")
                capture._one(
                    """insert into social.reddit_asset_links
                    (asset_id,reddit_post_id,reddit_comment_id,association_key) values(%s,%s,%s,%s)
                    on conflict do nothing returning asset_id""",
                    [saved["id"], pid, ref["reddit_comment_id"], association],
                    conn=conn,
                )
                capture.finish_target(
                    execution.run_id,
                    "media",
                    key,
                    execution.claim_token,
                    "committed",
                    result={"asset_id": str(saved["id"]), **ref},
                    conn=conn,
                )
        except (capture.LostClaim, capture.IdentityBlockedError):
            raise
        except Exception as exc:
            failed = True
            capture.finish_target(execution.run_id, "media", key, execution.claim_token, "retry_wait", error=str(exc))
    with execution.transaction() as conn:
        # Media in unresolved comments is not yet enumerated and must remain visibly incomplete.
        comment_state = capture._one(
            "select state from social.reddit_work_targets where run_id=%s and kind='comments' and target_key=%s",
            [execution.run_id, pid],
            conn=conn,
        )
        if comment_state is None:
            comment_state = capture._one(
                "select state from social.reddit_capture_state where reddit_post_id=%s and phase='comments'",
                [pid],
                conn=conn,
            )
        failed = failed or not comment_state or comment_state["state"] not in capture.SATISFIED
        state = "retry_wait" if failed else "committed"
        capture.record_capture(pid, "media", state, execution.run_id, evidence={"assets": len(refs)}, conn=conn)
        capture.finish_target(
            execution.run_id,
            "media",
            pid,
            execution.claim_token,
            state,
            result={"assets": len(refs)},
            error="Unresolved assets or comment media" if failed else None,
            conn=conn,
        )
        workflows.enroll_capture(post_id=pid, run_id=execution.run_id, conn=conn)


def execute(run: dict) -> dict:
    from trr_backend.repositories.reddit_transport import build_client

    payload = capture.normalize_policy(run["request_payload"])
    run_id = str(run["id"])
    execution = capture.Execution(run)
    try:
        with execution:
            client = build_client(require_oauth=True)
            if "discovery" in payload["requested_phases"]:
                discovery.discover(payload, client, execution)
            else:
                with execution.transaction() as conn:
                    capture._one(
                        "update social.reddit_run_manifests set coverage=%s::jsonb where run_id=%s returning run_id",
                        [capture._json({"state": "not_requested", "reasons": ["discovery_not_requested"]}), run_id],
                        conn=conn,
                    )
            posts = pg.fetch_all(
                """select distinct p.* from social.reddit_posts p
                join social.reddit_period_post_matches m using(reddit_post_id)
                where m.community_id=%s and m.season_id=%s and m.period_key=%s
                and (%s::text[]='{}' or p.reddit_post_id=any(%s::text[]))
                order by p.reddit_post_id""",
                [
                    payload["community_id"],
                    payload["season_id"],
                    payload["period_key"],
                    payload["selected_post_ids"],
                    payload["selected_post_ids"],
                ],
            )
            ids = [p["reddit_post_id"] for p in posts]
            for phase in ("comments", "media"):
                if phase not in payload["requested_phases"]:
                    continue
                due = capture.capture_due(ids, phase, payload["refresh_policy"], payload["stale_after_seconds"])
                for missing_id in sorted(set(payload["selected_post_ids"]) - set(ids)):
                    with execution.transaction() as conn:
                        capture.ensure_target(run_id, phase, missing_id, post_id=missing_id, conn=conn)
                        capture.finish_target(
                            run_id,
                            phase,
                            missing_id,
                            execution.claim_token,
                            "failed",
                            error="Selected post is not saved in this community/season/period; discover it first",
                            conn=conn,
                        )
                for pid in ids:
                    with execution.transaction() as conn:
                        target = capture.ensure_target(run_id, phase, pid, post_id=pid, conn=conn)
                        if pid not in due and target["state"] not in capture.SATISFIED:
                            capture.finish_target(
                                run_id,
                                phase,
                                pid,
                                execution.claim_token,
                                "committed",
                                result={"satisfied_by": "existing_capture"},
                                conn=conn,
                            )
                for post in posts:
                    execution.check()
                    if phase == "comments":
                        comment_capture(post, client, execution)
                    else:
                        media_capture(post, execution)
            with execution.transaction() as conn:
                capture._one(
                    """update social.reddit_posts p set detail_scraped_at=now()
                    where p.reddit_post_id=any(%s::text[]) and
                    (select count(*) from social.reddit_capture_state c where c.reddit_post_id=p.reddit_post_id
                     and c.state in ('committed','source_unavailable'))=2 returning p.reddit_post_id""",
                    [ids],
                    conn=conn,
                )
                capture._one(
                    """insert into social.reddit_run_post_observations
                    (run_id,reddit_post_id,score,num_comments,saved_comments,comments_state,media_state,source_revision)
                    select %s,p.reddit_post_id,p.score,p.num_comments,
                    (select count(*) from social.reddit_comments c where c.reddit_post_id=p.reddit_post_id),
                    cs.state,ms.state,cs.source_revision from social.reddit_posts p
                    left join social.reddit_capture_state cs
                      on cs.reddit_post_id=p.reddit_post_id and cs.phase='comments'
                    left join social.reddit_capture_state ms on ms.reddit_post_id=p.reddit_post_id and ms.phase='media'
                    where p.reddit_post_id=any(%s::text[]) on conflict(run_id,reddit_post_id) do update set
                    score=excluded.score,num_comments=excluded.num_comments,saved_comments=excluded.saved_comments,
                    comments_state=excluded.comments_state,media_state=excluded.media_state,
                    source_revision=excluded.source_revision,observed_at=now() returning reddit_post_id""",
                    [run_id, ids],
                    conn=conn,
                )
            contract = capture.run_contract(run_id)
            status = capture.terminal_status(contract, useful=bool(posts) or contract["satisfied"] > 0)
            retry = (
                pg.fetch_one(
                    "select min(next_retry_at) as at from social.reddit_work_targets "
                    "where run_id=%s and state='retry_wait'",
                    [run_id],
                )
                or {}
            )
            with execution.transaction() as conn:
                # Automatic retries retain the original run and manifest; exhausted work becomes terminal.
                capture._one(
                    """update social.reddit_refresh_runs set status=%s,
                    diagnostics=coalesce(diagnostics,'{}'::jsonb)||%s::jsonb,
                    total_rows=%s,matched_rows=%s,next_retry_at=%s,
                    completed_at=case when %s then null else now() end,
                    claim_token=null,lease_expires_at=null,updated_at=now() where id=%s returning id""",
                    [
                        "queued" if retry.get("at") else status,
                        capture._json({"capture": contract}),
                        len(posts),
                        len(posts),
                        retry.get("at"),
                        bool(retry.get("at")),
                        run_id,
                    ],
                    conn=conn,
                )
                capture._one(
                    """update social.reddit_dispatch_outbox set
state=%s,next_attempt_at=coalesce(%s,now()),updated_at=now() where run_id=%s returning run_id""",
                    ["pending" if retry.get("at") else "done", retry.get("at"), run_id],
                    conn=conn,
                )
    except capture.IdentityBlockedError as exc:
        with pg.transaction() as conn:
            capture.assert_owner(run_id, execution.claim_token, conn=conn)
            capture._one(
                """update social.reddit_refresh_runs set control_state='paused',status='queued',
                error_message=%s,claim_token=null,lease_expires_at=null,updated_at=now()
                where id=%s returning id""",
                [str(exc), run_id],
                conn=conn,
            )
    except capture.RunPaused:
        pg.execute_returning(
            """update social.reddit_refresh_runs set
status='queued',claim_token=null,lease_expires_at=null,updated_at=now() where id=%s and
claim_token=%s returning id""",
            [run_id, execution.claim_token],
        )
    except capture.LostClaim:
        logger.info("Reddit worker stopped after ownership/control change: %s", run_id)
    except Exception as exc:
        logger.exception("Reddit capture execution failed: %s", run_id)
        with pg.transaction() as conn:
            capture.assert_owner(run_id, execution.claim_token, conn=conn)
            capture._one(
                """update social.reddit_refresh_runs set
status='failed',error_message=%s,completed_at=now(),claim_token=null,lease_expires_at=null,updated_at=now()
where id=%s returning id""",
                [str(exc), run_id],
                conn=conn,
            )
    return legacy.get_refresh_run(run_id)

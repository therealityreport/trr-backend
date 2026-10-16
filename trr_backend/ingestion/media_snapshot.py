"""Read-only, account-bound media inventory; no source requests or repair.

A source count is not a pagination seal. Complete comments require a retained
business receipt naming the exact persisted inventory. Historical legacy rows
without that metadata remain unresolved rather than becoming an empty snapshot.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import (
    MEDIA_VERSION,
    AvatarSource,
    CommentMedia,
    MediaIncomplete,
    MediaSnapshot,
    legacy_comment_source_key,
    source_reference,
)


def _read_connection():
    from trr_backend.db import pg

    # This multi-query inventory needs one snapshot. The ordinary read helper
    # uses autocommit; the explicit transaction is switched to READ ONLY before
    # its first query and never issues a business write.
    return pg.db_connection(label="instagram-media-snapshot", pool_name="social_progress")


class AvatarSourcesMissing(MediaIncomplete):
    def __init__(self, handles):
        self.handles = tuple(sorted(set(handles)))
        super().__init__("avatar_source_identity_unresolved")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _urls(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise MediaIncomplete("invalid_persisted_media_inventory")
    return tuple(dict.fromkeys(item.strip() for item in value))


def _time(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise MediaIncomplete("original_source_observation_required") from None
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise MediaIncomplete("original_source_observation_required")
    return value


def _handle(value: Any) -> str:
    if not isinstance(value, str):
        raise MediaIncomplete("avatar_identity_unresolved")
    value = value.strip().lstrip("@").lower()
    if not re.fullmatch(r"[a-z0-9._]{1,30}", value):
        raise MediaIncomplete("avatar_identity_unresolved")
    return value


def _handles(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise MediaIncomplete("avatar_identity_unresolved")
    return tuple(dict.fromkeys(_handle(item) for item in value))


def _profile_url(source: Mapping[str, Any], *columns: str) -> str | None:
    for column in (
        *columns,
        "profile_pic_url_hd",
        "profile_pic_url",
        "owner_profile_pic_url_hd",
        "owner_profile_pic_url",
    ):
        value = source.get(column)
        if isinstance(value, str) and value.strip():
            source_reference(value.strip())
            return value.strip()
    hd = _mapping(source.get("hd_profile_pic_url_info"))
    if hd.get("url"):
        source_reference(hd["url"])
        return hd["url"]
    return None


def _source_key(row: Mapping[str, Any], retained: Mapping[str, str]) -> str:
    row_id, comment_id = str(row["id"]), str(row["comment_id"])
    key = retained.get(row_id)
    if key:
        return key
    found = set()
    for url in _urls(row.get("hosted_media_urls")):
        match = re.search(r"/(unknown-[a-f0-9]{24})/media-\d+\.[A-Za-z0-9]+$", urlsplit(url).path)
        if not match:
            raise MediaIncomplete("preserved_comment_storage_identity_missing")
        found.add(match[1])
    if len(found) > 1:
        raise MediaIncomplete("preserved_comment_storage_identity_conflict")
    if found:
        return next(iter(found))
    raw_key = _mapping(row.get("raw_data")).get("storage_source_key")
    if raw_key:
        if not isinstance(raw_key, str) or not re.fullmatch(r"unknown-[a-f0-9]{24}", raw_key):
            raise MediaIncomplete("preserved_comment_storage_identity_invalid")
        return raw_key
    if not comment_id:
        raise MediaIncomplete("comment_identity_required")
    return legacy_comment_source_key(row_id, _urls(row.get("media_urls")))


class MediaSnapshotLoader:
    def __init__(
        self,
        read_connection: Callable = _read_connection,
        *,
        include_comment_avatars: bool = False,
        unsupported_ttl_hours: int = 168,
        clock: Callable = lambda: datetime.now(UTC),
    ):
        if type(include_comment_avatars) is not bool:
            raise ValueError("Explicit comment avatar policy is required")
        self.read_connection = read_connection
        self.include_comment_avatars = include_comment_avatars
        if unsupported_ttl_hours < 1:
            raise ValueError("Unsupported avatar TTL must be positive")
        self.unsupported_ttl_hours, self.clock = unsupported_ttl_hours, clock

    def load(
        self,
        *,
        run_id: str,
        account: str,
        target_key: str,
        generation: int | None = None,
        check_cancelled: Callable[[], None] | None = None,
        on_request: Callable[[], None] | None = None,
    ) -> MediaSnapshot:
        # Database reads do not consume source-request accounting. The callback
        # remains accepted so this method can be bound directly to MediaActivity.
        if check_cancelled:
            check_cancelled()
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            if conn.autocommit:
                raise MediaIncomplete("media_snapshot_transaction_required")
            cur.execute("set transaction isolation level repeatable read read only")
            cur.execute(
                """select r.* from social.instagram_ingestion_runs r
                join social.instagram_ingestion_targets t using(run_id)
                where r.run_id=%s and r.account=%s and t.stage='media' and t.target_key=%s""",
                (run_id, account, target_key),
            )
            run = cur.fetchone()
            if not run:
                raise LookupError("Media target not found for this account and run")
            if run["state"] not in {"queued", "running"} or (
                generation is not None and run["generation"] != generation
            ):
                raise LostOwnership("Media run execution is no longer current")
            intent = RunIntent.from_payload(run["intent"])
            if intent.account != account or Stage.MEDIA not in intent.selected_tasks:
                raise Conflict("Run does not select media for this account")
            cur.execute(
                """select p.*,to_jsonb(s) as season,to_jsonb(c) as canonical,to_jsonb(b) as payload_sidecar,
                to_jsonb(j) as source_job from social.instagram_posts p
                join social.social_posts c on c.platform='instagram' and c.source_id=p.shortcode
                left join core.seasons s on s.id=p.season_id
                left join social.instagram_post_payloads b on b.post_id=p.id
                left join social.scrape_jobs j on j.id=p.job_id
                where p.shortcode=%s and lower(coalesce(nullif(p.source_account,''),p.username))=%s""",
                (target_key, account),
            )
            post = cur.fetchone()
            if not post:
                raise LookupError("Persisted media post not found for this account")
            post = dict(post)
            post_time = _time(post.get("scraped_at"))
            cur.execute(
                """select * from social.instagram_comments where post_id=%s and not is_missing
                and deleted_at is null order by comment_id,id""",
                (post["id"],),
            )
            comments = [dict(row) for row in cur.fetchall()]
            self._completion(
                cur, run_id, account, target_key, post, comments, selected=Stage.COMMENTS in intent.selected_tasks
            )
            # Completion covers every active comment. Only rows with media (or
            # selected author avatars) are media work; do not rewrite thousands
            # of text-only comments or retain them again as media evidence.
            media_comments = [row for row in comments if _urls(row.get("media_urls")) or self.include_comment_avatars]
            retained_keys = self._retained_keys(cur, account, target_key, media_comments)
            sidecar = _mapping(post.get("payload_sidecar"))
            raw = {**_mapping(post.get("raw_data")), **_mapping(sidecar.get("raw_data"))}
            avatars, unavailable_avatars = self._avatars(cur, post, raw, comments, post_time)
            manifest = _mapping(sidecar.get("asset_manifest")) or _mapping(post.get("asset_manifest"))
            show_id, season_number, week_index = self._storage_context(cur, run_id, target_key, post, manifest, intent)
            media_urls = _urls(post.get("media_urls"))
            detail = _mapping(raw.get("detail_snapshot"))
            expected = detail.get("media_count", raw.get("carousel_media_count"))
            children = sidecar.get("child_posts_data") or post.get("child_posts_data") or []
            if expected is None:
                expected = len(children) if children else 1
            if type(expected) is not int or expected < 1:
                raise MediaIncomplete("media_source_coverage_unknown")
            thumbnail = post.get("thumbnail_url") or next(
                (
                    url
                    for url in media_urls
                    if not urlsplit(url).path.lower().endswith((".mp4", ".mov", ".m4v", ".webm"))
                ),
                "",
            )
            snapshot = MediaSnapshot(
                observed_at=post_time,
                post_id=str(post["id"]),
                canonical_post_id=str(post["canonical"]["id"]),
                shortcode=target_key,
                show_id=show_id,
                season_number=season_number,
                week_index=week_index,
                media_urls=media_urls,
                thumbnail_url=thumbnail,
                expected_media_count=expected,
                comments=tuple(
                    CommentMedia(
                        str(row["comment_id"]),
                        str(row["id"]),
                        _urls(row.get("media_urls")),
                        _source_key(row, retained_keys),
                        _time(row.get("scraped_at")),
                    )
                    for row in media_comments
                ),
                avatars=avatars,
                comments_complete=True,
                avatars_complete=True,
                existing_manifest=manifest,
                include_comment_avatars=self.include_comment_avatars,
                unavailable_avatars=unavailable_avatars,
            )
            snapshot.validate()
        if check_cancelled:
            check_cancelled()
        return snapshot

    @staticmethod
    def _completion(
        cur, run_id: str, account: str, target_key: str, post: Mapping, comments: list[dict], *, selected: bool
    ):
        cur.execute(
            """select q.*,r.run_id as source_run_id from social.instagram_ingestion_receipts q
            join social.instagram_ingestion_runs r using(run_id)
            join social.instagram_ingestion_manifests m on m.run_id=q.run_id and m.stage=q.stage
            where r.account=%s and q.stage='comments' and q.target_key=%s and m.discovery_complete
            and q.outcome in ('committed','cached_satisfied')
            and (%s=false or r.run_id=%s)
            order by q.observed_at desc,q.committed_at desc limit 1""",
            (account, target_key, selected, run_id),
        )
        receipt = cur.fetchone()
        if not receipt:
            raise MediaIncomplete("complete_comment_inventory_proof_required")
        if selected:
            cur.execute(
                """select count(*) as unresolved from social.instagram_ingestion_targets
                where run_id=%s and stage='comments' and outcome='unresolved'""",
                (run_id,),
            )
            if cur.fetchone()["unresolved"]:
                raise MediaIncomplete("selected_comments_stage_unresolved")
        identity = _mapping(receipt.get("canonical_identity"))
        observed = identity.get("observed_comment_ids")
        if str(identity.get("post_id")) != str(post["id"]) or not isinstance(observed, list):
            raise MediaIncomplete("comment_inventory_identity_proof_required")
        if any(not isinstance(item, str) or not item for item in observed) or len(set(observed)) != len(observed):
            raise MediaIncomplete("comment_inventory_identity_proof_invalid")
        if set(observed) != {str(row["comment_id"]) for row in comments}:
            raise MediaIncomplete("persisted_comment_inventory_changed")
        stamp = _time(receipt["observed_at"])
        if stamp < _time(post["scraped_at"]):
            raise MediaIncomplete("comment_inventory_predates_post_source")
        for row in comments:
            if row.get("source_account") and row["source_account"].lower() != account:
                raise MediaIncomplete("comment_inventory_account_mismatch")
            if row.get("source_snapshot_type") != "full_comments_scrape" or _time(row.get("scraped_at")) > stamp:
                raise MediaIncomplete("comment_inventory_has_unproved_source")

    @staticmethod
    def _retained_keys(cur, account: str, target_key: str, comments: list[dict]) -> dict[str, str]:
        cur.execute(
            """select e.*,r.state<>'completed' as pinned from social.instagram_ingestion_evidence e
            join social.instagram_ingestion_runs r using(run_id) where r.account=%s and e.stage='media'
            and e.target_key=%s and e.parser_version=%s and e.payload is not null order by e.captured_at desc""",
            (account, target_key, MEDIA_VERSION),
        )
        expected = {str(row["id"]): str(row["comment_id"]) for row in comments}
        keys = {}
        for row in cur.fetchall():
            evidence = Evidence(
                source_identity=row["source_identity"],
                captured_at=row["captured_at"],
                parser_version=row["parser_version"],
                payload_json=row["payload"],
                content_hash=row["content_hash"],
                schema_version=row["schema_version"],
            )
            # Stable storage identities are non-secret provenance, but retained
            # expired source still cannot be read as if it were available.
            if not row["pinned"] and evidence.expires_at <= datetime.now(evidence.captured_at.tzinfo):
                continue
            payload = evidence.read(pinned=row["pinned"])
            for entry in payload.get("comments") or []:
                row_id = entry.get("row_id")
                key = entry.get("storage_source_key")
                if expected.get(row_id) == entry.get("comment_id") and isinstance(key, str) and key:
                    if not re.fullmatch(r"unknown-[a-f0-9]{24}", key):
                        raise MediaIncomplete("retained_comment_storage_key_invalid")
                    keys.setdefault(row_id, key)
        return keys

    def _avatars(
        self, cur, post: Mapping, raw: Mapping, comments: list[dict], observed_at: datetime
    ) -> tuple[tuple[AvatarSource, ...], tuple[str, ...]]:
        people: dict[str, Mapping] = {}
        for column in ("tagged_users_detail", "collaborators_detail"):
            for person in post.get(column) or raw.get(column) or []:
                if not isinstance(person, Mapping):
                    raise MediaIncomplete("avatar_identity_unresolved")
                people[_handle(person.get("username"))] = person
        owner = _handle(post.get("owner_username") or post.get("username"))
        people[owner] = {
            **_mapping(raw.get("owner_detail") or raw.get("owner") or raw.get("user")),
            **{
                key: value
                for key, value in {
                    "profile_pic_url_hd": post.get("owner_profile_pic_url_hd"),
                    "profile_pic_url": post.get("owner_profile_pic_url"),
                }.items()
                if value
            },
        }
        slots = [(owner, "owner", None, observed_at, people[owner])]
        for column, role in (("profile_tags", "tagged"), ("collaborators", "collaborator"), ("mentions", "mention")):
            for handle in _handles(post.get(column) or raw.get(column)):
                if handle != owner:
                    slots.append((handle, role, None, observed_at, people.get(handle, {})))
        # Detailed source identities also count even if an older projection did
        # not populate the parallel handle-only arrays.
        for handle, person in people.items():
            if handle != owner and not any(slot[0] == handle for slot in slots):
                slots.append((handle, "tagged", None, observed_at, person))
        if self.include_comment_avatars:
            for row in comments:
                handle = _handle(row.get("username"))
                slots.append(
                    (
                        handle,
                        "comment",
                        str(row["comment_id"]),
                        _time(row.get("scraped_at")),
                        {
                            "profile_pic_url_hd": row.get("author_profile_pic_url_hd"),
                            "profile_pic_url": row.get("author_profile_pic_url"),
                        },
                    )
                )
        handles = {handle for handle, *_ in slots}
        registry = {}
        if handles:
            cur.execute(
                """select distinct on(account_handle) * from social.avatar_registry
                where platform='instagram' and account_handle=any(%s)
                and last_checked_at is not null
                order by account_handle,last_checked_at desc,
                coalesce(status='mirrored' and nullif(hosted_url,'') is not null
                  and content_hash ~ '^[a-f0-9]{64}$',false) desc,id desc""",
                (sorted(handles),),
            )
            registry = {row["account_handle"]: dict(row) for row in cur.fetchall()}
        avatars, unavailable, unresolved = [], set(), set()
        for handle, role, comment_id, stamp, person in slots:
            url = _profile_url(person)
            fallback = registry.get(handle)
            if url and fallback:
                registry_time = _time(fallback["last_checked_at"])
                hosted = fallback.get("hosted_url")
                cached = (
                    fallback.get("status") == "mirrored"
                    and isinstance(hosted, str)
                    and bool(hosted)
                    and source_reference(hosted) == hosted
                    and re.fullmatch(r"[a-f0-9]{64}", fallback.get("content_hash") or "")
                    and fallback.get("source_url")
                    and source_reference(fallback["source_url"]) == source_reference(url)
                )
                if cached and registry_time >= stamp:
                    # This avatar has its own retained observation. Do not
                    # relabel the post or overwrite the registry with its older
                    # capture merely because another post uses the same image.
                    avatars.append(
                        AvatarSource(handle, url, role, comment_id, registry_time, hosted, fallback["content_hash"])
                    )
                    continue
                if registry_time > stamp:
                    # A newer conflicting or unproved identity cannot authorize
                    # propagation of the older explicit source or cached bytes.
                    raise MediaIncomplete("avatar_source_newer")
            if not url:
                if fallback and fallback.get("status") == "unsupported":
                    age = self.clock() - _time(fallback["last_checked_at"])
                    if fallback.get("failure_reason") == "profile_pic_not_available" and timedelta(
                        0
                    ) <= age < timedelta(hours=self.unsupported_ttl_hours):
                        unavailable.add(handle)
                        continue
                    unresolved.add(handle)
                    continue
                if (
                    not fallback
                    or fallback.get("status") not in {"mirrored", "pending"}
                    or not fallback.get("source_url")
                ):
                    unresolved.add(handle)
                    continue
                url, stamp = fallback["source_url"], _time(fallback["last_checked_at"])
                source_reference(url)
                if (
                    fallback.get("status") == "mirrored"
                    and isinstance(fallback.get("hosted_url"), str)
                    and bool(fallback["hosted_url"])
                    and source_reference(fallback["hosted_url"]) == fallback["hosted_url"]
                    and re.fullmatch(r"[a-f0-9]{64}", fallback.get("content_hash") or "")
                ):
                    avatars.append(
                        AvatarSource(
                            handle, url, role, comment_id, stamp, fallback["hosted_url"], fallback["content_hash"]
                        )
                    )
                    continue
            avatars.append(AvatarSource(handle, url, role, comment_id, stamp))
        if unresolved:
            raise AvatarSourcesMissing(unresolved)
        return tuple(avatars), tuple(sorted(unavailable))

    @classmethod
    def _storage_context(cls, cur, run_id, target_key, post, manifest, intent):
        # A failed download already retained the storage context. Preserve it
        # through this run's retry even if classification changes meanwhile.
        cur.execute(
            """select * from social.instagram_ingestion_evidence
            where run_id=%s and stage='media' and target_key=%s and parser_version=%s
            and payload is not null order by captured_at,evidence_id limit 1""",
            (run_id, target_key, MEDIA_VERSION),
        )
        retained = cur.fetchone()
        if retained:
            payload = Evidence(
                source_identity=retained["source_identity"],
                captured_at=retained["captured_at"],
                parser_version=retained["parser_version"],
                payload_json=retained["payload"],
                content_hash=retained["content_hash"],
                schema_version=retained["schema_version"],
            ).read(pinned=True)
            if payload.get("show_id") and type(payload.get("season_number")) is int:
                return str(UUID(payload["show_id"])), payload["season_number"], payload.get("week_index")
        roots = set()
        originals = _mapping(manifest.get("originals"))
        for entry in [_mapping(originals.get("cover")), *(originals.get("media") or [])]:
            key = _mapping(entry).get("storage_key") or ""
            match = re.search(r"(?:^|/)social/instagram/([a-fA-F0-9-]{36})/(-?\d+)/week-(unknown|-?\d+)/", key)
            if match:
                roots.add((str(UUID(match[1])), int(match[2]), None if match[3] == "unknown" else int(match[3])))
        if len(roots) > 1:
            raise MediaIncomplete("preserved_media_storage_context_conflict")
        if roots:
            return next(iter(roots))
        season = _mapping(post.get("season"))
        week = cls._week(post, manifest)
        if post.get("season_id"):
            if not post.get("show_id") or type(season.get("season_number")) is not int:
                raise MediaIncomplete("media_storage_context_required")
            if season.get("show_id") and str(season["show_id"]) != str(post["show_id"]):
                raise MediaIncomplete("media_season_show_identity_mismatch")
            return str(post["show_id"]), season["season_number"], week
        # Exact maintained shared-account storage identity. These identifiers
        # are never written as show/season assignments to canonical post rows.
        key = f"show:{intent.source_scope}:instagram:{intent.account}"
        show_id = str(UUID(hashlib.md5(key.encode("utf-8")).hexdigest()))
        if week is None and post.get("posted_at") is not None:
            # Synthetic contexts have no episodes; their single week starts at
            # midnight on the post's own New York date, matching legacy policy.
            week = 1
        return show_id, 0, week

    @staticmethod
    def _week(post: Mapping, manifest: Mapping) -> int | None:
        values = set()
        for entry in [
            _mapping(_mapping(manifest.get("originals")).get("cover")),
            *(_mapping(manifest.get("originals")).get("media") or []),
        ]:
            key = _mapping(entry).get("storage_key") or ""
            match = re.search(r"/week-(unknown|-?\d+)/", key)
            if match:
                values.add(None if match[1] == "unknown" else int(match[1]))
        if len(values) > 1:
            raise MediaIncomplete("preserved_media_week_conflict")
        if values:
            return next(iter(values))
        config = _mapping(_mapping(post.get("source_job")).get("config"))
        value = config.get("week_index", _mapping(post.get("raw_data")).get("week_index"))
        if value is not None and type(value) is not int:
            raise MediaIncomplete("media_week_context_invalid")
        return value

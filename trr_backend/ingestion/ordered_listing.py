"""Explicit ordered-profile contract, native evidence and bounded-window policy.

Grid cards establish membership. Actual permalink evidence establishes dates;
these inputs are never combined into a fabricated native grid response.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from trr_backend.ingestion.canonical import normalize_post_source
from trr_backend.ingestion.domain import RunIntent, SourceProfile, stable_json, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.providers import InvalidSourceError

FAMILY = "instagram_profile_ordered_public"
CONTRACT_VERSION = 2
ADAPTER_VERSION = 1
PARSER_VERSION = "instagram-ordered-listing-v2"
OPERATION = "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection"
DOC_ID = "28904005159232689"
ENDPOINT = "https://www.instagram.com/api/graphql"
CONNECTION = "polaris_ordered_timeline_connection"
PROVIDER = "__relay_internal__pv__PolarisSEOIsCrawlerrelayprovider"
PAGE_SIZE = 12
MAX_PAGES = 1024
WINDOW_POLICY = "ordered_window_crossing_heuristic_v1"
POLICY = {
    "family": FAMILY,
    "source_contract_version": CONTRACT_VERSION,
    "adapter_version": ADAPTER_VERSION,
    "window_policy": WINDOW_POLICY,
    "max_committed_pages": MAX_PAGES,
    "max_card_dates_per_page": PAGE_SIZE,
    "older_pages_required": 3,
    "missing_pin_flags": "unknown",
}


def is_ordered(intent: RunIntent) -> bool:
    return intent.effective_source_profile == SourceProfile.for_public_ordered()


def identity_fields(user: dict[str, Any]) -> dict[str, str]:
    fields = {}
    for key in ("id", "pk"):
        value = user.get(key)
        if type(value) is int and 0 < value < 10**30:
            value = str(value)
        if isinstance(value, str) and 0 < len(value) <= 256 and all(33 <= ord(c) <= 126 for c in value):
            fields[key] = value
    return fields


def identity_matches(fields: dict[str, str], expected: dict[str, str]) -> bool:
    common = fields.keys() & expected.keys()
    return bool(common) and all(fields[key] == expected[key] for key in common)


@dataclass(frozen=True)
class OrderedCard:
    native_id: str
    shortcode: str
    node: dict[str, Any]
    known_pinned: bool | None


@dataclass(frozen=True)
class OrderedPage:
    cards: tuple[OrderedCard, ...]
    has_next_page: bool
    end_cursor: str | None
    profile: dict[str, str]


def native_post_id(node: dict[str, Any]) -> str:
    value = node.get("pk", node.get("id"))
    if type(value) not in (str, int) or not re.fullmatch(r"[0-9]{1,40}(?:_[0-9]{1,40})?", str(value)):
        raise InvalidSourceError("Ordered card native identity is invalid")
    return str(value).split("_")[0]


def parse_page(payload: dict[str, Any], *, account: str, cursor: str | None) -> OrderedPage:
    if (
        payload.get("adapter_family") != FAMILY
        or type(payload.get("version")) is not int
        or payload["version"] != CONTRACT_VERSION
        or type(payload.get("adapter_version")) is not int
        or payload["adapter_version"] != ADAPTER_VERSION
        or payload.get("request_cursor") != cursor
        or payload.get("page_size") != PAGE_SIZE
        or payload.get("listing_profile_username") != account
        or payload.get("relay_doc_id") != (DOC_ID if cursor is not None else None)
        or payload.get("relay_friendly_name") != (OPERATION if cursor is not None else None)
        or payload.get("source_url") != (ENDPOINT if cursor is not None else f"https://www.instagram.com/{account}/")
    ):
        raise InvalidSourceError("Ordered listing evidence differs from its frozen request contract")
    metadata = payload.get("user")
    if (
        not isinstance(metadata, dict)
        or not isinstance(metadata.get("username"), str)
        or metadata["username"].casefold() != account
    ):
        raise InvalidSourceError("Ordered listing has no account-bound document metadata")
    if metadata.get("is_private") is True:
        raise InvalidSourceError("Ordered profile is explicitly private")
    expected = identity_fields(metadata)
    data = payload.get("data")
    user = data.get("node" if cursor is not None else "xig_user_by_username") if isinstance(data, dict) else None
    if not isinstance(user, dict) or not identity_matches(identity_fields(user), expected):
        raise InvalidSourceError("Ordered listing native profile identity differs from the document")
    if "username" in user and (not isinstance(user["username"], str) or user["username"].casefold() != account):
        raise InvalidSourceError("Ordered listing username differs from the selected account")
    fields = identity_fields(user)
    if "id" not in fields or payload.get("listing_profile_id") != fields["id"]:
        raise InvalidSourceError("Ordered listing native opaque ID is missing or mismatched")
    if payload.get("errors") or payload.get("error") or payload.get("status") not in (None, "ok"):
        raise InvalidSourceError("Ordered listing contains an unresolved source error")
    connection = user.get(CONNECTION)
    edges = connection.get("edges") if isinstance(connection, dict) else None
    info = connection.get("page_info") if isinstance(connection, dict) else None
    if not isinstance(edges, list) or len(edges) > PAGE_SIZE or not isinstance(info, dict):
        raise InvalidSourceError("Ordered listing page does not have bounded native edges and page info")
    has_next, end_cursor = info.get("has_next_page"), info.get("end_cursor")
    if type(has_next) is not bool or (
        end_cursor is not None and (not isinstance(end_cursor, str) or not 0 < len(end_cursor) <= 8192)
    ):
        raise InvalidSourceError("Ordered listing native pagination is invalid")
    if has_next and not end_cursor:
        raise InvalidSourceError("Ordered listing requires its actual continuation cursor")
    if cursor is not None and end_cursor == cursor:
        raise InvalidSourceError("Ordered listing cursor repeated; coverage is unresolved")
    cards = []
    for edge in edges:
        node = edge.get("node") if isinstance(edge, dict) else None
        if not isinstance(node, dict):
            raise InvalidSourceError("Ordered listing contains a malformed native card")
        code = node.get("code", node.get("shortcode"))
        if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_-]{5,64}", code):
            raise InvalidSourceError("Ordered listing shortcode is invalid")
        pins = [
            source[key]
            for source in (edge, node)
            for key in ("is_pinned", "isPinned", "pinned", "is_pinned_to_profile")
            if key in source
        ]
        if any(type(flag) is not bool for flag in pins) or (pins and len(set(pins)) != 1):
            raise InvalidSourceError("Ordered listing pin facts are malformed or conflicting")
        cards.append(OrderedCard(native_post_id(node), code, node, pins[0] if pins else None))
    if len({card.native_id for card in cards}) != len(cards) or len({card.shortcode for card in cards}) != len(cards):
        raise InvalidSourceError("Ordered listing contains duplicate identities within a native page")
    return OrderedPage(tuple(cards), has_next, end_cursor, fields)


def extract_document(payloads: list[Any], *, account: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Select original native fragments, without copying document runtime data."""
    roots = []
    pending = [(payload, 0) for payload in payloads]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > 200000 or depth > 40:
            raise InvalidSourceError("Ordered document scan exceeded its finite bound")
        if isinstance(value, dict):
            data = value.get("data")
            user = data.get("xig_user_by_username") if isinstance(data, dict) else None
            if isinstance(user, dict):
                roots.append(user)
            pending.extend((child, depth + 1) for child in value.values() if isinstance(child, (dict, list)))
        elif isinstance(value, list):
            if len(value) > 5000:
                raise InvalidSourceError("Ordered document array exceeded its finite bound")
            pending.extend((child, depth + 1) for child in value if isinstance(child, (dict, list)))
    metadata = [
        user for user in roots if isinstance(user.get("username"), str) and user["username"].casefold() == account
    ]
    fields = {}
    for user in metadata:
        for key, value in identity_fields(user).items():
            if key in fields and fields[key] != value:
                raise InvalidSourceError("Ordered document account identity is ambiguous")
            fields[key] = value
    if not metadata or not fields:
        raise InvalidSourceError("Ordered document has no native account metadata")
    grids = [user for user in roots if CONNECTION in user]
    unique = {stable_json(user): user for user in grids if identity_matches(identity_fields(user), fields)}
    if len(unique) != 1:
        raise InvalidSourceError("Ordered document has no unique account-bound native grid")
    # Keep the original metadata object. Same-field comparison never decodes an
    # opaque GraphQL ID or compares it to a numeric pk.
    representative = dict(metadata[0])
    for key, value in fields.items():
        representative.setdefault(key, value)
    return representative, next(iter(unique.values()))


@dataclass(frozen=True)
class DateProof:
    evidence: Evidence
    reference: dict[str, Any]
    posted_at: datetime
    source: dict[str, Any]
    canonical_identity: dict[str, str] | None = None


def date_proof(
    card: OrderedCard,
    evidence: Evidence,
    reference: dict[str, Any],
    *,
    account: str,
    canonical_identity: dict[str, str] | None = None,
) -> DateProof:
    from trr_backend.ingestion.listing import _posted_at
    from trr_backend.socials.instagram.post_normalizer import normalize_instagram_post

    source = evidence.read(pinned=True)
    # No request-account fallback is allowed to establish native date ownership.
    # raw_data is the actual unwrapped native node, including wrapped responses.
    normalized = normalize_instagram_post(source)
    if (
        normalized.shortcode != card.shortcode
        or not normalized.source_id
        or str(normalized.source_id).split("_")[0] != card.native_id
    ):
        raise InvalidSourceError("Permalink date evidence differs from its native listing card")
    native = normalized.raw_data
    owner_nodes = [native[key] for key in ("owner", "user") if isinstance(native.get(key), dict)]
    native_username = native.get("ownerUsername") or native.get("owner_username")
    if not native_username:
        native_username = next((owner.get("username") for owner in owner_nodes if owner.get("username")), None)
    if (
        not isinstance(native_username, str)
        or re.fullmatch(r"[A-Za-z0-9._]{1,30}", native_username) is None
        or normalized.owner is None
        or normalized.owner.username != native_username
    ):
        raise InvalidSourceError("Permalink date evidence has no native owner")
    actual = {}
    for owner in owner_nodes:
        if owner.get("username") and owner["username"] != native_username:
            raise InvalidSourceError("Permalink native owner fields conflict")
        for key, value in identity_fields(owner).items():
            if key in actual and actual[key] != value:
                raise InvalidSourceError("Permalink native owner identities conflict")
            actual[key] = value
    card_owner = card.node.get("owner") or card.node.get("user")
    if isinstance(card_owner, dict):
        detail_owner = normalized.owner
        if card_owner.get("username") and str(card_owner["username"]).casefold() != detail_owner.username.casefold():
            raise InvalidSourceError("Permalink owner differs from the native card owner")
        wanted = identity_fields(card_owner)
        if wanted and not identity_matches(actual, wanted):
            raise InvalidSourceError("Permalink native owner identity differs from the card")
    # _posted_at consumes an actual native detail field, never a canonical row's
    # posted_at or a date inferred from an Instagram ID.
    posted = _posted_at(normalized.raw_data)
    if posted is None:
        raise InvalidSourceError("Permalink evidence has no actual native publish date")
    return DateProof(evidence, reference, posted, source, canonical_identity)


def window_coverage(
    intent: RunIntent, page: OrderedPage, dates: tuple[DateProof, ...], previous: dict[str, Any]
) -> dict[str, Any]:
    if not is_ordered(intent) or intent.scope != "bounded_window" or not intent.date_start or not intent.date_end:
        raise InvalidSourceError("Ordered contract requires explicit bounded-window intent")
    if len(dates) != len(page.cards):
        raise InvalidSourceError("Every native card requires its own dated detail proof")
    pages = int(previous.get("committed_pages", 0)) + 1
    if pages > MAX_PAGES:
        raise InvalidSourceError("Ordered listing page budget is exhausted")
    membership = bool(previous.get("window_membership_observed")) or any(
        intent.date_start <= proof.posted_at < intent.date_end for proof in dates
    )
    stamps = [proof.posted_at for proof in dates]
    older = (
        bool(stamps)
        and all(stamp < intent.date_start for stamp in stamps)
        and not any(card.known_pinned is True for card in page.cards)
    )
    monotone = all(later <= earlier for earlier, later in zip(stamps, stamps[1:], strict=False))
    prior_last = previous.get("older_suffix_last_at")
    if prior_last and stamps and stamps[0] > datetime.fromisoformat(prior_last):
        monotone = False
    streak = int(previous.get("older_page_streak", 0)) + 1 if older and monotone else 0
    crossed = membership and streak >= 3
    boundary = not crossed and (not page.has_next_page or pages >= MAX_PAGES)
    return {
        "policy": dict(POLICY),
        "committed_pages": pages,
        "window_membership_observed": membership,
        "older_page_streak": streak,
        "older_suffix_last_at": utc_timestamp(stamps[-1]) if older and monotone else None,
        "coverage_basis": WINDOW_POLICY if crossed else None,
        "stop_reason": "date_window_crossed"
        if crossed
        else "access_scope_boundary"
        if not page.has_next_page
        else "page_budget_exhausted"
        if pages >= MAX_PAGES
        else None,
        "discovery_complete": crossed,
        "coverage_unresolved": boundary,
        "provider_exhausted": False,
        "native_has_next_page": page.has_next_page,
        "pin_semantics": "unknown"
        if any(card.known_pinned is None for card in page.cards)
        else "explicit_native_flags",
        "chronology_certified": False,
    }


class OrderedListingActivity:
    """Fenced durable hydration, followed by one atomic native page commit."""

    def __init__(self, store, *, writer, clock):
        self.store, self.writer, self.clock = store, writer, clock

    def _save(self, owned, expected, state):
        from trr_backend.ingestion.domain import Conflict

        # Future card refs must not mutate the already persisted CAS snapshot.
        checkpoint = json.loads(stable_json(dict(expected, ordered_v2=state)))
        with self.store.transaction() as conn, conn.cursor() as cur:
            self.store.check_owner(**owned, connection=conn)
            cur.execute(
                """update social.instagram_ingestion_targets set checkpoint=%s::jsonb
                where run_id=%s and stage='listing' and target_key=%s and owner_token=%s
                    and checkpoint=%s::jsonb and owner_started_at is not null
                    and owner_expires_at>clock_timestamp() and outcome='unresolved'""",
                (
                    stable_json(checkpoint),
                    owned["run_id"],
                    owned["target_key"],
                    owned["owner_token"],
                    stable_json(expected),
                ),
            )
            if cur.rowcount != 1:
                raise Conflict("Ordered hydration checkpoint changed")
        return checkpoint

    @staticmethod
    def _reference(evidence, evidence_id, owned):
        return {
            "evidence_id": evidence_id,
            "run_id": owned["run_id"],
            "stage": "listing",
            "target_key": owned["target_key"],
            "captured_at": utc_timestamp(evidence.captured_at),
            "content_hash": evidence.content_hash,
            "parser_version": evidence.parser_version,
            "schema_version": evidence.schema_version,
        }

    def _read(self, owned, reference, source_identity):
        from psycopg2.extras import RealDictCursor

        if (
            not isinstance(reference, dict)
            or reference.get("run_id") != owned["run_id"]
            or reference.get("stage") != "listing"
            or reference.get("target_key") != owned["target_key"]
        ):
            raise InvalidSourceError("Hydration evidence reference differs from its owned listing target")
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self.store.check_owner(**owned, connection=conn)
            cur.execute(
                """select * from social.instagram_ingestion_evidence
                where evidence_id=%s and run_id=%s and stage='listing' and target_key=%s""",
                (reference.get("evidence_id"), owned["run_id"], owned["target_key"]),
            )
            row = cur.fetchone()
        if not row or row["source_identity"] != source_identity:
            raise InvalidSourceError("Hydration evidence is missing or has another source identity")
        evidence = Evidence(
            source_identity=row["source_identity"],
            captured_at=row["captured_at"],
            parser_version=row["parser_version"],
            payload_json=row["payload"],
            content_hash=row["content_hash"],
            schema_version=row["schema_version"],
        )
        if self._reference(evidence, str(row["evidence_id"]), owned) != reference:
            raise InvalidSourceError("Hydration evidence reference changed")
        evidence.read(now=self.clock())
        return evidence

    def _hold(self, owned, checkpoint, state, code):
        from trr_backend.ingestion.domain import Failure

        state = dict(state, boundary=code)
        checkpoint = self._save(owned, checkpoint, state)
        with self.store.transaction() as conn, conn.cursor() as cur:
            self.store.check_owner(**owned, connection=conn)
            cur.execute(
                """update social.instagram_ingestion_targets set blocked_reason=%s,
                owner_token=null,owner_started_at=null,owner_expires_at=null
                where run_id=%s and stage='listing' and target_key=%s and owner_token=%s""",
                (code, owned["run_id"], owned["target_key"], owned["owner_token"]),
            )
            cur.execute(
                "update social.instagram_ingestion_runs set state='blocked' where run_id=%s", (owned["run_id"],)
            )
        return {"outcome": "unresolved", "ordered_held": True, "failure": Failure(code)}

    def execute(self, *, owned, run, intent, target, fetch, fetch_detail):
        from psycopg2.extras import RealDictCursor, execute_values

        from trr_backend.ingestion.application import validate_detail
        from trr_backend.ingestion.domain import Conflict, Stage
        from trr_backend.ingestion.freshness import DetailCache
        from trr_backend.ingestion.listing import page_target
        from trr_backend.ingestion.providers import PARSER_VERSION as DETAIL_PARSER_VERSION
        from trr_backend.ingestion.providers import ParsedSource

        if fetch_detail is None:
            raise InvalidSourceError("Ordered listing requires its serial permalink date provider")
        account, cursor = owned["account"], target["checkpoint"]["cursor"]
        checkpoint = dict(target["checkpoint"])
        state = dict(checkpoint.get("ordered_v2", {}))
        if state and state.get("policy") != POLICY:
            raise InvalidSourceError("Ordered hydration checkpoint has a different frozen policy")
        state.setdefault("policy", dict(POLICY))
        state.setdefault("date_refs", {})
        checkpoint = self._save(owned, checkpoint, state)
        # A resumed budget boundary cannot perform an extra source request.
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self.store.check_owner(**owned, connection=conn)
            cur.execute(
                "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
                (owned["run_id"],),
            )
            initial_manifest = cur.fetchone()
        if not initial_manifest or initial_manifest["cursor_state"].get("cursor") != cursor:
            raise Conflict("Ordered native cursor differs from its discovery manifest")
        initial_coverage = initial_manifest["cursor_state"].get("ordered_v2", {})
        if initial_coverage and initial_coverage.get("policy") != POLICY:
            raise InvalidSourceError("Ordered manifest has a different frozen policy")
        if int(initial_coverage.get("committed_pages", 0)) >= MAX_PAGES:
            return self._hold(owned, checkpoint, state, "ordered_page_budget_exhausted")
        source_identity = f"instagram:listing:{account}:{owned['target_key']}"
        retained = {}

        def observe(payload, metadata=None):
            evidence = Evidence.capture(
                source_identity=source_identity,
                captured_at=self.clock(),
                parser_version=PARSER_VERSION,
                payload=payload,
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
            retained[evidence.content_hash] = (evidence, evidence_id)

        if state.get("grid_ref"):
            evidence = self._read(owned, state["grid_ref"], source_identity)
            evidence_id = state["grid_ref"]["evidence_id"]
            payload = evidence.read(now=self.clock())
        else:
            payload = fetch(
                cursor=cursor,
                pre_source_request=lambda: self.store.check_owner(**owned, record_request=True),
                source_observer=observe,
            )
            if not isinstance(payload, dict):
                raise InvalidSourceError("Ordered listing returned no retained native mapping")
            candidate = Evidence.capture(
                source_identity=source_identity,
                captured_at=self.clock(),
                parser_version=PARSER_VERSION,
                payload=payload,
            )
            if candidate.content_hash not in retained:
                raise InvalidSourceError("Ordered listing native page was not retained before parsing")
            evidence, evidence_id = retained[candidate.content_hash]
            payload = evidence.read(pinned=True)
        page = parse_page(payload, account=account, cursor=cursor)
        if checkpoint.get("ordered_profile") is not None and not identity_matches(
            page.profile, checkpoint["ordered_profile"]
        ):
            raise InvalidSourceError("Ordered page changed its checkpoint's native profile")
        state["grid_ref"] = self._reference(evidence, evidence_id, owned)
        checkpoint = self._save(owned, checkpoint, state)

        dates = []
        cache = DetailCache(self.store.transaction)
        for card in page.cards:
            proof = cache.lookup_listing_card(owned["run_id"], account, card=card, now=self.clock())
            reference = state["date_refs"].get(card.shortcode)
            if proof is None and reference:
                retained_date = self._read(owned, reference, f"instagram:post:{card.shortcode}")
                # Retried hydration can reuse its actual source observation but
                # must not extend the run's frozen freshness horizon.
                age = self.clock() - retained_date.captured_at
                if age.total_seconds() < min(intent.metadata_ttl_days * 86400, intent.metrics_ttl_hours * 3600):
                    proof = date_proof(card, retained_date, reference, account=account)
            if proof is None:
                captured = {}

                def observe_date(kind, raw, *, card=card, captured=captured):
                    detail = Evidence.capture(
                        source_identity=f"instagram:post:{card.shortcode}",
                        captured_at=self.clock(),
                        parser_version=DETAIL_PARSER_VERSION,
                        payload=raw,
                    )
                    detail_id = self.store.retain_evidence(**owned, evidence=detail)
                    captured[detail.content_hash] = (detail, detail_id)

                raw = fetch_detail(
                    shortcode=card.shortcode,
                    check_cancelled=lambda: self.store.check_owner(**owned),
                    on_request=lambda: self.store.check_owner(**owned, record_request=True),
                    source_observer=observe_date,
                )
                if not isinstance(raw, dict):
                    raise InvalidSourceError("Ordered permalink date provider returned no native mapping")
                candidate = Evidence.capture(
                    source_identity=f"instagram:post:{card.shortcode}",
                    captured_at=self.clock(),
                    parser_version=DETAIL_PARSER_VERSION,
                    payload=raw,
                )
                if candidate.content_hash not in captured:
                    raise InvalidSourceError("Permalink date response was not retained before parsing")
                detail, detail_id = captured[candidate.content_hash]
                reference = self._reference(detail, detail_id, owned)
                proof = date_proof(card, detail, reference, account=account)
                state["date_refs"] = dict(state["date_refs"], **{card.shortcode: reference})
                checkpoint = self._save(owned, checkpoint, state)
            dates.append(proof)

        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self.store.check_owner(**owned, connection=conn)
            cur.execute(
                "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
                (owned["run_id"],),
            )
            manifest = cur.fetchone()
            previous_cursor_state = manifest["cursor_state"]
        if previous_cursor_state.get("cursor") != cursor:
            raise Conflict("Ordered manifest cursor changed during hydration")
        previous = previous_cursor_state.get("ordered_v2", {})
        if previous and previous.get("policy") != POLICY:
            raise InvalidSourceError("Ordered manifest has a different frozen coverage policy")
        coverage = window_coverage(intent, page, tuple(dates), previous)
        seen = set(previous.get("native_ids", []))
        native_ids = {card.native_id for card in page.cards}
        if cursor is not None and not native_ids - seen:
            return self._hold(owned, checkpoint, state, "ordered_no_new_native_ids")
        if not page.has_next_page and not coverage["discovery_complete"]:
            state["coverage"] = dict(coverage, committed_pages=int(previous.get("committed_pages", 0)))
            return self._hold(owned, checkpoint, state, "ordered_access_scope_boundary")
        coverage["native_ids"] = sorted(seen | native_ids)
        coverage["new_native_id_count"] = len(native_ids - seen)
        coverage["overlap_native_id_count"] = len(native_ids & seen)
        selected = [
            (card, proof)
            for card, proof in zip(page.cards, dates, strict=True)
            if intent.date_start <= proof.posted_at < intent.date_end
        ]

        def commit_page(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "select discovery_complete,cursor_state from social.instagram_ingestion_manifests "
                    "where run_id=%s and stage='listing' for update",
                    (owned["run_id"],),
                )
                locked = cur.fetchone()
                if not locked or locked["discovery_complete"] or locked["cursor_state"] != previous_cursor_state:
                    raise Conflict("Ordered discovery manifest changed before its atomic commit")
                cur.execute(
                    "select count(*) as count from social.instagram_ingestion_discovery_pages where "
                    "run_id=%s and stage='listing'",
                    (owned["run_id"],),
                )
                if cur.fetchone()["count"] != int(previous.get("committed_pages", 0)):
                    raise Conflict("Ordered committed page count differs from its coverage record")
                if page.end_cursor is not None:
                    cur.execute(
                        "select 1 from social.instagram_ingestion_targets where run_id=%s and stage='listing' "
                        "and checkpoint->>'cursor'=%s limit 1",
                        (owned["run_id"], page.end_cursor),
                    )
                    if cur.fetchone():
                        raise InvalidSourceError("Ordered native cursor was already scheduled")
                identities, targets = [], []
                for card, proof in selected:
                    if proof.canonical_identity is not None:
                        identity = dict(
                            proof.canonical_identity, shortcode=card.shortcode, cache_source=proof.reference
                        )
                    else:
                        normalized = normalize_post_source(proof.source, account=account)
                        source = dict(proof.source)
                        try:
                            detail_snapshot = validate_detail(
                                ParsedSource(
                                    Stage.DETAILS,
                                    card.shortcode,
                                    proof.evidence.captured_at,
                                    proof.source,
                                    post=normalized,
                                )
                            )
                        except InvalidSourceError:
                            pass
                        else:
                            source["detail_snapshot"] = dict(detail_snapshot, evidence_reference=proof.reference)
                        identity = self.writer(
                            conn,
                            run_id=owned["run_id"],
                            account=account,
                            observed_at=proof.evidence.captured_at,
                            normalized=normalized,
                            source_payload=source,
                        )
                    identities.append(identity)
                    for stage in intent.selected_tasks:
                        attempts = {
                            Stage.DETAILS: 3,
                            Stage.COMMENTS: intent.comments_max_attempts,
                            Stage.MEDIA: intent.media_max_attempts,
                        }[stage]
                        targets.append((owned["run_id"], stage.value, card.shortcode, attempts))
                if targets:
                    execute_values(
                        cur,
                        "insert into social.instagram_ingestion_targets (run_id,stage,target_key,max_attempts)"
                        " values %s on conflict do nothing",
                        targets,
                    )
                next_target = None
                if not coverage["discovery_complete"]:
                    next_target = page_target(page.end_cursor)
                    blocked = "ordered_page_budget_exhausted" if coverage["coverage_unresolved"] else None
                    cur.execute(
                        "insert into social.instagram_ingestion_targets "
                        "(run_id,stage,target_key,max_attempts,checkpoint,blocked_reason) "
                        "values(%s,'listing',%s,%s,%s::jsonb,%s)",
                        (
                            owned["run_id"],
                            next_target,
                            intent.listing_max_attempts,
                            stable_json(
                                {
                                    "cursor": page.end_cursor,
                                    "ordered_profile": page.profile,
                                    "ordered_v2": {"policy": POLICY, "date_refs": {}},
                                }
                            ),
                            blocked,
                        ),
                    )
                    if blocked:
                        cur.execute(
                            "update social.instagram_ingestion_runs set state='blocked' where run_id=%s",
                            (owned["run_id"],),
                        )
                cursor_state = {"cursor": page.end_cursor, "last_page": owned["target_key"], "ordered_v2": coverage}
                cur.execute(
                    "update social.instagram_ingestion_manifests set cursor_state=%s::jsonb where "
                    "run_id=%s and stage='listing'",
                    (stable_json(cursor_state), owned["run_id"]),
                )
                cur.execute(
                    "insert into "
                    "social.instagram_ingestion_discovery_pages(run_id,stage,page_id,request_hash) "
                    "values(%s,'listing',%s,%s)",
                    (owned["run_id"], owned["target_key"], evidence.content_hash),
                )
                if coverage["discovery_complete"]:
                    from trr_backend.ingestion.profiles import requested

                    profile_work = requested(intent, run["workflow_version"])
                    sealed = [
                        Stage.LISTING.value,
                        *(stage.value for stage in intent.selected_tasks if stage != Stage.MEDIA or not profile_work),
                    ]
                    if profile_work:
                        cur.execute(
                            "update social.instagram_ingestion_manifests set cursor_state=cursor_state || "
                            '\'{"commenter_profiles":{"version":1,"state":"enumerating"}}\'::jsonb where run_id=%s '
                            "and stage='media' and not discovery_complete",
                            (owned["run_id"],),
                        )
                    cur.execute(
                        "update social.instagram_ingestion_manifests set "
                        "discovery_complete=true,sealed_at=clock_timestamp() where run_id=%s and stage=any(%s)",
                        (owned["run_id"], sealed),
                    )
                return {
                    "page_id": owned["target_key"],
                    "post_keys": sorted(card.shortcode for card, _ in selected),
                    "posts": identities,
                    "next_target_key": next_target,
                    "discovery_complete": coverage["discovery_complete"],
                    "ordered_v2": coverage,
                    "date_evidence": {
                        card.shortcode: proof.reference for card, proof in zip(page.cards, dates, strict=True)
                    },
                }

        return self.store.commit_target(
            **owned,
            observed_at=evidence.captured_at,
            evidence_id=evidence_id,
            write_canonical=commit_page,
            checkpoint=checkpoint,
        )

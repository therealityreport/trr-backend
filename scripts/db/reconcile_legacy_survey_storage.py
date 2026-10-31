#!/usr/bin/env python3
"""Reconcile legacy Firebase surveys without replacing PostgreSQL submissions.

Default: read-only dry-run. Writes require --execute and explicit provider targets.
Only aggregate output is emitted. Firestore originals are never changed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import psycopg2
from dotenv import load_dotenv
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.protobuf.timestamp_pb2 import Timestamp as ProtobufTimestamp
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from trr_backend.db.connection import resolve_database_url
from trr_backend.services import survey_submissions as service

FIREBASE_PROJECT = "trr-web-25d2e"
POSTGRES_PROJECT = "vwxfvzutyufrkhfgoeaa"
EPISODE_PATH = re.compile(r"^surveyResults/([^/]+)/seasons/([^/]+)/episodes/([^/]+)/responses/([^/]+)$")


class ReconciliationError(Exception):
    """Safe error code; never include user data or credential-bearing errors."""


class InsertRaceError(ReconciliationError):
    pass


@dataclass
class Candidate:
    source_path: str
    source_version: datetime
    kind: str
    uid: str
    payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime


def timestamp(value: Any) -> datetime:
    # Collection-group snapshots expose protobuf update_time metadata, unlike
    # ordinary document snapshots. Both represent exact UTC instants.
    if isinstance(value, ProtobufTimestamp):
        return DatetimeWithNanoseconds.from_timestamp_pb(value)
    if isinstance(value, DatetimeWithNanoseconds):
        return DatetimeWithNanoseconds.from_timestamp_pb(value.timestamp_pb())
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ReconciliationError("missing_source_timestamp")
    return value.astimezone(UTC)


def source_revision(value: Any) -> ProtobufTimestamp:
    normalized = timestamp(value)
    if isinstance(normalized, DatetimeWithNanoseconds):
        return normalized.timestamp_pb()
    result = ProtobufTimestamp()
    result.FromDatetime(normalized)
    return result


def candidate_from_snapshot(snapshot: Any) -> Candidate | None:
    data = snapshot.to_dict() or {}
    path = snapshot.reference.path
    if path.startswith("users/") and len(path.split("/")) == 2:
        answer = data.get("surveyX")
        if not isinstance(answer, dict) or answer.get("completed") is not True:
            return None
        uid = snapshot.id
        if data.get("uid", uid) != uid:
            raise ReconciliationError("source_identity_mismatch")
        responses = answer.get("responses")
        if not isinstance(responses, dict) or set(responses) - service.X_FIELDS.keys():
            raise ReconciliationError("unsupported_source_answers")
        completed_at = timestamp(answer.get("completedAt"))
        return Candidate(
            path,
            timestamp(snapshot.update_time),
            "survey_x",
            uid,
            {"responses": responses, "username": data.get("username")},
            completed_at,
            completed_at,
        )
    match = EPISODE_PATH.fullmatch(path)
    if not match or data.get("completed") is not True:
        return None
    show, season, episode, uid = match.groups()
    key = (
        "rhoslc_s6"
        if show == "tt12623782" and season == "S06"
        else "rhop_s10"
        if show == "rhop" and season == "S10"
        else None
    )
    if key is None:
        raise ReconciliationError("unknown_survey_scope")
    for field, expected in {"uid": uid, "showId": show, "seasonId": season, "episodeId": episode}.items():
        if data.get(field, expected) != expected:
            raise ReconciliationError("source_identity_mismatch")
    if data.get("completionPct") != 100 or data.get("syncState") in {"pending", "error"}:
        raise ReconciliationError("source_not_confirmed_complete")
    answers = {"ranking": data.get("ranking"), "completed": True}
    if "seasonRating" in data:
        answers["season_rating"] = data["seasonRating"]
    created_at, updated_at = timestamp(data.get("createdAt")), timestamp(data.get("updatedAt"))
    if updated_at < created_at:
        raise ReconciliationError("invalid_source_timestamp_order")
    return Candidate(
        path,
        timestamp(snapshot.update_time),
        key,
        uid,
        {
            "surveyKey": key,
            "appUserId": uid,
            "showId": show,
            "seasonId": season,
            "episodeId": episode,
            "seasonNumber": int(season[1:]),
            "episodeNumber": int(re.sub(r"\D", "", episode)),
            "responses": answers,
        },
        created_at,
        updated_at,
    )


def prepare_rows(candidate: Candidate, user: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Use current submission validation, capturing prepared rows with no SQL capability.

    This standalone, single-threaded import process never runs alongside an API
    server. Replacing the service's DB object prevents validation from connecting.
    """
    if user.get("uid") != candidate.uid:
        raise ReconciliationError("verified_identity_mismatch")
    prepared: list[tuple[str, dict[str, Any]]] = []

    def capture(key, owner, answers, username=None):
        row = {
            "app_user_id": owner["uid"],
            "respondent_id": owner["uid"],
            "app_user_email": owner.get("email"),
            "source": "trr_app",
            **answers,
            "created_at": candidate.created_at,
            "updated_at": candidate.updated_at,
        }
        if isinstance(username, str):
            row["app_username"] = username
        prepared.append((key, row))
        return row

    try:
        with (
            patch.object(service, "pg", SimpleNamespace(transaction=nullcontext)),
            patch.object(service, "_identity", lambda _user: None),
            patch.object(service, "_upsert", capture),
        ):
            operation = service.submit_x if candidate.kind == "survey_x" else service.submit_show
            operation(user, candidate.payload)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ReconciliationError("source_validation_failed") from exc
    digest = hashlib.sha256(json.dumps(candidate.payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    for _key, row in prepared:
        row["extra"] = Json(
            {
                "legacy_import": {
                    "version": 1,
                    "provider": "firestore",
                    "project": FIREBASE_PROJECT,
                    "path": candidate.source_path,
                    "source_updated_at": source_revision(candidate.source_version).ToJsonString(),
                    "payload_sha256": digest,
                },
                "legacy_answers": candidate.payload["responses"],
            }
        )
    return prepared


def key_columns(key: str) -> list[str]:
    return ["app_user_id"] if key in {"survey_x", "global_profile"} else ["app_user_id", "season_id", "episode_id"]


def existing_state(cursor, key: str, row: dict[str, Any]) -> str:
    columns = key_columns(key)
    cursor.execute(
        sql.SQL("SELECT * FROM {} WHERE {}").format(
            sql.Identifier("public", service.TABLES[key]),
            sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(column)) for column in columns),
        ),
        [row[column] for column in columns],
    )
    found = cursor.fetchone()
    if found is None:
        return "absent"
    # Only semantic answers participate; presentation metadata may have changed.
    if key in {"rhoslc_s6", "rhop_s10"}:
        ranking = row["ranking"].adapted
        same = [item["id"] for item in found.get("ranking") or []] == [item["id"] for item in ranking]
        if "season_rating" in row:
            same = (
                same
                and (None if found.get("season_rating") is None else float(found["season_rating"]))
                == row["season_rating"]
            )
        return "matching" if same and found.get("completed") is True else "conflict"
    answer_columns = set(service.X_FIELDS.values()) if key == "survey_x" else set(service.PROFILE_VIEW_FIELDS.values())
    for column in answer_columns & row.keys():
        value = row[column].adapted if isinstance(row[column], Json) else row[column]
        if found.get(column) != value:
            return "conflict"
    return "matching"


def reconcile_candidate(connection, prepared: list[tuple[str, dict[str, Any]]], *, execute: bool) -> str:
    """One candidate transaction; an X/profile pair is all-or-nothing even on races."""
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '10s'")
                cursor.execute("SET LOCAL lock_timeout = '3s'")
                if not execute:
                    cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute("SELECT set_config('app.firebase_uid', %s, true)", [prepared[0][1]["app_user_id"]])
                states = [existing_state(cursor, key, row) for key, row in prepared]
                if any(state == "conflict" for state in states):
                    return "existing_conflict_preserved"
                if any(state != "absent" for state in states):
                    return (
                        "existing_matching_preserved"
                        if all(state == "matching" for state in states)
                        else "existing_partial_pair_preserved"
                    )
                if not execute:
                    return "eligible"
                for key, row in prepared:
                    columns = list(row)
                    cursor.execute(
                        sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING RETURNING id").format(
                            sql.Identifier("public", service.TABLES[key]),
                            sql.SQL(", ").join(map(sql.Identifier, columns)),
                            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                            sql.SQL(", ").join(map(sql.Identifier, key_columns(key))),
                        ),
                        [row[column] for column in columns],
                    )
                    if cursor.fetchone() is None:
                        raise InsertRaceError("concurrent_existing_row_preserved")
                return "inserted"
    except InsertRaceError:
        return "concurrent_existing_row_preserved"


def verify_targets(firebase_project: str, postgres_project: str, credentials: dict, dsn: str) -> None:
    if firebase_project != FIREBASE_PROJECT or postgres_project != POSTGRES_PROJECT:
        raise ReconciliationError("unexpected_requested_target")
    if credentials.get("project_id") != firebase_project:
        raise ReconciliationError("firebase_credential_target_mismatch")
    if (
        os.getenv("FIRESTORE_EMULATOR_HOST")
        or os.getenv("FIREBASE_AUTH_EMULATOR_HOST")
        or os.getenv("NEXT_PUBLIC_USE_FIREBASE_EMULATORS", "").lower() == "true"
    ):
        raise ReconciliationError("emulator_target_forbidden")
    parsed = urlsplit(dsn)
    host = parsed.hostname or ""
    username = unquote(parsed.username or "")
    direct = host == f"db.{postgres_project}.supabase.co" and username == "postgres"
    pooled = host.endswith(".pooler.supabase.com") and username == f"postgres.{postgres_project}"
    if not (direct or pooled):
        raise ReconciliationError("postgres_credential_target_mismatch")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--firebase-project", required=True)
    parser.add_argument("--postgres-project", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    counts: Counter[str] = Counter()
    connection = app = None
    try:
        if not 1 <= args.limit <= 500:
            raise ReconciliationError("invalid_read_limit")
        if not args.env_file.is_file():
            raise ReconciliationError("missing_environment_file")
        load_dotenv(args.env_file, override=False)
        credentials = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
        dsn = resolve_database_url()
        verify_targets(args.firebase_project, args.postgres_project, credentials, dsn)
        import firebase_admin
        from firebase_admin import auth, firestore
        from firebase_admin import credentials as firebase_credentials

        app = firebase_admin.initialize_app(
            firebase_credentials.Certificate(credentials), {"projectId": args.firebase_project}
        )
        firestore_db = firestore.client(app)
        if firestore_db.project != args.firebase_project:
            raise ReconciliationError("active_firestore_target_mismatch")
        connection = psycopg2.connect(dsn, connect_timeout=10, application_name="trr-survey-legacy-reconcile")
        connection.set_session(readonly=not args.execute)
        sources = [
            firestore_db.collection("users").select(["uid", "username", "surveyX"]),
            firestore_db.collection_group("responses"),
        ]
        snapshots = []
        for query in sources:
            batch = list(query.limit(args.limit + 1).stream(timeout=15))
            if len(batch) > args.limit:
                raise ReconciliationError("read_limit_exceeded")
            snapshots.extend(batch)
        for snapshot in snapshots:
            try:
                candidate = candidate_from_snapshot(snapshot)
                if candidate is None:
                    continue
                counts["candidates"] += 1
                try:
                    owner = auth.get_user(candidate.uid, app=app)
                except auth.UserNotFoundError:
                    raise ReconciliationError("firebase_owner_not_found") from None
                if owner.disabled:
                    raise ReconciliationError("firebase_owner_disabled")
                prepared = prepare_rows(candidate, {"uid": owner.uid, "email": owner.email})
                # Recheck the source revision immediately before the PG transaction.
                if source_revision(snapshot.reference.get(timeout=15).update_time) != source_revision(
                    candidate.source_version
                ):
                    raise ReconciliationError("source_changed_since_read")
                outcome = reconcile_candidate(connection, prepared, execute=args.execute)
                counts[f"{candidate.kind}:{outcome}"] += 1
            except ReconciliationError as exc:
                counts[f"skipped:{exc}"] += 1
        print(
            json.dumps(
                {
                    "mode": "execute" if args.execute else "dry-run",
                    "checked_at": datetime.now(UTC).isoformat(),
                    "firebase_project": args.firebase_project,
                    "postgres_project": args.postgres_project,
                    "counts": dict(counts),
                    "firestore_writes": 0,
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc, ReconciliationError) else "credential_or_read_operation_failed"
        print(json.dumps({"status": "blocked", "code": code, "counts": dict(counts)}))
        return 1
    finally:
        if connection is not None:
            connection.close()
        if app is not None:
            import firebase_admin

            firebase_admin.delete_app(app)


if __name__ == "__main__":
    raise SystemExit(main())

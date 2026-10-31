"""Copy and switch DB-owned originals using the existing operator ledger.

There is intentionally no object deletion or bucket administration operation here.
"""

from __future__ import annotations

import hashlib
from typing import Any

from botocore.exceptions import ClientError
from psycopg2.extras import Json

from trr_backend.db import pg
from trr_backend.video_storage import private_source_json, source_reference

TABLES = {
    "ml_analysis_media_assets": "ml.analysis_media_assets",
    "screenalytics_video_assets": "screenalytics.video_assets",
}


def references(source_bucket: str) -> list[dict[str, Any]]:
    result = []
    for kind, table in TABLES.items():
        for row in pg.fetch_all(f"SELECT id, source_url, source_json FROM {table}"):
            bucket, key = source_reference(row)
            if bucket == source_bucket:
                if not key.startswith("source/"):
                    raise ValueError("Refusing unclassified video outside source/ prefix")
                result.append({**row, "reference_kind": kind, "object_key": key})
    return result


def hash_object(client: Any, bucket: str, key: str, *, etag: str | None = None) -> tuple[int, str]:
    params = {"Bucket": bucket, "Key": key}
    if etag:
        params["IfMatch"] = etag
    body = client.get_object(**params)["Body"]
    digest, size = hashlib.sha256(), 0
    try:
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    finally:
        body.close()
    return size, digest.hexdigest()


def inventory(client: Any, source_bucket: str, destination_bucket: str) -> list[dict[str, Any]]:
    if source_bucket == destination_bucket:
        raise ValueError("Source and destination buckets must differ")
    grouped: dict[str, dict[str, Any]] = {}
    for ref in references(source_bucket):
        key = ref["object_key"]
        if key not in grouped:
            head = client.head_object(Bucket=source_bucket, Key=key)
            size, sha = hash_object(client, source_bucket, key, etag=head["ETag"])
            if size != head["ContentLength"]:
                raise RuntimeError("Source size changed during inventory")
            grouped[key] = {
                "key": key,
                "head": head,
                "size": size,
                "sha256": sha,
                "references": [],
            }
        grouped[key]["references"].append(ref)
    return list(grouped.values())


def register(source: str, destination: str, objects: list[dict[str, Any]], *, provider: str) -> str:
    """Persist a fresh inventory and all old values atomically before copying."""
    with pg.transaction():
        inv = pg.execute_returning(
            "INSERT INTO admin.object_storage_inventory_runs "
            "(provider,bucket,object_prefix,status,object_count,total_bytes,hashed_count,started_at,completed_at) "
            "VALUES (%s,%s,'source/','complete',%s,%s,%s,now(),now()) RETURNING id,scan_generation",
            (provider, source, len(objects), sum(o["size"] for o in objects), len(objects)),
        )[0]
        run = pg.execute_returning(
            "INSERT INTO admin.private_video_object_migration_runs "
            "(inventory_run_id,source_bucket,destination_bucket,discovered_count,reference_count) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING id",
            (inv["id"], source, destination, len(objects), sum(len(o["references"]) for o in objects)),
        )[0]
        for obj in objects:
            head = obj["head"]
            item = pg.execute_returning(
                "INSERT INTO admin.object_storage_inventory_items "
                "(run_id,scan_generation,bucket,object_key,size_bytes,last_modified,"
                "etag,sha256,content_type,hash_status) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'hashed') RETURNING id",
                (
                    inv["id"],
                    inv["scan_generation"],
                    source,
                    obj["key"],
                    obj["size"],
                    head["LastModified"],
                    head["ETag"],
                    obj["sha256"],
                    head.get("ContentType"),
                ),
            )[0]
            ledger = pg.execute_returning(
                "INSERT INTO admin.private_video_object_migration_items "
                "(run_id,inventory_item_id,source_bucket,destination_bucket,object_key,expected_size_bytes,"
                "source_last_modified,source_etag,expected_sha256) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (
                    run["id"],
                    item["id"],
                    source,
                    destination,
                    obj["key"],
                    obj["size"],
                    head["LastModified"],
                    head["ETag"],
                    obj["sha256"],
                ),
            )[0]
            for ref in obj["references"]:
                new_json = private_source_json(
                    destination,
                    obj["key"],
                    **{k: v for k, v in (ref["source_json"] or {}).items() if k not in {"bucket", "object_key"}},
                )
                new_json["checksum_sha256"] = obj["sha256"]
                new_json.setdefault("original_source_url", ref["source_url"])
                pg.execute(
                    "INSERT INTO admin.private_video_object_migration_references "
                    "(item_id,reference_kind,row_id,old_source_url,new_source_url,old_source_json,new_source_json) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        ledger["id"],
                        ref["reference_kind"],
                        ref["id"],
                        ref["source_url"],
                        f"s3://{destination}/{obj['key']}",
                        Json(ref["source_json"] or {}),
                        Json(new_json),
                    ),
                )
        return str(run["id"])


def copy_verified(client: Any, item: dict[str, Any]) -> dict[str, Any]:
    source, destination, key = item["source_bucket"], item["destination_bucket"], item["object_key"]
    head = client.head_object(Bucket=source, Key=key)
    if (
        head["ETag"] != item["source_etag"]
        or head["ContentLength"] != item["expected_size_bytes"]
        or head["LastModified"] != item["source_last_modified"]
    ):
        raise RuntimeError("Source changed since inventory; reconcile before copying")
    try:
        client.head_object(Bucket=destination, Key=key)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code")) not in {"404", "NoSuchKey", "NotFound"}:
            raise
        if item["expected_size_bytes"] > 5 * 1024**3:
            raise ValueError("Object requires multipart copy; hold this batch") from exc
        client.copy_object(
            Bucket=destination,
            Key=key,
            CopySource={"Bucket": source, "Key": key},
            CopySourceIfMatch=item["source_etag"],
        )
    # An existing destination is never overwritten. Verify before either switching or resuming.
    dest = client.head_object(Bucket=destination, Key=key)
    size, sha = hash_object(client, destination, key, etag=dest["ETag"])
    if size != item["expected_size_bytes"] or sha != item["expected_sha256"]:
        raise RuntimeError("Destination content mismatch; no references switched")
    return {"size": size, "sha256": sha, "etag": dest["ETag"]}


def switch_references(item: dict[str, Any]) -> None:
    with pg.transaction():
        refs = pg.fetch_all(
            "SELECT * FROM admin.private_video_object_migration_references WHERE item_id=%s ORDER BY id FOR UPDATE",
            (item["id"],),
        )
        for ref in refs:
            if ref["status"] == "db_updated":
                continue
            table = TABLES[ref["reference_kind"]]
            changed = pg.execute_returning(
                f"UPDATE {table} SET source_url=%s, source_json=%s WHERE id=%s "
                "AND source_url IS NOT DISTINCT FROM %s AND source_json IS NOT DISTINCT FROM %s RETURNING id",
                (
                    ref["new_source_url"],
                    Json(ref["new_source_json"]),
                    ref["row_id"],
                    ref["old_source_url"],
                    Json(ref["old_source_json"]),
                ),
            )
            if len(changed) != 1:
                raise RuntimeError("Video reference changed since inventory; batch rolled back")
            pg.execute(
                "UPDATE admin.private_video_object_migration_references "
                "SET status='db_updated',database_updated_at=now(),updated_at=now() WHERE id=%s",
                (ref["id"],),
            )
        pg.execute(
            "UPDATE admin.private_video_object_migration_items "
            "SET status='db_updated',database_updated_at=now(),updated_at=now() WHERE id=%s",
            (item["id"],),
        )


def execute_run(client: Any, run_id: str, destination: str) -> dict[str, Any]:
    run = pg.fetch_one("SELECT * FROM admin.private_video_object_migration_runs WHERE id=%s", (run_id,))
    if not run or run["destination_bucket"] != destination or run["source_bucket"] == destination:
        raise ValueError("Migration target mismatch")
    pg.execute(
        "UPDATE admin.private_video_object_migration_runs SET status='running',started_at=now() WHERE id=%s", (run_id,)
    )
    items = pg.fetch_all(
        "SELECT * FROM admin.private_video_object_migration_items WHERE run_id=%s ORDER BY id", (run_id,)
    )
    for item in items:
        if item["status"] == "db_updated":
            continue
        try:
            verified = copy_verified(client, item)
            pg.execute(
                "UPDATE admin.private_video_object_migration_items SET status='verified',"
                "destination_size_bytes=%s,destination_sha256=%s,destination_etag=%s,copied_at=now(),"
                "attempt_count=attempt_count+1,error_message=NULL,updated_at=now() WHERE id=%s",
                (verified["size"], verified["sha256"], verified["etag"], item["id"]),
            )
            switch_references(item)
        except Exception:
            pg.execute(
                "UPDATE admin.private_video_object_migration_runs SET status='failed',"
                "error_message='copy or reference verification failed',updated_at=now() WHERE id=%s",
                (run_id,),
            )
            raise
    try:
        remaining = references(run["source_bucket"])
        if remaining:
            raise RuntimeError("Source references remain or appeared concurrently; do not retire originals")
    except Exception:
        pg.execute(
            "UPDATE admin.private_video_object_migration_runs SET status='failed',"
            "error_message='final reference reconciliation failed',updated_at=now() WHERE id=%s",
            (run_id,),
        )
        raise
    pg.execute(
        "UPDATE admin.private_video_object_migration_runs SET status='complete',migrated_count=%s,"
        "completed_at=now(),updated_at=now() WHERE id=%s",
        (len(items), run_id),
    )
    return {"run_id": run_id, "objects_verified": len(items), "source_objects_deleted": 0}

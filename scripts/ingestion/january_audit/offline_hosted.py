"""Pure fixtures for the task's full-object reader; no live connection allowed."""

import copy
import hashlib
import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

from . import hosted as audit

BASE = "https://media.thereality.report"
AT = "2026-10-23T03:00:00+00:00"
POST = "11111111-1111-4111-8111-111111111111"
CANON = "22222222-2222-4222-8222-222222222222"
EVIDENCE = "33333333-3333-4333-8333-333333333333"
ASSET = "44444444-4444-4444-8444-444444444444"


def png():
    value = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(value, format="PNG")
    return value.getvalue()


def report():
    entry = {
        "url": BASE + "/social/a.png",
        "storage_key": "social/a.png",
        "role": "cover_original",
        "shortcode": "ABC",
        "expected_bytes": None,
        "historical_sha256": None,
        "format": "png",
        "content_type": "image/png",
        "width": 2,
        "height": 2,
        "original_observed_at": AT,
        "original_post_observed_at": AT,
        "source_evidence_id": EVIDENCE,
        "source_evidence_sha256": "a" * 64,
        "source_evidence_parser": "instagram-media-result-v1",
        "source_evidence_schema": 1,
        "required_decode_mode": "image_full_decode",
        "native_gif": False,
        "required_animation": None,
        "legacy_post_id": POST,
        "canonical_post_id": CANON,
        "canonical_asset_id": ASSET,
        "media_reservation": {"owner_token": EVIDENCE, "owner_generation": 2},
        "source_reference": "https://source.invalid/a.png",
    }
    verified = {
        "shortcode": "ABC",
        "source_evidence_id": EVIDENCE,
        "source_hash": "a" * 64,
        "object_roles": 1,
        "original_observed_at": AT,
        "record_identity": {k: entry[k] for k in ("legacy_post_id", "canonical_post_id", "media_reservation")},
        "object_role_sha256": {"cover_original": audit.digest(entry)},
        "avatar_slots": [],
        "comment_binding": {
            "post_id": POST,
            "media_reservation": entry["media_reservation"],
            "observed_comment_ids": [],
        },
    }
    return {
        "operator_scope": "media_records_and_byte_plan_not_final_certifier",
        "run_id": audit.common.RUN,
        "intent_hash": audit.common.INTENT,
        "full_history_gate_closed": True,
        "status": "passed",
        "failures": [],
        "pending": [],
        "verified_targets": 1,
        "audited_january_count": 1,
        "verified": [verified],
        "byte_read_plan": {"status": "ready_for_verified_subset", "physical_urls": 1, "objects": [entry]},
    }


class Body:
    def __init__(self, data):
        self.data = io.BytesIO(data)
        self.decode_content = None

    def read(self, size):
        return self.data.read(size)

    def close(self):
        self.data.close()


class Client:
    def __init__(self, data, content_type="image/png"):
        self.data = data
        self.content_type = content_type
        self.calls = 0

    def get_object(self, **kwargs):
        assert kwargs == {"Bucket": "trr-media-prod", "Key": "social/a.png"}
        self.calls += 1
        return {
            "Body": Body(self.data),
            "ContentLength": len(self.data),
            "ContentType": self.content_type,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }


class Response:
    def __init__(self, data, content_type="image/png", status=200):
        self.raw = Body(data)
        self.status_code = status
        self.headers = {"Content-Length": str(len(data)), "Content-Type": content_type}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.raw.close()


class Session:
    response = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get(self, url, **kwargs):
        assert url == BASE + "/social/a.png" and kwargs["stream"] and not kwargs["allow_redirects"]
        return self.response


class Offline(unittest.TestCase):
    def setUp(self):
        self.guard = patch("socket.create_connection", side_effect=AssertionError("network_forbidden"))
        self.guard.start()
        self.db = patch("psycopg2.connect", side_effect=AssertionError("database_forbidden"))
        self.db.start()
        self.tmp = tempfile.TemporaryDirectory(prefix="trr-byte-fixture-")
        self.path = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()
        self.db.stop()
        self.guard.stop()

    def test_plan_positive(self):
        self.assertEqual(len(audit.physical_plan(report(), BASE)), 1)

    def test_url_boundaries(self):
        for value in (
            "https://other.invalid/social/a.png",
            BASE + "/social/../a.png",
            BASE + "/social/%2e%2e/a.png",
            BASE + "/social/a.png?token=x",
        ):
            with self.subTest(value=value), self.assertRaises(audit.common.GuardError):
                audit.object_key(value, None, BASE)

    def test_omissions_duplicates_counts_and_readiness(self):
        mutations = [
            lambda r: r["byte_read_plan"]["objects"].clear(),
            lambda r: r["verified"].append(copy.deepcopy(r["verified"][0])),
            lambda r: r["byte_read_plan"]["objects"].append(copy.deepcopy(r["byte_read_plan"]["objects"][0])),
            lambda r: r["verified"][0].update(object_roles=2),
            lambda r: r.update(audited_january_count=2),
            lambda r: r["byte_read_plan"].update(status="pending"),
            lambda r: r["byte_read_plan"].update(physical_urls=2),
            lambda r: r["pending"].append({"shortcode": "missing"}),
            lambda r: r["verified"][0]["object_role_sha256"].clear(),
        ]
        for mutate in mutations:
            r = report()
            mutate(r)
            with self.subTest(mutate=mutate), self.assertRaises(audit.common.GuardError):
                audit.physical_plan(r, BASE)

    def test_provenance_tampering(self):
        changes = {
            "source_evidence_id": ASSET,
            "source_evidence_parser": "other",
            "source_evidence_schema": 2,
            "legacy_post_id": CANON,
            "canonical_post_id": POST,
            "original_post_observed_at": "2020-01-01T00:00:00+00:00",
            "original_observed_at": "2020-01-01T00:00:00+00:00",
            "native_gif": True,
        }
        for field, value in changes.items():
            r = report()
            entry = r["byte_read_plan"]["objects"][0]
            entry[field] = value
            r["verified"][0]["object_role_sha256"]["cover_original"] = audit.digest(entry)
            with self.subTest(field=field), self.assertRaises(audit.common.GuardError):
                audit.physical_plan(r, BASE)

    def test_body_full_length_and_cap(self):
        with self.assertRaises(audit.common.GuardError):
            audit.copy_body(io.BytesIO(b"ab").read, self.path / "truncated", 3)
        # Large objects stream to disk; only claimed length bounds them.
        body = b"x" * (51 * 1024 * 1024)
        self.assertEqual(audit.copy_body(io.BytesIO(body).read, self.path / "large", len(body))[0], len(body))
        n, h = audit.copy_body(io.BytesIO(b"abc").read, self.path / "good", 3)
        self.assertEqual((n, h), (3, hashlib.sha256(b"abc").hexdigest()))

    def test_native_gif_plan_binding_and_downgrade(self):
        r = report()
        e = r["byte_read_plan"]["objects"][0]
        gif = "https://cdn.fbsbx.com/fixture.gif"
        e.update(
            role="comment:123:1",
            comment_id="123",
            comment_row_id=ASSET,
            source_reference=gif,
            storage_key="social/g.gif",
            url=BASE + "/social/g.gif",
            format="gif",
            content_type="image/gif",
            required_decode_mode="gif_all_frames_native_identity",
            native_gif=True,
            required_animation="all_frames_native_identity_no_unavailable_placeholder",
            native_gif_proof={
                "kind": "native_giphy",
                "native_gif": True,
                "native_gif_id": "456",
                "giphy_media_info": {"id": "456", "images": {"fixed_height": {"url": gif}}},
                "row_id": ASSET,
                "comment_id": "123",
                "post_id": POST,
                "parent_comment_id": None,
            },
        )
        v = r["verified"][0]
        v["object_role_sha256"] = {e["role"]: audit.digest(e)}
        v["comment_binding"]["observed_comment_ids"] = ["123"]
        self.assertEqual(len(audit.physical_plan(r, BASE)), 1)
        for mutate in (
            lambda x: x["native_gif_proof"]["giphy_media_info"]["images"]["fixed_height"].update(
                url="https://cdn.fbsbx.com/wrong.gif"
            ),
            lambda x: x["native_gif_proof"].update(row_id=POST),
            lambda x: x.update(native_gif=False, required_decode_mode="image_full_decode"),
        ):
            changed = copy.deepcopy(r)
            entry = changed["byte_read_plan"]["objects"][0]
            mutate(entry)
            changed["verified"][0]["object_role_sha256"][entry["role"]] = audit.digest(entry)
            with self.subTest(mutate=mutate), self.assertRaises(audit.common.GuardError):
                audit.physical_plan(changed, BASE)

    def inspect(self, data=None, cdn=None, storage_type="image/png", cdn_type="image/png", status=200, historical=None):
        data = png() if data is None else data
        cdn = data if cdn is None else cdn
        item = audit.physical_plan(report(), BASE)[0]
        item["historical_sha256"] = historical
        Session.response = Response(cdn, cdn_type, status)
        with patch("requests.Session", Session):
            return audit.inspect_object(item, Client(data, storage_type), SimpleNamespace(bucket="trr-media-prod"))

    def test_storage_and_cdn_full_positive(self):
        r = self.inspect()
        self.assertEqual(r["status"], "verified")
        self.assertTrue(r["decode"]["all_frames_decoded"])
        self.assertEqual(r["original_hash_comparison"], "historical_hash_unavailable_current_bytes_only")

    def test_delivery_and_hash_negatives(self):
        for kw in (
            {"cdn": png()[:-1]},
            {"storage_type": "text/html", "cdn_type": "text/html"},
            {"cdn_type": "image/jpeg"},
            {"status": 206},
            {"historical": "0" * 64},
        ):
            with self.subTest(kw=kw):
                self.assertEqual(self.inspect(**kw)["status"], "failed")

    def test_retained_hash_match(self):
        self.assertEqual(self.inspect(historical=hashlib.sha256(png()).hexdigest())["status"], "verified")

    def test_native_gif_all_frames_and_static_rejection(self):
        frames = [Image.new("RGB", (2, 2), color) for color in ("red", "blue")]
        path = self.path / "animated.gif"
        frames[0].save(path, save_all=True, append_images=frames[1:], duration=[40, 80], loop=0)
        m = {"required_decode_mode": "gif_all_frames_native_identity", "format": "gif", "width": 2, "height": 2}
        r = audit.decode_file(path, m)
        self.assertEqual(r["frame_count"], 2)
        self.assertEqual(r["frame_durations_ms"], [40, 80])
        frames[0].save(self.path / "static.gif")
        with self.assertRaises(audit.common.GuardError):
            audit.decode_file(self.path / "static.gif", m)

    def test_video_full_and_still_playlist_rejection(self):
        path = self.path / "clip.mp4"
        subprocess.run(
            [
                "/opt/homebrew/bin/ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=red:s=16x16:r=5",
                "-t",
                "0.4",
                "-an",
                "-c:v",
                "mpeg4",
                str(path),
            ],
            check=True,
            capture_output=True,
        )
        metadata = {
            "required_decode_mode": "video_full_stream",
            "format": "mp4",
            "storage_key": "clip.mp4",
            "width": 16,
            "height": 16,
        }
        result = audit.decode_file(path, metadata)
        self.assertGreater(result["decoded_video_frames"], 0)
        for name, raw in (("still.mp4", png()), ("playlist.mp4", b"#EXTM3U\nhttps://example.invalid/private\n")):
            p = self.path / name
            p.write_bytes(raw)
            with self.subTest(name=name), self.assertRaises(audit.common.GuardError):
                audit.decode_file(p, metadata)


if __name__ == "__main__":
    unittest.main()

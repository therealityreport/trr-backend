from __future__ import annotations

from trr_backend.media import s3_mirror


def test_mirror_show_image_row_prefers_joined_imdb_id_for_hosted_key(monkeypatch) -> None:
    monkeypatch.setattr(s3_mirror, "download_image", lambda *_args, **_kwargs: (b"img", "image/jpeg"))
    monkeypatch.setattr(s3_mirror, "_head_object", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(s3_mirror, "upload_bytes_to_s3", lambda *_args, **_kwargs: ("etag", 3))
    monkeypatch.setattr(s3_mirror, "get_s3_bucket", lambda: "bucket")
    monkeypatch.setattr(s3_mirror, "get_s3_client", lambda: object())
    monkeypatch.setattr(s3_mirror, "build_hosted_url", lambda key: f"https://cdn.test/{key}")

    patch = s3_mirror.mirror_show_image_row(
        {
            "show_id": "show-uuid-1",
            "shows": {"imdb_id": "tt1234567"},
            "source": "tmdb",
            "kind": "poster",
            "file_path": "/poster.jpg",
            "hosted_url": None,
            "hosted_key": None,
            "hosted_sha256": None,
        }
    )

    assert patch is not None
    assert patch["hosted_key"].startswith("images/shows/tt1234567/poster/tmdb/")

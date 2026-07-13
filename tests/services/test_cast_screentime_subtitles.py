from __future__ import annotations

import io
import json

import pytest

from trr_backend.services import cast_screentime_subtitles as subtitles


def test_normalize_subtitle_language_recognizes_supported_english_tags():
    assert subtitles.normalize_subtitle_language("eng") == "en"
    assert subtitles.normalize_subtitle_language("en-US") == "en"
    assert subtitles.normalize_subtitle_language("EN_AU") == "en"
    assert subtitles.normalize_subtitle_language("spa") == "spa"
    assert subtitles.normalize_subtitle_language(None) is None


def test_parse_and_normalize_srt_preserves_speaker_and_sdh_text(monkeypatch):
    monkeypatch.delenv("CAST_SCREENTIME_SUBTITLE_MAX_SRT_BYTES", raising=False)
    raw = (
        b"8\r\n00:00:14.448 --> 00:00:16.984\r\n"
        b"<i>JEFF PROBST:</i> Welcome.\r\n[ music plays ]\r\n\r\n"
        b"11\r\n00:00:16,984 --> 00:00:17,500\r\n(laughs)\r\n"
    )

    normalized, cues = subtitles.parse_and_normalize_srt(raw)

    assert normalized.startswith(b"1\n00:00:14,448 --> 00:00:16,984\n")
    assert normalized.endswith(b"(laughs)\n\n")
    assert cues[0]["text"] == "<i>JEFF PROBST:</i> Welcome.\n[ music plays ]"
    assert cues[0]["plain_text"] == "JEFF PROBST: Welcome.\n[ music plays ]"
    assert cues[-1]["end_ms"] == 17_500


@pytest.mark.parametrize(
    "raw,error",
    [
        (b"1\n00:00:03,000 --> 00:00:02,000\nNo\n", "end_before_start"),
        (b"1\nnot a timestamp\nNo\n", "malformed_timestamp"),
        (b"", "empty"),
    ],
)
def test_parse_and_normalize_srt_rejects_invalid_cues(raw, error):
    with pytest.raises(subtitles.SubtitleExtractionError, match=error):
        subtitles.parse_and_normalize_srt(raw)


def test_probe_subtitle_streams_inventories_all_subtitle_streams(monkeypatch):
    payload = {
        "streams": [
            {"index": 0, "codec_type": "video", "codec_name": "h264"},
            {
                "index": 2,
                "codec_type": "subtitle",
                "codec_name": "mov_text",
                "tags": {"language": "eng", "handler_name": "English"},
                "disposition": {"default": 1, "forced": 0},
            },
            {
                "index": 3,
                "codec_type": "subtitle",
                "codec_name": "hdmv_pgs_subtitle",
                "tags": {"language": "eng"},
                "disposition": {},
            },
            {
                "index": 4,
                "codec_type": "subtitle",
                "codec_name": "subrip",
                "tags": {},
                "disposition": {},
            },
        ]
    }

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(subtitles.subprocess, "run", lambda *args, **kwargs: Result())

    tracks = subtitles.probe_subtitle_streams("fixture.mp4")

    assert [track["stream_index"] for track in tracks] == [2, 3, 4]
    assert tracks[0]["selection_status"] == "eligible_english"
    assert tracks[0]["is_default"] is True
    assert tracks[1]["selection_status"] == "unsupported_codec"
    assert tracks[2]["selection_status"] == "skipped_unknown_language"


def test_probe_subtitle_streams_skips_missing_or_invalid_stream_indexes(monkeypatch):
    payload = {
        "streams": [
            {"codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": None, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": "bad", "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": True, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": -1, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": 2.5, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
            {"index": "7", "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "eng"}},
        ]
    }

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(subtitles.subprocess, "run", lambda *args, **kwargs: Result())

    tracks = subtitles.probe_subtitle_streams("fixture.mp4")

    assert [track["stream_index"] for track in tracks] == [7]


def test_primary_selection_prefers_default_non_forced_then_lowest_index():
    tracks = [
        {"stream_index": 5, "is_default": False, "is_forced": False},
        {"stream_index": 7, "is_default": True, "is_forced": True},
        {"stream_index": 4, "is_default": True, "is_forced": False},
    ]
    assert min(tracks, key=subtitles._primary_sort_key)["stream_index"] == 4


class _Storage:
    def __init__(self):
        self.objects = {}
        self.deleted = []

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = kwargs

    def head_object(self, **kwargs):
        return self.objects[kwargs["Key"]]

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs["Key"])
        self.objects.pop(kwargs["Key"], None)

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[kwargs["Key"]]["Body"])}

    def generate_presigned_url(self, operation, **kwargs):
        assert operation == "get_object"
        return f"https://signed.invalid/{kwargs['Params']['Key']}?ttl={kwargs['ExpiresIn']}"


def test_put_object_pair_sets_private_content_contract():
    storage = _Storage()
    subtitles._put_object_pair(
        storage,
        "media",
        srt_key="captions.srt",
        cue_key="cues.json",
        srt=b"srt",
        cue_json=b"{}",
        metadata={"source": "embedded"},
    )
    assert storage.objects["captions.srt"]["ContentType"] == subtitles.SRT_CONTENT_TYPE
    assert storage.objects["captions.srt"]["CacheControl"] == "private, no-store"
    assert storage.objects["cues.json"]["ContentType"] == subtitles.CUE_JSON_CONTENT_TYPE


def test_put_object_pair_never_deletes_preexisting_content_on_retry_failure():
    class _FailingStorage(_Storage):
        def put_object(self, **kwargs):
            if kwargs["Key"] == "cues.json":
                raise RuntimeError("upload failed")
            super().put_object(**kwargs)

    storage = _FailingStorage()
    storage.objects["captions.srt"] = {"Body": b"existing"}

    with pytest.raises(RuntimeError, match="upload failed"):
        subtitles._put_object_pair(
            storage,
            "media",
            srt_key="captions.srt",
            cue_key="cues.json",
            srt=b"srt",
            cue_json=b"{}",
            metadata={"source": "embedded"},
        )

    assert storage.objects["captions.srt"]["Body"] == b"existing"
    assert "captions.srt" not in storage.deleted


def test_put_object_pair_refreshes_derived_cue_json_for_same_srt_hash():
    storage = _Storage()
    storage.objects["captions.srt"] = {"Body": b"same srt"}
    storage.objects["cues.json"] = {
        "Body": b'{"is_primary":true}',
        "ContentType": subtitles.CUE_JSON_CONTENT_TYPE,
        "CacheControl": "private, no-store",
        "Metadata": {},
    }

    subtitles._put_object_pair(
        storage,
        "media",
        srt_key="captions.srt",
        cue_key="cues.json",
        srt=b"same srt",
        cue_json=b'{"is_primary":false}',
        metadata={"source": "embedded"},
    )

    assert storage.objects["cues.json"]["Body"] == b'{"is_primary":false}'


def test_load_subtitle_cues_searches_plain_text(monkeypatch):
    storage = _Storage()
    storage.objects["cues.json"] = {
        "Body": json.dumps(
            {
                "cues": [
                    {"ordinal": 1, "plain_text": "Hello Sigala", "text": "Hello Sigala"},
                    {"ordinal": 2, "plain_text": "Goodbye", "text": "Goodbye"},
                ]
            }
        ).encode()
    }
    monkeypatch.setattr(
        subtitles.cast_screentime,
        "get_subtitle_track",
        lambda *_args: {"extraction_status": "complete", "cue_json_object_key": "cues.json"},
    )

    result = subtitles.load_subtitle_cues("asset", "track", query="SIGALA", storage_client=storage, bucket="media")

    assert result["total_cues"] == 2
    assert result["matched_cues"] == 1
    assert result["items"][0]["ordinal"] == 1


def test_generate_download_url_sanitizes_filename(monkeypatch):
    storage = _Storage()
    monkeypatch.setattr(
        subtitles.cast_screentime,
        "get_subtitle_track",
        lambda *_args: {
            "stream_index": 2,
            "language_normalized": "en",
            "extraction_status": "complete",
            "srt_object_key": "captions.srt",
        },
    )
    monkeypatch.setattr(
        subtitles.cast_screentime,
        "get_video_asset",
        lambda *_args: {"source_json": {"original_filename": 'Love/Island;".mp4'}},
    )

    result = subtitles.generate_subtitle_download_url("asset", "track", storage_client=storage, bucket="media")

    assert result["filename"] == "Island.stream-2.en.srt"
    assert result["expires_in_seconds"] == 300


def test_extraction_promotes_next_successful_track_when_preferred_track_fails(monkeypatch, tmp_path):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"fixture")
    storage = _Storage()
    tracks = [
        {
            "id": "track-2",
            "stream_index": 2,
            "codec_name": "mov_text",
            "language_raw": "eng",
            "language_normalized": "en",
            "is_default": True,
            "is_forced": False,
            "selection_status": "eligible_english",
            "extraction_status": "detected",
        },
        {
            "id": "track-3",
            "stream_index": 3,
            "codec_name": "subrip",
            "language_raw": "eng",
            "language_normalized": "en",
            "is_default": False,
            "is_forced": False,
            "selection_status": "eligible_english",
            "extraction_status": "detected",
        },
    ]
    selected = []

    monkeypatch.setattr(subtitles.cast_screentime, "get_video_asset", lambda *_: {"source_json": {}})
    monkeypatch.setattr(subtitles.cast_screentime, "claim_subtitle_extraction", lambda *_a, **_k: {"id": "asset"})
    monkeypatch.setattr(subtitles, "probe_subtitle_streams", lambda *_: tracks)
    monkeypatch.setattr(subtitles.cast_screentime, "upsert_subtitle_track_inventory", lambda *_: tracks)

    def _update(track_id, payload):
        track = next(item for item in tracks if item["id"] == track_id)
        track.update(payload)
        return track

    monkeypatch.setattr(subtitles.cast_screentime, "update_subtitle_track", _update)
    monkeypatch.setattr(
        subtitles.cast_screentime,
        "get_subtitle_summary",
        lambda *_a, **_k: {"video_asset_id": "asset", "status": "complete", "tracks": tracks},
    )
    monkeypatch.setattr(
        subtitles.cast_screentime,
        "set_primary_subtitle_track",
        lambda _asset, track_id: selected.append(track_id),
    )
    monkeypatch.setattr(subtitles.cast_screentime, "finalize_subtitle_extraction", lambda *_: {})
    monkeypatch.setattr(subtitles.cast_screentime, "fail_subtitle_extraction", lambda *_: {})

    def _extract(_video, stream_index, output):
        if stream_index == 2:
            raise subtitles.SubtitleExtractionError("first track failed")
        output.write_text("1\n00:00:01,000 --> 00:00:02,000\nFallback\n\n")

    monkeypatch.setattr(subtitles, "extract_srt_stream", _extract)

    subtitles.extract_video_asset_subtitles("asset", local_video_path=video, storage_client=storage, bucket="media")

    cue_key, cue_object = next(
        (key, value) for key, value in storage.objects.items() if "/cues-" in key and key.endswith(".json")
    )
    assert json.loads(cue_object["Body"])["is_primary"] is True
    assert subtitles._sha256(cue_object["Body"]) in cue_key
    assert selected == ["track-3"]

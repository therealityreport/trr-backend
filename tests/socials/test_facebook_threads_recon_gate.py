from __future__ import annotations

import json
from pathlib import Path


def test_facebook_threads_recon_fixture_pack_is_complete_and_stable() -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "socials"
        / "recon"
        / "facebook_threads_recon_fixture_pack.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    required_classes = {
        "facebook_page_feed",
        "facebook_reels",
        "facebook_photos",
        "threads_profile",
        "threads_post",
        "threads_media",
    }

    classes = payload.get("classes") or {}
    assert required_classes.issubset(classes.keys())

    for class_name in required_classes:
        class_payload = classes[class_name]
        runs = class_payload.get("runs") or []
        assert len(runs) >= 2, f"{class_name} requires at least two recon runs"

        first_signature = str(runs[0].get("source_signature") or "")
        second_signature = str(runs[1].get("source_signature") or "")
        assert first_signature and first_signature == second_signature, (
            f"{class_name} recon source signature must be stable across consecutive runs"
        )

        strategy = class_payload.get("strategy") or {}
        assert strategy.get("primary")
        assert strategy.get("secondary")
        assert strategy.get("fallback")

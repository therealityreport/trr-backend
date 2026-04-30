from __future__ import annotations

import scripts.socials.instagram.comments_worker as comments_worker


def test_comments_worker_sets_lane_and_forwards_args(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_worker_main():
        captured["argv"] = list(comments_worker.sys.argv)
        captured["lane"] = comments_worker.os.environ.get("SOCIAL_WORKER_LANE")
        captured["script"] = comments_worker.os.environ.get("SOCIAL_WORKER_SCRIPT")
        return 7

    monkeypatch.setattr(comments_worker, "worker_main", _fake_worker_main)
    monkeypatch.setattr(comments_worker.sys, "argv", ["comments_worker.py", "--once"])
    monkeypatch.delenv("SOCIAL_WORKER_LANE", raising=False)
    monkeypatch.delenv("SOCIAL_WORKER_SCRIPT", raising=False)

    rc = comments_worker.main()

    assert rc == 7
    assert captured["lane"] == "instagram_comments_scrapling"
    assert captured["script"] == "scripts.socials.instagram.comments_worker"
    assert captured["argv"] == [
        "comments_worker.py",
        "--stage",
        "comments_scrapling",
        "--platform",
        "instagram",
        "--once",
    ]

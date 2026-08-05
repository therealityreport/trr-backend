from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2 import admin_reddit_reads
from trr_backend.db.pg import DatabaseServiceUnavailableError

COMMUNITY_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "22222222-2222-2222-2222-222222222222"


def _post_detail(**overrides: Any) -> dict[str, Any]:
    payload = {
        "reddit_post_id": "post-1",
        "subreddit": "BravoRealHousewives",
        "title": "Episode Thread",
        "text": "Body",
        "url": "https://reddit.com/r/show/comments/abc123",
        "permalink": "https://reddit.com/r/show/comments/abc123",
        "author": "BravoFan",
        "score": 12,
        "num_comments": 4,
        "posted_at": "2026-03-26T00:00:00Z",
        "link_flair_text": "Cast",
        "canonical_flair_key": "cast",
        "upvote_ratio": 0.98,
        "is_self": True,
        "post_type": "self",
        "thumbnail": None,
        "content_url": None,
        "is_nsfw": False,
        "is_spoiler": False,
        "author_flair_text": None,
        "detail_scraped_at": "2026-03-26T00:10:00Z",
        "source_sorts": ["hot"],
        "media_metadata": {"images": 1},
        "poll_data": {},
        "matches": [{"period_key": "episode-1", "match_score": 41}],
        "comments": [{"reddit_comment_id": "comment-1", "score": 5}],
        "comment_summary": {
            "total_comments": 1,
            "top_level_comments": 1,
            "reply_comments": 0,
            "earliest_comment_at": None,
            "latest_comment_at": None,
        },
        "media": [],
        "media_summary": {
            "total_media": 0,
            "mirrored_media": 0,
            "pending_media": 0,
            "failed_media": 0,
        },
        "assigned_threads": [],
    }
    payload.update(overrides)
    return payload


def _resolved_post() -> dict[str, Any]:
    return {
        "reddit_post_id": "post-1",
        "detail_slug": "episode-thread--u-bravofan",
        "collision": False,
        "post": {
            "title": "Episode Thread",
            "author": "BravoFan",
            "posted_at": "2026-03-26T00:00:00Z",
            "url": "https://reddit.com/r/show/comments/abc123",
            "permalink": "https://reddit.com/r/show/comments/abc123",
        },
    }


def _post_window_counts() -> dict[str, Any]:
    return {
        "counts": {"episode-1": 2},
        "total_posts": 3,
        "tracked_total_posts": 2,
        "tracked_flair_counts": [
            {
                "flair_key": "cast",
                "flair_label": "Cast",
                "post_count": 2,
                "container_counts": [{"container_key": "episode-1", "post_count": 2}],
            }
        ],
        "pending_tracked_flair_counts": [
            {
                "container_key": "episode-1",
                "flair_key": "cast",
                "flair_label": "Cast",
                "post_count": 1,
            }
        ],
        "flair_counts": [{"flair": "Cast", "post_count": 2}],
    }


def _post_window() -> dict[str, Any]:
    return {
        "pagination": {"page": 2, "per_page": 50, "total_count": 1},
        "posts": [
            {
                "reddit_post_id": "post-1",
                "title": "Episode Thread",
                "text": "Body",
                "url": "https://reddit.com/r/show/comments/abc123",
                "permalink": "https://reddit.com/r/show/comments/abc123",
                "author": "BravoFan",
                "score": 12,
                "num_comments": 4,
                "posted_at": "2026-03-26T00:00:00Z",
                "link_flair_text": "Cast",
                "is_show_match": True,
                "passes_flair_filter": True,
                "match_score": 41,
                "match_type": "flair",
            }
        ],
    }


@dataclass
class FakeAdminRedditReadsRepository:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def get_reddit_post_details_by_community_and_season(self, **kwargs: Any):
        self.calls.append(("get_reddit_post_details_by_community_and_season", kwargs))
        if kwargs["reddit_post_id"] == "missing":
            return None, 1
        return _post_detail(), 7

    def resolve_reddit_post_detail_by_slug(self, **kwargs: Any):
        self.calls.append(("resolve_reddit_post_detail_by_slug", kwargs))
        if kwargs["reddit_post_id"] == "missing":
            return None, 1
        return _resolved_post(), 1

    def get_stored_post_counts_by_community_and_season(self, community_id: str, season_id: str):
        self.calls.append(
            (
                "get_stored_post_counts_by_community_and_season",
                {"community_id": community_id, "season_id": season_id},
            )
        )
        return _post_window_counts(), 4

    def get_stored_window_posts_by_community_and_season(
        self,
        community_id: str,
        season_id: str,
        container_key: str,
        *,
        page: int,
        per_page: int,
    ):
        self.calls.append(
            (
                "get_stored_window_posts_by_community_and_season",
                {
                    "community_id": community_id,
                    "season_id": season_id,
                    "container_key": container_key,
                    "page": page,
                    "per_page": per_page,
                },
            )
        )
        return _post_window(), 2


@pytest.fixture
def fake_repository(monkeypatch: pytest.MonkeyPatch) -> FakeAdminRedditReadsRepository:
    repository = FakeAdminRedditReadsRepository()
    monkeypatch.setattr(admin_reddit_reads, "reddit_reads_repo", repository)
    return repository


@pytest.fixture
def app() -> FastAPI:
    test_app = FastAPI()
    test_app.include_router(admin_reddit_reads.router, prefix="/api/v2")
    test_app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    return test_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_reddit_post_and_post_window_reads_use_the_frozen_v2_contracts(
    client: TestClient,
    fake_repository: FakeAdminRedditReadsRepository,
) -> None:
    post = client.get(
        "/api/v2/admin/reddit/posts/post-1",
        params={"community_id": COMMUNITY_ID, "season_id": SEASON_ID, "comments_limit": 100},
    )
    resolved = client.get(
        "/api/v2/admin/reddit/posts/resolve",
        params={"community_id": COMMUNITY_ID, "season_id": SEASON_ID, "window_key": "w1", "post_id": "post-1"},
    )
    counts = client.get(
        "/api/v2/admin/reddit/post-window-counts",
        params={"community_id": COMMUNITY_ID, "season_id": SEASON_ID},
    )
    window = client.get(
        "/api/v2/admin/reddit/post-windows",
        params={
            "community_id": COMMUNITY_ID,
            "season_id": SEASON_ID,
            "container_key": "episode-1",
            "page": 2,
            "per_page": 50,
        },
    )

    assert post.status_code == 200
    assert post.json()["post"]["reddit_post_id"] == "post-1"
    assert post.json()["post"]["comment_summary"]["total_comments"] == 1
    assert resolved.status_code == 200
    assert resolved.json()["detail_slug"] == "episode-thread--u-bravofan"
    assert counts.status_code == 200
    assert counts.json()["counts"] == {"episode-1": 2}
    assert window.status_code == 200
    assert window.json()["pagination"] == {"page": 2, "per_page": 50, "total_count": 1}
    assert window.json()["posts"][0]["passes_flair_filter"] is True
    assert fake_repository.calls == [
        (
            "get_reddit_post_details_by_community_and_season",
            {
                "community_id": COMMUNITY_ID,
                "season_id": SEASON_ID,
                "reddit_post_id": "post-1",
                "comments_limit": 100,
            },
        ),
        (
            "resolve_reddit_post_detail_by_slug",
            {
                "community_id": COMMUNITY_ID,
                "season_id": SEASON_ID,
                "container_key": "episode-1",
                "title_slug": None,
                "author_slug": None,
                "reddit_post_id": "post-1",
            },
        ),
        (
            "get_stored_post_counts_by_community_and_season",
            {"community_id": COMMUNITY_ID, "season_id": SEASON_ID},
        ),
        (
            "get_stored_window_posts_by_community_and_season",
            {
                "community_id": COMMUNITY_ID,
                "season_id": SEASON_ID,
                "container_key": "episode-1",
                "page": 2,
                "per_page": 50,
            },
        ),
    ]


@pytest.mark.parametrize(
    ("path", "expected_code"),
    [
        (
            f"/api/v2/admin/reddit/posts/post-1?community_id=bad&season_id={SEASON_ID}",
            "INVALID_COMMUNITY_ID",
        ),
        (
            f"/api/v2/admin/reddit/posts/resolve?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&window_key=bad&post_id=post-1",
            "INVALID_WINDOW_KEY",
        ),
        (
            f"/api/v2/admin/reddit/posts/post-1?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&comments_limit=501",
            "INVALID_COMMENTS_LIMIT",
        ),
        (
            f"/api/v2/admin/reddit/post-windows?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&container_key=episode-0",
            "INVALID_WINDOW_KEY",
        ),
        (
            f"/api/v2/admin/reddit/post-windows?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&container_key=episode-1&page=0",
            "INVALID_PAGINATION",
        ),
    ],
)
def test_invalid_reddit_read_inputs_use_stable_400_problems_without_fastapi_422(
    client: TestClient,
    fake_repository: FakeAdminRedditReadsRepository,
    path: str,
    expected_code: str,
) -> None:
    response = client.get(path, headers={"x-request-id": "invalid-reddit-read"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code
    assert response.json()["detail"]["request_id"] == "invalid-reddit-read"
    assert "422" not in response.text
    assert fake_repository.calls == []


@pytest.mark.parametrize(
    "path",
    [
        f"/api/v2/admin/reddit/posts/missing?community_id={COMMUNITY_ID}&season_id={SEASON_ID}",
        f"/api/v2/admin/reddit/posts/resolve?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&window_key=e1&post_id=missing",
    ],
)
def test_missing_reddit_posts_use_a_typed_404(
    client: TestClient,
    fake_repository: FakeAdminRedditReadsRepository,
    path: str,
) -> None:
    response = client.get(path)

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "REDDIT_POST_NOT_FOUND"
    assert len(fake_repository.calls) == 1


def test_database_capacity_and_unexpected_failures_use_safe_problem_responses(
    client: TestClient,
    fake_repository: FakeAdminRedditReadsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args: Any, **_kwargs: Any):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(fake_repository, "get_stored_post_counts_by_community_and_season", unavailable)
    unavailable_response = client.get(
        "/api/v2/admin/reddit/post-window-counts",
        params={"community_id": COMMUNITY_ID, "season_id": SEASON_ID},
    )

    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in unavailable_response.text

    def unexpected(*_args: Any, **_kwargs: Any):
        raise RuntimeError("secret implementation detail")

    monkeypatch.setattr(fake_repository, "get_reddit_post_details_by_community_and_season", unexpected)
    failed_response = client.get(
        "/api/v2/admin/reddit/posts/post-1",
        params={"community_id": COMMUNITY_ID, "season_id": SEASON_ID},
    )

    assert failed_response.status_code == 500
    assert failed_response.json()["detail"]["code"] == "REDDIT_READ_REQUEST_FAILED"
    assert "secret implementation detail" not in failed_response.text


def test_all_reddit_read_routes_require_strict_internal_admin_auth(
    fake_repository: FakeAdminRedditReadsRepository,
) -> None:
    unauthenticated_app = FastAPI()
    unauthenticated_app.include_router(admin_reddit_reads.router, prefix="/api/v2")
    client = TestClient(unauthenticated_app)

    responses = [
        client.get(f"/api/v2/admin/reddit/posts/post-1?community_id={COMMUNITY_ID}&season_id={SEASON_ID}"),
        client.get(
            f"/api/v2/admin/reddit/posts/resolve?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&window_key=e1&post_id=post-1"
        ),
        client.get(f"/api/v2/admin/reddit/post-window-counts?community_id={COMMUNITY_ID}&season_id={SEASON_ID}"),
        client.get(
            f"/api/v2/admin/reddit/post-windows?community_id={COMMUNITY_ID}&season_id={SEASON_ID}&container_key=episode-1"
        ),
    ]

    assert [response.status_code for response in responses] == [401, 401, 401, 401]
    assert fake_repository.calls == []


def test_v2_admin_reddit_reads_openapi_is_explicit_strict_and_bounded(app: FastAPI) -> None:
    schema = app.openapi()
    expected = {
        "/api/v2/admin/reddit/posts/{post_id}": "getAdminRedditPostV2",
        "/api/v2/admin/reddit/posts/resolve": "resolveAdminRedditPostV2",
        "/api/v2/admin/reddit/post-window-counts": "getAdminRedditPostWindowCountsV2",
        "/api/v2/admin/reddit/post-windows": "listAdminRedditPostWindowV2",
    }
    for path, operation_id in expected.items():
        operation = schema["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert operation["security"] == [{"InternalAdminBearer": []}]
        assert "422" not in operation["responses"]
        assert {"200", "400", "500", "503"}.issubset(operation["responses"])

    assert "404" in schema["paths"]["/api/v2/admin/reddit/posts/{post_id}"]["get"]["responses"]
    assert "404" in schema["paths"]["/api/v2/admin/reddit/posts/resolve"]["get"]["responses"]
    parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v2/admin/reddit/post-windows"]["get"]["parameters"]
    }
    assert parameters["container_key"]["schema"]["pattern"] == "^(?:period-(?:preseason|postseason)|episode-[1-9]\\d*)$"
    assert parameters["page"]["schema"] == {"type": "integer", "minimum": 1, "default": 1}
    assert parameters["per_page"]["schema"] == {"type": "integer", "minimum": 1, "maximum": 200, "default": 200}
    for model_name in (
        "AdminRedditPostDetailV2",
        "AdminRedditPostDetailResponseV2",
        "AdminRedditPostResolveResponseV2",
        "AdminRedditPostWindowCountsResponseV2",
        "AdminRedditPostWindowResponseV2",
        "AdminRedditReadProblemDetailV2",
        "AdminRedditReadProblemResponseV2",
    ):
        assert schema["components"]["schemas"][model_name]["additionalProperties"] is False

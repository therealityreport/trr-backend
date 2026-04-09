from __future__ import annotations

import pytest
import requests

from trr_backend.socials.instagram.request_client import InstagramRequestClient, InstagramRequestFailure


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_payload: dict | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {"content-type": "application/json"}
        self._json_payload = json_payload or {}
        self.text = text
        self.cookies = requests.cookies.RequestsCookieJar()
        self.reason = "OK"

    @property
    def is_redirect(self) -> bool:
        return self.status_code in {301, 302, 303, 307, 308}

    def json(self) -> dict:
        return self._json_payload


def test_request_client_classifies_redirect_to_login() -> None:
    session = requests.Session()
    client = InstagramRequestClient(session=session)

    def _fake_get(*_args, **_kwargs):
        return _FakeResponse(status_code=302, headers={"location": "https://www.instagram.com/accounts/login/"})

    with pytest.raises(InstagramRequestFailure) as exc:
        client.get_json(
            "https://www.instagram.com/graphql/query/",
            query_type="graphql_profile_posts",
            headers={},
            cookies={},
            params={"doc_id": "1"},
            sender=_fake_get,
        )

    assert exc.value.error_code == "redirect_login"
    assert exc.value.retryable is False


def test_request_client_classifies_checkpoint_and_feedback() -> None:
    session = requests.Session()
    client = InstagramRequestClient(session=session)

    def _fake_post(*_args, **_kwargs):
        return _FakeResponse(status_code=400, json_payload={"message": "checkpoint_required"})

    with pytest.raises(InstagramRequestFailure) as exc:
        client.post_form_json(
            "https://www.instagram.com/graphql/query/",
            query_type="graphql_profile_posts",
            headers={},
            cookies={},
            data={"doc_id": "1"},
            sender=_fake_post,
        )

    assert exc.value.error_code == "checkpoint_required"
    assert exc.value.retryable is False

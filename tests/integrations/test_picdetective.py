from __future__ import annotations

from unittest.mock import MagicMock

from trr_backend.integrations.picdetective import (
    parse_search_response,
    search_by_image_url,
)

SAMPLE_RESPONSE = {
    "exact_matches": [
        {
            "title": "Kim Richards Arrested",
            "link": "https://www.glamour.com/story/kim-richards",
            "url": "https://www.glamour.com/story/kim-richards",
            "source": "Glamour",
            "thumbnail": "data:image/jpeg;base64,abc123",
            "image": {"src": "https://glamour.com/img.jpg", "width": 1500, "height": 1000},
        },
        {
            "title": "RHOBH Season 5",
            "link": "https://www.gettyimages.com/detail/photo/1234",
            "url": "https://www.gettyimages.com/detail/photo/1234",
            "source": "Getty Images",
            "thumbnail": "data:image/jpeg;base64,def456",
            "image": {"src": "https://getty.com/img.jpg", "width": 612, "height": 408},
        },
        {
            "title": "Ask Her Anything",
            "link": "https://www.menshealth.com/article",
            "url": "https://www.menshealth.com/article",
            "source": "Men's Health",
            "thumbnail": "data:image/jpeg;base64,ghi789",
            "image": {"src": "https://menshealth.com/img.jpg", "width": 2004, "height": 2000},
        },
        {
            "title": "Small blog post",
            "link": "https://blog.example.com/post",
            "url": "https://blog.example.com/post",
            "source": "Example Blog",
            "thumbnail": "",
            "image": {"src": "", "width": 400, "height": 300},
        },
        {
            "title": "No dimensions",
            "link": "https://nodims.example.com/post",
            "url": "https://nodims.example.com/post",
            "source": "NoDims",
            "thumbnail": "",
            "image": {},
        },
    ],
}


def test_parse_search_response_filters_by_min_width() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=1080)
    assert len(candidates) == 2
    assert candidates[0].source_domain == "menshealth.com"
    assert candidates[0].width == 2004
    assert candidates[1].source_domain == "glamour.com"
    assert candidates[1].width == 1500


def test_parse_search_response_excludes_getty_domains() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=0)
    domains = [c.source_domain for c in candidates]
    assert "gettyimages.com" not in domains


def test_parse_search_response_sorts_by_resolution_descending() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=0)
    areas = [(c.width or 0) * (c.height or 0) for c in candidates]
    assert areas == sorted(areas, reverse=True)


def test_parse_search_response_limits_results() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=0, limit=2)
    assert len(candidates) == 2


def test_parse_search_response_extracts_domain_from_url() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=0)
    glamour = next(c for c in candidates if "glamour" in c.source_domain)
    assert glamour.source_domain == "glamour.com"
    assert glamour.page_url == "https://www.glamour.com/story/kim-richards"


def test_parse_search_response_handles_missing_image_fields() -> None:
    candidates = parse_search_response(SAMPLE_RESPONSE, min_width=0)
    nodims = next((c for c in candidates if "nodims" in c.source_domain), None)
    assert nodims is not None
    assert nodims.width is None
    assert nodims.height is None


def test_search_by_image_url_calls_api(monkeypatch) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_RESPONSE
    mock_response.raise_for_status = MagicMock()

    mock_get = MagicMock(return_value=mock_response)
    monkeypatch.setattr("trr_backend.integrations.picdetective.requests.get", mock_get)

    candidates = search_by_image_url(
        "https://media.gettyimages.com/id/467051416/photo/test.jpg?s=2048x2048&w=gi&k=20&c=abc"
    )

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "picdetective.com/api/search" in call_args[0][0] or "picdetective.com/api/search" in str(call_args)
    assert len(candidates) <= 5


def test_search_by_image_url_returns_empty_on_api_error(monkeypatch) -> None:
    mock_get = MagicMock(side_effect=Exception("Connection refused"))
    monkeypatch.setattr("trr_backend.integrations.picdetective.requests.get", mock_get)

    candidates = search_by_image_url("https://example.com/image.jpg")
    assert candidates == []

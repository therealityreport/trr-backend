from __future__ import annotations

from unittest.mock import MagicMock


def test_env_truthy_reads_common_truthy_strings(monkeypatch):
    from trr_backend.socials._scrapling_http_utils import env_truthy

    for raw, expected in [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("", False),
        ("no", False),
    ]:
        monkeypatch.setenv("TEST_TRUTHY_KEY", raw)
        assert env_truthy("TEST_TRUTHY_KEY", default=False) is expected, f"raw={raw!r}"


def test_env_truthy_returns_default_when_unset(monkeypatch):
    from trr_backend.socials._scrapling_http_utils import env_truthy

    monkeypatch.delenv("TEST_TRUTHY_KEY", raising=False)
    assert env_truthy("TEST_TRUTHY_KEY", default=True) is True
    assert env_truthy("TEST_TRUTHY_KEY", default=False) is False


def test_response_text_reads_str_attribute():
    from trr_backend.socials._scrapling_http_utils import response_text

    resp = MagicMock(text="hello")
    assert response_text(resp) == "hello"


def test_response_text_handles_callable_text():
    """Some mock response objects expose .text as a zero-arg callable."""
    from trr_backend.socials._scrapling_http_utils import response_text

    resp = MagicMock()
    resp.text = MagicMock(return_value="callable_hello")
    assert response_text(resp) == "callable_hello"


def test_response_text_returns_empty_on_none():
    from trr_backend.socials._scrapling_http_utils import response_text

    resp = MagicMock(text=None)
    assert response_text(resp) == ""


def test_response_text_decodes_scrapling_body_bytes_when_text_is_empty():
    from trr_backend.socials._scrapling_http_utils import response_text

    resp = MagicMock(text="")
    resp.body = b"<html>SocialBlade body</html>"
    assert response_text(resp) == "<html>SocialBlade body</html>"


def test_status_code_reads_status_code_attr():
    from trr_backend.socials._scrapling_http_utils import status_code

    resp = MagicMock(status_code=200)
    assert status_code(resp) == 200


def test_status_code_falls_back_to_status_attr():
    """Scrapling's response object uses .status instead of .status_code."""
    from trr_backend.socials._scrapling_http_utils import status_code

    resp = MagicMock(spec=["status"])
    resp.status = 429
    assert status_code(resp) == 429


def test_status_code_defaults_to_zero_on_missing():
    from trr_backend.socials._scrapling_http_utils import status_code

    assert status_code(MagicMock(spec=[])) == 0


def test_safe_location_extracts_lowercase_path():
    from trr_backend.socials._scrapling_http_utils import safe_location

    resp = MagicMock()
    resp.headers = {"location": "https://www.example.com/Accounts/Login?next=/"}
    assert safe_location(resp) == "/accounts/login"


def test_safe_location_returns_empty_when_missing():
    from trr_backend.socials._scrapling_http_utils import safe_location

    resp = MagicMock()
    resp.headers = {}
    assert safe_location(resp) == ""


def test_extract_response_cookies_from_dict():
    from trr_backend.socials._scrapling_http_utils import extract_response_cookies

    resp = MagicMock(cookies={"sessionid": "abc", "csrftoken": "xyz"})
    assert extract_response_cookies(resp) == {"sessionid": "abc", "csrftoken": "xyz"}


def test_extract_response_cookies_from_items_protocol():
    from trr_backend.socials._scrapling_http_utils import extract_response_cookies

    mock_cookies = MagicMock(spec=["items"])
    mock_cookies.items = MagicMock(return_value=[("a", "1"), ("b", "2")])
    resp = MagicMock()
    resp.cookies = mock_cookies
    assert extract_response_cookies(resp) == {"a": "1", "b": "2"}


def test_extract_response_cookies_from_jar():
    """httpx responses expose cookies via .jar."""
    from trr_backend.socials._scrapling_http_utils import extract_response_cookies

    cookie_obj_1 = MagicMock(name="jar1", spec=["name", "value"])
    cookie_obj_1.name = "sessionid"
    cookie_obj_1.value = "abc"
    cookie_obj_2 = MagicMock(name="jar2", spec=["name", "value"])
    cookie_obj_2.name = "ttwid"
    cookie_obj_2.value = "xyz"

    mock_cookies = MagicMock(spec=["jar"])
    mock_cookies.jar = [cookie_obj_1, cookie_obj_2]
    resp = MagicMock()
    resp.cookies = mock_cookies
    assert extract_response_cookies(resp) == {"sessionid": "abc", "ttwid": "xyz"}


def test_extract_response_cookies_returns_empty_on_none():
    from trr_backend.socials._scrapling_http_utils import extract_response_cookies

    resp = MagicMock()
    resp.cookies = None
    assert extract_response_cookies(resp) == {}

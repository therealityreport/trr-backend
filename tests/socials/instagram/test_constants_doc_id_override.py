"""Phase 4.1 regression tests for ``resolve_profile_posts_doc_ids``."""

from __future__ import annotations

from trr_backend.socials.instagram.constants import (
    _PROFILE_PAGE_CONTENT_DOC_IDS_FALLBACK,
    _PROFILE_POSTS_DOC_IDS_FALLBACK,
    PROFILE_PAGE_CONTENT_DOC_IDS_ENV,
    PROFILE_POSTS_DOC_IDS_ENV,
    resolve_profile_page_content_doc_ids,
    resolve_profile_posts_doc_ids,
)


def test_unset_env_returns_hardcoded_fallback(monkeypatch):
    monkeypatch.delenv(PROFILE_POSTS_DOC_IDS_ENV, raising=False)
    assert resolve_profile_posts_doc_ids() == _PROFILE_POSTS_DOC_IDS_FALLBACK
    assert resolve_profile_posts_doc_ids()[0] == "26859136577041380"


def test_single_id_override(monkeypatch):
    monkeypatch.setenv(PROFILE_POSTS_DOC_IDS_ENV, "12345678901234567")
    assert resolve_profile_posts_doc_ids() == ("12345678901234567",)


def test_multi_id_override_dedupes_and_strips_whitespace(monkeypatch):
    monkeypatch.setenv(
        PROFILE_POSTS_DOC_IDS_ENV,
        "  111 ,  222, 111 ,333,  ",
    )
    assert resolve_profile_posts_doc_ids() == ("111", "222", "333")


def test_invalid_entries_are_skipped(monkeypatch):
    monkeypatch.setenv(PROFILE_POSTS_DOC_IDS_ENV, "abc, 123, def-ghi, 456")
    assert resolve_profile_posts_doc_ids() == ("123", "456")


def test_all_invalid_entries_falls_back_to_hardcoded_default(monkeypatch):
    monkeypatch.setenv(PROFILE_POSTS_DOC_IDS_ENV, "abc, ,, def")
    assert resolve_profile_posts_doc_ids() == _PROFILE_POSTS_DOC_IDS_FALLBACK


def test_profile_page_content_doc_id_defaults_to_copied_profile_curl(monkeypatch):
    monkeypatch.delenv(PROFILE_PAGE_CONTENT_DOC_IDS_ENV, raising=False)
    assert resolve_profile_page_content_doc_ids() == _PROFILE_PAGE_CONTENT_DOC_IDS_FALLBACK
    assert resolve_profile_page_content_doc_ids()[0] == "35710877621861450"

from __future__ import annotations

import os
from functools import lru_cache

from supabase import Client, create_client


@lru_cache
def get_supabase_url() -> str:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_URL environment variable is not set")
    return url


@lru_cache
def get_supabase_service_key() -> str:
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")
    return key


def create_supabase_admin_client(*, url: str | None = None, service_role_key: str | None = None) -> Client:
    """
    Create a Supabase client using the service role key (bypasses RLS).

    Intended for scripts and admin tasks (imports/backfills).
    """

    return create_client(url or get_supabase_url(), service_role_key or get_supabase_service_key())

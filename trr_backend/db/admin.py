"""Legacy Supabase helpers (SDK-free)."""

from __future__ import annotations

from trr_backend.db.session import DbSession, get_db_session


# NOTE: Supabase Python SDK has been removed from this repo. These helpers
# provide backwards-compatible entry points for call sites that previously
# expected supabase-py client objects.


def create_supabase_admin_client(*, url: str | None = None, service_role_key: str | None = None) -> DbSession:
    return get_db_session()


def call_rpc_with_cache_reload_hint(
    db: DbSession,
    schema: str,
    function_name: str,
    params: dict,
) -> any:
    response = db.schema(schema).rpc(function_name, params).execute()
    if getattr(response, "error", None):
        raise RuntimeError(response.error.message)
    return response.data


def is_timeout_error(exc: BaseException) -> bool:
    return False

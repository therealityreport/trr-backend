"""Dependency injection for DB access and shared resources."""

from __future__ import annotations

import logging
import warnings
from typing import Annotated, Any

from fastapi import Depends, HTTPException

from trr_backend.db.session import DbSession, get_db_session

logger = logging.getLogger(__name__)

Client = DbSession


def get_supabase_client() -> Client:
    """
    Returns a PostgREST-backed DB session for SDK-style public read operations.

    Prefer `trr_backend.db.pg` for direct database reads and writes.
    """
    return get_db_session()


def get_postgrest_admin_client() -> Client:
    """
    Returns a PostgREST-backed DB session for admin service-role operations.

    Prefer `trr_backend.db.pg` for direct database reads and writes.
    """
    from trr_backend.db.admin import create_supabase_admin_client

    return create_supabase_admin_client()


def get_supabase_admin_client() -> Client:
    """
    Deprecated alias for `get_postgrest_admin_client`.
    """
    warnings.warn(
        "get_supabase_admin_client is deprecated; use get_postgrest_admin_client for PostgREST "
        "surfaces and trr_backend.db.pg for direct database access.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_postgrest_admin_client()


# Type aliases for dependency injection
SupabaseClient = Annotated[Client, Depends(get_supabase_client)]
PostgrestAdminClient = Annotated[Client, Depends(get_postgrest_admin_client)]
SupabaseAdminClient = PostgrestAdminClient


class SupabaseError(Exception):
    """Wrapper for Supabase errors."""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def raise_for_supabase_error(response: Any, context: str = "database operation") -> None:
    """
    Check a Supabase response for errors and raise appropriate HTTP exceptions.

    Args:
        response: The response object from a Supabase query
        context: Description of the operation for error messages

    Raises:
        HTTPException: 502 for Supabase connectivity/server errors, 500 for other errors
    """
    # Check if response indicates an error
    if hasattr(response, "error") and response.error:
        error_msg = str(response.error)
        logger.error(f"Supabase error during {context}: {error_msg}")
        # Don't leak internal error details to client
        raise HTTPException(status_code=502, detail=f"Database error during {context}")


def require_single_result(response: Any, entity_name: str = "Resource") -> dict:
    """
    Ensure a Supabase response contains exactly one result.

    Args:
        response: The response object from a Supabase .single() query
        entity_name: Name of the entity for error messages (e.g., "Show", "Survey")

    Returns:
        The single result dict

    Raises:
        HTTPException: 404 if no result found, 502 for Supabase errors
    """
    raise_for_supabase_error(response, f"fetching {entity_name.lower()}")

    if not response.data:
        raise HTTPException(status_code=404, detail=f"{entity_name} not found")

    return response.data


def get_list_result(response: Any, context: str = "listing") -> list:
    """
    Extract list results from a Supabase response with error handling.

    Args:
        response: The response object from a Supabase query
        context: Description of the operation for error messages

    Returns:
        List of results (empty list if no data)

    Raises:
        HTTPException: 502 for Supabase errors
    """
    raise_for_supabase_error(response, context)
    return response.data or []

"""
Dependency injection for Supabase client and other shared resources.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Annotated, Any

from fastapi import Depends, HTTPException
from supabase import Client, create_client

# Load environment variables if running standalone
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


@lru_cache
def get_supabase_url() -> str:
    url = os.getenv("SUPABASE_URL")
    if not url:
        raise RuntimeError("SUPABASE_URL environment variable is not set")
    return url


@lru_cache
def get_supabase_anon_key() -> str:
    key = os.getenv("SUPABASE_ANON_KEY")
    if not key:
        raise RuntimeError("SUPABASE_ANON_KEY environment variable is not set")
    return key


@lru_cache
def get_supabase_service_key() -> str:
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")
    return key


def get_supabase_client() -> Client:
    """
    Returns a Supabase client using the anon key (for public read operations).
    """
    return create_client(get_supabase_url(), get_supabase_anon_key())


def get_supabase_admin_client() -> Client:
    """
    Returns a Supabase client using the service role key (bypasses RLS).
    Use only for admin operations like updating aggregates.
    """
    return create_client(get_supabase_url(), get_supabase_service_key())


# Type aliases for dependency injection
SupabaseClient = Annotated[Client, Depends(get_supabase_client)]
SupabaseAdminClient = Annotated[Client, Depends(get_supabase_admin_client)]


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
    if hasattr(response, 'error') and response.error:
        error_msg = str(response.error)
        logger.error(f"Supabase error during {context}: {error_msg}")
        # Don't leak internal error details to client
        raise HTTPException(
            status_code=502,
            detail=f"Database error during {context}"
        )


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

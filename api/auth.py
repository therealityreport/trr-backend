"""
Authentication utilities for FastAPI.

Extracts user information from Supabase JWT tokens.
All writes must use the user-scoped client to enforce RLS.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from api.deps import get_supabase_anon_key, get_supabase_url
from supabase import Client, create_client

logger = logging.getLogger(__name__)


def get_bearer_token(request: Request) -> str | None:
    """
    Extract Bearer token from Authorization header.

    Returns None if no token is present.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1]


async def get_current_user(request: Request) -> dict | None:
    """
    Get the current user from the Supabase JWT token.

    Returns None if no token or invalid token.
    Returns user dict with 'id', 'email', etc. if valid.
    """
    token = get_bearer_token(request)
    if not token:
        return None

    try:
        # Create a client with the user's token to validate it
        client = create_client(get_supabase_url(), get_supabase_anon_key())
        # Get user from the token
        user_response = client.auth.get_user(token)

        if user_response and user_response.user:
            return {
                "id": str(user_response.user.id),
                "email": user_response.user.email,
                "role": user_response.user.role,
                "token": token,  # Store token for user-scoped client
            }
        return None
    except Exception as e:
        logger.warning(f"Failed to validate token: {e}")
        return None


async def require_user(request: Request) -> dict:
    """
    Dependency that requires a valid authenticated user.

    Raises 401 if no token or invalid token.
    Returns user dict with 'id', 'email', 'token', etc. if valid.
    """
    user = await get_current_user(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Please provide a valid access token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def get_user_supabase_client(user: dict) -> Client:
    """
    Returns a Supabase client scoped to the user's token.

    This client enforces RLS based on the user's identity.
    Use this for authenticated write operations.
    """
    client = create_client(get_supabase_url(), get_supabase_anon_key())
    # Apply the user's access token for RLS enforcement
    client.postgrest.auth(user["token"])
    return client


# Type alias for dependency injection
CurrentUser = Annotated[dict, Depends(require_user)]
OptionalUser = Annotated[dict | None, Depends(get_current_user)]

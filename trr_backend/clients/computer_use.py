"""
Claude Computer Use client for TRR-Backend.

Provides an opt-in FastAPI router and a programmatic helper for running
computer use tasks via the Anthropic API.

Router usage (in api/main.py):
    from trr_backend.clients.computer_use import router as computer_use_router
    app.include_router(computer_use_router, prefix="/api/v1/computer-use", tags=["computer-use"])

Programmatic usage:
    from trr_backend.clients.computer_use import run_computer_task
    result = await run_computer_task("Take a screenshot and describe what you see")
"""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser

try:
    from claude_computer_use import ComputerUseClient
    from claude_computer_use.loop import async_sampling_loop
except ImportError:  # pragma: no cover - exercised via configuration failure path
    ComputerUseClient = None
    async_sampling_loop = None

logger = logging.getLogger(__name__)

router = APIRouter()

COMPUTER_USE_CONFIG_ERROR = "Computer use service is not configured."
COMPUTER_USE_EXECUTION_ERROR = "Computer use task failed."


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ComputerUseRequest(BaseModel):
    prompt: str = Field(..., min_length=1, description="Task for Claude to perform via computer use")
    model: str = Field(default="claude-opus-4-6")
    max_iterations: int = Field(default=10, ge=1, le=50)


class ComputerUseResponse(BaseModel):
    success: bool
    final_text: str | None = None
    iterations: int = 0


class ComputerUseConfigurationError(RuntimeError):
    """Raised when the computer use runtime is unavailable or misconfigured."""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/run", response_model=ComputerUseResponse)
async def run_task(req: ComputerUseRequest, _: InternalAdminUser = None):
    """Execute a computer use task via Claude."""
    try:
        result = await run_computer_task(
            prompt=req.prompt,
            model=req.model,
            max_iterations=req.max_iterations,
        )
        return result
    except ComputerUseConfigurationError as exc:
        logger.warning("Computer use unavailable: %s", exc)
        raise HTTPException(status_code=503, detail=COMPUTER_USE_CONFIG_ERROR) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid computer use request.") from exc
    except Exception as exc:
        logger.exception("Computer use execution failed")
        raise HTTPException(status_code=500, detail=COMPUTER_USE_EXECUTION_ERROR) from exc


# ---------------------------------------------------------------------------
# Programmatic helper
# ---------------------------------------------------------------------------

async def run_computer_task(
    prompt: str,
    model: str = "claude-opus-4-6",
    max_iterations: int = 10,
    api_key: str | None = None,
) -> ComputerUseResponse:
    """
    Run a computer use task and return structured results.

    Args:
        prompt: Task description for Claude.
        model: Claude model to use.
        max_iterations: Max agentic loop iterations.
        api_key: API key (falls back to ANTHROPIC_API_KEY env var).
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ComputerUseConfigurationError("ANTHROPIC_API_KEY not set")
    if ComputerUseClient is None or async_sampling_loop is None:
        raise ComputerUseConfigurationError("claude-computer-use is not installed")

    client = ComputerUseClient(
        api_key=key,
        model=model,
        display_width_px=1024,
        display_height_px=768,
    )

    messages = await async_sampling_loop(
        client=client,
        prompt=prompt,
        max_iterations=max_iterations,
    )

    # Extract final text
    final_text = _extract_final_text(messages)

    return ComputerUseResponse(
        success=True,
        final_text=final_text,
        iterations=len([m for m in messages if m.get("role") == "assistant"]),
    )


def _extract_final_text(messages: list[dict]) -> str | None:
    """Extract the last assistant text block from a message history."""
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            for block in msg.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    return block["text"]
    return None

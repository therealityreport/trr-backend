from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class TwitterRuntimeState:
    request_count: int = 0
    transport: str = "graphql"
    fallback_chain: list[str] = field(default_factory=lambda: ["graphql"])
    stop_reason: str | None = None
    retryable: bool = False
    complete: bool = False


def build_fallback_chain(*, retrieval_mode: str | None, fallback_attempts: list[str]) -> list[str]:
    chain = ["graphql"]
    for attempt in fallback_attempts or []:
        normalized = str(attempt or "").strip().lower()
        if normalized and normalized not in chain:
            chain.append(normalized)
    normalized_mode = str(retrieval_mode or "").strip().lower()
    if normalized_mode and normalized_mode not in chain:
        chain.append(normalized_mode)
    return chain

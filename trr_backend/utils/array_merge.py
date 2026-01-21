from __future__ import annotations


def merge_str_arrays(existing: object, incoming: list[str] | None) -> list[str] | None:
    if not incoming:
        return None
    existing_values = [
        str(v).strip()
        for v in (existing if isinstance(existing, list) else [])
        if isinstance(v, str) and str(v).strip()
    ]
    incoming_values = [str(v).strip() for v in incoming if isinstance(v, str) and str(v).strip()]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


def merge_int_arrays(existing: object, incoming: list[int] | None) -> list[int] | None:
    if not incoming:
        return None
    existing_values = [v for v in (existing if isinstance(existing, list) else []) if isinstance(v, int)]
    incoming_values = [v for v in incoming if isinstance(v, int)]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged or merged == sorted(existing_values):
        return None
    return merged

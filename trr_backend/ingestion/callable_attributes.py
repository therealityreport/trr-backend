"""Typed access for the duck-typed attributes that source request hooks carry."""

from typing import Any, TypeVar

_T = TypeVar("_T")


def with_attributes(target: _T, **attributes: Any) -> _T:
    """Attach request-control attributes (permit, clock, deadline hooks) to a callable and return it.

    Source adapters read these optionally with ``getattr``; the hook itself stays a plain function.
    """
    for name, value in attributes.items():
        setattr(target, name, value)
    return target

"""Small helpers for deferring optional module imports until first use."""

from __future__ import annotations

import importlib
from typing import Any


class LazyModule:
    """Proxy a module import behind the first attribute access."""

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name
        self._module: Any | None = None

    def _load(self) -> Any:
        if self._module is None:
            self._module = importlib.import_module(self.module_name)
        return self._module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)

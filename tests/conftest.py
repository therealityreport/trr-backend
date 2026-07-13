from __future__ import annotations

import os

import pytest

_PRISTINE_ENVIRON = dict(os.environ)


@pytest.fixture
def pristine_environ() -> dict[str, str]:
    return _PRISTINE_ENVIRON

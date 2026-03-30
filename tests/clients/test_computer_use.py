from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app as main_app
from trr_backend.clients import computer_use


def _build_router_app() -> FastAPI:
    app = FastAPI()
    app.include_router(computer_use.router, prefix="/api/v1/computer-use")
    return app


def test_run_computer_task_returns_bounded_summary(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    class DummyClient:
        def __init__(self, **_: object) -> None:
            pass

    async def fake_sampling_loop(*, client, prompt: str, max_iterations: int):  # noqa: ARG001
        assert prompt == "Take a screenshot"
        assert max_iterations == 3
        return [
            {"role": "user", "content": [{"type": "text", "text": "Take a screenshot"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "Opened the page."}]},
            {"role": "assistant", "content": [{"type": "text", "text": "Captured the screenshot."}]},
        ]

    monkeypatch.setattr(computer_use, "ComputerUseClient", DummyClient)
    monkeypatch.setattr(computer_use, "async_sampling_loop", fake_sampling_loop)

    result = computer_use.run_computer_task
    response = __import__("asyncio").run(result("Take a screenshot", max_iterations=3))

    assert response.success is True
    assert response.final_text == "Captured the screenshot."
    assert response.iterations == 2
    assert "messages" not in response.model_dump()


def test_router_rejects_unauthenticated_requests(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    client = TestClient(_build_router_app())

    response = client.post("/api/v1/computer-use/run", json={"prompt": "Take a screenshot"})

    assert response.status_code == 401


def test_router_sanitizes_execution_errors(monkeypatch) -> None:
    app = _build_router_app()
    app.dependency_overrides[require_internal_admin] = lambda: {"id": "admin-1", "role": "admin"}
    client = TestClient(app)

    async def fake_run_computer_task(**_: object):
        raise RuntimeError("internal stack detail should not leak")

    monkeypatch.setattr(computer_use, "run_computer_task", fake_run_computer_task)

    response = client.post("/api/v1/computer-use/run", json={"prompt": "Take a screenshot"})

    assert response.status_code == 500
    assert response.json() == {"detail": computer_use.COMPUTER_USE_EXECUTION_ERROR}


def test_router_returns_bounded_json_response(monkeypatch) -> None:
    app = _build_router_app()
    app.dependency_overrides[require_internal_admin] = lambda: {"id": "admin-1", "role": "admin"}
    client = TestClient(app)

    async def fake_run_computer_task(**_: object):
        return computer_use.ComputerUseResponse(
            success=True,
            final_text="Completed.",
            iterations=4,
        )

    monkeypatch.setattr(computer_use, "run_computer_task", fake_run_computer_task)

    response = client.post("/api/v1/computer-use/run", json={"prompt": "Take a screenshot"})

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "final_text": "Completed.",
        "iterations": 4,
    }


def test_computer_use_router_is_not_registered_in_main_app() -> None:
    assert all(not route.path.startswith("/api/v1/computer-use") for route in main_app.routes)

import surfra
from surfra.sdk import Request, bind_request, reset_registry, unbind_request
from surfra.worker import deployment_mode


def test_decorated_progress_records_the_current_task(monkeypatch):
    reset_registry()
    monkeypatch.delenv("SURFRA_WORKER_MODE", raising=False)

    @surfra.progress
    def progress_fn(progress: float):
        return progress

    request = Request(input={})
    token = bind_request(request)
    try:
        assert progress_fn(0.25) == 0.25
    finally:
        unbind_request(token)
    assert request.events[0].fraction == 0.25


def test_worker_mode_defaults_to_isolated(monkeypatch):
    monkeypatch.delenv("SURFRA_WORKER_MODE", raising=False)
    assert deployment_mode() == "isolated"
    monkeypatch.setenv("SURFRA_WORKER_MODE", "joint")
    assert deployment_mode() == "joint"

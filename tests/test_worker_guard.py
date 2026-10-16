import httpx
import pytest

from surfra import worker
from surfra.worker import WorkerClient


def _status(code: int, text: str = "") -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.surfra.io/v1/workers/next")
    return httpx.HTTPStatusError("boom", request=request, response=httpx.Response(code, text=text, request=request))


def _raise(exc):
    def step():
        raise exc

    return step


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(worker.time, "sleep", lambda _s: None)
    return WorkerClient("https://api.surfra.io", "sfp_test")


def test_a_control_plane_500_waits_and_retries(client):
    assert client.guard(_raise(_status(500)), 1) is False
    assert client.backoff == 2.0
    assert client.guard(lambda: True, 1) is True
    assert client.backoff == 1.0


def test_a_network_error_waits_and_retries(client):
    assert client.guard(_raise(httpx.ConnectError("down")), 1) is False


def test_a_dropped_worker_registers_again(client, monkeypatch):
    calls = []
    monkeypatch.setattr(client, "register", lambda copies: calls.append(copies) or "sfw_new")
    assert client.guard(_raise(_status(401, '{"detail":"worker is not registered on this replica"}')), 3) is False
    assert calls == [3]


def test_a_stopped_replica_waits_for_shutdown(client, monkeypatch):
    class Stop(Exception):
        pass

    def stop():
        raise Stop

    monkeypatch.setattr(worker, "_wait_for_shutdown", stop)
    with pytest.raises(Stop):
        client.guard(_raise(_status(401, '{"detail":"unknown replica token"}')), 1)


def test_a_bad_request_still_raises(client):
    with pytest.raises(httpx.HTTPStatusError):
        client.guard(_raise(_status(400, "bad")), 1)

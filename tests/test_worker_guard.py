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


def test_a_finished_result_is_sent_again_until_surfra_takes_it(client, monkeypatch):
    answers = [_status(503, '{"error":{"type":"SERVER_BUSY"}}'), httpx.ConnectError("down"), None]
    sent = []

    def complete(stage_id, output):
        sent.append(stage_id)
        answer = answers.pop(0)
        if answer is not None:
            raise answer

    monkeypatch.setattr(client, "complete", complete)
    client.settle([{"id": "stg_1"}], outputs=[{"ok": True}])
    assert sent == ["stg_1", "stg_1", "stg_1"]


def test_a_result_surfra_no_longer_holds_is_dropped_not_retried(client, monkeypatch):
    sent = []

    def complete(stage_id, output):
        sent.append(stage_id)
        raise _status(409, '{"detail":"lease fence rejected the completion"}')

    monkeypatch.setattr(client, "complete", complete)
    client.settle([{"id": "stg_1"}, {"id": "stg_2"}], outputs=[1, 2])
    assert sent == ["stg_1", "stg_2"]


def test_a_result_is_given_up_after_the_retry_window(client, monkeypatch):
    clock = iter(range(0, 10_000, 120))
    monkeypatch.setattr(worker.time, "monotonic", lambda: float(next(clock)))
    monkeypatch.setattr(client, "fail", lambda stage_id, error: (_ for _ in ()).throw(_status(503)))
    with pytest.raises(httpx.HTTPStatusError):
        client.settle([{"id": "stg_1"}], error="boom")

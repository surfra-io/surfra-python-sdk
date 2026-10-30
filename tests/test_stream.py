"""The replica stream client against a scripted connection: credit, calls, the outbox, reconnects, fallback."""

import json
import queue
import threading
import time

import pytest

from surfra import stream as stream_module
from surfra.stream import ReplicaGone, ReplicaStream, WorkerUnknown, stream_url


class FakeConn:
    """Surfra's side of one connection. Tests read what the supervisor sent and push what Surfra says."""

    def __init__(self) -> None:
        self.sent: "queue.Queue[dict]" = queue.Queue()
        self.inbox: "queue.Queue[str | Exception]" = queue.Queue()
        self.closed = False

    def send(self, raw: str) -> None:
        if self.closed:
            raise ConnectionError("closed")
        self.sent.put(json.loads(raw))

    def recv(self, timeout: float | None = None) -> str:
        try:
            item = self.inbox.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError() from None
        if isinstance(item, Exception):
            raise item
        return item

    def push(self, message: dict) -> None:
        self.inbox.put(json.dumps(message))

    def drop(self) -> None:
        self.closed = True
        self.inbox.put(ConnectionError("dropped"))

    def close(self) -> None:
        self.closed = True

    def until(self, kind: str, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            message = self.sent.get(timeout=max(0.01, deadline - time.monotonic()))
            if message["type"] == kind:
                return message


class Server:
    """Hands out FakeConns in order, answering each hello with welcome."""

    def __init__(self) -> None:
        self.conns: "queue.Queue[FakeConn]" = queue.Queue()
        self.refuse: Exception | None = None

    def connect(self, url: str) -> FakeConn:
        if self.refuse is not None:
            raise self.refuse
        conn = FakeConn()
        conn.push({"type": "welcome", "v": 1, "beat": 0.2, "rotate": 0})
        self.conns.put(conn)
        return conn

    def next(self, timeout: float = 5.0) -> FakeConn:
        return self.conns.get(timeout=timeout)


def _wait(check, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        time.sleep(0.01)


@pytest.fixture()
def server():
    return Server()


@pytest.fixture()
def replica(server):
    sent_http: list[dict] = []
    s = ReplicaStream("https://api.surfra.io", "sfp_test", send_http=sent_http.append, connect=server.connect)
    s.sent_http = sent_http
    s.attach("sfw_a")
    s.start()
    yield s
    s.stop()


def test_urls_are_tls_except_to_this_machine():
    assert stream_url("https://api.surfra.io") == "wss://api.surfra.io/v1/workers/stream"
    assert stream_url("http://127.0.0.1:8080") == "ws://127.0.0.1:8080/v1/workers/stream"
    assert stream_url("http://api.surfra.io") is None


def test_credit_brings_calls_and_results_wait_for_their_ack(server, replica):
    conn = server.next()
    hello = conn.until("hello")
    assert hello["replica_token"] == "sfp_test" and hello["workers"] == ["sfw_a"]
    _wait(replica.ready)
    got: list = []
    thread = threading.Thread(target=lambda: got.append(replica.take("sfw_a", 1, 5)))
    thread.start()
    assert conn.until("credit") == {"type": "credit", "worker": "sfw_a", "n": 1}
    conn.push({"type": "calls", "worker": "sfw_a", "stages": [{"id": "s1", "input": {}}]})
    thread.join(5)
    assert [stage["id"] for stage in got[0]] == ["s1"]
    replica.begin("sfw_a", got[0])
    assert replica.held() == {"sfw_a": ["s1"]}
    replica.result("sfw_a", "s1", output={"ok": True})
    done = conn.until("done")
    assert done["stage"] == "s1" and done["output"] == {"ok": True}
    assert replica.held() == {"sfw_a": ["s1"]}  # until Surfra acknowledges it
    conn.push({"type": "ack", "seq": done["seq"], "ok": True})
    _wait(lambda: not replica.outbox)
    assert replica.held() == {"sfw_a": []}


def test_a_dropped_connection_resends_unacknowledged_results(server, replica):
    conn = server.next()
    _wait(replica.ready)
    replica.begin("sfw_a", [{"id": "s1"}])
    replica.result("sfw_a", "s1", output=1)
    conn.until("done")
    conn.drop()
    again = server.next(10)
    hello = again.until("hello")
    assert hello["held"] == {"sfw_a": ["s1"]}
    done = again.until("done")
    assert done["stage"] == "s1"
    again.push({"type": "ack", "seq": done["seq"], "ok": False, "discard": "lease fence rejected the completion"})
    _wait(lambda: not replica.outbox)


def test_a_waiting_worker_gets_its_credit_back_after_a_reconnect(server, replica):
    conn = server.next()
    _wait(replica.ready)
    thread = threading.Thread(target=lambda: replica.take("sfw_a", 2, 0.3))
    thread.start()
    conn.until("credit")
    thread.join(5)
    conn.drop()
    again = server.next(10)
    assert again.until("hello")["credit"] == {"sfw_a": 2}


def test_beats_list_what_each_worker_holds(server, replica):
    conn = server.next()
    _wait(replica.ready)
    replica.begin("sfw_a", [{"id": "s9"}])
    assert conn.until("beat")["held"] == {"sfw_a": ["s9"]}


def test_a_closed_worker_raises_unknown_and_is_replaced(server, replica):
    conn = server.next()
    _wait(replica.ready)
    conn.push({"type": "unknown", "workers": ["sfw_a"]})
    _wait(lambda: "sfw_a" in replica.unknown)
    with pytest.raises(WorkerUnknown):
        replica.take("sfw_a", 1, 0.1)
    replica.replace("sfw_a", "sfw_b")
    assert conn.until("add")["workers"] == ["sfw_b"]


def test_gone_ends_the_stream(server, replica):
    conn = server.next()
    _wait(replica.ready)
    conn.push({"type": "gone"})
    _wait(lambda: replica.gone)
    with pytest.raises(ReplicaGone):
        replica.take("sfw_a", 1, 0.1)


def test_an_older_control_plane_sends_workers_to_http(server, monkeypatch):
    class Refused(Exception):
        class response:
            status_code = 404

    server.refuse = Refused()
    replica = ReplicaStream("https://api.surfra.io", "sfp_test", connect=server.connect)
    replica.start()
    _wait(lambda: replica.http_until > 0)
    assert not replica.ready()
    replica.stop()


def test_results_go_over_http_when_no_connection_comes_back(server, replica, monkeypatch):
    monkeypatch.setattr(stream_module, "HTTP_AFTER", 0.0)
    conn = server.next()
    _wait(replica.ready)
    replica.begin("sfw_a", [{"id": "s1"}])
    replica.result("sfw_a", "s1", error="boom")
    conn.until("fail")
    server.refuse = ConnectionError("down")
    conn.drop()
    _wait(lambda: replica.sent_http, timeout=10)
    assert replica.sent_http[0]["stage"] == "s1" and replica.sent_http[0]["type"] == "fail"
    _wait(lambda: not replica.outbox)

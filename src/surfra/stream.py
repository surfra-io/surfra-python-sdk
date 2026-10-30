"""The replica stream: one WebSocket to Surfra carries every worker in this container.

Over HTTP each call costs a worker two round trips it waits on: ask for work, then report the result. On
the stream a worker that is free grants credit, Surfra pushes the next call as soon as one is queued, and a
finished result goes up without the worker waiting for the answer; an outbox keeps it until Surfra
acknowledges it and sends it again on the next connection if this one drops.

Nothing depends on the connection staying up. Leases belong to the call and the worker, so a stream that
drops, rotates, or meets an API restart reconnects (at once after a restart, with backoff otherwise) and says
what it holds; Surfra puts back anything it sent that never arrived. A control plane without the stream, or a
stream that keeps failing, sends the workers back to the HTTP poll, and the stream is tried again later.

Only TLS (wss://) is used, except to a control plane on this machine in local development.
"""

from __future__ import annotations

import json
import random
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse

from surfra import logship
from surfra.version import HEADERS

# Seconds without a connection before results waiting in the outbox go over HTTP instead.
HTTP_AFTER = 15.0
# A control plane that refuses the stream (an older one) is asked again after this long.
RETRY_REFUSED = 300.0
# Consecutive failed connections before the workers go back to HTTP for a while.
FAILURES_BEFORE_HTTP = 3
RETRY_FAILING = 60.0
BACKOFF_MAX = 30.0
MAX_MESSAGE = 96 << 20  # a batch of 64 inputs of up to 1 MB each, with room


class WorkerUnknown(Exception):
    """Surfra closed this worker (it requeued what it held). Register again."""


class ReplicaGone(Exception):
    """Surfra stopped this replica."""


class _Worker:
    def __init__(self) -> None:
        self.buffer: list[dict[str, Any]] = []  # calls received and not started
        self.running: set[str] = set()
        self.want = 0  # slots this worker is waiting to fill
        self.owed = 0  # credit granted on this connection and not yet filled


def stream_url(master: str) -> str | None:
    """wss:// for the control plane, or None when only plain ws:// to another machine is possible."""

    url = urlparse(master)
    if url.scheme == "https":
        return f"wss://{url.netloc}{url.path.rstrip('/')}/v1/workers/stream"
    if url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1", "::1"):
        return f"ws://{url.netloc}{url.path.rstrip('/')}/v1/workers/stream"
    return None


class ReplicaStream:
    def __init__(self, master: str, replica_token: str, *, send_http: Callable[[dict[str, Any]], None] | None = None, connect: Any = None):
        self.url = stream_url(master)
        self.token = replica_token
        self.send_http = send_http  # sends one outbox item over HTTP; raises when Surfra did not take it
        self._connect = connect
        self.cond = threading.Condition()
        self.workers: dict[str, _Worker] = {}
        self.unknown: set[str] = set()
        self.gone = False
        self.outbox: dict[int, dict[str, Any]] = {}
        self.seq = 0
        self.conn: Any = None
        self.connected = False
        self.down_since = time.monotonic()
        self.http_until = 0.0  # use HTTP until this time
        self.beat = 10.0
        self.rotate = 0.0
        self.stopped = threading.Event()
        self._send_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="surfra-stream", daemon=True)

    # Called by worker threads.

    def start(self) -> "ReplicaStream":
        if self.url is not None:
            self._thread.start()
        return self

    def stop(self) -> None:
        self.stopped.set()
        conn = self.conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def ready(self) -> bool:
        return self.connected and not self.gone

    def attach(self, worker_id: str) -> None:
        with self.cond:
            self.workers.setdefault(worker_id, _Worker())
            self.unknown.discard(worker_id)
        self._send({"type": "add", "workers": [worker_id], "credit": {}})

    def replace(self, old: str, new: str) -> None:
        with self.cond:
            self.workers.pop(old, None)
            self.unknown.discard(old)
        self.attach(new)

    def take(self, worker_id: str, limit: int, wait: float) -> list[dict[str, Any]]:
        """Calls for this worker, up to limit, waiting up to wait seconds. They stay buffered until begin."""

        deadline = time.monotonic() + wait
        with self.cond:
            self._check(worker_id)
            worker = self.workers.setdefault(worker_id, _Worker())
            worker.want = limit
            grant = limit - len(worker.buffer) - worker.owed if self.connected else 0
            if grant > 0:
                worker.owed += grant
        if grant > 0:
            self._send({"type": "credit", "worker": worker_id, "n": grant})
        with self.cond:
            while not worker.buffer and self.connected and not self.gone and worker_id not in self.unknown:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                self.cond.wait(left)
            self._check(worker_id)
            return list(worker.buffer[:limit])

    def begin(self, worker_id: str, stages: list[dict[str, Any]]) -> None:
        """These calls run now. Calls taken over HTTP are recorded too, so beats list them."""

        ids = {stage["id"] for stage in stages}
        with self.cond:
            worker = self.workers.setdefault(worker_id, _Worker())
            worker.buffer = [stage for stage in worker.buffer if stage["id"] not in ids]
            worker.running |= ids
            worker.want = 0

    def result(self, worker_id: str, stage_id: str, *, output: Any = None, error: str | None = None, counts: bool = True) -> None:
        """Queue a finished call's result. Returns at once; the outbox sends it until Surfra takes it."""

        with self.cond:
            self.seq += 1
            seq = self.seq
            item: dict[str, Any] = {"seq": seq, "worker": worker_id, "stage": stage_id, "since": time.monotonic()}
            if error is None:
                item.update(type="done", output=output)
            else:
                item.update(type="fail", error=error, counts=counts)
            self.outbox[seq] = item
            worker = self.workers.get(worker_id)
            if worker is not None:
                worker.running.discard(stage_id)
        self._send(_wire(item))

    def finished(self, worker_id: str, stage_ids: list[str]) -> None:
        """Calls whose results went over HTTP: nothing more to report for them."""

        with self.cond:
            worker = self.workers.get(worker_id)
            if worker is not None:
                worker.running -= set(stage_ids)

    def progress(self, worker_id: str, stage_id: str, fraction: float) -> bool:
        if not self.connected:
            return False
        return self._send({"type": "progress", "worker": worker_id, "stage": stage_id, "fraction": fraction})

    def held(self) -> dict[str, list[str]]:
        with self.cond:
            out = {worker_id: [stage["id"] for stage in w.buffer] + list(w.running) for worker_id, w in self.workers.items() if worker_id not in self.unknown}
            for item in self.outbox.values():
                if item["worker"] in out:
                    out[item["worker"]].append(item["stage"])
            return out

    def _check(self, worker_id: str) -> None:
        if self.gone:
            raise ReplicaGone()
        if worker_id in self.unknown:
            raise WorkerUnknown(worker_id)

    def _send(self, message: dict[str, Any]) -> bool:
        conn = self.conn
        if conn is None or not self.connected:
            return False
        try:
            with self._send_lock:
                conn.send(json.dumps(message, default=str))
            return True
        except Exception:
            return False

    # The stream's own thread.

    def _open(self) -> Any:
        if self._connect is not None:
            return self._connect(self.url)
        from websockets.sync.client import connect

        return connect(
            self.url,
            additional_headers=HEADERS,
            open_timeout=10,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=MAX_MESSAGE,
            compression=None,
        )

    def _run(self) -> None:
        backoff = 0.5
        failures = 0
        while not self.stopped.is_set():
            if time.monotonic() < self.http_until:
                self._flush_http()
                self.stopped.wait(1.0)
                continue
            quick = False
            try:
                quick = self._session()
                failures = 0
                backoff = 0.5
            except _Refused as exc:
                logship.note(f"Surfra did not take the replica stream ({exc}); using HTTP polls, trying again in {RETRY_REFUSED:.0f}s")
                self.http_until = time.monotonic() + RETRY_REFUSED
                continue
            except ReplicaGone:
                return
            except Exception as exc:
                failures += 1
                if failures >= FAILURES_BEFORE_HTTP:
                    logship.note(f"replica stream unavailable ({type(exc).__name__}: {exc}); using HTTP polls for {RETRY_FAILING:.0f}s")
                    self.http_until = time.monotonic() + RETRY_FAILING
                    failures = 0
                    continue
            if self.stopped.is_set():
                return
            if not quick:
                self._flush_http()
                self.stopped.wait(backoff * random.uniform(0.5, 1.5))
                backoff = min(backoff * 2, BACKOFF_MAX)

    def _session(self) -> bool:
        """One connection, from hello until it closes. True when the next one should open at once."""

        try:
            conn = self._open()
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (400, 401, 403, 404, 405, 426):
                raise _Refused(f"HTTP {status}") from exc
            raise
        opened = time.monotonic()
        with self.cond:
            for worker in self.workers.values():
                worker.owed = 0
                grant = max(0, worker.want - len(worker.buffer))
                worker.owed = grant
            credit = {worker_id: w.owed for worker_id, w in self.workers.items() if w.owed and worker_id not in self.unknown}
            workers = [worker_id for worker_id in self.workers if worker_id not in self.unknown]
        conn.send(json.dumps({"type": "hello", "v": 1, "replica_token": self.token, "workers": workers, "held": self.held(), "credit": credit}))
        first = json.loads(conn.recv(timeout=15))
        if first.get("type") == "gone":
            self._gone(conn)
        if first.get("type") != "welcome":
            conn.close()
            raise RuntimeError(f"unexpected {first.get('type')!r} instead of welcome")
        self.beat = float(first.get("beat") or 10)
        self.rotate = float(first.get("rotate") or 0) * random.uniform(0.85, 1.0)
        with self.cond:
            self.conn = conn
            self.connected = True
            pending = [dict(item) for item in self.outbox.values()]
            self.cond.notify_all()
        for item in pending:
            self._send(_wire(item))
        next_beat = time.monotonic() + self.beat
        draining = False
        try:
            while not self.stopped.is_set():
                now = time.monotonic()
                if now >= next_beat:
                    next_beat = now + self.beat
                    self._send({"type": "beat", "held": self.held()})
                    self._resend_due(now)
                if self.rotate and not draining and now - opened >= self.rotate:
                    draining = True
                    self._send({"type": "drain"})
                try:
                    raw = conn.recv(timeout=max(0.05, min(1.0, next_beat - time.monotonic())))
                except TimeoutError:
                    continue
                message = json.loads(raw)
                kind = message.get("type")
                if kind == "calls":
                    self._calls(message)
                elif kind == "ack":
                    self._ack(message)
                elif kind == "unknown":
                    with self.cond:
                        for worker_id in message.get("workers") or []:
                            self.unknown.add(worker_id)
                            worker = self.workers.get(worker_id)
                            if worker is not None:
                                worker.buffer, worker.owed = [], 0
                        self.cond.notify_all()
                elif kind == "gone":
                    self._gone(conn)
                elif kind in ("drained", "goaway"):
                    return True
            return True
        except ReplicaGone:
            raise
        except Exception as exc:
            code = getattr(getattr(exc, "rcvd", None), "code", None)
            # 1012 and 1001: the API instance is restarting or going away. Another one answers now.
            return code in (1001, 1012)
        finally:
            with self.cond:
                self.connected = False
                self.conn = None
                self.down_since = time.monotonic()
                for worker in self.workers.values():
                    worker.owed = 0
                self.cond.notify_all()
            try:
                conn.close()
            except Exception:
                pass

    def _calls(self, message: dict[str, Any]) -> None:
        stages = message.get("stages") or []
        with self.cond:
            worker = self.workers.get(message.get("worker") or "")
            if worker is None:
                return  # a worker replaced since; Surfra puts the call back after the next beat
            worker.buffer.extend(stages)
            worker.owed = max(0, worker.owed - len(stages))
            self.cond.notify_all()

    def _ack(self, message: dict[str, Any]) -> None:
        with self.cond:
            item = self.outbox.get(message.get("seq"))
            if item is None:
                return
            if message.get("ok") or message.get("discard"):
                self.outbox.pop(item["seq"], None)
                if message.get("discard"):
                    logship.note(f"Surfra no longer holds request stage {item['stage']} for this worker ({message['discard']}); result discarded")
                return
            item["due"] = time.monotonic() + float(message.get("retry") or 2)

    def _resend_due(self, now: float) -> None:
        with self.cond:
            due = [dict(item) for item in self.outbox.values() if item.get("due") and item["due"] <= now]
            for item in due:
                self.outbox[item["seq"]]["due"] = None
        for item in due:
            self._send(_wire(item))

    def _gone(self, conn: Any) -> None:
        with self.cond:
            self.gone = True
            self.cond.notify_all()
        try:
            conn.close()
        except Exception:
            pass
        raise ReplicaGone()

    def _flush_http(self) -> None:
        """Results that waited too long for a connection go over HTTP."""

        if self.send_http is None or self.connected:
            return
        if time.monotonic() - self.down_since < HTTP_AFTER and time.monotonic() >= self.http_until:
            return
        with self.cond:
            items = [dict(item) for item in self.outbox.values()]
        for item in items:
            try:
                self.send_http(item)
            except Exception as exc:
                logship.note(f"could not send a finished result over HTTP either ({type(exc).__name__}); will try again")
                return
            with self.cond:
                self.outbox.pop(item["seq"], None)


class _Refused(Exception):
    pass


def _wire(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key not in ("since", "due")}

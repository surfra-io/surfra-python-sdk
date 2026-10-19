import multiprocessing
import queue
import textwrap
import threading
import time

import httpx
import pytest

from surfra import worker
from surfra.sdk import handler, reset_registry
from surfra.worker import Heartbeat, WorkerClient


def _status(code: int, text: str = "") -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.surfra.io/v1/workers/complete")
    return httpx.HTTPStatusError("boom", request=request, response=httpx.Response(code, text=text, request=request))


class FakePlane(WorkerClient):
    """A worker client whose control plane is a list of calls."""

    def __init__(self, stages=None):
        super().__init__("https://api.surfra.io", "sfp_test")
        self.worker_id = "sfw_test"
        self.queued = list(stages or [])
        self.beats: list[float] = []
        self.completed: dict[str, object] = {}
        self.failed: dict[str, str] = {}
        self.refuse: set[str] = set()

    def next_stages(self, limit, *, wait=0.0):
        taken, self.queued = self.queued[:limit], self.queued[limit:]
        return taken

    def heartbeat(self, held):
        self.beats.append(time.monotonic())

    def complete(self, stage_id, output):
        if stage_id in self.refuse:
            raise _status(409, '{"detail":"lease fence rejected the completion"}')
        self.completed[stage_id] = output

    def fail(self, stage_id, error, *, counts=True):
        if stage_id in self.refuse:
            raise _status(409, '{"detail":"lease fence rejected the failure"}')
        self.failed[stage_id] = error

    def report_progress(self, stage_id, fraction):
        pass


@pytest.fixture(autouse=True)
def _fresh_registry(monkeypatch):
    reset_registry()
    monkeypatch.setattr(worker, "HEARTBEAT_SECONDS", 0.1)
    monkeypatch.delenv("SURFRA_EXECUTION_TIMEOUT", raising=False)
    yield
    reset_registry()


def test_a_long_request_keeps_heartbeating_while_the_handler_runs():
    @handler
    def slow(request):
        time.sleep(0.6)
        return {"done": request.input["n"]}

    plane = FakePlane([{"id": "st_1", "input": {"n": 1}}])
    assert plane.step() is True
    assert plane.completed == {"st_1": {"done": 1}}
    # The handler held the loop thread for 0.6s. The heartbeat thread reported in throughout.
    assert len(plane.beats) >= 3


def test_a_request_past_the_execution_timeout_fails_and_its_late_result_is_dropped(monkeypatch):
    monkeypatch.setenv("SURFRA_EXECUTION_TIMEOUT", "0.3")

    @handler
    def stuck(request):
        time.sleep(1.0)
        return {"late": True}

    plane = FakePlane([{"id": "st_1", "input": {}}])
    assert plane.step() is True
    assert "execution timeout" in plane.failed["st_1"]
    assert plane.completed == {}


def test_a_stage_surfra_took_back_does_not_lose_the_rest_of_the_batch():
    plane = FakePlane()
    plane.refuse = {"st_2"}
    plane.settle([{"id": "st_1"}, {"id": "st_2"}, {"id": "st_3"}], outputs=["a", "b", "c"])
    assert plane.completed == {"st_1": "a", "st_3": "c"}


def test_a_dropped_worker_stops_heartbeating_and_discards_its_result():
    class Dropped(FakePlane):
        def heartbeat(self, held):
            raise _status(401, '{"detail":"worker is not registered on this replica"}')

    plane = Dropped()
    with Heartbeat(plane, 1, timeout=0, interval=0.05) as beat:
        time.sleep(0.2)
    assert beat.lost


def test_the_execution_timeout_reads_the_autoscaler_environment(monkeypatch):
    assert worker.execution_timeout() == worker.DEFAULT_EXECUTION_TIMEOUT
    monkeypatch.setenv("SURFRA_EXECUTION_TIMEOUT", "45")
    assert worker.execution_timeout() == 45.0
    monkeypatch.setenv("SURFRA_EXECUTION_TIMEOUT", "0")
    assert worker.execution_timeout() == 0.0


APP = textwrap.dedent(
    """
    import os
    import time

    import surfra


    @surfra.handler
    def run(request):
        mode = request.input.get("mode")
        if mode == "hang":
            time.sleep(60)
        if mode == "die":
            os._exit(3)
        return {"pid": os.getpid()}
    """
)


def test_a_hung_or_crashed_worker_process_is_replaced(tmp_path, monkeypatch):
    app = tmp_path / "app.py"
    app.write_text(APP)
    monkeypatch.setenv("SURFRA_EXECUTION_TIMEOUT", "2")
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    ready: "queue.Queue[dict]" = queue.Queue()
    pending: dict = {}
    answers: dict = {}
    lock = threading.Lock()
    slot = worker._Slot(ctx, str(app), "isolated", "sfp_test", results, ready)
    try:
        worker._await_ready(results, [slot.process], 60)
        threading.Thread(target=worker._collect, args=(results, ready, pending, answers, lock), daemon=True).start()

        plane = FakePlane([{"id": "st_ok", "input": {}}])
        assert worker._step_remote(plane, slot, pending, answers, lock)
        first_pid = plane.completed["st_ok"]["pid"]

        plane.queued = [{"id": "st_hang", "input": {"mode": "hang"}}]
        began = time.monotonic()
        assert worker._step_remote(plane, slot, pending, answers, lock)
        assert time.monotonic() - began < 30
        assert "execution timeout" in plane.failed["st_hang"]
        assert slot.generation == 1
        assert slot.process.is_alive()

        plane.queued = [{"id": "st_die", "input": {"mode": "die"}}]
        assert worker._step_remote(plane, slot, pending, answers, lock)
        assert "exited (exit code 3)" in plane.failed["st_die"]
        assert slot.generation == 2

        plane.queued = [{"id": "st_after", "input": {}}]
        assert worker._step_remote(plane, slot, pending, answers, lock)
        assert plane.completed["st_after"]["pid"] != first_pid
        assert len(plane.beats) >= 1
    finally:
        worker._stop_process(slot.process)


def test_a_worker_process_that_dies_while_loading_does_not_hang_the_replica(tmp_path):
    app = tmp_path / "broken.py"
    app.write_text("raise RuntimeError('missing weights')\n")
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    slot = worker._Slot(ctx, str(app), "isolated", "sfp_test", results, queue.Queue())
    try:
        with pytest.raises(worker._StartFailed, match="exited while loading"):
            worker._await_ready(results, [slot.process], 60)
    finally:
        worker._stop_process(slot.process)

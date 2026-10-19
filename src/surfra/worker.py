"""Cloud worker loop. The container command is `surfra worker`.

The parent process talks to the control plane and keeps the replica token.
Batching shares one loaded model. Concurrent workers follow the app's worker
mode, which is set on the deployment. Isolated starts one process per worker,
and each process loads its own weights. Joint keeps those workers as threads
in one process. Ray is not used.
"""

from __future__ import annotations

import multiprocessing
import os
import queue
import random
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from surfra import logship, usage
from surfra.runner import load_app, run_handler_batch, run_warmup
from surfra.sdk import adopt_handler_spec, bind_progress_hook, registered_handler_spec, take_batch, unbind_progress_hook

_BAKED = ("SURFRA_MASTER", "SURFRA_REPLICA_TOKEN", "SURFRA_ENTRYPOINT")
# An idle poll waits on Surfra this long for work, so a worker does not ask every few seconds.
POLL_WAIT = 20.0
# A busy worker tells Surfra it is alive this often. Surfra counts a worker silent after 45 seconds.
HEARTBEAT_SECONDS = 15.0
# The Go supervisor stops a request after 20 minutes unless the app sets execution_timeout_sec.
DEFAULT_EXECUTION_TIMEOUT = 1200.0
# How long a replacement worker process may take to load the app before the container restarts.
RESTART_READY_SECONDS = 900.0


def execution_timeout() -> float:
    """Seconds one request may run. 0 turns the limit off."""

    raw = os.environ.get("SURFRA_EXECUTION_TIMEOUT", "").strip()
    try:
        return max(0.0, float(raw)) if raw else DEFAULT_EXECUTION_TIMEOUT
    except ValueError:
        return DEFAULT_EXECUTION_TIMEOUT


class Heartbeat:
    """Keeps a busy worker's heartbeat and leases fresh while a handler runs.

    Runs on its own thread with its own HTTP client, so a handler that holds the GIL in C
    code or keeps a GPU busy for an hour does not stop the worker from reporting in. It
    also watches the execution timeout and calls on_timeout once when a request runs past it.
    """

    def __init__(
        self,
        client: "WorkerClient",
        held: int,
        *,
        timeout: float | None = None,
        on_timeout: Any = None,
        interval: float | None = None,
    ):
        self.client = client
        self.held = max(1, held)
        self.timeout = execution_timeout() if timeout is None else timeout
        self.on_timeout = on_timeout
        self.interval = HEARTBEAT_SECONDS if interval is None else interval
        self.started = time.monotonic()
        self.timed_out = False
        self.lost = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="surfra-heartbeat", daemon=True)

    def __enter__(self) -> "Heartbeat":
        self._thread.start()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def _run(self) -> None:
        next_beat = time.monotonic() + self.interval
        while not self._stop.is_set():
            now = time.monotonic()
            if self.timeout and not self.timed_out and now - self.started >= self.timeout:
                self.timed_out = True
                logship.note(f"request ran past the execution timeout of {self.timeout:.0f}s")
                if self.on_timeout is not None:
                    try:
                        self.on_timeout()
                    except Exception as exc:
                        logship.note(f"stopping the timed-out request failed: {exc}")
            if now >= next_beat and not self.lost:
                next_beat = now + self.interval
                try:
                    self.client.heartbeat(self.held)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 401:
                        # Surfra already gave this work to another worker. Results from here are discarded.
                        self.lost = True
                        logship.note("Surfra dropped this worker while it was busy. Its results will be discarded.")
                except Exception:
                    # A missed beat is fine. Surfra allows three before it gives the work away.
                    pass
            deadline = next_beat
            if self.timeout and not self.timed_out:
                deadline = min(deadline, self.started + self.timeout)
            self._stop.wait(max(0.05, min(1.0, deadline - time.monotonic())))


class WorkerClient:
    def __init__(self, master: str, replica_token: str):
        self.master = master.rstrip("/")
        self.replica_token = replica_token
        self.worker_id: str | None = None
        self.waiting = False
        self.backoff = 1.0
        self._pause: float | None = None
        self._http = httpx.Client(timeout=60)
        # httpx clients are not shared across threads, so heartbeats get their own.
        self._beat_http = httpx.Client(timeout=10)

    def pause(self, interval: float) -> float:
        """Seconds to sleep after a step that ran nothing."""

        pause, self._pause = self._pause, None
        return interval if pause is None else pause

    def note_empty(self, started: float) -> None:
        # Surfra already held the poll open. An older control plane answers at once, so keep the interval then.
        self._pause = 0.0 if time.monotonic() - started >= 1.0 else None

    def note_filling(self, oldest_age_ms: int, wait_ms: int) -> None:
        """A batch is filling. Look again when the oldest call has waited long enough, at most 4 times a second."""

        self._pause = min(0.25, max(0.05, (wait_ms - oldest_age_ms) / 1000)) if oldest_age_ms < wait_ms else 0.25

    def register(self, concurrent_workers: int = 1, *, wait: float = 15.0, outage: float = 300.0) -> str:
        # The control plane can start a worker before the replica row commits as ready.
        deadline = time.monotonic() + wait
        # A control plane that is restarting or unreachable gets a few minutes, like the Go supervisor.
        give_up = time.monotonic() + outage
        backoff = 1.0
        while True:
            try:
                response = self._http.post(
                    f"{self.master}/v1/workers/register",
                    json={"replica_token": self.replica_token, "concurrent_workers": concurrent_workers},
                )
            except httpx.TransportError:
                if time.monotonic() >= give_up:
                    raise
                time.sleep(backoff * random.uniform(0.5, 1.5))
                backoff = min(backoff * 2, 30.0)
                continue
            unknown = response.status_code == 401 and "unknown replica token" in response.text
            if unknown and time.monotonic() < deadline:
                time.sleep(0.5)
                continue
            if (response.status_code >= 500 or response.status_code == 429) and time.monotonic() < give_up:
                time.sleep(backoff * random.uniform(0.5, 1.5))
                backoff = min(backoff * 2, 30.0)
                continue
            break
        response.raise_for_status()
        self.worker_id = response.json()["worker_id"]
        return self.worker_id

    def guard(self, step: Any, copies: int) -> bool:
        """Run one step. Control plane hiccups wait and retry instead of ending the worker."""

        try:
            ran = step()
            self.backoff = 1.0
            return ran
        except httpx.HTTPStatusError as exc:
            status, text = exc.response.status_code, exc.response.text
            if status == 401 and "unknown replica token" in text:
                _wait_for_shutdown()
            if status == 401 and "worker is not registered" in text:
                # Surfra requeued what this worker held. Register again and keep going.
                self.register(copies)
                return False
            if status < 500 and status != 429:
                raise
            reason = f"{status} {exc.response.reason_phrase}"
        except httpx.TransportError as exc:
            reason = type(exc).__name__
        logship.note(f"control plane unavailable ({reason}), retrying in {self.backoff:.0f}s")
        # Jitter keeps every replica from retrying in the same second after an outage.
        time.sleep(self.backoff * random.uniform(0.5, 1.5))
        self.backoff = min(self.backoff * 2, 30.0)
        return False

    def next_stages(self, limit: int, *, wait: float = 0.0) -> list[dict[str, Any]]:
        response = self._http.post(
            f"{self.master}/v1/workers/next",
            json={"replica_token": self.replica_token, "worker_id": self.worker_id, "limit": limit, "wait": wait},
        )
        response.raise_for_status()
        body = response.json()
        stages = body.get("stages")
        if isinstance(stages, list):
            return stages
        stage = body.get("stage")
        return [stage] if stage else []

    def heartbeat(self, held: int) -> None:
        """Report in without taking work. Refreshes the leases on what this worker holds."""

        response = self._beat_http.post(
            f"{self.master}/v1/workers/next",
            json={
                "replica_token": self.replica_token,
                "worker_id": self.worker_id,
                # Never more than this worker holds, so an older control plane that ignores claim hands out nothing new.
                "limit": max(1, held),
                "wait": 0,
                "held": "ids",
                "claim": False,
            },
        )
        response.raise_for_status()

    def next_stage(self) -> dict[str, Any] | None:
        stages = self.next_stages(1)
        return stages[0] if stages else None

    def complete(self, stage_id: str, output: Any) -> None:
        response = self._http.post(
            f"{self.master}/v1/workers/complete",
            json={"replica_token": self.replica_token, "worker_id": self.worker_id, "stage_id": stage_id, "output": output},
        )
        response.raise_for_status()

    def report_progress(self, stage_id: str, fraction: float) -> None:
        try:
            self._http.post(
                f"{self.master}/v1/workers/progress",
                json={
                    "replica_token": self.replica_token,
                    "worker_id": self.worker_id,
                    "stage_id": stage_id,
                    "fraction": fraction,
                },
                timeout=2,
            )
        except Exception:
            return

    def fail(self, stage_id: str, error: str, *, counts: bool = True) -> None:
        response = self._http.post(
            f"{self.master}/v1/workers/fail",
            json={"replica_token": self.replica_token, "worker_id": self.worker_id, "stage_id": stage_id, "error": error, "counts": counts},
        )
        response.raise_for_status()

    def settle(self, stages: list[dict[str, Any]], *, outputs: list[Any] | None = None, error: str | None = None) -> None:
        """Report every stage of a batch. One stage Surfra already took back does not lose the rest."""

        for index, stage in enumerate(stages):
            try:
                if error is not None:
                    self.fail(stage["id"], error)
                else:
                    self.complete(stage["id"], (outputs or [])[index])
            except httpx.HTTPStatusError as exc:
                status, text = exc.response.status_code, exc.response.text
                if status == 409 or (status == 401 and "worker is not registered" in text):
                    # The lease moved on: the request timed out, was retried elsewhere, or this worker was closed.
                    logship.note(f"Surfra no longer holds request stage {stage['id']} for this worker ({status}); result discarded")
                    continue
                raise

    def step(self) -> bool:
        """Pull a batch, or one request, and run the handler."""

        spec = registered_handler_spec() or {}
        batch_max = int(spec.get("batch_max") or 1)
        started = time.monotonic()
        stages = self.next_stages(batch_max, wait=POLL_WAIT)
        if not stages:
            self.waiting = False
            self.note_empty(started)
            return False
        if batch_max > 1:
            age = _oldest_age_ms(stages)
            count = take_batch(
                len(stages),
                batch_min=int(spec.get("batch_min") or 1),
                batch_max=batch_max,
                wait_ms=int(spec.get("wait_ms") or 0),
                oldest_age_ms=age,
            )
            if count == 0:
                self.waiting = True
                self.note_filling(age, int(spec.get("wait_ms") or 0))
                return False
            stages = stages[:count]
        self.waiting = False
        hook = bind_progress_hook(lambda fraction, _message="": [self.report_progress(stage["id"], fraction) for stage in stages])
        timeout = execution_timeout()

        def give_up() -> None:
            # A thread cannot be stopped from outside, so the request is failed now and its result dropped later.
            self.settle(stages, error=f"the request ran longer than the execution timeout of {timeout:.0f}s")

        result: Any = None
        failure: str | None = None
        try:
            with Heartbeat(self, len(stages), timeout=timeout, on_timeout=give_up) as beat:
                with logship.running(stage.get("transaction_id") for stage in stages):
                    result = run_handler_batch([stage.get("input") or {} for stage in stages])
        except Exception as exc:
            failure = str(exc)
        finally:
            unbind_progress_hook(hook)
        if beat.timed_out or beat.lost:
            return True
        if failure is not None:
            self.settle(stages, error=failure)
            return True
        if result is None or not isinstance(result, list) or len(result) != len(stages):
            self.settle(stages, error="a batch handler returns one result per request, or None")
            return True
        self.settle(stages, outputs=result)
        return True


def _oldest_age_ms(stages: list[dict[str, Any]]) -> int:
    ages = []
    now = datetime.now(timezone.utc)
    for stage in stages:
        raw = stage.get("created_at")
        if not raw:
            continue
        try:
            moment = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        ages.append(int((now - moment).total_seconds() * 1000))
    return max(ages) if ages else 0


def _signal_ready(results: Any) -> None:
    """Boot probe. Imports this module and returns, without loading a model."""

    results.put({"kind": "ready"})


def deployment_mode() -> str:
    """Isolated or joint. The autoscaler copies this from the app deployment."""

    mode = os.environ.get("SURFRA_WORKER_MODE", "isolated").strip().lower()
    return mode if mode in {"isolated", "joint"} else "isolated"


def _child_entry(app_path: str, jobs: Any, results: Any, mode: str) -> None:
    """User code. This process does not have the replica token in its environment."""

    os.environ.pop("SURFRA_REPLICA_TOKEN", None)
    load_app(app_path)
    run_warmup()
    spec = registered_handler_spec() or {}
    copies = 1 if mode == "isolated" else max(1, int(spec.get("concurrent_workers") or 1))
    results.put({"kind": "ready", "spec": spec})

    def run_one() -> None:
        while True:
            item = jobs.get()
            if item is None:
                return

            def hook(fraction: float, _message: str = "", job_id: str = item["id"]) -> None:
                results.put({"kind": "progress", "id": job_id, "fraction": fraction})

            token = bind_progress_hook(hook)
            try:
                output = run_handler_batch(item["payloads"])
                results.put({"kind": "done", "id": item["id"], "output": output})
            except Exception as exc:
                results.put({"kind": "done", "id": item["id"], "error": str(exc)})
            finally:
                unbind_progress_hook(token)

    threads = [threading.Thread(target=run_one, daemon=True) for _ in range(copies)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def _serve_in_process(master: str, replica_token: str, app_path: str, *, once: bool, interval: float) -> None:
    began = time.monotonic()
    load_app(app_path)
    run_warmup()
    logship.note(f"{app_path} loaded and warmed up in {time.monotonic() - began:.1f}s")
    spec = registered_handler_spec() or {}
    copies = max(1, int(spec.get("concurrent_workers") or 1))
    clients = [WorkerClient(master, replica_token) for _ in range(copies)]
    for client in clients:
        client.register(copies)
    threads = [
        threading.Thread(target=_loop, args=(client,), kwargs={"once": once, "interval": interval, "copies": copies}, daemon=True)
        for client in clients
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def _spawn_child(ctx: Any, app_path: str, jobs: Any, results: Any, mode: str, replica_token: str) -> Any:
    saved = os.environ.pop("SURFRA_REPLICA_TOKEN", None)
    process = ctx.Process(target=_child_entry, args=(app_path, jobs, results, mode))
    try:
        process.start()
    finally:
        os.environ["SURFRA_REPLICA_TOKEN"] = replica_token if saved is None else saved
    return process


class _StartFailed(RuntimeError):
    """Worker processes could not start. The parent may run the app itself instead."""


def _stop_process(process: Any) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
    if process.is_alive():
        process.kill()
        process.join(timeout=5)


def _await_ready(source: Any, processes: list[Any], timeout: float) -> dict[str, Any]:
    """The next ready message. Fails when every process died first or the time runs out."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            message = source.get(timeout=1.0)
        except queue.Empty:
            if processes and not any(process.is_alive() for process in processes):
                codes = ", ".join(str(process.exitcode) for process in processes)
                raise _StartFailed(f"worker process exited while loading the app (exit code {codes})")
            continue
        if isinstance(message, dict) and message.get("kind") == "ready":
            return message
    raise _StartFailed(f"worker process did not finish loading the app within {timeout:.0f}s")


class _Slot:
    """One worker process and its job queue. A process that hangs or dies is replaced in place."""

    def __init__(self, ctx: Any, app_path: str, mode: str, replica_token: str, results: Any, ready: "queue.Queue[dict[str, Any]]"):
        self.ctx = ctx
        self.app_path = app_path
        self.mode = mode
        self.replica_token = replica_token
        self.results = results
        self.ready = ready
        self.lock = threading.Lock()
        self.generation = 0
        self.jobs = ctx.Queue()
        self.process = _spawn_child(ctx, app_path, self.jobs, results, mode, replica_token)

    def restart(self, generation: int, why: str) -> None:
        with self.lock:
            if generation != self.generation:
                return
            logship.note(f"restarting a worker process because {why}")
            _stop_process(self.process)
            self.jobs = self.ctx.Queue()
            self.process = _spawn_child(self.ctx, self.app_path, self.jobs, self.results, self.mode, self.replica_token)
            try:
                _await_ready(self.ready, [self.process], RESTART_READY_SECONDS)
            except _StartFailed as exc:
                # Nothing can serve on this replica now. Exit so the container restarts clean.
                logship.note(f"the replacement worker process did not start: {exc}. Restarting the container.")
                logship.flush(timeout=10)
                os._exit(1)
            self.generation += 1
            logship.note("worker process restarted")


def _collect(
    results: Any,
    ready: "queue.Queue[dict[str, Any]]",
    pending: dict[str, dict[str, Any]],
    answers: dict[str, dict[str, Any]],
    lock: threading.Lock,
) -> None:
    """Route messages from worker processes: answers to the waiting request, ready to a restart."""

    while True:
        message = results.get()
        if not isinstance(message, dict):
            continue
        kind = message.get("kind")
        if kind == "ready":
            ready.put(message)
            continue
        if kind == "progress":
            with lock:
                job = pending.get(message["id"])
            if job is not None:
                for stage in job["stages"]:
                    job["client"].report_progress(stage["id"], float(message["fraction"]))
            continue
        if kind != "done":
            continue
        with lock:
            job = pending.get(message["id"])
            # An answer for a request already given up on (timed out, process restarted) is dropped.
            if job is not None:
                answers[message["id"]] = message
        if job is not None:
            job["event"].set()


def _serve_supervised(master: str, replica_token: str, app_path: str, *, once: bool, interval: float) -> None:
    mode = deployment_mode()
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    ready: "queue.Queue[dict[str, Any]]" = queue.Queue()
    began = time.monotonic()
    slots: list[_Slot] = []
    try:
        slots.append(_Slot(ctx, app_path, mode, replica_token, results, ready))
    except Exception as exc:
        raise _StartFailed(f"could not start a worker process: {exc}") from exc
    try:
        first = _await_ready(results, [slots[0].process], RESTART_READY_SECONDS)
        logship.note(f"{app_path} loaded and warmed up in {time.monotonic() - began:.1f}s")
        adopt_handler_spec(first["spec"])
        spec = registered_handler_spec() or {}
        copies = max(1, int(spec.get("concurrent_workers") or 1))
        if mode == "isolated" and copies > 1:
            for _ in range(copies - 1):
                slots.append(_Slot(ctx, app_path, mode, replica_token, results, ready))
            for _ in range(copies - 1):
                _await_ready(results, [slot.process for slot in slots[1:]], RESTART_READY_SECONDS)
    except BaseException:
        for slot in slots:
            _stop_process(slot.process)
        raise
    # From here a child holds the model, so a failure must not load it again in this process.
    try:
        clients = [WorkerClient(master, replica_token) for _ in range(copies)]
        for client in clients:
            client.register(copies)
        pending: dict[str, dict[str, Any]] = {}
        answers: dict[str, dict[str, Any]] = {}
        lock = threading.Lock()

        threading.Thread(target=_collect, args=(results, ready, pending, answers, lock), daemon=True).start()

        def loop(client: WorkerClient, slot: _Slot) -> None:
            while True:
                ran = client.guard(lambda: _step_remote(client, slot, pending, answers, lock), copies)
                if once and ran:
                    return
                if not ran:
                    time.sleep(client.pause(interval))

        threads = [
            threading.Thread(target=loop, args=(client, slots[index % len(slots)]), daemon=True)
            for index, client in enumerate(clients)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        for slot in slots:
            try:
                slot.jobs.put(None)
            except Exception:
                pass
        for slot in slots:
            slot.process.join(timeout=5)
            _stop_process(slot.process)


def _step_remote(
    client: WorkerClient,
    slot: _Slot,
    pending: dict[str, dict[str, Any]],
    answers: dict[str, dict[str, Any]],
    lock: threading.Lock,
) -> bool:
    spec = registered_handler_spec() or {}
    batch_max = int(spec.get("batch_max") or 1)
    started = time.monotonic()
    stages = client.next_stages(batch_max, wait=POLL_WAIT)
    if not stages:
        client.waiting = False
        client.note_empty(started)
        return False
    if batch_max > 1:
        age = _oldest_age_ms(stages)
        count = take_batch(
            len(stages),
            batch_min=int(spec.get("batch_min") or 1),
            batch_max=batch_max,
            wait_ms=int(spec.get("wait_ms") or 0),
            oldest_age_ms=age,
        )
        if count == 0:
            client.waiting = True
            client.note_filling(age, int(spec.get("wait_ms") or 0))
            return False
        stages = stages[:count]
    client.waiting = False
    job_id = f"{client.worker_id}-{time.monotonic_ns()}"
    event = threading.Event()
    with lock:
        pending[job_id] = {"event": event, "client": client, "stages": stages}
    timeout = execution_timeout()
    generation = slot.generation
    reason: str | None = None
    restart = False
    with Heartbeat(client, len(stages), timeout=timeout) as beat:
        with logship.running(stage.get("transaction_id") for stage in stages):
            slot.jobs.put({"id": job_id, "payloads": [stage.get("input") or {} for stage in stages]})
            while not event.wait(1.0):
                if beat.timed_out:
                    reason, restart = f"the request ran longer than the execution timeout of {timeout:.0f}s", True
                    break
                if slot.generation != generation:
                    reason = "its worker process was restarted while it ran"
                    break
                if not slot.process.is_alive():
                    reason, restart = f"its worker process exited (exit code {slot.process.exitcode})", True
                    break
    with lock:
        message = answers.pop(job_id, None)
        pending.pop(job_id, None)
    if beat.lost:
        return True
    if reason is not None:
        client.settle(stages, error=reason)
        if restart:
            slot.restart(generation, reason)
        return True
    assert message is not None
    if message.get("error"):
        client.settle(stages, error=message["error"])
        return True
    result = message.get("output")
    if result is None or not isinstance(result, list) or len(result) != len(stages):
        client.settle(stages, error="a batch handler returns one result per request, or None")
        return True
    client.settle(stages, outputs=result)
    return True


def serve(master: str, replica_token: str, app_path: str, *, once: bool = False, interval: float = 2.0) -> None:
    in_process = os.environ.get("SURFRA_IN_PROCESS", "").strip().lower() in {"1", "true", "yes"}
    if in_process:
        _serve_in_process(master, replica_token, app_path, once=once, interval=interval)
        return
    try:
        _serve_supervised(master, replica_token, app_path, once=once, interval=interval)
    except _StartFailed as exc:
        print(f"worker processes failed to start ({exc}); running in this process", flush=True)
        _serve_in_process(master, replica_token, app_path, once=once, interval=interval)


def _wait_for_shutdown() -> None:
    """Surfra stopped this replica. Exiting would make a cloud restart policy start it again."""

    logship.note("Surfra stopped this replica. Waiting for the container to be shut down.")
    while True:
        time.sleep(3600)


def _loop(client: WorkerClient, *, once: bool, interval: float, copies: int = 1) -> None:
    while True:
        ran = client.guard(client.step, copies)
        if once and ran:
            return
        if not ran:
            time.sleep(client.pause(interval))


def _flush_on_term() -> None:
    """A stop sends SIGTERM. Ship the last log lines, which usually explain the stop, before exiting."""

    def stop(signum: int, _frame: Any) -> None:
        logship.note("worker stopping")
        logship.flush(timeout=10)
        raise SystemExit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, stop)
    except ValueError:
        # Not the main thread, as in tests.
        pass


def baked_runtime() -> tuple[str, str, str]:
    """Master, replica token, and entrypoint. All three are process env, not flags."""

    values = {name: os.environ.get(name, "").strip() for name in _BAKED}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise SystemExit(
            "this process is started by the autoscaler with "
            + ", ".join(_BAKED)
            + f"; missing {', '.join(missing)}"
        )
    return values["SURFRA_MASTER"], values["SURFRA_REPLICA_TOKEN"], values["SURFRA_ENTRYPOINT"]


def main() -> None:
    master, token, app_path = baked_runtime()
    logship.start(master, token)
    _flush_on_term()
    usage.start(master, token)
    logship.note(f"worker starting, loading {app_path}")
    once = os.environ.get("SURFRA_ONCE", "").strip().lower() in {"1", "true", "yes"}
    serve(master, token, app_path, once=once)


if __name__ == "__main__":
    main()

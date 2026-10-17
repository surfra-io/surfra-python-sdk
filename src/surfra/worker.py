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
import threading
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from surfra import logship
from surfra.runner import load_app, run_handler_batch, run_warmup
from surfra.sdk import adopt_handler_spec, bind_progress_hook, registered_handler_spec, take_batch, unbind_progress_hook

_BAKED = ("SURFRA_MASTER", "SURFRA_REPLICA_TOKEN", "SURFRA_ENTRYPOINT")
# An idle poll waits on Surfra this long for work, so a worker does not ask every few seconds.
POLL_WAIT = 20.0


class WorkerClient:
    def __init__(self, master: str, replica_token: str):
        self.master = master.rstrip("/")
        self.replica_token = replica_token
        self.worker_id: str | None = None
        self.waiting = False
        self.backoff = 1.0
        self._pause: float | None = None
        self._http = httpx.Client(timeout=60)

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

    def register(self, concurrent_workers: int = 1, *, wait: float = 15.0) -> str:
        # The control plane can start a worker before the replica row commits as ready.
        deadline = time.monotonic() + wait
        while True:
            response = self._http.post(
                f"{self.master}/v1/workers/register",
                json={"replica_token": self.replica_token, "concurrent_workers": concurrent_workers},
            )
            unknown = response.status_code == 401 and "unknown replica token" in response.text
            if not unknown or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
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
        time.sleep(self.backoff)
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

    def fail(self, stage_id: str, error: str) -> None:
        response = self._http.post(
            f"{self.master}/v1/workers/fail",
            json={"replica_token": self.replica_token, "worker_id": self.worker_id, "stage_id": stage_id, "error": error},
        )
        response.raise_for_status()

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
        try:
            with logship.running(stage.get("transaction_id") for stage in stages):
                result = run_handler_batch([stage.get("input") or {} for stage in stages])
        except Exception as exc:
            for stage in stages:
                self.fail(stage["id"], str(exc))
            return True
        finally:
            unbind_progress_hook(hook)
        if result is None or not isinstance(result, list) or len(result) != len(stages):
            message = "a batch handler returns one result per request, or None"
            for stage in stages:
                self.fail(stage["id"], message)
            return True
        for stage, output in zip(stages, result):
            self.complete(stage["id"], output)
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


def _serve_supervised(master: str, replica_token: str, app_path: str, *, once: bool, interval: float) -> None:
    mode = deployment_mode()
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    first_jobs = ctx.Queue()
    began = time.monotonic()
    process = _spawn_child(ctx, app_path, first_jobs, results, mode, replica_token)
    ready = results.get()
    if ready.get("kind") != "ready":
        process.terminate()
        raise RuntimeError("worker child did not start")
    logship.note(f"{app_path} loaded and warmed up in {time.monotonic() - began:.1f}s")
    adopt_handler_spec(ready["spec"])
    spec = registered_handler_spec() or {}
    copies = max(1, int(spec.get("concurrent_workers") or 1))
    queues = [first_jobs]
    processes = [process]
    if mode == "isolated" and copies > 1:
        for _ in range(copies - 1):
            jobs = ctx.Queue()
            processes.append(_spawn_child(ctx, app_path, jobs, results, mode, replica_token))
            queues.append(jobs)
        for _ in range(copies - 1):
            extra = results.get()
            if extra.get("kind") != "ready":
                raise RuntimeError("worker child did not start")
    clients = [WorkerClient(master, replica_token) for _ in range(copies)]
    for client in clients:
        client.register(copies)
    pending: dict[str, dict[str, Any]] = {}
    answers: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()

    def collect() -> None:
        while True:
            message = results.get()
            if not isinstance(message, dict):
                continue
            if message.get("kind") == "progress":
                with lock:
                    job = pending.get(message["id"])
                if job is not None:
                    for stage in job["stages"]:
                        job["client"].report_progress(stage["id"], float(message["fraction"]))
                continue
            if message.get("kind") != "done":
                continue
            with lock:
                job = pending.get(message["id"])
                answers[message["id"]] = message
            if job is not None:
                job["event"].set()

    threading.Thread(target=collect, daemon=True).start()

    def loop(client: WorkerClient, jobs: Any) -> None:
        while True:
            ran = client.guard(lambda: _step_remote(client, jobs, pending, answers, lock), copies)
            if once and ran:
                return
            if not ran:
                time.sleep(client.pause(interval))

    threads = [
        threading.Thread(target=loop, args=(client, queues[index % len(queues)]), daemon=True)
        for index, client in enumerate(clients)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for jobs in queues:
        jobs.put(None)
    for process in processes:
        process.join(timeout=5)


def _step_remote(
    client: WorkerClient,
    jobs: Any,
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
    with logship.running(stage.get("transaction_id") for stage in stages):
        jobs.put({"id": job_id, "payloads": [stage.get("input") or {} for stage in stages]})
        event.wait()
    with lock:
        message = answers.pop(job_id)
        pending.pop(job_id, None)
    if message.get("error"):
        for stage in stages:
            client.fail(stage["id"], message["error"])
        return True
    result = message.get("output")
    if result is None or not isinstance(result, list) or len(result) != len(stages):
        for stage in stages:
            client.fail(stage["id"], "a batch handler returns one result per request, or None")
        return True
    for stage, output in zip(stages, result):
        client.complete(stage["id"], output)
    return True


def serve(master: str, replica_token: str, app_path: str, *, once: bool = False, interval: float = 2.0) -> None:
    in_process = os.environ.get("SURFRA_IN_PROCESS", "").strip().lower() in {"1", "true", "yes"}
    if in_process:
        _serve_in_process(master, replica_token, app_path, once=once, interval=interval)
        return
    try:
        _serve_supervised(master, replica_token, app_path, once=once, interval=interval)
    except Exception as exc:
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
    logship.note(f"worker starting, loading {app_path}")
    once = os.environ.get("SURFRA_ONCE", "").strip().lower() in {"1", "true", "yes"}
    serve(master, token, app_path, once=once)


if __name__ == "__main__":
    main()

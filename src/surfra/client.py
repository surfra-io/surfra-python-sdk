"""Programmatic app updates and job calls. The app name is the stable id."""

from __future__ import annotations

import os
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator
from urllib.parse import quote

import httpx

from surfra.credentials import as_registry
from surfra.types import Autoscale, Runtime

_DONE = {"succeeded", "failed"}
JOB_DONE = {"succeeded", "failed", "stopped", "lost"}
# Lines are stored a little after they are printed, so a follow rereads this far back.
LATE_LINES_SECONDS = 30


def _rewound(stamp: str, seconds: float) -> str:
    try:
        moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return stamp
    return (moment - timedelta(seconds=seconds)).isoformat()


class SurfraError(RuntimeError):
    """The API refused a request. status is the HTTP code. detail is the server's reason."""

    def __init__(self, status: int, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status = status
        self.detail = detail


def _checked(response: httpx.Response) -> Any:
    if response.is_success:
        return response.json()
    try:
        detail = response.json().get("detail")
    except ValueError:
        detail = None
    raise SurfraError(response.status_code, str(detail or response.text or response.reason_phrase))


class Call:
    """One transaction. `poll` waits until it finishes. `result` returns the output."""

    def __init__(self, client: SurfraClient, view: dict[str, Any]):
        self.client = client
        self.view = view
        self.id = str(view["id"])

    def get(self) -> dict[str, Any]:
        self.view = self.client.transaction(self.id)
        return self.view

    def poll(self, *, interval: float = 0.5, timeout: float | None = None) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            view = self.get()
            if view.get("status") in _DONE:
                return view
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"transaction {self.id} is still {view.get('status')}")
            time.sleep(interval)

    def result(self, *, timeout: float | None = None) -> Any:
        return _result_of(self.poll(timeout=timeout))


def _result_of(view: dict[str, Any]) -> Any:
    """The output of a finished transaction, or RuntimeError with each stage's error."""

    stages = view.get("stages") or []
    if view.get("status") != "succeeded":
        details = []
        for stage in stages:
            output = stage.get("output") or {}
            err = output.get("error") if isinstance(output, dict) else None
            details.append(f"{stage.get('name')}: {err or stage.get('status')}")
        raise RuntimeError("; ".join(details) or str(view.get("status")))
    if len(stages) == 1:
        return stages[0].get("output")
    return {stage.get("name"): stage.get("output") for stage in stages}


class Job:
    """One job. `wait` blocks until it ends. `logs` reads its output, and follows it with follow=True."""

    def __init__(self, client: SurfraClient, view: dict[str, Any]):
        self.client = client
        self.view = view
        self.id = str(view["id"])

    @property
    def status(self) -> str:
        return str(self.view.get("status"))

    def get(self) -> dict[str, Any]:
        self.view = _checked(self.client._http.get(f"{self.client.base_url}/v1/jobs/{self.id}"))
        return self.view

    def stop(self) -> dict[str, Any]:
        self.view = _checked(self.client._http.post(f"{self.client.base_url}/v1/jobs/{self.id}/stop"))
        return self.view

    def wait(self, *, interval: float = 5.0, timeout: float | None = None) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.get().get("status") not in JOB_DONE:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"job {self.id} is still {self.status}")
            time.sleep(interval)
        return self.view

    def logs(self, *, follow: bool = False, after: str | None = None, interval: float = 2.0) -> Iterator[dict[str, Any]]:
        """Lines oldest first, as {"t", "s", "m"}. "s" is out, err, or sys for Surfra's own lines."""

        cursor = _LogCursor(after)
        while True:
            lines = _checked(self.client._http.get(f"{self.client.base_url}/v1/jobs/{self.id}/logs", params=cursor.params())).get("lines") or []
            yield from cursor.feed(lines)
            if cursor.more(lines):
                continue
            if not follow:
                return
            cursor.rewind = True
            if self.get().get("status") in JOB_DONE:
                follow = False
            time.sleep(interval)


class _LogCursor:
    """Paging state for job logs, shared by the sync and async clients.

    A follow rereads LATE_LINES_SECONDS back, because lines are stored a little after they are printed,
    and drops lines it has already returned.
    """

    PAGE = 1000

    def __init__(self, after: str | None):
        self.cursor = after
        self.rewind = False
        self.seen: dict[tuple[str, str, str], str] = {}
        self.fresh = 0

    def params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": self.PAGE, "order": "asc"}
        if self.cursor:
            params["after"] = _rewound(self.cursor, LATE_LINES_SECONDS) if self.rewind else self.cursor
        return params

    def feed(self, lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        self.fresh = 0
        for line in lines:
            stamp = str(line.get("t") or "")
            key = (stamp, str(line.get("s")), str(line.get("m")))
            if key in self.seen:
                continue
            self.seen[key] = stamp
            self.fresh += 1
            if stamp > (self.cursor or ""):
                self.cursor = stamp
            out.append(line)
        if len(self.seen) > 20000 and self.cursor:
            horizon = _rewound(self.cursor, LATE_LINES_SECONDS)
            self.seen = {key: stamp for key, stamp in self.seen.items() if stamp >= horizon}
        return out

    def more(self, lines: list[dict[str, Any]]) -> bool:
        """True when a full page came back with something new: read the next page now."""

        if len(lines) >= self.PAGE and (self.fresh or self.rewind):
            self.rewind = False
            return True
        return False


def _submit_body(stages: list[dict[str, Any]], metadata: dict[str, Any] | None, callback_url: str | None) -> dict[str, Any]:
    return {"stages": stages, "metadata": metadata or {}, "callback_url": callback_url}


def _one_stage(app: str, payload: dict[str, Any] | None, name: str) -> dict[str, Any]:
    return {"name": name, "app_name": app, "depends_on": [], "input": payload or {}}


def _workflow_stages(saved: dict[str, Any], payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """A saved workflow as transaction stages. Stages with no dependencies receive payload."""

    stages = []
    for row in saved.get("stages") or []:
        depends = list(row.get("depends") or [])
        stages.append({"name": row["name"], "app_name": row["app"], "depends_on": depends, "input": {} if depends else payload or {}})
    return stages


def _job_body(fields: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value is not None}


def _deploy_body(image: str, entrypoint: str, env: dict[str, str] | None, registry: object, autoscale: Any, runtime: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "image": image,
        "entrypoint": entrypoint,
        "env": env or {},
        "registry": as_registry(registry),
        "autoscale": autoscale or {},
    }
    if runtime is not None:
        body["runtime"] = runtime
    return body


def _api_key(api_key: str | None) -> str:
    from surfra.volumes import _saved_login

    # Never the volumes token from SURFRA_VOLUMES_TOKEN_FILE: only the volumes service accepts it.
    found = api_key or os.environ.get("SURFRA_API_KEY") or _saved_login()
    if not found:
        raise KeyError("SURFRA_API_KEY")
    return found


def _base_url(base_url: str | None) -> str:
    return (base_url or os.environ.get("SURFRA_MASTER") or "https://api.surfra.io").rstrip("/")


# Over the organization's request rate, submit waits as Surfra asks, this many times.
RATE_RETRIES = 5


def _retry_after(response: httpx.Response) -> float:
    try:
        return min(60.0, max(1.0, float(response.headers.get("retry-after") or 1)))
    except ValueError:
        return 1.0


class SurfraClient:
    """HTTP client for the Surfra API.

    api_key defaults to SURFRA_API_KEY, then the session `surfra login` saved. base_url defaults to SURFRA_MASTER, then https://api.surfra.io.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ):
        self.api_key = _api_key(api_key)
        self.base_url = _base_url(base_url)
        self._http = httpx.Client(
            timeout=60,
            headers={"Authorization": f"Bearer {self.api_key}"},
            transport=transport,
        )

    def close(self) -> None:
        """Close the HTTP connections. The client can't be used afterwards."""

        self._http.close()

    def __enter__(self) -> SurfraClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def deploy_app(
        self,
        app_name: str,
        *,
        image: str,
        entrypoint: str,
        env: dict[str, str] | None = None,
        registry: object = None,
        autoscale: Autoscale | dict[str, Any] | None = None,
        runtime: Runtime | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create the app or update it in place. The name, or an sfa_ id, picks the app.

        runtime is {"engine": "python"} or {"engine": "http", "port": ..., "path": ...}.
        Left out, the engine follows the entrypoint.
        """

        body = _deploy_body(image, entrypoint, env, registry, autoscale, runtime)
        return _checked(self._http.put(f"{self.base_url}/v1/apps/{quote(app_name, safe='')}", json=body))

    def apps(self) -> list[dict[str, Any]]:
        """Every app on this account."""

        return _checked(self._http.get(f"{self.base_url}/v1/apps"))["apps"]

    def app(self, app: str) -> dict[str, Any]:
        """One app by name or sfa_ id."""

        return _checked(self._http.get(f"{self.base_url}/v1/apps/{quote(app, safe='')}"))

    def deploy_workflow(self, name: str, stages: list[dict[str, Any]]) -> dict[str, Any]:
        return _checked(self._http.put(f"{self.base_url}/v1/workflows/{quote(name, safe='')}", json={"stages": stages}))

    def workflow(self, name: str) -> dict[str, Any]:
        return _checked(self._http.get(f"{self.base_url}/v1/workflows/{quote(name, safe='')}"))

    def enqueue_workflow(self, name: str, payload: dict[str, Any] | None = None) -> Call:
        """Start a saved workflow. Stages with no dependencies receive payload."""

        stages = _workflow_stages(self.workflow(name), payload)
        return Call(self, self.submit(stages, metadata={"workflow": name}))

    def submit(self, stages: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None, callback_url: str | None = None) -> dict[str, Any]:
        """Start a transaction. Each stage names an app and lists the stages it depends on."""

        body = _submit_body(stages, metadata, callback_url)
        for attempt in range(RATE_RETRIES + 1):
            response = self._http.post(f"{self.base_url}/v1/transactions", json=body)
            if response.status_code != 429 or attempt == RATE_RETRIES:
                return _checked(response)
            time.sleep(_retry_after(response))
        raise AssertionError("unreachable")

    def transaction(self, transaction_id: str) -> dict[str, Any]:
        return _checked(self._http.get(f"{self.base_url}/v1/transactions/{quote(transaction_id, safe='')}"))

    def enqueue(self, app: str, payload: dict[str, Any] | None = None, *, name: str = "call", callback_url: str | None = None) -> Call:
        """Queue one call to an app and return it before the worker finishes.

        Surfra POSTs the task's latest state to callback_url each time it changes. See verify_webhook.
        """

        view = self.submit([_one_stage(app, payload, name)], callback_url=callback_url)
        return Call(self, view)

    def run(self, app: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Enqueue, poll, and return the output."""

        return self.enqueue(app, payload).result(timeout=timeout)

    def map(self, app: str, items: Iterable[dict[str, Any]], *, concurrency: int = 8, timeout: float | None = None) -> Iterator[Any]:
        """Call an app once per item and yield outputs in order. At most `concurrency` calls wait at a time."""

        source = iter(items)
        waiting: deque[Call] = deque()

        def fill() -> None:
            while len(waiting) < max(1, concurrency):
                try:
                    item = next(source)
                except StopIteration:
                    return
                waiting.append(self.enqueue(app, item))

        fill()
        while waiting:
            call = waiting.popleft()
            fill()
            yield call.result(timeout=timeout)

    def submit_job(
        self,
        entrypoint: str | None = None,
        *,
        args: list[str] | None = None,
        image: str | None = None,
        spec: str | None = None,
        providers: list[dict[str, Any]] | None = None,
        env: dict[str, str] | None = None,
        secret: str | None = None,
        docker_secret: str | None = None,
        deadline_sec: int | None = None,
        liveness_sec: int | None = None,
        grace_sec: int | None = None,
        call_apps: list[str] | None = None,
        deny_apps: list[str] | None = None,
        volumes: list[str] | None = None,
        name: str | None = None,
        submission_id: str | None = None,
    ) -> Job:
        """Run a command once on one machine. With spec, the saved job spec fills in what is left out.

        entrypoint is a shell command, such as "bash train.sh". args are appended, quoted.
        providers are machine rows in order, such as [{"provider": "runner", "runner": "desk-4090"}].
        env is set in the container. Names starting with SURFRA_ are reserved.
        call_apps lists the apps the job may call with the SURFRA_API_KEY Surfra gives it. ["*"] allows all.
        volumes lists the volumes the job may reach, such as ["weights:r", "outputs:rw"]. Missing volumes are created.
        submission_id makes the call safe to retry: the same id returns the same job.
        """

        body = _job_body({
                "entrypoint": entrypoint,
                "args": args,
                "image": image,
                "spec": spec,
                "providers": providers,
                "env": env,
                "secret": secret,
                "docker_secret": docker_secret,
                "deadline_sec": deadline_sec,
                "liveness_sec": liveness_sec,
                "grace_sec": grace_sec,
                "call_apps": call_apps,
                "deny_apps": deny_apps,
                "volumes": volumes,
                "name": name,
                "submission_id": submission_id,
            })
        return Job(self, _checked(self._http.post(f"{self.base_url}/v1/jobs", json=body)))

    def app_volumes(self, app_name: str) -> list[dict[str, str]]:
        """The volumes an app's replicas may reach, as [{"volume", "mode"}]."""

        return _checked(self._http.get(f"{self.base_url}/v1/apps/{quote(app_name, safe='')}/volumes"))["volumes"]

    def set_app_volumes(self, app_name: str, volumes: list[str]) -> list[dict[str, str]]:
        """Replace an app's volume grants, such as ["weights:r", "outputs:rw"]. Running replicas see it within 15 minutes."""

        return _checked(self._http.put(f"{self.base_url}/v1/apps/{quote(app_name, safe='')}/volumes", json={"volumes": volumes}))["volumes"]

    def job(self, job_id: str) -> Job:
        return Job(self, _checked(self._http.get(f"{self.base_url}/v1/jobs/{quote(job_id, safe='')}")))

    def jobs(self, *, limit: int = 50, status: str = "") -> list[dict[str, Any]]:
        return _checked(self._http.get(f"{self.base_url}/v1/jobs", params={"limit": limit, "status": status}))["jobs"]

    def save_job_spec(self, name: str, **fields: Any) -> dict[str, Any]:
        """Save a named job spec: image, entrypoint, args, providers, env, secret, deadline_sec, call_apps, and so on."""

        return _checked(self._http.put(f"{self.base_url}/v1/job-specs/{quote(name, safe='')}", json=fields))

    def job_specs(self) -> list[dict[str, Any]]:
        return _checked(self._http.get(f"{self.base_url}/v1/job-specs"))["specs"]

    def delete_job_spec(self, name: str) -> dict[str, Any]:
        return _checked(self._http.delete(f"{self.base_url}/v1/job-specs/{quote(name, safe='')}"))

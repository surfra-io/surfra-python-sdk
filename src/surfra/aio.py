"""The Surfra SDK for asyncio. The same names as `surfra`, and every call that talks to Surfra is async.

    import surfra.aio as surfra

    call = await surfra.App("hello").enqueue({"name": "x"})
    out = await call.result(timeout=300)

Requests go through httpx.AsyncClient, so nothing here blocks the event loop. Volume methods that
sync folders to disk run in a worker thread (asyncio.to_thread). Sync code keeps `import surfra`.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from typing import Any, AsyncIterator, Iterable
from urllib.parse import quote

import httpx

from surfra import volumes as _volumes
from surfra.client import (
    _DONE,
    JOB_DONE,
    SurfraError,
    _api_key,
    _base_url,
    _checked,
    _deploy_body,
    _job_body,
    _LogCursor,
    _one_stage,
    _result_of,
    _policy,
    _submit_body,
    _workflow_stages,
)
from surfra.credentials import Credentials
from surfra.sdk import Request, batch_size, handler, progress, registered_stages, stage, warmup
from surfra.types import Autoscale, ProviderRow, Runtime
from surfra.volumes import Entry
from surfra.webhooks import InvalidSignature, verify_webhook
from surfra.version import HEADERS

__all__ = [
    "App",
    "AsyncClient",
    "Autoscale",
    "Call",
    "Credentials",
    "Entry",
    "InvalidSignature",
    "Job",
    "ProviderRow",
    "Request",
    "Runtime",
    "SurfraClient",
    "SurfraError",
    "Volume",
    "VolumePath",
    "Workflow",
    "batch_size",
    "handler",
    "progress",
    "stage",
    "verify_webhook",
    "warmup",
]


class Call:
    """One transaction. `poll` waits until it finishes. `result` returns the output."""

    def __init__(self, client: SurfraClient, view: dict[str, Any]):
        self.client = client
        self.view = view
        self.id = str(view["id"])

    async def get(self) -> dict[str, Any]:
        self.view = await self.client.transaction(self.id)
        return self.view

    async def poll(self, *, interval: float = 0.5, timeout: float | None = None) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            view = await self.get()
            if view.get("status") in _DONE:
                return view
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"transaction {self.id} is still {view.get('status')}")
            await asyncio.sleep(interval)

    async def result(self, *, timeout: float | None = None) -> Any:
        return _result_of(await self.poll(timeout=timeout))


class Job:
    """One job. `wait` returns when it ends. `logs` reads its output, and follows it with follow=True."""

    def __init__(self, client: SurfraClient, view: dict[str, Any]):
        self.client = client
        self.view = view
        self.id = str(view["id"])

    @property
    def status(self) -> str:
        return str(self.view.get("status"))

    def _url(self, rest: str = "") -> str:
        return f"{self.client.base_url}/v1/jobs/{quote(self.id, safe='')}{rest}"

    async def get(self) -> dict[str, Any]:
        self.view = _checked(await self.client._http.get(self._url()))
        return self.view

    async def stop(self) -> dict[str, Any]:
        self.view = _checked(await self.client._http.post(self._url("/stop")))
        return self.view

    async def wait(self, *, interval: float = 5.0, timeout: float | None = None) -> dict[str, Any]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while (await self.get()).get("status") not in JOB_DONE:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"job {self.id} is still {self.status}")
            await asyncio.sleep(interval)
        return self.view

    async def logs(self, *, follow: bool = False, after: str | None = None, interval: float = 2.0) -> AsyncIterator[dict[str, Any]]:
        """Lines oldest first, as {"t", "s", "m"}. Use with `async for`."""

        cursor = _LogCursor(after)
        while True:
            lines = _checked(await self.client._http.get(self._url("/logs"), params=cursor.params())).get("lines") or []
            for line in cursor.feed(lines):
                yield line
            if cursor.more(lines):
                continue
            if not follow:
                return
            cursor.rewind = True
            if (await self.get()).get("status") in JOB_DONE:
                follow = False
            await asyncio.sleep(interval)


class SurfraClient:
    """Async HTTP client for the Surfra API. The same methods as surfra.SurfraClient, awaited.

    api_key defaults to SURFRA_API_KEY, then the session `surfra login` saved. base_url defaults to SURFRA_MASTER, then https://api.surfra.io.
    Use `async with SurfraClient() as client:` or call `await client.aclose()` when done.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        max_retries: int | None = None,
        retry_deadline: float | None = None,
        on_retry: Any = None,
    ):
        from surfra.retry import AsyncRetryTransport

        self.api_key = _api_key(api_key)
        self.base_url = _base_url(base_url)
        self._transport = AsyncRetryTransport(transport, _policy(max_retries, retry_deadline, on_retry))
        self._owner = True
        self._http = self._client(self._transport)

    def _client(self, transport: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=60, headers={"Authorization": f"Bearer {self.api_key}", **HEADERS}, transport=transport)

    @property
    def retry_policy(self) -> Any:
        return self._transport.policy

    def with_retry(self, *, max_retries: int | None = None, retry_deadline: float | None = None, on_retry: Any = None) -> SurfraClient:
        """A client sharing this one's connections, with other retry limits. The same as surfra.SurfraClient.with_retry."""

        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        clone._transport = self._transport.with_policy(self._transport.policy.with_(max_retries=max_retries, deadline=retry_deadline, on_retry=on_retry))
        clone._owner = False
        clone._http = clone._client(clone._transport)
        return clone

    async def aclose(self) -> None:
        if self._owner:
            await self._http.aclose()

    async def __aenter__(self) -> SurfraClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.aclose()

    def _app_url(self, app: str, rest: str = "") -> str:
        return f"{self.base_url}/v1/apps/{quote(app, safe='')}{rest}"

    async def deploy_app(
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
        """Create the app or update it in place. The name, or an sfa_ id, picks the app."""

        body = _deploy_body(image, entrypoint, env, registry, autoscale, runtime)
        return _checked(await self._http.put(self._app_url(app_name), json=body))

    async def apps(self) -> list[dict[str, Any]]:
        return _checked(await self._http.get(f"{self.base_url}/v1/apps"))["apps"]

    async def app(self, app: str) -> dict[str, Any]:
        return _checked(await self._http.get(self._app_url(app)))

    async def deploy_workflow(self, name: str, stages: list[dict[str, Any]]) -> dict[str, Any]:
        return _checked(await self._http.put(f"{self.base_url}/v1/workflows/{quote(name, safe='')}", json={"stages": stages}))

    async def workflow(self, name: str) -> dict[str, Any]:
        return _checked(await self._http.get(f"{self.base_url}/v1/workflows/{quote(name, safe='')}"))

    async def enqueue_workflow(self, name: str, payload: dict[str, Any] | None = None) -> Call:
        """Start a saved workflow. Stages with no dependencies receive payload."""

        stages = _workflow_stages(await self.workflow(name), payload)
        return Call(self, await self.submit(stages, metadata={"workflow": name}))

    async def submit(self, stages: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None, callback_url: str | None = None) -> dict[str, Any]:
        """Start a transaction. Each stage names an app and lists the stages it depends on."""

        return _checked(await self._http.post(f"{self.base_url}/v1/transactions", json=_submit_body(stages, metadata, callback_url)))

    async def transaction(self, transaction_id: str) -> dict[str, Any]:
        return _checked(await self._http.get(f"{self.base_url}/v1/transactions/{quote(transaction_id, safe='')}"))

    async def enqueue(self, app: str, payload: dict[str, Any] | None = None, *, name: str = "call", callback_url: str | None = None) -> Call:
        """Queue one call to an app and return it before the worker finishes.

        Surfra POSTs the task's latest state to callback_url each time it changes. See verify_webhook.
        """

        return Call(self, await self.submit([_one_stage(app, payload, name)], callback_url=callback_url))

    async def run(self, app: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Enqueue, poll, and return the output."""

        return await (await self.enqueue(app, payload)).result(timeout=timeout)

    async def map(self, app: str, items: Iterable[dict[str, Any]], *, concurrency: int = 8, timeout: float | None = None) -> AsyncIterator[Any]:
        """Call an app once per item and yield outputs in order, with `async for`. At most `concurrency` calls wait at a time."""

        source = iter(items)
        waiting: deque[Call] = deque()

        async def fill() -> None:
            while len(waiting) < max(1, concurrency):
                try:
                    item = next(source)
                except StopIteration:
                    return
                waiting.append(await self.enqueue(app, item))

        await fill()
        while waiting:
            call = waiting.popleft()
            await fill()
            yield await call.result(timeout=timeout)

    async def submit_job(
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
        """Run a command once on one machine. The arguments are those of surfra.SurfraClient.submit_job."""

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
        return Job(self, _checked(await self._http.post(f"{self.base_url}/v1/jobs", json=body)))

    async def app_volumes(self, app_name: str) -> list[dict[str, str]]:
        return _checked(await self._http.get(self._app_url(app_name, "/volumes")))["volumes"]

    async def set_app_volumes(self, app_name: str, volumes: list[str]) -> list[dict[str, str]]:
        return _checked(await self._http.put(self._app_url(app_name, "/volumes"), json={"volumes": volumes}))["volumes"]

    async def job(self, job_id: str) -> Job:
        return Job(self, _checked(await self._http.get(f"{self.base_url}/v1/jobs/{quote(job_id, safe='')}")))

    async def jobs(self, *, limit: int = 50, status: str = "") -> list[dict[str, Any]]:
        return _checked(await self._http.get(f"{self.base_url}/v1/jobs", params={"limit": limit, "status": status}))["jobs"]

    async def save_job_spec(self, name: str, **fields: Any) -> dict[str, Any]:
        return _checked(await self._http.put(f"{self.base_url}/v1/job-specs/{quote(name, safe='')}", json=fields))

    async def job_specs(self) -> list[dict[str, Any]]:
        return _checked(await self._http.get(f"{self.base_url}/v1/job-specs"))["specs"]

    async def delete_job_spec(self, name: str) -> dict[str, Any]:
        return _checked(await self._http.delete(f"{self.base_url}/v1/job-specs/{quote(name, safe='')}"))


AsyncClient = SurfraClient


class _Named:
    """App and Workflow make their own client on first use. An httpx.AsyncClient belongs to the
    event loop that made it, so the made client is kept per loop."""

    def __init__(self, client: SurfraClient | None):
        self._client = client
        self._made: dict[int, tuple[asyncio.AbstractEventLoop, SurfraClient]] = {}

    def client(self) -> SurfraClient:
        if self._client is not None:
            return self._client
        loop = asyncio.get_running_loop()
        for key, (owner, _) in list(self._made.items()):
            if owner.is_closed():
                del self._made[key]
        found = self._made.get(id(loop))
        if found is None or found[0] is not loop:
            found = self._made[id(loop)] = (loop, SurfraClient())
        return found[1]


def _single_token(name: str, what: str) -> None:
    if not name or any(char.isspace() for char in name):
        raise ValueError(what)


class App(_Named):
    """The unit you deploy. The same methods as surfra.App, awaited."""

    def __init__(self, name: str, *, client: SurfraClient | None = None):
        _single_token(name, "app name is a single token, for example myllm-staging")
        self.name = name
        super().__init__(client)

    async def deploy(
        self,
        *,
        image: str,
        entrypoint: str,
        env: dict[str, str] | None = None,
        registry: object = None,
        autoscale: Autoscale | dict[str, Any] | None = None,
        runtime: Runtime | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create the app, or update the app with this name. Returns the saved app with its sfa_ id."""

        return await self.client().deploy_app(self.name, image=image, entrypoint=entrypoint, env=env, registry=registry, autoscale=autoscale, runtime=runtime)

    async def submit(self, stages: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None, callback_url: str | None = None) -> dict[str, Any]:
        return await self.client().submit(stages, metadata=metadata, callback_url=callback_url)

    async def info(self) -> dict[str, Any]:
        return await self.client().app(self.name)

    async def volumes(self) -> list[dict[str, str]]:
        return await self.client().app_volumes(self.name)

    async def set_volumes(self, volumes: list[str]) -> list[dict[str, str]]:
        return await self.client().set_app_volumes(self.name, volumes)

    async def enqueue(self, payload: dict[str, Any] | None = None, *, callback_url: str | None = None) -> Call:
        """Queue one request and return a Call right away."""

        return await self.client().enqueue(self.name, payload, callback_url=callback_url)

    async def run(self, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Queue one request and wait for the handler's output."""

        return await self.client().run(self.name, payload, timeout=timeout)


class Workflow(_Named):
    """A DAG of stages. The same methods as surfra.Workflow, awaited."""

    def __init__(self, name: str, *, client: SurfraClient | None = None):
        _single_token(name, "workflow name is a single token")
        self.name = name
        super().__init__(client)

    async def deploy(self, stages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """Save the stages registered with @surfra.stage, or the list you pass."""

        rows = stages if stages is not None else [spec.public() for spec in registered_stages()]
        return await self.client().deploy_workflow(self.name, rows)

    async def enqueue(self, payload: dict[str, Any] | None = None) -> Call:
        return await self.client().enqueue_workflow(self.name, payload)

    async def run(self, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        return await (await self.enqueue(payload)).result(timeout=timeout)


class VolumePath:
    """A file or folder inside a volume. A trailing slash means a folder."""

    def __init__(self, volume: Volume, path: str):
        self.volume = volume
        self._path = volume._volume.path(path)
        self.path = self._path.path

    def __repr__(self) -> str:
        return f"VolumePath({self.volume.name!r}, {self.path!r})"

    def __str__(self) -> str:
        return str(self._path)

    async def local(self, mode: str = "r", *, refresh: bool = False) -> str:
        """Download this piece into the local cache and return its real path."""

        return await asyncio.to_thread(self._path.local, mode, refresh=refresh)

    async def download(self, dest: str | os.PathLike[str]) -> str:
        return await asyncio.to_thread(self._path.download, dest)


class _Upload:
    """A volume file opened for writing. write() goes to a local temporary file; the upload happens on close.

        async with await volume.open("runs/1.jsonl", "w") as handle:
            handle.write("...")
    """

    def __init__(self, inner: Any):
        self._inner = inner

    @property
    def entry(self) -> Entry | None:
        return self._inner.entry

    def write(self, data: Any) -> int:
        return self._inner.write(data)

    async def close(self) -> None:
        await asyncio.to_thread(self._inner.close)

    async def __aenter__(self) -> _Upload:
        return self

    async def __aexit__(self, kind, value, traceback) -> None:
        await asyncio.to_thread(self._inner.__exit__, kind, value, traceback)


class Volume:
    """A volume by name. The same methods as surfra.Volume, awaited. No request is made until it is used.

    Transfers run in a worker thread, so the event loop keeps running while files move.
    """

    def __init__(
        self,
        name: str,
        *,
        api_key: str | None = None,
        volumes_url: str | None = None,
        master: str | None = None,
        max_retries: int | None = None,
        retry_deadline: float | None = None,
    ):
        self._volume = _volumes.Volume(name, api_key=api_key, volumes_url=volumes_url, master=master, max_retries=max_retries, retry_deadline=retry_deadline)
        self.name = name

    def __repr__(self) -> str:
        return f"Volume({self.name!r})"

    def grant(self, mode: str) -> str:
        return self._volume.grant(mode)

    mode = grant

    def path(self, path: str) -> VolumePath:
        return VolumePath(self, path)

    async def create(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._volume.create)

    async def info(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._volume.info)

    async def delete(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._volume.delete)

    @staticmethod
    async def list(*, api_key: str | None = None, master: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_volumes.Volume.list, api_key=api_key, master=master)

    async def ls(self, prefix: str = "", *, recursive: bool = False) -> list[Entry]:
        return await asyncio.to_thread(self._volume.ls, prefix, recursive=recursive)

    async def rm(self, path: str, *, recursive: bool = False) -> int:
        return await asyncio.to_thread(self._volume.rm, path, recursive=recursive)

    async def put_file(self, local: str | os.PathLike[str], remote: str, *, if_match: str | None = None) -> Entry:
        return await asyncio.to_thread(self._volume.put_file, local, remote, if_match=if_match)

    async def put_directory(self, local: str | os.PathLike[str], remote: str = "") -> int:
        return await asyncio.to_thread(self._volume.put_directory, local, remote)

    async def read(self, path: str) -> bytes:
        return await asyncio.to_thread(self._volume.read, path)

    async def write(self, path: str, data: bytes | str, *, if_match: str | None = None) -> Entry:
        return await asyncio.to_thread(self._volume.write, path, data, if_match=if_match)

    async def open(self, path: str, mode: str = "r", *, if_match: str | None = None) -> Any:
        """"r"/"rb" returns the file's contents as an in-memory file. "w"/"wb" returns a handle that uploads when closed."""

        opened = await asyncio.to_thread(self._volume.open, path, mode, if_match=if_match)
        return _Upload(opened) if mode in ("w", "wb") else opened

    async def commit(self) -> dict[str, int]:
        return await asyncio.to_thread(self._volume.commit)

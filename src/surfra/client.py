"""Programmatic app updates and job calls. The app name is the stable id."""

from __future__ import annotations

import os
import time
from typing import Any
from urllib.parse import quote

import httpx

from surfra.credentials import as_registry
from surfra.types import Autoscale, Runtime

_DONE = {"succeeded", "failed"}


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
        view = self.poll(timeout=timeout)
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


class SurfraClient:
    """HTTP client for the Surfra API.

    api_key defaults to SURFRA_API_KEY. base_url defaults to SURFRA_MASTER, then https://api.surfra.io.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ):
        self.api_key = api_key or os.environ["SURFRA_API_KEY"]
        self.base_url = (base_url or os.environ.get("SURFRA_MASTER") or "https://api.surfra.io").rstrip("/")
        self._http = httpx.Client(
            timeout=60,
            headers={"Authorization": f"Bearer {self.api_key}"},
            transport=transport,
        )

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

        body: dict[str, Any] = {
            "image": image,
            "entrypoint": entrypoint,
            "env": env or {},
            "registry": as_registry(registry),
            "autoscale": autoscale or {},
        }
        if runtime is not None:
            body["runtime"] = runtime
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

        stages = []
        for row in self.workflow(name).get("stages") or []:
            depends = list(row.get("depends") or [])
            stages.append({"name": row["name"], "app_name": row["app"], "depends_on": depends, "input": {} if depends else payload or {}})
        return Call(self, self.submit(stages, metadata={"workflow": name}))

    def submit(self, stages: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None, callback_url: str | None = None) -> dict[str, Any]:
        """Start a transaction. Each stage names an app and lists the stages it depends on."""

        return _checked(self._http.post(
            f"{self.base_url}/v1/transactions",
            json={"stages": stages, "metadata": metadata or {}, "callback_url": callback_url},
        ))

    def transaction(self, transaction_id: str) -> dict[str, Any]:
        return _checked(self._http.get(f"{self.base_url}/v1/transactions/{transaction_id}"))

    def enqueue(self, app: str, payload: dict[str, Any] | None = None, *, name: str = "call") -> Call:
        """Queue one call to an app and return it before the worker finishes."""

        view = self.submit([{"name": name, "app_name": app, "depends_on": [], "input": payload or {}}])
        return Call(self, view)

    def run(self, app: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Enqueue, poll, and return the output."""

        return self.enqueue(app, payload).result(timeout=timeout)

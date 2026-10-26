"""A named deployment. The same name updates the app in place."""

from __future__ import annotations

from typing import Any

from surfra.client import Call, SurfraClient
from surfra.sdk import registered_stages
from surfra.types import Autoscale, Runtime


class App:
    """The unit you deploy.

    Model code stays in a handler file and does not need this object.
    This object is the devops side: image, entrypoint, env, and autoscaling.
    """

    def __init__(self, name: str, *, client: SurfraClient | None = None):
        if not name or any(char.isspace() for char in name):
            raise ValueError("app name is a single token, for example myllm-staging")
        self.name = name
        self._client = client

    def client(self) -> SurfraClient:
        if self._client is None:
            self._client = SurfraClient()
        return self._client

    def deploy(
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

        return self.client().deploy_app(
            self.name,
            image=image,
            entrypoint=entrypoint,
            env=env,
            registry=registry,
            autoscale=autoscale,
            runtime=runtime,
        )

    def submit(self, stages: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None, callback_url: str | None = None) -> dict[str, Any]:
        return self.client().submit(stages, metadata=metadata, callback_url=callback_url)

    def info(self) -> dict[str, Any]:
        """The saved app: image, entrypoint, runtime, and autoscale."""

        return self.client().app(self.name)

    def volumes(self) -> list[dict[str, str]]:
        """The volumes this app's replicas may reach, as [{"volume", "mode"}]."""

        return self.client().app_volumes(self.name)

    def set_volumes(self, volumes: list[str]) -> list[dict[str, str]]:
        """Replace the app's volumes, such as ["weights:r", "outputs:rw"]. Running replicas see it within 15 minutes."""

        return self.client().set_app_volumes(self.name, volumes)

    def enqueue(self, payload: dict[str, Any] | None = None, *, callback_url: str | None = None) -> Call:
        """Queue one request and return a Call right away.

        Surfra POSTs the task's latest state to callback_url each time it changes. See verify_webhook.
        """

        return self.client().enqueue(self.name, payload, callback_url=callback_url)

    def run(self, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Queue one request and wait for the handler's output."""

        return self.client().run(self.name, payload, timeout=timeout)


class Workflow:
    """A DAG of stages. Each stage names an app, or is filled in later in the console."""

    def __init__(self, name: str, *, client: SurfraClient | None = None):
        if not name or any(char.isspace() for char in name):
            raise ValueError("workflow name is a single token")
        self.name = name
        self._client = client

    def client(self) -> SurfraClient:
        if self._client is None:
            self._client = SurfraClient()
        return self._client

    def deploy(self, stages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """Save the stages registered with @surfra.stage, or the list you pass."""

        rows = stages if stages is not None else [spec.public() for spec in registered_stages()]
        return self.client().deploy_workflow(self.name, rows)

    def enqueue(self, payload: dict[str, Any] | None = None) -> Call:
        """Start one run. Stages with no dependencies receive payload."""

        return self.client().enqueue_workflow(self.name, payload)

    def run(self, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        """Start one run and wait. Returns {stage name: output}, or the output for a single stage."""

        return self.enqueue(payload).result(timeout=timeout)

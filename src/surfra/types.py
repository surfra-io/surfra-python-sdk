"""Shapes for App.deploy. Plain dicts work too. These only help an editor check keys."""

from __future__ import annotations

from typing import Literal, TypedDict


class Runtime(TypedDict, total=False):
    """How a replica runs the image.

    python runs `surfra worker` on the entrypoint file and calls its handler.
    http starts the entrypoint as a server and forwards each request to it.
    """

    engine: Literal["python", "http"]
    port: int | None
    path: str | None
    ready_path: str | None
    ready_timeout: int | None


class ProviderRow(TypedDict, total=False):
    """One row of the priority list. Lower priority numbers are tried first.

    min_replicas and max_replicas only apply when Autoscale.per_provider is True.
    max_replicas left out means the row has no limit of its own.
    """

    provider: str
    priority: int
    gpu: str
    gpu_count: int
    provider_gpu: str
    spot: bool
    num_workers: int
    vcpu: int
    ram_gb: int
    runner: str
    min_replicas: int
    max_replicas: int


class Autoscale(TypedDict, total=False):
    """Replica limits, timeouts, and the machines an app may run on."""

    min_replicas: int
    max_replicas: int
    per_provider: bool
    idle_timeout_sec: int
    boot_timeout_sec: int
    execution_timeout_sec: int
    worker_mode: Literal["isolated", "joint"]
    max_concurrent: int
    disk_gb: int
    http_ports: list[int]
    tcp_ports: list[int]
    gpu: str | None
    providers: list[ProviderRow]

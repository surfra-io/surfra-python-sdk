"""Decorators a model author uses. The worker is the only caller."""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

WarmupFn = Callable[[], None]
HandlerFn = Callable[["Request"], Any]

_warmup: WarmupFn | None = None
_handler: HandlerFn | None = None
_handler_spec: dict[str, Any] | None = None
_stages: list[StageSpec] = []
_current: ContextVar[Request | None] = ContextVar("surfra_request", default=None)


@dataclass
class StageSpec:
    name: str
    depends: tuple[str, ...]
    fn: Callable[..., Any]
    app: str = ""
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None

    def public(self) -> dict[str, Any]:
        row: dict[str, Any] = {"name": self.name, "app": self.app, "depends": list(self.depends)}
        if self.input_schema is not None:
            row["input_schema"] = self.input_schema
        if self.output_schema is not None:
            row["output_schema"] = self.output_schema
        return row


@dataclass
class ProgressEvent:
    fraction: float
    message: str


@dataclass
class Request:
    """One invocation. `input` is the JSON object the client sent."""

    input: dict[str, Any]
    events: list[ProgressEvent] = field(default_factory=list)

    def progress(self, fraction: float, message: str = "") -> None:
        if not 0 <= fraction <= 1:
            raise ValueError(f"progress fraction must be between 0 and 1, got {fraction}")
        self.events.append(ProgressEvent(fraction, message))


def warmup(fn: WarmupFn | None = None, **_options: Any) -> Any:
    """Register the function that loads weights. Called once per worker."""

    def decorate(func: WarmupFn) -> WarmupFn:
        global _warmup
        _warmup = func
        return func

    if fn is not None:
        return decorate(fn)
    return decorate


def batch_size(minimum: int, maximum: int) -> tuple[int, int]:
    """Inclusive bounds for one worker pull. `batch_size(1, 8)` means at most 8."""

    if minimum < 1 or maximum < minimum:
        raise ValueError("batch_size minimum is at least 1 and maximum is at least the minimum")
    return (minimum, maximum)


def take_batch(queued: int, *, batch_min: int, batch_max: int, wait_ms: int, oldest_age_ms: int) -> int:
    """How many queued calls one worker should take.

    Zero means keep waiting. A full batch leaves immediately. Otherwise the
    worker waits until `wait_ms` has passed since the oldest queued call.
    """

    if queued <= 0:
        return 0
    if queued >= batch_max:
        return batch_max
    if queued >= batch_min and oldest_age_ms >= wait_ms:
        return queued
    return 0


def handler(
    fn: HandlerFn | None = None,
    *,
    batch_size: tuple[int, int] = (1, 1),
    wait: int = 0,
    batch_wait: int | None = None,
    concurrent_workers: int = 1,
    input_schema: dict[str, Any] | None = None,
    output_schema: dict[str, Any] | None = None,
) -> Any:
    """Register the function that runs one request, or a list of them.

    `batch_size(1, 8)` and `batch_wait=300` mean the handler receives between
    1 and 8 requests after waiting up to 300 ms. It returns a list of the
    same length, or None when the whole batch failed. `concurrent_workers`
    is how many of those loops share this process.
    """

    low, high = batch_size
    wait_ms = wait if batch_wait is None else batch_wait
    if low < 1 or high < low:
        raise ValueError("batch_size must be (minimum, maximum) with minimum >= 1")
    if wait_ms < 0:
        raise ValueError("batch_wait is milliseconds and cannot be negative")
    if concurrent_workers < 1:
        raise ValueError("concurrent_workers is at least 1")

    def decorate(func: HandlerFn) -> HandlerFn:
        global _handler, _handler_spec
        if _handler is not None:
            raise RuntimeError("a Surfra file registers one handler")
        _handler = func
        _handler_spec = {
            "batch_min": low,
            "batch_max": high,
            "wait_ms": wait_ms,
            "concurrent_workers": concurrent_workers,
            "input_schema": input_schema,
            "output_schema": output_schema,
        }
        return func

    if fn is not None:
        return decorate(fn)
    return decorate


def stage(
    name: str | None = None,
    *,
    app: str = "",
    depends: tuple[str, ...] | list[str] = (),
    input_schema: dict[str, Any] | None = None,
    output_schema: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a function as a workflow node. `depends` names earlier stages."""

    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        _stages.append(
            StageSpec(
                name or func.__name__,
                tuple(depends),
                func,
                app,
                input_schema,
                output_schema,
            )
        )
        return func

    return decorate


_progress_hook: ContextVar[Any] = ContextVar("surfra_progress_hook", default=None)


def bind_progress_hook(hook: Any):
    """Worker installs this so a progress callback can leave the handler process."""

    return _progress_hook.set(hook)


def unbind_progress_hook(token) -> None:
    _progress_hook.reset(token)


def _emit_progress(fraction: float, message: str = "") -> None:
    if not 0 <= fraction <= 1:
        raise ValueError(f"progress fraction must be between 0 and 1, got {fraction}")
    request = _current.get()
    if request is not None:
        request.progress(fraction, message)
    hook = _progress_hook.get()
    if hook is not None:
        hook(fraction, message)


def _decorate_progress(fn: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(fraction: float, *args: Any, **kwargs: Any) -> Any:
        _emit_progress(float(fraction))
        return fn(fraction, *args, **kwargs)

    return wrapped


def progress(fraction: Any = None, message: str = "") -> Any:
    """Report progress for the request that is running.

    Call it from the handler, or decorate the function you pass into the model.
    The model calls that function with a fraction from 0 to 1 and Surfra records
    it on the current task.
    """

    if fraction is None:
        return _decorate_progress
    if callable(fraction):
        return _decorate_progress(fraction)
    request = _current.get()
    if request is None and _progress_hook.get() is None:
        raise RuntimeError("surfra.progress() is only valid inside a handler call")
    _emit_progress(float(fraction), message)


def registered_warmup() -> WarmupFn | None:
    return _warmup


def registered_handler() -> HandlerFn | None:
    return _handler


def registered_handler_spec() -> dict[str, Any] | None:
    return None if _handler_spec is None else dict(_handler_spec)


def adopt_handler_spec(spec: dict[str, Any]) -> None:
    """Parent process copies the spec the child learned by importing the handler."""

    global _handler_spec
    _handler_spec = dict(spec)


def registered_stages() -> list[StageSpec]:
    return list(_stages)


def reset_registry() -> None:
    """Test hook. A worker process loads one user file and does not reset."""

    global _warmup, _handler, _handler_spec
    _warmup = None
    _handler = None
    _handler_spec = None
    _stages.clear()


def bind_request(request: Request):
    return _current.set(request)


def unbind_request(token) -> None:
    _current.reset(token)

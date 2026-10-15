"""Load a user's file and run it in this process."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from surfra.sdk import Request, bind_request, registered_handler, registered_handler_spec, registered_warmup, reset_registry, unbind_request


def load_app(path: str | Path) -> None:
    """Import the user's module so the decorators register warmup and handler."""

    reset_registry()
    file = Path(path).resolve()
    if not file.is_file():
        raise FileNotFoundError(file)
    spec = importlib.util.spec_from_file_location("surfra_user_app", file)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {file}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)


def run_warmup() -> None:
    warmup = registered_warmup()
    if warmup is not None:
        warmup()


def run_handler_batch(payloads: list[dict[str, Any]]) -> Any:
    """Call the handler with a list when batching is on. None fails the batch."""

    handler = registered_handler()
    if handler is None:
        raise RuntimeError("the user file has no @surfra.handler")
    spec = registered_handler_spec() or {}
    if int(spec.get("batch_max") or 1) <= 1:
        output, _request = run_handler(payloads[0])
        return [output]
    requests = [Request(input=payload) for payload in payloads]
    token = bind_request(requests[0])
    try:
        return handler(requests)
    finally:
        unbind_request(token)


def run_handler(payload: dict[str, Any]) -> tuple[Any, Request]:
    handler = registered_handler()
    if handler is None:
        raise RuntimeError("the user file has no @surfra.handler")
    request = Request(input=payload)
    token = bind_request(request)
    try:
        result = handler(request)
    finally:
        unbind_request(token)
    return result, request


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: surfra-run path/to/app.py")
    load_app(sys.argv[1])
    run_warmup()
    print("worker ready")


if __name__ == "__main__":
    main()

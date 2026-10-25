"""Every client test runs twice: once against `surfra` (sync) and once against `surfra.aio` (async).

A test takes the `sx` fixture and writes its body once:

    out = sx(sx.surfra.SurfraClient().run("embed", {}))

sx(value) checks the mode: in sync mode the call must return a plain value, in async mode an awaitable,
which sx runs on this test's event loop. sx.list() drains an iterator or async iterator, and sx.ctx()
enters a context manager or async context manager.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from typing import Any

import pytest

import surfra
import surfra.aio


class Mode:
    def __init__(self, name: str):
        self.name = name
        self.is_async = name == "async"
        self.surfra = surfra.aio if self.is_async else surfra
        self.loop = asyncio.new_event_loop() if self.is_async else None

    def __call__(self, value: Any) -> Any:
        if self.is_async:
            assert inspect.isawaitable(value), f"surfra.aio returned {type(value).__name__}, not an awaitable"
            return self.loop.run_until_complete(value)
        assert not inspect.isawaitable(value), "surfra returned an awaitable from a sync call"
        return value

    def list(self, items: Any) -> list[Any]:
        if self.is_async:
            assert hasattr(items, "__aiter__"), f"surfra.aio returned {type(items).__name__}, not an async iterator"

            async def drain() -> list[Any]:
                return [item async for item in items]

            return self.loop.run_until_complete(drain())
        assert not hasattr(items, "__aiter__")
        return list(items)

    @contextlib.contextmanager
    def ctx(self, value: Any):
        if not self.is_async:
            with value as handle:
                yield handle
            return
        manager = self(value)
        handle = self.loop.run_until_complete(manager.__aenter__())
        try:
            yield handle
        except BaseException as exc:
            if not self.loop.run_until_complete(manager.__aexit__(type(exc), exc, exc.__traceback__)):
                raise
        else:
            self.loop.run_until_complete(manager.__aexit__(None, None, None))

    def close(self) -> None:
        if self.loop is not None:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()


@pytest.fixture(params=["sync", "async"])
def sx(request):
    mode = Mode(request.param)
    yield mode
    mode.close()

"""surfra and surfra.aio expose the same API, and each mode keeps its promise: sync blocks, async doesn't."""

from __future__ import annotations

import asyncio
import inspect
import threading
import time

import httpx
import pytest

import surfra
import surfra.aio

CLASSES = ["App", "Workflow", "Call", "Job", "SurfraClient", "Volume", "VolumePath"]


def _public(cls) -> dict[str, object]:
    """Public methods, inherited ones included. aclose() is the async spelling of close()."""

    found = {name: getattr(cls, name) for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name))}
    if "aclose" in found:
        found["close"] = found.pop("aclose")
    return found


def test_aio_exports_every_name_surfra_does():
    missing = sorted(set(surfra.__all__) - set(surfra.aio.__all__))
    assert missing == []
    for name in surfra.aio.__all__:
        assert hasattr(surfra.aio, name), name


@pytest.mark.parametrize("name", CLASSES)
def test_each_class_has_the_same_methods_and_parameters(name):
    sync_cls, async_cls = getattr(surfra, name), getattr(surfra.aio, name)
    sync_methods, async_methods = _public(sync_cls), _public(async_cls)
    assert sorted(sync_methods) == sorted(async_methods)
    for method, sync_fn in sync_methods.items():
        async_fn = async_methods[method]
        sync_fn = getattr(sync_fn, "__func__", sync_fn)
        async_fn = getattr(async_fn, "__func__", async_fn)
        assert list(inspect.signature(sync_fn).parameters) == list(inspect.signature(async_fn).parameters), f"{name}.{method}"


@pytest.mark.parametrize("name", CLASSES)
def test_network_methods_are_async_only_in_aio(name):
    local = {"grant", "mode", "path", "with_retry"}  # build names, handles or copies; no request
    for method, fn in _public(getattr(surfra.aio, name)).items():
        fn = getattr(fn, "__func__", fn)
        if method in local or method == "client":
            assert not (inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)), method
        else:
            assert inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn), f"surfra.aio.{name}.{method} is not async"
    for method, fn in _public(getattr(surfra, name)).items():
        fn = getattr(fn, "__func__", fn)
        assert not (inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)), f"surfra.{name}.{method} is async"


def _slow_fake(delay: float, finish_after: int = 3):
    seen = {"gets": 0, "loop_in_handler": []}

    def handler(request: httpx.Request) -> httpx.Response:
        try:
            asyncio.get_running_loop()
            seen["loop_in_handler"].append(True)
        except RuntimeError:
            seen["loop_in_handler"].append(False)
        if request.method == "POST":
            return httpx.Response(200, json={"id": "tx_1", "status": "running", "stages": []})
        seen["gets"] += 1
        status = "succeeded" if seen["gets"] >= finish_after else "running"
        return httpx.Response(200, json={"id": "tx_1", "status": status, "stages": [{"name": "call", "output": {"ok": True}}]})

    return handler, seen


def test_waiting_blocks_only_in_sync_mode(sx):
    """Async: other tasks keep running while a result is awaited. Sync: the call blocks and needs no event loop."""

    handler, seen = _slow_fake(0)
    client = sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))
    if sx.is_async:
        ticks = []

        async def ticker():
            while True:
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        async def scenario():
            tick = asyncio.create_task(ticker())
            call = await client.enqueue("hello", {})
            out = await call.result(timeout=5)
            tick.cancel()
            return out

        assert sx(scenario()) == {"ok": True}
        assert len(ticks) >= 20  # Call.poll sleeps 0.5 s between reads; the loop kept running through them
        assert all(seen["loop_in_handler"])
    else:
        start = time.monotonic()
        assert sx(sx(client.enqueue("hello", {})).result(timeout=5)) == {"ok": True}
        assert time.monotonic() - start >= 0.9  # two 0.5 s polls, spent blocking
        assert not any(seen["loop_in_handler"])


def test_a_bounded_wait_raises_timeout_error(sx):
    """Sync: timeout=. Async: the same timeout=, and asyncio.wait_for around the call."""

    handler, _ = _slow_fake(0, finish_after=10**9)
    client = sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))
    call = sx(client.enqueue("hello", {}))
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        sx(call.result(timeout=0.2))
    if sx.is_async:
        with pytest.raises(TimeoutError):
            sx(asyncio.wait_for(call.result(), 0.2))
    assert time.monotonic() - start < 3


def test_many_calls_at_once(sx):
    """100 calls: async with asyncio.gather, sync from a thread pool. Same results either way."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            import json

            n = json.loads(request.content)["stages"][0]["input"]["n"]
            return httpx.Response(200, json={"id": f"tx_{n}", "status": "running", "stages": []})
        n = int(request.url.path.rsplit("_", 1)[1])
        return httpx.Response(200, json={"id": f"tx_{n}", "status": "succeeded", "stages": [{"name": "call", "output": {"n": n}}]})

    client = sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))
    if sx.is_async:
        async def all_of_them():
            return await asyncio.gather(*(client.run("hello", {"n": n}, timeout=5) for n in range(100)))

        outs = sx(all_of_them())
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(16) as pool:
            outs = list(pool.map(lambda n: sx(client.run("hello", {"n": n}, timeout=5)), range(100)))
    assert outs == [{"n": n} for n in range(100)]


def test_clients_close(sx):
    handler, _ = _slow_fake(0)
    client = sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))
    if sx.is_async:
        async def use():
            async with client as open_client:
                await open_client.transaction("tx_1")

        sx(use())
    else:
        with client as open_client:
            open_client.transaction("tx_1")
    assert client._http.is_closed


def test_app_makes_one_client_per_event_loop(sx, monkeypatch):
    monkeypatch.setenv("SURFRA_API_KEY", "sfk_test")
    app = sx.surfra.App("hello")
    if not sx.is_async:
        assert app.client() is app.client()
        return

    async def made():
        return app.client(), app.client()

    first, again = sx(made())
    assert first is again
    other = asyncio.run(made())[0]  # a second event loop gets its own httpx.AsyncClient
    assert other is not first


def test_sync_calls_work_from_threads_and_inside_a_running_loop(sx):
    """Existing code: the sync client from many threads, and from inside async def (blocking, as before)."""

    handler, _ = _slow_fake(0, finish_after=1)
    client = sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))
    if sx.is_async:
        # The async twin: the async client from many tasks on one loop.
        async def tasks():
            return await asyncio.gather(*(client.transaction("tx_1") for _ in range(32)))

        assert all(view["id"] == "tx_1" for view in sx(tasks()))
        return
    errors = []

    def use():
        try:
            assert client.transaction("tx_1")["id"] == "tx_1"
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=use) for _ in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []

    async def inside_async_def():
        return client.transaction("tx_1")  # no await: today's sync behavior

    assert asyncio.run(inside_async_def())["id"] == "tx_1"


def test_the_deploy_setting_wins_over_the_handler(monkeypatch):
    from surfra.worker import worker_count

    monkeypatch.setenv("SURFRA_MAX_ONGOING", "8")
    assert worker_count({"concurrent_workers": 1}) == 8
    monkeypatch.setenv("SURFRA_MAX_ONGOING", "500")
    assert worker_count({"concurrent_workers": 1}) == 64
    monkeypatch.delenv("SURFRA_MAX_ONGOING")
    assert worker_count({"concurrent_workers": 3}) == 3  # a local run, or nothing set at deploy
    monkeypatch.setenv("SURFRA_MAX_ONGOING", "nope")
    assert worker_count({}) == 1

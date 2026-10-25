import json

import httpx
import pytest

from surfra import SurfraError

# Every test here runs against surfra (sync) and surfra.aio (async); see conftest.py.


def _client(sx, handler):
    return sx.surfra.SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))


def test_enqueue_posts_one_stage_and_result_reads_output(sx):
    seen = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["stages"] == [
                {"name": "call", "app_name": "hello", "depends_on": [], "input": {"message": "hi"}}
            ]
            return httpx.Response(200, json={"id": "tx_1", "status": "running", "stages": []})
        seen["gets"] += 1
        status = "running" if seen["gets"] == 1 else "succeeded"
        output = None if status == "running" else {"echo": "hi"}
        return httpx.Response(200, json={"id": "tx_1", "status": status, "stages": [{"name": "call", "status": status, "output": output}]})

    call = sx(_client(sx, handler).enqueue("hello", {"message": "hi"}))
    assert call.id == "tx_1"
    assert sx(call.result(timeout=2)) == {"echo": "hi"}
    assert seen["gets"] == 2


def test_result_raises_the_stage_error(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": "tx_2", "status": "running", "stages": []})
        return httpx.Response(
            200,
            json={"id": "tx_2", "status": "failed", "stages": [{"name": "call", "status": "failed", "output": {"error": "boom"}}]},
        )

    with pytest.raises(RuntimeError, match="boom"):
        sx(_client(sx, handler).run("hello", {}, timeout=1))


def test_poll_times_out_while_running(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": "tx_3", "status": "running", "stages": []})
        return httpx.Response(200, json={"id": "tx_3", "status": "running", "stages": []})

    with pytest.raises(TimeoutError):
        sx(sx(_client(sx, handler).enqueue("hello")).poll(interval=0, timeout=0))


def test_app_run_uses_the_app_name(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["stages"][0]["app_name"] == "myllm-staging"
            return httpx.Response(200, json={"id": "tx_4", "status": "succeeded", "stages": [{"name": "call", "output": {"ok": True}}]})
        return httpx.Response(200, json={"id": "tx_4", "status": "succeeded", "stages": [{"name": "call", "output": {"ok": True}}]})

    client = _client(sx, handler)
    assert sx(sx.surfra.App("myllm-staging", client=client).run({})) == {"ok": True}


def test_deploy_puts_by_name_with_per_provider_limits(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == "/v1/apps/myllm-staging"
        body = json.loads(request.content)
        assert body["runtime"] == {"engine": "python"}
        assert body["autoscale"]["providers"][0]["min_replicas"] == 1
        return httpx.Response(200, json={"id": "sfa_1", "name": "myllm-staging"})

    saved = sx(sx.surfra.App("myllm-staging", client=_client(sx, handler)).deploy(
        image="ghcr.io/me/llm:1",
        entrypoint="app.py",
        runtime={"engine": "python"},
        autoscale={"max_replicas": 5, "per_provider": True, "providers": [{"provider": "salad", "gpu": "rtx3090", "priority": 1, "min_replicas": 1, "max_replicas": 1}]},
    ))
    assert saved["id"] == "sfa_1"


def test_workflow_run_feeds_the_payload_to_root_stages(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/v1/workflows/hello-world":
            return httpx.Response(200, json={"name": "hello-world", "stages": [
                {"name": "hello", "app": "hello", "depends": []},
                {"name": "world", "app": "world", "depends": ["hello"]},
            ]})
        if request.method == "POST":
            stages = json.loads(request.content)["stages"]
            assert stages[0] == {"name": "hello", "app_name": "hello", "depends_on": [], "input": {"x": 1}}
            assert stages[1] == {"name": "world", "app_name": "world", "depends_on": ["hello"], "input": {}}
            return httpx.Response(200, json={"id": "tx_5", "status": "running", "stages": []})
        return httpx.Response(200, json={"id": "tx_5", "status": "succeeded", "stages": [
            {"name": "hello", "output": {"text": "hello"}},
            {"name": "world", "output": {"text": "hello world"}},
        ]})

    result = sx(sx.surfra.Workflow("hello-world", client=_client(sx, handler)).run({"x": 1}, timeout=2))
    assert result == {"hello": {"text": "hello"}, "world": {"text": "hello world"}}


def test_a_refused_request_carries_the_server_reason(sx):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "autoscale provider minimums add up to 3, above max_replicas 2"})

    with pytest.raises(SurfraError, match="above max_replicas") as caught:
        sx(sx.surfra.App("myllm", client=_client(sx, handler)).deploy(image="x", entrypoint="app.py"))
    assert caught.value.status == 400


def test_callback_url_is_sent_with_enqueue(sx):
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "tx_6", "status": "running", "stages": []})

    client = _client(sx, handler)
    sx(client.enqueue("hello", {}, callback_url="https://me.test/hook"))
    sx(sx.surfra.App("hello", client=client).enqueue({}, callback_url="https://me.test/app"))
    sx(client.enqueue("hello", {}))
    assert [body["callback_url"] for body in sent] == ["https://me.test/hook", "https://me.test/app", None]


def test_a_rate_limited_submit_waits_as_asked(sx, monkeypatch):
    waits = []
    if sx.is_async:
        async def fake_sleep(seconds):
            waits.append(seconds)

        monkeypatch.setattr("surfra.aio.asyncio.sleep", fake_sleep)
    else:
        monkeypatch.setattr("surfra.client.time.sleep", waits.append)
    replies = iter([
        httpx.Response(429, headers={"retry-after": "3"}, json={"detail": "slow down"}),
        httpx.Response(200, json={"id": "tx_7", "status": "running", "stages": []}),
    ])
    view = sx(_client(sx, lambda request: next(replies)).submit([]))
    assert view["id"] == "tx_7" and waits == [3.0]


def test_map_yields_outputs_in_order(sx):
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            counter["n"] += 1
            item = json.loads(request.content)["stages"][0]["input"]
            return httpx.Response(200, json={"id": f"tx_{item['i']}", "status": "running", "stages": []})
        number = request.url.path.rsplit("_", 1)[1]
        return httpx.Response(200, json={"id": f"tx_{number}", "status": "succeeded", "stages": [{"name": "call", "output": {"i": int(number)}}]})

    outs = sx.list(_client(sx, handler).map("hello", [{"i": i} for i in range(5)], concurrency=2, timeout=2))
    assert outs == [{"i": i} for i in range(5)] and counter["n"] == 5


def test_job_wait_logs_and_stop(sx):
    lines = [{"t": f"2026-10-25T00:00:0{i}Z", "s": "out", "m": f"line {i}"} for i in range(3)]
    seen = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/jobs" and request.method == "POST":
            return httpx.Response(200, json={"id": "job_1", "status": "queued"})
        if path.endswith("/logs"):
            return httpx.Response(200, json={"lines": lines})
        if path.endswith("/stop"):
            return httpx.Response(200, json={"id": "job_1", "status": "stopped"})
        seen["gets"] += 1
        return httpx.Response(200, json={"id": "job_1", "status": "running" if seen["gets"] < 2 else "succeeded"})

    client = _client(sx, handler)
    job = sx(client.submit_job("echo hi", image="img"))
    assert job.status == "queued"
    assert sx(job.wait(interval=0, timeout=2))["status"] == "succeeded"
    assert [line["m"] for line in sx.list(job.logs())] == ["line 0", "line 1", "line 2"]
    assert sx(job.stop())["status"] == "stopped"
    assert sx(client.job("job_1")).id == "job_1"


def test_ids_are_escaped_in_paths(sx):
    paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"id": "x", "status": "running", "stages": []})

    client = _client(sx, handler)
    sx(client.transaction("tx/1"))
    sx(client.job("job 1"))
    assert paths == ["/v1/transactions/tx%2F1", "/v1/jobs/job%201"]

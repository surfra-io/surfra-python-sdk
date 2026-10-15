import json

import httpx
import pytest

from surfra import App, SurfraClient, SurfraError, Workflow


def _client(handler) -> SurfraClient:
    return SurfraClient(api_key="sfk_test", base_url="http://surfra.test", transport=httpx.MockTransport(handler))


def test_enqueue_posts_one_stage_and_result_reads_output():
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

    call = _client(handler).enqueue("hello", {"message": "hi"})
    assert call.id == "tx_1"
    assert call.result(timeout=2) == {"echo": "hi"}
    assert seen["gets"] == 2


def test_result_raises_the_stage_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": "tx_2", "status": "running", "stages": []})
        return httpx.Response(
            200,
            json={"id": "tx_2", "status": "failed", "stages": [{"name": "call", "status": "failed", "output": {"error": "boom"}}]},
        )

    with pytest.raises(RuntimeError, match="boom"):
        _client(handler).run("hello", {}, timeout=1)


def test_poll_times_out_while_running():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": "tx_3", "status": "running", "stages": []})
        return httpx.Response(200, json={"id": "tx_3", "status": "running", "stages": []})

    with pytest.raises(TimeoutError):
        _client(handler).enqueue("hello").poll(interval=0, timeout=0)


def test_app_run_uses_the_app_name():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["stages"][0]["app_name"] == "myllm-staging"
            return httpx.Response(200, json={"id": "tx_4", "status": "succeeded", "stages": [{"name": "call", "output": {"ok": True}}]})
        return httpx.Response(200, json={"id": "tx_4", "status": "succeeded", "stages": [{"name": "call", "output": {"ok": True}}]})

    client = _client(handler)
    assert App("myllm-staging", client=client).run({}) == {"ok": True}


def test_deploy_puts_by_name_with_per_provider_limits():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == "/v1/apps/myllm-staging"
        body = json.loads(request.content)
        assert body["runtime"] == {"engine": "python"}
        assert body["autoscale"]["providers"][0]["min_replicas"] == 1
        return httpx.Response(200, json={"id": "sfa_1", "name": "myllm-staging"})

    saved = App("myllm-staging", client=_client(handler)).deploy(
        image="ghcr.io/me/llm:1",
        entrypoint="app.py",
        runtime={"engine": "python"},
        autoscale={"max_replicas": 5, "per_provider": True, "providers": [{"provider": "salad", "gpu": "rtx3090", "priority": 1, "min_replicas": 1, "max_replicas": 1}]},
    )
    assert saved["id"] == "sfa_1"


def test_workflow_run_feeds_the_payload_to_root_stages():
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

    result = Workflow("hello-world", client=_client(handler)).run({"x": 1}, timeout=2)
    assert result == {"hello": {"text": "hello"}, "world": {"text": "hello world"}}


def test_a_refused_request_carries_the_server_reason():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "autoscale provider minimums add up to 3, above max_replicas 2"})

    with pytest.raises(SurfraError, match="above max_replicas") as caught:
        App("myllm", client=_client(handler)).deploy(image="x", entrypoint="app.py")
    assert caught.value.status == 400

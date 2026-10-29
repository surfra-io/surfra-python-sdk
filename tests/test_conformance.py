"""The shared SDK cases (surfra-sdk-tests/CASES.md), run against its fake server.

Set SURFRA_SDK_TESTS to the suite's folder; a sibling checkout is found on its own. Skipped when absent.
Each case runs against surfra (sync) and surfra.aio (async) through the `sx` fixture in conftest.py.
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from surfra.client import SurfraError

SUITE = Path(os.environ.get("SURFRA_SDK_TESTS") or Path(__file__).resolve().parents[2] / "surfra-sdk-tests")
pytestmark = pytest.mark.skipif(not (SUITE / "server.py").is_file(), reason="surfra-sdk-tests is not checked out")
PART = 1024


@pytest.fixture(scope="module")
def server():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = {**os.environ, "SURFRA_VOLUMES_PART_BYTES": str(PART)}
    process = subprocess.Popen([sys.executable, str(SUITE / "server.py"), "--port", str(port)], stdout=subprocess.PIPE, env=env, text=True)
    assert "listening" in process.stdout.readline()
    yield f"http://127.0.0.1:{port}"
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def fake(server, tmp_path, monkeypatch, sx):
    for name in ("SURFRA_VOLUMES_TOKEN_FILE", "SURFRA_REPLICA_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SURFRA_MASTER", server)
    monkeypatch.setenv("SURFRA_VOLUMES_URL", server)
    monkeypatch.setenv("SURFRA_API_KEY", "sfk_test")
    monkeypatch.setenv("SURFRA_VOLUMES_PART_BYTES", str(PART))
    monkeypatch.setenv("SURFRA_VOLUME_CACHE", str(tmp_path / "cache"))
    monkeypatch.setattr("surfra.host.CONFIG", tmp_path / "no-config.json")
    httpx.post(f"{server}/_test/reset")

    class Fake:
        url = server
        cache = tmp_path / "cache"

        def seed(self, volume, path, body):
            httpx.post(f"{server}/_test/files", json={"volume": volume, "path": path, "body": body})

        def state(self):
            return httpx.get(f"{server}/_test/state").json()

        def faults(self, *items):
            httpx.post(f"{server}/_test/faults", json={"faults": list(items)})

        def count(self, method, path):
            return sum(1 for item in self.state()["log"] if item["method"] == method and item["path"].startswith(path))

        def volume(self, name):
            httpx.post(f"{server}/v1/volumes", json={"name": name}, headers={"Authorization": "Bearer sfk_test"})
            return sx.surfra.Volume(name)

    return Fake()


def v01_small_upload_round_trips(fake, tmp_path, sx):
    source = tmp_path / "a.txt"
    source.write_text("hello")
    weights = fake.volume("weights")
    sx(weights.put_file(source, "a.txt"))
    assert sx(weights.open("a.txt")).read() == "hello"
    assert not any(item["path"].endswith("/uploads") for item in fake.state()["log"])


def v02_large_upload_goes_in_parts(fake, tmp_path, sx):
    source = tmp_path / "big.bin"
    data = os.urandom(PART * 3 + 17)
    source.write_bytes(data)
    weights = fake.volume("weights")
    sx(weights.put_file(source, "big.bin"))
    assert sx(weights.open("big.bin", "rb")).read() == data
    assert any(item["path"].endswith("/uploads") for item in fake.state()["log"])


def v03_put_directory_keeps_relative_paths(fake, tmp_path, sx):
    (tmp_path / "m" / "sub").mkdir(parents=True)
    (tmp_path / "m" / "a").write_text("a")
    (tmp_path / "m" / "sub" / "b").write_text("b")
    assert sx(fake.volume("weights").put_directory(tmp_path / "m", "minilm/")) == 2
    assert sorted(fake.state()["files"]["weights"]) == ["minilm/a", "minilm/sub/b"]


def v04_ls_lists_one_level_or_everything(fake, sx):
    weights = fake.volume("weights")
    for path in ("m/a", "m/sub/b", "top"):
        fake.seed("weights", path, "x")
    assert [entry.path for entry in sx(weights.ls("m/"))] == ["m/a", "m/sub/"]
    assert sorted(entry.path for entry in sx(weights.ls("", recursive=True))) == ["m/a", "m/sub/b", "top"]


def v05_local_downloads_once(fake, sx):
    weights = fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    fake.seed("weights", "m/b", "bbb")
    path = Path(sx(weights.path("m/").local()))
    assert (path / "a").read_text() == "aaa" and (path / "b").read_text() == "bbb"
    before = len(fake.state()["log"])
    sx(weights.path("m/").local())
    assert len(fake.state()["log"]) == before + 1  # only the state read itself


def v06_a_new_object_fetches_only_changes(fake, sx):
    fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    fake.seed("weights", "m/b", "bbb")
    sx(sx.surfra.Volume("weights").path("m/").local())
    fake.seed("weights", "m/a", "AAAA")
    httpx.post(f"{fake.url}/v1/weights/delete", json={"paths": ["m/b"]}, headers={"Authorization": "Bearer sfk_test"})
    downloads = fake.state()["downloads"]
    path = Path(sx(sx.surfra.Volume("weights").path("m/").local()))
    assert fake.state()["downloads"] == downloads + 1
    assert (path / "a").read_text() == "AAAA" and not (path / "b").exists()


def v07_commit_uploads_changes_and_deletes(fake, sx):
    ckpts = fake.volume("ckpts")
    for path in ("r/a", "r/b", "r/c"):
        fake.seed("ckpts", path, path)
    folder = Path(sx(ckpts.path("r/").local(mode="rw")))
    (folder / "a").write_text("changed!")
    (folder / "d").write_text("new")
    (folder / "c").unlink()
    assert sx(ckpts.commit()) == {"uploaded": 2, "deleted": 1}
    files = fake.state()["files"]["ckpts"]
    assert sorted(files) == ["r/a", "r/b", "r/d"] and files["r/a"]["body"] == "changed!"
    assert sx(ckpts.commit()) == {"uploaded": 0, "deleted": 0}


def v08_open_write_uploads_on_close_only(fake, sx):
    results = fake.volume("results")
    with sx.ctx(results.open("runs/1.jsonl", "w")) as handle:
        handle.write("one\n")
    with pytest.raises(RuntimeError):
        with sx.ctx(results.open("runs/2.jsonl", "w")) as handle:
            handle.write("partial")
            raise RuntimeError("boom")
    assert sorted(fake.state()["files"]["results"]) == ["runs/1.jsonl"]


def v09_rm_file_and_folder(fake, sx):
    weights = fake.volume("weights")
    for path in ("a", "old/x", "old/y"):
        fake.seed("weights", path, "x")
    sx(weights.rm("a"))
    assert sx(weights.rm("old/", recursive=True)) == 2
    assert fake.state()["files"]["weights"] == {}


def v10_missing_file_raises_404(fake, sx):
    with pytest.raises(SurfraError) as caught:
        sx(fake.volume("weights").open("nope"))
    assert caught.value.status == 404 and "no such file" in caught.value.detail


def v11_token_file_wins_over_the_api_key(fake, tmp_path, monkeypatch, sx):
    token = tmp_path / "token"
    token.write_text("aaa.bbb.ccc")
    monkeypatch.setenv("SURFRA_VOLUMES_TOKEN_FILE", str(token))
    sx(fake.volume("weights").ls())
    sent = [item for item in fake.state()["log"] if not item["path"].startswith("/_test/")]
    assert sent[-1]["auth"] == "Bearer aaa.bbb.ccc"


def v12_no_credential_says_what_to_set(fake, monkeypatch, sx):
    monkeypatch.delenv("SURFRA_API_KEY")
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.Volume("weights").ls())
    assert "SURFRA_API_KEY" in str(caught.value)


def v13_writing_to_a_new_volume_creates_it(fake, sx):
    with sx.ctx(sx.surfra.Volume("fresh").open("a.txt", "w")) as handle:
        handle.write("x")
    assert fake.state()["files"]["fresh"]["a.txt"]["body"] == "x"


def v14_another_sdks_state_file_is_honored(fake, sx):
    fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    local = fake.cache / "vol-weights" / "m" / "a"
    local.parent.mkdir(parents=True)
    local.write_text("aaa")
    etag = fake.state()["files"]["weights"]["m/a"]["etag"]
    state = fake.cache / ".state" / "vol-weights.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"m/a": {"etag": etag, "size": 3, "mtime_ns": local.stat().st_mtime_ns}}))
    sx(sx.surfra.Volume("weights").path("m/").local())
    assert fake.state()["downloads"] == 0


def a01_run_returns_the_output(fake, sx):
    assert sx(sx.surfra.SurfraClient().run("embed", {"texts": ["hi"]}, timeout=30)) == {"app": "embed", "echo": {"texts": ["hi"]}}


def a02_enqueue_then_result(fake, sx):
    call = sx(sx.surfra.SurfraClient().enqueue("embed", {"n": 1}))
    assert sx(call.result(timeout=30)) == {"app": "embed", "echo": {"n": 1}}
    with pytest.raises(RuntimeError, match="ValueError"):
        sx(sx(sx.surfra.SurfraClient().enqueue("fails", {})).result(timeout=30))


def a06_submit_a_graph_and_wait(fake, sx):
    client = sx.surfra.SurfraClient()
    view = sx(client.submit([
        {"name": "a", "app_name": "embed", "depends_on": [], "input": {"n": 1}},
        {"name": "b", "app_name": "rerank", "depends_on": ["a"], "input": {}},
    ]))
    assert sx(sx.surfra.Call(client, view).result(timeout=30)) == {"a": {"app": "embed", "echo": {"n": 1}}, "b": {"app": "rerank", "echo": {}}}


def j01_job_with_volumes_finishes(fake, sx):
    job = sx(sx.surfra.SurfraClient().submit_job("python train.py", image="img", providers=[{"provider": "local"}], volumes=["weights:r"]))
    assert fake.state()["jobs"][job.id]["volumes"] == ["weights:r"]
    view = sx(job.wait(interval=0.05, timeout=30))
    assert view["status"] == "succeeded" and view["exit_code"] == 0


def v15_write_and_read_helpers(fake, sx):
    weights = fake.volume("weights")
    sx(weights.write("bytes.bin", b"\x00\x01raw"))
    assert sx(weights.read("bytes.bin")) == b"\x00\x01raw"


def c01_control_plane_calls_skip_the_volumes_token(fake, tmp_path, monkeypatch, sx):
    token = tmp_path / "token"
    token.write_text("aaa.bbb.ccc")
    monkeypatch.setenv("SURFRA_VOLUMES_TOKEN_FILE", str(token))
    weights = sx.surfra.Volume("weights")
    sx(weights.create())
    sx(weights.ls())
    sx(sx.surfra.SurfraClient().run("embed", {}, timeout=30))
    sent = {item["path"]: item["auth"] for item in fake.state()["log"] if not item["path"].startswith("/_test/")}
    assert sent["/v1/volumes"] == "Bearer sfk_test"
    assert sent["/v1/transactions"] == "Bearer sfk_test"
    assert sent["/v1/weights/list"] == "Bearer aaa.bbb.ccc"


def a03_app_deploy_sends_the_spec(fake, sx):
    saved = sx(sx.surfra.App("embed").deploy(image="ghcr.io/acme/embed:1", entrypoint="app.py", autoscale={"max_replicas": 2}, runtime={"engine": "python"}))
    assert saved["name"] == "embed" and saved["id"].startswith("sfa_")
    assert saved["image"] == "ghcr.io/acme/embed:1" and saved["autoscale"] == {"max_replicas": 2}
    assert any(item["method"] == "PUT" and item["path"] == "/v1/apps/embed" for item in fake.state()["log"])


def a04_app_run_and_volumes(fake, sx):
    app = sx.surfra.App("embed")
    assert sx(app.run({"x": 1}, timeout=30)) == {"app": "embed", "echo": {"x": 1}}
    sx(app.set_volumes(["weights:r"]))
    assert sx(app.volumes()) == [{"volume": "weights", "mode": "r"}]


def a05_a_rate_limited_submit_waits_and_succeeds(fake, sx):
    assert sx(sx.surfra.SurfraClient().run("busy", {"n": 1}, timeout=30)) == {"app": "busy", "echo": {"n": 1}}
    assert [item["path"] for item in fake.state()["log"]].count("/v1/transactions") == 2


def w01_workflow_run_returns_each_stage(fake, sx):
    out = sx(sx.surfra.Workflow("pipeline").run({"q": "hi"}, timeout=30))
    assert out == {"a": {"app": "embed", "echo": {"q": "hi"}}, "b": {"app": "rerank", "echo": {}}}


def w02_workflow_deploy_saves_stages(fake, sx):
    flow = sx.surfra.Workflow("solo")
    sx(flow.deploy([{"name": "only", "app": "embed", "depends": []}]))
    assert sx(flow.run({"q": "hi"}, timeout=30)) == {"app": "embed", "echo": {"q": "hi"}}


def j02_job_logs_oldest_first(fake, sx):
    job = sx(sx.surfra.SurfraClient().submit_job("echo hi", image="img", providers=[{"provider": "local"}]))
    assert [line["m"] for line in sx.list(job.logs())] == ["line 0", "line 1", "line 2"]


def h01_callback_signatures_verify(sx):
    """Pure, so the same function in both modes. Every vector in webhooks.json."""

    doc = json.loads((SUITE / "webhooks.json").read_text())
    for case in doc["cases"]:
        if case["ok"]:
            assert sx.surfra.verify_webhook(case["body"].encode(), {"x-surfra-signature": case["signature"]}, case["secret"], now=case["now"])["seq"] == 7, case["name"]
        else:
            with pytest.raises(sx.surfra.InvalidSignature):
                sx.surfra.verify_webhook(case["body"].encode(), case["signature"], [case["secret"]], now=case["now"])


def _refusal(status, kind, retry_after=None):
    fault = {"status": status, "body": {"detail": kind.lower(), "error": {"type": kind, "message": kind.lower()}}}
    if retry_after is not None:
        fault["body"]["error"]["retry_after"] = retry_after
        fault["headers"] = {"Retry-After": str(retry_after)}
    return fault


SUBMIT = {"method": "POST", "path": "/v1/transactions"}
POLL = {"method": "GET", "path": "/v1/transactions/"}


def r01_rate_limited_waits_retry_after_then_succeeds(fake, sx):
    fake.faults({**SUBMIT, **_refusal(429, "RATE_LIMITED", 1)})
    started = time.monotonic()
    assert sx(sx.surfra.SurfraClient().run("embed", {"n": 1}, timeout=30)) == {"app": "embed", "echo": {"n": 1}}
    assert time.monotonic() - started >= 0.9 and fake.count("POST", "/v1/transactions") == 2


def r02_queue_full_is_retried(fake, sx):
    fake.faults({**SUBMIT, **_refusal(503, "QUEUE_FULL", 0)})
    assert sx(sx.surfra.SurfraClient().run("embed", {}, timeout=30)) == {"app": "embed", "echo": {}}
    assert fake.count("POST", "/v1/transactions") == 2


def r03_server_busy_on_a_get_is_retried(fake, sx):
    client = sx.surfra.SurfraClient()
    call = sx(client.enqueue("embed", {"n": 3}))
    fake.faults({**POLL, **_refusal(503, "SERVER_BUSY", 0)})
    assert sx(call.result(timeout=30)) == {"app": "embed", "echo": {"n": 3}}


def r04_app_paused_is_not_retried(fake, sx):
    fake.faults({**SUBMIT, **_refusal(503, "APP_PAUSED")})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().enqueue("embed", {}))
    assert (caught.value.status, caught.value.type, caught.value.attempts) == (503, "APP_PAUSED", 1)
    assert fake.count("POST", "/v1/transactions") == 1


def r05_defcon_active_is_not_retried(fake, sx):
    fake.faults({**SUBMIT, **_refusal(503, "DEFCON_ACTIVE")})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().enqueue("embed", {}))
    assert caught.value.type == "DEFCON_ACTIVE" and fake.count("POST", "/v1/transactions") == 1


def r13_http_calls_limit_is_not_retried(fake, sx):
    fake.faults({**SUBMIT, **_refusal(402, "HTTP_CALLS_LIMIT")})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().enqueue("embed", {}))
    assert (caught.value.status, caught.value.type, caught.value.attempts) == (402, "HTTP_CALLS_LIMIT", 1)
    assert str(caught.value)
    assert fake.count("POST", "/v1/transactions") == 1


def r06_a_plain_502_is_retried_on_get_only(fake, sx):
    client = sx.surfra.SurfraClient()
    call = sx(client.enqueue("embed", {}))
    fake.faults({**POLL, "status": 502, "raw": "<html>bad gateway</html>"})
    assert sx(call.result(timeout=30)) == {"app": "embed", "echo": {}}
    fake.faults({**SUBMIT, "status": 502, "raw": "<html>bad gateway</html>"})
    with pytest.raises(SurfraError) as caught:
        sx(client.enqueue("embed", {}))
    assert caught.value.status == 502 and caught.value.type is None and not caught.value.outcome_unknown
    assert fake.count("POST", "/v1/transactions") == 2  # one from the first enqueue, one refused


def r07_a_plain_504_on_a_submit_is_an_unknown_outcome(fake, sx):
    fake.faults({**SUBMIT, "status": 504, "raw": "upstream timed out"})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().enqueue("embed", {}))
    assert caught.value.status == 504 and caught.value.outcome_unknown and "may be running" in str(caught.value)
    assert fake.count("POST", "/v1/transactions") == 1


def r08_a_dropped_connection_is_retried_on_get_only(fake, sx):
    client = sx.surfra.SurfraClient()
    call = sx(client.enqueue("embed", {}))
    fake.faults({**POLL, "drop": True})
    assert sx(call.result(timeout=30)) == {"app": "embed", "echo": {}}
    fake.faults({**SUBMIT, "drop": True})
    with pytest.raises(SurfraError) as caught:
        sx(client.enqueue("embed", {}))
    assert caught.value.outcome_unknown and caught.value.status == 0 and "may be running" in str(caught.value)
    assert fake.count("POST", "/v1/transactions") == 2


def r09_a_400_is_not_retried(fake, sx):
    fake.faults({**SUBMIT, "status": 400, "body": {"detail": "stages must be a list"}})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().enqueue("embed", {}))
    assert caught.value.status == 400 and caught.value.attempts == 1 and fake.count("POST", "/v1/transactions") == 1


def r10_a_refused_connection_is_retried(fake, sx):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    events = []
    client = sx.surfra.SurfraClient(base_url=f"http://127.0.0.1:{port}", max_retries=2, on_retry=events.append)
    with pytest.raises(SurfraError) as caught:
        sx(client.enqueue("embed", {}))
    assert caught.value.attempts == 3 and caught.value.status == 0 and not caught.value.outcome_unknown
    assert [event.attempt for event in events] == [1, 2] and all(event.status is None for event in events)


def r11_retries_stop_after_max_retries(fake, sx):
    fake.faults({**SUBMIT, **_refusal(503, "SERVER_BUSY", 0), "times": 10})
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient(max_retries=2).enqueue("embed", {}))
    assert (caught.value.type, caught.value.attempts) == ("SERVER_BUSY", 3)
    assert fake.count("POST", "/v1/transactions") == 3


def r12_a_wait_past_the_deadline_is_not_taken(fake, sx):
    fake.faults({**SUBMIT, **_refusal(429, "RATE_LIMITED", 30)})
    started = time.monotonic()
    with pytest.raises(SurfraError) as caught:
        sx(sx.surfra.SurfraClient().with_retry(retry_deadline=2).enqueue("embed", {}))
    assert time.monotonic() - started < 1.5
    assert (caught.value.type, caught.value.retry_after, caught.value.attempts) == ("RATE_LIMITED", 30, 1)


# pytest collects test_* functions; the case names above are the shared ids.
for _name, _case in list(globals().items()):
    if _name[:1] in "vacjwhr" and _name[1:3].isdigit() and callable(_case):
        globals()[f"test_{_name}"] = _case

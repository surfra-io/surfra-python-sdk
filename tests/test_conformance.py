"""The shared SDK cases (surfra-sdk-tests/CASES.md), run against its fake server.

Set SURFRA_SDK_TESTS to the suite's folder; a sibling checkout is found on its own. Skipped when absent.
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

import surfra
from surfra.client import SurfraClient, SurfraError

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
def fake(server, tmp_path, monkeypatch):
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

        def volume(self, name):
            httpx.post(f"{server}/v1/volumes", json={"name": name}, headers={"Authorization": "Bearer sfk_test"})
            return surfra.Volume(name)

    return Fake()


def v01_small_upload_round_trips(fake, tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("hello")
    weights = fake.volume("weights")
    weights.put_file(source, "a.txt")
    assert weights.open("a.txt").read() == "hello"
    assert not any(item["path"].endswith("/uploads") for item in fake.state()["log"])


def v02_large_upload_goes_in_parts(fake, tmp_path):
    source = tmp_path / "big.bin"
    data = os.urandom(PART * 3 + 17)
    source.write_bytes(data)
    weights = fake.volume("weights")
    weights.put_file(source, "big.bin")
    assert weights.open("big.bin", "rb").read() == data
    assert any(item["path"].endswith("/uploads") for item in fake.state()["log"])


def v03_put_directory_keeps_relative_paths(fake, tmp_path):
    (tmp_path / "m" / "sub").mkdir(parents=True)
    (tmp_path / "m" / "a").write_text("a")
    (tmp_path / "m" / "sub" / "b").write_text("b")
    assert fake.volume("weights").put_directory(tmp_path / "m", "minilm/") == 2
    assert sorted(fake.state()["files"]["weights"]) == ["minilm/a", "minilm/sub/b"]


def v04_ls_lists_one_level_or_everything(fake):
    weights = fake.volume("weights")
    for path in ("m/a", "m/sub/b", "top"):
        fake.seed("weights", path, "x")
    assert [entry.path for entry in weights.ls("m/")] == ["m/a", "m/sub/"]
    assert sorted(entry.path for entry in weights.ls("", recursive=True)) == ["m/a", "m/sub/b", "top"]


def v05_local_downloads_once(fake):
    weights = fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    fake.seed("weights", "m/b", "bbb")
    path = Path(weights.path("m/").local())
    assert (path / "a").read_text() == "aaa" and (path / "b").read_text() == "bbb"
    before = len(fake.state()["log"])
    weights.path("m/").local()
    assert len(fake.state()["log"]) == before + 1  # only the state read itself


def v06_a_new_object_fetches_only_changes(fake):
    fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    fake.seed("weights", "m/b", "bbb")
    surfra.Volume("weights").path("m/").local()
    fake.seed("weights", "m/a", "AAAA")
    httpx.post(f"{fake.url}/v1/weights/delete", json={"paths": ["m/b"]}, headers={"Authorization": "Bearer sfk_test"})
    downloads = fake.state()["downloads"]
    path = Path(surfra.Volume("weights").path("m/").local())
    assert fake.state()["downloads"] == downloads + 1
    assert (path / "a").read_text() == "AAAA" and not (path / "b").exists()


def v07_commit_uploads_changes_and_deletes(fake):
    ckpts = fake.volume("ckpts")
    for path in ("r/a", "r/b", "r/c"):
        fake.seed("ckpts", path, path)
    folder = Path(ckpts.path("r/").local(mode="rw"))
    (folder / "a").write_text("changed!")
    (folder / "d").write_text("new")
    (folder / "c").unlink()
    assert ckpts.commit() == {"uploaded": 2, "deleted": 1}
    files = fake.state()["files"]["ckpts"]
    assert sorted(files) == ["r/a", "r/b", "r/d"] and files["r/a"]["body"] == "changed!"
    assert ckpts.commit() == {"uploaded": 0, "deleted": 0}


def v08_open_write_uploads_on_close_only(fake):
    results = fake.volume("results")
    with results.open("runs/1.jsonl", "w") as handle:
        handle.write("one\n")
    with pytest.raises(RuntimeError):
        with results.open("runs/2.jsonl", "w") as handle:
            handle.write("partial")
            raise RuntimeError("boom")
    assert sorted(fake.state()["files"]["results"]) == ["runs/1.jsonl"]


def v09_rm_file_and_folder(fake):
    weights = fake.volume("weights")
    for path in ("a", "old/x", "old/y"):
        fake.seed("weights", path, "x")
    weights.rm("a")
    assert weights.rm("old/", recursive=True) == 2
    assert fake.state()["files"]["weights"] == {}


def v10_missing_file_raises_404(fake):
    with pytest.raises(SurfraError) as caught:
        fake.volume("weights").open("nope")
    assert caught.value.status == 404 and "no such file" in caught.value.detail


def v11_token_file_wins_over_the_api_key(fake, tmp_path, monkeypatch):
    token = tmp_path / "token"
    token.write_text("aaa.bbb.ccc")
    monkeypatch.setenv("SURFRA_VOLUMES_TOKEN_FILE", str(token))
    fake.volume("weights").ls()
    sent = [item for item in fake.state()["log"] if not item["path"].startswith("/_test/")]
    assert sent[-1]["auth"] == "Bearer aaa.bbb.ccc"


def v12_no_credential_says_what_to_set(fake, monkeypatch):
    monkeypatch.delenv("SURFRA_API_KEY")
    with pytest.raises(SurfraError) as caught:
        surfra.Volume("weights").ls()
    assert "SURFRA_API_KEY" in str(caught.value)


def v13_writing_to_a_new_volume_creates_it(fake):
    with surfra.Volume("fresh").open("a.txt", "w") as handle:
        handle.write("x")
    assert fake.state()["files"]["fresh"]["a.txt"]["body"] == "x"


def v14_another_sdks_state_file_is_honored(fake):
    fake.volume("weights")
    fake.seed("weights", "m/a", "aaa")
    local = fake.cache / "vol-weights" / "m" / "a"
    local.parent.mkdir(parents=True)
    local.write_text("aaa")
    etag = fake.state()["files"]["weights"]["m/a"]["etag"]
    state = fake.cache / ".state" / "vol-weights.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"m/a": {"etag": etag, "size": 3, "mtime_ns": local.stat().st_mtime_ns}}))
    surfra.Volume("weights").path("m/").local()
    assert fake.state()["downloads"] == 0


def a01_run_returns_the_output(fake):
    assert SurfraClient().run("embed", {"texts": ["hi"]}, timeout=30) == {"app": "embed", "echo": {"texts": ["hi"]}}


def a02_enqueue_then_result(fake):
    call = SurfraClient().enqueue("embed", {"n": 1})
    assert call.result(timeout=30) == {"app": "embed", "echo": {"n": 1}}
    with pytest.raises(RuntimeError, match="ValueError"):
        SurfraClient().enqueue("fails", {}).result(timeout=30)


def j01_job_with_volumes_finishes(fake):
    job = SurfraClient().submit_job("python train.py", image="img", providers=[{"provider": "local"}], volumes=["weights:r"])
    assert fake.state()["jobs"][job.id]["volumes"] == ["weights:r"]
    view = job.wait(interval=0.05, timeout=30)
    assert view["status"] == "succeeded" and view["exit_code"] == 0


def v15_write_and_read_helpers(fake):
    weights = fake.volume("weights")
    weights.write("bytes.bin", b"\x00\x01raw")
    assert weights.read("bytes.bin") == b"\x00\x01raw"


def c01_control_plane_calls_skip_the_volumes_token(fake, tmp_path, monkeypatch):
    token = tmp_path / "token"
    token.write_text("aaa.bbb.ccc")
    monkeypatch.setenv("SURFRA_VOLUMES_TOKEN_FILE", str(token))
    weights = surfra.Volume("weights")
    weights.create()
    weights.ls()
    SurfraClient().run("embed", {}, timeout=30)
    sent = {item["path"]: item["auth"] for item in fake.state()["log"] if not item["path"].startswith("/_test/")}
    assert sent["/v1/volumes"] == "Bearer sfk_test"
    assert sent["/v1/transactions"] == "Bearer sfk_test"
    assert sent["/v1/weights/list"] == "Bearer aaa.bbb.ccc"


def a03_app_deploy_sends_the_spec(fake):
    saved = surfra.App("embed").deploy(image="ghcr.io/acme/embed:1", entrypoint="app.py", autoscale={"max_replicas": 2}, runtime={"engine": "python"})
    assert saved["name"] == "embed" and saved["id"].startswith("sfa_")
    assert saved["image"] == "ghcr.io/acme/embed:1" and saved["autoscale"] == {"max_replicas": 2}
    assert any(item["method"] == "PUT" and item["path"] == "/v1/apps/embed" for item in fake.state()["log"])


def a04_app_run_and_volumes(fake):
    app = surfra.App("embed")
    assert app.run({"x": 1}, timeout=30) == {"app": "embed", "echo": {"x": 1}}
    app.set_volumes(["weights:r"])
    assert app.volumes() == [{"volume": "weights", "mode": "r"}]


def a05_a_rate_limited_submit_waits_and_succeeds(fake):
    assert SurfraClient().run("busy", {"n": 1}, timeout=30) == {"app": "busy", "echo": {"n": 1}}
    assert [item["path"] for item in fake.state()["log"]].count("/v1/transactions") == 2


def w01_workflow_run_returns_each_stage(fake):
    out = surfra.Workflow("pipeline").run({"q": "hi"}, timeout=30)
    assert out == {"a": {"app": "embed", "echo": {"q": "hi"}}, "b": {"app": "rerank", "echo": {}}}


def w02_workflow_deploy_saves_stages(fake):
    flow = surfra.Workflow("solo")
    flow.deploy([{"name": "only", "app": "embed", "depends": []}])
    assert flow.run({"q": "hi"}, timeout=30) == {"app": "embed", "echo": {"q": "hi"}}


def j02_job_logs_oldest_first(fake):
    job = SurfraClient().submit_job("echo hi", image="img", providers=[{"provider": "local"}])
    assert [line["m"] for line in job.logs()] == ["line 0", "line 1", "line 2"]


# pytest collects test_* functions; the case names above are the shared ids.
for _name, _case in list(globals().items()):
    if _name[:1] in "vacjw" and _name[1:3].isdigit() and callable(_case):
        globals()[f"test_{_name}"] = _case

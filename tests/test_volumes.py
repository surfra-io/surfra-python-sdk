import hashlib
import json
import os
from pathlib import Path
from urllib.parse import unquote

import httpx
import pytest

from surfra import volumes
from surfra.client import SurfraError
from surfra.volumes import Volume


class FakeVolumes:
    """The volumes Worker's file API, in memory."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.uploads: dict[str, dict[int, bytes]] = {}
        self.downloads = 0
        self.part_size = 64 * 1024 * 1024

    def etag(self, data: bytes) -> str:
        return f'"{hashlib.md5(data).hexdigest()}"'

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"].startswith("Bearer ")
        parts = request.url.path.split("/", 4)  # "", "v1", name, kind, rest
        kind = parts[3]
        rest = unquote(parts[4]) if len(parts) > 4 else ""
        if kind == "list":
            prefix = request.url.params.get("prefix", "")
            recursive = request.url.params.get("recursive") == "1"
            files, folders = [], set()
            for path, data in sorted(self.files.items()):
                if not path.startswith(prefix):
                    continue
                tail = path[len(prefix):]
                if not recursive and "/" in tail:
                    folders.add(prefix + tail.split("/")[0] + "/")
                    continue
                files.append({"path": path, "size": len(data), "etag": self.etag(data), "modified": "2026-10-22T00:00:00Z"})
            return httpx.Response(200, json={"files": files, "folders": sorted(folders), "cursor": None})
        if kind == "files":
            if request.method == "PUT":
                assert int(request.headers["Content-Length"]) == len(request.content)
                self.files[rest] = request.content
                return httpx.Response(200, json={"path": rest, "size": len(request.content), "etag": self.etag(request.content)})
            if rest not in self.files:
                return httpx.Response(404, json={"error": "no such file"})
            data = self.files[rest]
            if request.method == "HEAD":
                return httpx.Response(200, headers={"Content-Length": str(len(data)), "ETag": self.etag(data)})
            if request.method == "DELETE":
                del self.files[rest]
                return httpx.Response(200, json={"deleted": rest})
            self.downloads += 1
            return httpx.Response(200, content=data)
        if kind == "delete":
            prefix = json.loads(request.content)["prefix"]
            gone = [path for path in self.files if path.startswith(prefix)]
            for path in gone:
                del self.files[path]
            return httpx.Response(200, json={"deleted": len(gone), "more": False})
        if kind == "uploads":
            if not rest:
                upload = f"up{len(self.uploads)}"
                self.uploads[upload] = {}
                return httpx.Response(200, json={"upload_id": upload, "part_size": self.part_size})
            upload, step = rest.split("/")
            if step == "complete":
                body = json.loads(request.content)
                chunks = self.uploads.pop(upload)
                data = b"".join(chunks[item["part"]] for item in sorted(body["parts"], key=lambda item: item["part"]))
                self.files[body["path"]] = data
                return httpx.Response(200, json={"path": body["path"], "size": len(data), "etag": self.etag(data)})
            self.uploads[upload][int(step)] = request.content
            return httpx.Response(200, json={"part": int(step), "etag": f"p{step}"})
        return httpx.Response(404, json={"error": "unknown route"})


@pytest.fixture
def fake(tmp_path, monkeypatch):
    monkeypatch.setenv("SURFRA_VOLUME_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("SURFRA_API_KEY", "sfk_test")
    monkeypatch.delenv("SURFRA_VOLUMES_TOKEN_FILE", raising=False)
    return FakeVolumes()


def volume(fake: FakeVolumes, name: str = "weights") -> Volume:
    found = Volume(name, volumes_url="https://volumes.test", master="https://api.test")
    found._http_client = httpx.Client(transport=httpx.MockTransport(fake))
    return found


def test_put_directory_and_list(fake, tmp_path):
    source = tmp_path / "minilm"
    (source / "1_Pooling").mkdir(parents=True)
    (source / "config.json").write_text("{}")
    (source / "1_Pooling" / "config.json").write_text("{}")
    weights = volume(fake)
    assert weights.put_directory(source, "minilm/") == 2
    assert sorted(fake.files) == ["minilm/1_Pooling/config.json", "minilm/config.json"]
    top = [entry.path for entry in weights.ls("minilm/")]
    assert top == ["minilm/config.json", "minilm/1_Pooling/"]
    assert len(weights.ls("minilm/", recursive=True)) == 2


def test_local_downloads_once_then_only_what_changed(fake):
    fake.files = {"minilm/a.bin": b"a" * 10, "minilm/b.bin": b"b" * 10, "other/x": b"x"}
    weights = volume(fake)
    path = Path(weights.path("minilm/").local())
    assert (path / "a.bin").read_bytes() == b"a" * 10
    assert not (path.parent / "other").exists()
    assert fake.downloads == 2

    # Same process: no listing, no downloads.
    weights.path("minilm/").local()
    assert fake.downloads == 2

    # A new process with the same cache only fetches changed files, and drops deleted ones.
    fake.files["minilm/a.bin"] = b"A" * 12
    del fake.files["minilm/b.bin"]
    again = volume(fake)
    again.path("minilm/").local()
    assert fake.downloads == 3
    assert (path / "a.bin").read_bytes() == b"A" * 12
    assert not (path / "b.bin").exists()


def test_commit_uploads_changes_and_deletes_removed_files(fake):
    fake.files = {"ckpt/a": b"old", "ckpt/b": b"keep", "ckpt/c": b"gone"}
    ckpts = volume(fake, "ckpts")
    folder = Path(ckpts.path("ckpt/").local(mode="rw"))
    (folder / "a").write_bytes(b"new")
    os.utime(folder / "a", (1, 1))
    (folder / "d").write_bytes(b"added")
    (folder / "c").unlink()
    assert ckpts.commit() == {"uploaded": 2, "deleted": 1}
    assert fake.files == {"ckpt/a": b"new", "ckpt/b": b"keep", "ckpt/d": b"added"}
    assert ckpts.commit() == {"uploaded": 0, "deleted": 0}


def test_a_read_only_sync_never_uploads(fake):
    fake.files = {"m/a": b"1"}
    weights = volume(fake)
    folder = Path(weights.path("m/").local())
    (folder / "a").write_bytes(b"changed locally")
    assert weights.commit() == {"uploaded": 0, "deleted": 0}
    assert fake.files == {"m/a": b"1"}


def test_open_reads_and_writes_single_files(fake):
    results = volume(fake, "results")
    with results.open("runs/1.jsonl", "w") as handle:
        handle.write('{"ok": true}\n')
    assert fake.files["runs/1.jsonl"] == b'{"ok": true}\n'
    assert results.open("runs/1.jsonl").read() == '{"ok": true}\n'
    with pytest.raises(RuntimeError):
        with results.open("runs/2.jsonl", "w") as handle:
            handle.write("partial")
            raise RuntimeError("boom")
    assert "runs/2.jsonl" not in fake.files


def test_large_files_upload_in_parts(fake, tmp_path, monkeypatch):
    monkeypatch.setattr(volumes, "PART_BYTES", 4)
    fake.part_size = 4
    source = tmp_path / "big.bin"
    source.write_bytes(b"0123456789")
    entry = volume(fake).put_file(source, "big.bin")
    assert entry.size == 10 and fake.files["big.bin"] == b"0123456789"


def test_errors_carry_the_services_reason(fake):
    with pytest.raises(SurfraError) as caught:
        volume(fake).open("missing")
    assert caught.value.status == 404 and "no such file" in caught.value.detail


def test_user_code_uses_the_workers_token_file(tmp_path, monkeypatch):
    token_file = tmp_path / "volume-token"
    token_file.write_text("a.b.c")
    monkeypatch.setenv("SURFRA_VOLUMES_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("SURFRA_API_KEY", "sfk_other")
    assert volumes._credential(None) == "a.b.c"
    monkeypatch.delenv("SURFRA_VOLUMES_TOKEN_FILE")
    assert volumes._credential(None) == "sfk_other"


def test_grants_read_like_the_spec():
    assert Volume("weights").mode("r") == "weights:r"
    with pytest.raises(ValueError):
        Volume("weights").mode("x")

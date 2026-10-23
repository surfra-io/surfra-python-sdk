"""Volumes: named storage your apps, jobs and machines share, kept in R2 and served by volumes.surfra.io.

There is no mount. Code names a piece of a volume and asks for it:

    path = surfra.Volume("weights").path("minilm/").local()   # downloads that folder, returns a real path
    with surfra.Volume("results").open("runs/1.jsonl", "w") as f:
        f.write(line)                                          # uploaded when the file closes

.local() downloads only files that changed since the last sync, once per process unless refresh=True.
A folder synced with mode="rw" is uploaded by commit(): new and changed files go up, removed files are deleted.
"""

from __future__ import annotations

import io
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Iterator
from urllib.parse import quote

import httpx

from surfra.client import SurfraError

PART_BYTES = 64 * 1024 * 1024


def _part_bytes() -> int:
    """The largest single upload. SURFRA_VOLUMES_PART_BYTES lowers it, for tests."""

    return int(os.environ.get("SURFRA_VOLUMES_PART_BYTES") or PART_BYTES)
DOWNLOADS = 8
UPLOADS = 4
MODES = ("r", "w", "rw")


def _volumes_url() -> str:
    return (os.environ.get("SURFRA_VOLUMES_URL") or "https://volumes.surfra.io").rstrip("/")


def _cache_root() -> Path:
    return Path(os.environ.get("SURFRA_VOLUME_CACHE") or Path(tempfile.gettempdir()) / "surfra")


def _credential(explicit: str | None) -> str:
    """The credential this process may use. Inside an app or job, the worker writes a volumes-only token to a file."""

    if explicit:
        return explicit
    token_file = os.environ.get("SURFRA_VOLUMES_TOKEN_FILE")
    if token_file and Path(token_file).is_file():
        token = Path(token_file).read_text().strip()
        if token:
            return token
    for name in ("SURFRA_API_KEY", "SURFRA_REPLICA_TOKEN"):
        if os.environ.get(name):
            return os.environ[name]
    try:
        from surfra.host import CONFIG, _read

        saved = _read(CONFIG).get("token")
    except Exception:
        saved = None
    if saved:
        return saved
    raise SurfraError(401, "no credential: set SURFRA_API_KEY or run `surfra login`")


def _saved_login() -> str | None:
    try:
        from surfra.host import CONFIG, _read

        return _read(CONFIG).get("token") or None
    except Exception:
        return None


def _control_credential(explicit: str | None) -> str:
    """Control-plane calls never send the volumes token, which only the volumes service accepts."""

    found = explicit or os.environ.get("SURFRA_API_KEY") or _saved_login()
    if not found:
        raise SurfraError(401, "no credential: set SURFRA_API_KEY or run `surfra login`")
    return found


def _master() -> str:
    try:
        from surfra.host import _master as saved_master

        return saved_master()
    except Exception:
        return (os.environ.get("SURFRA_MASTER") or "https://api.surfra.io").rstrip("/")


def _raise(response: httpx.Response) -> None:
    if response.is_success:
        return
    try:
        body = response.json()
        detail = body.get("error") or body.get("detail")
    except ValueError:
        detail = None
    raise SurfraError(response.status_code, str(detail or response.text or response.reason_phrase))


def _encode(path: str) -> str:
    return "/".join(quote(part, safe="") for part in path.split("/"))


@dataclass(frozen=True)
class Entry:
    """One file in a volume."""

    path: str
    size: int
    etag: str
    modified: str


class VolumePath:
    """A file or folder inside a volume. A trailing slash means a folder."""

    def __init__(self, volume: Volume, path: str):
        self.volume = volume
        self.path = path.lstrip("/")

    def __repr__(self) -> str:
        return f"VolumePath({self.volume.name!r}, {self.path!r})"

    def __str__(self) -> str:
        return f"{self.volume.name}:{self.path}"

    def local(self, mode: str = "r", *, refresh: bool = False) -> str:
        """Download this piece into the local cache and return its real path."""

        return self.volume._sync(self.path, mode, refresh)

    def download(self, dest: str | os.PathLike[str]) -> str:
        """A plain copy into dest, outside the sync cache."""

        return self.volume._copy_out(self.path, Path(dest))


class Volume:
    """A volume by name. No request is made until it is used.

    Credentials, in order: api_key, the token file a Surfra worker provides, SURFRA_API_KEY,
    SURFRA_REPLICA_TOKEN, then the session saved by `surfra login`.
    """

    def __init__(self, name: str, *, api_key: str | None = None, volumes_url: str | None = None, master: str | None = None):
        self.name = name
        self._api_key = api_key
        self._url = (volumes_url or _volumes_url()).rstrip("/")
        self._master = (master or _master()).rstrip("/")
        self._lock = threading.Lock()
        self._synced: dict[str, str] = {}
        self._http_client: httpx.Client | None = None

    def __repr__(self) -> str:
        return f"Volume({self.name!r})"

    # Grants for jobs and apps --------------------------------------------------------------

    def grant(self, mode: str) -> str:
        """A grant such as "weights:r", for job specs and app volumes."""

        if mode not in MODES:
            raise ValueError("mode is r, w or rw")
        return f"{self.name}:{mode}"

    mode = grant

    def path(self, path: str) -> VolumePath:
        return VolumePath(self, path)

    # HTTP ---------------------------------------------------------------------------------

    @property
    def _http(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=httpx.Timeout(60.0, read=300.0), follow_redirects=False)
        return self._http_client

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {_credential(self._api_key)}"}
        if extra:
            headers.update(extra)
        return headers

    def _request(self, method: str, route: str, *, create: bool = False, **kwargs: Any) -> httpx.Response:
        url = f"{self._url}/v1/{self.name}/{route}"
        extra = kwargs.pop("headers", None)
        response = self._http.request(method, url, headers=self._headers(extra), **kwargs)
        if response.status_code == 404 and create and "no volume" in response.text:
            self.create()
            response = self._http.request(method, url, headers=self._headers(extra), **kwargs)
        _raise(response)
        return response

    def _control(self, method: str, route: str, **kwargs: Any) -> Any:
        headers = {"Authorization": f"Bearer {_control_credential(self._api_key)}"}
        response = self._http.request(method, f"{self._master}{route}", headers=headers, **kwargs)
        _raise(response)
        return response.json()

    # The control plane: names, not bytes ------------------------------------------------------

    def create(self) -> dict[str, Any]:
        """Create the volume. Writing to a volume that does not exist yet does this for you."""

        return self._control("POST", "/v1/volumes", json={"name": self.name})

    def info(self) -> dict[str, Any]:
        return self._control("GET", f"/v1/volumes/{quote(self.name)}")

    def delete(self) -> dict[str, Any]:
        """Delete the whole volume and every file in it. Needs an admin."""

        return self._control("DELETE", f"/v1/volumes/{quote(self.name)}")

    @staticmethod
    def list(*, api_key: str | None = None, master: str | None = None) -> list[dict[str, Any]]:
        """Every volume in the organization, with size, file count and what uses it."""

        return Volume("_", api_key=api_key, master=master)._control("GET", "/v1/volumes")["volumes"]

    # Files ----------------------------------------------------------------------------------

    def ls(self, prefix: str = "", *, recursive: bool = False) -> list[Entry]:
        """Files under prefix. Without recursive, folders show as entries ending in / with size 0."""

        out: list[Entry] = []
        for page in self._pages(prefix, recursive):
            out.extend(Entry(item["path"], int(item["size"]), item["etag"], item["modified"]) for item in page["files"])
            if not recursive:
                out.extend(Entry(folder, 0, "", "") for folder in page["folders"])
        return out

    def _pages(self, prefix: str, recursive: bool) -> Iterator[dict[str, Any]]:
        cursor = None
        while True:
            params = {"prefix": prefix, "recursive": "1" if recursive else "0"}
            if cursor:
                params["cursor"] = cursor
            page = self._request("GET", "list", params=params).json()
            yield page
            cursor = page.get("cursor")
            if not cursor:
                return

    def rm(self, path: str, *, recursive: bool = False) -> int:
        """Delete a file, or with recursive=True everything under a folder. Returns how many were deleted."""

        if not recursive:
            self._request("DELETE", f"files/{_encode(path)}")
            return 1
        prefix = path if path.endswith("/") else path + "/"
        total = 0
        while True:
            result = self._request("POST", "delete", json={"prefix": prefix}).json()
            total += int(result["deleted"])
            if not result.get("more"):
                return total

    def put_file(self, local: str | os.PathLike[str], remote: str, *, if_match: str | None = None) -> Entry:
        """Upload one file. Files over 64 MiB go up in parts."""

        source = Path(local)
        size = source.stat().st_size
        if size <= _part_bytes():
            headers = {"Content-Length": str(size)}
            if if_match:
                headers["If-Match"] = if_match
            body = self._request("PUT", f"files/{_encode(remote)}", create=True, content=source.read_bytes(), headers=headers).json()
        else:
            body = self._multipart(source, remote, size)
        return Entry(body["path"], int(body["size"]), body["etag"], "")

    def _multipart(self, source: Path, remote: str, size: int) -> dict[str, Any]:
        started = self._request("POST", "uploads", create=True, json={"path": remote, "size": size}).json()
        upload = quote(started["upload_id"], safe="")
        part_size = int(started["part_size"])
        count = (size + part_size - 1) // part_size

        def send(number: int) -> dict[str, Any]:
            with source.open("rb") as handle:
                handle.seek((number - 1) * part_size)
                chunk = handle.read(part_size)
            return self._request("PUT", f"uploads/{upload}/{number}", params={"path": remote}, content=chunk, headers={"Content-Length": str(len(chunk))}).json()

        try:
            with ThreadPoolExecutor(UPLOADS) as pool:
                parts = list(pool.map(send, range(1, count + 1)))
            return self._request("POST", f"uploads/{upload}/complete", json={"path": remote, "parts": parts}).json()
        except BaseException:
            try:
                self._request("DELETE", f"uploads/{upload}", params={"path": remote})
            except Exception:
                pass
            raise

    def put_directory(self, local: str | os.PathLike[str], remote: str = "") -> int:
        """Upload a folder's files under remote. Returns how many were uploaded."""

        root = Path(local)
        prefix = remote if not remote or remote.endswith("/") else remote + "/"
        files = [item for item in sorted(root.rglob("*")) if item.is_file()]
        with ThreadPoolExecutor(UPLOADS) as pool:
            list(pool.map(lambda item: self.put_file(item, prefix + item.relative_to(root).as_posix()), files))
        return len(files)

    def _get_to(self, remote: str, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".surfra-")
        try:
            with os.fdopen(fd, "wb") as handle, self._http.stream("GET", f"{self._url}/v1/{self.name}/files/{_encode(remote)}", headers=self._headers()) as response:
                if not response.is_success:
                    response.read()
                    _raise(response)
                for chunk in response.iter_bytes(1024 * 1024):
                    handle.write(chunk)
            os.replace(temp, target)
        except BaseException:
            Path(temp).unlink(missing_ok=True)
            raise

    def read(self, path: str) -> bytes:
        """One file's bytes."""

        return self._request("GET", f"files/{_encode(path)}").content

    def write(self, path: str, data: bytes | str, *, if_match: str | None = None) -> Entry:
        """Upload bytes or text as one file. More than the part size goes up in parts."""

        body = data.encode() if isinstance(data, str) else bytes(data)
        if len(body) <= _part_bytes():
            headers = {"Content-Length": str(len(body))}
            if if_match:
                headers["If-Match"] = if_match
            stored = self._request("PUT", f"files/{_encode(path)}", create=True, content=body, headers=headers).json()
            return Entry(stored["path"], int(stored["size"]), stored["etag"], "")
        with tempfile.NamedTemporaryFile(prefix=".surfra-", delete=False) as handle:
            handle.write(body)
        try:
            return self.put_file(handle.name, path)
        finally:
            Path(handle.name).unlink(missing_ok=True)

    def open(self, path: str, mode: str = "r", *, if_match: str | None = None) -> IO[Any]:
        """One file. "r"/"rb" reads it; "w"/"wb" writes to a temporary file and uploads it when closed."""

        if mode in ("r", "rb"):
            body = self._request("GET", f"files/{_encode(path)}").content
            return io.BytesIO(body) if mode == "rb" else io.StringIO(body.decode())
        if mode in ("w", "wb"):
            return _Upload(self, path, binary=mode == "wb", if_match=if_match)
        raise ValueError("mode is r, rb, w or wb")

    # Sync ---------------------------------------------------------------------------------

    def _root(self) -> Path:
        return _cache_root() / f"vol-{self.name}"

    def _state_path(self) -> Path:
        return _cache_root() / ".state" / f"vol-{self.name}.json"

    def _load_state(self) -> dict[str, dict[str, Any]]:
        path = self._state_path()
        return json.loads(path.read_text()) if path.is_file() else {}

    def _save_state(self, state: dict[str, dict[str, Any]]) -> None:
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(state))
        os.replace(temp, path)

    def _sync(self, path: str, mode: str, refresh: bool) -> str:
        if mode not in MODES:
            raise ValueError("mode is r, w or rw")
        root = self._root()
        target = root / path
        with self._lock:
            if path in self._synced and not refresh:
                self._synced[path] = _wider(self._synced[path], mode)
                return str(target)
            if mode == "w":
                # Write-only: nothing to download, only a place to write.
                (target if path.endswith("/") or not path else target.parent).mkdir(parents=True, exist_ok=True)
                self._synced[path] = mode
                return str(target)
            state = self._load_state()
            folder = path.endswith("/") or not path
            remote = {entry.path: entry for entry in (self.ls(path, recursive=True) if folder else self._one(path))}
            changed = [entry for entry in remote.values() if _stale(root / entry.path, state.get(entry.path), entry)]

            def fetch(entry: Entry) -> None:
                self._get_to(entry.path, root / entry.path)

            with ThreadPoolExecutor(DOWNLOADS) as pool:
                list(pool.map(fetch, changed))
            for entry in changed:
                local = root / entry.path
                state[entry.path] = {"etag": entry.etag, "size": entry.size, "mtime_ns": local.stat().st_mtime_ns}
            scope = path if folder else None
            for known in list(state):
                inside = known.startswith(scope) if scope is not None else known == path
                if inside and known not in remote:
                    (root / known).unlink(missing_ok=True)
                    del state[known]
            self._save_state(state)
            if folder:
                target.mkdir(parents=True, exist_ok=True)
            self._synced[path] = mode
            return str(target)

    def _one(self, path: str) -> list[Entry]:
        response = self._http.head(f"{self._url}/v1/{self.name}/files/{_encode(path)}", headers=self._headers())
        if response.status_code == 404:
            return []
        _raise(response)
        return [Entry(path, int(response.headers.get("Content-Length") or 0), response.headers.get("ETag") or "", response.headers.get("Last-Modified") or "")]

    def _copy_out(self, path: str, dest: Path) -> str:
        if path.endswith("/") or not path:
            for entry in self.ls(path, recursive=True):
                self._get_to(entry.path, dest / entry.path[len(path):])
        else:
            self._get_to(path, dest if not dest.is_dir() else dest / Path(path).name)
        return str(dest)

    def commit(self) -> dict[str, int]:
        """Upload what changed in every folder this process synced with mode "w" or "rw"."""

        uploaded = deleted = 0
        with self._lock:
            state = self._load_state()
            root = self._root()
            for path, mode in list(self._synced.items()):
                if mode == "r":
                    continue
                folder = path.endswith("/") or not path
                base = root / path
                found = [item for item in base.rglob("*") if item.is_file() and not item.name.startswith(".surfra-")] if folder else ([base] if base.is_file() else [])
                seen = set()
                for item in found:
                    remote = item.relative_to(root).as_posix()
                    seen.add(remote)
                    stat = item.stat()
                    known = state.get(remote)
                    if known and known["size"] == stat.st_size and known.get("mtime_ns") == stat.st_mtime_ns:
                        continue
                    entry = self.put_file(item, remote)
                    state[remote] = {"etag": entry.etag, "size": entry.size, "mtime_ns": stat.st_mtime_ns}
                    uploaded += 1
                if mode == "rw":
                    for known in [key for key in state if (key.startswith(path) if folder else key == path)]:
                        if known not in seen:
                            try:
                                self.rm(known)
                            except SurfraError as exc:
                                if exc.status != 404:
                                    raise
                            del state[known]
                            deleted += 1
            self._save_state(state)
        return {"uploaded": uploaded, "deleted": deleted}


def _wider(old: str, new: str) -> str:
    return old if old == new else "rw"


def _stale(local: Path, known: dict[str, Any] | None, entry: Entry) -> bool:
    if not known or known.get("etag") != entry.etag or not local.is_file():
        return True
    stat = local.stat()
    return stat.st_size != entry.size or stat.st_mtime_ns != known.get("mtime_ns")


class _Upload(io.RawIOBase):
    """A local temporary file that is uploaded to the volume when it closes."""

    def __init__(self, volume: Volume, path: str, *, binary: bool, if_match: str | None):
        super().__init__()
        self._volume = volume
        self._path = path
        self._if_match = if_match
        self._file = tempfile.NamedTemporaryFile(prefix=".surfra-", delete=False)
        self._text = None if binary else io.TextIOWrapper(self._file, encoding="utf-8", write_through=True)
        self.entry: Entry | None = None

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:  # type: ignore[override]
        if self._text is not None:
            return self._text.write(data)
        return self._file.write(data)

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self._text is not None:
                self._text.flush()
                self._text.detach()
            self._file.close()
            self.entry = self._volume.put_file(self._file.name, self._path, if_match=self._if_match)
        finally:
            Path(self._file.name).unlink(missing_ok=True)
            super().close()

    def __exit__(self, kind, value, traceback) -> None:
        if kind is not None:
            # A failed block uploads nothing.
            Path(self._file.name).unlink(missing_ok=True)
            self._file.close()
            io.RawIOBase.close(self)
            return
        self.close()


def refresh_token_file(master: str, replica_token: str, path: Path) -> bool:
    """Exchange the replica token for a volumes-only token and write it where user code can read it."""

    response = httpx.post(f"{master.rstrip('/')}/v1/tokens", headers={"Authorization": f"Bearer {replica_token}"}, json={"audience": "volumes"}, timeout=30)
    if not response.is_success:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(dir=path.parent, prefix=".token-")
    with os.fdopen(fd, "w") as handle:
        handle.write(response.json()["token"])
    os.chmod(temp, 0o600)
    os.replace(temp, path)
    return True


class VolumeTokens(threading.Thread):
    """Keeps a volumes-only token fresh for user code, which never sees the replica token."""

    def __init__(self, master: str, replica_token: str, path: Path, every: float = 600.0):
        super().__init__(daemon=True, name="surfra-volume-tokens")
        self.master = master
        self.replica_token = replica_token
        self.path = path
        self.every = every
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                ok = refresh_token_file(self.master, self.replica_token, self.path)
            except Exception:
                ok = False
            self.stop.wait(self.every if ok else 30.0)


_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host"}


def _proxy_handler(upstream: str, token_file: Path, client: httpx.Client) -> type[BaseHTTPRequestHandler]:
    """Forwards each request to the volumes service, adding the volumes token unless one is already set."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _forward(self) -> None:
            headers = {key: value for key, value in self.headers.items() if key.lower() not in _HOP}
            if not any(key.lower() == "authorization" for key in headers):
                try:
                    headers["Authorization"] = f"Bearer {token_file.read_text().strip()}"
                except OSError:
                    pass
            length = int(self.headers.get("Content-Length") or 0)

            def body() -> Iterator[bytes]:
                left = length
                while left > 0:
                    chunk = self.rfile.read(min(left, 1024 * 1024))
                    if not chunk:
                        return
                    left -= len(chunk)
                    yield chunk

            try:
                with client.stream(self.command, upstream + self.path, headers=headers, content=body() if length else None) as response:
                    self.send_response(response.status_code)
                    for key, value in response.headers.multi_items():
                        if key.lower() not in _HOP:
                            self.send_header(key, value)
                    if "content-length" not in response.headers:
                        self.send_header("Connection", "close")
                        self.close_connection = True
                    self.end_headers()
                    if self.command != "HEAD":
                        for chunk in response.iter_raw():
                            self.wfile.write(chunk)
            except httpx.HTTPError as exc:
                message = json.dumps({"error": f"the volumes service could not be reached: {exc}"}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(message)))
                self.end_headers()
                self.wfile.write(message)

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = _forward

        def log_message(self, *_args: Any) -> None:
            pass

    return Handler


def start_local_proxy(upstream: str, token_file: Path) -> str | None:
    """A proxy on a loopback port that adds the volumes token, so plain requests from any language work."""

    client = httpx.Client(timeout=httpx.Timeout(60.0, read=300.0))
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _proxy_handler(upstream.rstrip("/"), token_file, client))
    except OSError:
        return None
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True, name="surfra-volumes-proxy").start()
    return f"http://127.0.0.1:{server.server_address[1]}"


def start_token_file(master: str, replica_token: str) -> VolumeTokens | None:
    """Keep a volumes-only token fresh, serve the loopback proxy, and point user code at both.

    SURFRA_VOLUMES_URL is the proxy, so a plain request from any language needs no credential.
    SURFRA_VOLUMES_TOKEN_FILE holds the same token, for clients that talk to the service directly.
    """

    path = Path(tempfile.gettempdir()) / "surfra" / f"volume-token-{os.getpid()}"
    try:
        refresh_token_file(master, replica_token, path)
    except Exception:
        pass
    refresher = VolumeTokens(master, replica_token, path)
    refresher.start()
    upstream = _volumes_url()
    os.environ["SURFRA_VOLUMES_TOKEN_FILE"] = str(path)
    os.environ["SURFRA_VOLUMES_URL"] = start_local_proxy(upstream, path) or upstream
    return refresher

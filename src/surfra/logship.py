"""Send the worker's stdout and stderr to Surfra.

File descriptors 1 and 2 are swapped for pipes, so prints, the logging module,
child processes, and native libraries are all captured. Each line is still
written to the container's own output. Lines are kept in a file on local disk
and sent as a gzip batch every 60 to 90 seconds, or sooner when a batch fills.
Unsent batches wait on disk, up to 5% of it, and the oldest go first.
"""

from __future__ import annotations

import atexit
import gzip
import json
import os
import random
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import httpx

BATCH_BYTES = 8 << 20
LINE_BYTES = 16 << 10
MIN_DISK = 16 << 20
DISK_SHARE = 0.05

_running: dict[str, int] = {}
_running_lock = threading.Lock()
_shipper: "LogShipper | None" = None


def note(message: str) -> None:
    """A Surfra line in the app's logs, apart from what the app prints."""

    if _shipper is not None:
        _shipper.note(message)


def flush(timeout: float = 60.0) -> None:
    """Send what is buffered. Used before the process exits."""

    if _shipper is not None:
        _shipper.flush(timeout)


@contextmanager
def running(request_ids: Iterable[str | None]) -> Iterator[None]:
    """Lines printed inside this block are tagged with the request, when only one is running."""

    ids = [item for item in request_ids if item]
    with _running_lock:
        for item in ids:
            _running[item] = _running.get(item, 0) + 1
    try:
        yield
    finally:
        with _running_lock:
            for item in ids:
                left = _running.get(item, 0) - 1
                if left > 0:
                    _running[item] = left
                else:
                    _running.pop(item, None)


def _request_for(message: str) -> str | None:
    with _running_lock:
        active = list(_running)
    if len(active) == 1:
        return active[0]
    for item in active:
        if item in message:
            return item
    return None


class LogShipper:
    def __init__(self, master: str, token: str, directory: str | None = None, every: float = 60.0, jitter: float = 30.0):
        self.master = master.rstrip("/")
        self.token = token
        self.dir = Path(directory or os.path.join(tempfile.gettempdir(), "surfra-logs"))
        self.every = every
        self.jitter = jitter
        self.lock = threading.Lock()
        self.sending = threading.Lock()
        self.kick = threading.Event()
        self.dropped = 0
        self.lost = 0
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cap = max(MIN_DISK, int(shutil.disk_usage(self.dir).total * DISK_SHARE))
        self.pending = self.dir / "pending.ndjson"
        self.file = open(self.pending, "ab")
        self.size = self.file.tell()
        self.echo: dict[str, int] = {}

    def add(self, stream: str, message: str) -> None:
        message = message.rstrip("\r")
        if not message:
            return
        record = {"t": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "s": stream, "m": message}
        request = _request_for(message)
        if request:
            record["r"] = request
        line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
        with self.lock:
            # A full disk drops the line. The pump must keep draining, or the app blocks on print.
            try:
                self.file.write(line)
                self.file.flush()
            except OSError:
                self.lost += 1
                return
            self.size += len(line)
            if self.size >= BATCH_BYTES:
                try:
                    self._rotate()
                except OSError:
                    pass
                self.kick.set()

    def note(self, message: str) -> None:
        self.add("sys", message)

    def _rotate(self) -> None:
        if self.size == 0:
            return
        self.file.close()
        os.replace(self.pending, self.dir / f"batch-{time.time_ns():020d}.ndjson")
        self.file = open(self.pending, "ab")
        self.size = 0
        self._trim()

    def _batches(self) -> list[Path]:
        return sorted(self.dir.glob("batch-*.ndjson"))

    def _trim(self) -> None:
        batches = self._batches()
        sizes = [path.stat().st_size for path in batches]
        total = sum(sizes)
        for path, size in zip(batches, sizes):
            if total <= self.cap:
                break
            path.unlink(missing_ok=True)
            total -= size
            self.dropped += 1

    def flush(self, timeout: float = 60.0) -> None:
        with self.lock:
            try:
                self._rotate()
            except OSError:
                pass
            dropped, self.dropped = self.dropped, 0
            lost, self.lost = self.lost, 0
        if dropped:
            self.note(f"dropped {dropped} log batches because Surfra was unreachable and the buffer was full")
        if lost:
            self.note(f"dropped {lost} log lines because the disk was full")
        # One sender at a time, or two flushes upload the same batch twice.
        with self.sending:
            for path in self._batches():
                try:
                    raw = path.read_bytes()
                except FileNotFoundError:
                    continue
                if raw.strip():
                    try:
                        response = httpx.post(
                            f"{self.master}/v1/workers/logs",
                            content=gzip.compress(raw),
                            headers={"Content-Type": "application/x-ndjson", "Content-Encoding": "gzip", "X-Surfra-Replica-Token": self.token},
                            timeout=timeout,
                        )
                    except httpx.HTTPError:
                        return
                    if response.status_code >= 500 or response.status_code == 429:
                        return
                path.unlink(missing_ok=True)

    def run(self) -> None:
        while True:
            self.kick.wait(self.every + random.uniform(0, self.jitter))
            self.kick.clear()
            try:
                self.flush()
            except Exception:
                continue

    def capture(self) -> None:
        """Swap fd 1 and 2 for pipes. Each line is echoed to the original output, then kept."""

        for fd, stream in ((1, "out"), (2, "err")):
            saved = os.dup(fd)
            read_end, write_end = os.pipe()
            os.dup2(write_end, fd)
            os.close(write_end)
            self.echo[stream] = saved
            threading.Thread(target=self._pump, args=(read_end, saved, stream), daemon=True).start()

    def _pump(self, read_end: int, echo: int, stream: str) -> None:
        pending = b""
        while True:
            try:
                chunk = os.read(read_end, 65536)
            except OSError:
                return
            if not chunk:
                break
            try:
                os.write(echo, chunk)
            except OSError:
                pass
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                self.add(stream, line.decode("utf-8", "replace"))
            while len(pending) > LINE_BYTES:
                self.add(stream, pending[:LINE_BYTES].decode("utf-8", "replace"))
                pending = pending[LINE_BYTES:]
        if pending:
            self.add(stream, pending.decode("utf-8", "replace"))


def start(master: str, token: str) -> LogShipper | None:
    """Start shipping for this worker. SURFRA_LOGS=0 turns it off."""

    if os.environ.get("SURFRA_LOGS", "").strip() == "0":
        return None
    try:
        shipper = LogShipper(master, token, os.environ.get("SURFRA_LOG_DIR") or None)
        shipper.capture()
    except OSError:
        return None
    threading.Thread(target=shipper.run, name="surfra-logs", daemon=True).start()
    atexit.register(lambda: shipper.flush(timeout=10))
    global _shipper
    _shipper = shipper
    return shipper

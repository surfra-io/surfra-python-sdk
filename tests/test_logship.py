import gzip
import json
import subprocess
import sys
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from surfra.logship import LogShipper, running


class _Sink:
    def __init__(self, status: int = 200):
        self.records: list[dict] = []
        self.tokens: list[str] = []
        self.status = status
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                sink.tokens.append(self.headers.get("X-Surfra-Replica-Token"))
                if sink.status == 200:
                    sink.records += [json.loads(line) for line in gzip.decompress(body).splitlines()]
                self.send_response(sink.status)
                self.end_headers()

            def log_message(self, *args):
                return

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"


def test_lines_carry_time_stream_and_the_running_request(tmp_path):
    sink = _Sink()
    shipper = LogShipper(sink.url, "rtok", str(tmp_path))
    shipper.add("out", "loading")
    with running(["tx_1"]):
        shipper.add("out", "handling")
        with running(["tx_2"]):
            shipper.add("err", "no id")
            shipper.add("err", "failed tx_2")
    shipper.flush()
    assert [row["m"] for row in sink.records] == ["loading", "handling", "no id", "failed tx_2"]
    assert [row.get("r") for row in sink.records] == [None, "tx_1", None, "tx_2"]
    assert sink.records[2]["s"] == "err"
    assert sink.tokens == ["rtok"]
    assert not list(tmp_path.glob("batch-*"))


def test_a_failed_upload_waits_on_disk(tmp_path):
    sink = _Sink(status=502)
    shipper = LogShipper(sink.url, "rtok", str(tmp_path))
    shipper.add("out", "kept")
    shipper.flush()
    assert len(list(tmp_path.glob("batch-*"))) == 1
    sink.status = 200
    shipper.flush()
    assert [row["m"] for row in sink.records] == ["kept"]


def test_prints_and_child_output_are_captured_and_still_echoed(tmp_path):
    sink = _Sink()
    src = Path(__file__).resolve().parents[1] / "src"
    script = textwrap.dedent(f"""
        import os, subprocess, sys
        sys.path.insert(0, {str(src)!r})
        from surfra.logship import LogShipper
        shipper = LogShipper({sink.url!r}, "rtok", {str(tmp_path)!r})
        shipper.capture()
        print("from print", flush=True)
        os.write(2, b"raw stderr\\n")
        subprocess.run([sys.executable, "-c", "print('from child')"])
        import time; time.sleep(0.3)
        shipper.flush()
    """)
    done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30)
    assert "from print" in done.stdout and "from child" in done.stdout
    assert "raw stderr" in done.stderr
    assert {(row["s"], row["m"]) for row in sink.records} == {("out", "from print"), ("err", "raw stderr"), ("out", "from child")}

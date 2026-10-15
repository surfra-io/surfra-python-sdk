import gzip
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from surfra.job import Agent


class FakeSurfra:
    def __init__(self, plan, *, stop="", logs_status=200, beat_status=200, lose_first_start=False, logs_delay=0.0):
        self.plan = plan
        self.stop = stop
        self.logs_status = logs_status
        self.beat_status = beat_status
        self.lose_first_start = lose_first_start
        self.logs_delay = logs_delay
        self.starts: list[dict] = []
        self.finished: list[dict] = []
        self.lines: list[dict] = []
        self.beats = 0
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length") or 0))
                if self.path == "/v1/jobs/agent/start":
                    fake.starts.append(json.loads(body or b"{}"))
                    if fake.lose_first_start and len(fake.starts) == 1:
                        self.close_connection = True
                        return
                    self._json(fake.plan)
                elif self.path == "/v1/jobs/agent/heartbeat":
                    fake.beats += 1
                    if fake.beat_status != 200:
                        self.send_response(fake.beat_status)
                        self.end_headers()
                        return
                    self._json({"stop": fake.stop, "tail": False})
                elif self.path == "/v1/jobs/agent/finish":
                    fake.finished.append(json.loads(body))
                    self._json({"status": "ok"})
                elif self.path == "/v1/workers/logs":
                    time.sleep(fake.logs_delay)
                    if fake.logs_status == 200:
                        fake.lines.extend(json.loads(line) for line in gzip.decompress(body).splitlines() if line.strip())
                    self.send_response(fake.logs_status)
                    self.end_headers()
                else:
                    self.send_response(404)
                    self.end_headers()

            def _json(self, payload):
                raw = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def messages(self, stream):
        return [line["m"] for line in self.lines if line["s"] == stream]


@pytest.fixture()
def surfra():
    made = []

    def make(plan, **kwargs):
        fake = FakeSurfra(plan, **kwargs)
        made.append(fake)
        return fake

    yield make
    for fake in made:
        fake.server.shutdown()


def _plan(command, **extra):
    return {"run": True, "command": command, "grace_sec": 1, "heartbeat_sec": 0.2, "deadline_at": None, **extra}


def test_exit_code_and_output_reach_surfra(surfra, tmp_path):
    fake = surfra(_plan("echo hello; echo oops >&2; exit 3"))
    code = Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run()
    assert code == 3
    assert fake.finished == [{"exit_code": 3, "signal": "", "by": ""}]
    assert "hello" in fake.messages("out") and "oops" in fake.messages("err")
    assert any("command exited with code 3" in message for message in fake.messages("sys"))


def test_the_deadline_stops_the_command_without_surfra(surfra, tmp_path):
    fake = surfra(_plan("sleep 30", deadline_at=time.time() + 1))
    began = time.monotonic()
    code = Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run()
    assert time.monotonic() - began < 10
    assert code == 143 and fake.finished[0]["by"] == "deadline" and fake.finished[0]["signal"] == "SIGTERM"


def test_a_stop_from_surfra_ends_the_command(surfra, tmp_path):
    fake = surfra(_plan("sleep 30"), stop="user")
    code = Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run()
    assert code == 143 and fake.finished[0]["by"] == "control"


def test_sigkill_follows_when_sigterm_is_ignored(surfra, tmp_path):
    fake = surfra(_plan("trap '' TERM; sleep 30 & wait", deadline_at=time.time() + 1))
    began = time.monotonic()
    code = Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run()
    assert time.monotonic() - began < 10
    assert code == 137 and fake.finished[0]["signal"] == "SIGKILL"


def test_a_job_that_already_ran_is_not_run_again(surfra, tmp_path):
    marker = tmp_path / "ran"
    fake = surfra({"run": False})
    assert Agent(fake.url, "sfp_test", log_dir=str(tmp_path / "logs"), heartbeat=0.2).run() == 0
    assert not marker.exists() and fake.finished == []


def test_a_chatty_command_never_waits_on_log_shipping(surfra, tmp_path):
    fake = surfra(_plan("python3 -c \"import sys\nfor i in range(200000): print('line', i)\""), logs_status=500)
    began = time.monotonic()
    code = Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run()
    assert code == 0 and time.monotonic() - began < 30
    assert fake.finished[0]["exit_code"] == 0


def test_a_lost_start_reply_is_retried_with_the_same_attempt(surfra, tmp_path, monkeypatch):
    monkeypatch.setattr("surfra.job.time.sleep", lambda _seconds: None)
    fake = surfra(_plan("exit 0"), lose_first_start=True)
    assert Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run() == 0
    assert len(fake.starts) == 2 and fake.starts[0]["attempt"] == fake.starts[1]["attempt"] != ""


def test_a_refused_heartbeat_does_not_stop_the_command(surfra, tmp_path):
    fake = surfra(_plan("sleep 1.5; exit 0"), beat_status=401)
    assert Agent(fake.url, "sfp_test", log_dir=str(tmp_path), heartbeat=0.2).run() == 0
    assert fake.beats >= 2 and fake.finished == [{"exit_code": 0, "signal": "", "by": ""}]


def test_overlapping_flushes_send_each_line_once(surfra, tmp_path):
    from surfra.logship import LogShipper

    fake = surfra(_plan("true"), logs_delay=0.3)
    shipper = LogShipper(fake.url, "sfp_test", str(tmp_path), every=60, jitter=0)
    for index in range(50):
        shipper.add("out", f"line {index}")
    threads = [threading.Thread(target=shipper.flush) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    sent = fake.messages("out")
    assert sorted(sent) == sorted(f"line {index}" for index in range(50))

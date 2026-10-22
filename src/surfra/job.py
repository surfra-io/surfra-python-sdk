"""The job agent. A job's container command is `surfra job agent`.

The agent asks Surfra for the command, runs it in its own process group, and
reads its stdout and stderr all the time, so the command never waits on Surfra.
Lines go to the container's output and to a capped spool on disk, which is sent
in the background. Heartbeats tell Surfra the job is alive. If Surfra cannot be
reached, the command keeps running and the agent keeps trying. The command ends
on its own, on a stop from Surfra, at its deadline, or when the container is
told to stop. The agent reports the exit code and exits with it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
import uuid
from typing import Any

import httpx

from surfra import logship

FINISH_RETRY_SECONDS = 3600


class Agent:
    def __init__(self, master: str, token: str, *, log_dir: str | None = None, heartbeat: float | None = None):
        self.master = master.rstrip("/")
        self.token = token
        self.volume_tokens = None
        self.http = httpx.Client(timeout=30, headers={"X-Surfra-Replica-Token": token})
        self.heartbeat = heartbeat
        self.grace = 30.0
        self.proc: subprocess.Popen[bytes] | None = None
        self.stopping = threading.Event()
        self.by = ""
        self.unreachable = False
        self.attempt = uuid.uuid4().hex
        self.disowned = False
        try:
            self.shipper: logship.LogShipper | None = logship.LogShipper(master, token, log_dir, every=20.0, jitter=10.0)
        except OSError:
            self.shipper = None
        if self.shipper is not None:
            threading.Thread(target=self.shipper.run, name="surfra-logs", daemon=True).start()

    def say(self, message: str) -> None:
        print(f"[surfra] {message}", flush=True)
        if self.shipper is not None:
            self.shipper.note(message)

    def _post(self, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
        return self.http.post(f"{self.master}{path}", json=body or {})

    def _plan(self) -> dict[str, Any]:
        delay = 2.0
        tries = 0
        while True:
            problem = ""
            try:
                response = self._post("/v1/jobs/agent/start", {"attempt": self.attempt})
                if response.status_code == 401:
                    return {"run": False}
                if response.is_success:
                    return response.json()
                problem = f"HTTP {response.status_code}"
            except httpx.HTTPError as error:
                problem = str(error) or type(error).__name__
            tries += 1
            if tries in (1, 5) or tries % 20 == 0:
                self.say(f"can't reach Surfra at {self.master} yet ({problem}). Retrying.")
            time.sleep(delay)
            delay = min(delay * 2, 30.0)

    def run(self) -> int:
        self.say("job agent starting")
        plan = self._plan()
        if not plan.get("run"):
            self.say("this job already ran or was stopped, so the command is not run again")
            self._flush()
            return 0
        command = str(plan["command"])
        self.grace = float(plan.get("grace_sec") or 30)
        every = self.heartbeat or float(plan.get("heartbeat_sec") or 15)
        deadline = plan.get("deadline_at")
        env = dict(os.environ)
        env.pop("SURFRA_REPLICA_TOKEN", None)
        if self.volume_tokens is None:
            from surfra.volumes import start_token_file

            self.volume_tokens = start_token_file(self.master, self.token)
        env["SURFRA_VOLUMES_TOKEN_FILE"] = os.environ.get("SURFRA_VOLUMES_TOKEN_FILE", "")
        env["SURFRA_VOLUMES_URL"] = os.environ.get("SURFRA_VOLUMES_URL", "")
        shell = "/bin/bash" if os.path.exists("/bin/bash") else "/bin/sh"
        self.say(f"running: {command}")
        began = time.monotonic()
        self.proc = subprocess.Popen(
            [shell, "-c", command],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
        assert self.proc.stdout is not None and self.proc.stderr is not None
        pumps = [
            threading.Thread(target=self._drain, args=(self.proc.stdout.fileno(), 1, "out"), daemon=True),
            threading.Thread(target=self._drain, args=(self.proc.stderr.fileno(), 2, "err"), daemon=True),
        ]
        for pump in pumps:
            pump.start()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: self.terminate("container"))
        threading.Thread(target=self._beat, args=(every,), name="surfra-heartbeat", daemon=True).start()
        while True:
            try:
                self.proc.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if deadline and time.time() > float(deadline):
                    self.terminate("deadline")
        for pump in pumps:
            pump.join(timeout=10)
        code = self.proc.returncode
        signal_name = ""
        if code < 0:
            try:
                signal_name = signal.Signals(-code).name
            except ValueError:
                signal_name = f"signal {-code}"
            code = 128 - code
        self.say(f"command exited with code {code}" + (f" ({signal_name})" if signal_name else "") + f" after {time.monotonic() - began:.1f}s")
        self._flush()
        self._finish(code, signal_name)
        return code

    def _drain(self, fd: int, echo: int, stream: str) -> None:
        if self.shipper is not None:
            self.shipper._pump(fd, echo, stream)
            return
        while True:
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            try:
                os.write(echo, chunk)
            except OSError:
                pass

    def terminate(self, by: str) -> None:
        """SIGTERM the command's process group, then SIGKILL after the grace period."""

        if self.stopping.is_set() or self.proc is None or self.proc.poll() is not None:
            return
        self.stopping.set()
        self.by = by
        why = {"deadline": "it reached its deadline", "control": "Surfra asked it to stop", "container": "the container is stopping"}.get(by, by)
        self.say(f"stopping the command because {why}: SIGTERM now, SIGKILL in {self.grace:.0f}s")
        self._signal(signal.SIGTERM)
        timer = threading.Timer(self.grace, self._signal, args=(signal.SIGKILL,))
        timer.daemon = True
        timer.start()

    def _signal(self, signum: int) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.killpg(self.proc.pid, signum)
        except (ProcessLookupError, PermissionError):
            pass

    def _beat(self, every: float) -> None:
        while self.proc is not None and self.proc.poll() is None:
            try:
                response = self._post("/v1/jobs/agent/heartbeat")
            except httpx.HTTPError as exc:
                if not self.unreachable:
                    self.unreachable = True
                    self.say(f"cannot reach Surfra ({exc.__class__.__name__}). The command keeps running.")
                time.sleep(every)
                continue
            if self.unreachable:
                self.unreachable = False
                self.say("reached Surfra again")
            if response.status_code == 401:
                # Only a stop or the liveness timeout ends a job, never a refused heartbeat.
                if not self.disowned:
                    self.disowned = True
                    self.say("Surfra did not accept a heartbeat. The command keeps running.")
            elif response.is_success:
                self.disowned = False
                answer = response.json()
                stop = answer.get("stop") or ""
                if stop == "deadline":
                    self.terminate("deadline")
                elif stop:
                    self.terminate("control")
                if self.shipper is not None:
                    fast = bool(answer.get("tail"))
                    if fast and self.shipper.every > 2:
                        self.shipper.kick.set()
                    self.shipper.every, self.shipper.jitter = (2.0, 1.0) if fast else (20.0, 10.0)
            time.sleep(every)

    def _flush(self) -> None:
        if self.shipper is not None:
            try:
                self.shipper.flush(timeout=30)
            except Exception:
                pass

    def _finish(self, code: int, signal_name: str) -> None:
        body = {"exit_code": code, "signal": signal_name, "by": self.by}
        give_up = time.monotonic() + FINISH_RETRY_SECONDS
        delay = 2.0
        while time.monotonic() < give_up:
            try:
                response = self._post("/v1/jobs/agent/finish", body)
                if response.is_success or response.status_code in (400, 401, 404):
                    return
            except httpx.HTTPError:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        print("[surfra] could not report the exit code to Surfra", flush=True)


def agent_main() -> int:
    missing = [name for name in ("SURFRA_MASTER", "SURFRA_REPLICA_TOKEN") if not os.environ.get(name)]
    if missing:
        print(f"surfra job agent runs inside a job's container. Missing {', '.join(missing)}.", flush=True)
        return 2
    heartbeat = float(os.environ["SURFRA_JOB_HEARTBEAT"]) if os.environ.get("SURFRA_JOB_HEARTBEAT") else None
    agent = Agent(os.environ["SURFRA_MASTER"], os.environ["SURFRA_REPLICA_TOKEN"], log_dir=os.environ.get("SURFRA_LOG_DIR") or None, heartbeat=heartbeat)
    return agent.run()

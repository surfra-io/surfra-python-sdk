"""The process that runs on a person's own machine."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from surfra.box import open_box
from surfra.cli import open_page

HOME = Path.home() / ".surfra"
CONFIG = HOME / "config.json"
RUNNER = HOME / "runner.json"


def _master() -> str:
    saved = _read(CONFIG)
    return (os.environ.get("SURFRA_MASTER") or saved.get("master") or "https://api.surfra.io").rstrip("/")


def _read(path: Path) -> dict:
    if not path.is_file():
        return {}
    return json.loads(path.read_text())


def _write(path: Path, body: dict) -> None:
    HOME.mkdir(mode=0o700, exist_ok=True)
    path.write_text(json.dumps(body, indent=2) + "\n")
    path.chmod(0o600)


def _session() -> str:
    token = os.environ.get("SURFRA_API_KEY") or _read(CONFIG).get("token") or ""
    if not token:
        raise SystemExit("run `surfra login` on this machine first")
    return token


def login() -> int:
    master = _master()
    with httpx.Client(timeout=30) as http:
        started = http.post(f"{master}/v1/cli/device").json()
        print(f"Approve this machine at {started['verification_url']}")
        open_page(started["verification_url"])
        deadline = time.time() + 600
        while time.time() < deadline:
            time.sleep(float(started.get("interval") or 2))
            response = http.post(f"{master}/v1/cli/device/token", json={"device_code": started["device_code"]})
            if response.status_code == 404:
                print("that login expired. Run surfra login again.", file=sys.stderr)
                return 1
            body = response.json()
            if body.get("status") == "approved" and body.get("token"):
                _write(CONFIG, {"token": body["token"], "master": master})
                print("logged in")
                return 0
    print("login timed out", file=sys.stderr)
    return 1


def probe() -> dict:
    ram_gb = 1
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                ram_gb = max(1, int(line.split()[1]) // 1024 // 1024)
                break
    gpu = ""
    count = 0
    if shutil.which("nvidia-smi"):
        try:
            output = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            names = [line.strip() for line in output.splitlines() if line.strip()]
            count = len(names)
            gpu = names[0] if names else ""
        except (subprocess.CalledProcessError, FileNotFoundError):
            gpu = ""
            count = 0
    return {"vcpu": os.cpu_count() or 1, "ram_gb": ram_gb, "gpu": gpu, "gpu_count": count}


def _listen_for_callback():
    from http.server import BaseHTTPRequestHandler, HTTPServer

    finished = threading.Event()

    class Callback(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.startswith("/callback"):
                finished.set()
                body = b"<p>This machine is on your Surfra account. You can close this tab.</p>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, _format: str, *_args) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Callback)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, finished


def register() -> int:
    master = _master()
    token = _session()
    hardware = probe()
    private = X25519PrivateKey.generate()
    public = private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    import base64

    public_b64 = base64.b64encode(public).decode()
    private_b64 = base64.b64encode(
        private.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    ).decode()
    existing = _read(RUNNER)
    server, finished = _listen_for_callback()
    callback = f"http://127.0.0.1:{server.server_address[1]}/callback"
    try:
        with httpx.Client(timeout=30, headers={"Authorization": f"Bearer {token}"}) as http:
            started = http.post(
                f"{master}/v1/runners/pending",
                json={
                    **hardware,
                    "public_key": public_b64,
                    "existing_runner_id": existing.get("id") or "",
                    "callback": callback,
                },
            )
            started.raise_for_status()
            body = started.json()
            print("Adding this machine in the dashboard.")
            print(f"Guessed {hardware['vcpu']} vCPU, {hardware['ram_gb']} GB RAM, {hardware['gpu'] or 'no GPU'}.")
            print(body["url"])
            open_page(body["url"])
            deadline = time.time() + 600
            while time.time() < deadline:
                finished.wait(2)
                pending = http.get(f"{master}/v1/runners/pending/{body['pending_id']}")
                pending.raise_for_status()
                viewed = pending.json()
                if viewed.get("token") and viewed.get("runner_id"):
                    _write(
                        RUNNER,
                        {
                            "id": viewed["runner_id"],
                            "token": viewed["token"],
                            "private_key": private_b64,
                            "master": master,
                            "gpu_count": hardware["gpu_count"],
                        },
                    )
                    print(f"runner {viewed['runner_id']}")
                    print("Start it with: surfra runner start")
                    return 0
    finally:
        server.shutdown()
    print("registration timed out", file=sys.stderr)
    return 1


def whoami() -> int:
    saved = _read(RUNNER)
    token = saved.get("token")
    if not token:
        print("this machine is not registered. Run surfra runner register.", file=sys.stderr)
        return 1
    master = saved.get("master") or _master()
    response = httpx.get(f"{master}/v1/runners/me", headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if response.status_code == 401:
        print("this runner token is no longer valid. Run surfra runner register.", file=sys.stderr)
        return 1
    response.raise_for_status()
    body = response.json()
    state = body.get("busy_app") or "idle"
    online = "online" if body.get("online") else "offline"
    gpu = body.get("gpu") or "cpu"
    print(f"id        {body.get('id')}")
    print(f"org       {body.get('org_id')}")
    print(f"cpu       {body.get('vcpu')}")
    print(f"ram_gb    {body.get('ram_gb')}")
    print(f"gpu       {gpu} x{body.get('gpu_count') or 0}")
    print(f"status    {online}, {state}")
    return 0


def _missing_runtime(gpu: bool) -> str | None:
    script = _script("install-gpu.sh" if gpu else "install-cpu.sh")
    if shutil.which("docker") is None:
        return f"Docker is not installed. Run:\n  bash {script}"
    if gpu and not Path("/usr/bin/nvidia-container-runtime").exists() and shutil.which("nvidia-ctk") is None:
        return f"This machine has a GPU, and Docker cannot see it yet. Run:\n  bash {script}"
    return None


def start() -> int:
    saved = _read(RUNNER)
    token = saved.get("token")
    if not token:
        print("this machine is not registered. Run surfra runner register.", file=sys.stderr)
        return 1
    problem = _missing_runtime(int(saved.get("gpu_count") or 0) > 0)
    if problem:
        print(problem, file=sys.stderr)
        return 1
    master = (saved.get("master") or _master()).rstrip("/")
    headers = {"Authorization": f"Bearer {token}"}
    print(f"runner {saved.get('id')} waiting for an app")
    with httpx.Client(timeout=30, headers=headers) as http:
        while True:
            response = http.post(f"{master}/v1/runners/heartbeat")
            if response.status_code == 401:
                print("this runner was registered to another account. Run surfra runner register.", file=sys.stderr)
                return 1
            response.raise_for_status()
            assignment = (response.json() or {}).get("assignment")
            if not assignment:
                time.sleep(5)
                continue
            _run_assignment(http, master, saved, assignment)


def _run_assignment(http: httpx.Client, master: str, saved: dict, assignment: dict) -> None:
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(20):
            http.post(f"{master}/v1/runners/heartbeat")

    thread = threading.Thread(target=beat, daemon=True)
    thread.start()
    try:
        _docker(saved, assignment)
    finally:
        stop.set()
        http.post(f"{master}/v1/runners/release", json={"replica_id": assignment.get("replica_id")})


def _docker(saved: dict, assignment: dict) -> None:
    password = ""
    if assignment.get("docker_password_box"):
        password = open_box(saved["private_key"], assignment["docker_password_box"])
    server = str(assignment.get("docker_server") or "").removeprefix("https://").removeprefix("http://").strip("/")
    if password and assignment.get("docker_user"):
        login = ["docker", "login", "-u", assignment["docker_user"], "--password-stdin"]
        if server:
            login.append(server)
        subprocess.run(login, input=password, text=True, check=True)
    image = assignment["image"]
    subprocess.run(["docker", "pull", image], check=True)
    command = ["docker", "run", "--rm", "--name", f"surfra-{assignment['replica_id']}"]
    if assignment.get("gpus"):
        command.extend(["--gpus", "all"])
    for key, value in (assignment.get("env") or {}).items():
        command.extend(["-e", f"{key}={value}"])
    command.append(image)
    command.extend(["surfra", "worker"])
    print(f"running {assignment.get('app_name')} from {image}")
    subprocess.run(command, check=False)

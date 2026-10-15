"""The process that runs on a person's own machine."""

from __future__ import annotations

import json
import os
import shutil
import signal
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
PID = HOME / "runner.pid"


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


def _ram_bytes() -> int:
    if sys.platform == "win32":
        import ctypes

        class Status(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong), ("total", ctypes.c_ulonglong), ("free", ctypes.c_ulonglong)] + [(f"pad{index}", ctypes.c_ulonglong) for index in range(5)]

        status = Status()
        status.length = ctypes.sizeof(Status)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return int(status.total)
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        return 0


def probe() -> dict:
    ram_gb = max(1, round(_ram_bytes() / 2**30))
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


def _save_runner(viewed: dict, private_b64: str, master: str, hardware: dict) -> None:
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


def register(name: str = "") -> int:
    """Add this machine to the account. With a name it finishes here; without one a dashboard page confirms it."""

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
    server, finished = (None, None) if name else _listen_for_callback()
    callback = f"http://127.0.0.1:{server.server_address[1]}/callback" if server else ""
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
            print(f"Found {hardware['vcpu']} vCPU, {hardware['ram_gb']} GB RAM, {hardware['gpu'] or 'no GPU'}.")
            if name:
                confirmed = http.post(f"{master}/v1/runners", json={"pending_id": body["pending_id"], "name": name, **hardware})
                if confirmed.status_code >= 400:
                    print(f"registration failed: {confirmed.text[:300]}", file=sys.stderr)
                    return 1
                pending = http.get(f"{master}/v1/runners/pending/{body['pending_id']}")
                pending.raise_for_status()
                _save_runner(pending.json(), private_b64, master, hardware)
                return 0
            print("Name this machine in the dashboard:")
            print(body["url"])
            open_page(body["url"])
            deadline = time.time() + 600
            while time.time() < deadline:
                finished.wait(2)
                pending = http.get(f"{master}/v1/runners/pending/{body['pending_id']}")
                pending.raise_for_status()
                viewed = pending.json()
                if viewed.get("token") and viewed.get("runner_id"):
                    _save_runner(viewed, private_b64, master, hardware)
                    return 0
    finally:
        if server:
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
    command = f"surfra runner install {'gpu' if gpu else 'cpu'}"
    if shutil.which("docker") is None:
        return f"Docker is not installed. Run:\n  {command}"
    # Docker Desktop on macOS and Windows brings its own GPU support, so only Linux needs the toolkit.
    if gpu and sys.platform == "linux" and not Path("/usr/bin/nvidia-container-runtime").exists() and shutil.which("nvidia-ctk") is None:
        return f"This machine has a GPU, and Docker cannot see it yet. Run:\n  {command}"
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
    PID.write_text(str(os.getpid()))
    signal.signal(signal.SIGTERM, _exit_on_term)
    print(f"runner {saved.get('id')} waiting for an app")
    with httpx.Client(timeout=30, headers=headers) as http:
        while True:
            try:
                response = http.post(f"{master}/v1/runners/heartbeat")
            except httpx.HTTPError as exc:
                print(f"cannot reach {master}: {exc}. Retrying.", file=sys.stderr)
                time.sleep(10)
                continue
            if response.status_code == 401:
                print("this runner was registered to another account. Run surfra runner register.", file=sys.stderr)
                return 1
            if response.status_code >= 500:
                time.sleep(10)
                continue
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
            try:
                http.post(f"{master}/v1/runners/heartbeat")
            except httpx.HTTPError:
                continue

    thread = threading.Thread(target=beat, daemon=True)
    thread.start()
    try:
        _docker(saved, assignment)
    finally:
        stop.set()
        for _attempt in range(30):
            try:
                http.post(f"{master}/v1/runners/release", json={"replica_id": assignment.get("replica_id")})
                break
            except httpx.HTTPError:
                time.sleep(10)


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
    name = f"surfra-{assignment['replica_id']}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
    command = ["docker", "run", "--rm", "--init", "--name", name]
    if assignment.get("gpus"):
        command.extend(["--gpus", "all"])
    for key, value in (assignment.get("env") or {}).items():
        command.extend(["-e", f"{key}={value}"])
    command.append(image)
    command.extend(["surfra", "worker"])
    print(f"running {assignment.get('app_name')} from {image}")
    try:
        subprocess.run(command, check=False)
    finally:
        subprocess.run(["docker", "stop", "-t", "10", name], capture_output=True, check=False)


def _exit_on_term(_signum: int, _frame: object) -> None:
    raise SystemExit(0)


def _service_active() -> bool:
    if sys.platform != "linux" or shutil.which("systemctl") is None:
        return False
    return subprocess.run(["systemctl", "--user", "is-active", "--quiet", "surfra-runner"], check=False).returncode == 0


def stop() -> int:
    """Take this machine out of rotation: stop the runner and its app, and mark it offline."""

    saved = _read(RUNNER)
    token = saved.get("token")
    if not token:
        print("this machine is not registered. Run surfra runner register.", file=sys.stderr)
        return 1
    master = (saved.get("master") or _master()).rstrip("/")
    told = _go_offline(master, token)
    service_on = _service_active()
    if service_on:
        subprocess.run(["systemctl", "--user", "stop", "surfra-runner"], check=False)
    elif PID.exists():
        try:
            os.kill(int(PID.read_text().strip()), signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    PID.unlink(missing_ok=True)
    if shutil.which("docker"):
        running = subprocess.run(["docker", "ps", "-q", "--filter", "name=surfra-rep_"], capture_output=True, text=True, check=False).stdout.split()
        if running:
            print(f"stopping {len(running)} app container{'s' if len(running) > 1 else ''}")
            subprocess.run(["docker", "stop", *running], capture_output=True, check=False)
    # A heartbeat sent while the runner was shutting down can mark it online again.
    if not _go_offline(master, token) and not told:
        print(f"stopped here, but could not tell {master}. It shows offline within a minute.", file=sys.stderr)
    print("runner stopped. It takes no work until you start it again:")
    print("  systemctl --user start surfra-runner" if service_on or UNIT.exists() else "  surfra runner start")
    return 0


def _go_offline(master: str, token: str) -> bool:
    try:
        response = httpx.post(f"{master}/v1/runners/offline", headers={"Authorization": f"Bearer {token}"}, timeout=30)
        response.raise_for_status()
    except httpx.HTTPError:
        return False
    return True


UNIT = Path.home() / ".config" / "systemd" / "user" / "surfra-runner.service"


def service(action: str) -> int:
    """Keep `surfra runner start` running with a systemd user service."""

    if sys.platform != "linux" or shutil.which("systemctl") is None:
        print("The runner service uses systemd. On this system, keep `surfra runner start` running in a terminal.", file=sys.stderr)
        return 1
    if action == "remove":
        subprocess.run(["systemctl", "--user", "disable", "--now", "surfra-runner"], check=False)
        UNIT.unlink(missing_ok=True)
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
        print("runner service removed")
        return 0
    if not _read(RUNNER).get("token"):
        print("this machine is not registered. Run surfra runner register.", file=sys.stderr)
        return 1
    UNIT.parent.mkdir(parents=True, exist_ok=True)
    UNIT.write_text(
        "[Unit]\n"
        "Description=Surfra runner\n"
        "After=network-online.target\n\n"
        "[Service]\n"
        f"ExecStart={sys.executable} -m surfra runner start\n"
        "Restart=always\n"
        "RestartSec=10\n\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", "--now", "surfra-runner"], check=True)
    print("runner service started. Logs: journalctl --user -u surfra-runner -f")
    print(f"It stops when you log out unless lingering is on: sudo loginctl enable-linger {os.environ.get('USER') or '$USER'}")
    return 0

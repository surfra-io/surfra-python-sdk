"""`surfra` is the command users run. The container command is `surfra worker`."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import webbrowser
from pathlib import Path

from surfra.worker import main as worker_main


def _script(name: str) -> Path:
    return Path(__file__).resolve().parent / "scripts" / name


def _install(kind: str) -> int:
    if sys.platform != "linux":
        print("Install Docker Desktop from https://www.docker.com/products/docker-desktop/ and start it, then run: surfra runner start")
        if kind == "gpu" and sys.platform == "win32":
            print("For an NVIDIA GPU, turn on the WSL 2 backend in Docker Desktop: https://docs.docker.com/desktop/features/gpu/")
        elif kind == "gpu":
            print("Docker on macOS has no GPU access, so a Mac runs CPU apps only.")
        return 0
    script = _script("install-gpu.sh" if kind == "gpu" else "install-cpu.sh")
    if not script.is_file():
        print(f"missing install script {script}", file=sys.stderr)
        return 1
    completed = subprocess.run(["bash", str(script)])
    return completed.returncode


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="surfra")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("worker", help="Run inside a container. The autoscaler starts this.")
    sub.add_parser("login", help="Open a browser page and save a session on this machine.")

    runner = sub.add_parser("runner", help="Attach this computer so your apps can run here.")
    runner_sub = runner.add_subparsers(dest="runner_command", required=True)
    register = runner_sub.add_parser("register", help="Guess this machine's hardware and open a page to name it.")
    register.add_argument("--name", default="", help="Name the machine here instead of in the dashboard, such as desk-4090.")
    runner_sub.add_parser("start", help="Heartbeat and run one app at a time.")
    runner_stop = runner_sub.add_parser("stop", help="Stop the runner and its app, and mark this machine offline.")
    runner_stop.add_argument("--force", action="store_true", help="Stop even if a job is running here. The job ends as stopped.")
    service = runner_sub.add_parser("service", help="Keep the runner running in the background with systemd (Linux).")
    service.add_argument("action", choices=("install", "remove"))
    runner_sub.add_parser("whoami", help="Print the runner id, owner, and whether it is busy.")
    install = runner_sub.add_parser("install", help="Install local runner dependencies.")
    install.add_argument(
        "kind",
        choices=("cpu", "gpu"),
        help="cpu installs Surfra's local runner dependencies for CPU support. gpu installs them for GPU support.",
    )

    from surfra.jobcli import build as build_jobs
    from surfra.volumecli import build as build_volumes

    build_jobs(sub)
    build_volumes(sub)

    args = parser.parse_args(argv)
    if args.command == "worker":
        worker_main()
        return
    if args.command == "job" and args.job_command == "agent":
        from surfra.jobcli import run as run_job

        raise SystemExit(run_job(args))
    from surfra.update import notice

    message = notice()
    if message:
        print(message + "\n", file=sys.stderr)
    if args.command == "login":
        from surfra.host import login

        raise SystemExit(login())
    if args.command == "job":
        from surfra.jobcli import run as run_job

        raise SystemExit(run_job(args))
    if args.command == "volume":
        from surfra.volumecli import run as run_volume

        raise SystemExit(run_volume(args))
    if args.command == "runner" and args.runner_command == "install":
        raise SystemExit(_install(args.kind))
    from surfra.host import register, service, start, stop, whoami

    if args.runner_command == "register":
        raise SystemExit(register(args.name))
    if args.runner_command == "service":
        raise SystemExit(service(args.action))
    if args.runner_command == "stop":
        raise SystemExit(stop(args.force))
    commands = {"start": start, "stop": stop, "whoami": whoami}
    raise SystemExit(commands[args.runner_command]())


def open_page(url: str) -> None:
    if os.environ.get("SURFRA_NO_BROWSER"):
        print(url)
        return
    opened = webbrowser.open(url)
    if not opened:
        print(url)


if __name__ == "__main__":
    main()

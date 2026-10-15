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
    runner_sub.add_parser("register", help="Guess this machine's hardware and open a page to name it.")
    runner_sub.add_parser("start", help="Heartbeat and run one app at a time.")
    runner_sub.add_parser("whoami", help="Print the runner id, owner, and whether it is busy.")
    install = runner_sub.add_parser("install", help="Install local runner dependencies.")
    install.add_argument(
        "kind",
        choices=("cpu", "gpu"),
        help="cpu installs Surfra's local runner dependencies for CPU support. gpu installs them for GPU support.",
    )

    args = parser.parse_args(argv)
    if args.command == "worker":
        worker_main()
        return
    if args.command == "login":
        from surfra.host import login

        raise SystemExit(login())
    if args.command == "runner" and args.runner_command == "install":
        raise SystemExit(_install(args.kind))
    from surfra.host import register, start, whoami

    commands = {"register": register, "start": start, "whoami": whoami}
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

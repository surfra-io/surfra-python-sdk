"""`surfra job ...`: submit a command, follow its logs, list, stop, and save job specs."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from surfra.client import JOB_DONE, Job, SurfraClient, SurfraError

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def duration(text: str) -> int:
    """90, 90s, 10m, 3h, or 2d, in seconds."""

    match = re.fullmatch(r"\s*(\d+)\s*([smhd]?)\s*", text or "")
    if not match:
        raise argparse.ArgumentTypeError(f"{text!r} is not a duration such as 90s, 10m, 3h, or 2d")
    return int(match.group(1)) * UNITS[match.group(2) or "s"]


def _env_pairs(pairs: list[str], files: list[str]) -> dict[str, str]:
    env: dict[str, str] = {}
    for name in files:
        for line in Path(name).read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.removeprefix("export ").split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            env[key.strip()] = value
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--env takes NAME=VALUE, got {pair!r}")
        key, value = pair.split("=", 1)
        env[key.strip()] = value
    return env


def add_fields(parser: argparse.ArgumentParser) -> None:
    """Flags shared by submit and spec save."""

    parser.add_argument("--image", help="Container image with surfra installed, such as ghcr.io/me/train:3.")
    parser.add_argument("--runner", action="append", default=[], help="Run on your machine with this name. Repeat to try several in order.")
    parser.add_argument("--provider", help="Run on a cloud, such as runpod or vast.")
    parser.add_argument("--gpu", help="GPU for --provider, such as rtx4090 or a100.")
    parser.add_argument("--gpu-count", type=int, help="GPUs per machine for --provider.")
    parser.add_argument("--spot", action="store_true", help="Use a spot machine for --provider. The provider can take it back, and jobs are not retried.")
    parser.add_argument("--providers-json", help="Machine rows as JSON, for anything the flags above do not cover.")
    parser.add_argument("--env", action="append", default=[], metavar="NAME=VALUE", help="Set an environment variable. Repeatable.")
    parser.add_argument("--env-file", action="append", default=[], help="Read NAME=VALUE lines from a file. Repeatable.")
    parser.add_argument("--secret", help="A Surfra credential (Doppler or generic) whose values become environment variables.")
    parser.add_argument("--docker-secret", help="A Surfra Docker credential for pulling a private image.")
    parser.add_argument("--deadline", type=duration, help="Stop the job if it runs longer than this, such as 12h.")
    parser.add_argument("--liveness", type=duration, help="Release the machine after this long with no heartbeat. Default 10m.")
    parser.add_argument("--grace", type=duration, help="Time between SIGTERM and SIGKILL when the job is stopped. Default 30s.")
    parser.add_argument("--call-app", action="append", default=[], help="An app the job may call. Use '*' for every app. Repeatable.")
    parser.add_argument("--deny-app", action="append", default=[], help="An app the job may never call. Repeatable.")


def _fields(args: argparse.Namespace) -> dict[str, Any]:
    rows: list[dict[str, Any]] = [{"provider": "runner", "runner": name, "priority": index + 1} for index, name in enumerate(args.runner)]
    if args.provider:
        row: dict[str, Any] = {"provider": args.provider, "priority": len(rows) + 1}
        if args.gpu:
            row["gpu"] = args.gpu
        if args.gpu_count:
            row["gpu_count"] = args.gpu_count
        if args.spot:
            row["spot"] = True
        rows.append(row)
    if args.providers_json:
        rows.extend(json.loads(args.providers_json))
    fields: dict[str, Any] = {
        "image": args.image,
        "providers": rows or None,
        "env": _env_pairs(args.env, args.env_file) or None,
        "secret": args.secret,
        "docker_secret": args.docker_secret,
        "deadline_sec": args.deadline,
        "liveness_sec": args.liveness,
        "grace_sec": args.grace,
        "call_apps": args.call_app or None,
        "deny_apps": args.deny_app or None,
    }
    return {key: value for key, value in fields.items() if value is not None}


def build(sub: argparse._SubParsersAction) -> None:
    job = sub.add_parser("job", help="Run a command once on one machine, like a training run or a batch job.")
    job_sub = job.add_subparsers(dest="job_command", required=True)

    submit = job_sub.add_parser(
        "submit",
        help="Run a command. Everything after -- is the command.",
        description="Examples:\n  surfra job submit --image ghcr.io/me/train:3 --runner desk-4090 -- bash train.sh --epochs 10\n  surfra job submit --spec train-llm -- --epochs 10",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    submit.add_argument("--spec", help="A saved job spec. With --spec, the words after -- are extra arguments.")
    submit.add_argument("--name", help="A name to find the job by.")
    submit.add_argument("--submission-id", help="Submitting again with the same id returns the same job.")
    submit.add_argument("--no-wait", action="store_true", help="Print the job id and return instead of following the logs.")
    add_fields(submit)
    submit.add_argument("words", nargs=argparse.REMAINDER, metavar="-- COMMAND")

    logs = job_sub.add_parser("logs", help="Print a job's logs.")
    logs.add_argument("job_id")
    logs.add_argument("-f", "--follow", action="store_true", help="Keep printing until the job ends.")

    status = job_sub.add_parser("status", help="Show a job.")
    status.add_argument("job_id")
    status.add_argument("--json", action="store_true")

    listing = job_sub.add_parser("list", help="Recent jobs.")
    listing.add_argument("--status", default="")
    listing.add_argument("--limit", type=int, default=20)

    stop = job_sub.add_parser("stop", help="Stop a job: SIGTERM, then SIGKILL after its grace period.")
    stop.add_argument("job_id")

    wait = job_sub.add_parser("wait", help="Wait for a job to end. Exits with its exit code.")
    wait.add_argument("job_id")

    job_sub.add_parser("agent", help="Run inside a job's container. Surfra starts this.")

    spec = job_sub.add_parser("spec", help="Save, list, show, or delete job specs.")
    spec_sub = spec.add_subparsers(dest="spec_command", required=True)
    save = spec_sub.add_parser("save", help="Save a job spec. Everything after -- is the command.")
    save.add_argument("name")
    add_fields(save)
    save.add_argument("words", nargs=argparse.REMAINDER, metavar="-- COMMAND")
    spec_sub.add_parser("list", help="Saved job specs.")
    show = spec_sub.add_parser("show", help="One job spec.")
    show.add_argument("name")
    delete = spec_sub.add_parser("delete", help="Delete a job spec.")
    delete.add_argument("name")


def _command_words(words: list[str]) -> list[str]:
    return words[1:] if words and words[0] == "--" else words


def _client() -> SurfraClient:
    from surfra.host import _master, _session

    return SurfraClient(api_key=_session(), base_url=_master())


def _print_line(line: dict[str, Any]) -> None:
    stamp = str(line.get("t") or "")[11:19]
    stream = line.get("s")
    if stream == "sys":
        print(f"{stamp} [surfra] {line.get('m')}", flush=True)
    else:
        print(f"{stamp} {line.get('m')}", file=sys.stderr if stream == "err" else sys.stdout, flush=True)


def _summary(view: dict[str, Any]) -> str:
    reason = view.get("reason") or ""
    code = "" if view.get("exit_code") is None or "exit code" in reason else f", exit code {view['exit_code']}"
    return f"job {view['id']} {view['status']}{code}: {reason}".rstrip(": ")


def _exit_code(view: dict[str, Any]) -> int:
    if view.get("status") == "succeeded":
        return 0
    code = view.get("exit_code")
    return int(code) if isinstance(code, int) and code > 0 else 1


def _follow(job: Job) -> int:
    try:
        for line in job.logs(follow=True):
            _print_line(line)
    except KeyboardInterrupt:
        print(f"\nThe job keeps running. Follow it with: surfra job logs -f {job.id}   Stop it with: surfra job stop {job.id}", file=sys.stderr)
        return 130
    view = job.get()
    print(_summary(view), file=sys.stderr)
    return _exit_code(view)


def run(args: argparse.Namespace) -> int:
    if args.job_command == "agent":
        from surfra.job import agent_main

        return agent_main()
    client = _client()
    try:
        return _dispatch(client, args)
    except SurfraError as exc:
        print(f"surfra: {exc.detail}", file=sys.stderr)
        return 1


def _dispatch(client: SurfraClient, args: argparse.Namespace) -> int:
    command = args.job_command
    if command == "submit":
        words = _command_words(args.words)
        fields = _fields(args)
        if args.spec:
            fields["spec"] = args.spec
            if words:
                fields["args"] = words
        elif words:
            fields["entrypoint"] = " ".join(words)
        else:
            raise SystemExit("give the command after --, such as: surfra job submit --image IMAGE --runner NAME -- bash run.sh")
        job = client.submit_job(**fields, name=args.name, submission_id=args.submission_id)
        print(f"job {job.id} {job.status}: {job.view.get('command')}", file=sys.stderr)
        for warning in job.view.get("warnings") or []:
            print(f"warning: {warning}", file=sys.stderr)
        if args.no_wait:
            print(job.id)
            return 0
        return _follow(job)
    if command == "logs":
        job = client.job(args.job_id)
        if args.follow:
            return _follow(job)
        for line in job.logs():
            _print_line(line)
        return 0
    if command == "status":
        view = client.job(args.job_id).view
        if args.json:
            print(json.dumps(view, indent=2))
            return 0
        for key in ("id", "name", "status", "reason", "exit_code", "command", "image", "machine", "created_at", "started_at", "finished_at", "deadline_at", "last_heartbeat"):
            if view.get(key) not in (None, ""):
                print(f"{key:<15} {view[key]}")
        if view.get("env_keys"):
            print(f"{'env':<15} " + ", ".join(f"{key} ({source})" for key, source in view["env_keys"].items()))
        return 0
    if command == "list":
        for view in client.jobs(limit=args.limit, status=args.status):
            print(f"{view['id']}  {view['status']:<10} {str(view.get('created_at'))[:19]}  {view['name']}  {view.get('command')}")
        return 0
    if command == "stop":
        view = client.job(args.job_id).stop()
        print(_summary(view) if view["status"] in JOB_DONE else f"job {view['id']} is stopping")
        return 0
    if command == "wait":
        view = client.job(args.job_id).wait()
        print(_summary(view))
        return _exit_code(view)
    if command == "spec":
        return _spec(client, args)
    raise SystemExit(f"unknown job command {command}")


def _spec(client: SurfraClient, args: argparse.Namespace) -> int:
    if args.spec_command == "save":
        fields = _fields(args)
        words = _command_words(args.words)
        if words:
            fields["entrypoint"] = " ".join(words)
        saved = client.save_job_spec(args.name, **fields)
        print(f"saved job spec {saved['name']}: {saved.get('entrypoint')}")
        return 0
    if args.spec_command == "list":
        for spec in client.job_specs():
            print(f"{spec['name']:<24} {spec.get('image')}  {spec.get('entrypoint')}")
        return 0
    if args.spec_command == "show":
        print(json.dumps(next((spec for spec in client.job_specs() if spec["name"] == args.name), None) or {}, indent=2))
        return 0
    if args.spec_command == "delete":
        client.delete_job_spec(args.name)
        print(f"deleted job spec {args.name}")
        return 0
    raise SystemExit(f"unknown spec command {args.spec_command}")

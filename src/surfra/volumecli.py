"""surfra volume: create, list, copy and delete files in volumes from your machine."""

from __future__ import annotations

import argparse
from pathlib import Path

from surfra.client import SurfraError

EXAMPLES = """Examples:
  surfra volume create weights
  surfra volume ls
  surfra volume ls weights minilm/
  surfra volume put weights ./minilm minilm/
  surfra volume get weights minilm/ ./minilm
  surfra volume rm weights old/ -r
  surfra volume delete weights"""


def _size(count: int) -> str:
    value = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1000 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{count} B"


def build(sub: argparse._SubParsersAction) -> None:
    volume = sub.add_parser("volume", help="Files your apps, jobs and machines share.", description=EXAMPLES, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = volume.add_subparsers(dest="volume_command", required=True)
    create = commands.add_parser("create", help="Create a volume. Uploading to a new name also creates it.")
    create.add_argument("name")
    listing = commands.add_parser("ls", help="List volumes, or the files in one.")
    listing.add_argument("name", nargs="?")
    listing.add_argument("prefix", nargs="?", default="")
    listing.add_argument("-r", "--recursive", action="store_true", help="List every file under the prefix, not just one level.")
    put = commands.add_parser("put", help="Upload a file or folder.")
    put.add_argument("name")
    put.add_argument("local")
    put.add_argument("remote", nargs="?", default="")
    get = commands.add_parser("get", help="Download a file or folder.")
    get.add_argument("name")
    get.add_argument("remote")
    get.add_argument("local", nargs="?", default=".")
    remove = commands.add_parser("rm", help="Delete a file, or a folder with -r.")
    remove.add_argument("name")
    remove.add_argument("path")
    remove.add_argument("-r", "--recursive", action="store_true")
    delete = commands.add_parser("delete", help="Delete a whole volume and all its files. Needs an admin.")
    delete.add_argument("name")
    delete.add_argument("--yes", action="store_true", help="Do not ask first.")


def run(args: argparse.Namespace) -> int:
    from surfra.volumes import Volume

    try:
        command = args.volume_command
        if command == "create":
            made = Volume(args.name).create()
            print(f"{made['name']} ({made['id']})")
        elif command == "ls" and not args.name:
            rows = Volume.list()
            if not rows:
                print("No volumes yet. Create one with: surfra volume create NAME")
            for row in rows:
                used = ", ".join(item["name"] for item in row.get("used_by") or []) or "-"
                print(f"{row['name']:<24} {_size(row['bytes']):>10} {row['objects']:>9} files   used by {used}")
        elif command == "ls":
            for entry in Volume(args.name).ls(args.prefix, recursive=args.recursive):
                print(f"{entry.path}" if entry.path.endswith("/") else f"{entry.path:<60} {_size(entry.size):>10}  {entry.modified[:19]}")
        elif command == "put":
            source = Path(args.local)
            volume = Volume(args.name)
            if source.is_dir():
                count = volume.put_directory(source, args.remote)
                print(f"uploaded {count} files to {args.name}:{args.remote or '/'}")
            else:
                remote = args.remote if args.remote and not args.remote.endswith("/") else f"{args.remote}{source.name}"
                entry = volume.put_file(source, remote)
                print(f"uploaded {args.name}:{entry.path} ({_size(entry.size)})")
        elif command == "get":
            path = Volume(args.name).path(args.remote).download(args.local)
            print(f"downloaded to {path}")
        elif command == "rm":
            count = Volume(args.name).rm(args.path, recursive=args.recursive)
            print(f"deleted {count} file{'s' if count != 1 else ''}")
        elif command == "delete":
            if not args.yes and input(f"Delete volume {args.name} and every file in it? Type its name to confirm: ").strip() != args.name:
                print("Not deleted.")
                return 1
            Volume(args.name).delete()
            print(f"deleted volume {args.name}")
        return 0
    except SurfraError as exc:
        print(f"error: {exc.detail}")
        return 1

"""Tell people when a newer surfra is out, and how to install it."""

from __future__ import annotations

import json
import os
import sys
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import httpx

REPO = "surfra-io/surfra-python-sdk"
CHECK_SECONDS = 24 * 3600
HOME = Path(os.environ.get("SURFRA_HOME") or Path.home() / ".surfra")
CACHE = HOME / "update.json"


def installed() -> str:
    """The version on disk now. After an update this differs from the running code."""

    try:
        return version("surfra")
    except PackageNotFoundError:
        return "0"


RUNNING = installed()


def _parts(text: str) -> tuple[int, ...]:
    numbers = []
    for piece in text.lstrip("v").split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        if not digits:
            break
        numbers.append(int(digits))
    return tuple(numbers)


def newer(candidate: str, current: str) -> bool:
    return bool(_parts(candidate)) and _parts(candidate) > _parts(current)


def latest(*, timeout: float = 1.5) -> str:
    """The newest release tag, checked at most once a day. Empty when unknown."""

    try:
        cached = json.loads(CACHE.read_text())
    except (OSError, ValueError):
        cached = {}
    if time.time() - float(cached.get("checked") or 0) < CHECK_SECONDS:
        return str(cached.get("latest") or "")
    found = ""
    try:
        response = httpx.head(f"https://github.com/{REPO}/releases/latest", follow_redirects=True, timeout=timeout)
        tag = str(response.url).rstrip("/").rsplit("/", 1)[-1]
        if tag.startswith("v"):
            found = tag[1:]
    except httpx.HTTPError:
        pass
    try:
        HOME.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps({"checked": time.time(), "latest": found or cached.get("latest") or ""}))
    except OSError:
        pass
    return found or str(cached.get("latest") or "")


def how_to_update(release: str) -> str:
    if Path(sys.prefix).resolve() == (HOME / "venv").resolve():
        if sys.platform == "win32":
            return "irm https://surfra.io/runner.ps1 | iex"
        return "curl -fsSL https://surfra.io/runner.sh | sh"
    return f"{Path(sys.executable).name} -m pip install -U https://github.com/{REPO}/releases/download/v{release}/surfra-{release}-py3-none-any.whl"


def notice() -> str:
    """A short message when a newer release is out, else empty."""

    if os.environ.get("SURFRA_NO_UPDATE_CHECK"):
        return ""
    release = latest()
    if not newer(release, RUNNING):
        return ""
    return f"surfra {release} is available (you have {RUNNING}). Update with:\n  {how_to_update(release)}"

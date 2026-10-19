import base64
import json
import os
import stat
import subprocess

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from surfra import host


def test_secret_values_never_reach_the_docker_command_line():
    command, cli_env = host._docker_command(
        ["docker", "run"],
        {"HF_TOKEN": "hf_secret", "SURFRA_REPLICA_TOKEN": "sfp_secret", "DOCKER_HOST": "unix:///x", "PATH": "/opt/bin"},
    )
    joined = " ".join(command)
    assert "hf_secret" not in joined and "sfp_secret" not in joined
    assert ["-e", "HF_TOKEN"] == command[command.index("HF_TOKEN") - 1 : command.index("HF_TOKEN") + 1]
    assert cli_env["HF_TOKEN"] == "hf_secret"
    # Names the docker CLI reads itself stay on the command line, so they cannot redirect the CLI.
    assert "DOCKER_HOST=unix:///x" in command and "PATH=/opt/bin" in command
    assert cli_env.get("DOCKER_HOST") == os.environ.get("DOCKER_HOST")


def _seal_v1(public_b64: str, text: str) -> str:
    import hashlib

    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PublicKey
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    ephemeral = X25519PrivateKey.generate()
    shared = ephemeral.exchange(X25519PublicKey.from_public_bytes(base64.b64decode(public_b64)))
    nonce = os.urandom(12)
    blob = AESGCM(hashlib.sha256(shared).digest()).encrypt(nonce, text.encode(), None)
    eph = ephemeral.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return "x25519:" + base64.b64encode(eph + nonce + blob).decode()


def test_a_run_passes_secrets_through_the_cli_environment_only(monkeypatch, tmp_path):
    private = X25519PrivateKey.generate()
    raw = private.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    public = private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    calls = []

    def fake_run(argv, **kwargs):
        calls.append({"argv": list(argv), "env": kwargs.get("env"), "input": kwargs.get("input")})
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(host.subprocess, "run", fake_run)
    monkeypatch.setattr(host.shutil, "which", lambda name: "/usr/bin/docker")
    saved = {"id": "run_a", "private_key": base64.b64encode(raw).decode()}
    assignment = {
        "replica_id": "rep_1",
        "app_name": "demo",
        "image": "ghcr.io/acme/app:1",
        "env": {"HF_TOKEN": "hf_secret", "SURFRA_REPLICA_TOKEN": "sfp_secret"},
        "docker_user": "acme",
        "docker_server": "ghcr.io",
        "docker_password_box": _seal_v1(base64.b64encode(public).decode(), "ghp_password"),
    }
    host._docker(saved, assignment)

    for call in calls:
        for secret in ("hf_secret", "sfp_secret", "ghp_password"):
            assert secret not in " ".join(call["argv"])
    login = next(call for call in calls if call["argv"][1] == "login")
    assert login["input"] == "ghp_password"
    config = login["env"]["DOCKER_CONFIG"]
    # The throwaway login config is gone once the image is pulled.
    assert not os.path.exists(config)
    pull = next(call for call in calls if call["argv"][1] == "pull")
    assert pull["env"]["DOCKER_CONFIG"] == config
    run = next(call for call in calls if call["argv"][1] == "run")
    assert run["env"]["HF_TOKEN"] == "hf_secret"
    assert "DOCKER_CONFIG" not in run["env"] or run["env"]["DOCKER_CONFIG"] != config


def test_credential_files_are_created_owner_only(monkeypatch, tmp_path):
    home = tmp_path / ".surfra"
    home.mkdir(mode=0o755)
    monkeypatch.setattr(host, "HOME", home)
    old = os.umask(0o022)
    try:
        host._write(home / "runner.json", {"private_key": "k"})
    finally:
        os.umask(old)
    assert stat.S_IMODE((home / "runner.json").stat().st_mode) == 0o600
    assert stat.S_IMODE(home.stat().st_mode) == 0o700
    assert json.loads((home / "runner.json").read_text()) == {"private_key": "k"}

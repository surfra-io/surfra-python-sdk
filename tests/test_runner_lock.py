import json

import pytest

from surfra import host

fcntl = pytest.importorskip("fcntl")


@pytest.fixture()
def home(tmp_path, monkeypatch):
    monkeypatch.setattr(host, "HOME", tmp_path)
    monkeypatch.setattr(host, "LOCK", tmp_path / "runner.lock")
    monkeypatch.setattr(host, "PID", tmp_path / "runner.pid")
    monkeypatch.setattr(host, "RUNNER", tmp_path / "runner.json")
    monkeypatch.setattr(host, "UNIT", tmp_path / "no-such.service")
    monkeypatch.setattr(host, "_LOCK_HANDLE", None)
    yield tmp_path
    if host._LOCK_HANDLE:
        host._LOCK_HANDLE.close()


def test_a_second_runner_on_one_registration_is_refused_until_the_first_ends(home):
    assert host._hold_lock()
    first = host._LOCK_HANDLE
    assert not host._hold_lock(), "a second copy must not take the lock"
    assert host._LOCK_HANDLE is first
    first.close()  # the first runner ends; the OS drops its lock
    assert host._hold_lock()


def test_start_stops_before_talking_to_surfra_when_a_runner_is_running(home, capsys, monkeypatch):
    (home / "runner.json").write_text(json.dumps({"token": "sfr_x", "id": "run_a", "gpu_count": 0}))
    (home / "runner.pid").write_text("4242")
    monkeypatch.setattr(host, "_missing_runtime", lambda gpu: None)
    holder = open(home / "runner.lock", "a")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert host.start() == 1
    finally:
        holder.close()
    err = capsys.readouterr().err
    assert "already running on this machine (pid 4242)" in err and "surfra runner stop" in err
    assert (home / "runner.pid").read_text() == "4242", "the running runner's pid file is left alone"

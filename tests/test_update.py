import json
import time

from surfra import host, update


def test_versions_compare_by_number():
    assert update.newer("0.1.10", "0.1.9")
    assert update.newer("v1.0.0", "0.9.9")
    assert not update.newer("0.1.3", "0.1.3")
    assert not update.newer("", "0.1.3")


def test_notice_names_the_release_and_how_to_get_it(tmp_path, monkeypatch):
    monkeypatch.setattr(update, "HOME", tmp_path)
    monkeypatch.setattr(update, "CACHE", tmp_path / "update.json")
    monkeypatch.setattr(update, "RUNNING", "0.1.3")
    monkeypatch.delenv("SURFRA_NO_UPDATE_CHECK", raising=False)
    (tmp_path / "update.json").write_text(json.dumps({"checked": time.time(), "latest": "0.2.0"}))
    message = update.notice()
    assert "surfra 0.2.0 is available (you have 0.1.3)" in message
    assert "surfra-0.2.0-py3-none-any.whl" in message

    monkeypatch.setattr(update, "RUNNING", "0.2.0")
    assert update.notice() == ""
    monkeypatch.setattr(update, "RUNNING", "0.1.3")
    monkeypatch.setenv("SURFRA_NO_UPDATE_CHECK", "1")
    assert update.notice() == ""


def test_installer_users_are_told_to_rerun_the_script(tmp_path, monkeypatch):
    monkeypatch.setattr(update, "HOME", tmp_path)
    monkeypatch.setattr(update.sys, "prefix", str(tmp_path / "venv"))
    monkeypatch.setattr(update.sys, "platform", "linux")
    assert update.how_to_update("0.2.0") == "curl -fsSL https://surfra.io/runner.sh | sh"
    monkeypatch.setattr(update.sys, "platform", "win32")
    assert update.how_to_update("0.2.0") == "irm https://surfra.io/runner.ps1 | iex"


def test_idle_runner_restarts_only_onto_a_newer_install(monkeypatch):
    calls = []
    monkeypatch.setattr(host.os, "execv", lambda *args: calls.append(args))
    monkeypatch.setattr(update, "notice", lambda: "")
    monkeypatch.setattr(update, "RUNNING", "0.1.3")

    monkeypatch.setattr(update, "installed", lambda: "0")
    host._pick_up_update("")
    monkeypatch.setattr(update, "installed", lambda: "0.1.3")
    host._pick_up_update("")
    assert calls == []

    monkeypatch.setattr(update, "installed", lambda: "0.1.4")
    host._pick_up_update("")
    assert calls and calls[0][1][-2:] == ["runner", "start"]

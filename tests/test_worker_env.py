import pytest

from surfra.worker import baked_runtime


def test_worker_reads_the_autoscaler_environment(monkeypatch):
    monkeypatch.setenv("SURFRA_MASTER", "http://surfra.test")
    monkeypatch.setenv("SURFRA_REPLICA_TOKEN", "sfp_baked")
    monkeypatch.setenv("SURFRA_ENTRYPOINT", "examples/hello_app.py")
    assert baked_runtime() == ("http://surfra.test", "sfp_baked", "examples/hello_app.py")


def test_worker_does_not_start_without_a_baked_token(monkeypatch):
    monkeypatch.delenv("SURFRA_REPLICA_TOKEN", raising=False)
    monkeypatch.delenv("SURFRA_MASTER", raising=False)
    monkeypatch.delenv("SURFRA_ENTRYPOINT", raising=False)
    with pytest.raises(SystemExit):
        baked_runtime()

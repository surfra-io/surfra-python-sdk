import multiprocessing
import time

from surfra.worker import _signal_ready


def test_supervisor_handshake_stays_under_five_seconds():
    """The Surfra process start, before any model load, stays inside a few seconds."""

    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    started = time.perf_counter()
    process = ctx.Process(target=_signal_ready, args=(results,))
    process.start()
    message = results.get(timeout=5)
    elapsed = time.perf_counter() - started
    process.join(timeout=5)
    assert message == {"kind": "ready"}
    assert elapsed < 5, f"supervisor handshake took {elapsed:.2f}s"

from surfra.runner import run_handler_batch
from surfra.sdk import Request, batch_size, handler, registered_handler_spec, reset_registry, stage, registered_stages, take_batch


def test_batch_waits_then_takes_a_full_group():
    assert take_batch(3, batch_min=1, batch_max=8, wait_ms=300, oldest_age_ms=100) == 0
    assert take_batch(3, batch_min=1, batch_max=8, wait_ms=300, oldest_age_ms=300) == 3
    assert take_batch(9, batch_min=1, batch_max=8, wait_ms=300, oldest_age_ms=0) == 8


def test_handler_records_batch_controls():
    reset_registry()

    @handler(batch_size=batch_size(1, 8), wait=300)
    def predict(request):
        return request.input

    spec = registered_handler_spec()
    assert spec["batch_min"] == 1
    assert spec["batch_max"] == 8
    assert spec["wait_ms"] == 300
    assert spec["concurrent_workers"] == 1
    assert predict is not None
    reset_registry()

    @handler(batch_size=batch_size(1, 8), batch_wait=300, concurrent_workers=4)
    def batched(requests):
        return requests

    spec = registered_handler_spec()
    assert spec["wait_ms"] == 300
    assert spec["concurrent_workers"] == 4
    assert batched is not None
    reset_registry()


def test_batch_handler_returns_one_result_per_request():
    reset_registry()

    @handler(batch_size=batch_size(1, 8), batch_wait=300)
    def predict(requests):
        return [item.input["n"] + 1 for item in requests]

    assert run_handler_batch([{"n": 1}, {"n": 2}]) == [2, 3]
    reset_registry()


def test_single_handler_still_receives_one_request():
    reset_registry()

    @handler
    def predict(request):
        assert isinstance(request, Request)
        return request.input["n"]

    assert run_handler_batch([{"n": 7}]) == [7]
    reset_registry()


def test_stage_keeps_depends_and_schema():
    reset_registry()

    @stage(app="hello", output_schema={"type": "object"})
    def hello(data):
        return data

    @stage(app="world", depends=("hello",), input_schema={"type": "object"})
    def world(data):
        return data

    rows = [item.public() for item in registered_stages()]
    assert rows[1]["depends"] == ["hello"]
    assert rows[1]["input_schema"]["type"] == "object"
    assert hello is not world
    reset_registry()

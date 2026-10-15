from surfra.runner import load_app, run_handler, run_warmup
from surfra.sdk import reset_registry


def test_example_handler_runs(tmp_path, monkeypatch):
    reset_registry()
    source = (
        "import surfra\n"
        "model = None\n"
        "@surfra.warmup()\n"
        "def warmup():\n"
        "    global model\n"
        "    model = lambda tokens: [*tokens, 'ok']\n"
        "@surfra.handler()\n"
        "def main(request: surfra.Request):\n"
        "    surfra.progress(1.0, 'done')\n"
        "    return {'output': model(request.input['text'].split())}\n"
    )
    app = tmp_path / "app.py"
    app.write_text(source)
    monkeypatch.syspath_prepend(str(tmp_path))
    load_app(app)
    run_warmup()
    result, request = run_handler({"text": "hello"})
    assert result == {"output": ["hello", "ok"]}
    assert request.events[-1].message == "done"
    reset_registry()

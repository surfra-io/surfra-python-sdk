# Surfra Python SDK

Run your own model as a serverless app on the GPU clouds you connect to [Surfra](https://surfra.io).

```bash
pip install surfra
```

```python
import surfra


@surfra.handler()
def main(request: surfra.Request):
    surfra.progress(1.0, "hello")
    return {"text": "hello"}
```

Call a deployed app:

```python
from surfra import SurfraClient

client = SurfraClient()  # reads SURFRA_API_KEY
print(client.run("my-app", {"prompt": "hi"}))
```

From async code, use `surfra.aio`. It has the same names, and every call that talks to Surfra is
awaited, so nothing blocks the event loop:

```python
import surfra.aio as surfra

call = await surfra.App("my-app").enqueue({"prompt": "hi"})
print(await call.result(timeout=300))
```

Sync code keeps `import surfra` and works as before.

Give a task a `callback_url` and Surfra POSTs its latest state there each time it changes. Check the
signature in your handler:

```python
from surfra import verify_webhook

event = verify_webhook(raw_body, request_headers, secret)  # raises surfra.InvalidSignature
```

Guides and the API reference are at [docs.surfra.io](https://docs.surfra.io).

## License

Apache 2.0. See [LICENSE](LICENSE).

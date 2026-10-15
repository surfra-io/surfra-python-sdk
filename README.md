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

Guides and the API reference are at [docs.surfra.io](https://docs.surfra.io).

## License

Apache 2.0. See [LICENSE](LICENSE).

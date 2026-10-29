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

## Errors and retries

When Surfra can't take work right now, it refuses with a typed error and queues nothing, so the SDK
retries it for you, after the `Retry-After` Surfra sends:

| Status | `SurfraError.type` | Retried? |
| --- | --- | --- |
| 429 | `RATE_LIMITED`: your organization's per-minute limit | yes |
| 503 | `QUEUE_FULL`: the app's queue is as long as its machines can clear soon | yes |
| 503 | `SERVER_BUSY`: Surfra is short of database connections | yes |
| 503 | `APP_PAUSED`, `DEFCON_ACTIVE`: the app is paused, or the kill switch is on | no |
| 402 | `HTTP_CALLS_LIMIT`: this month's included HTTP calls are used up and the organization has no card on file. Add one in Settings, or wait for the month to turn | no |
| 502/503/504 with no Surfra error | `None` | only reads (`GET`) |
| no connection at all | `None`, status 0 | yes |
| other 4xx | | no |

**Limits:** at most 7 retries (8 attempts) within 3 minutes of the first try. Without `Retry-After`, the
waits back off exponentially with jitter (0.5 s base, 30 s cap). Each retry is logged on the `surfra`
logger, and passed to `on_retry` if you give one.

```python
client = surfra.SurfraClient(max_retries=3, retry_deadline=30, on_retry=print)
client.with_retry(max_retries=0).submit(stages)   # one call, never retried
```

The same options work on `surfra.aio.SurfraClient` and on `Volume`.

**The final error:** `SurfraError` carries `status`, `type`, `detail`, `attempts` and `retry_after`.

**When a submission's outcome is unknown:** a submission (a run, a transaction or a job) that got a 504, or
lost its connection after it was sent, is never retried, because it may already be running. Its error has
`outcome_unknown=True`.
- Check the task before submitting again.
- Prefer `enqueue()` then `result()`, or a `callback_url`, over one long `run()` for long work.
- `submit_job` takes a `submission_id` that makes a retry return the same job.

Guides and the API reference are at [docs.surfra.io](https://docs.surfra.io).

## License

Apache 2.0. See [LICENSE](LICENSE).

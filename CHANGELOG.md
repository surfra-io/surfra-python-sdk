# Changelog

## Unreleased

### Added
- Every request sends `Surfra-Client: surfra-python/<version>`, so the control plane can trace errors to a release.
- Overload retries in every client, sync and async, as one policy:
  - **Retried for every request:** 429 `RATE_LIMITED`, 503 `QUEUE_FULL` and 503 `SERVER_BUSY`, after `Retry-After`, plus connections that never reached Surfra.
  - **Retried only for `GET`:** 502/503/504 without a Surfra error body, and connections dropped after sending.
  - **Never retried:** `APP_PAUSED`, `DEFCON_ACTIVE`, other 4xx, and submissions that got a 504 or lost their connection after sending.
  - **Limits:** `max_retries` (7), `retry_deadline` (180 s) and `on_retry` on `SurfraClient`, `surfra.aio.SurfraClient` and `Volume`; `with_retry()` changes them for some calls.
- `SurfraError.type`, `attempts`, `outcome_unknown` and `retry_after`.
- 402 `HTTP_CALLS_LIMIT` (the organization used its included HTTP calls this month and has no card on file) is named in the docs and never retried; conformance case r13.

### Changed
- The submit-only 429 retry (5 tries, 1 to 60 s) is replaced by the policy above.

## [0.4.1] - 2026-10-26

### Added
- `surfra.verify_webhook` (also in `surfra.aio`) checks a callback's `X-Surfra-Signature`, and
  `surfra.InvalidSignature` is what it raises.

## [0.4.0] - 2026-10-25

### Added
- `surfra.aio`: the SDK for asyncio, with the same names as `surfra` (`App`, `Workflow`, `Call`, `Job`,
  `SurfraClient`, `Volume`, `VolumePath`, ...). Every call that talks to Surfra is `async`, on
  `httpx.AsyncClient`. `map()` and `Job.logs()` are async iterators. Volume transfers run in a worker thread.
  Switch with `import surfra.aio as surfra` and add `await`.
- `callback_url` on `SurfraClient.enqueue` and `App.enqueue`: Surfra POSTs the task's latest state there
  each time it changes.
- `SurfraClient.close()` and `with SurfraClient() as client:`.

### Changed
- Transaction and job ids are escaped in request paths.
- The client tests run once against `surfra` and once against `surfra.aio`.

## [0.3.1]

- Earlier releases: see the GitHub releases page.

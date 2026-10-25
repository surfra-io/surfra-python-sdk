# Changelog

## [0.4.0] - unreleased

### Added
- `surfra.aio`: the SDK for asyncio, with the same names as `surfra` (`App`, `Workflow`, `Call`, `Job`,
  `SurfraClient`, `Volume`, `VolumePath`, ...). Every call that talks to Surfra is `async`, on
  `httpx.AsyncClient`. `map()` and `Job.logs()` are async iterators. Volume transfers run in a worker thread.
  Switch with `import surfra.aio as surfra` and add `await`.
- `callback_url` on `SurfraClient.enqueue` and `App.enqueue`. It is stored with the request; Surfra
  does not call it yet.
- `SurfraClient.close()` and `with SurfraClient() as client:`.

### Changed
- Transaction and job ids are escaped in request paths.
- The client tests run once against `surfra` and once against `surfra.aio`.

## [0.3.1]

- Earlier releases: see the GitHub releases page.

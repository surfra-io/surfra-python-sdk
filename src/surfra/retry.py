"""Retrying Surfra's overload answers, the same way in surfra and surfra.aio.

Surfra refuses work it can't take right now with a typed error, and nothing is queued, so a retry
can't run the work twice:

    429 RATE_LIMITED   the organization's own per-minute limit (any 429 is retried)
    503 QUEUE_FULL     the app's queue is as long as its machines can clear soon
    503 SERVER_BUSY    the API is short of database connections

Those are retried for every request, after Retry-After. So is a connection that never reached the
server. A 502/503/504 with no Surfra error body (the platform's edge during a deploy), or a connection
dropped after the request was sent, is retried only for GET, which is safe to repeat. Never retried:
503 APP_PAUSED, 503 DEFCON_ACTIVE, other 4xx, and a submission (a POST that starts a run, transaction or
job) that got a 504 or lost its connection after it was sent: it may be running, so the error says the
outcome is unknown instead.

The retries live in an httpx transport, so every request a client makes goes through them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from dataclasses import dataclass, replace
from typing import Any, Callable

import httpx

log = logging.getLogger("surfra")

RETRY_TYPES = {"RATE_LIMITED", "QUEUE_FULL", "SERVER_BUSY"}
NEVER_TYPES = {"APP_PAUSED", "DEFCON_ACTIVE", "HTTP_CALLS_LIMIT"}
EDGE_STATUSES = {502, 503, 504}
SAFE_METHODS = {"GET", "HEAD"}
# POSTs that start work. A 504 or a dropped connection after sending one leaves its outcome unknown.
_SUBMISSION = re.compile(r"/v1/(transactions|jobs|apps/[^/]+/run)/?$")


@dataclass(frozen=True)
class RetryEvent:
    """One retry about to happen: after `attempt` failed, wait `delay` seconds."""

    attempt: int
    delay: float
    method: str
    url: str
    reason: str
    status: int | None = None
    type: str | None = None


@dataclass(frozen=True)
class RetryPolicy:
    """How overload answers are retried. max_retries=0 turns retries off.

    max_retries: retries after the first try (7: at most 8 attempts).
    deadline: seconds from the first attempt; a wait that would end later is not taken.
    on_retry: called before each wait, in addition to a log line on the "surfra" logger.
    """

    max_retries: int = 7
    deadline: float = 180.0
    base: float = 0.5
    cap: float = 30.0
    on_retry: Callable[[RetryEvent], Any] | None = None

    def with_(self, *, max_retries: int | None = None, deadline: float | None = None, on_retry: Callable[[RetryEvent], Any] | None = None) -> "RetryPolicy":
        changes: dict[str, Any] = {}
        if max_retries is not None:
            changes["max_retries"] = max(0, int(max_retries))
        if deadline is not None:
            changes["deadline"] = float(deadline)
        if on_retry is not None:
            changes["on_retry"] = on_retry
        return replace(self, **changes)

    def backoff(self, attempt: int) -> float:
        """Full jitter: anything from 0 to min(cap, base * 2^(attempt-1))."""

        return random.uniform(0, min(self.cap, self.base * (2 ** (attempt - 1))))


def error_type(response: httpx.Response) -> str | None:
    """error.type from a Surfra error body, or None when there isn't one."""

    try:
        body = json.loads(response.content or b"null")
    except ValueError:
        return None
    error = body.get("error") if isinstance(body, dict) else None
    return error.get("type") if isinstance(error, dict) and isinstance(error.get("type"), str) else None


def retry_after(response: httpx.Response) -> float | None:
    header = response.headers.get("retry-after")
    if header is not None:
        try:
            return max(0.0, float(header))
        except ValueError:
            from email.utils import parsedate_to_datetime

            try:
                return max(0.0, parsedate_to_datetime(header).timestamp() - time.time())
            except (TypeError, ValueError):
                return None
    try:
        body = json.loads(response.content or b"null")
        value = body["error"]["retry_after"]
        return max(0.0, float(value))
    except (ValueError, KeyError, TypeError):
        return None


def is_submission(request: httpx.Request) -> bool:
    return request.method == "POST" and bool(_SUBMISSION.search(request.url.path))


def _resendable(request: httpx.Request) -> bool:
    return isinstance(request.stream, httpx.ByteStream)


def _never_sent(exc: Exception) -> bool:
    """The connection failed before the request left: nothing reached the server."""

    return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.UnsupportedProtocol))


class _Decision:
    __slots__ = ("retry", "unknown", "reason", "wait")

    def __init__(self, retry: bool, reason: str = "", wait: float | None = None, unknown: bool = False):
        self.retry, self.reason, self.wait, self.unknown = retry, reason, wait, unknown


def _judge_response(request: httpx.Request, response: httpx.Response) -> _Decision:
    status = response.status_code
    if status < 400:
        return _Decision(False)
    kind = error_type(response)
    if kind in NEVER_TYPES:
        return _Decision(False)
    if status == 429 or kind in RETRY_TYPES:
        return _Decision(True, f"{status} {kind or ''}".strip(), retry_after(response))
    if status in EDGE_STATUSES and kind is None:
        if request.method in SAFE_METHODS:
            return _Decision(True, f"{status} from the edge", retry_after(response))
        return _Decision(False, unknown=status == 504 and is_submission(request))
    return _Decision(False)


def _judge_error(request: httpx.Request, exc: Exception) -> _Decision:
    if _never_sent(exc):
        return _Decision(True, type(exc).__name__)
    if request.method in SAFE_METHODS:
        return _Decision(True, type(exc).__name__)
    return _Decision(False, unknown=is_submission(request))


class _Loop:
    """The bookkeeping shared by the sync and async transports."""

    def __init__(self, policy: RetryPolicy, request: httpx.Request):
        self.policy = policy
        self.request = request
        self.started = time.monotonic()
        self.attempt = 0

    def next_wait(self, decision: _Decision, response: httpx.Response | None) -> float | None:
        """Seconds to wait before the next attempt, or None to stop here."""

        if not decision.retry or not _resendable(self.request) or self.attempt > self.policy.max_retries:
            return None
        wait = decision.wait if decision.wait is not None else self.policy.backoff(self.attempt)
        if time.monotonic() - self.started + wait > self.policy.deadline:
            return None
        event = RetryEvent(
            attempt=self.attempt,
            delay=wait,
            method=self.request.method,
            url=str(self.request.url),
            reason=decision.reason,
            status=response.status_code if response is not None else None,
            type=error_type(response) if response is not None else None,
        )
        log.warning("surfra: %s %s failed (%s), attempt %s; retrying in %.2fs", event.method, self.request.url.path, event.reason, event.attempt, wait)
        if self.policy.on_retry is not None:
            self.policy.on_retry(event)
        return wait

    def mark(self, response: httpx.Response, decision: _Decision) -> httpx.Response:
        response.extensions["surfra_attempts"] = self.attempt
        if decision.unknown:
            response.extensions["surfra_outcome_unknown"] = True
        return response

    def failure(self, exc: Exception, decision: _Decision) -> Exception:
        from surfra.client import SurfraError

        if decision.unknown:
            message = (
                f"{self.request.method} {self.request.url.path}: the connection dropped after the request was sent "
                f"({type(exc).__name__}). It may be running: check it before submitting again, or use enqueue() and "
                "result() (or a callback_url) for long work."
            )
        else:
            message = f"{self.request.method} {self.request.url.path}: could not reach Surfra ({type(exc).__name__}: {exc})"
        error = SurfraError(0, message, type=None, attempts=self.attempt, outcome_unknown=decision.unknown)
        error.__cause__ = exc
        return error


class RetryTransport(httpx.BaseTransport):
    def __init__(self, inner: httpx.BaseTransport | None = None, policy: RetryPolicy | None = None):
        self.inner = inner or httpx.HTTPTransport()
        self.policy = policy or RetryPolicy()

    def with_policy(self, policy: RetryPolicy) -> "RetryTransport":
        return RetryTransport(self.inner, policy)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        loop = _Loop(self.policy, request)
        while True:
            loop.attempt += 1
            try:
                response = self.inner.handle_request(request)
            except httpx.TransportError as exc:
                decision = _judge_error(request, exc)
                wait = loop.next_wait(decision, None)
                if wait is None:
                    raise loop.failure(exc, decision) from exc
                time.sleep(wait)
                continue
            if response.status_code < 400:
                return loop.mark(response, _Decision(False))
            response.read()
            decision = _judge_response(request, response)
            wait = loop.next_wait(decision, response)
            if wait is None:
                return loop.mark(response, decision)
            response.close()
            time.sleep(wait)

    def close(self) -> None:
        self.inner.close()


class AsyncRetryTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport | None = None, policy: RetryPolicy | None = None):
        self.inner = inner or httpx.AsyncHTTPTransport()
        self.policy = policy or RetryPolicy()

    def with_policy(self, policy: RetryPolicy) -> "AsyncRetryTransport":
        return AsyncRetryTransport(self.inner, policy)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        loop = _Loop(self.policy, request)
        while True:
            loop.attempt += 1
            try:
                response = await self.inner.handle_async_request(request)
            except httpx.TransportError as exc:
                decision = _judge_error(request, exc)
                wait = loop.next_wait(decision, None)
                if wait is None:
                    raise loop.failure(exc, decision) from exc
                await asyncio.sleep(wait)
                continue
            if response.status_code < 400:
                return loop.mark(response, _Decision(False))
            await response.aread()
            decision = _judge_response(request, response)
            wait = loop.next_wait(decision, response)
            if wait is None:
                return loop.mark(response, decision)
            await response.aclose()
            await asyncio.sleep(wait)

    async def aclose(self) -> None:
        await self.inner.aclose()

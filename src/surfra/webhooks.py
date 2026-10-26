"""Check that a callback came from Surfra.

Surfra signs every callback with your organization's secret (Settings → Callbacks in the dashboard):

    X-Surfra-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>

During a secret rotation there is one v1 per secret. A timestamp more than five minutes away from your
clock is refused, so a captured callback can't be replayed later.

    from surfra import verify_webhook

    @api.post("/hooks/surfra")
    async def hook(request: Request):
        event = verify_webhook(await request.body(), request.headers, os.environ["SURFRA_CALLBACK_SECRET"])
        save(event["task"]["id"], event["seq"], event["task"])  # keep it only if seq is higher than what you have
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Mapping

HEADER = "X-Surfra-Signature"
TOLERANCE_SECONDS = 300


class InvalidSignature(ValueError):
    """The callback is not signed with this secret, or its timestamp is too far from now."""


def _header(headers: Mapping[str, Any] | str) -> str:
    if isinstance(headers, str):
        return headers
    for key, value in headers.items():
        if str(key).lower() == HEADER.lower():
            return value[0] if isinstance(value, (list, tuple)) else str(value)
    raise InvalidSignature(f"no {HEADER} header")


def verify_webhook(
    body: bytes | str,
    headers: Mapping[str, Any] | str,
    secret: str | list[str],
    *,
    tolerance: float = TOLERANCE_SECONDS,
    now: float | None = None,
) -> dict[str, Any]:
    """The parsed callback, if it is signed with secret (or any of a list) and recent enough.

    body is the raw request body, exactly as received. headers is the request's headers, or the
    X-Surfra-Signature value. Raises InvalidSignature otherwise. A pure function: the same in surfra and surfra.aio.
    """

    raw = body.encode() if isinstance(body, str) else bytes(body)
    stamp: int | None = None
    given: list[str] = []
    for part in _header(headers).split(","):
        key, _, value = part.strip().partition("=")
        if key == "t" and value.lstrip("-").isdigit():
            stamp = int(value)
        elif key == "v1" and value:
            given.append(value)
    if stamp is None or not given:
        raise InvalidSignature(f"{HEADER} needs t= and at least one v1=")
    clock = time.time() if now is None else now
    if abs(clock - stamp) > tolerance:
        raise InvalidSignature("the callback's timestamp is outside the allowed window")
    signed = f"{stamp}.".encode() + raw
    for candidate in [secret] if isinstance(secret, str) else list(secret):
        expected = hmac.new(candidate.encode(), signed, hashlib.sha256).hexdigest()
        if any(hmac.compare_digest(expected, value) for value in given):
            return json.loads(raw)
    raise InvalidSignature("the signature does not match")

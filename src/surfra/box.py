"""Open a secret the control plane sealed to this runner's key.

v2 boxes are X25519 with a fresh ephemeral key, HKDF-SHA256 over both public keys, and
AES-256-GCM with a context as associated data. The context names the runner, the replica, and
the field, so a box cannot be opened for anything other than what it was sealed for.
"""

from __future__ import annotations

import base64
import hashlib

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

SEAL_V2 = "x25519v2:"
# The seal versions this runner can open. Sent with every heartbeat.
SEAL_VERSION = 2


def box_context(runner_id: str, replica_id: str, field: str) -> str:
    return f"surfra/runner/{runner_id}/{replica_id}/{field}"


def open_box(private_key: str, sealed: str, context: str | None = None) -> str:
    if not sealed:
        return ""
    private = X25519PrivateKey.from_private_bytes(base64.b64decode(private_key))
    if sealed.startswith(SEAL_V2):
        if context is None:
            raise ValueError("a v2 credential seal needs its context")
        raw = base64.b64decode(sealed.removeprefix(SEAL_V2))
        eph_pub, nonce, blob = raw[:32], raw[32:44], raw[44:]
        mine = private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        shared = private.exchange(X25519PublicKey.from_public_bytes(eph_pub))
        key = HKDF(algorithm=hashes.SHA256(), length=32, salt=eph_pub + mine, info=b"surfra runner seal v2").derive(shared)
        return AESGCM(key).decrypt(nonce, blob, context.encode()).decode()
    if not sealed.startswith("x25519:"):
        raise ValueError("unrecognized credential seal")
    raw = base64.b64decode(sealed.removeprefix("x25519:"))
    eph_pub, nonce, blob = raw[:32], raw[32:44], raw[44:]
    shared = private.exchange(X25519PublicKey.from_public_bytes(eph_pub))
    return AESGCM(hashlib.sha256(shared).digest()).decrypt(nonce, blob, None).decode()

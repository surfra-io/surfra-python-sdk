"""Open a secret the control plane sealed to this runner's key."""

from __future__ import annotations

import base64
import hashlib

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def open_box(private_key: str, sealed: str) -> str:
    if not sealed:
        return ""
    if not sealed.startswith("x25519:"):
        raise ValueError("unrecognized credential seal")
    raw = base64.b64decode(sealed.removeprefix("x25519:"))
    eph_pub, nonce, blob = raw[:32], raw[32:44], raw[44:]
    private = X25519PrivateKey.from_private_bytes(base64.b64decode(private_key))
    shared = private.exchange(X25519PublicKey.from_public_bytes(eph_pub))
    return AESGCM(hashlib.sha256(shared).digest()).decrypt(nonce, blob, None).decode()

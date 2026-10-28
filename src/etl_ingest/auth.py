"""JWT (HS256) issue and verification for the run API.

Deliberately small and strict, using only the standard library:

* the algorithm is pinned to HS256: the token's own ``alg`` header is checked
  but never trusted to pick the algorithm (this blocks the ``alg: none`` and
  algorithm-confusion attacks);
* signatures are compared in constant time (``hmac.compare_digest``);
* ``exp`` is required, ``nbf``/``iss``/``aud`` are enforced when configured;
* the shared secret must be at least 32 bytes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Iterable, Optional

ALGORITHM = "HS256"
MIN_SECRET_BYTES = 32


class AuthError(Exception):
    """Token missing, malformed, expired, or lacking the required role."""


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (ValueError, TypeError):
        raise AuthError("malformed token") from None


def _check_secret(secret: str) -> bytes:
    key = secret.encode("utf-8")
    if len(key) < MIN_SECRET_BYTES:
        raise ValueError(f"JWT secret must be at least {MIN_SECRET_BYTES} bytes")
    return key


def _sign(key: bytes, signing_input: bytes) -> str:
    return _b64url_encode(hmac.new(key, signing_input, hashlib.sha256).digest())


def issue_token(subject: str, roles: Iterable[str], secret: str, ttl_seconds: int = 3600,
                issuer: Optional[str] = None, audience: Optional[str] = None) -> str:
    key = _check_secret(secret)
    now = int(time.time())
    claims: dict[str, Any] = {"sub": subject, "roles": sorted(set(roles)), "iat": now, "exp": now + ttl_seconds}
    if issuer:
        claims["iss"] = issuer
    if audience:
        claims["aud"] = audience
    header = _b64url_encode(json.dumps({"alg": ALGORITHM, "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url_encode(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode("ascii")
    return f"{header}.{payload}.{_sign(key, signing_input)}"


def verify_token(token: str, secret: str, required_role: Optional[str] = None,
                 issuer: Optional[str] = None, audience: Optional[str] = None,
                 leeway_seconds: int = 30) -> dict[str, Any]:
    key = _check_secret(secret)
    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 3 or not all(parts):
        raise AuthError("malformed token")
    header_b64, payload_b64, signature = parts

    try:
        header = json.loads(_b64url_decode(header_b64))
    except ValueError:
        raise AuthError("malformed token") from None
    if not isinstance(header, dict) or header.get("alg") != ALGORITHM:
        raise AuthError("unsupported algorithm")

    expected = _sign(key, f"{header_b64}.{payload_b64}".encode("ascii"))
    if not hmac.compare_digest(expected, signature):
        raise AuthError("invalid signature")

    try:
        claims = json.loads(_b64url_decode(payload_b64))
    except ValueError:
        raise AuthError("malformed token") from None
    if not isinstance(claims, dict):
        raise AuthError("malformed token")

    now = time.time()
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)):
        raise AuthError("token has no expiry")
    if now > exp + leeway_seconds:
        raise AuthError("token expired")
    nbf = claims.get("nbf")
    if isinstance(nbf, (int, float)) and now + leeway_seconds < nbf:
        raise AuthError("token not yet valid")
    if issuer is not None and claims.get("iss") != issuer:
        raise AuthError("wrong issuer")
    if audience is not None:
        aud = claims.get("aud")
        if aud != audience and not (isinstance(aud, list) and audience in aud):
            raise AuthError("wrong audience")
    if required_role is not None and required_role not in (claims.get("roles") or []):
        raise AuthError(f"missing role {required_role!r}")
    return claims

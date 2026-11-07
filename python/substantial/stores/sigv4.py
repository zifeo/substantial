"""AWS Signature Version 4 for the four S3 requests the adapter makes."""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from urllib.parse import quote

from substantial.stores.credentials import Credentials


def encode(value: str, *, keep_slash: bool = False) -> str:
    """URI-encodes as SigV4 requires: every byte but the unreserved characters."""
    return quote(value, safe="-_.~/" if keep_slash else "-_.~")


def canonical_query(params: dict[str, str]) -> str:
    pairs = sorted((encode(k), encode(v)) for k, v in params.items())
    return "&".join(f"{k}={v}" for k, v in pairs)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def sign(
    *,
    method: str,
    host: str,
    path: str,
    query: str,
    headers: dict[str, str],
    payload: bytes,
    credentials: Credentials,
    region: str,
    at: datetime | None = None,
) -> dict[str, str]:
    """The headers of a request to `host`, `headers` included, with its `Authorization`.

    `path` and `query` are already encoded as they go on the wire.
    """
    at = at or datetime.now(UTC)
    amz_date = at.strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    payload_hash = sha256_hex(payload)
    signed = {k.lower(): v.strip() for k, v in headers.items()}
    signed |= {"host": host, "x-amz-date": amz_date, "x-amz-content-sha256": payload_hash}
    if credentials.session_token:
        signed["x-amz-security-token"] = credentials.session_token
    names = sorted(signed)
    canonical_headers = "".join(f"{name}:{signed[name]}\n" for name in names)
    signed_headers = ";".join(names)
    request = "\n".join([method, path, query, canonical_headers, signed_headers, payload_hash])
    scope = f"{date}/{region}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, sha256_hex(request.encode())])
    key = _hmac(f"AWS4{credentials.secret_access_key}".encode(), date)
    for part in (region, "s3", "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    signed["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={credentials.access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return signed

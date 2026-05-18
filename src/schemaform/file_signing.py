from __future__ import annotations

import hmac
import secrets
import time
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote


def load_or_create_secret(path: Path) -> bytes:
    if path.exists():
        data = path.read_bytes().strip()
        if data:
            return data
    path.parent.mkdir(parents=True, exist_ok=True)
    data = secrets.token_urlsafe(48).encode("ascii")
    path.write_bytes(data)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return data


def _sign(file_id: str, expires_at: int, secret: bytes) -> str:
    message = f"{file_id}:{expires_at}".encode("utf-8")
    return hmac.new(secret, message, sha256).hexdigest()


def file_url_builder(request):
    """Request から、file_id を署名付き URL に変換する関数を生成する。"""
    secret = request.app.state.file_url_secret
    ttl = request.app.state.settings.file_url_ttl_seconds

    def build(file_id: str) -> str:
        return signed_file_url(file_id, secret, ttl)

    return build


def signed_file_url(file_id: str, secret: bytes, ttl_seconds: int) -> str:
    expires_at = int(time.time()) + max(60, ttl_seconds)
    token = _sign(file_id, expires_at, secret)
    return f"/files/{quote(file_id, safe='')}?e={expires_at}&t={token}"


def verify_file_token(
    file_id: str, expires_at: str | None, token: str | None, secret: bytes
) -> bool:
    if not expires_at or not token:
        return False
    try:
        exp = int(expires_at)
    except (TypeError, ValueError):
        return False
    if exp < int(time.time()):
        return False
    expected = _sign(file_id, exp, secret)
    return hmac.compare_digest(expected, token)

from __future__ import annotations

import json
import os
import ssl
import sys
import urllib.request
from typing import Any
from urllib.error import URLError


_INSECURE_SSL_FALLBACK = os.environ.get("ALLOW_INSECURE_SSL_FALLBACK", "1").strip().lower()
_ALLOW_INSECURE_SSL_FALLBACK = _INSECURE_SSL_FALLBACK not in {"0", "false", "no", "off"}
_SSL_FALLBACK_WARNED = False


def _is_ssl_verification_error(exc: BaseException) -> bool:
    pending = [exc]
    seen: set[int] = set()
    while pending:
        cur = pending.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if isinstance(cur, ssl.SSLCertVerificationError):
            return True
        if isinstance(cur, ssl.SSLError) and "CERTIFICATE_VERIFY_FAILED" in str(cur):
            return True
        for attr in ("reason", "__cause__", "__context__"):
            nxt = getattr(cur, attr, None)
            if isinstance(nxt, BaseException):
                pending.append(nxt)
    return False


def _warn_insecure_ssl_retry(url: str) -> None:
    global _SSL_FALLBACK_WARNED
    if _SSL_FALLBACK_WARNED:
        return
    _SSL_FALLBACK_WARNED = True
    print(
        f"warning: SSL verification failed for {url}; retrying without certificate verification",
        file=sys.stderr,
    )


def get_json(url: str, timeout_s: float = 20.0, user_agent: str = "python-json/1.0") -> Any:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": user_agent},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
    except (URLError, ssl.SSLError) as exc:
        if not (
            _ALLOW_INSECURE_SSL_FALLBACK
            and url.lower().startswith("https://")
            and _is_ssl_verification_error(exc)
        ):
            raise
        _warn_insecure_ssl_retry(url)
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout_s, context=ctx) as resp:
            raw = resp.read()
    return json.loads(raw.decode("utf-8"))

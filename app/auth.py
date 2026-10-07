"""
API keys for every /api/v2 route.

    API_KEYS="portal:3f9c...,noc:a71b..."     # name:key pairs, comma-separated
    API_KEYS="3f9c..."                        # or bare keys

Clients send the key as `X-API-Key: <key>` or `Authorization: Bearer <key>`.
The key's name (or "key-1", "key-2", ... for bare keys) is logged with each
request and stored on queued jobs' log lines, never the key itself.

When API_KEYS is not set:
  - with MOCK_MODE=true the routes are open, with a warning at startup, so
    `MOCK_MODE=true python main.py` works as before;
  - otherwise every /api/v2 route answers 503 auth_not_configured. A real
    deployment cannot be left open by forgetting a variable.

/api/v1 (rogue ONT detection, out of scope) and /health are not covered.
Browsers' EventSource cannot set headers, so a browser following a job's SSE
stream needs a server-side proxy that adds the key.
"""

import hmac
import logging
from typing import Dict, Optional

from fastapi import Header, Request

logger = logging.getLogger(__name__)


class ApiAuthError(Exception):
    def __init__(self, code: str, message: str, http_status: int):
        super().__init__(message)
        self.code, self.message, self.http_status = code, message, http_status

    def to_dict(self) -> Dict[str, object]:
        return {"code": self.code, "message": self.message, "retryable": False}


def parse_keys(raw: Optional[str]) -> Dict[str, str]:
    """'portal:abc,def' -> {'portal': 'abc', 'key-2': 'def'}"""
    keys: Dict[str, str] = {}
    for position, entry in enumerate((raw or "").split(","), start=1):
        entry = entry.strip()
        if not entry:
            continue
        name, _, key = entry.partition(":") if ":" in entry else (f"key-{position}", "", entry)
        name, key = name.strip(), key.strip()
        if len(key) < 16:
            raise ValueError(f"API key '{name}' is shorter than 16 characters")
        if name in keys:
            raise ValueError(f"API key name '{name}' is used twice")
        keys[name] = key
    return keys


def check_configuration(settings) -> None:
    """At startup: refuse malformed keys, warn when the API is open"""
    keys = parse_keys(settings.api_keys)
    if not keys and settings.mock_mode:
        logger.warning("API_KEYS is not set: /api/v2 is open because MOCK_MODE is on")
    elif not keys:
        logger.warning("API_KEYS is not set: every /api/v2 route will answer 503 until it is")


async def require_api_key(request: Request,
                          x_api_key: Optional[str] = Header(None, description="API key"),
                          authorization: Optional[str] = Header(None, include_in_schema=False)) -> Optional[str]:
    """FastAPI dependency on every /api/v2 router. Returns the key's name."""
    from config import settings

    keys = parse_keys(settings.api_keys)
    if not keys:
        if settings.mock_mode:
            return None
        raise ApiAuthError("auth_not_configured",
                           "API_KEYS is not set on this server, so the API refuses all requests.", 503)

    offered = x_api_key
    if offered is None and authorization and authorization.lower().startswith("bearer "):
        offered = authorization[7:].strip()
    if not offered:
        raise ApiAuthError("unauthorized", "Send an API key in the X-API-Key header.", 401)

    match = None
    for name, key in keys.items():  # compare against every key, so timing does not tell which was close
        if hmac.compare_digest(offered.encode(), key.encode()):
            match = name
    if match is None:
        logger.warning(f"Rejected an API key for {request.method} {request.url.path}")
        raise ApiAuthError("unauthorized", "The API key is not valid.", 401)
    request.state.api_key_name = match
    logger.info(f"{request.method} {request.url.path} by API key '{match}'")
    return match

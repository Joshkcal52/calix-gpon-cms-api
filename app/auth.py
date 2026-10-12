"""
API keys for every /api/v2 route.

    API_KEYS="portal:3f9c...:read+change,noc:a71b..."   # name:key[:scopes] entries, comma-separated
    API_KEYS="3f9c..."                                  # or bare keys

Clients send the key as `X-API-Key: <key>` or `Authorization: Bearer <key>`.
The key's name (or "key-1", "key-2", ... for bare keys) is logged with each
request and stored on queued jobs' log lines, never the key itself.

Scopes limit what a key may do, so a key leaked from a portal that only
signs customers up cannot delete ONTs:

  read     every GET, and the POSTs that only read (status-check,
           snapshot-compare, SMx plans)
  change   every other POST and PATCH: signups, cutovers, enable/disable,
           changes
  delete   every DELETE: services and ONTs

A key listed without scopes may do all three, with a warning at startup.

Changes are also rate limited per key and per API process
(WRITE_REQUESTS_PER_MINUTE, DELETES_PER_HOUR); over a limit the request is
answered 429 and nothing is queued.

When API_KEYS is not set:
  - with MOCK_MODE=true the routes are open, with a warning at startup, so
    `MOCK_MODE=true python main.py` works as before;
  - otherwise every /api/v2 route answers 503 auth_not_configured. A real
    deployment cannot be left open by forgetting a variable.

/health and / are not covered, so probes need no key.
Browsers' EventSource cannot set headers, so a browser following a job's SSE
stream needs a server-side proxy that adds the key.
"""

import hmac
import logging
import time
from collections import defaultdict, deque
from typing import Deque, Dict, FrozenSet, Optional, Tuple

from fastapi import Header, Request

logger = logging.getLogger(__name__)

READ, CHANGE, DELETE = "read", "change", "delete"
SCOPES: FrozenSet[str] = frozenset({READ, CHANGE, DELETE})

# POST routes that change nothing, so a read-only key may call them
READ_ONLY_POSTS = frozenset({
    "/api/v2/networks/{network}/onts/status-check",
    "/api/v2/networks/{network}/snapshot-compare",
    "/api/v2/smx/plans",
})


class ApiAuthError(Exception):
    def __init__(self, code: str, message: str, http_status: int):
        super().__init__(message)
        self.code, self.message, self.http_status = code, message, http_status

    def to_dict(self) -> Dict[str, object]:
        return {"code": self.code, "message": self.message, "retryable": self.http_status == 429}


def parse_entries(raw: Optional[str]) -> Dict[str, Tuple[str, Optional[FrozenSet[str]]]]:
    """'portal:abc:read,def' -> {'portal': ('abc', {'read'}), 'key-2': ('def', None)}. None: no scopes listed."""
    entries: Dict[str, Tuple[str, Optional[FrozenSet[str]]]] = {}
    for position, entry in enumerate((raw or "").split(","), start=1):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) == 1:
            name, key, scopes = f"key-{position}", parts[0], None
        elif len(parts) in (2, 3):
            name, key = parts[0], parts[1]
            scopes = frozenset(s.strip() for s in parts[2].split("+") if s.strip()) if len(parts) == 3 else None
        else:
            raise ValueError(f"API key entry {position} has more than two ':'; use name:key or name:key:scopes")
        name, key = name.strip(), key.strip()
        if len(key) < 16:
            raise ValueError(f"API key '{name}' is shorter than 16 characters")
        if name in entries:
            raise ValueError(f"API key name '{name}' is used twice")
        if scopes is not None and (not scopes or scopes - SCOPES):
            raise ValueError(f"API key '{name}' has scopes '{parts[2]}'; use read, change and/or delete joined by '+'")
        entries[name] = (key, scopes)
    return entries


def parse_keys(raw: Optional[str]) -> Dict[str, str]:
    """'portal:abc,def' -> {'portal': 'abc', 'key-2': 'def'}"""
    return {name: key for name, (key, _) in parse_entries(raw).items()}


def check_configuration(settings) -> None:
    """At startup: refuse malformed keys, warn when the API is open or a key may do everything"""
    entries = parse_entries(settings.api_keys)
    if not entries and settings.mock_mode:
        logger.warning("API_KEYS is not set: /api/v2 is open because MOCK_MODE is on")
    elif not entries:
        logger.warning("API_KEYS is not set: every /api/v2 route will answer 503 until it is")
    unscoped = sorted(name for name, (_, scopes) in entries.items() if scopes is None)
    if unscoped:
        logger.warning(f"API key(s) {', '.join(unscoped)} have no scopes, so they may read, change and delete. "
                       f"Give each key only what it needs: name:key:read+change")


def required_scope(request: Request) -> str:
    """What a request needs: read, change or delete"""
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return READ
    if request.method == "DELETE":
        return DELETE
    route = request.scope.get("route")
    if request.method == "POST" and getattr(route, "path", None) in READ_ONLY_POSTS:
        return READ
    return CHANGE


class WriteLimiter:
    """
    Per key, in one API process: at most per_minute changes a minute and per_hour deletes an hour
    (0 turns a limit off). Counted when a change is let through, whether or not it later succeeds.
    """

    def __init__(self, per_minute: int, deletes_per_hour: int, clock=time.monotonic):
        self.per_minute, self.deletes_per_hour, self._clock = per_minute, deletes_per_hour, clock
        self._writes: Dict[str, Deque[float]] = defaultdict(deque)
        self._deletes: Dict[str, Deque[float]] = defaultdict(deque)

    def check(self, caller: str, scope: str) -> None:
        now = self._clock()
        if scope == READ:
            return
        self._take(self._writes[caller], self.per_minute, 60.0, now,
                   f"more than {self.per_minute} changes a minute")
        if scope == DELETE:
            try:
                self._take(self._deletes[caller], self.deletes_per_hour, 3600.0, now,
                           f"more than {self.deletes_per_hour} deletes an hour")
            except ApiAuthError:
                self._writes[caller].pop()  # the refused delete does not count as a change either
                raise

    @staticmethod
    def _take(window: Deque[float], limit: int, seconds: float, now: float, what: str) -> None:
        if limit <= 0:
            return
        while window and now - window[0] >= seconds:
            window.popleft()
        if len(window) >= limit:
            raise ApiAuthError("rate_limited", f"This API key has sent {what}, so nothing was queued. "
                                               f"Wait and send it again, or check what is sending them.", 429)
        window.append(now)


async def require_api_key(request: Request,
                          x_api_key: Optional[str] = Header(None, description="API key"),
                          authorization: Optional[str] = Header(None, include_in_schema=False)) -> Optional[str]:
    """FastAPI dependency on every /api/v2 router. Returns the key's name."""
    from config import settings

    entries = parse_entries(settings.api_keys)
    scope = required_scope(request)
    if not entries:
        if settings.mock_mode:
            _limit(request, "(open)", scope)
            return None
        raise ApiAuthError("auth_not_configured",
                           "API_KEYS is not set on this server, so the API refuses all requests.", 503)

    offered = x_api_key
    if offered is None and authorization and authorization.lower().startswith("bearer "):
        offered = authorization[7:].strip()
    if not offered:
        raise ApiAuthError("unauthorized", "Send an API key in the X-API-Key header.", 401)

    match = None
    for name, (key, _) in entries.items():  # compare against every key, so timing does not tell which was close
        if hmac.compare_digest(offered.encode(), key.encode()):
            match = name
    if match is None:
        logger.warning(f"Rejected an API key for {request.method} {request.url.path}")
        raise ApiAuthError("unauthorized", "The API key is not valid.", 401)
    scopes = entries[match][1]
    if scopes is not None and scope not in scopes:
        logger.warning(f"API key '{match}' ({'+'.join(sorted(scopes))}) refused {request.method} {request.url.path}")
        raise ApiAuthError("forbidden", f"This API key may not {scope} (its scopes: {'+'.join(sorted(scopes))}).", 403)
    _limit(request, match, scope)
    request.state.api_key_name = match
    logger.info(f"{request.method} {request.url.path} by API key '{match}'")
    return match


def _limit(request: Request, caller: str, scope: str) -> None:
    limiter: Optional[WriteLimiter] = getattr(request.app.state, "limiter", None)
    if limiter is not None:
        limiter.check(caller, scope)

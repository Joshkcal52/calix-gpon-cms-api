"""
Audit trail: who asked for each change, and how it ended.

Every change is a queued job, and every job carries `requested_by`: the name
of the API key that asked for it (the part before the colon in API_KEYS, such
as "billing" or "noc"), or, for an event written straight to the
legacy-gpon-events topic, whatever its producer put in `requested_by`.
Producers on that topic are trusted to name themselves; the API never lets a
client set the field.

One JSON line is written per audit event, to the "gpon_cms.audit" logger:

  accepted   the API put the change on the queue (written by the API)
  succeeded  a worker finished it (written by the worker)
  failed     a worker gave up on it for good (written by the worker)

Those lines go wherever the process's logs go. Job history in the API only
lasts as long as the updates topic keeps its messages, so for a lasting record
either ship the logs somewhere that keeps them, or set AUDIT_LOG_PATH: each
API process and worker then also appends its lines to that file (use one path
per process, or a shared volume that supports appends).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

AUDIT = logging.getLogger("gpon_cms.audit")
ANONYMOUS = "anonymous (MOCK_MODE without API_KEYS)"
TOPIC_UNNAMED = "events topic (the producer did not say)"

_configured_path: Optional[str] = None


def configure(path: Optional[str]) -> None:
    """Also append audit lines to `path` (AUDIT_LOG_PATH). Safe to call more than once."""
    global _configured_path
    AUDIT.setLevel(logging.INFO)
    if not path or path == _configured_path:
        return
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    AUDIT.addHandler(handler)
    _configured_path = path


def caller(request: Any) -> str:
    """The name of the API key that made this request (set by app.auth.require_api_key)"""
    return getattr(request.state, "api_key_name", None) or ANONYMOUS


def record(event: str, command: Any, **fields: Any) -> None:
    payload = command.payload if isinstance(command.payload, dict) else {}
    line = {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event,
            "job_id": command.id, "requested_by": command.requested_by, "kind": command.kind,
            "action": command.action, "network": payload.get("network") or command.key,
            "summary": command.summary, **fields}
    AUDIT.info(json.dumps(line, default=str))

"""
Legacy topic wire format.

The legacy-gpon-events topic carries plain event JSON (see LegacyGponEvent),
so any producer can write to it: this API, a Sonar webhook bridge, a script.
Inside the app each event travels as a Command, like every other queued job.

Producers should use the network name as the Kafka message key, so one
network's events stay in order.
"""

import json

from app.legacy.models import GponSignupEvent, LegacyGponEventAdapter, XgsCutoverEvent
from app.models.job_model import Command

LANE_LEGACY = "legacy"
ACTION_GPON_SIGNUP = "GPON_SIGNUP"
ACTION_XGS_CUTOVER = "XGS_CUTOVER"


def event_to_command(event) -> Command:
    if isinstance(event, GponSignupEvent):
        kind = "signup"
        summary = f"Provision GPON ONT {event.serial_number} for subscriber {event.subscriber_id} on {event.network}"
    elif isinstance(event, XgsCutoverEvent):
        kind = "cutover"
        summary = f"Hand subscriber {event.subscriber_id} on {event.network} over to XGS"
    else:
        raise TypeError(f"Not a legacy GPON event: {type(event).__name__}")
    return Command(id=event.event_id, action=event.type, lane=LANE_LEGACY, key=event.network,
                   kind=kind, summary=summary, payload=event.model_dump(mode="json"),
                   created_at=event.occurred_at)


def encode(command: Command) -> bytes:
    """Command -> the event JSON that goes on the topic"""
    return json.dumps(command.payload).encode()


def decode(raw: bytes) -> Command:
    """Event JSON from the topic -> Command. Raises ValueError if it is not a valid event."""
    return event_to_command(LegacyGponEventAdapter.validate_json(raw))

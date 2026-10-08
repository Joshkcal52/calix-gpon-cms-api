"""
Legacy GPON Handlers
What the worker does with each event from the legacy-gpon-events topic.

  GPON_SIGNUP   Mode A. Provision the subscriber's ONT and services on CMS.
  XGS_CUTOVER   Mode B. Read the subscriber's ONT and services from CMS,
                translate them, and publish a migration request for the XGS
                side. CMS is only read; the legacy ONT is left in service.

Both are safe to run twice, which the at-least-once queue requires: signup
checks what already exists before changing anything, and a repeated cutover
publishes the same request with the same migration_id.
"""

import logging
from typing import Any, Dict

from app.controller.provisioning_controller import activate_ont
from app.legacy.events import ACTION_GPON_SIGNUP, ACTION_XGS_CUTOVER
from app.legacy.models import (
    GponSignupEvent, LegacySubscriberRecord, ProfileMap, TranslationError, XgsCutoverEvent, translate
)
from app.messaging.bus import MessageBus
from app.models.provisioning_model import ActivationRequest
from app.nbi.errors import NbiConflictError, NbiNotFoundError
from app.nbi.gateway import NbiGateway
from app.worker.runner import Handler, JobContext, JobError

logger = logging.getLogger(__name__)


def build_legacy_handlers(settings: Any, gateway: NbiGateway, bus: MessageBus) -> Dict[str, Handler]:
    profile_map = ProfileMap.load(settings.legacy_profile_map_path)  # fail at startup if it is broken
    logger.info(f"Legacy profile map: {len(profile_map.bandwidth_profiles)} bandwidth profiles, "
                f"{len(profile_map.tag_action_vlans)} tag actions from {settings.legacy_profile_map_path}")

    async def handle_signup(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = GponSignupEvent.model_validate(payload)
        request = ActivationRequest(
            serial_number=event.serial_number, ont_id=event.ont_id, profile=event.profile,
            description=event.description, subscriber_id=event.subscriber_id, services=event.services,
            ports=event.ports,
            require_discovery=False,  # a signup can arrive before the ONT is installed
            wait_for_online=event.wait_for_online)
        result = await activate_ont(
            context, gateway, event.network, request,
            verify_timeout=settings.activation_verify_timeout_seconds,
            poll_seconds=settings.activation_poll_seconds,
            bandwidth_profiles=settings.known_bandwidth_profiles())
        return {**result, "subscriber_id": event.subscriber_id}

    async def handle_cutover(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = XgsCutoverEvent.model_validate(payload)
        network, subscriber = event.network, event.subscriber_id

        await context.step(1, 3, f"Looking up subscriber {subscriber} on {network}")
        matches = await gateway.find_onts_by_subscriber(network, subscriber)
        if event.ont_id is not None:
            matches = [ont for ont in matches if ont.ont_id == event.ont_id]
        if not matches:
            which = f"ONT {event.ont_id} for subscriber {subscriber}" if event.ont_id else f"Subscriber {subscriber}"
            raise NbiNotFoundError(f"{which} was not found on {network}.")
        if len(matches) > 1:
            ids = ", ".join(str(ont.ont_id) for ont in matches)
            raise NbiConflictError(
                f"Subscriber {subscriber} has {len(matches)} ONTs on {network} ({ids}). "
                f"Send the cutover again with ont_id set to the one to migrate.")

        ont = await gateway.get_ont(network, matches[0].ont_id)  # with its services
        record = LegacySubscriberRecord.from_ont(network, subscriber, ont)
        await context.emit("log", f"Found ONT {record.ont_id} ({record.serial_number}) with "
                                  f"{len(record.services)} service(s)",
                           ont_id=record.ont_id, serial_number=record.serial_number)

        await context.step(2, 3, "Translating legacy profiles and VLANs")
        try:
            request = translate(record, event, profile_map)
        except TranslationError as e:
            raise JobError("untranslatable",
                           f"Subscriber {subscriber} cannot be handed over: {e}. Nothing was sent.",
                           data={"problems": e.problems}) from None

        topic = settings.xgs_migration_topic
        await context.step(3, 3, f"Publishing the migration request to {topic}")
        await bus.publish_event(topic, key=subscriber, value=request.model_dump(mode="json"))

        return {"migration_id": request.migration_id, "subscriber_id": subscriber, "network": network,
                "legacy_ont_id": record.ont_id, "services": len(request.services), "topic": topic}

    return {ACTION_GPON_SIGNUP: handle_signup, ACTION_XGS_CUTOVER: handle_cutover}

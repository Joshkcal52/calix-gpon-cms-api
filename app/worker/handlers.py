"""
Command Handlers
Maps each queued action to the code that carries it out. Handlers receive the
payload exactly as it came off the queue and validate it again before use.
"""

from typing import Any, Dict

from app.controller.lifecycle_controller import apply_ont_change
from app.controller.provisioning_controller import activate_ont
from app.legacy.handlers import build_legacy_handlers
from app.messaging.bus import MessageBus
from app.models.lifecycle_model import ACTION_CHANGE_ONT, OntChangeAdapter
from app.models.job_model import ACTION_ACTIVATE_ONT
from app.models.provisioning_model import ActivationRequest
from app.nbi.gateway import NbiGateway
from app.worker.runner import Handler, JobContext


def build_handlers(settings: Any, gateway: NbiGateway, bus: MessageBus) -> Dict[str, Handler]:

    async def handle_activate_ont(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        request = ActivationRequest.model_validate(payload["request"])
        return await activate_ont(
            context, gateway, payload["network"], request,
            verify_timeout=settings.activation_verify_timeout_seconds,
            poll_seconds=settings.activation_poll_seconds)

    async def handle_change_ont(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        change = OntChangeAdapter.validate_python(payload["change"])
        return await apply_ont_change(context, gateway, payload["network"], int(payload["ont_id"]), change)

    return {
        ACTION_ACTIVATE_ONT: handle_activate_ont,
        ACTION_CHANGE_ONT: handle_change_ont,
        **build_legacy_handlers(settings, gateway, bus),
    }

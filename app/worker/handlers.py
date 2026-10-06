"""
Command Handlers
Maps each queued action to the code that carries it out. Handlers receive the
payload exactly as it came off the queue and validate it again before use.
"""

from typing import Any, Dict

from app.controller.automation_controller import PlaybookRunner, run_playbook
from app.controller.provisioning_controller import activate_ont
from app.legacy.handlers import build_legacy_handlers
from app.messaging.bus import MessageBus
from app.models.job_model import ACTION_ACTIVATE_ONT, ACTION_RUN_PLAYBOOK
from app.models.provisioning_model import ActivationRequest
from app.nbi.gateway import NbiGateway
from app.worker.runner import Handler, JobContext


def build_handlers(settings: Any, gateway: NbiGateway, playbook_runner: PlaybookRunner,
                   bus: MessageBus) -> Dict[str, Handler]:

    async def handle_activate_ont(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        request = ActivationRequest.model_validate(payload["request"])
        return await activate_ont(
            context, gateway, payload["network"], request,
            verify_timeout=settings.activation_verify_timeout_seconds,
            poll_seconds=settings.activation_poll_seconds)

    async def handle_run_playbook(context: JobContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await run_playbook(context, playbook_runner, payload, api_url=settings.api_base_url)

    return {
        ACTION_ACTIVATE_ONT: handle_activate_ont,
        ACTION_RUN_PLAYBOOK: handle_run_playbook,
        **build_legacy_handlers(settings, gateway, bus),
    }

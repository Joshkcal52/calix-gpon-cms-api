"""
Provisioning Service
ONT operations that go through the NBI gateway: quick lookups that answer
within the request, and the multi-step activation that a worker runs.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List

from app.models.provisioning_model import (
    ActivationRequest, OntSpec, OperState, StatusCheckItem, StatusCheckResult
)
from app.nbi.errors import NbiConflictError, NbiError, NbiNotFoundError
from app.nbi.gateway import NbiGateway
from app.worker.runner import JobContext

logger = logging.getLogger(__name__)


async def check_ont_status(gateway: NbiGateway, network: str, ont_ids: List[int]) -> StatusCheckResult:
    """
    Read many ONTs at once. The calls run in parallel (the gateway caps how
    many hit CMS at a time) and one bad ONT does not fail the whole check.
    """
    unique_ids = list(dict.fromkeys(ont_ids))

    async def check(ont_id: int) -> StatusCheckItem:
        try:
            return StatusCheckItem(ont_id=ont_id, ont=await gateway.get_ont(network, ont_id))
        except NbiError as e:
            return StatusCheckItem(ont_id=ont_id, error=e.to_dict())

    results = await asyncio.gather(*(check(ont_id) for ont_id in unique_ids))
    return StatusCheckResult(
        network=network,
        checked=len(results),
        online=sum(1 for r in results if r.ont and r.ont.oper_state == OperState.ONLINE),
        failed=sum(1 for r in results if r.error),
        results=list(results))


async def activate_ont(context: JobContext, gateway: NbiGateway, network: str,
                       request: ActivationRequest, *, verify_timeout: float,
                       poll_seconds: float) -> Dict[str, Any]:
    """
    Activate a discovered ONT. Each step is reported as a job event.

    1. Find the ONT: already provisioned, or discovered on a PON
    2. Create the ONT (skipped if it already exists)
    3. Bind each Ethernet service (VLAN) that is not bound yet
    4. Wait for the ONT to come online

    Safe to run again: every step first checks what is already in place, so a
    command that is delivered twice (worker crash, CMS timeout) picks up where
    the first run stopped instead of failing on "already exists".

    If CMS rejects a service, the ONT created by this run is deleted again
    (unless rollback_on_failure is false), so a failed activation leaves
    nothing half-provisioned. An ONT this run did not create is never deleted.
    A provisioned ONT that is slow to come online is left in place.
    """
    total_steps = 4
    serial = request.serial_number

    await context.step(1, total_steps, f"Looking for ONT {serial} on {network}")
    ont = await gateway.find_ont_by_serial(network, serial)
    created_here = False

    if ont is not None:
        if request.ont_id is not None and ont.ont_id != request.ont_id:
            raise NbiConflictError(
                f"Serial number {serial} is already provisioned as ONT {ont.ont_id} on {network}, "
                f"not ONT {request.ont_id}.")
        if ont.profile is not None and ont.profile != request.profile:
            raise NbiConflictError(
                f"Serial number {serial} is already provisioned as ONT {ont.ont_id} with profile "
                f"'{ont.profile}', not '{request.profile}'.")
        await context.step(2, total_steps, f"ONT {serial} already exists as ONT {ont.ont_id}; not creating it again")
    else:
        discovered = await gateway.list_discovered_onts(network)
        match = next((d for d in discovered if d.serial_number == serial), None)
        if match is None:
            raise NbiNotFoundError(
                f"ONT {serial} has not been discovered on {network}. "
                f"Check that it is connected and powered.")
        await context.emit("log", f"Found {match.model} on GPON port {match.pon_port}",
                           model=match.model, pon_port=match.pon_port)

        await context.step(2, total_steps, f"Creating ONT {serial}")
        spec = OntSpec(**request.model_dump(include=set(OntSpec.model_fields)))
        ont = await gateway.create_ont(network, spec)
        created_here = True
        await context.emit("log", f"Created ONT {ont.ont_id}", ont_id=ont.ont_id)

    ont_id = ont.ont_id
    bound = {(s.vlan, s.ont_port) for s in ont.services}
    missing = [s for s in request.services if (s.vlan, s.ont_port) not in bound]

    await context.step(3, total_steps, f"Binding {len(missing)} service(s) to ONT {ont_id}")
    try:
        # One at a time: parallel edits to the same ONT are not worth the risk
        for service in missing:
            await gateway.add_ethernet_service(network, ont_id, service)
            await context.emit("log", f"Bound VLAN {service.vlan} to ONT port {service.ont_port}",
                               vlan=service.vlan, ont_port=service.ont_port)
    except NbiError as e:
        # Roll back only when CMS said no. If CMS is merely unreachable the
        # command is retried and carries on from here.
        if not e.retryable and created_here and request.rollback_on_failure:
            await _roll_back(context, gateway, network, ont_id)
        raise

    await context.step(4, total_steps, f"Waiting for ONT {ont_id} to come online")
    deadline = time.monotonic() + verify_timeout
    while True:
        ont = await gateway.get_ont(network, ont_id)
        if ont.oper_state == OperState.ONLINE or time.monotonic() >= deadline:
            break
        await asyncio.sleep(poll_seconds)

    online = ont.oper_state == OperState.ONLINE
    if not online:
        await context.emit("warning",
                           f"ONT {ont_id} is provisioned but did not come online within "
                           f"{verify_timeout:g} seconds. It has been left in place.", ont_id=ont_id)

    return {"network": network, "ont_id": ont_id, "serial_number": serial,
            "online": online, "services": len(ont.services)}


async def _roll_back(context: JobContext, gateway: NbiGateway, network: str, ont_id: int) -> None:
    await context.emit("warning", f"CMS rejected the activation; rolling back by deleting ONT {ont_id}",
                       ont_id=ont_id)
    try:
        await gateway.delete_ont(network, ont_id)
        await context.emit("log", f"Rolled back: ONT {ont_id} deleted", ont_id=ont_id)
    except NbiError as e:
        logger.error(f"Rollback of ONT {ont_id} on {network} failed: {e.message}")
        await context.emit("warning",
                           f"Rollback failed: ONT {ont_id} could not be deleted ({e.message}) "
                           f"and needs manual cleanup.", ont_id=ont_id, rollback_failed=True)

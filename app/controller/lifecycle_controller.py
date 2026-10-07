"""
Lifecycle Service
Applies one change to an existing ONT (see app/models/lifecycle_model.py).

Each change reads CMS first and sends only what is still different, so a
command delivered twice (worker crash, timeout after CMS applied it) finishes
cleanly the second time instead of failing or doing it again. Each change is
a single CMS write, so there is nothing to undo if it fails.
"""

import logging
from typing import Any, Dict

from app.controller.provisioning_controller import port_differences
from app.models.lifecycle_model import DeleteOnt, DeleteService, OntChange, UpdatePort, UpdateService
from app.models.provisioning_model import Attachment, PortChange, ServiceChange, parse_port
from app.nbi.errors import NbiConflictError, NbiNotFoundError, NbiRejectedError
from app.nbi.gateway import NbiGateway
from app.worker.runner import JobContext

logger = logging.getLogger(__name__)


async def apply_ont_change(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                           change: OntChange) -> Dict[str, Any]:
    base = {"network": network, "ont_id": ont_id, "op": change.op}
    if isinstance(change, UpdatePort):
        return {**base, **await _update_port(context, gateway, network, ont_id, change)}
    if isinstance(change, UpdateService):
        return {**base, **await _update_service(context, gateway, network, ont_id, change)}
    if isinstance(change, DeleteService):
        return {**base, **await _delete_service(context, gateway, network, ont_id, change)}
    return {**base, **await _delete_ont(context, gateway, network, ont_id, change)}


async def _update_port(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                       change: UpdatePort) -> Dict[str, Any]:
    attachment, number = parse_port(change.port)
    wanted = change.change.model_dump(exclude_none=True)
    if attachment == Attachment.ROUTER and set(wanted) - {"admin_state"}:
        raise NbiRejectedError("Only the admin state of a router port can be changed.")

    await context.step(1, 2, f"Reading port {change.port} of ONT {ont_id}")
    ports = await gateway.get_ont_ports(network, ont_id)
    if attachment == Attachment.ROUTER:
        if ports.router.present is False:
            raise NbiRejectedError(f"ONT {ont_id} (profile {ports.profile}) has no router.")
        current = next((p for p in ports.router.ports if p.number == number), None)
    else:
        current = next((p for p in ports.ethernet if p.number == number), None)
        if current is None:
            raise NbiNotFoundError(f"ONT {ont_id} (profile {ports.profile}) has no Ethernet port {number}.")

    differences = port_differences(current, wanted)
    if not differences:
        await context.step(2, 2, f"Port {change.port} already matches; nothing sent")
        return {"port": change.port, "changed": False, "applied": {}}
    await context.step(2, 2, f"Changing port {change.port}: " + ", ".join(f"{k} {v}" for k, v in differences.items()))
    await gateway.update_port(network, ont_id, attachment, number, PortChange(**differences))
    return {"port": change.port, "changed": True, "applied": differences}


async def _update_service(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                          change: UpdateService) -> Dict[str, Any]:
    attachment, number = parse_port(change.port)
    label = f"service {change.service_index} on port {change.port} of ONT {ont_id}"

    await context.step(1, 2, f"Reading {label}")
    service = (await gateway.get_ont(network, ont_id)).service_at(attachment, number, change.service_index)
    if service is None:
        raise NbiNotFoundError(f"There is no {label} on {network}.")

    wanted = change.change.model_dump(exclude_none=True)
    differences = {k: v for k, v in wanted.items() if getattr(service, k) != v}
    if not differences:
        await context.step(2, 2, f"The {label} already matches; nothing sent")
        return {"port": change.port, "service_index": change.service_index, "changed": False, "applied": {}}
    await context.step(2, 2, f"Changing {label}: " + ", ".join(f"{k} {v}" for k, v in differences.items()))
    await gateway.update_service(network, ont_id, attachment, number, change.service_index,
                                 ServiceChange(**differences))
    return {"port": change.port, "service_index": change.service_index, "changed": True, "applied": differences}


async def _delete_service(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                          change: DeleteService) -> Dict[str, Any]:
    attachment, number = parse_port(change.port)
    label = f"service {change.service_index} on port {change.port} of ONT {ont_id}"

    await context.step(1, 2, f"Reading {label}")
    service = (await gateway.get_ont(network, ont_id)).service_at(attachment, number, change.service_index)
    result = {"port": change.port, "service_index": change.service_index}
    if service is None:
        await context.step(2, 2, f"The {label} does not exist; nothing to delete")
        return {**result, "deleted": False, "already_absent": True}
    if change.expect_vlan is not None and service.vlan != change.expect_vlan:
        raise NbiConflictError(f"The {label} is on VLAN {service.vlan}, not {change.expect_vlan}. Not deleted.")

    await context.step(2, 2, f"Deleting {label} (VLAN {service.vlan})")
    await gateway.delete_service(network, ont_id, attachment, number, change.service_index)
    return {**result, "deleted": True, "vlan": service.vlan}


async def _delete_ont(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                      change: DeleteOnt) -> Dict[str, Any]:
    await context.step(1, 2, f"Reading ONT {ont_id}")
    try:
        ont = await gateway.get_ont(network, ont_id)
    except NbiNotFoundError:
        # Gone already, or never there. Make sure the serial is not on another ONT ID
        # (this also fails if the network itself does not exist).
        elsewhere = await gateway.find_ont_by_serial(network, change.serial_number)
        if elsewhere is not None:
            raise NbiConflictError(f"Serial number {change.serial_number} is ONT {elsewhere.ont_id} on {network}, "
                                   f"not ONT {ont_id}. Nothing was deleted.") from None
        await context.step(2, 2, f"ONT {ont_id} does not exist; nothing to delete")
        return {"deleted": False, "already_absent": True, "serial_number": change.serial_number}

    if ont.serial_number != change.serial_number:
        raise NbiConflictError(f"ONT {ont_id} on {network} has serial number {ont.serial_number}, not "
                               f"{change.serial_number}. Nothing was deleted.")
    if ont.services and not change.force:
        listed = ", ".join(f"{s.attachment.value} port {s.ont_port} service {s.service_index} (VLAN {s.vlan})"
                           for s in ont.services)
        raise NbiConflictError(f"ONT {ont_id} still has {len(ont.services)} service(s): {listed}. Delete them "
                               f"first, or send force=true to delete them with the ONT.")

    await context.step(2, 2, f"Deleting ONT {ont_id} ({ont.serial_number}) and {len(ont.services)} service(s)")
    await gateway.delete_ont(network, ont_id)
    return {"deleted": True, "serial_number": ont.serial_number, "services_removed": len(ont.services)}

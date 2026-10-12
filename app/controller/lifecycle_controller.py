"""
Lifecycle Service
Applies one change to an existing ONT (see app/models/lifecycle_model.py).

Each change reads CMS first and sends only what is still different, so a
command delivered twice (worker crash, timeout after CMS applied it) finishes
cleanly the second time instead of failing or doing it again. Each change is
a single CMS write, so there is nothing to undo if it fails.

CMS's GUI and the E7's CLI change the same ONTs, and ONT IDs and service
numbers are reused, so nothing here trusts a read for longer than it must:
every change says which ONT it means (serial_number) and every service change
which service (expect_vlan, or "none" for a service without an outer VLAN), an ONT delete checks each service again just
before deleting it, and every write is read back. A write CMS accepted but
does not show fails the job (not_applied) rather than reporting success.
"""

import logging
from typing import Any, Dict, FrozenSet, List, Optional

from app.controller.provisioning_controller import check_bandwidth_profiles, port_differences
from app.models.lifecycle_model import DeleteOnt, DeleteService, OntChange, UpdatePort, UpdateService
from app.models.provisioning_model import Attachment, OntState, PortChange, ProvisionedService, ServiceChange, parse_port
from app.nbi.errors import NbiConflictError, NbiError, NbiNotFoundError, NbiRejectedError, NbiVerifyError
from app.nbi.gateway import NbiGateway
from app.worker.runner import JobContext

logger = logging.getLogger(__name__)

# Peak-rate overrides read back as None or "none" for no override
_NO_OVERRIDE = {"us_pir_override", "ds_pir_override"}


async def apply_ont_change(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                           change: OntChange, *, bandwidth_profiles: Optional[FrozenSet[str]] = None) -> Dict[str, Any]:
    base = {"network": network, "ont_id": ont_id, "op": change.op}
    if change.dry_run:
        base["dry_run"] = True
    if isinstance(change, UpdateService):
        check_bandwidth_profiles([change.change.bandwidth_profile], bandwidth_profiles)
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
    _check_serial(await gateway.get_ont(network, ont_id), change.serial_number, "changed")
    current = await _read_port(gateway, network, ont_id, attachment, number)

    differences = port_differences(current, wanted)
    if not differences:
        await context.step(2, 2, f"Port {change.port} already matches; nothing sent")
        return {"port": change.port, "changed": False, "applied": {}}
    if change.dry_run:
        await context.step(2, 2, f"Dry run: would change port {change.port}: {_describe(differences)}")
        return {"port": change.port, "changed": False, "would_apply": differences}
    await context.step(2, 2, f"Changing port {change.port}: {_describe(differences)}")
    await gateway.update_port(network, ont_id, attachment, number, PortChange(**differences))

    after = port_differences(await _read_port(gateway, network, ont_id, attachment, number), differences)
    if after:
        raise NbiVerifyError(f"CMS accepted the change to port {change.port} of ONT {ont_id}, but reading it back "
                             f"still shows it differs in: {', '.join(after)}. Check the port in CMS.")
    return {"port": change.port, "changed": True, "applied": differences, "verified": True}


async def _read_port(gateway: NbiGateway, network: str, ont_id: int, attachment: Attachment, number: int):
    ports = await gateway.get_ont_ports(network, ont_id)
    if attachment == Attachment.ROUTER:
        if ports.router.present is False:
            raise NbiRejectedError(f"ONT {ont_id} (profile {ports.profile}) has no router.")
        return next((p for p in ports.router.ports if p.number == number), None)
    current = next((p for p in ports.ethernet if p.number == number), None)
    if current is None:
        raise NbiNotFoundError(f"ONT {ont_id} (profile {ports.profile}) has no Ethernet port {number}.")
    return current


async def _update_service(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                          change: UpdateService) -> Dict[str, Any]:
    attachment, number = parse_port(change.port)
    label = f"service {change.service_index} on port {change.port} of ONT {ont_id}"

    await context.step(1, 2, f"Reading {label}")
    ont = await gateway.get_ont(network, ont_id)
    _check_serial(ont, change.serial_number, "changed")
    service = ont.service_at(attachment, number, change.service_index)
    if service is None:
        raise NbiNotFoundError(f"There is no {label} on {network}.")
    wanted = change.change.model_dump(exclude_none=True)
    differences = _service_differences(service, wanted)
    if not differences:
        # Nothing to send. Checked before the VLAN, so a repeat of a VLAN change, which no longer finds the
        # old VLAN, finishes cleanly
        await context.step(2, 2, f"The {label} already matches; nothing sent")
        return {"port": change.port, "service_index": change.service_index, "changed": False, "applied": {}}
    _check_vlan(service, change.expect_vlan, label, "changed")
    if change.dry_run:
        await context.step(2, 2, f"Dry run: would change {label}: {_describe(differences)}")
        return {"port": change.port, "service_index": change.service_index, "changed": False,
                "would_apply": differences}
    await context.step(2, 2, f"Changing {label}: {_describe(differences)}")
    await gateway.update_service(network, ont_id, attachment, number, change.service_index,
                                 ServiceChange(**differences))

    after = (await gateway.get_ont(network, ont_id)).service_at(attachment, number, change.service_index)
    left = list(differences) if after is None else list(_service_differences(after, differences))
    if left:
        raise NbiVerifyError(f"CMS accepted the change to {label}, but reading it back "
                             + ("finds no such service." if after is None else f"still shows it differs in: "
                                                                                 f"{', '.join(left)}.")
                             + " Check the service in CMS.")
    return {"port": change.port, "service_index": change.service_index, "changed": True, "applied": differences,
            "verified": True}


async def _delete_service(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                          change: DeleteService) -> Dict[str, Any]:
    attachment, number = parse_port(change.port)
    label = f"service {change.service_index} on port {change.port} of ONT {ont_id}"

    await context.step(1, 2, f"Reading {label}")
    ont = await gateway.get_ont(network, ont_id)
    _check_serial(ont, change.serial_number, "deleted")
    service = ont.service_at(attachment, number, change.service_index)
    result = {"port": change.port, "service_index": change.service_index}
    if service is None:
        await context.step(2, 2, f"The {label} does not exist; nothing to delete")
        return {**result, "deleted": False, "already_absent": True}
    _check_vlan(service, change.expect_vlan, label, "deleted")

    if change.dry_run:
        await context.step(2, 2, f"Dry run: would delete {label} ({_vlan_text(service.vlan)})")
        return {**result, "deleted": False, "would_delete": True, "vlan": service.vlan}
    await context.step(2, 2, f"Deleting {label} ({_vlan_text(service.vlan)})")
    await gateway.delete_service(network, ont_id, attachment, number, change.service_index)

    after = (await gateway.get_ont(network, ont_id)).service_at(attachment, number, change.service_index)
    if after is not None and after.identity() == service.identity():
        raise NbiVerifyError(f"CMS accepted the delete of {label}, but reading it back still shows it "
                             f"({_vlan_text(service.vlan)}). Check the service in CMS.")
    return {**result, "deleted": True, "vlan": service.vlan, "verified": True}


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

    _check_serial(ont, change.serial_number, "deleted")
    if ont.services and not change.force:
        listed = ", ".join(_service_text(s) for s in ont.services)
        raise NbiConflictError(f"ONT {ont_id} still has {len(ont.services)} service(s): {listed}. Delete them "
                               f"first, or send force=true to delete them with the ONT.")
    if not change.voice_checked:
        await _refuse_if_voice_possible(gateway, network, ont)

    if change.dry_run:
        await context.step(2, 2, f"Dry run: would delete ONT {ont_id} ({ont.serial_number}) and "
                                 f"{len(ont.services)} service(s)")
        return {"deleted": False, "would_delete": True, "serial_number": ont.serial_number,
                "services": [_service_text(s) for s in ont.services]}

    # The services this API can see are deleted one by one, then the ONT without CMS's force. A
    # service added since the read above, or a voice service (never read here), is then still on
    # the ONT, and CMS refuses the delete instead of removing it unseen.
    await context.step(2, 2, f"Deleting ONT {ont_id} ({ont.serial_number}) and {len(ont.services)} service(s)")
    removed: List[str] = []
    try:
        for service in ont.services:
            # Service numbers are reused: check again, just before deleting, that this number still holds the
            # service read at step 1 and not one a technician has put there since
            now = (await gateway.get_ont(network, ont_id)).service_at(service.attachment, service.ont_port,
                                                                     service.service_index)
            if now is None:
                continue
            if now.identity() != service.identity():
                raise NbiConflictError(f"{_service_text(now)} changed since ONT {ont_id} was read; "
                                       f"it is not deleted, and neither is the ONT.")
            await gateway.delete_service(network, ont_id, service.attachment, service.ont_port, service.service_index)
            removed.append(_service_text(service))
            await context.emit("log", f"Deleted {_service_text(service)}")
        await gateway.delete_ont(network, ont_id, force=False)
    except NbiError as e:
        if removed:
            # Say exactly what state the failure left behind: the ONT is still there, these services are not
            e.message += (f" ONT {ont_id} was NOT deleted, but these data services already were: "
                          f"{'; '.join(removed)}. The customer has no data service on them until they are added again.")
        raise

    try:
        await gateway.get_ont(network, ont_id)
    except NbiNotFoundError:
        return {"deleted": True, "serial_number": ont.serial_number, "services_removed": len(removed),
                "verified": True}
    raise NbiVerifyError(f"CMS accepted the delete of ONT {ont_id}, but reading it back still finds it. "
                         f"Check it in CMS.")


async def _refuse_if_voice_possible(gateway: NbiGateway, network: str, ont: OntState) -> None:
    """
    This API does not read voice services, so it cannot tell whether an ONT has a working phone line.
    Refuse to delete one whose profile has voice ports, or whose profile cannot be read, until
    someone has checked and says so with voice_checked.
    """
    profile = next((p for p in await gateway.list_ont_profiles(network) if p.id == ont.profile), None)
    pots = profile.pots_ports if profile else None
    if pots == 0:
        return
    why = (f"its ONT profile '{ont.profile}' has {pots} voice port(s)" if pots is not None
           else f"the voice port count of its ONT profile '{ont.profile}' could not be read")
    raise NbiConflictError(f"ONT {ont.ont_id} may carry voice: {why}, and this API does not read voice services. "
                           f"Check in CMS that it has no voice service to keep, then send voice_checked=true. "
                           f"Nothing was deleted.")


def _check_serial(ont: OntState, expected: str, verb: str) -> None:
    """ONT IDs are reused: refuse if this ID now holds another ONT"""
    if ont.serial_number.upper() != expected.upper():
        raise NbiConflictError(f"ONT {ont.ont_id} has serial number {ont.serial_number}, not {expected}. "
                               f"Nothing was {verb}.")


def _check_vlan(service: ProvisionedService, expected: "int | str", label: str, verb: str) -> None:
    """Service numbers are reused: refuse if this number now holds a service on another VLAN. "none": no outer VLAN."""
    wanted = None if expected == "none" else expected
    if service.vlan != wanted:
        raise NbiConflictError(f"The {label} is on {_vlan_text(service.vlan)}, not {_vlan_text(wanted)}. "
                               f"Not {verb}.")


def _service_differences(service: ProvisionedService, wanted: Dict[str, Any]) -> Dict[str, Any]:
    def same(field: str, value: Any) -> bool:
        have = getattr(service, field)
        if field in _NO_OVERRIDE:
            return (have or "none") == (value or "none")
        return have == value
    return {k: v for k, v in wanted.items() if not same(k, v)}


def _describe(changes: Dict[str, Any]) -> str:
    return ", ".join(f"{k} {v}" for k, v in changes.items())


def _service_text(s: ProvisionedService) -> str:
    return f"{s.attachment.value} port {s.ont_port} service {s.service_index} ({_vlan_text(s.vlan)})"


def _vlan_text(vlan: "int | None") -> str:
    return f"VLAN {vlan}" if vlan is not None else "no outer VLAN"

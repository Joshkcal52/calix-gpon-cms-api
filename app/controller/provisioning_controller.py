"""
Provisioning Service
ONT operations that go through the NBI gateway: quick lookups that answer
within the request, and the multi-step activation that a worker runs.
"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from app.models.provisioning_model import (
    ActivationRequest, Attachment, EthernetService, OntSpec, OperState, PortChange, PortState, ProvisionedService,
    ServiceChange, StatusCheckItem, StatusCheckResult, port_name, service_identity
)
from app.messaging.bus import BusUnavailableError
from app.nbi.errors import NbiConflictError, NbiError, NbiNotFoundError, NbiRejectedError, NbiVerifyError
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


def check_bandwidth_profiles(profiles: Iterable[Optional[str]], known: Optional[FrozenSet[str]]) -> None:
    """
    Refuse bandwidth profiles that are not in BANDWIDTH_PROFILES (known is None when it is not set).
    Run before a job is queued and again by the worker, before anything is sent to CMS.
    """
    if known is None:
        return
    unknown = sorted({p for p in profiles if p is not None and p not in known})
    if unknown:
        raise NbiRejectedError(
            f"Unknown bandwidth profile(s): {', '.join(unknown)}. Allowed (BANDWIDTH_PROFILES): "
            f"{', '.join(sorted(known))}. Nothing was changed.")


async def activate_ont(context: JobContext, gateway: NbiGateway, network: str,
                       request: ActivationRequest, *, verify_timeout: float,
                       poll_seconds: float, bandwidth_profiles: Optional[FrozenSet[str]] = None) -> Dict[str, Any]:
    """
    Activate a discovered ONT. Each step is reported as a job event.

    1. Find the ONT: already provisioned, or discovered on a PON (with
       require_discovery=false an ONT can be provisioned before it is installed)
    2. Create the ONT (skipped if it already exists)
    3. Configure each Ethernet port listed in `ports` (only what differs)
    4. Bind each service that is not bound yet, on an Ethernet port or on the
       router side. A service is identified by its side, port and VLAN, or,
       without a VLAN, by its side, port and tag action. One that is already
       bound but differs (bandwidth profile, tag action, peak rates) is changed
       to match the request
    5. Wait for the ONT to come online, and check that every requested service
       reads back as asked

    CMS applies no port template through its API, so each port setting and
    each service is sent on its own.

    Safe to run again: every step first checks what is already in place, so a
    command that is delivered twice (worker crash, CMS timeout) picks up where
    the first run stopped instead of failing on "already exists".

    If CMS rejects a port setting or a service, the ONT created by this job is deleted again
    (unless rollback_on_failure is false), so a failed activation leaves
    nothing half-provisioned. An ONT this job did not create is never deleted,
    and neither is one that has picked up a service this job did not add (see
    _roll_back). A provisioned ONT that is slow to come online is left in place.

    What this job created is recorded in its history (context.record_ownership)
    as it is created, so an attempt after one that created the ONT and then
    failed for a passing reason can still roll it back. A missing record means
    "not this job's", so a write whose record the bus will not take is undone
    before the attempt fails (and is retried).
    """
    total_steps = 5
    serial = request.serial_number

    await context.step(1, total_steps, f"Looking for ONT {serial} on {network}")
    check_bandwidth_profiles((s.bandwidth_profile for s in request.services), bandwidth_profiles)
    await _check_router_services(context, gateway, network, request)
    ont = await gateway.find_ont_by_serial(network, serial)
    created_here = False  # by this job, in this attempt or an earlier one
    added: Set[Tuple[Any, ...]] = set()  # identities of the services this job bound, the only ones a rollback deletes

    if ont is not None:
        if request.ont_id is not None and ont.ont_id != request.ont_id:
            raise NbiConflictError(
                f"Serial number {serial} is already provisioned as ONT {ont.ont_id} on {network}, "
                f"not ONT {request.ont_id}.")
        if ont.profile is not None and ont.profile != request.profile:
            raise NbiConflictError(
                f"Serial number {serial} is already provisioned as ONT {ont.ont_id} with profile "
                f"'{ont.profile}', not '{request.profile}'.")
        owned = await _earlier_attempts_work(context, network, serial, ont.ont_id) if request.rollback_on_failure else None
        if owned is not None:
            created_here, added = True, owned
            await context.step(2, total_steps, f"ONT {serial} was created as ONT {ont.ont_id} by an earlier attempt "
                                               f"of this job; carrying on from there")
        else:
            await context.step(2, total_steps, f"ONT {serial} already exists as ONT {ont.ont_id}; not creating it again")
    else:
        discovered = await gateway.list_discovered_onts(network)
        match = next((d for d in discovered if d.serial_number == serial), None)
        if match is not None:
            await context.emit("log", f"Found {match.model} on GPON port {match.pon_port}",
                               model=match.model, pon_port=match.pon_port)
        elif request.require_discovery:
            raise NbiNotFoundError(
                f"ONT {serial} has not been discovered on {network}. "
                f"Check that it is connected and powered.")
        else:
            await context.emit("log", f"ONT {serial} has not been seen on a PON yet; provisioning it "
                                      f"ahead of installation")

        await context.step(2, total_steps, f"Creating ONT {serial}")
        spec = OntSpec(**request.model_dump(include=set(OntSpec.model_fields)))
        ont = await gateway.create_ont(network, spec)
        created_here = True
        try:
            await context.record_ownership(f"Created ONT {ont.ont_id}", network=network, ont_id=ont.ont_id,
                                           serial_number=serial)
        except BusUnavailableError:
            # Unrecorded, a later attempt would take it for someone else's and never roll it back
            await _undo(f"ONT {ont.ont_id}", gateway.delete_ont(network, ont.ont_id, force=False))
            raise

    ont_id = ont.ont_id
    # A service is already there if one with the same identity is: side, port and VLAN, or for a
    # service without a VLAN, side, port and tag action (see service_identity)
    bound = {s.identity(): s for s in ont.services}
    missing = [s for s in request.services if s.identity() not in bound]
    # Already bound, but not as the request asks: changed in place rather than skipped
    changed = [(bound[s.identity()], d) for s in request.services
               if s.identity() in bound and (d := service_differences(bound[s.identity()], s))]

    try:
        await context.step(3, total_steps, f"Configuring {len(request.ports)} Ethernet port(s) on ONT {ont_id}")
        if request.ports:
            current = {p.number: p for p in (await gateway.get_ont_ports(network, ont_id)).ethernet}
            for config in request.ports:
                changes = port_differences(current.get(config.port), config.changes().model_dump(exclude_none=True))
                if not changes:
                    await context.emit("log", f"Port eth{config.port} already set", port=f"eth{config.port}")
                    continue
                await gateway.update_port(network, ont_id, Attachment.ETHERNET, config.port, PortChange(**changes))
                await context.emit("log", f"Set port eth{config.port}: {_describe(changes)}",
                                   port=f"eth{config.port}", **changes)

        await context.step(4, total_steps, f"Binding {len(missing)} service(s) to ONT {ont_id}"
                                           + (f" and changing {len(changed)}" if changed else ""))
        for existing, differences in changed:
            where = f"{port_name(existing.attachment, existing.ont_port)} service {existing.service_index}"
            await gateway.update_service(network, ont_id, existing.attachment, existing.ont_port,
                                         existing.service_index, ServiceChange(**differences))
            await context.emit("log", f"Changed {where}: {_describe(differences)}", port=where, **differences)
        # One at a time: parallel edits to the same ONT are not worth the risk
        for service in missing:
            side = "router port rg" if service.attach_to == "router" else "ONT port "
            what = f"VLAN {service.vlan}" if service.vlan is not None else f"tag action {service.tag_action} (no VLAN)"
            try:
                await gateway.add_ethernet_service(network, ont_id, service)
            except NbiConflictError:
                # "Already exists": another run (this job's own, delivered twice at once, or another
                # job's) bound it after step 1 read the ONT. Not this run's, so a rollback leaves the ONT.
                if not any(s.identity() == service.identity() for s in (await gateway.get_ont(network, ont_id)).services):
                    raise
                await context.emit("log", f"{what} on {side}{service.ont_port} was bound meanwhile by another run",
                                   vlan=service.vlan, ont_port=service.ont_port, attach_to=service.attach_to)
                continue
            added.add(service.identity())
            fields = dict(vlan=service.vlan, tag_action=service.tag_action, ont_port=service.ont_port,
                          attach_to=service.attach_to)
            if created_here:
                try:
                    await context.record_ownership(f"Bound {what} to {side}{service.ont_port}", network=network,
                                                   ont_id=ont_id, service=fields)
                except BusUnavailableError:
                    await _undo(f"{what} on {side}{service.ont_port}",
                                _delete_bound(gateway, network, ont_id, service.identity()))
                    raise
            else:
                await context.emit("log", f"Bound {what} to {side}{service.ont_port}", **fields)
    except NbiError as e:
        # Roll back only when CMS said no. If CMS is merely unreachable the
        # command is retried and carries on from here.
        if not e.retryable and request.rollback_on_failure:
            if created_here:
                await _roll_back(context, gateway, network, ont_id, added)
            elif context.attempt > 1:
                # This attempt cannot tell whether an earlier one created the ONT, so it is not rolled
                # back. Say so plainly rather than leave a partial ONT unmentioned.
                e.message += (f" ONT {ont_id} was already on CMS when attempt {context.attempt} of this job started, "
                              f"so it was not rolled back. If an earlier attempt created it, it is left partially "
                              f"provisioned: check it in CMS and delete it if no one else is using it.")
        raise

    if request.wait_for_online:
        await context.step(5, total_steps, f"Waiting for ONT {ont_id} to come online")
    else:
        await context.step(5, total_steps, f"Checking ONT {ont_id}")
    deadline = time.monotonic() + (verify_timeout if request.wait_for_online else 0)
    while True:
        ont = await gateway.get_ont(network, ont_id)
        if ont.oper_state == OperState.ONLINE or time.monotonic() >= deadline:
            break
        await asyncio.sleep(poll_seconds)

    # CMS accepting each write is not proof: the last read must show every requested service as asked
    held = {s.identity(): s for s in ont.services}
    wrong = [s for s in request.services
             if s.identity() not in held or service_differences(held[s.identity()], s)]
    if wrong:
        listed = ", ".join(f"{port_name(Attachment(s.attach_to), s.ont_port)} "
                           f"{f'VLAN {s.vlan}' if s.vlan is not None else f'tag action {s.tag_action}'}"
                           + (" (missing)" if s.identity() not in held else " (differs)") for s in wrong)
        raise NbiVerifyError(f"CMS accepted every step, but reading ONT {ont_id} back does not show these "
                             f"services as asked: {listed}. ONT {ont_id} is left in place; check it in CMS.")

    online = ont.oper_state == OperState.ONLINE
    if not online and request.wait_for_online:
        await context.emit("warning",
                           f"ONT {ont_id} is provisioned but did not come online within "
                           f"{verify_timeout:g} seconds. It has been left in place.", ont_id=ont_id)

    # A succeeded job is not a live customer unless outcome says online
    outcome = "online" if online else "provisioned_offline" if request.wait_for_online else "provisioned_not_waited"
    return {"network": network, "ont_id": ont_id, "serial_number": serial, "outcome": outcome,
            "online": online, "services": len(ont.services), "ports_configured": len(request.ports),
            "verified": True}


def port_differences(current: "PortState | None", wanted: Dict[str, Any]) -> Dict[str, Any]:
    """The wanted port values that CMS does not already hold"""
    if current is None:
        return dict(wanted)
    return {k: v for k, v in wanted.items() if getattr(current, k) != v}


def service_differences(current: ProvisionedService, wanted: EthernetService) -> Dict[str, Any]:
    """
    What an already bound service needs changed to match a requested one with the same identity.
    An empty peak rate in the request means no override, as when the service is first bound. The
    VLAN is part of the identity, and the description and admin state are left as they are.
    """
    target = {"bandwidth_profile": wanted.bandwidth_profile,
              "us_pir_override": wanted.us_pir_override or "none",
              "ds_pir_override": wanted.ds_pir_override or "none"}
    if wanted.tag_action:
        target["tag_action"] = wanted.tag_action
    have = {"us_pir_override": current.us_pir_override or "none",
            "ds_pir_override": current.ds_pir_override or "none"}
    return {k: v for k, v in target.items() if have.get(k, getattr(current, k)) != v}


def _describe(changes: Dict[str, Any]) -> str:
    return ", ".join(f"{k.replace('_', ' ')} {v}" for k, v in changes.items())


async def _check_router_services(context: JobContext, gateway: NbiGateway, network: str,
                                 request: ActivationRequest) -> None:
    """
    Before anything is created: a service on the router side needs an ONT
    profile with a router. Refuse it if the profile says it has none. Only
    looked up when the request has router-side services.
    """
    router_services = [s for s in request.services if s.attach_to == "router"]
    if not router_services:
        return
    profiles = {p.id: p for p in await gateway.list_ont_profiles(network)}
    profile = profiles.get(request.profile)
    if profile is None or profile.rg_ports is None:
        await context.emit("warning", f"Could not tell from CMS whether ONT profile '{request.profile}' has a "
                                      f"router; sending the router-side service(s) anyway.")
        return
    if profile.rg_ports == 0:
        raise NbiRejectedError(
            f"ONT profile '{request.profile}' has no router (its rg count is 0), so a service cannot be "
            f"attached to the router side. Attach it to an Ethernet port instead. Nothing was changed.")
    for service in router_services:
        if service.ont_port > profile.rg_ports:
            raise NbiRejectedError(
                f"ONT profile '{request.profile}' has {profile.rg_ports} router port(s); there is no "
                f"rg{service.ont_port}. Nothing was changed.")


async def _undo(what: str, deletion: Awaitable[None]) -> None:
    """Best effort: if the delete fails too, the record is still missing and a person has to look"""
    try:
        await deletion
    except NbiError as e:
        logger.error(f"Could not record or undo {what} ({e.message}); it is on CMS and not recorded as this job's")


async def _delete_bound(gateway: NbiGateway, network: str, ont_id: int, identity: Tuple[Any, ...]) -> None:
    service = next(s for s in (await gateway.get_ont(network, ont_id)).services if s.identity() == identity)
    await gateway.delete_service(network, ont_id, service.attachment, service.ont_port, service.service_index)


async def _earlier_attempts_work(context: JobContext, network: str, serial: str,
                                 ont_id: int) -> Optional[Set[Tuple[Any, ...]]]:
    """
    If an earlier attempt of this job recorded creating this ONT, the identities of the services it
    recorded binding to it; otherwise None (not this job's ONT). Only asked when the ONT exists.
    """
    records = [r for r in await context.ownership_records()
               if r.get("network") == network and r.get("ont_id") == ont_id]
    if not any(r.get("serial_number") == serial and "service" not in r for r in records):
        return None
    return {service_identity(Attachment(s["attach_to"]), s["ont_port"], s.get("vlan"), s.get("tag_action"))
            for r in records if (s := r.get("service"))}


async def _roll_back(context: JobContext, gateway: NbiGateway, network: str, ont_id: int,
                     added: Set[Tuple[Any, ...]]) -> None:
    """
    Delete the ONT this run created, the way a DELETE does: the services this run added one by
    one, then the ONT without CMS's force. A command can run twice at once (Kafka redelivers it
    after a rebalance, or two worker groups take it), so the ONT is read again first. If it holds
    a service this run did not add, another run is provisioning it and it is left alone. A
    service added after that read, or a voice service this API cannot see, makes CMS refuse the
    delete instead of removing it.
    """
    await context.emit("warning", f"CMS rejected the activation; rolling back by deleting ONT {ont_id}",
                       ont_id=ont_id)
    try:
        ont = await gateway.get_ont(network, ont_id)
        others = [s for s in ont.services if s.identity() not in added]
        if others:
            listed = ", ".join(f"{port_name(s.attachment, s.ont_port)} service {s.service_index}" for s in others)
            await context.emit("warning",
                               f"Not rolled back: ONT {ont_id} also has service(s) this run did not add "
                               f"({listed}), so another job may be provisioning it. It has been left in place.",
                               ont_id=ont_id, rollback_skipped=True)
            return
        for service in ont.services:
            await gateway.delete_service(network, ont_id, service.attachment, service.ont_port,
                                         service.service_index)
        await gateway.delete_ont(network, ont_id, force=False)
        await context.emit("log", f"Rolled back: ONT {ont_id} deleted", ont_id=ont_id)
    except NbiNotFoundError:
        await context.emit("log", f"ONT {ont_id} is already gone; nothing to roll back", ont_id=ont_id)
    except NbiError as e:
        logger.error(f"Rollback of ONT {ont_id} on {network} failed: {e.message}")
        await context.emit("warning",
                           f"Rollback failed: ONT {ont_id} could not be deleted ({e.message}) "
                           f"and needs manual cleanup.", ont_id=ont_id, rollback_failed=True)

"""
Mock CMS
An in-memory stand-in for the Calix CMS northbound interface, used when
MOCK_MODE=true. It models one network of fourteen ONTs on two PONs
(MOCK_ONTS below), plus three ONTs that have been discovered on a PON but
not provisioned yet. MockCmsBackend(pon_size=64) (or MOCK_PON_SIZE) also puts
that many discovered ONTs on the empty PON 1/1/3 (see app/nbi/mock_pon.py).

It checks the things a real CMS would refuse (duplicate ONT IDs, serial
numbers already in use, VLANs and bandwidth profiles that do not exist, a missing or unwanted outer
VLAN under the simulation's own tag-action rule) and can simulate an outage
so the circuit breaker can be exercised.

This is a behavioural sketch, not a copy of CMS: no XML is exchanged and the
rules are simplified.
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, EthernetService, OntPorts, OntProfile, OntSpec, OntState, OperState,
    PortChange, PortState, ProvisionedService, RouterState, ServiceChange, Vlan, port_name
)
from app.nbi.errors import (
    NbiConflictError, NbiConnectionError, NbiNotFoundError, NbiRejectedError
)
from app.nbi.mock_pon import EXTRA_BW_PROFILES, EXTRA_VLANS, check_pon_size, sim_onts

MOCK_NETWORK = "NTWK-MOCK-E7"
MOCK_VLANS = {100: "data", 200: "voice", 300: "video"}
# Port counts are invented for the simulation. They are not Calix's figures for these models.
MOCK_PROFILE_DETAILS = {
    "716GE-I": OntProfile(id="716GE-I", name="716GE-I", model="716GE-I", vendor="CXNK", ge_ports=2,
                          fe_ports=0, pots_ports=2, rg_ports=0, default_to_rg=False),
    "812G-1": OntProfile(id="812G-1", name="812G-1", model="812G", vendor="CXNK", ge_ports=2,
                         fe_ports=0, pots_ports=2, rg_ports=0, default_to_rg=False),
    "844G-1": OntProfile(id="844G-1", name="844G-1", model="844G", vendor="CXNK", ge_ports=4,
                         fe_ports=0, pots_ports=2, rg_ports=1, default_to_rg=True),
}
MOCK_ONT_PROFILES = tuple(MOCK_PROFILE_DETAILS)
MOCK_SECURITY_PROFILES = {str(n) for n in range(1, 17)}
MOCK_BANDWIDTH_PROFILES = ("RES-1G", "VOICE")  # invented names; on a real CMS they are numeric IDs

# SIMULATION RULE, NOT A CALIX FACT. IDI's CMS R15.x docs say the outer VLAN is optional and is
# left out "if the value is not required by Service Tag Action", but not which tag actions those
# are: that is set up on each CMS. The simulation has two tag actions that take no outer VLAN, with
# obviously fake names. Every other tag action (or none) needs one, and these two refuse one.
MOCK_TAG_ACTIONS_NO_VLAN = ("MOCK-NO-VLAN-A", "MOCK-NO-VLAN-B")
MOCK_SIM_PON = "1/1/3"  # empty in the default network; pon_size fills it


class MockOnt(NamedTuple):
    """One provisioned ONT in the simulated network"""
    ont_id: int
    pon: str  # shelf/card/port
    serial: str
    model: str  # also its ONT profile
    description: str
    online: bool = True


# The simulated network. ONT 6 is unplugged, so it reads as offline.
MOCK_ONTS = (
    MockOnt(1, "1/1/1", "CXNK00A3F101", "716GE-I", "101 Alder St"),
    MockOnt(2, "1/1/1", "CXNK00A3F102", "716GE-I", "105 Alder St"),
    MockOnt(3, "1/1/1", "CXNK00A3F103", "844G-1", "112 Alder St"),
    MockOnt(4, "1/1/1", "CXNK00A3F104", "844G-1", "118 Alder St"),
    MockOnt(5, "1/1/1", "CXNK00A3F105", "716GE-I", "124 Alder St"),
    MockOnt(6, "1/1/1", "CXNK00A3F106", "812G-1", "131 Alder St", online=False),
    MockOnt(7, "1/1/1", "CXNK00A3F107", "844G-1", "137 Alder St"),
    MockOnt(8, "1/1/1", "CXNK00A3F108", "716GE-I", "140 Alder St"),
    MockOnt(9, "1/1/2", "CXNK00B7C209", "716GE-I", "12 Birch Ave"),
    MockOnt(10, "1/1/2", "CXNK00B7C210", "844G-1", "18 Birch Ave"),
    MockOnt(11, "1/1/2", "CXNK00B7C211", "716GE-I", "25 Birch Ave"),
    MockOnt(12, "1/1/2", "CXNK00B7C212", "812G-1", "31 Birch Ave"),
    MockOnt(13, "1/1/2", "CXNK00B7C213", "716GE-I", "40 Birch Ave"),
    MockOnt(14, "1/1/2", "CXNK00B7C214", "844G-1", "46 Birch Ave"),
)


def _new_port() -> Dict[str, Any]:
    # A new port starts as a bridge in the simulation. That is the simulation's choice, not a
    # statement about CMS: real code reads each port's mode and never assumes one.
    return {"admin_state": "enabled", "subscriber_id": None, "security_profile": None, "description": None,
            "mode": "bridge"}


class _MockOnt:
    def __init__(self, state: OntState, on_wire: bool, online_at: float):
        self.state = state
        self.on_wire = on_wire  # is the ONT physically connected?
        self.online_at = online_at  # when ranging completes
        profile = MOCK_PROFILE_DETAILS.get(state.profile or "")
        ge, rg = (profile.ge_ports or 0, profile.rg_ports or 0) if profile else (0, 0)
        self.ports: Dict[int, Dict[str, Any]] = {n: _new_port() for n in range(1, ge + 1)}
        self.router_ports: Dict[int, Dict[str, Any]] = {n: {"admin_state": "enabled"} for n in range(1, rg + 1)}
        # POTS voice lines. Like a real CMS read through this API, nothing reports them: they only
        # show when a delete without force is refused, or are lost when one with force is sent.
        self.voice_lines = 0


class MockCmsBackend:
    name = "mock-cms"

    def __init__(self, latency: float = 0.05, ranging_seconds: float = 1.5,
                 clock: Callable[[], float] = time.monotonic, extra_onts: int = 0, pon_size: int = 0):
        self.latency = latency  # seconds each call takes
        self.ranging_seconds = ranging_seconds  # how long a new ONT takes to come online
        self.outage: Optional[str] = None  # None, "timeout" or "refused"
        self.calls = 0  # how many calls reached the backend
        self._clock = clock
        self._onts: Dict[int, _MockOnt] = {}
        self._discovered: Dict[str, DiscoveredOnt] = {}
        self.vlans = dict(MOCK_VLANS)
        self.bandwidth_profiles = MOCK_BANDWIDTH_PROFILES

        for ont in MOCK_ONTS:
            # Everyone has data; every third subscriber also has voice on port 2
            services = [ProvisionedService(service_index=1, ont_port=1, vlan=100,
                                           bandwidth_profile="RES-1G", tag_action="1",
                                           us_pir_override="none", ds_pir_override="none")]
            if ont.ont_id % 3 == 0:
                services.append(ProvisionedService(service_index=1, ont_port=2, vlan=200,
                                                   bandwidth_profile="VOICE", tag_action="2",
                                                   us_pir_override="none", ds_pir_override="none"))
            state = OntState(
                ont_id=ont.ont_id, serial_number=ont.serial, model=ont.model, profile=ont.model,
                admin_state="enabled", pon_port=ont.pon, description=ont.description,
                subscriber_id=f"SUB-{1000 + ont.ont_id}", services=services)
            self._onts[ont.ont_id] = _MockOnt(state, on_wire=ont.online, online_at=0.0)
            if ont.ont_id % 3 == 0:
                self._onts[ont.ont_id].voice_lines = 1  # a POTS line the API cannot see

        # Optional bulk ONTs (IDs from 1001, 32 per PON on card 2) for trying things at scale
        for n in range(extra_onts):
            ont_id, pon = 1001 + n, f"1/2/{n // 32 + 1}"
            self._onts[ont_id] = _MockOnt(OntState(
                ont_id=ont_id, serial_number=f"CXNK00B{n:05X}", model="844G-1", profile="844G-1",
                pon_port=pon, description=f"Bulk ONT {ont_id}", subscriber_id=f"SUB-{ont_id}",
                services=[ProvisionedService(service_index=1, ont_port=1, vlan=100,
                                             bandwidth_profile="RES-1G", tag_action="1",
                                             us_pir_override="none", ds_pir_override="none")]),
                on_wire=True, online_at=0.0)

        for serial, model, pon in (("CXNK00C4D301", "844G-1", "1/1/1"),
                                   ("CXNK00C4D302", "716GE-I", "1/1/2"),
                                   ("CXNK00C4D303", "812G-1", "1/1/2")):
            self._discovered[serial] = DiscoveredOnt(serial_number=serial, model=model, pon_port=pon)

        # Optional: a PON of discovered ONTs, for provisioning at scale (app/nbi/mock_pon.py)
        if check_pon_size(pon_size):
            for serial, profile in sim_onts(pon_size):
                self._discovered[f"CXNK{serial}"] = DiscoveredOnt(
                    serial_number=f"CXNK{serial}", model=profile, pon_port=MOCK_SIM_PON)
            self.vlans.update(EXTRA_VLANS)
            self.bandwidth_profiles = MOCK_BANDWIDTH_PROFILES + EXTRA_BW_PROFILES

    # --- NbiBackend operations ------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        await self._exchange(network)
        return list(self._discovered.values())

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        await self._exchange(network)
        return self._snapshot(self._require(network, ont_id))

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        await self._exchange(network)
        for ont in self._onts.values():
            if ont.state.serial_number == serial_number:
                return self._snapshot(ont)
        return None

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        await self._exchange(network)
        return [self._snapshot(ont) for ont in self._onts.values()
                if ont.state.subscriber_id == subscriber_id]

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        await self._exchange(network)

        if spec.profile not in MOCK_ONT_PROFILES:
            raise NbiRejectedError(
                f"ONT profile '{spec.profile}' does not exist on {network}. "
                f"Available profiles: {', '.join(MOCK_ONT_PROFILES)}.")
        for existing in self._onts.values():
            if existing.state.serial_number == spec.serial_number:
                raise NbiConflictError(
                    f"Serial number {spec.serial_number} is already assigned to "
                    f"ONT {existing.state.ont_id} on {network}.")
        ont_id = spec.ont_id or max(self._onts, default=0) + 1
        if ont_id in self._onts:
            raise NbiConflictError(f"ONT {ont_id} already exists on {network}.")

        # A provisioned ONT is no longer listed as discovered
        discovered = self._discovered.pop(spec.serial_number, None)
        state = OntState(
            ont_id=ont_id, serial_number=spec.serial_number, profile=spec.profile,
            model=discovered.model if discovered else None,
            pon_port=discovered.pon_port if discovered else None,
            description=spec.description, subscriber_id=spec.subscriber_id)
        self._onts[ont_id] = _MockOnt(state, on_wire=discovered is not None,
                                      online_at=self._clock() + self.ranging_seconds)
        return self._snapshot(self._onts[ont_id])

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        attachment = Attachment(service.attach_to)

        self._check_tag_vlan(network, service.tag_action, service.vlan)
        self._check_bandwidth_profile(network, service.bandwidth_profile)
        self._check_port(ont, attachment, service.ont_port)
        name = port_name(attachment, service.ont_port)
        for existing in ont.state.services:
            if existing.identity() == service.identity():
                what = (f"VLAN {service.vlan}" if service.vlan is not None
                        else f"tag action {service.tag_action} and no VLAN")
                raise NbiConflictError(f"ONT {ont_id} port {name} already has a service on {what}.")
        on_port = [s.service_index for s in ont.state.services
                   if (s.attachment, s.ont_port) == (attachment, service.ont_port)]
        ont.state.services.append(ProvisionedService(
            service_index=max(on_port, default=0) + 1, ont_port=service.ont_port, attachment=attachment,
            vlan=service.vlan, bandwidth_profile=service.bandwidth_profile, tag_action=service.tag_action,
            description=service.description, us_pir_override=service.us_pir_override or "none",
            ds_pir_override=service.ds_pir_override or "none"))

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        profile = MOCK_PROFILE_DETAILS.get(ont.state.profile or "")
        return OntPorts(
            network=network, ont_id=ont_id, profile=ont.state.profile,
            ethernet=[PortState(port=port_name(Attachment.ETHERNET, n), attachment=Attachment.ETHERNET, number=n,
                                **values) for n, values in sorted(ont.ports.items())],
            router=RouterState(
                present=bool(profile.rg_ports) if profile else None,
                default_to_rg=profile.default_to_rg if profile else None,
                ports=[PortState(port=port_name(Attachment.ROUTER, n), attachment=Attachment.ROUTER, number=n,
                                 **values) for n, values in sorted(ont.router_ports.items())],
                services=sum(1 for s in ont.state.services if s.attachment == Attachment.ROUTER)),
            voice_ports=profile.pots_ports if profile else None)

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        attachment = Attachment(attachment)
        self._check_port(ont, attachment, number)
        values = change.model_dump(exclude_none=True)
        if attachment == Attachment.ROUTER:
            if set(values) - {"admin_state"}:
                raise NbiRejectedError("Only the admin state of a router port can be changed.")
            ont.router_ports[number].update(values)
            return
        if values.get("mode") == "router" and not ont.router_ports:
            raise NbiRejectedError(f"ONT {ont_id} (profile {ont.state.profile}) has no router, so port "
                                   f"eth{number} cannot be in router mode.")
        if "security_profile" in values and values["security_profile"] not in MOCK_SECURITY_PROFILES:
            raise NbiRejectedError(f"Ethernet security profile {values['security_profile']} does not exist "
                                   f"on {network}. Available: 1 to 16.")
        ont.ports[number].update(values)

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        position = self._service_position(ont, ont_port, service_index, attachment)
        if position is None:
            raise NbiNotFoundError(f"Service {service_index} on ONT {ont_id} port "
                                   f"{port_name(attachment, ont_port)} does not exist.")
        current = ont.state.services[position]
        updated = current.model_copy(update=change.model_dump(exclude_none=True))
        self._check_tag_vlan(network, updated.tag_action, updated.vlan)
        self._check_bandwidth_profile(network, updated.bandwidth_profile)
        ont.state.services[position] = updated

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        await self._exchange(network)
        return [p.model_copy() for p in MOCK_PROFILE_DETAILS.values()]

    async def list_vlans(self, network: str) -> List[Vlan]:
        await self._exchange(network)
        return [Vlan(id=vlan, name=name) for vlan, name in self.vlans.items()]

    # --- Extra operations used by the mock NBI server (app/nbi/mock_cms_server.py) ---

    async def update_ont(self, network: str, ont_id: int, changes: Dict[str, object]) -> OntState:
        """Merge new values into an ONT (serial_number, profile, admin_state, description, subscriber_id)"""
        await self._exchange(network)
        ont = self._require(network, ont_id)
        if "profile" in changes and changes["profile"] not in MOCK_ONT_PROFILES:
            raise NbiRejectedError(f"ONT profile '{changes['profile']}' does not exist on {network}.")
        ont.state = ont.state.model_copy(update=changes)
        return self._snapshot(ont)

    async def put_service(self, network: str, ont_id: int, service: ProvisionedService, create: bool) -> None:
        """Create a service at an explicit index, or merge new values into an existing one"""
        await self._exchange(network)
        ont = self._require(network, ont_id)
        self._check_tag_vlan(network, service.tag_action, service.vlan)
        self._check_bandwidth_profile(network, service.bandwidth_profile)
        self._check_port(ont, service.attachment, service.ont_port)
        position = self._service_position(ont, service.ont_port, service.service_index, service.attachment)
        label = (f"Service {service.service_index} on ONT {ont_id} port "
                 f"{port_name(service.attachment, service.ont_port)}")
        if create:
            if position is not None:
                raise NbiConflictError(f"{label} already exists.")
            ont.state.services.append(service)
        else:
            if position is None:
                raise NbiNotFoundError(f"{label} does not exist.")
            ont.state.services[position] = service

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        position = self._service_position(ont, ont_port, service_index, attachment)
        if position is None:
            raise NbiNotFoundError(f"Service {service_index} on ONT {ont_id} port "
                                   f"{port_name(attachment, ont_port)} does not exist.")
        del ont.state.services[position]

    async def list_onts(self, network: str) -> List[OntState]:
        await self._exchange(network)
        return [self._snapshot(ont) for ont in self._onts.values()]

    def damage(self, pon_port: str) -> List[str]:
        """
        Simulate a bad change on one PON, for snapshot-compare demos: delete an ONT,
        disable one, repoint one at the wrong VLAN and give one a stray service.
        Returns a description of what was broken.
        """
        victims = sorted(o.state.ont_id for o in self._onts.values() if o.state.pon_port == pon_port)
        if len(victims) < 4:
            return []
        deleted, disabled, revlan, stray = victims[0], victims[1], victims[2], victims[3]
        done = []
        gone = self._onts.pop(deleted)
        if gone.on_wire:  # still plugged in, so CMS sees it as an unprovisioned ONT again
            self._discovered[gone.state.serial_number] = DiscoveredOnt(
                serial_number=gone.state.serial_number, model=gone.state.model or "unknown",
                pon_port=gone.state.pon_port or "unknown")
        done.append(f"deleted ONT {deleted}")
        self._onts[disabled].state.admin_state = "disabled"
        done.append(f"disabled ONT {disabled}")
        self._onts[revlan].state.services[0].vlan = 300
        done.append(f"moved ONT {revlan} service 1 to VLAN 300")
        self._onts[stray].state.services.append(ProvisionedService(
            service_index=9, ont_port=1, vlan=300, bandwidth_profile="RES-1G", tag_action="1",
            us_pir_override="none", ds_pir_override="none"))
        done.append(f"added a stray service to ONT {stray}")
        return done

    async def delete_ont(self, network: str, ont_id: int, *, force: bool) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        # SIMULATION RULE: without force, an ONT that still has services (voice included) is refused.
        # That a real CMS does the same is assumed, not confirmed.
        if not force and (ont.state.services or ont.voice_lines):
            raise NbiConflictError(
                f"ONT {ont_id} still has {len(ont.state.services)} data service(s) and {ont.voice_lines} voice "
                f"line(s). Delete them first, or delete with force.")
        del self._onts[ont_id]
        if ont.on_wire:
            # Still plugged in, so it shows up as discovered again
            self._discovered[ont.state.serial_number] = DiscoveredOnt(
                serial_number=ont.state.serial_number,
                model=ont.state.model or "unknown", pon_port=ont.state.pon_port or "unknown")

    async def close(self) -> None:
        return None

    # --- Internals -------------------------------------------------------

    async def _exchange(self, network: str) -> None:
        """Stand-in for one request/response with CMS"""
        self.calls += 1
        if self.outage == "refused":
            raise NbiConnectionError("Could not connect to CMS (connection refused).")
        if self.outage == "timeout":
            await asyncio.sleep(3600)  # the gateway's timeout cuts this short
        if self.latency:
            await asyncio.sleep(self.latency)
        if network != MOCK_NETWORK:
            raise NbiNotFoundError(
                f"Network '{network}' is not known to CMS. The simulated CMS has one network: {MOCK_NETWORK}.")

    @staticmethod
    def _service_position(ont: _MockOnt, ont_port: int, service_index: int,
                          attachment: Attachment = Attachment.ETHERNET) -> Optional[int]:
        for position, existing in enumerate(ont.state.services):
            if (existing.attachment, existing.ont_port, existing.service_index) == (
                    Attachment(attachment), ont_port, service_index):
                return position
        return None

    def _check_vlan(self, network: str, vlan: int) -> None:
        if vlan not in self.vlans:
            raise NbiRejectedError(
                f"VLAN {vlan} does not exist on {network}. "
                f"Available VLANs: {', '.join(str(v) for v in self.vlans)}.")

    def _check_tag_vlan(self, network: str, tag_action: Optional[str], vlan: Optional[int]) -> None:
        """The simulation's rule for which tag actions take an outer VLAN (MOCK_TAG_ACTIONS_NO_VLAN)"""
        if tag_action in MOCK_TAG_ACTIONS_NO_VLAN:
            if vlan is not None:
                raise NbiRejectedError(f"Tag action {tag_action} takes no outer VLAN, but VLAN {vlan} was given.")
            return
        if vlan is None:
            raise NbiRejectedError(f"Tag action {tag_action or '(none)'} needs an outer VLAN, and none was given. "
                                   f"In the simulated CMS only tag actions {' and '.join(MOCK_TAG_ACTIONS_NO_VLAN)} take none.")
        self._check_vlan(network, vlan)

    def _check_bandwidth_profile(self, network: str, profile: Optional[str]) -> None:
        if profile not in self.bandwidth_profiles:
            raise NbiRejectedError(
                f"Bandwidth profile '{profile}' does not exist on {network}. "
                f"Available bandwidth profiles: {', '.join(self.bandwidth_profiles)}.")

    @staticmethod
    def _check_port(ont: _MockOnt, attachment: Attachment, number: int) -> None:
        """The simulation refuses ports the ONT's profile does not have"""
        if Attachment(attachment) == Attachment.ROUTER:
            if number not in ont.router_ports:
                raise NbiRejectedError(f"ONT {ont.state.ont_id} (profile {ont.state.profile}) has no router "
                                       f"port {number}.")
        elif number not in ont.ports:
            raise NbiRejectedError(f"ONT {ont.state.ont_id} (profile {ont.state.profile}) has no Ethernet "
                                   f"port {number}.")

    def _require(self, network: str, ont_id: int) -> _MockOnt:
        ont = self._onts.get(ont_id)
        if ont is None:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        return ont

    def _snapshot(self, ont: _MockOnt) -> OntState:
        online = ont.on_wire and self._clock() >= ont.online_at
        return ont.state.model_copy(deep=True, update={
            "oper_state": OperState.ONLINE if online else OperState.OFFLINE})

"""
Mock CMS
An in-memory stand-in for the Calix CMS northbound interface, used when
MOCK_MODE=true. It models one network, built from the same simulated ONTs
as the mock E7-20 (app/controller/mock_e7.py), plus three ONTs that have
been discovered on a PON but not provisioned yet.

It checks the things a real CMS would refuse (duplicate ONT IDs, serial
numbers already in use, VLANs that do not exist) and can simulate an outage
so the circuit breaker can be exercised.

This is a behavioural sketch, not a copy of CMS: no XML is exchanged and the
rules are simplified.
"""

import asyncio
import time
from typing import Callable, Dict, List, Optional

from app.controller.mock_e7 import MOCK_ONTS
from app.models.provisioning_model import (
    DiscoveredOnt, EthernetService, OntSpec, OntState, OperState, ProvisionedService
)
from app.nbi.errors import (
    NbiConflictError, NbiConnectionError, NbiNotFoundError, NbiRejectedError
)

MOCK_NETWORK = "NTWK-MOCK-E7"
MOCK_VLANS = {100: "data", 200: "voice", 300: "video"}
MOCK_ONT_PROFILES = ("716GE-I", "812G-1", "844G-1")


class _MockOnt:
    def __init__(self, state: OntState, on_wire: bool, online_at: float):
        self.state = state
        self.on_wire = on_wire  # is the ONT physically connected?
        self.online_at = online_at  # when ranging completes


class MockCmsBackend:
    name = "mock-cms"

    def __init__(self, latency: float = 0.05, ranging_seconds: float = 1.5,
                 clock: Callable[[], float] = time.monotonic, extra_onts: int = 0):
        self.latency = latency  # seconds each call takes
        self.ranging_seconds = ranging_seconds  # how long a new ONT takes to come online
        self.outage: Optional[str] = None  # None, "timeout" or "refused"
        self.calls = 0  # how many calls reached the backend
        self._clock = clock
        self._onts: Dict[int, _MockOnt] = {}
        self._discovered: Dict[str, DiscoveredOnt] = {}

        for ont in MOCK_ONTS:
            # Everyone has data; every third subscriber also has voice on port 2
            services = [ProvisionedService(service_index=1, ont_port=1, vlan=100,
                                           bandwidth_profile="RES-1G", tag_action="1")]
            if ont.ont_id % 3 == 0:
                services.append(ProvisionedService(service_index=1, ont_port=2, vlan=200,
                                                   bandwidth_profile="VOICE", tag_action="2"))
            state = OntState(
                ont_id=ont.ont_id, serial_number=ont.serial, model=ont.model, profile=ont.model,
                admin_state=ont.admin, pon_port=ont.pon, description=ont.description,
                subscriber_id=f"SUB-{1000 + ont.ont_id}", services=services)
            self._onts[ont.ont_id] = _MockOnt(state, on_wire=ont.online, online_at=0.0)

        # Optional bulk ONTs (IDs from 1001, 32 per PON on card 2) for trying things at scale
        for n in range(extra_onts):
            ont_id, pon = 1001 + n, f"1/2/{n // 32 + 1}"
            self._onts[ont_id] = _MockOnt(OntState(
                ont_id=ont_id, serial_number=f"CXNK00B{n:05X}", model="844G-1", profile="844G-1",
                pon_port=pon, description=f"Bulk ONT {ont_id}", subscriber_id=f"SUB-{ont_id}",
                services=[ProvisionedService(service_index=1, ont_port=1, vlan=100,
                                             bandwidth_profile="RES-1G", tag_action="1")]),
                on_wire=True, online_at=0.0)

        for serial, model, pon in (("CXNK00C4D301", "844G-1", "1/1/1"),
                                   ("CXNK00C4D302", "716GE-I", "1/1/2"),
                                   ("CXNK00C4D303", "812G-1", "1/1/2")):
            self._discovered[serial] = DiscoveredOnt(serial_number=serial, model=model, pon_port=pon)

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

        if service.vlan not in MOCK_VLANS:
            raise NbiRejectedError(
                f"VLAN {service.vlan} does not exist on {network}. "
                f"Available VLANs: {', '.join(str(v) for v in MOCK_VLANS)}.")
        for existing in ont.state.services:
            if (existing.vlan, existing.ont_port) == (service.vlan, service.ont_port):
                raise NbiConflictError(
                    f"ONT {ont_id} port {service.ont_port} already has a service on VLAN {service.vlan}.")
        on_port = [s.service_index for s in ont.state.services if s.ont_port == service.ont_port]
        ont.state.services.append(ProvisionedService(
            service_index=max(on_port, default=0) + 1, ont_port=service.ont_port, vlan=service.vlan,
            bandwidth_profile=service.bandwidth_profile, tag_action=service.tag_action,
            description=service.description))

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
        if service.vlan is not None and service.vlan not in MOCK_VLANS:
            raise NbiRejectedError(
                f"VLAN {service.vlan} does not exist on {network}. "
                f"Available VLANs: {', '.join(str(v) for v in MOCK_VLANS)}.")
        position = self._service_position(ont, service.ont_port, service.service_index)
        label = f"Service {service.service_index} on ONT {ont_id} port {service.ont_port}"
        if create:
            if position is not None:
                raise NbiConflictError(f"{label} already exists.")
            ont.state.services.append(service)
        else:
            if position is None:
                raise NbiNotFoundError(f"{label} does not exist.")
            ont.state.services[position] = service

    async def delete_service(self, network: str, ont_id: int, ont_port: int, service_index: int) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
        position = self._service_position(ont, ont_port, service_index)
        if position is None:
            raise NbiNotFoundError(f"Service {service_index} on ONT {ont_id} port {ont_port} does not exist.")
        del ont.state.services[position]

    async def list_onts(self, network: str) -> List[OntState]:
        await self._exchange(network)
        return [self._snapshot(ont) for ont in self._onts.values()]

    def damage(self, pon_port: str) -> List[str]:
        """
        Simulate a bad change on one PON, for rollback demos: delete an ONT,
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
            service_index=9, ont_port=1, vlan=300, bandwidth_profile="RES-1G", tag_action="1"))
        done.append(f"added a stray service to ONT {stray}")
        return done

    async def delete_ont(self, network: str, ont_id: int) -> None:
        await self._exchange(network)
        ont = self._require(network, ont_id)
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
    def _service_position(ont: _MockOnt, ont_port: int, service_index: int) -> Optional[int]:
        for position, existing in enumerate(ont.state.services):
            if (existing.ont_port, existing.service_index) == (ont_port, service_index):
                return position
        return None

    def _require(self, network: str, ont_id: int) -> _MockOnt:
        ont = self._onts.get(ont_id)
        if ont is None:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        return ont

    def _snapshot(self, ont: _MockOnt) -> OntState:
        online = ont.on_wire and self._clock() >= ont.online_at
        return ont.state.model_copy(deep=True, update={
            "oper_state": OperState.ONLINE if online else OperState.OFFLINE})

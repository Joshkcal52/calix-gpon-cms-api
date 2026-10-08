"""
NBI Gateway
The only door to CMS. Routes and jobs call the gateway; the gateway calls a
backend (the mock CMS today, a real CMS client later).

The gateway adds, in one place:
  - a timeout on every call
  - retries with backoff, for reads only (writes are retried by the worker,
    which first checks what was already applied)
  - a circuit breaker, so a CMS outage fails fast instead of stalling the API
  - a cap on concurrent CMS calls
  - typed errors (app/nbi/errors.py) that map to clean API responses

Backends only translate one operation into one CMS exchange and raise
NbiError subclasses. They need no resilience logic of their own.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, EthernetService, OntPorts, OntProfile, OntSpec, OntState, PortChange,
    ServiceChange, Vlan
)
from app.nbi.breaker import CircuitBreaker, OPEN
from app.nbi.errors import NbiError, NbiNotConfiguredError, NbiTimeoutError, NbiTransportError

logger = logging.getLogger(__name__)


class NbiBackend(Protocol):
    """What a CMS client has to provide to sit behind the gateway"""

    name: str

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]: ...

    async def get_ont(self, network: str, ont_id: int) -> OntState: ...

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]: ...

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]: ...

    async def list_onts(self, network: str) -> List[OntState]: ...

    async def create_ont(self, network: str, spec: OntSpec) -> OntState: ...

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None: ...

    async def delete_ont(self, network: str, ont_id: int) -> None: ...

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts: ...

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None: ...

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None: ...

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None: ...

    async def list_ont_profiles(self, network: str) -> List[OntProfile]: ...

    async def list_vlans(self, network: str) -> List[Vlan]: ...

    async def close(self) -> None: ...


class NbiGateway:
    def __init__(self, backend: NbiBackend, *, timeout: float = 10.0, max_retries: int = 2,
                 retry_backoff: float = 0.25, max_concurrency: int = 8,
                 breaker: Optional[CircuitBreaker] = None):
        self.backend = backend
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.breaker = breaker or CircuitBreaker()
        self._slots = asyncio.Semaphore(max_concurrency)

    # --- Operations ------------------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        return await self._call("list_discovered_onts",
                                lambda: self.backend.list_discovered_onts(network), idempotent=True)

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        return await self._call("get_ont",
                                lambda: self.backend.get_ont(network, ont_id), idempotent=True)

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        return await self._call("find_ont_by_serial",
                                lambda: self.backend.find_ont_by_serial(network, serial_number),
                                idempotent=True)

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        """ONTs provisioned for a subscriber. Use get_ont() on a match for its services."""
        return await self._call("find_onts_by_subscriber",
                                lambda: self.backend.find_onts_by_subscriber(network, subscriber_id),
                                idempotent=True)

    async def list_onts(self, network: str) -> List[OntState]:
        """Every provisioned ONT on a network, without services. Slow on a large network."""
        return await self._call("list_onts", lambda: self.backend.list_onts(network), idempotent=True)

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        return await self._call("create_ont",
                                lambda: self.backend.create_ont(network, spec), idempotent=False)

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        return await self._call("add_ethernet_service",
                                lambda: self.backend.add_ethernet_service(network, ont_id, service),
                                idempotent=False)

    async def delete_ont(self, network: str, ont_id: int) -> None:
        return await self._call("delete_ont",
                                lambda: self.backend.delete_ont(network, ont_id), idempotent=False)

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        """Ethernet ports and the router side of one ONT. Costs one CMS call per port."""
        return await self._call("get_ont_ports", lambda: self.backend.get_ont_ports(network, ont_id),
                                idempotent=True)

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        return await self._call("update_port",
                                lambda: self.backend.update_port(network, ont_id, attachment, number, change),
                                idempotent=False)

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        return await self._call(
            "update_service",
            lambda: self.backend.update_service(network, ont_id, attachment, ont_port, service_index, change),
            idempotent=False)

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        return await self._call(
            "delete_service",
            lambda: self.backend.delete_service(network, ont_id, attachment, ont_port, service_index),
            idempotent=False)

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        return await self._call("list_ont_profiles", lambda: self.backend.list_ont_profiles(network),
                                idempotent=True)

    async def list_vlans(self, network: str) -> List[Vlan]:
        return await self._call("list_vlans", lambda: self.backend.list_vlans(network), idempotent=True)

    def health(self) -> Dict[str, Any]:
        return {"backend": self.backend.name, "breaker": self.breaker.snapshot()}

    async def close(self) -> None:
        await self.backend.close()

    # --- Resilience ------------------------------------------------------

    async def _call(self, operation: str, fn: Callable[[], Awaitable[Any]], *, idempotent: bool) -> Any:
        attempts = 1 + (self.max_retries if idempotent else 0)

        for attempt in range(1, attempts + 1):
            self.breaker.allow(operation)  # raises NbiCircuitOpenError while CMS is rested

            try:
                async with self._slots:
                    result = await asyncio.wait_for(fn(), timeout=self.timeout)
            except asyncio.TimeoutError:
                error: NbiTransportError = NbiTimeoutError(self._timeout_message(operation, idempotent))
            except NbiTransportError as e:
                error = e
            except NbiError as e:
                # CMS answered, it just said no. That is not an outage.
                self.breaker.record_success()
                e.operation = operation
                raise
            except BaseException:
                self.breaker.abandon()
                raise
            else:
                self.breaker.record_success()
                return result

            error.operation = operation
            self.breaker.record_failure()
            logger.warning(f"CMS call {operation} failed (attempt {attempt}/{attempts}): {error.message}")

            if attempt == attempts or self.breaker.state == OPEN:
                raise error
            await asyncio.sleep(self.retry_backoff * 2 ** (attempt - 1))

    def _timeout_message(self, operation: str, idempotent: bool) -> str:
        message = f"CMS did not answer within {self.timeout:g} seconds while running {operation}."
        if not idempotent:
            message += " The change may or may not have been applied; check the ONT before retrying."
        return message


class NotConfiguredBackend:
    """Placeholder used when MOCK_MODE is off and no real CMS client exists yet"""

    name = "not-configured"

    def _refuse(self) -> NbiNotConfiguredError:
        return NbiNotConfiguredError(
            "No CMS connection is configured. Set CMS_HOST (with CMS_USERNAME and CMS_PASSWORD) "
            "to reach a CMS, E7_NODES to use each E7's command line, or MOCK_MODE=true to use the simulated one.")

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        raise self._refuse()

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        raise self._refuse()

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        raise self._refuse()

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        raise self._refuse()

    async def list_onts(self, network: str) -> List[OntState]:
        raise self._refuse()

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        raise self._refuse()

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        raise self._refuse()

    async def delete_ont(self, network: str, ont_id: int) -> None:
        raise self._refuse()

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        raise self._refuse()

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        raise self._refuse()

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        raise self._refuse()

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        raise self._refuse()

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        raise self._refuse()

    async def list_vlans(self, network: str) -> List[Vlan]:
        raise self._refuse()

    async def close(self) -> None:
        return None


def build_gateway(settings: Any) -> NbiGateway:
    """Create the gateway for the current configuration"""
    if settings.mock_mode:
        from app.nbi.mock_cms import MockCmsBackend
        backend: NbiBackend = MockCmsBackend()
        logger.warning("MOCK_MODE is on: CMS calls go to a simulated CMS, not a real one")
    elif settings.cms_host and getattr(settings, "e7_nodes", None):
        raise ValueError("Set CMS_HOST (provision through CMS) or E7_NODES (through each E7's CLI), not both")
    elif getattr(settings, "e7_nodes", None):
        from app.nbi.e7_backend import backend_from_settings
        backend = backend_from_settings(settings)
        logger.warning(f"E7 CLI backend: provisioning over SSH on {', '.join(backend.nodes)}. NOT VERIFIED against "
                       f"a real E7; see app/nbi/e7_backend.py")
        if (settings.e7_known_hosts or "").lower() == "none":
            logger.warning("E7_KNOWN_HOSTS is none: the E7s' SSH host keys are not checked")
    elif settings.cms_host:
        from app.nbi.cms_backend import CmsBackend
        from app.nbi.router_side import router_side_addressing
        backend = CmsBackend(
            host=settings.cms_host, port=settings.cms_port,
            username=settings.cms_username, password=settings.cms_password or "",
            timeout=settings.nbi_timeout_seconds, router=router_side_addressing(settings),
            scheme=settings.cms_scheme, verify=settings.cms_verify())
        logger.info(f"CMS backend: {backend.client.url}")
        if settings.cms_scheme == "http":
            logger.warning("CMS_SCHEME is http: the CMS login and every request cross the network unencrypted")
        elif settings.cms_verify() is False:
            logger.warning("CMS_VERIFY_TLS is false: the CMS server's certificate is not checked")
    else:
        backend = NotConfiguredBackend()

    return NbiGateway(
        backend,
        timeout=settings.nbi_timeout_seconds,
        max_retries=settings.nbi_max_retries,
        max_concurrency=settings.nbi_max_concurrency,
        breaker=CircuitBreaker(
            failure_threshold=settings.nbi_breaker_failure_threshold,
            recovery_seconds=settings.nbi_breaker_recovery_seconds),
    )
